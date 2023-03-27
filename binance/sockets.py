import asyncio
import gzip
import ujson as json
import logging
from asyncio import sleep
from random import random
from socket import gaierror
from typing import Optional, Dict

from .enums import SocketType, EventType, WSListenerState
from websockets.exceptions import ConnectionClosedError, ConnectionClosed
from websockets.client import WebSocketClientProtocol
from websockets.legacy.protocol import State
import websockets.client

from .client import AsyncClient
from .exceptions import WebsocketUnableToConnect

KEEPALIVE_TIMEOUT = 5 * 60  # 5 minutes

logger = logging.getLogger(__name__)


class ReconnectingWebsocket:
    MAX_RECONNECTS = 5
    MAX_RECONNECT_SECONDS = 60
    MIN_RECONNECT_WAIT = 0.1
    TIMEOUT = 10
    NO_MESSAGE_RECONNECT_TIMEOUT = 60

    def __init__(
            self, client: AsyncClient, url: str, path: Optional[str] = None,
            prefix: str = 'ws/',
            is_binary: bool = False, exit_coro=None,
            socket_type: SocketType = None,
            user_timeout=None
    ):
        self.client = client
        self.socket_type = socket_type
        self._loop = asyncio.get_event_loop()
        self._path = path
        self._url = url
        self._exit_coro = exit_coro
        self._prefix = prefix
        self._reconnects = 0
        self._is_binary = is_binary
        self._conn = None
        self._socket = None
        self.ws: Optional[WebSocketClientProtocol] = None  # type: ignore
        self.ws_state = WSListenerState.INITIALISING
        self._handle_read_loop = None

        self._user_timeout = user_timeout or KEEPALIVE_TIMEOUT
        self._timer = None

        self._disconnected = asyncio.get_event_loop().create_future()
        self._disconnected.set_result(None)

        self._events = {}

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.exit()

    def get_url(self):
        return self._url + self._prefix + self._path

    async def connect(self):
        await self._before_connect()
        ws_url = self.get_url()
        logger.info(f"WSURL: {ws_url}")
        self._conn = websockets.client.connect(ws_url, close_timeout=0.1)  # type: ignore

        try:
            self.ws = await self._conn.__aenter__()
        except Exception as e:  # noqa
            logger.exception(e)
            await self._reconnect()
            return
        self.ws_state = WSListenerState.STREAMING
        self._reconnects = 0
        await self._after_connect()
        # To manage the "cannot call recv while another coroutine is already waiting for the next message"
        if not self._handle_read_loop:
            await self._read_loop()

    async def exit(self):
        if self._exit_coro:
            await self._exit_coro(self._path)
        self.ws_state = WSListenerState.EXITING
        if self.ws:
            self.ws.fail_connection()
        # TODO if self._conn and hasattr(self._conn, 'protocol'):
        #     await self._conn.close()
        self.ws = None
        if not self._handle_read_loop:
            logger.error("CANCEL read_loop")
            await self._kill_read_loop()

    async def _kill_read_loop(self):
        self.ws_state = WSListenerState.EXITING
        while self._handle_read_loop:
            await sleep(0.1)

    async def _before_connect(self):
        pass

    async def _after_connect(self):
        logger.info("Connected")
        for callback in self._events[EventType.ON_CONNECT]:
            await callback()

    def _parse_message(self, evt):
        if self._is_binary:
            try:
                evt = gzip.decompress(evt)
            except (ValueError, OSError):
                return None
        try:
            return json.loads(evt)
        except ValueError:
            logger.debug(f'error parsing evt json:{evt}')
            return None

    async def _handle_message(self, evt):
        msg = self._parse_message(evt)
        for callback in self._events[EventType.NEW_MESSAGE]:
            await callback(msg)

    async def _read_loop(self):
        logger.debug("Start read loop")
        try:
            while True:
                try:
                    if self.ws_state == WSListenerState.RECONNECTING:
                        logger.info(f"Status RECONNECTING")
                        await self._run_reconnect()

                    if not self.ws or self.ws_state != WSListenerState.STREAMING:
                        logger.info(f"Status STREAMING")
                        await self._wait_for_reconnect()
                        break
                    elif self.ws_state == WSListenerState.EXITING:
                        logger.info(f"Status EXITING")
                        break
                    elif self.ws.state == State.CLOSING:  # type: ignore
                        logger.info(f"Status CLOSING")
                        await asyncio.sleep(0.1)
                        continue
                    elif self.ws.state == State.CLOSED:  # type: ignore
                        logger.info(f"Status Closed")
                        await self._reconnect()
                    elif self.ws_state == WSListenerState.STREAMING:
                        res = await asyncio.wait_for(self.ws.recv(), timeout=self.TIMEOUT)
                        await self._handle_message(res)
                except asyncio.TimeoutError:
                    logger.debug(f"no message in {self.TIMEOUT} seconds")
                    # _no_message_received_reconnect
                except asyncio.CancelledError as e:
                    logger.debug(f"cancelled error {e}")
                    break
                except asyncio.IncompleteReadError as e:
                    logger.debug(f"incomplete read error ({e})")
                except ConnectionClosedError as e:
                    logger.debug(f"connection close error ({e})")
                except gaierror as e:
                    logger.debug(f"DNS Error ({e})")
                except WebsocketUnableToConnect as e:
                    logger.debug(f"WebsocketUnableToConnect ({e})")
                    break
                except Exception as e:
                    logger.debug(f"Unknown exception ({e})")
                    logger.exception(e)
                    continue
        finally:
            self._handle_read_loop = None  # Signal the coro is stopped
            self._reconnects = 0

    async def _run_reconnect(self):
        await self.before_reconnect()
        if self._reconnects < self.MAX_RECONNECTS:
            reconnect_wait = self._get_reconnect_wait(self._reconnects)
            logger.debug(
                f"websocket reconnecting. {self.MAX_RECONNECTS - self._reconnects} reconnects left - "
                f"waiting {reconnect_wait}"
            )
            await asyncio.sleep(reconnect_wait)
            await self.connect()
        else:
            logger.error(f'Max reconnections {self.MAX_RECONNECTS} reached:')
            raise WebsocketUnableToConnect

    def add_event_handler(
            self,
            callback,
            event: EventType):
        if self._events.get(event) is None:
            self._events[event] = []
        self._events[event].append(callback)

    async def send(self, msg: Dict):
        msg = json.dumps(msg)
        try:
            logger.info(f"Sending: {msg}")
            await self.ws.send(msg)
        except ConnectionClosed:
            logger.debug("Can't send the message connection closed")
        return

    async def _wait_for_reconnect(self):
        while self.ws_state != WSListenerState.STREAMING and self.ws_state != WSListenerState.EXITING:
            await sleep(0.1)

    def _get_reconnect_wait(self, attempts: int) -> int:
        expo = 2 ** attempts
        return round(random() * min(self.MAX_RECONNECT_SECONDS, expo - 1) + 1)

    async def before_reconnect(self):
        if self.ws and self._conn:
            await self._conn.__aexit__(None, None, None)
            self.ws = None
        self._reconnects += 1

    def _no_message_received_reconnect(self):
        logger.debug('No message received, reconnecting')
        self.ws_state = WSListenerState.RECONNECTING

    async def _reconnect(self):
        self.ws_state = WSListenerState.RECONNECTING

    @property
    def disconnected(self) -> asyncio.Future:
        """
        Property with a ``Future`` that resolves upon disconnection.

        Example
            .. code-block:: python

                # Wait for a disconnection to occur
                try:
                    await client.disconnected
                except OSError:
                    print('Error on disconnect')
        """
        return asyncio.shield(self._disconnected)

    async def _run_until_disconnected(self):
        try:
            # Make a high-level request to notify that we want updates
            return await self.disconnected
        except KeyboardInterrupt:
            pass
        finally:
            pass
            # await self.disconnect()

    async def _start(self):
        await self.connect()

    def run_until_disconnected(self):
        """
        Runs the event loop until the library is disconnected.


        """
        self._loop.run_until_complete(self._start())

        #
        # if self._loop.is_running():
        #     return self._run_until_disconnected()
        # try:
        #     return self._loop.run_until_complete(self._run_until_disconnected())
        # except KeyboardInterrupt:
        #     pass
        # finally:
        #     pass
        #     # No loop.run_until_complete; it's already syncified
        #     # self.disconnect()


import asyncio
from contextlib import suppress


class Periodic:
    def __init__(self, func, time):
        self.func = func
        self.time = time
        self.is_started = False
        self._task = None

    async def start(self):
        logger.debug('Periodic Start')
        if not self.is_started:
            self.is_started = True
            # Start task to call func periodically:
            self._task = asyncio.ensure_future(self._run())

    async def stop(self):
        logger.debug('Periodic stop')
        if self.is_started:
            self.is_started = False
            # Stop task and await it stopped:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    async def _run(self):
        logger.debug('Periodic Run')
        while True:
            await asyncio.sleep(self.time)
            await self.func()


class KeepAliveWebsocket(ReconnectingWebsocket):

    def __init__(self, client: AsyncClient, url: str, path: Optional[str] = None, prefix: str = 'ws/',
                 is_binary: bool = False, exit_coro=None, socket_type: SocketType = None, user_timeout=None):
        super().__init__(client, url, path, prefix, is_binary, exit_coro, socket_type, user_timeout)
        self._keepalive_periodic = Periodic(self._keepalive_socket, 20)

    async def __aexit__(self, *args, **kwargs):
        if not self._path:
            return
        if self._timer:
            self._timer.cancel()
            self._timer = None
        await super().__aexit__(*args, **kwargs)

    async def _before_connect(self):
        self._listen_key = await self._get_listen_key()

    async def _after_connect(self):
        await super(KeepAliveWebsocket, self)._after_connect()
        await self._start_socket_timer()

    async def _start_socket_timer(self):
        self._timer = self._loop.call_later(
            self._user_timeout,
            lambda: asyncio.create_task(self._keepalive_socket())
        )
        await self._keepalive_periodic.start()

    def get_url(self):
        param = ''
        if self._listen_key:
            param = f"?listenKey={self._listen_key}"
        path = self._path or ''
        return self._url + self._prefix + path + param

    async def _get_listen_key(self):
        if self.socket_type == SocketType.ACCOUNT:
            listen_key = await self.client.stream_get_listen_key()
        elif self.socket_type == SocketType.SPOT:  # cross-margin
            listen_key = await self.client.margin_stream_get_listen_key()
        elif self.socket_type == SocketType.USD_M_FUTURES:
            listen_key = await self.client.futures_stream_get_listen_key()
        elif self.socket_type == SocketType.COIN_M_FUTURES:
            listen_key = await self.client.futures_coin_stream_get_listen_key()
        else:  # isolated margin
            # Passing symbol for isolated margin
            listen_key = await self.client.isolated_margin_stream_get_listen_key(self.socket_type)
        return listen_key

    async def _keepalive_socket(self):
        try:
            listen_key = await self._get_listen_key()
            if listen_key != self._listen_key:
                logger.info("listen key changed: reconnect")
                self._listen_key = listen_key
                await self._reconnect()
            else:
                logger.info("listen key same: keepalive")
                if self.socket_type == SocketType.ACCOUNT:
                    await self.client.stream_keepalive(self._listen_key)
                elif self.socket_type == SocketType.SPOT:  # cross-margin
                    await self.client.margin_stream_keepalive(self._listen_key)
                elif self.socket_type == SocketType.USD_M_FUTURES:
                    await self.client.futures_stream_keepalive(self._listen_key)
                elif self.socket_type == SocketType.COIN_M_FUTURES:
                    await self.client.futures_coin_stream_keepalive(self._listen_key)
                else:  # isolated margin
                    # Passing symbol for isolated margin
                    await self.client.isolated_margin_stream_keepalive(self.socket_type, self._listen_key)
        except Exception as e:
            logger.exception(e)
