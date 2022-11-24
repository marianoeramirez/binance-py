import asyncio
import gzip
import json
import logging
from asyncio import sleep
from enum import Enum
from random import random
from socket import gaierror
from typing import Optional, Dict

import websockets as ws
from websockets.exceptions import ConnectionClosedError, ConnectionClosed

from .client import AsyncClient
from .exceptions import BinanceWebsocketUnableToConnect

KEEPALIVE_TIMEOUT = 5 * 60  # 5 minutes


class WSListenerState(Enum):
    INITIALISING = 'Initialising'
    STREAMING = 'Streaming'
    RECONNECTING = 'Reconnecting'
    EXITING = 'Exiting'


class EventType(Enum):
    NEW_MESSAGE = 'new_message'
    ON_CONNECT = 'on_connect'


class ReconnectingWebsocket:
    MAX_RECONNECTS = 5
    MAX_RECONNECT_SECONDS = 60
    MIN_RECONNECT_WAIT = 0.1
    TIMEOUT = 10
    NO_MESSAGE_RECONNECT_TIMEOUT = 60

    def __init__(
            self, url: str, path: Optional[str] = None, prefix: str = 'ws/', is_binary: bool = False, exit_coro=None
    ):
        self._loop = asyncio.get_event_loop()
        self._log = logging.getLogger(__name__)
        self._path = path
        self._url = url
        self._exit_coro = exit_coro
        self._prefix = prefix
        self._reconnects = 0
        self._is_binary = is_binary
        self._conn = None
        self._socket = None
        self.ws: Optional[ws.WebSocketClientProtocol] = None  # type: ignore
        self.ws_state = WSListenerState.INITIALISING
        self._handle_read_loop = None

        self._disconnected = asyncio.get_event_loop().create_future()
        self._disconnected.set_result(None)

        self._events = {}

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._exit_coro:
            await self._exit_coro(self._path)
        self.ws_state = WSListenerState.EXITING
        if self.ws:
            self.ws.fail_connection()
        if self._conn and hasattr(self._conn, 'protocol'):
            await self._conn.__aexit__(exc_type, exc_val, exc_tb)
        self.ws = None
        if not self._handle_read_loop:
            self._log.error("CANCEL read_loop")
            await self._kill_read_loop()

    async def connect(self):
        await self._before_connect()
        ws_url = self._url + self._prefix + self._path
        # ws_url = "ws://localhost:8765"
        self._conn = ws.connect(ws_url, close_timeout=0.1)  # type: ignore

        try:
            self.ws = await self._conn.__aenter__()
        except Exception as e:  # noqa
            self._log.error(e)
            await self._reconnect()
            return
        self.ws_state = WSListenerState.STREAMING
        self._reconnects = 0
        await self._after_connect()
        # To manage the "cannot call recv while another coroutine is already waiting for the next message"
        if not self._handle_read_loop:
            await self._read_loop()

    async def _kill_read_loop(self):
        self.ws_state = WSListenerState.EXITING
        while self._handle_read_loop:
            await sleep(0.1)

    async def _before_connect(self):
        pass

    async def _after_connect(self):
        self._log.info("Connected")
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
            self._log.debug(f'error parsing evt json:{evt}')
            return None

    async def _handle_message(self, evt):
        msg = self._parse_message(evt)
        for callback in self._events[EventType.NEW_MESSAGE]:
            await callback(msg)

    async def _read_loop(self):
        self._log.debug(f"Start read loop")
        try:
            while True:
                try:
                    if self.ws_state == WSListenerState.RECONNECTING:
                        await self._run_reconnect()

                    if not self.ws or self.ws_state != WSListenerState.STREAMING:
                        await self._wait_for_reconnect()
                        break
                    elif self.ws_state == WSListenerState.EXITING:
                        break
                    elif self.ws.state == ws.protocol.State.CLOSING:  # type: ignore
                        await asyncio.sleep(0.1)
                        continue
                    elif self.ws.state == ws.protocol.State.CLOSED:  # type: ignore
                        await self._reconnect()
                    elif self.ws_state == WSListenerState.STREAMING:
                        res = await asyncio.wait_for(self.ws.recv(), timeout=self.TIMEOUT)
                        await self._handle_message(res)
                except asyncio.TimeoutError:
                    self._log.debug(f"no message in {self.TIMEOUT} seconds")
                    # _no_message_received_reconnect
                except asyncio.CancelledError as e:
                    self._log.debug(f"cancelled error {e}")
                    break
                except asyncio.IncompleteReadError as e:
                    self._log.debug(f"incomplete read error ({e})")
                except ConnectionClosedError as e:
                    self._log.debug(f"connection close error ({e})")
                except gaierror as e:
                    self._log.debug(f"DNS Error ({e})")
                except BinanceWebsocketUnableToConnect as e:
                    self._log.debug(f"BinanceWebsocketUnableToConnect ({e})")
                    break
                except Exception as e:
                    self._log.debug(f"Unknown exception ({e})")
                    continue
        finally:
            self._handle_read_loop = None  # Signal the coro is stopped
            self._reconnects = 0

    async def _run_reconnect(self):
        await self.before_reconnect()
        if self._reconnects < self.MAX_RECONNECTS:
            reconnect_wait = self._get_reconnect_wait(self._reconnects)
            self._log.debug(
                f"websocket reconnecting. {self.MAX_RECONNECTS - self._reconnects} reconnects left - "
                f"waiting {reconnect_wait}"
            )
            await asyncio.sleep(reconnect_wait)
            await self.connect()
        else:
            self._log.error(f'Max reconnections {self.MAX_RECONNECTS} reached:')
            raise BinanceWebsocketUnableToConnect

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
            self._log.info(f"Sending: {msg}")
            await self.ws.send(msg)
        except ConnectionClosed:
            self._log.debug(f"Can't send the message connection closed")
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
        self._log.debug('No message received, reconnecting')
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


class KeepAliveWebsocket(ReconnectingWebsocket):

    def __init__(
            self, client: AsyncClient, url, keepalive_type, prefix='ws/', is_binary=False, exit_coro=None,
            user_timeout=None
    ):
        super().__init__(path=None, url=url, prefix=prefix, is_binary=is_binary, exit_coro=exit_coro)
        self._keepalive_type = keepalive_type
        self._client = client
        self._user_timeout = user_timeout or KEEPALIVE_TIMEOUT
        self._timer = None

    async def __aexit__(self, *args, **kwargs):
        if not self._path:
            return
        if self._timer:
            self._timer.cancel()
            self._timer = None
        await super().__aexit__(*args, **kwargs)

    async def _before_connect(self):
        if not self._path:
            self._path = await self._get_listen_key()

    async def _after_connect(self):
        await super(KeepAliveWebsocket, self)._after_connect()
        self._start_socket_timer()

    def _start_socket_timer(self):
        self._timer = self._loop.call_later(
            self._user_timeout,
            lambda: asyncio.create_task(self._keepalive_socket())
        )

    async def _get_listen_key(self):
        from binance.streams import BinanceSocketType
        if self._keepalive_type == BinanceSocketType.ACCOUNT:
            listen_key = await self._client.stream_get_listen_key()
        elif self._keepalive_type == BinanceSocketType.SPOT:  # cross-margin
            listen_key = await self._client.margin_stream_get_listen_key()
        elif self._keepalive_type == BinanceSocketType.USD_M_FUTURES:
            listen_key = await self._client.futures_stream_get_listen_key()
        elif self._keepalive_type == BinanceSocketType.COIN_M_FUTURES:
            listen_key = await self._client.futures_coin_stream_get_listen_key()
        else:  # isolated margin
            # Passing symbol for isolated margin
            listen_key = await self._client.isolated_margin_stream_get_listen_key(self._keepalive_type)
        return listen_key

    async def _keepalive_socket(self):
        try:
            listen_key = await self._get_listen_key()
            if listen_key != self._path:
                self._log.debug("listen key changed: reconnect")
                self._path = listen_key
                await self._reconnect()
            else:
                self._log.debug("listen key same: keepalive")
                if self._keepalive_type == 'user':
                    await self._client.stream_keepalive(self._path)
                elif self._keepalive_type == 'margin':  # cross-margin
                    await self._client.margin_stream_keepalive(self._path)
                elif self._keepalive_type == 'futures':
                    await self._client.futures_stream_keepalive(self._path)
                elif self._keepalive_type == 'coin_futures':
                    await self._client.futures_coin_stream_keepalive(self._path)
                else:  # isolated margin
                    # Passing symbol for isolated margin
                    await self._client.isolated_margin_stream_keepalive(self._keepalive_type, self._path)
        except Exception:
            pass  # Ignore
        finally:
            self._start_socket_timer()
