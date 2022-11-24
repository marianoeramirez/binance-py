"""An unofficial Python wrapper for the Binance exchange API v3

The original author is Sam McHardy, (https://github.com/sammchardy/python-binance)
I just take his work to improve and maintain.

.. moduleauthor:: Mariano Ramirez

"""

__version__ = '1.0.0'

from binance.client import Client, AsyncClient  # noqa
from binance.depthcache import DepthCacheManager, OptionsDepthCacheManager  # noqa
from binance.streams import SocketManager  # noqa
