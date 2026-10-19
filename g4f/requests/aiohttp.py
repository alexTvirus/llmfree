from __future__ import annotations

import json
import asyncio
import weakref
from aiohttp import (
    ClientSession,
    ClientResponse,
    ClientTimeout,
    BaseConnector,
    TCPConnector,
    FormData,
)
from typing import AsyncIterator, Any, Optional
from urllib.parse import urlparse

from .defaults import DEFAULT_HEADERS, has_brotli
from ..errors import MissingRequirementsError
from ..config import AppConfig  # for security checks

_loop_connectors: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, TCPConnector] = weakref.WeakKeyDictionary()


def get_shared_connector() -> Optional[TCPConnector]:
    """Retrieve or create a loop-bound TCPConnector with keep-alive and DNS caching."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return None
    connector = _loop_connectors.get(loop)
    if connector is None or connector.closed:
        connector = TCPConnector(
            limit=100,
            limit_per_host=20,
            keepalive_timeout=30,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        _loop_connectors[loop] = connector
    return connector


async def close_shared_connectors() -> None:
    """Close all open shared connectors."""
    for connector in list(_loop_connectors.values()):
        if not connector.closed:
            await connector.close()
    _loop_connectors.clear()


class StreamResponse(ClientResponse):
    async def iter_lines(self) -> AsyncIterator[bytes]:
        async for line in self.content:
            yield line.rstrip(b"\r\n")

    async def iter_content(self) -> AsyncIterator[bytes]:
        async for chunk in self.content.iter_any():
            yield chunk

    async def json(self, content_type: str = None) -> Any:
        return await super().json(content_type=content_type)

    async def sse(self) -> AsyncIterator[dict]:
        """Asynchronously iterate over the Server-Sent Events of the response."""
        async for line in self.content:
            if line.startswith(b"data: "):
                chunk = line[6:]
                if chunk.startswith(b"[DONE]"):
                    break
                try:
                    yield json.loads(chunk)
                except json.JSONDecodeError:
                    continue


class StreamSession:
    """
    Wrapper around aiohttp.ClientSession that:
      - stores session-level headers (lowercased keys)
      - intercepts request(...) calls to optionally route to a header-provided target
        (e.g. headers: { "url-to-send": "https://proxy..." })
      - honors AppConfig.allow_headers_proxy and AppConfig.headers_proxy_whitelist
    """

    def __init__(
        self,
        headers=None,
        timeout: int = 0,
        connector: BaseConnector = None,
        proxy: str = None,
        proxies=None,
        impersonate=None,
        connector_owner: bool = None,
        **kwargs,
    ):
        if proxies is None:
            proxies = {}
        if headers is None:
            headers = {}
        if impersonate:
            headers = {**DEFAULT_HEADERS, **headers}
        if not has_brotli and "br" in headers.get("accept-encoding", ""):
            headers["accept-encoding"] = "gzip, deflate"
        if isinstance(timeout, tuple):
            timeout, connect = timeout
            timeout = ClientTimeout(timeout, connect)
        elif timeout is not None:
            timeout = ClientTimeout(timeout)
        if proxy is None:
            proxy = proxies.get("all", proxies.get("https"))

        actual_connector = get_connector(connector, proxy)
        if actual_connector is None and not proxy:
            actual_connector = get_shared_connector()
            if connector_owner is None and actual_connector is not None:
                connector_owner = False
        if connector_owner is None:
            connector_owner = True

        # Save session-level headers for lookup (lowercased keys)
        self._session_headers = {k.lower(): v for k, v in headers.items()} if headers else {}
        self._inner = ClientSession(
            **kwargs,
            timeout=timeout,
            response_class=StreamResponse,
            connector=actual_connector,
            connector_owner=connector_owner,
            headers=headers,
        )

    async def __aenter__(self) -> "StreamSession":
        # Return wrapper so we can intercept request() calls
        return self

    async def __aexit__(self, *args, **kwargs) -> None:
        await self._inner.close()

    async def close(self) -> None:
        await self._inner.close()

    async def _should_route_via_header(self, combined_headers: dict) -> Optional[str]:
        """
        Decide whether we should route using header url-to-send.
        Returns the target URL string if allowed, otherwise None.
        """
        proxy_target = combined_headers.get("url-to-send") or combined_headers.get("url_to_send")
        if not proxy_target:
            return None

        # Security checks controlled by AppConfig
        if not getattr(AppConfig, "allow_headers_proxy", False):
            return None

        try:
            parsed = urlparse(proxy_target)
            host = (parsed.hostname or "").lower()
        except Exception:
            return None

        whitelist = getattr(AppConfig, "headers_proxy_whitelist", []) or []
        if whitelist:
            if host in whitelist:
                return proxy_target
            return None

        # If whitelist empty and allow_headers_proxy=True: allow all hosts (use cautiously)
        return proxy_target

    async def request(self, method: str, url: str, **kwargs) -> StreamResponse:
        """
        Intercept requests to optionally route to header-provided target.
        Works with the usage pattern:
            async with StreamSession(headers=...) as session:
                async with session.post(url, ...) as resp:
                    ...
        """
        call_headers = kwargs.get("headers") or {}
        # combine session headers + per-call headers (lower-case keys)
        combined = {**(self._session_headers or {}), **{k.lower(): v for k, v in call_headers.items()}}
        target = await self._should_route_via_header(combined)
        target_url = target if target else url

        # keep headers param as provided (ClientSession will merge session headers and per-call headers)
        return await self._inner.request(method, target_url, **kwargs)

    # expose convenience methods like ClientSession
    async def get(self, url: str, **kwargs):
        return await self.request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs):
        return await self.request("POST", url, **kwargs)

    async def put(self, url: str, **kwargs):
        return await self.request("PUT", url, **kwargs)

    async def patch(self, url: str, **kwargs):
        return await self.request("PATCH", url, **kwargs)

    async def delete(self, url: str, **kwargs):
        return await self.request("DELETE", url, **kwargs)

    async def head(self, url: str, **kwargs):
        return await self.request("HEAD", url, **kwargs)

    async def options(self, url: str, **kwargs):
        return await self.request("OPTIONS", url, **kwargs)


def get_connector(
    connector: BaseConnector = None, proxy: str = None, rdns: bool = False
) -> Optional[BaseConnector]:
    if proxy and not connector:
        try:
            from aiohttp_socks import ProxyConnector

            if proxy.startswith("socks5h://"):
                proxy = proxy.replace("socks5h://", "socks5://")
                rdns = True
            connector = ProxyConnector.from_url(proxy, rdns=rdns)
        except ImportError:
            raise MissingRequirementsError(
                'Install "aiohttp_socks" package for proxy support'
            )
    return connector
