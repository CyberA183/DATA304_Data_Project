"""Async HTTP client wrappers with proxy rotation, retries and polite concurrency.

Two transports are used:
  * Airbnb  -> curl_cffi AsyncSession with a Chrome TLS fingerprint over an Apify
    residential proxy. This reliably passes Airbnb's edge checks.
  * Vrbo    -> curl_cffi over the Apify Unblocker proxy, which transparently solves
    the DataDome challenge that guards vrbo.com.

Every request is retried with backoff on transient status codes and network errors.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field
from typing import Any

from apify import Actor
from curl_cffi.requests import AsyncSession

# Status codes worth retrying (rate limits, edge hiccups, gateway errors).
RETRY_STATUSES = {408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 524}
# curl_cffi impersonation target used for Airbnb. A modern desktop Chrome.
AIRBNB_IMPERSONATE = "chrome"


@dataclass
class HttpResult:
    status: int
    text: str
    ok: bool
    elapsed: float
    error: str | None = None

    def json(self) -> Any:
        import json

        return json.loads(self.text)


@dataclass
class RequestClient:
    """Wraps a curl_cffi AsyncSession, an optional proxy source and a concurrency gate."""

    label: str
    proxy_configuration: Any | None = None
    static_proxy_url: str | None = None
    proxy_url_fn: Any | None = None
    impersonate: str | None = None
    verify: bool = True
    max_retries: int = 4
    base_backoff: float = 1.5
    concurrency: int = 8
    min_delay: float = 0.0
    _semaphore: asyncio.Semaphore = field(init=False)
    _last_request_at: float = field(default=0.0, init=False)
    _lock: asyncio.Lock = field(init=False)

    def __post_init__(self) -> None:
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._lock = asyncio.Lock()

    async def _proxy_url(self, session_id: str | None) -> str | None:
        if self.proxy_url_fn is not None:
            return self.proxy_url_fn(session_id)
        if self.static_proxy_url is not None:
            return self.static_proxy_url
        if self.proxy_configuration is None:
            return None
        try:
            return await self.proxy_configuration.new_url(session_id=session_id)
        except Exception as exc:  # pragma: no cover - defensive
            Actor.log.warning(f"[{self.label}] proxy url failed: {exc}")
            return None

    async def _throttle(self) -> None:
        if self.min_delay <= 0:
            return
        async with self._lock:
            now = asyncio.get_event_loop().time()
            wait = self._last_request_at + self.min_delay - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request_at = asyncio.get_event_loop().time()

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict | None = None,
        params: dict | None = None,
        json_body: Any | None = None,
        session_id: str | None = None,
        timeout: int = 45,
        ok_predicate=None,
    ) -> HttpResult:
        """Perform one request with retries. `ok_predicate(status, text)` decides success
        beyond the HTTP status (e.g. body must contain an expected key)."""
        loop = asyncio.get_event_loop()
        attempt = 0
        last: HttpResult | None = None
        while attempt <= self.max_retries:
            attempt += 1
            sid = session_id or f"{self.label}{random.randint(0, 10_000_000)}"
            proxy = await self._proxy_url(sid)
            await self._throttle()
            start = loop.time()
            async with self._semaphore:
                try:
                    kwargs: dict[str, Any] = {}
                    if self.impersonate:
                        kwargs["impersonate"] = self.impersonate
                    async with AsyncSession(verify=self.verify, **kwargs) as session:
                        resp = await session.request(
                            method,
                            url,
                            headers=headers,
                            params=params,
                            json=json_body,
                            proxy=proxy,
                            timeout=timeout,
                        )
                        text = resp.text
                        status = resp.status_code
                except Exception as exc:  # network / TLS / timeout
                    last = HttpResult(0, "", False, loop.time() - start, error=type(exc).__name__)
                    await self._sleep_backoff(attempt)
                    continue

            elapsed = loop.time() - start
            ok = status == 200
            if ok and ok_predicate is not None:
                ok = bool(ok_predicate(status, text))
            last = HttpResult(status, text, ok, elapsed)
            if ok:
                return last
            if status not in RETRY_STATUSES and status != 200:
                # Non-retryable HTTP error (e.g. 400/404) - return immediately.
                return last
            await self._sleep_backoff(attempt)
        return last if last is not None else HttpResult(0, "", False, 0.0, error="no_attempt")

    async def _sleep_backoff(self, attempt: int) -> None:
        delay = self.base_backoff * (2 ** (attempt - 1))
        delay = min(delay, 20.0) * (0.6 + random.random() * 0.8)
        await asyncio.sleep(delay)