# news_crawler/fetcher.py
from __future__ import annotations
import time
from typing import Optional, Dict, Any
import httpx 

class Fetcher:
    """
    Centralized HTTP client with retries + backoff.
    Only returns text for HTML/XML payloads (guards against binary).
    """

    def __init__(
        self,
        user_agent: str = "NewsCrawlerMVP/1.0 (+https://example.local)",
        timeout: float = 20.0,
        max_retries: int = 3,
        backoff_seconds: float = 0.5,
        follow_redirects: bool = True,
        default_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.user_agent = user_agent
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.follow_redirects = follow_redirects
        self.default_headers = {"User-Agent": self.user_agent, **(default_headers or {})}

    def _request(self, url: str) -> Optional["httpx.Response"]:
        last_exc: Optional[BaseException] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                r = httpx.get(
                    url,
                    headers=self.default_headers,
                    timeout=self.timeout,
                    follow_redirects=self.follow_redirects,
                )
                return r
            except Exception as e:
                last_exc = e
                if attempt < self.max_retries:
                    time.sleep(self.backoff_seconds * attempt)
        # If all attempts fail, surface nothing (MVP behavior)
        return None

    def get_text(self, url: str) -> Optional[str]:
        """
        Returns decoded text for HTML/XML responses; otherwise None.
        """
        r = self._request(url)
        if r is None:
            return None
        if r.status_code >= 400:
            return None
        ctype = (r.headers.get("content-type") or "").lower()
        if "html" not in ctype and "xml" not in ctype and "rss" not in ctype:
            return None
        return r.text

    def get_bytes(self, url: str) -> Optional[bytes]:
        r = self._request(url)
        if r is None or r.status_code >= 400:
            return None
        return r.content

    def get_json(self, url: str) -> Optional[Any]:
        r = self._request(url)
        if r is None or r.status_code >= 400:
            return None
        try:
            return r.json()
        except Exception:
            return None
