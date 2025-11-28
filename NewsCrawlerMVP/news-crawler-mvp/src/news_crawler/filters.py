# news_crawler/filters.py
from __future__ import annotations

from datetime import datetime
from typing import Callable, Mapping, Sequence, Iterable
from urllib.parse import urlparse
from dataclasses import dataclass

from .models import Article  # see note below; if Article lives elsewhere, adjust import

Predicate = Callable[[Article], bool]

class Filters:
    @staticmethod
    def by_domain(allowed: Sequence[str]) -> Predicate:
        s = {d.lower() for d in allowed}
        return lambda a: (a.source or "").lower() in s

    @staticmethod
    # filters.py
    def by_date_range(start, end_excl):
        def _pred(a):
            dt = a.published_dt() if callable(getattr(a, "published_dt", None)) else None
            if dt is None:
                return False   # <<-- változtatás: ne engedjük át a dátum nélkülieket
            if start and dt < start: return False
            if end_excl and dt >= end_excl: return False
            return True
        return _pred


    @staticmethod
    def by_label(labels: Sequence[str]) -> Predicate:
        s = set(labels)
        return lambda a: (getattr(a, "label", None) in s)

    @staticmethod
    def compose(*preds: Predicate) -> Predicate:
        return lambda a: all(p(a) for p in preds)

    @staticmethod
    def by_url_section(allowed_by_domain: Mapping[str, Sequence[str]]) -> Predicate:
        """
        Csak azokat az Article-öket engedi át, ahol az URL első path-szegmense
        benne van az adott domainhez tartozó allowlistben.

        pl. https://telex.hu/belfold/2025/...  -> "belfold"
        """
        mapping = {
            d.lower(): {s.lower() for s in secs}
            for d, secs in allowed_by_domain.items()
        }

        def _pred(a: Article) -> bool:
            dom = (a.source or "").lower()
            allowed = mapping.get(dom)
            if not allowed:
                # ha nincs konfigurálva, nem szűrünk
                return True

            try:
                path = urlparse(a.link).path.strip("/")
            except Exception:
                return True

            if not path:
                return True

            first = path.split("/")[0].lower()
            return first in allowed

        return _pred
    
POLITICAL_SECTIONS = {
    "telex.hu": [
        "belfold", "kulfold", "gazdasag",
        "velemeny", "eu", "english",
    ],
    "index.hu": [
        "belfold", "kulfold", "gazdasag",
        "velemeny", "kozelet",
    ],
    "hvg.hu": [
        "itthon", "vilag", "gazdasag",
        "kozelet", "kkv",
    ]
}