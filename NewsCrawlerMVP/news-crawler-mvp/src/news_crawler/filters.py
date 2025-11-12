# news_crawler/filters.py
from __future__ import annotations

from datetime import datetime
from typing import Callable, Optional, Sequence

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
