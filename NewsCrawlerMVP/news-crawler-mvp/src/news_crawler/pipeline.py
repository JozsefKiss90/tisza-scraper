from __future__ import annotations
from .adapters.factories import SourceAdapter   
from .repository import Repository
from typing import List, Optional, Callable
from .embedder import EmbedderClassifier
from .filters import Predicate
from .models import Article

OnItem = Callable[[Article, int], None]  # (art, count_so_far)

class Pipeline:
    """End-to-end process: crawl -> save -> (optional) embed/label -> done."""

    def __init__(self, adapters: List[SourceAdapter], repo: Repository, embedder: Optional[EmbedderClassifier] = None) -> None:
        self.adapters = adapters
        self.repo = repo
        self.embedder = embedder

    def collect(self, years: int = 10,
                date_from: Optional[str] = None,
                date_to: Optional[str] = None,
                predicate: Optional[Callable[[Article], bool]] = None,
                on_item: Optional[OnItem] = None) -> int:
        total = 0
        for ad in self.adapters:
            for art in ad.iter_archive(years=years, date_from=date_from, date_to=date_to):
                if predicate and not predicate(art):
                    continue
                self.repo.upsert(art)
                total += 1
                if on_item:
                    on_item(art, total)
        return total

    def postprocess(self) -> None:
        if self.embedder:
            self.embedder.run()