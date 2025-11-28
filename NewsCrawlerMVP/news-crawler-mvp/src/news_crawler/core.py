from .adapters.factories import make_telex_adapter, make_index_adapter, make_444_adapter, make_hvg_adapter
from .repository import Repository
from .search import SearchEngine
from .pipeline import Pipeline
from .embedder import EmbedderClassifier
from .fetcher import Fetcher
from typing import Optional, List, Dict, Any

class NewsCrawlerMVP:
    def __init__(self, db_path: str = "news.sqlite") -> None:
        self.repo = Repository(db_path)
        self.fetcher = Fetcher()
        self.adapters = [
            make_telex_adapter(self.fetcher),
            make_index_adapter(self.fetcher),
            make_444_adapter(self.fetcher), 
            make_hvg_adapter(self.fetcher),
        ]
        self.embedder = EmbedderClassifier(self.repo)
        self.pipeline = Pipeline(self.adapters, self.repo, self.embedder)
        self.search_engine = SearchEngine(self.repo)

    def crawl_all(
        self,
        years: int = 10,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> int:
        return self.pipeline.collect(years=years, date_from=date_from, date_to=date_to)

    def crawl_domain_range(
        self,
        domain: Optional[str] = None,
        *,
        years: int = 10,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        verbose: bool = False,
    ) -> int:
        """
        Csak a megadott domain(eke)t járja be az adott dátum intervallumban.

        - domain=None → minden adapter
        - domain='telex.hu' → csak a Telex adapter
        """
        if domain:
            adapters = [ad for ad in self.adapters if getattr(ad, "domain", None) == domain]
        else:
            adapters = self.adapters

        if not adapters:
            print(f"[TISZA] crawl_domain_range: nincs adapter a domainhez: {domain}")
            return 0

        # Új, átmeneti Pipeline, hogy ne piszkáljuk a self.pipeline.adapters-t
        pipe = Pipeline(adapters, self.repo, self.embedder)

        def _log(art, n):
            if verbose:
                print(f"[{n:05d}] {art.source or '—'}  pub={art.published or '—'}  {art.link}")

        inserted = pipe.collect(
            years=years,
            date_from=date_from,
            date_to=date_to,
            predicate=None,
            on_item=_log if verbose else None,
        )
        return inserted

    def search(self, text: str, *, label: Optional[str] = None, limit: int = 200, order: str = "bm25") -> List[Dict[str, Any]]:
        return self.search_engine.search(text=text, label=label, limit=limit, order=order)