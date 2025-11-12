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

    def crawl_all(self, years: int = 10, date_from: Optional[str] = None, date_to: Optional[str] = None) -> int:
        return self.pipeline.collect(years=years, date_from=date_from, date_to=date_to)

    def search(self, text: str, *, label: Optional[str] = None, limit: int = 200, order: str = "bm25") -> List[Dict[str, Any]]:
        return self.search_engine.search(text=text, label=label, limit=limit, order=order)