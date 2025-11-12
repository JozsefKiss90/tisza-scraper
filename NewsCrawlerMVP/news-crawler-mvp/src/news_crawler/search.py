from typing import List, Dict, Any
from .repository import Repository
from .adapters.factories import make_telex_adapter, make_index_adapter, make_444_adapter, make_hvg_adapter
from typing import List, Dict, Any, Tuple, Optional
from .adapters.regex_archive_adapter import SourceAdapter

class SearchEngine:
    def __init__(self, repo: Repository) -> None:
        self.repo = repo

    def search(self, text: str, *, label: Optional[str] = None, limit: int = 200, order: str = "bm25") -> List[Dict[str, Any]]:
        rows = self.search_engine.search(text, limit=limit, order=order)
        if label:
            rows = [r for r in rows if r.get("label") == label]
        return rows


def initialize_components(db_path: str = "news.sqlite") -> Tuple[Repository, List[SourceAdapter], SearchEngine]:
    repo = Repository(db_path)
    adapters = [
        make_telex_adapter(),
        make_index_adapter(),
        make_444_adapter(),
        make_hvg_adapter(),
    ]
    search_engine = SearchEngine(repo)
    return repo, adapters, search_engine