from __future__ import annotations

from typing import List, Dict, Any, Optional, Tuple

from .repository import Repository
from .adapters.regex_archive_adapter import SourceAdapter
from .adapters.factories import make_telex_adapter, make_index_adapter, make_444_adapter, make_hvg_adapter


class SearchEngine:
    """
    Vékony wrapper a Repository.search() fölött, opcionális label-szűréssel.
    """

    def __init__(self, repo: Repository) -> None:
        self.repo = repo

    def search(
        self,
        text: str,
        *,
        label: Optional[str] = None,
        limit: int = 200,
        order: str = "bm25",
    ) -> List[Dict[str, Any]]:
        rows = self.repo.search(text, limit=limit, order=order)
        if label is not None:
            rows = [r for r in rows if r.get("label") == label]
        return rows


def initialize_components(db_path: str = "news.sqlite") -> Tuple[Repository, List[SourceAdapter], SearchEngine]:
    """
    Kényelmi helper, ha kézzel akarod összedrótozni a komponenseket.
    scrape_archive.py közvetlenül nem használja, csak REPL/teszteléshez hasznos.
    """
    repo = Repository(db_path)
    adapters: List[SourceAdapter] = [
        make_telex_adapter(),
        make_index_adapter(),
        make_444_adapter(),
        make_hvg_adapter(),
    ]
    search_engine = SearchEngine(repo)
    return repo, adapters, search_engine
