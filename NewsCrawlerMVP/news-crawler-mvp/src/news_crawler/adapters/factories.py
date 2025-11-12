# news_crawler/adapters/factories.py
from __future__ import annotations
from typing import Optional
from .regex_archive_adapter import RegexArchiveAdapter, SourceAdapter
from ..fetcher import Fetcher

def make_telex_adapter(fetcher: Optional[Fetcher] = None) -> SourceAdapter:
    return RegexArchiveAdapter(
        domain="telex.hu",
        article_regex=r"https?://telex\.hu/(?:[a-z0-9\-_]+/)?(20\d{2})/([01]\d)/([0-3]\d)/[a-z0-9\-\._/]+",
        page_templates={"archivum": "https://telex.hu/archivum?page={PAGE}"},
        fetcher=fetcher,
    )

def make_index_adapter(fetcher: Optional[Fetcher] = None) -> SourceAdapter:
    return RegexArchiveAdapter(
        domain="index.hu",
        article_regex=r"https?://index\.hu/(?:[a-z0-9\-_]+/)?(20\d{2})/([01]\d)/([0-3]\d)/[a-z0-9\-\._/]+",
        page_templates={"archivum": "https://index.hu/archivum/?p={PAGE}"},
        fetcher=fetcher,
    )

def make_444_adapter(fetcher: Optional[Fetcher] = None) -> SourceAdapter:
    return RegexArchiveAdapter(
        domain="444.hu",
        article_regex=r"https?://444\.hu/(20\d{2})/([01]\d)/([0-3]\d)/[a-z0-9\-\._%/]+",
        page_templates={
            "archivum": "https://444.hu/archivum?page={PAGE}",
            "ym":       "https://444.hu/{YYYY}/{MM}",
            "ym_page":  "https://444.hu/{YYYY}/{MM}?page={PAGE}",        # ÚJ
            "ymd":      "https://444.hu/{YYYY}/{MM}/{DD}",
            "ymd_page": "https://444.hu/{YYYY}/{MM}/{DD}?page={PAGE}",   # ÚJ
        },
        fetcher=fetcher,
    )

def make_hvg_adapter(fetcher: Optional[Fetcher] = None) -> SourceAdapter:
    # Lásd a fenti alternációs mintát; ha a YAML-od már bevált, használd azt!
    return RegexArchiveAdapter(
        domain="hvg.hu",
        article_regex=(
            r"https?://hvg\.hu/(?:[a-z0-9\-_]+/)?("
            r"(20\d{2})/([01]\d)/([0-3]\d)/[a-z0-9\-\._/]+"
            r"|[a-z0-9\-_]*?([12]\d{3})[_\-\.]([01]\d)[_\-\.]([0-3]\d)[a-z0-9\-\._/]*"
            r")"
        ),
        page_templates={"archivum": "https://hvg.hu/cimke/arch%C3%ADvum?p={PAGE}"},
        fetcher=fetcher,
    )
