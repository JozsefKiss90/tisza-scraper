# news_crawler/adapters/factories.py
from __future__ import annotations
from typing import Optional
from .regex_archive_adapter import RegexArchiveAdapter, SourceAdapter
from ..fetcher import Fetcher

def make_telex_adapter(fetcher: Optional[Fetcher] = None) -> SourceAdapter:
    return RegexArchiveAdapter(
        domain="telex.hu",
        article_regex=r"https?://telex\.hu/(?:[a-z0-9\-]+/)?(20\d{2})/([01]\d)/([0-3]\d)/[^\"'<>\s]+",
        page_templates={"archivum": "https://telex.hu/archivum?oldal={PAGE}"},
        relative_article_regex=r'href=["\']/((?:[a-z0-9\-]+/)?(20\d{2})/([01]\d)/([0-3]\d)/[^"\'<>]+)["\']',
        base_url="https://telex.hu",
        fetcher=fetcher,
    )

def make_index_adapter(fetcher: Optional[Fetcher] = None) -> SourceAdapter:
    # Klasszikus index.hu cikk-URL szerkezet: /YYYY/MM/DD/slug
    return RegexArchiveAdapter(
        domain="index.hu",
        article_regex=r"https?://index\.hu/(?:[a-z0-9\-_]+/)?(20\d{2})/([01]\d)/([0-3]\d)/[^\"'<> \t]+",
        page_templates={"archivum": "https://index.hu/24ora/?p={PAGE}"},
        relative_article_regex=r'href=["\']/((?:[a-z0-9\-_]+/)?(20\d{2})/([01]\d)/([0-3]\d)/[^"\'<>]+)["\']',
        base_url="https://index.hu",
        fetcher=fetcher,
    )

def make_444_adapter(fetcher: Optional[Fetcher] = None) -> SourceAdapter:
    # 444: /YYYY/MM/DD/slug (+ YM/YMD fallbackok, lapozós variánsokkal)
    return RegexArchiveAdapter(
        domain="444.hu",
        article_regex=r"https?://444\.hu/(20\d{2})/([01]\d)/([0-3]\d)/[^\"'<>% \t]+",
        page_templates={
            "archivum": "https://444.hu/archivum?page={PAGE}",
            "ym":       "https://444.hu/{YYYY}/{MM}",
            "ym_page":  "https://444.hu/{YYYY}/{MM}?page={PAGE}",
            "ymd":      "https://444.hu/{YYYY}/{MM}/{DD}",
            "ymd_page": "https://444.hu/{YYYY}/{MM}/{DD}?page={PAGE}",
        },
        relative_article_regex=r'href=["\']/((20\d{2})/([01]\d)/([0-3]\d)/[^"\'<>%]+)["\']',
        base_url="https://444.hu",
        fetcher=fetcher,
    )

def make_hvg_adapter(fetcher: Optional[Fetcher] = None) -> SourceAdapter:
    # HVG: /<rovat>/<YYYYMMDD>_<slug>  — Friss hírek lista: /frisshirek, /frisshirek/2, /frisshirek/3, ...
    return RegexArchiveAdapter(
        domain="hvg.hu",
        article_regex=r"https?://hvg\.hu/(?:[a-z0-9\-]+/)+(20\d{2})([01]\d)([0-3]\d)_[^\"'<> \t]+",
        page_templates={"archivum": "https://hvg.hu/frisshirek/{PAGE}"},
        relative_article_regex=r'href=["\']/((?:[a-z0-9\-]+/)+(20\d{2})([01]\d)([0-3]\d)_[^"\'<>]+)["\']',
        base_url="https://hvg.hu",
        fetcher=fetcher,
    )