"""
NewsCrawlerMVP – egységesített osztályrendszer
------------------------------------------------
Cél: 444, Index, HVG, Telex források (és bővíthető) egységes kezelése,
keresés/szűrés optimalizálása, részfunkciók definiálása, testreszabhatóság.

Megjegyzés: ez egy MVP-minőségű architekturális váz + alap implementációk.
A projekt meglévő scriptjei (crawler-ek, RSS/DB/FTS/embedding) könnyen
plug-in jelleggel illeszthetők ide.
"""
from __future__ import annotations

import abc
import dataclasses
import json
import re
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Protocol, Sequence, Tuple

# ===========================
# --- Alap domain objektumok
# ===========================

@dataclass
class Article:
    id: str
    title: str
    link: str
    published: Optional[str] = None  # ISO string vagy None
    source: Optional[str] = None     # domain vagy feed URL
    content: Optional[str] = None
    matched_tags: List[str] = field(default_factory=list)
    ts: Optional[int] = None         # unix timestamp (DB-hez praktikus)
    label: Optional[str] = None      # "kormánypárti" | "ellenzéki" | "semleges"
    label_score: Optional[float] = None
    cluster_id: Optional[int] = None

    @property
    def published_dt(self) -> Optional[datetime]:
        try:
            return datetime.fromisoformat(self.published) if self.published else None
        except Exception:
            return None

# ===============================
# --- HTTP letöltés és normalizálás
# ===============================

class Fetcher:
    """Központi HTTP kliens (helyettesíthető/mokkolható)."""

    def __init__(self, user_agent: str = "NewsMVP/1.0") -> None:
        self.user_agent = user_agent

    def get_text(self, url: str, timeout: int = 20) -> Optional[str]:
        import httpx  # lazy import
        try:
            r = httpx.get(url, headers={"User-Agent": self.user_agent}, timeout=timeout, follow_redirects=True)
            if r.status_code >= 400:
                return None
            ctype = (r.headers.get("content-type") or "").lower()
            if "html" not in ctype and "xml" not in ctype:
                return None
            return r.text
        except Exception:
            return None

# ==============================
# --- Forrás adapterek (parszolók)
# ==============================

class SourceAdapter(abc.ABC):
    """Absztrakt forrásadapter: egységes interfész minden site-hoz.

    Egy adapter tetszőleges stratégiát használhat (archívum lista, YM/YMD,
    sitemap, RSS), a kimenet mindig Article-objektumok iterálható sorozata.
    """

    domain: str

    def __init__(self, domain: str, fetcher: Optional[Fetcher] = None) -> None:
        self.domain = domain
        self.fetcher = fetcher or Fetcher()

    @abc.abstractmethod
    def iter_archive(self, years: int = 10, *, date_from: Optional[str] = None, date_to: Optional[str] = None) -> Iterator[Article]:
        """Cikkek iterálása archívum-stratégiával (MVP: link + pubdate_guess alapú)."""

    @abc.abstractmethod
    def name(self) -> str:
        """Emberi olvasású név a beépített adapterhez."""

# --- Konkrét adapter vázak (a regexeket/sablonokat projektkonfigból töltsd) ---

class RegexArchiveAdapter(SourceAdapter):
    """Általános adapter regex-alapú URL-kinyeréssel (YM/YMD/archivum oldalakhoz)."""

    def __init__(self, domain: str, article_regex: str, page_templates: Dict[str, str], fetcher: Optional[Fetcher] = None) -> None:
        super().__init__(domain, fetcher)
        self._article_re = re.compile(article_regex, re.IGNORECASE)
        self._pages = page_templates  # pl. {"archivum":"...{PAGE}", "ym":"...{YYYY}/{MM}", "ymd":"...{YYYY}/{MM}/{DD}"}

    def name(self) -> str:
        return f"RegexArchiveAdapter<{self.domain}>"

    def _extract(self, html: str) -> List[Tuple[str, Optional[str]]]:
        out: List[Tuple[str, Optional[str]]] = []
        for m in self._article_re.finditer(html or ""):
            url = m.group(0)
            pub = None
            try:
                y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
                pub = f"{y:04d}-{mo:02d}-{d:02d}"
            except Exception:
                pass
            out.append((url, pub))
        return out

    def _iter_pages(self, mode: str, years: int, date_from: Optional[str], date_to: Optional[str]) -> Iterator[str]:
        tmpl = self._pages.get("archivum")
        if not tmpl:
            return iter(())
        page = 1
        for _ in range(200):  # hard cap az MVP-ben
            yield tmpl.format(PAGE=page)
            page += 1

    def iter_archive(self, years: int = 10, *, date_from: Optional[str] = None, date_to: Optional[str] = None) -> Iterator[Article]:
        seen: set[str] = set()
        for page_url in self._iter_pages("archivum", years, date_from, date_to):
            html = self.fetcher.get_text(page_url)
            if not html:
                continue
            for url, pub in self._extract(html):
                if url in seen:
                    continue
                seen.add(url)
                yield Article(
                    id=f"sha256:{hash((url, pub))}",
                    title="",  # később tölthető trafilatura-val
                    link=url,
                    published=pub,
                    source=self.domain,
                    ts=int(time.time()),
                )

# ========================== 
# --- Tároló réteg (SQLite)
# ==========================

class Repository:
    """SQLite-alapú tároló az egységes Article-objektumokhoz.
    Feltételezi az "items" táblát/FTS-t (migráció kezelhető külön)."""

    def __init__(self, db_path: str = "news.sqlite") -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS items(
                id TEXT PRIMARY KEY,
                title TEXT,
                link TEXT,
                published TEXT,
                source TEXT,
                content TEXT,
                matched_tags TEXT,
                ts INTEGER,
                label TEXT,
                label_score REAL,
                cluster_id INTEGER
            )
            """
        )
        self.conn.commit()

    def upsert(self, art: Article) -> None:
        cur = self.conn.cursor()
        try:
            cur.execute(
                "INSERT INTO items(id,title,link,published,source,content,matched_tags,ts,label,label_score,cluster_id)\n                 VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    art.id, art.title, art.link, art.published, art.source,
                    art.content or "", ",".join(art.matched_tags),
                    art.ts or int(time.time()), art.label, art.label_score, art.cluster_id,
                ),
            )
        except sqlite3.IntegrityError:
            cur.execute(
                "UPDATE items SET title=?, published=?, source=?, content=?, matched_tags=?, ts=?, label=?, label_score=?, cluster_id=? WHERE id=?",
                (
                    art.title, art.published, art.source, art.content or "",
                    ",".join(art.matched_tags), art.ts or int(time.time()),
                    art.label, art.label_score, art.cluster_id, art.id,
                ),
            )
        self.conn.commit()

    def bulk_upsert(self, arts: Iterable[Article]) -> int:
        n = 0
        for a in arts:
            self.upsert(a); n += 1
        return n

# ==============================
# --- Keresőmotor (rétegelt)
# ==============================

@dataclass
class Query:
    text: str
    years: int = 10
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    label: Optional[str] = None
    limit: int = 200
    order: str = "bm25"  # "bm25" | "time"

class SearchEngine:
    def __init__(self, repo: Repository) -> None:
        self.repo = repo

    def search(self, q: Query) -> List[Dict[str, Any]]:
        rows = self.repo.search_fts(q.text, limit=q.limit, order=q.order)
        if q.label:
            rows = [r for r in rows if r.get("label") == q.label]
        return rows

# ==============================
# --- Orchestrator / Pipeline
# ==============================

class Pipeline:
    """End-to-end folyamat: crawl -> mentés -> (opcionális) embed/label -> kész."""

    def __init__(self, adapters: List[SourceAdapter], repo: Repository) -> None:
        self.adapters = adapters
        self.repo = repo

    def collect(self, years: int = 10, date_from: Optional[str] = None, date_to: Optional[str] = None) -> int:
        total = 0
        for ad in self.adapters:
            for art in ad.iter_archive(years=years, date_from=date_from, date_to=date_to):
                self.repo.upsert(art)
                total += 1
        return total
"""