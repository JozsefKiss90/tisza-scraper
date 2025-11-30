from __future__ import annotations

import hashlib
import sqlite3
import time
from typing import Iterable, List, Dict, Any, Optional
from urllib.parse import urlparse
import unicodedata  # a file tetején már legyen importálva 
from .models import Article
from .article_reader import read_article
from .fetcher import Fetcher


class Repository:
    """
    SQLite-backed storage for unified Article objects.

    Uses the normalized schema:

      - sources  (domain-level metadata)
      - articles (all articles, any domain)
      - article_fts (FTS5 full-text index, if available)

    On first use it will create / migrate the DB in-place.
    """

    def __init__(self, db_path: str = "news.sqlite") -> None:
        self.db_path = db_path
        # FONTOS: check_same_thread=False, hogy FastAPI alatt több szálról is használható legyen
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        # Ensure foreign keys
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self._init_schema()

    # ------------------------------------------------------------------
    # Schema setup
    # ------------------------------------------------------------------
    def _init_schema(self) -> None:
        cur = self.conn.cursor()

        # 1) Core tables
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS sources (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                domain      TEXT NOT NULL UNIQUE,
                name        TEXT,
                base_url    TEXT,
                timezone    TEXT,
                is_active   INTEGER NOT NULL DEFAULT 1,
                created_at  INTEGER NOT NULL,
                updated_at  INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS articles (
                id              TEXT PRIMARY KEY,
                source_id       INTEGER NOT NULL,
                url             TEXT NOT NULL UNIQUE,
                title           TEXT,
                content         TEXT,
                summary         TEXT,
                published_date  TEXT,
                path_year       INTEGER,
                path_month      INTEGER,
                path_day        INTEGER,
                section         TEXT,
                tags            TEXT,
                matched_tags    TEXT,
                author          TEXT,
                label           TEXT,
                label_score     REAL,
                cluster_id      INTEGER,
                created_at      INTEGER NOT NULL,
                updated_at      INTEGER NOT NULL,
                FOREIGN KEY (source_id) REFERENCES sources(id)
            );

            CREATE INDEX IF NOT EXISTS idx_articles_source_published
                ON articles (source_id, published_date DESC);

            CREATE INDEX IF NOT EXISTS idx_articles_published
                ON articles (published_date DESC);

            CREATE INDEX IF NOT EXISTS idx_articles_cluster
                ON articles (cluster_id);
            """
        )

        # 2) FTS index (ha van FTS5)
        '''try:
            cur.executescript(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS article_fts
                USING fts5(
                    title,
                    content,
                    summary,
                    tags,
                    url,
                    content='articles',
                    content_rowid='rowid'
                );

                CREATE TRIGGER IF NOT EXISTS article_ai
                AFTER INSERT ON articles
                BEGIN
                    INSERT INTO article_fts(rowid, title, content, summary, tags, url)
                    VALUES (new.rowid, new.title, new.content, new.summary, new.tags, new.url);
                END;

                CREATE TRIGGER IF NOT EXISTS article_au
                AFTER UPDATE ON articles
                BEGIN
                    UPDATE article_fts
                    SET title   = new.title,
                        content = new.content,
                        summary = new.summary,
                        tags    = new.tags,
                        url     = new.url
                    WHERE rowid = new.rowid;
                END;

                CREATE TRIGGER IF NOT EXISTS article_ad
                AFTER DELETE ON articles
                BEGIN
                    DELETE FROM article_fts WHERE rowid = old.rowid;
                END;
                """
            )
        except sqlite3.OperationalError:
            # Nincs FTS5 → search() LIKE-ra fog visszaesni
            pass
        '''
        # 3) Alapértelmezett források (idempotens)
        self._ensure_default_sources(cur)

        # 4) Kompatibilitási VIEW: "items" (csak ha nincs már ilyen TABLE)
        exists = cur.execute(
            "SELECT name, type FROM sqlite_master WHERE name='items'"
        ).fetchone()
        if not exists:
            cur.executescript(
                """
                CREATE VIEW IF NOT EXISTS items AS
                SELECT
                    a.id             AS id,
                    a.title          AS title,
                    a.url            AS link,
                    a.published_date AS published,
                    s.domain         AS source,
                    a.content        AS content,
                    a.matched_tags   AS matched_tags,
                    a.updated_at     AS ts,
                    a.label          AS label,
                    a.label_score    AS label_score,
                    a.cluster_id     AS cluster_id
                FROM articles a
                JOIN sources s ON s.id = a.source_id;
                """
            )

        self.conn.commit()

    def _ensure_default_sources(self, cur: sqlite3.Cursor) -> None:
        now = int(time.time())
        defaults = [
            ("telex.hu", "Telex", "https://telex.hu", "Europe/Budapest"),
            ("index.hu", "Index", "https://index.hu", "Europe/Budapest"),
            ("444.hu", "444", "https://444.hu", "Europe/Budapest"),
            ("hvg.hu", "HVG", "https://hvg.hu", "Europe/Budapest"),
        ]
        for domain, name, base_url, tz in defaults:
            cur.execute(
                """
                INSERT OR IGNORE INTO sources (domain, name, base_url, timezone, is_active, created_at, updated_at)
                VALUES (?, ?, ?, ?, 1, ?, ?)
                """,
                (domain, name, base_url, tz, now, now),
            )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _get_or_create_source_id(self, domain: str) -> int:
        """Return the sources.id for a domain, creating it if needed."""
        domain = (domain or "").lower()
        cur = self.conn.cursor()
        row = cur.execute(
            "SELECT id FROM sources WHERE domain = ?",
            (domain,),
        ).fetchone()
        if row:
            return int(row["id"])
        now = int(time.time())
        cur.execute(
            """
            INSERT INTO sources (domain, name, base_url, timezone, is_active, created_at, updated_at)
            VALUES (?, ?, NULL, NULL, 1, ?, ?)
            """,
            (domain, domain, now, now),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def get_article_row_by_url(self, url: str) -> Optional[sqlite3.Row]:
        """Nyers DB-sor visszaadása URL alapján (ha létezik)."""
        cur = self.conn.cursor()
        row = cur.execute(
            "SELECT * FROM articles WHERE url = ?",
            (url,),
        ).fetchone()
        return row

    def row_to_article(self, row: sqlite3.Row) -> Article:
        """sqlite3.Row -> Article dataclass."""
        tags_raw = row["matched_tags"] or ""
        matched_tags = [t for t in tags_raw.split(",") if t]

        return Article(
            id=row["id"],
            title=row["title"] or "",
            link=row["url"],
            published=row["published_date"],
            source=urlparse(row["url"]).netloc if row["url"] else None,
            content=row["content"],
            matched_tags=matched_tags,
            ts=row["updated_at"],
            label=row["label"],
            label_score=row["label_score"],
            cluster_id=row["cluster_id"],
        )

    def update_article_content_by_url(
        self,
        url: str,
        title: Optional[str],
        content: Optional[str],
    ) -> None:
        """Title/content frissítése URL alapján."""
        now = int(time.time())
        self.conn.execute(
            """
            UPDATE articles
            SET title   = COALESCE(?, title),
                content = COALESCE(?, content),
                updated_at = ?
            WHERE url = ?
            """,
            (title, content, now, url),
        )
        self.conn.commit()

    def get_or_fetch_article(self, url: str, fetcher: Optional[Fetcher] = None) -> Article:
        """
        Magas szintű API: URL -> Article.

        - Ha az URL már szerepel az adatbázisban ÉS van content,
          akkor csak visszaadjuk az Article-t.
        - Ha nincs, akkor read_article()-lel letöltjük/parszoljuk,
          elmentjük, és úgy adjuk vissza.
        """
        row = self.get_article_row_by_url(url)
        if row is not None and row["content"]:
            return self.row_to_article(row)

        # Nincs (hasznos) content -> le kell húzni
        article_fetcher = fetcher or Fetcher()
        title, body = read_article(url, fetcher=article_fetcher)

        # Ha teljesen új az URL
        if row is None:
            stable_id = hashlib.sha256(url.encode("utf-8")).hexdigest()
            now = int(time.time())
            art = Article(
                id=stable_id,
                title=title or "",
                link=url,
                published=None,
                source=urlparse(url).netloc,
                content=body or "",
                matched_tags=[],
                ts=now,
                label=None,
                label_score=None,
                cluster_id=None,
            )
            self.upsert(art)
            return art

        # Ha volt sor, de content nélkül -> frissítés
        self.update_article_content_by_url(url, title, body)
        updated = self.get_article_row_by_url(url)
        assert updated is not None
        return self.row_to_article(updated)

    # ------------------------------------------------------------------
    # Write API
    # ------------------------------------------------------------------
    def upsert(self, art: Article) -> None:
        """
        Insert or update a single Article into the normalized schema.
        Keeps the SHA-256 based Article.id stable across runs.
        """
        cur = self.conn.cursor()
        # Domain meghatározása: adapterek beállítják a .source-ot, de azért fallback is van.
        domain = (art.source or urlparse(art.link).netloc).lower()
        source_id = self._get_or_create_source_id(domain)
        now = int(time.time())
        published_date = art.published or None
        matched_tags = ",".join(art.matched_tags) if art.matched_tags else None

        cur.execute(
            """
            INSERT INTO articles (
                id, source_id, url,
                title, content, summary,
                published_date,
                path_year, path_month, path_day,
                section, tags, matched_tags,
                author,
                label, label_score, cluster_id,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, ?, NULL, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title          = COALESCE(excluded.title, articles.title),
                content        = COALESCE(excluded.content, articles.content),
                summary        = COALESCE(excluded.summary, articles.summary),
                published_date = COALESCE(excluded.published_date, articles.published_date),
                matched_tags   = COALESCE(excluded.matched_tags, articles.matched_tags),
                label          = COALESCE(excluded.label, articles.label),
                label_score    = COALESCE(excluded.label_score, articles.label_score),
                cluster_id     = COALESCE(excluded.cluster_id, articles.cluster_id),
                updated_at     = excluded.updated_at
            """,
            (
                art.id,
                source_id,
                art.link,
                art.title,
                art.content,
                None,  # summary – majd az embedder/összefoglaló pipeline tölti ki
                published_date,
                matched_tags,
                art.label,
                art.label_score,
                art.cluster_id,
                now if art.ts is None else art.ts,
                now,
            ),
        )
        self.conn.commit()

    def upsert_many(self, articles: Iterable[Article]) -> int:
        """Batch upsert – returns number of processed records."""
        count = 0
        for art in articles:
            self.upsert(art)
            count += 1
        return count

    # ------------------------------------------------------------------
    # Read / search API
    # ------------------------------------------------------------------
    def search(self, query: str, *, limit: int = 200, order: str = "bm25") -> List[Dict[str, Any]]:
        """
        Full-text search over articles. Tries FTS5 first, falls back to
        a simple LIKE search if FTS is unavailable.

        Returns a list of dicts with keys:
          - title, link, label, label_score, date, rank, cluster_id, snippet
        """
        cur = self.conn.cursor()
        rows: List[sqlite3.Row]

        # Próbáljuk FTS-sel
        try:
            sql = (
                "SELECT a.title, a.url AS link, a.label, a.label_score, "
                "COALESCE(a.published_date, datetime(a.created_at, 'unixepoch')) AS date, "
                "bm25(article_fts) AS rank, "
                "a.cluster_id, "
                "substr(a.content, 1, 400) AS snippet "
                "FROM articles a "
                "JOIN article_fts ON article_fts.rowid = a.rowid "
                "WHERE article_fts MATCH ? "
                "ORDER BY "
                + ("rank" if order == 'bm25' else "a.created_at DESC")
                + " LIMIT ?"
            )
            rows = cur.execute(sql, (query, limit)).fetchall()
        except sqlite3.OperationalError:
            # Nincs FTS → LIKE fallback
            like = f"%{query}%"
            sql = (
                "SELECT a.title, a.url AS link, a.label, a.label_score, "
                "COALESCE(a.published_date, datetime(a.created_at, 'unixepoch')) AS date, "
                "NULL AS rank, "
                "a.cluster_id, "
                "substr(a.content, 1, 400) AS snippet "
                "FROM articles a "
                "WHERE a.title LIKE ? OR a.content LIKE ? "
                "ORDER BY a.created_at DESC "
                "LIMIT ?"
            )
            rows = cur.execute(sql, (like, like, limit)).fetchall()

        cols = ["title", "link", "label", "label_score", "date", "rank", "cluster_id", "snippet"]
        result: List[Dict[str, Any]] = []
        for row in rows:
            result.append({c: row[c] for c in cols})
        return result
    
    def search_by_meta(
        self,
        *,
        domain: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        q: Optional[str] = None,
        topic: Optional[str] = None,  
        entity: Optional[str] = None,     # ÚJ
        keyword: Optional[str] = None,    # ÚJ
        limit: int = 200,
    ):
        """
        Egyszerű meta-alapú keresés:

          - opcionális domain (sources.domain)
          - opcionális dátum intervallum (articles.published_date, 'YYYY-MM-DD')
          - opcionális kulcsszó: title/content LIKE
        """
        cur = self.conn.cursor()

        sql = (
            "SELECT a.title, a.url AS link, "
            "COALESCE(a.published_date, datetime(a.created_at, 'unixepoch')) AS date, "
            "a.label, a.label_score, a.cluster_id, "
            "substr(a.content, 1, 400) AS snippet "
            "FROM articles a "
            "JOIN sources s ON s.id = a.source_id "
        )

        where = []
        params: List[Any] = []

        if domain:
            where.append("s.domain = ?")
            params.append(domain)

        if date_from:
            where.append("a.published_date >= ?")
            params.append(date_from)

        if date_to:
            where.append("a.published_date <= ?")
            params.append(date_to)

        if topic:
            where.append("a.tags LIKE ?")
            params.append(f"%{topic}%")
        if entity:
            # JSON-ben így szerepel: "text": "Orbán Viktor"
            where.append("a.matched_tags LIKE ?")
            params.append(f'%\"text\": \"{entity}\"%')

        if keyword:
            where.append("a.tags LIKE ?")
            params.append(f"%{keyword}%")

        import unicodedata  # a file tetején már legyen importálva

        if q:
            # 1) Eredeti keresőkifejezés (title/content-hez jó lehet)
            like_raw = f"%{q}%"

            # 2) Ékezetek leszedése + lower → jobban hasonlít a slugokra
            q_norm = unicodedata.normalize("NFKD", q)
            q_ascii = "".join(c for c in q_norm if not unicodedata.combining(c))
            q_ascii = q_ascii.lower()

            # 3) Tipikus slug: szavak kötőjellel
            slug_like = "%" + "-".join(q_ascii.split()) + "%"

            where.append(
                "("
                "a.title   LIKE ? OR "
                "a.content LIKE ? OR "
                "LOWER(a.url) LIKE ? OR "
                "LOWER(a.url) LIKE ?"
                ")"
            )
            params.extend([like_raw, like_raw, f"%{q_ascii}%", slug_like])

        if where:
            sql += " WHERE " + " AND ".join(where)

        sql += " ORDER BY a.published_date DESC, a.created_at DESC LIMIT ?"
        params.append(limit)

        rows = cur.execute(sql, params).fetchall()

        result: List[Dict[str, Any]] = []
        for row in rows:
            result.append(
                {
                    "title": row["title"] or "",
                    "link": row["link"] or "",
                    "date": row["date"],
                    "label": row["label"],
                    "label_score": row["label_score"],
                    "cluster_id": row["cluster_id"],
                    "snippet": row["snippet"] or "",
                    "rank": None,  # itt nincs bm25
                }
            )
        return result

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass
