from __future__ import annotations

import sqlite3
import time
from typing import Iterable, List, Dict, Any

from .models import Article

class Repository:
    """SQLite-alapú tároló az egységes Article-objektumokhoz.
    Feltételezi az \"items\" táblát/FTS-t (migráció kezelhető külön)."""

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

    def search_fts(self, query: str, *, limit: int = 100, order: str = "bm25") -> List[Dict[str, Any]]:
        # FTS5 integráció feltételezett; ha nincs, sima LIKE fallback
        try:
            sql = (
                "SELECT title, link, label, label_score, datetime(ts,'unixepoch') as date, bm25(items_fts) as rank, cluster_id, substr(content,1,400) as snippet\n"
                "FROM items i JOIN items_fts ON items_fts.rowid = i.rowid\n"
                "WHERE items_fts MATCH ? ORDER BY " + ("rank" if order=="bm25" else "i.ts DESC") + " LIMIT ?"
            )
            rows = self.conn.execute(sql, [query, limit]).fetchall()
        except sqlite3.OperationalError:
            rows = self.conn.execute(
                "SELECT title, link, label, label_score, datetime(ts,'unixepoch') as date, NULL as rank, cluster_id, substr(content,1,400) as snippet\n"
                "FROM items WHERE title LIKE ? OR content LIKE ? ORDER BY ts DESC LIMIT ?",
                [f"%{query}%", f"%{query}%", limit],
            ).fetchall()
        cols = ["title","link","label","label_score","date","rank","cluster_id","snippet"]
        return [dict(zip(cols, r)) for r in rows]