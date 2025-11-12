# news_crawler/embedder.py
from __future__ import annotations

import subprocess
import sqlite3
from typing import Optional

from .repository import Repository

class EmbedderClassifier:
    """
    Glue layer to your existing embedding + classification pipeline.

    Options:
      - call your script (e.g., embed_classify_summarize.py) as a subprocess
      - or run a minimal SQL-only 'fallback' labeling for MVP/demo

    Usage:
        EmbedderClassifier(repo).run(
            script_path="embed_classify_summarize.py",
            python_exec="python",
            dry_run=False
        )
    """

    def __init__(self, repo: Repository) -> None:
        self.repo = repo

    def run(
        self,
        *,
        script_path: Optional[str] = None,
        python_exec: str = "python",
        dry_run: bool = False,
    ) -> None:
        # If you have a full pipeline script already, prefer calling it:
        if script_path and not dry_run:
            subprocess.run([python_exec, script_path], check=False)
            return

        # --- Minimal in-DB fallback (keeps the interface working) ---
        # Example: naive label from simple keyword heuristics
        conn: sqlite3.Connection = self.repo.conn
        conn.executescript(
            """
            -- Add columns if missing (idempotent-ish)
            PRAGMA foreign_keys=off;
            CREATE TABLE IF NOT EXISTS items_tmp AS SELECT * FROM items WHERE 0;
            DROP TABLE IF EXISTS items_tmp;

            -- Heuristic labels (replace with your actual logic)
            UPDATE items
            SET label = CASE
                WHEN title LIKE '%kormány%' OR content LIKE '%kormány%' THEN 'kormánypárti'
                WHEN title LIKE '%ellenzék%' OR content LIKE '%ellenzék%' THEN 'ellenzéki'
                ELSE 'semleges'
            END
            WHERE label IS NULL;
            """
        )
        conn.commit()
