
# fts_migrate.py
import sqlite3
from pathlib import Path

DB_PATH = "news.sqlite"

SCHEMA_FTS = """
CREATE VIRTUAL TABLE IF NOT EXISTS items_fts USING fts5(
    title, 
    content, 
    link UNINDEXED, 
    ts UNINDEXED, 
    item_id UNINDEXED,
    content='',
    tokenize = 'porter'
);
"""

TRIGGER_INSERT = """
CREATE TRIGGER IF NOT EXISTS items_ai AFTER INSERT ON items BEGIN
  INSERT INTO items_fts(rowid, title, content, link, ts, item_id)
  VALUES (new.rowid, new.title, new.content, new.link, new.ts, new.id);
END;
"""

TRIGGER_UPDATE = """
CREATE TRIGGER IF NOT EXISTS items_au AFTER UPDATE ON items BEGIN
  UPDATE items_fts SET 
    title = new.title,
    content = new.content,
    link = new.link,
    ts = new.ts,
    item_id = new.id
  WHERE rowid = new.rowid;
END;
"""

TRIGGER_DELETE = """
CREATE TRIGGER IF NOT EXISTS items_ad AFTER DELETE ON items BEGIN
  DELETE FROM items_fts WHERE rowid = old.rowid;
END;
"""

def rebuild(conn):
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS items_fts")
    conn.commit()
    cur.execute(SCHEMA_FTS)
    conn.commit()
    # bulk load existing
    cur.execute("""
        INSERT INTO items_fts(rowid, title, content, link, ts, item_id)
        SELECT rowid, title, content, link, ts, id FROM items
    """)
    conn.commit()

def ensure_triggers(conn):
    cur = conn.cursor()
    cur.execute(TRIGGER_INSERT)
    cur.execute(TRIGGER_UPDATE)
    cur.execute(TRIGGER_DELETE)
    conn.commit()

def main():
    p = Path(DB_PATH)
    if not p.exists():
        print(f"❌ Database not found: {DB_PATH}")
        return
    conn = sqlite3.connect(DB_PATH)
    try:
        rebuild(conn)
        ensure_triggers(conn)
        # stats
        n = conn.execute("SELECT count(*) FROM items_fts").fetchone()[0]
        print(f"✅ FTS index rebuilt with {n} rows. Triggers installed.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
