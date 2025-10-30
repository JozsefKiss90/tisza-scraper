import sqlite3

conn = sqlite3.connect("news.sqlite")
cur = conn.cursor()

for stmt in [
    "ALTER TABLE items ADD COLUMN views INTEGER",
    "ALTER TABLE items ADD COLUMN comments INTEGER",
    "ALTER TABLE items ADD COLUMN likes INTEGER",
    "ALTER TABLE items ADD COLUMN popularity_score REAL",
    "ALTER TABLE items ADD COLUMN last_popcheck_ts INTEGER"
]:
    try:
        cur.execute(stmt)
    except sqlite3.OperationalError:
        pass  # oszlop már létezik

conn.commit()
conn.close()

print("✅ Adatbázis frissítve: új oszlopok létrehozva (ha még nem voltak).")
