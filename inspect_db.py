import sqlite3
from textwrap import shorten

DB_PATH = "news.sqlite"   # ha máshol van, add meg az elérési útját
LIMIT = 10                # hány cikket listázzon

def main():
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        rows = cur.execute(
            "SELECT title, link, content, matched_tags, datetime(ts, 'unixepoch') "
            "FROM items ORDER BY ts DESC LIMIT ?;", (LIMIT,)
        ).fetchall()

        if not rows:
            print("⚠️  Nincs találat az adatbázisban.")
            return

        print(f"\n📚 Legfrissebb {len(rows)} találat a '{DB_PATH}' adatbázisból:\n")
        for i, (title, link, content, tags, timestamp) in enumerate(rows, 1):
            snippet = shorten(content or "", width=400, placeholder="…")
            print(f"{i:02d}. 📰 {title}")
            print(f"    📅 {timestamp}")
            print(f"    🔗 {link}")
            print(f"    🏷️  {tags}")
            print(f"    🧾  {snippet}\n")

    except Exception as e:
        print("❌ Hiba történt az adatbázis olvasásakor:", e)
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
