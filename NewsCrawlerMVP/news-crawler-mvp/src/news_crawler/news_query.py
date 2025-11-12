# news_query.py
import argparse, os, sqlite3, sys

p = argparse.ArgumentParser()
p.add_argument("--db", default="news_10d.sqlite")
args = p.parse_args()

db = os.path.abspath(args.db)
print("Megnyitott adatbázis abszolút elérési útja:", db)

if not os.path.exists(db):
    sys.exit("❌ A megadott DB nem létezik ezen az útvonalon.")

conn = sqlite3.connect(db)

tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
if "items" not in tables:
    print("Elérhető táblák:", tables)
    sys.exit("❌ Nincs 'items' tábla ebben a DB-ben (valószínűleg üres fájl).")

print("== 444.hu utolsó 10 nap ==")
for row in conn.execute("""
  SELECT published, COUNT(*) 
  FROM items
  WHERE source='444.hu' AND published >= date('now','-10 days')
  GROUP BY published ORDER BY published DESC
"""):
    print(row)

print("\n== Duplikált linkek? ==")
for row in conn.execute("""
  SELECT link, COUNT(*) c
  FROM items GROUP BY link HAVING c>1
  ORDER BY c DESC LIMIT 10
"""):
    print(row)

print("Min/max published az ablakban:")
for row in conn.execute("""
  SELECT MIN(published), MAX(published)
  FROM items
  WHERE source='444.hu' AND published >= date('now','-10 days') AND published < date('now','+1 day')
"""):
    print(row)

for row in conn.execute("""
  SELECT published, COUNT(*) c
  FROM items
  WHERE source='444.hu' 
    AND published >= date('now','-10 days') AND published < date('now','+1 day')
  GROUP BY published
  HAVING c < 10
  ORDER BY published DESC
"""):
    print(row)
