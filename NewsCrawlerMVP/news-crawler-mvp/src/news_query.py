#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Egyszerű DB query util:

- Kiírja, hány cikk van az adatbázisban összesen.
- Kiírja, domainenként hány cikk van.
- Opcionálisan listázza az összes (vagy limitált számú) cikket: dátum, domain, cím, URL.

Használat példák:

  python db_query_all.py --db index_30d.sqlite

  python db_query_all.py --db index_30d.sqlite --list --limit 100

  python db_query_all.py --db telex_30d.sqlite --domain telex.hu --list
"""

import argparse
import sqlite3
from pathlib import Path

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--db",
        required=True,
        help="SQLite adatbázis fájl (pl. telex_30d.sqlite, index_30d.sqlite).",
    )
    p.add_argument(
        "--domain",
        default=None,
        help="Opcionális domain szűrő (pl. telex.hu, index.hu).",
    )
    p.add_argument(
        "--list",
        action="store_true",
        help="Ha megadod, ki is listázza a cikkeket (nem csak számol).",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Legfeljebb ennyi cikket listáz (alap: nincs limit, ha --list-et használsz).",
    )
    args = p.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"❌ Nincs ilyen DB fájl: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    print(f"=== DB: {db_path} ===")

    # 1) összes cikk
    total_sql = "SELECT COUNT(*) AS n FROM articles"
    if args.domain:
        total_sql += " WHERE source_id IN (SELECT id FROM sources WHERE domain = ?)"
        total_row = cur.execute(total_sql, (args.domain,)).fetchone()
    else:
        total_row = cur.execute(total_sql).fetchone()

    total = total_row["n"]
    if args.domain:
        print(f"Összes cikk (domain={args.domain}): {total}")
    else:
        print(f"Összes cikk (minden domain): {total}")

    # 2) domainenkénti bontás
    print("\n--- Domainenkénti cikkdarabszám ---")
    if args.domain:
        rows = cur.execute(
            """
            SELECT s.domain, COUNT(*) AS n
            FROM articles a
            JOIN sources s ON s.id = a.source_id
            WHERE s.domain = ?
            GROUP BY s.domain
            ORDER BY n DESC;
            """,
            (args.domain,),
        ).fetchall()
    else:
        rows = cur.execute(
            """
            SELECT s.domain, COUNT(*) AS n
            FROM articles a
            JOIN sources s ON s.id = a.source_id
            GROUP BY s.domain
            ORDER BY n DESC;
            """
        ).fetchall()

    if not rows:
        print("(Nincs adat.)")
    else:
        for r in rows:
            print(f"{r['domain']}: {r['n']} cikk")

    # 3) opcionális részletes lista
    if args.list:
        print("\n--- Cikklista ---")
        list_sql = """
            SELECT
                a.id,
                s.domain,
                a.published_date,
                a.title,
                a.url
            FROM articles a
            JOIN sources s ON s.id = a.source_id
        """
        params = []
        conds = []
        if args.domain:
            conds.append("s.domain = ?")
            params.append(args.domain)

        if conds:
            list_sql += " WHERE " + " AND ".join(conds)

        list_sql += " ORDER BY a.published_date DESC, a.id DESC"

        if args.limit is not None:
            list_sql += f" LIMIT {int(args.limit)}"

        rows = cur.execute(list_sql, params).fetchall()
        for r in rows:
            print(
                f"[{r['id']:06d}] {r['published_date'] or '----'} "
                f"{r['domain']:10s} {r['title'] or '(cím nélkül)'}"
            )
            print(f"          {r['url']}")
        print(f"\n(Listázott cikkek száma: {len(rows)})")

    conn.close()


if __name__ == "__main__":
    main()
