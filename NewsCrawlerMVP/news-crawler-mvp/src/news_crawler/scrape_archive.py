# news_crawler/scrape_archive.py  (fejléc)
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta 
# Próbáljuk csomagként (python -m news_crawler.scrape_archive)
try:
    from .core import NewsCrawlerMVP
    from .filters import Filters
except Exception:
    # Ha közvetlenül fut (python scrape_archive.py), vegyük fel a src gyökerét
    here = Path(__file__).resolve()
    src_root = here.parents[1]  # .../src
    if str(src_root) not in sys.path:
        sys.path.insert(0, str(src_root))
    from news_crawler.core import NewsCrawlerMVP
    from news_crawler.filters import Filters

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Egydomaines archívum-scraper (MVP)."
    )
    p.add_argument("--domain",
                   help="Opcionális domain a bejáráshoz (pl. 444.hu, index.hu, telex.hu, hvg.hu). Ha nincs megadva, minden adapter fut.")
    p.add_argument("--years", type=int, default=10,
                   help="Visszamenőleges évek száma (MVP; az archív lapozás kiterjedését befolyásolja).")
    p.add_argument("--date-from", dest="date_from", default=None,
                   help="ISO dátum kezdete (YYYY-MM-DD).")
    p.add_argument("--date-to", dest="date_to", default=None,
                   help="ISO dátum vége (YYYY-MM-DD, kizáró).")
    p.add_argument("--db", default="news.sqlite",
                   help="SQLite adatbázis fájl (alapértelmezett: news.sqlite).")
    p.add_argument("--query", default=None,
                   help="(Opcionális) azonnali keresés a gyűjtés után (FTS/BM25, ha elérhető).")
    p.add_argument("--limit", type=int, default=25,
                   help="Keresési találatok száma (alapértelmezett: 25).")
    p.add_argument("--order", default="bm25", choices=["bm25", "time"],
                   help="Keresési rendezés: bm25 vagy time.")
    p.add_argument("--verbose", "-v", action="store_true",
                   help="Részletes log: minden talált cikket kiír crawl közben.")
    p.add_argument("--last-days", type=int, default=None,
               help="Csak az elmúlt N nap cikkeit gyűjti (date_from beállítása automatikusan).")
    return p.parse_args()


def main() -> None:
    
    args = parse_args()
    from datetime import date, timedelta 
    
    if args.last_days is not None and not args.date_from:
        start = (datetime.utcnow() - timedelta(days=args.last_days)).date()
        args.date_from = start.isoformat()
    if args.date_from and not args.date_to:
        args.date_to = (date.today() + timedelta(days=1)).isoformat()

    # Alkalmazás inicializálása
    app = NewsCrawlerMVP(db_path=args.db)
    if args.domain:
        app.pipeline.adapters = [ad for ad in app.pipeline.adapters if ad.domain == args.domain]

    predicate = None
    preds = []

    if args.domain:
        preds.append(Filters.by_domain([args.domain]))

    # ha van date_from/date_to, építsünk dátumszűrőt
    start_dt = datetime.fromisoformat(args.date_from) if args.date_from else None
    end_dt   = datetime.fromisoformat(args.date_to)   if args.date_to   else None
    if start_dt or end_dt:
        preds.append(Filters.by_date_range(start_dt, end_dt))

    if preds:
        predicate = Filters.compose(*preds)

    def _log(art, n):
        if args.verbose:
            print(f"[{n:05d}] {art.source or '—'}  pub={art.published or '—'}  {art.link}")
    
    # Crawl
    inserted = app.pipeline.collect(
        years=args.years,
        date_from=args.date_from,
        date_to=args.date_to,
        predicate=predicate,
        on_item=_log
    )
    print(f"[OK] Mentett rekordok (upsert): ~{inserted}")

    # Opcionális keresés közvetlenül utána
    if args.query:
        print(f"\n[KERESÉS] \"{args.query}\" — top {args.limit} ({args.order})\n")
        hits = app.search(args.query, limit=args.limit, order=args.order)
        for i, h in enumerate(hits, 1):
            date = h.get("date") or "—"
            title = h.get("title") or ""
            link = h.get("link") or ""
            print(f"{i:02d}. [{date}] {title} — {link}")

if __name__ == "__main__":
    main()
