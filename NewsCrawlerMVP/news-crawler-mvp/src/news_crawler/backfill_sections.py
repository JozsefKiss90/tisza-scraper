#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Backfill script: archívum crawl + politikai rovat szűrés + cikk-tartalom scrapelés.

Használat (példák):

  # Telex, csak politikai rovatok, utolsó 365 nap, news.sqlite DB-be
  python -m news_crawler.backfill_sections \
      --domain telex.hu \
      --last-days 365 \
      --db news.sqlite \
      -v

  # Index, fix dátumtartomány
  python -m news_crawler.backfill_sections \
      --domain index.hu \
      --date-from 2023-01-01 \
      --date-to 2023-12-31 \
      --db news.sqlite
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta, date
from .AI_tools.ai_tagging import (
    SimpleHeuristicTagger,
    HuSpacyNerTopicTagger,
    tag_article_and_update,
)

# --- Import: csomagként vagy fallback-kel, ugyanaz a minta mint scrape_archive.py-ben ---
try:
    from .core import NewsCrawlerMVP
    from .filters import Filters, POLITICAL_SECTIONS
except Exception:
    here = Path(__file__).resolve()
    src_root = here.parents[1]  # .../src
    if str(src_root) not in sys.path:
        sys.path.insert(0, str(src_root))
    from news_crawler.core import NewsCrawlerMVP  # type: ignore
    from news_crawler.filters import Filters, POLITICAL_SECTIONS  # type: ignore


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Backfill: archívum crawl + politikai rovatok + cikk-tartalom scrapelés."
    )
    p.add_argument(
        "--domain",
        help="Kötelező domain a bejáráshoz (pl. 444.hu, index.hu, telex.hu, hvg.hu).",
    )
    p.add_argument(
        "--years",
        type=int,
        default=10,
        help="Visszamenőleges évek száma (csak akkor számít, ha nincs date-from).",
    )
    p.add_argument(
        "--date-from",
        dest="date_from",
        default=None,
        help="ISO dátum kezdete (YYYY-MM-DD).",
    )
    p.add_argument(
        "--date-to",
        dest="date_to",
        default=None,
        help="ISO dátum vége (YYYY-MM-DD, kizáró). Ha nincs megadva, automatikusan holnap.",
    )
    p.add_argument(
        "--last-days",
        type=int,
        default=None,
        help="Csak az elmúlt N nap cikkeit gyűjti (date_from beállítása automatikusan).",
    )
    p.add_argument(
        "--db",
        default="news.sqlite",
        help="SQLite adatbázis fájl (alapértelmezett: news.sqlite).",
    )
    p.add_argument(
        "--max-articles",
        type=int,
        default=None,
        help="Legfeljebb ennyi cikk tartalmát scrapeli a 2. fázisban (None = mind).",
    )
    p.add_argument(
        "--tagger",
        choices=["heuristic", "huspacy"],
        default="heuristic",
        help="AI tagger backend: 'heuristic' (gyorsabb) vagy 'huspacy' (magyar NER + topic).",
    )

    p.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Részletes log.",
    )
    return p.parse_args()


def normalize_dates(args: argparse.Namespace) -> None:
    """
    last_days / date_from / date_to kezelés – ugyanaz a logika, mint scrape_archive.py-ben.
    """
    # last_days -> date_from
    if args.last_days is not None and not args.date_from:
        start = (datetime.utcnow() - timedelta(days=args.last_days)).date()
        args.date_from = start.isoformat()

    # ha van date_from, de nincs date_to -> holnap
    if args.date_from and not args.date_to:
        args.date_to = (date.today() + timedelta(days=1)).isoformat()

    # ha fordítva adták meg, cseréljük fel
    if args.date_from and args.date_to and args.date_from > args.date_to:
        args.date_from, args.date_to = args.date_to, args.date_from


def build_predicate(args: argparse.Namespace):
    """
    Pipeline.collect() predicate összerakása:

      - kötelező domain (biztonság kedvéért)
      - dátum intervallum (Filters.by_date_range)
      - politikai rovat-szűrés (Filters.by_url_section(POLITICAL_SECTIONS))
    """
    preds = []

    if args.domain:
        preds.append(Filters.by_domain([args.domain]))

    # dátum intervallum – Article.published_dt() datetime-et ad vissza, ha published ISO. 
    start_dt = datetime.fromisoformat(args.date_from) if args.date_from else None
    end_dt = datetime.fromisoformat(args.date_to) if args.date_to else None
    if start_dt or end_dt:
        preds.append(Filters.by_date_range(start_dt, end_dt))

    # politikai rovatok – POLITICAL_SECTIONS domain→[section] mappinget használ. 
    preds.append(Filters.by_url_section(POLITICAL_SECTIONS))

    if not preds:
        return None
    return Filters.compose(*preds)


def crawl_phase(app: NewsCrawlerMVP, args: argparse.Namespace) -> int:
    """
    1. fázis: archívum bejárás, csak politikai rovatokra szűrve.
    Pipeline.collect() + RegexArchiveAdapter.iter_archive(). 
    """
    # csak az adott domain adaptere maradjon
    if args.domain:
        app.pipeline.adapters = [ad for ad in app.pipeline.adapters if getattr(ad, "domain", None) == args.domain]

    if not app.pipeline.adapters:
        print(f"[BACKFILL] Nincs adapter ehhez a domainhez: {args.domain}")
        return 0

    predicate = build_predicate(args)

    def _log(art, n):
        if args.verbose:
            print(f"[CRAWL {n:05d}] {art.source or '—'}  pub={art.published or '—'}  {art.link}")

    inserted = app.pipeline.collect(
        years=args.years,
        date_from=args.date_from,
        date_to=args.date_to,
        predicate=predicate,
        on_item=_log,
    )
    print(f"[BACKFILL] Crawl fázis kész, upsertelt rekordok (meta): ~{inserted}")
    return inserted


def content_backfill_phase(app: NewsCrawlerMVP, args: argparse.Namespace) -> int:
    """
    2. fázis: tartalom backfill.

    Minden olyan cikkre, ahol content NULL vagy üres, meghívja a
    Repository.get_or_fetch_article(url, fetcher=app.fetcher)-t. 
    """
    conn = app.repo.conn
    cur = conn.cursor()

    # SELECT a domain + dátum intervallumba eső, content nélküli cikkeket
    sql = [
        "SELECT a.url, a.published_date, s.domain",
        "FROM articles a",
        "JOIN sources s ON s.id = a.source_id",
        "WHERE (a.content IS NULL OR a.content = '')",
    ]
    params = []

    if args.domain:
        sql.append("AND s.domain = ?")
        params.append(args.domain)

    if args.date_from:
        sql.append("AND a.published_date >= ?")
        params.append(args.date_from)

    if args.date_to:
        sql.append("AND a.published_date <= ?")
        params.append(args.date_to)

    sql.append("ORDER BY a.published_date ASC")

    full_sql = " ".join(sql)
    rows = cur.execute(full_sql, params).fetchall()
    total_candidates = len(rows)

    if args.max_articles is not None:
        rows = rows[: args.max_articles]

    print(
        f"[BACKFILL] Tartalom backfill indul: {len(rows)} cikk (jelölt: {total_candidates}, limit: {args.max_articles})"
    )

    # Tagger választása CLI alapján
    if getattr(args, "tagger", "heuristic") == "huspacy":
        tagger = HuSpacyNerTopicTagger()
        if args.verbose:
            print("[BACKFILL] HuSpaCy NER+topic tagger lesz használva.")
    else:
        tagger = SimpleHeuristicTagger()
        if args.verbose:
            print("[BACKFILL] Heurisztikus (offline) tagger lesz használva.")


    ok = 0
    for i, row in enumerate(rows, 1):
        url = row["url"]
        try:
            art = app.repo.get_or_fetch_article(url, fetcher=app.fetcher)
            clen = len(art.content or "")
            if args.verbose:
                print(f"[CONTENT {i:05d}] OK len={clen:5d}  {url}")

            # 🔹 ÚJ: AI tagging + DB update
            tagging = tag_article_and_update(app.repo, art, tagger)
            if args.verbose:
                # csak rövid infó, ne legyen spam
                print(f"[TAG   {i:05d}] topics={tagging.topics} "
                      f"keywords={tagging.keywords[:5]}")

            ok += 1

        except Exception as e:
            # meglévő HIBA log marad
            if args.verbose:
                print(f"[CONTENT {i:05d}] HIBA {url} -> {e}")
            # stb.

    return ok


def main() -> None:
    args = parse_args()

    if not args.domain:
        sys.exit("❌ A backfill script jelenleg egyetlen domainre van tervezve. Add meg a --domain paramétert.")

    normalize_dates(args)

    print(
        f"[BACKFILL] Start domain={args.domain} "
        f"from={args.date_from or f'-{args.years} év'} "
        f"to={args.date_to or 'ma+1'} db={args.db}"
    )

    app = NewsCrawlerMVP(db_path=args.db)

    # 1) Cikk URL-ek (meta) backfill
    crawl_inserted = crawl_phase(app, args)

    # 2) Cikk-tartalom backfill
    content_filled = content_backfill_phase(app, args)

    print(
        f"[BACKFILL] Összegzés: meta upsert ~{crawl_inserted} rekord, "
        f"tartalom scrapelve: {content_filled} cikk."
    )


if __name__ == "__main__":
    main()

