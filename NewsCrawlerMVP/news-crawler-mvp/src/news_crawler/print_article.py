# news_crawler/print_article.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import sys

# --- Import: csomagként vagy fallback-kel (ugyanaz a minta, mint scrape_archive.py-ben) ---
try:
    from .fetcher import Fetcher
    from .article_reader import read_article
except Exception:
    from pathlib import Path
    here = Path(__file__).resolve()
    src_root = here.parents[1]  # ./src
    if str(src_root) not in sys.path:
        sys.path.insert(0, str(src_root))
    from news_crawler.fetcher import Fetcher  # type: ignore
    from news_crawler.article_reader import read_article  # type: ignore


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Egyszerű cikk-olvasó: URL(ek) -> cím + törzsszöveg a terminálra."
    )
    ap.add_argument("urls", nargs="*", help="Cikk URL(ek). Ha nincs megadva, --stdin használható.")
    ap.add_argument("--stdin", action="store_true", help="URL-ek beolvasása STDIN-ről (soronként egy).")
    ap.add_argument("--max-chars", type=int, default=None,
                    help="Vágd meg a törzsszöveget legfeljebb N karakterre.")
    ap.add_argument("--ua", default="NewsCrawlerMVP/reader (+crawler)",
                    help="User-Agent az HTTP kérésekhez.")
    args = ap.parse_args()

    urls = list(args.urls)
    if args.stdin:
        for line in sys.stdin:
            s = line.strip()
            if s:
                urls.append(s)

    if not urls:
        ap.error("Adj meg legalább egy URL-t, vagy használd a --stdin kapcsolót.")

    # A projekt Fetcher-je: stabil HTML letöltés
    fetcher = Fetcher(user_agent=args.ua)

    for i, u in enumerate(urls, 1):
        title, body = read_article(u, fetcher=fetcher)
        if not body:
            print(f"[{i:02d}] ⚠️ Nem sikerült hasznos törzsszöveget kinyerni: {u}")
            continue

        if args.max_chars and len(body) > args.max_chars:
            body_out = body[:args.max_chars].rstrip() + "…"
        else:
            body_out = body

        print("=" * 80)
        print(f"[{i:02d}] {u}")
        if title:
            print(f"TITLE: {title}")
        print("-" * 80)
        print(body_out)
        print()


if __name__ == "__main__":
    main()
