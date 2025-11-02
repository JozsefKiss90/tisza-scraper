#!/usr/bin/env python3
# 444_archive_crawler.py
#  - Cél: 444.hu archív cikk-URL-ek begyűjtése sitemapok nélkül.
#  - Források: havi (/YYYY/MM), napi (/YYYY/MM/DD) archívum oldal, illetve az /archivum pagináció.
#  - Kimenet: részletes konzol-riport + opcionális CSV (url,pubdate_guess).
#  - Nincs DB-művelet.
#
# Használat példa:
#   python 444_archive_crawler.py --years 10 --allow-missing-date --report-csv 444_archive.csv
#   python 444_archive_crawler.py --date-from 2015-01-01 --date-to 2025-01-01 --mode auto
#
# Megjegyzések:
#  - A 444 cikk-URL-jei tipikusan így néznek ki: https://444.hu/YYYY/MM/DD/slug...
#  - A pubdate_guess a linkből regexszel kinyert dátum (ha sikerül).
#  - Az /archivum paginációt (?page=N) addig léptetjük, amíg találunk új cikk-linket vagy elérjük az időablak alját.
#  - A havi (/YYYY/MM) és napi (/YYYY/MM/DD) oldalakra kérhető futás (mode=ym, ymd); 'auto' esetén először /archivum,
#    majd (szükség esetén) YM fallback.
#
import argparse
import csv
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import Dict, Iterable, Iterator, List, Optional, Set, Tuple
from urllib.parse import urlunsplit

try:
    import httpx
except Exception:
    print("A futtatáshoz szükséges a 'httpx' csomag: pip install httpx", file=sys.stderr)
    raise

UA = "444ArchiveCrawler/1.0 (+https://example.org)"
DEFAULT_TIMEOUT = 20
DEFAULT_SLEEP = 0.2

ARTICLE_RE = re.compile(r"https?://444\.hu/(20\d{2})/([01]\d)/([0-3]\d)/[a-z0-9\-\._%/]+", re.IGNORECASE)
YM_PAGE = "https://444.hu/{YYYY}/{MM:02d}"
YMD_PAGE = "https://444.hu/{YYYY}/{MM:02d}/{DD:02d}"
ARCHIVUM_PAGE = "https://444.hu/archivum?page={PAGE}"

@dataclass(frozen=True)
class FoundUrl:
    url: str
    pubdate_guess: Optional[date]

def parse_iso_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s).date()
    except Exception:
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        except Exception:
            return None

def extract_article_links(html: str) -> List[FoundUrl]:
    out: List[FoundUrl] = []
    seen: Set[str] = set()
    for m in ARTICLE_RE.finditer(html):
        y, mth, d = m.group(1), m.group(2), m.group(3)
        try:
            dt = date(int(y), int(mth), int(d))
        except Exception:
            dt = None
        url = m.group(0)
        # tisztítsuk a duplumokat
        if url not in seen:
            seen.add(url)
            out.append(FoundUrl(url=url, pubdate_guess=dt))
    return out

def within_range(d: Optional[date], start: Optional[date], end_excl: Optional[date], allow_missing: bool) -> bool:
    if d is None:
        return allow_missing
    if start and d < start:
        return False
    if end_excl and d >= end_excl:
        return False
    return True

def daterange_months(start: date, end_excl: date) -> Iterator[Tuple[int,int]]:
    # yields (year, month) for months intersecting [start, end_excl)
    y, m = start.year, start.month
    while True:
        cur = date(y, m, 1)
        if cur >= end_excl:
            break
        yield (y, m)
        # next month
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1

def daterange_days(start: date, end_excl: date) -> Iterator[date]:
    d = start
    while d < end_excl:
        yield d
        d += timedelta(days=1)

def fetch_text(client: httpx.Client, url: str) -> Optional[str]:
    try:
        r = client.get(url, headers={"User-Agent": UA}, timeout=DEFAULT_TIMEOUT, follow_redirects=True)
        if r.status_code >= 400:
            return None
        ctype = r.headers.get("content-type","").lower()
        if "html" not in ctype and "xml" not in ctype:
            return None
        return r.text
    except Exception:
        return None

def crawl_archivum(client: httpx.Client, *, start: Optional[date], end_excl: Optional[date], sleep: float, allow_missing: bool, max_pages: Optional[int], counters: Dict[str,int], progress_every: int = 50) -> List[FoundUrl]:
    found: List[FoundUrl] = []
    seen: Set[str] = set()
    page = 1
    empty_streak = 0
    while True:
        if max_pages is not None and page > max_pages:
            break
        if page % progress_every == 0:
            print(f"[archivum] page={page} fetched={counters['pages_fetched']} links_seen={counters['links_seen']}")
        url = ARCHIVUM_PAGE.format(PAGE=page)
        time.sleep(sleep)
        html = fetch_text(client, url)
        counters["pages_fetched"] += 1
        if not html:
            counters["page_fetch_errors"] += 1
            empty_streak += 1
            if empty_streak >= 2:  # két hiba után lépjünk ki
                break
            page += 1
            continue
        links = extract_article_links(html)
        counters["links_seen"] += len(links)
        new_on_page = 0
        # logika: ha az oldalon túlnyomórészt a start előttiek vannak, és van start, kiléphetünk
        page_has_any_after_start = False
        for fu in links:
            if fu.url in seen:
                counters["dup_links"] += 1
                continue
            if within_range(fu.pubdate_guess, start, end_excl, allow_missing):
                found.append(fu)
                seen.add(fu.url)
                new_on_page += 1
                if start and fu.pubdate_guess and fu.pubdate_guess >= start:
                    page_has_any_after_start = True
            else:
                counters["range_filtered"] += 1
        if new_on_page == 0:
            empty_streak += 1
        else:
            empty_streak = 0
        # Heurisztika: ha van 'start' és a listán már alig látunk annál frissebb napot, pár üres oldal után megállunk
        if empty_streak >= 2 and start is not None:
            break
        page += 1
    return found

def crawl_ym(client: httpx.Client, *, start: date, end_excl: date, sleep: float, allow_missing: bool, counters: Dict[str,int], progress_every: int = 50, reverse: bool = False) -> List[FoundUrl]:
    found: List[FoundUrl] = []
    seen: Set[str] = set()
    months = list(daterange_months(start, end_excl))
    if reverse:
        months.reverse()
    for idx, (y, m) in enumerate(months, 1):
        if idx % progress_every == 0:
            print(f"[ym] step={idx}/{len(months)} fetched={counters['pages_fetched']} links_seen={counters['links_seen']}")
        url = YM_PAGE.format(YYYY=y, MM=m)
        time.sleep(sleep)
        html = fetch_text(client, url)
        counters["pages_fetched"] += 1
        if not html:
            counters["page_fetch_errors"] += 1
            continue
        links = extract_article_links(html)
        counters["links_seen"] += len(links)
        for fu in links:
            if fu.url in seen:
                counters["dup_links"] += 1
                continue
            if within_range(fu.pubdate_guess, start, end_excl, allow_missing):
                found.append(fu)
                seen.add(fu.url)
            else:
                counters["range_filtered"] += 1
    return found

def crawl_ymd(client: httpx.Client, *, start: date, end_excl: date, sleep: float, allow_missing: bool, counters: Dict[str,int], progress_every: int = 50, reverse: bool = False, max_days: Optional[int] = None) -> List[FoundUrl]:
    found: List[FoundUrl] = []
    seen: Set[str] = set()
    days = list(daterange_days(start, end_excl))
    if reverse:
        days.reverse()
    if max_days is not None:
        days = days[:max_days]
    for idx, d in enumerate(days, 1):
        if idx % progress_every == 0:
            print(f"[ymd] day_step={idx}/{len(days)} fetched={counters['pages_fetched']} links_seen={counters['links_seen']}")
        url = YMD_PAGE.format(YYYY=d.year, MM=d.month, DD=d.day)
        time.sleep(sleep)
        html = fetch_text(client, url)
        counters["pages_fetched"] += 1
        if not html:
            counters["page_fetch_errors"] += 1
            continue
        links = extract_article_links(html)
        counters["links_seen"] += len(links)
        for fu in links:
            if fu.url in seen:
                counters["dup_links"] += 1
                continue
            if within_range(fu.pubdate_guess, start, end_excl, allow_missing):
                found.append(fu)
                seen.add(fu.url)
            else:
                counters["range_filtered"] += 1
    return found

def main():
    ap = argparse.ArgumentParser(description="444.hu archívum-crawler (havi/napi/archivum), részletes riporttal")
    ap.add_argument("--years", type=int, help="Hány évre visszamenőleg (alternatíva: --date-from/--date-to)")
    ap.add_argument("--date-from", help="Kezdő dátum (YYYY-MM-DD)")
    ap.add_argument("--date-to", help="Záró dátum (YYYY-MM-DD, kizáró)")
    ap.add_argument("--mode", choices=["auto","archivum","ym","ymd"], default="auto", help="Bejárási mód: archivum listázás, year-month, year-month-day, vagy auto")
    ap.add_argument("--sleep", type=float, default=DEFAULT_SLEEP, help="Várakozás két kérés között (s)")
    ap.add_argument("--allow-missing-date", action="store_true", help="Ha a linkből nem nyerhető ki dátum, engedjük át")
    ap.add_argument("--max-archivum-pages", type=int, help="archivum mód: ennyi oldal után megállunk (ha nincs időablak)")
    ap.add_argument("--report-csv", help="CSV export elérési út (url,pubdate_guess)")
    ap.add_argument("--sort", choices=["asc","desc"], help="Rendezés a kimenethez és CSV-hez (alap: ha időablak van -> desc, különben asc)")
    ap.add_argument("--limit", type=int, help="Összesített lista levágása a legelső N rekord után")
    ap.add_argument("--print", type=int, help="Ennyi rekordot írjunk ki a konzolra (alap: 30)")
    ap.add_argument("--progress-every", type=int, default=50, help="Ennyi oldalanként írjunk haladási naplót")
    ap.add_argument("--max-days", type=int, help="YMD módban legfeljebb ennyi napot dolgozzunk fel (gyors mintákhoz)")
    args = ap.parse_args()

    # időablak számítás
    if args.date_from or args.date_to:
        start = parse_iso_date(args.date_from) if args.date_from else None
        end_excl = parse_iso_date(args.date_to) if args.date_to else None
    elif args.years:
        today = date.today()
        end_excl = today + timedelta(days=1)  # holnap mint kizáró felső korlát
        start = date(today.year - args.years, today.month, today.day)
    else:
        # alap: 10 év
        today = date.today()
        end_excl = today + timedelta(days=1)
        start = date(today.year - 10, today.month, today.day)

    counters: Dict[str,int] = {
        "pages_fetched": 0,
        "page_fetch_errors": 0,
        "links_seen": 0,
        "dup_links": 0,
        "range_filtered": 0,
    }

    found: List[FoundUrl] = []
    with httpx.Client(headers={"User-Agent": UA}, follow_redirects=True, timeout=DEFAULT_TIMEOUT) as client:
        if args.mode == "archivum" or args.mode == "auto":
            res = crawl_archivum(client, start=start, end_excl=end_excl, sleep=args.sleep, allow_missing=args.allow_missing_date, max_pages=args.max_archivum_pages, counters=counters, progress_every=args.progress_every)
            found.extend(res)
            if args.mode == "auto" and len(found) < 200:  # kevés? próbáljuk YM-mel bővíteni
                res2 = crawl_ym(client, start=start or date(2013,1,1), end_excl=end_excl or (date.today()+timedelta(days=1)), sleep=args.sleep, allow_missing=args.allow_missing_date, counters=counters, progress_every=args.progress_every, reverse=(args.sort=="desc"))
                found.extend(res2)
        elif args.mode == "ym":
            found = crawl_ym(client, start=start or date(2013,1,1), end_excl=end_excl or (date.today()+timedelta(days=1)), sleep=args.sleep, allow_missing=args.allow_missing_date, counters=counters, progress_every=args.progress_every, reverse=(args.sort=="desc"))
        elif args.mode == "ymd":
            found = crawl_ymd(client, start=start or date(2013,1,1), end_excl=end_excl or (date.today()+timedelta(days=1)), sleep=args.sleep, allow_missing=args.allow_missing_date, counters=counters, progress_every=args.progress_every, reverse=(args.sort=="desc"), max_days=args.max_days)

    # Egyedi URL-ek biztosítása
    uniq: Dict[str, FoundUrl] = {}
    for fu in found:
        if fu.url not in uniq:
            uniq[fu.url] = fu
    found_unique = list(uniq.values())

    # Összegző riport
    print("\n=== ÖSSZEGZŐ METRIKÁK ===")
    print(f"pages_fetched                 : {counters['pages_fetched']}")
    print(f"page_fetch_errors             : {counters['page_fetch_errors']}")
    print(f"links_seen                    : {counters['links_seen']}")
    print(f"dup_links                     : {counters['dup_links']}")
    print(f"range_filtered                : {counters['range_filtered']}")
    print(f"elfogadott_url_ossz           : {len(found_unique)}")

    # CSV kiírás
    if args.report_csv:
        with open(args.report_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["url", "pubdate_guess"])
            for fu in found_unique:
                w.writerow([fu.url, fu.pubdate_guess.isoformat() if fu.pubdate_guess else ""])
        print(f"CSV riport mentve: {args.report_csv}")

    # Mintalista
    sample = found_unique[-30:]
    if sample:
        print("\n— Mintalista (első 30) —")
        for fu in sample:
            print(f"{(fu.pubdate_guess.isoformat() if fu.pubdate_guess else '').ljust(12)} {fu.url}")

if __name__ == "__main__":
    main()

python 444_archive_crawler.py --date-from 2023-11-01 --date-to 2025-11-02 --mode auto --sort desc --limit 100 --report-csv 444_top100.csv --print 20