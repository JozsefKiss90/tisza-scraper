#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Dict, Iterator, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

import httpx
import yaml

# --- HTTP beállítások ---------------------------------------------------------

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
DEFAULT_TIMEOUT = 25
DEFAULT_SLEEP = 0.25
MAX_RETRIES = 3
RETRY_BACKOFF = 0.75  # sec (exponenciális)

def build_headers(page_url: str) -> dict:
    return {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "hu-HU,hu;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": page_url,
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

def fetch_text(client: httpx.Client, url: str) -> Optional[str]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = client.get(
                url,
                headers=build_headers(url),
                timeout=DEFAULT_TIMEOUT,
                follow_redirects=True,
            )
            if r.status_code >= 400:
                if r.status_code in (403, 429) and attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF * attempt)
                    continue
                print(f"[HTTP] {r.status_code} {url}")
                return None
            ctype = (r.headers.get("content-type") or "").lower()
            if "html" not in ctype and "xml" not in ctype:
                return None
            return r.text
        except Exception:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF * attempt)
                continue
            return None

# --- Adatszerkezet ------------------------------------------------------------

@dataclass(frozen=True)
class FoundUrl:
    url: str
    pubdate_guess: Optional[date]

# --- Segédfüggvények ----------------------------------------------------------

def compile_article_regex(pattern: str) -> re.Pattern:
    return re.compile(pattern, re.IGNORECASE)

def canonicalize_url(u: str, *, force_https: bool = True) -> str:
    try:
        pr = urlparse(u)
        scheme = "https" if force_https else (pr.scheme or "https")
        netloc = pr.netloc.lower()
        path = pr.path or "/"
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/")
        return urlunparse(pr._replace(scheme=scheme, netloc=netloc, path=path))
    except Exception:
        return u

def _extract_href_value(attr: str) -> str:
    """
    href="...": csak a belső értéket adja vissza.
    (Itt volt a hiba: nem raw-string/regex volt, ezért bent maradt a 'href="' prefix is.)
    """
    m = re.search(r'href\s*=\s*([\'"])(.*?)\1', attr, re.IGNORECASE)
    if m:
        return m.group(2).strip()
    # Biztonsági fallback
    s = re.sub(r'^\s*href\s*=\s*', '', attr, flags=re.IGNORECASE).strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1]
    return s.strip()

def parse_iso_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

def daterange_months(start: date, end_excl: date) -> Iterator[Tuple[int, int]]:
    y, m = start.year, start.month
    while True:
        d = date(y, m, 1)
        if d >= end_excl:
            break
        yield (y, m)
        if m == 12:
            y += 1; m = 1
        else:
            m += 1

def daterange_days(start: date, end_excl: date) -> Iterator[date]:
    d = start
    while d < end_excl:
        yield d
        d += timedelta(days=1)

# --- Link-kinyerés ------------------------------------------------------------

def extract_article_links(
    html: str,
    article_re: re.Pattern,
    rel_re: re.Pattern,
    base_url: str,
    *,
    force_https: bool,
) -> List[FoundUrl]:
    out: List[FoundUrl] = []
    seen: Set[str] = set()

    '''for m in re.finditer(r'href=[\'"]([^\'"<>]+)[\'"]', html):
        href = m.group(1)
        if not href.startswith('#') and not href.startswith('javascript:'):
            print(f"[DEBUG HREF] {href}")'''

    # Abszolút URL-ek (pl. https://telex.hu/belfold/2025/11/04/...)
    for m in article_re.finditer(html):
        dt = None
        try:
            y = int(m.group(1)); mo = int(m.group(2))
            d = int(m.group(3)) if m.lastindex and m.lastindex >= 3 else None
            if d is not None:
                dt = date(y, mo, d)
        except Exception:
            dt = None
        url = canonicalize_url(m.group(0), force_https=force_https)
        if url not in seen:
            seen.add(url)
            out.append(FoundUrl(url, dt))

    # Relatív URL-ek (href="/belfold/2025/11/04/...")
    for m in rel_re.finditer(html):
        dt = None
        try:
            y = int(m.group(2)); mo = int(m.group(3))
            d = int(m.group(4)) if m.lastindex and m.lastindex >= 4 else None
            if d is not None:
                dt = date(y, mo, d)
        except Exception:
            dt = None
        rel = _extract_href_value(m.group(0))
        url = canonicalize_url(urljoin(base_url.rstrip('/')+'/', rel), force_https=force_https)
        if url not in seen:
            seen.add(url)
            out.append(FoundUrl(url, dt))

    return out

def within_range(d: Optional[date], start: Optional[date], end_excl: Optional[date], allow_missing: bool) -> bool:
    if d is None:
        return allow_missing
    if start and d < start:
        return False
    if end_excl and d >= end_excl:
        return False
    return True

# --- Crawler-ek ---------------------------------------------------------------

def crawl_archivum(
    client: httpx.Client,
    page_tmpl: str,
    *,
    start: Optional[date],
    end_excl: Optional[date],
    sleep: float,
    allow_missing: bool,
    counters: Dict[str, int],
    progress_every: int,
    max_pages: Optional[int],
    reverse: bool,
    article_re: re.Pattern,
    rel_re: re.Pattern,
    base_url: str,
    force_https: bool,
) -> List[FoundUrl]:
    found: List[FoundUrl] = []
    seen: Set[str] = set()
    page = 1
    empty_streak = 0

    while True:
        in_range_seen = False
        if max_pages is not None and page > max_pages:
            break
        if page % max_pages if max_pages else page % progress_every == 0:
            print(f"[archivum] page={page} fetched={counters['pages_fetched']} links_seen={counters['links_seen']}")
        url = page_tmpl.format(PAGE=page)
        time.sleep(sleep)
        html = fetch_text(client, url); counters["pages_fetched"] += 1
        if not html:
            counters["page_fetch_errors"] += 1
            empty_streak += 1
            if empty_streak >= 3:
                break
            page += 1
            continue

        links = extract_article_links(html, article_re, rel_re, base_url, force_https=force_https)
        counters["links_seen"] += len(links)

        new_on_page = 0
        for fu in links:
            if fu.url in seen:
                counters["dup_links"] += 1
                continue
            if within_range(fu.pubdate_guess, start, end_excl, allow_missing):
                found.append(fu); seen.add(fu.url); new_on_page += 1
                in_range_seen = True
            else:
                counters["range_filtered"] += 1


        empty_streak = 0 if new_on_page > 0 else empty_streak + 1
        if in_range_seen and empty_streak >= 3 and start is not None:
            break

        page += 1

    return found

def crawl_ym(
    client: httpx.Client,
    tmpl: str,
    *,
    start: date,
    end_excl: date,
    sleep: float,
    allow_missing: bool,
    counters: Dict[str, int],
    progress_every: int,
    reverse: bool,
    article_re: re.Pattern,
    rel_re: re.Pattern,
    base_url: str,
    force_https: bool,
) -> List[FoundUrl]:
    found: List[FoundUrl] = []
    seen: Set[str] = set()
    months = list(daterange_months(start, end_excl))
    if reverse:
        months.reverse()
    for idx, (y, m) in enumerate(months, 1):
        if idx % progress_every == 0:
            print(f"[ym] step={idx}/{len(months)} fetched={counters['pages_fetched']} links_seen={counters['links_seen']}")
        url = tmpl.format(YYYY=y, MM=m)
        time.sleep(sleep)
        html = fetch_text(client, url); counters["pages_fetched"] += 1
        if not html:
            counters["page_fetch_errors"] += 1
            continue
        links = extract_article_links(html, article_re, rel_re, base_url, force_https=force_https)
        counters["links_seen"] += len(links)
        for fu in links:
            if fu.url in seen:
                counters["dup_links"] += 1
                continue
            if within_range(fu.pubdate_guess, start, end_excl, allow_missing):
                found.append(fu); seen.add(fu.url)
            else:
                counters["range_filtered"] += 1
    return found

def crawl_ymd(
    client: httpx.Client,
    tmpl: str,
    *,
    start: date,
    end_excl: date,
    sleep: float,
    allow_missing: bool,
    counters: Dict[str, int],
    progress_every: int,
    reverse: bool,
    max_days: Optional[int],
    article_re: re.Pattern,
    rel_re: re.Pattern,
    base_url: str,
    force_https: bool,
) -> List[FoundUrl]:
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
        url = tmpl.format(YYYY=d.year, MM=d.month, DD=d.day)
        time.sleep(sleep)
        html = fetch_text(client, url); counters["pages_fetched"] += 1
        if not html:
            counters["page_fetch_errors"] += 1
            continue
        links = extract_article_links(html, article_re, rel_re, base_url, force_https=force_https)
        counters["links_seen"] += len(links)
        for fu in links:
            if fu.url in seen:
                counters["dup_links"] += 1
                continue
            if within_range(fu.pubdate_guess, start, end_excl, allow_missing):
                found.append(fu); seen.add(fu.url)
            else:
                counters["range_filtered"] += 1
    return found


# --- CLI / main ---------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Archívum-crawler (telexfix)")
    ap.add_argument("--config", default="news_sites_telex.yaml")
    ap.add_argument("--site", required=True)
    ap.add_argument("--years", type=int)
    ap.add_argument("--date-from")
    ap.add_argument("--date-to")
    ap.add_argument("--mode", choices=["auto", "archivum", "ym", "ymd"], default="auto")
    ap.add_argument("--sleep", type=float, default=DEFAULT_SLEEP)
    ap.add_argument("--allow-missing-date", action="store_true")
    ap.add_argument("--max-archivum-pages", type=int)
    ap.add_argument("--max-days", type=int)
    ap.add_argument("--report-csv")
    ap.add_argument("--sort", choices=["asc", "desc"])
    ap.add_argument("--limit", type=int)
    ap.add_argument("--print", type=int)
    ap.add_argument("--progress-every", type=int, default=10)
    ap.add_argument("--no-force-https", action="store_true")
    args = ap.parse_args()

    # YAML betöltése
    try:
        with open(args.config, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Nincs meg a config: {args.config}", file=sys.stderr)
        sys.exit(2)

    if args.site not in cfg:
        print(f"Ismeretlen --site: {args.site}", file=sys.stderr)
        sys.exit(2)

    site = cfg[args.site]
    article_regex = site.get("article_regex")
    rel_regex = site.get("relative_article_regex")
    base_url = site.get("base_url") or f"https://{args.site}"

    if not article_regex:
        print("article_regex hiányzik", file=sys.stderr)
        sys.exit(2)
    if not rel_regex:
        # jól bevált alapértelmezés Telexhez
        rel_regex = r'href=[\'\"]/((?:[a-z0-9\-]+/)?(20\d{2})/([01]\d)/([0-3]\d)/[^\'\"<>]+)[\'\"]'

    article_re = compile_article_regex(article_regex)
    rel_re = re.compile(rel_regex, re.IGNORECASE)

    # Időtartomány
    if args.date_from or args.date_to:
        start = parse_iso_date(args.date_from) if args.date_from else None
        end_excl = parse_iso_date(args.date_to) if args.date_to else None
    elif args.years:
        today = date.today()
        end_excl = today + timedelta(days=1)
        start = date(today.year - args.years, today.month, today.day)
    else:
        today = date.today()
        end_excl = today + timedelta(days=1)
        start = date(today.year - 10, today.month, today.day)

    counters: Dict[str, int] = {
        "pages_fetched": 0,
        "page_fetch_errors": 0,
        "links_seen": 0,
        "dup_links": 0,
        "range_filtered": 0,
    }

    force_https = not args.no_force_https
    default_sort = "desc" if (args.date_from or args.date_to or args.years) else "asc"
    sort_dir = args.sort or default_sort
    progress_every = max(1, args.progress_every)
    max_pages = args.max_archivum_pages
    max_days = args.max_days

    # kliens
    client = httpx.Client(http2=True)

    found: List[FoundUrl] = []
    reverse_iter = (sort_dir == "desc")
    allow_missing = bool(args.allow_missing_date)
    sleep = args.sleep

    # Módfuttatások
    page_tmpl = site.get("archivum", {}).get("page_template")
    ym_tmpl = site.get("ym", {}).get("template")
    ymd_tmpl = site.get("ymd", {}).get("template")

    if args.mode == "archivum":
        if not page_tmpl:
            print("archivum.page_template hiányzik a YAML-ban", file=sys.stderr)
            sys.exit(2)
        found = crawl_archivum(
            client, page_tmpl,
            start=start, end_excl=end_excl, sleep=sleep,
            allow_missing=allow_missing, counters=counters,
            progress_every=progress_every, max_pages=max_pages, reverse=reverse_iter,
            article_re=article_re, rel_re=rel_re, base_url=base_url, force_https=force_https
        )
    elif args.mode == "ym":
        if not ym_tmpl:
            print("ym.template hiányzik a YAML-ban", file=sys.stderr)
            sys.exit(2)
        if not start or not end_excl:
            print("YM-hez konkrét dátumtartomány kell (start, end_excl)", file=sys.stderr)
            sys.exit(2)
        found = crawl_ym(
            client, ym_tmpl,
            start=start, end_excl=end_excl, sleep=sleep,
            allow_missing=allow_missing, counters=counters,
            progress_every=progress_every, reverse=reverse_iter,
            article_re=article_re, rel_re=rel_re, base_url=base_url, force_https=force_https
        )
    elif args.mode == "ymd":
        if not ymd_tmpl:
            print("ymd.template hiányzik a YAML-ban", file=sys.stderr)
            sys.exit(2)
        if not start or not end_excl:
            print("YMD-hez konkrét dátumtartomány kell (start, end_excl)", file=sys.stderr)
            sys.exit(2)
        found = crawl_ymd(
            client, ymd_tmpl,
            start=start, end_excl=end_excl, sleep=sleep,
            allow_missing=allow_missing, counters=counters,
            progress_every=progress_every, reverse=reverse_iter, max_days=max_days,
            article_re=article_re, rel_re=rel_re, base_url=base_url, force_https=force_https
        )
    else:
        # AUTO: archivum → ha kevés, YM fallback → ha még mindig kevés és van ymd, opcionálisan YMD
        if page_tmpl:
            found = crawl_archivum(
                client, page_tmpl,
                start=start, end_excl=end_excl, sleep=sleep,
                allow_missing=allow_missing, counters=counters,
                progress_every=progress_every, max_pages=max_pages, reverse=reverse_iter,
                article_re=article_re, rel_re=rel_re, base_url=base_url, force_https=force_https
            )
        if ym_tmpl and len(found) < 200 and start and end_excl:
            more = crawl_ym(
                client, ym_tmpl,
                start=start, end_excl=end_excl, sleep=sleep,
                allow_missing=allow_missing, counters=counters,
                progress_every=progress_every, reverse=reverse_iter,
                article_re=article_re, rel_re=rel_re, base_url=base_url, force_https=force_https
            )
            found.extend(more)
        if ymd_tmpl and len(found) < 200 and start and end_excl:
            more2 = crawl_ymd(
                client, ymd_tmpl,
                start=start, end_excl=end_excl, sleep=sleep,
                allow_missing=allow_missing, counters=counters,
                progress_every=progress_every, reverse=reverse_iter, max_days=max_days,
                article_re=article_re, rel_re=rel_re, base_url=base_url, force_https=force_https
            )
            found.extend(more2)

    # dedup-kanonizálás (URL szerint)
    # mivel kanonizált URL-ekkel dolgoztunk, elég a set
    seen2: Set[str] = set()
    found_unique: List[FoundUrl] = []
    for fu in found:
        if fu.url in seen2:
            continue
        seen2.add(fu.url)
        found_unique.append(fu)

    # statok
    print("\n=== ÖSSZEGZŐ METRIKÁK ===")
    print(f"pages_fetched                 : {counters['pages_fetched']}")
    print(f"page_fetch_errors             : {counters['page_fetch_errors']}")
    print(f"links_seen                    : {counters['links_seen']}")
    print(f"dup_links                     : {counters['dup_links']}")
    print(f"range_filtered                : {counters['range_filtered']}")
    print(f"elfogadott_url_ossz           : {len(found_unique)}")

    # rendezés: dátum szerint, a dátum nélküliek a végére kerülnek
    def sort_key(fu: FoundUrl):
        return (fu.pubdate_guess or date.min, fu.url)

    sorted_list = sorted(found_unique, key=sort_key, reverse=(sort_dir == "desc"))
    with_date = [fu for fu in sorted_list if fu.pubdate_guess is not None]
    without_date = [fu for fu in sorted_list if fu.pubdate_guess is None]
    sorted_list = with_date + without_date

    # limit
    full_limit = args.limit if args.limit is not None else len(sorted_list)
    sorted_list = sorted_list[:full_limit]

    # CSV
    if args.report_csv:
        with open(args.report_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["url", "pubdate_guess"])
            for fu in sorted_list:
                w.writerow([fu.url, fu.pubdate_guess.isoformat() if fu.pubdate_guess else ""])
        print(f"CSV riport mentve: {args.report_csv}")

    # minta-lista
    to_show = args.print if args.print is not None else min(30, len(sorted_list))
    if to_show:
        print(f"\n— Mintalista ({to_show} elem, rendezés: {sort_dir}) —")
        for fu in sorted_list[:to_show]:
            print(f"{(fu.pubdate_guess.isoformat() if fu.pubdate_guess else '').ljust(12)} {fu.url}")

if __name__ == "__main__":
    main()
