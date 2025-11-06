#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
hvg_archive_crawler.py
- Cél: hvg.hu archív (Friss hírek) oldalakról cikk-URL-ek begyűjtése sitemap nélkül.
- Bejárás: /frisshirek (1. oldal), majd /frisshirek/2, /frisshirek/3, ...
- Kinyerés: URL-ekből YYYYMMDD dátum minta alapján (pl. /itthon/20251105_valami-cikk)
- Kimenet: részletes konzol-riport + opcionális CSV (url, pubdate_guess)
"""

import argparse
import csv
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse, urlunparse

try:
    import httpx
except Exception:
    print("A futtatáshoz szükséges a 'httpx' csomag: pip install httpx", file=sys.stderr)
    raise

try:
    import yaml  # PyYAML
except Exception:
    print("A futtatáshoz szükséges a 'PyYAML' csomag: pip install pyyaml", file=sys.stderr)
    raise


UA = "HVGArchiveCrawler/1.0 (+https://example.org)"
DEFAULT_TIMEOUT = 20
DEFAULT_SLEEP = 0.25

@dataclass(frozen=True)
class FoundUrl:
    url: str
    pubdate_guess: Optional[date]


def build_headers(page_url: str) -> dict:
    return {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "hu-HU,hu;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": page_url,
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def fetch_text(client: httpx.Client, url: str) -> Optional[str]:
    try:
        r = client.get(url, headers=build_headers(url), timeout=DEFAULT_TIMEOUT, follow_redirects=True)
        if r.status_code >= 400:
            print(f"[HTTP] {r.status_code} {url}")
            return None
        ctype = (r.headers.get("content-type") or "").lower()
        if "html" not in ctype and "xml" not in ctype:
            return None
        return r.text
    except Exception:
        return None


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


def compile_article_regex(pattern: str) -> re.Pattern:
    """
    HVG cikk-URL minta:
      https://hvg.hu/<rovat>/<YYYYMMDD>_<slug>
    Példák: /itthon/20251105_A-gyori-polgarmester..., /gazdasag/20251016_...
    """
    return re.compile(pattern, re.IGNORECASE)


def extract_article_links(html: str, article_re: re.Pattern, rel_re: re.Pattern,
                          base_url: str, *, force_https: bool) -> List[FoundUrl]:
    out: List[FoundUrl] = []
    seen: Set[str] = set()

    # Abszolút egyezések
    for m in article_re.finditer(html):
        dt = None
        try:
            y = int(m.group(1)); mo = int(m.group(2)); d = int(m.group(3))
            dt = date(y, mo, d)
        except Exception:
            dt = None
        url = canonicalize_url(m.group(0), force_https=force_https)
        if url not in seen:
            seen.add(url)
            out.append(FoundUrl(url=url, pubdate_guess=dt))

    # Relatív egyezések (href="/...")
    for m in rel_re.finditer(html):
        dt = None
        try:
            y = int(m.group(1)); mo = int(m.group(2)); d = int(m.group(3))
            dt = date(y, mo, d)
        except Exception:
            dt = None
        rel = m.group(0)
        # href="...": csak az érték
        href_m = re.search(r'href\s*=\s*([\'"])(.*?)\1', rel, re.IGNORECASE)
        if not href_m:
            continue
        rel_value = href_m.group(2)
        if rel_value.startswith("#") or rel_value.lower().startswith("javascript:"):
            continue
        url = urljoin(base_url.rstrip("/") + "/", rel_value.lstrip("/"))
        url = canonicalize_url(url, force_https=force_https)
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


def crawl_archivum(client: httpx.Client, page_tmpl: str, *,
                   start: Optional[date], end_excl: Optional[date],
                   sleep: float, allow_missing: bool, counters: Dict[str,int],
                   progress_every: int, max_pages: Optional[int],
                   article_re: re.Pattern, rel_re: re.Pattern,
                   base_url: str, force_https: bool) -> List[FoundUrl]:
    """
    HVG: /frisshirek (1. oldal) és /frisshirek/{PAGE} (2..N)
    Ha a sablon '/{PAGE}'-re végződik és PAGE==1, automatikusan elhagyjuk a számozást.
    """
    found: List[FoundUrl] = []
    seen: Set[str] = set()
    page = 1
    empty_streak = 0
    in_range_seen = False

    while True:
        if max_pages is not None and page > max_pages:
            break

        # progress
        if page % max(1, progress_every) == 0:
            print(f"[archivum] page={page} fetched={counters['pages_fetched']} links_seen={counters['links_seen']}")

        # 1. oldal: /frisshirek (nem számozott)
        url = page_tmpl.format(PAGE=page)
        if "{PAGE}" in page_tmpl and page == 1 and page_tmpl.rstrip("/").endswith("/{PAGE}"):
            url = page_tmpl.rstrip("/").replace("/{PAGE}", "")

        time.sleep(sleep)
        html = fetch_text(client, url)
        counters["pages_fetched"] += 1
        if not html:
            counters["page_fetch_errors"] += 1
            empty_streak += 1
            if in_range_seen and empty_streak >= 3:
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
                found.append(fu); seen.add(fu.url)
                new_on_page += 1
                if fu.pubdate_guess is not None:
                    in_range_seen = True
            else:
                counters["range_filtered"] += 1

        empty_streak = 0 if new_on_page > 0 else empty_streak + 1
        if in_range_seen and empty_streak >= 3 and start is not None:
            break

        page += 1

    return found


def main():
    ap = argparse.ArgumentParser(description="hvg.hu archívum-crawler (Friss hírek pagináció)")
    ap.add_argument("--config", default="news_sites_hvg.yaml", help="YAML konfig útvonala")
    ap.add_argument("--site", default="hvg.hu", help="Konfig kulcs (alap: hvg.hu)")
    ap.add_argument("--years", type=int, help="Hány évre vissza (alternatíva: --date-from/--date-to)")
    ap.add_argument("--date-from", help="Kezdő dátum (YYYY-MM-DD)")
    ap.add_argument("--date-to", help="Záró dátum (YYYY-MM-DD, kizáró)")
    ap.add_argument("--sleep", type=float, default=DEFAULT_SLEEP, help="Várakozás két kérés között (s)")
    ap.add_argument("--allow-missing-date", action="store_true", help="Ha a linkből nem nyerhető ki dátum, engedjük át")
    ap.add_argument("--max-archivum-pages", type=int, help="Legfeljebb ennyi archív oldal (ha nincs időablak)")
    ap.add_argument("--report-csv", help="CSV export (url,pubdate_guess)")
    ap.add_argument("--sort", choices=["asc","desc"], help="Kimenet rendezés (alap: időablak esetén desc, különben asc)")
    ap.add_argument("--limit", type=int, help="Lista vágása N elemre")
    ap.add_argument("--print", type=int, help="Ennyi rekordot írjunk a konzolra (alap: 30)")
    ap.add_argument("--progress-every", type=int, default=25, help="Haladási naplózás gyakorisága")
    ap.add_argument("--no-force-https", action="store_true")
    args = ap.parse_args()

    # YAML betöltés
    try:
        with open(args.config, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    except Exception as e:
        print(f"Nem tudtam betölteni a konfigot: {e}", file=sys.stderr)
        sys.exit(2)

    if args.site not in cfg:
        print(f"A megadott --site nincs a konfigban: {args.site}", file=sys.stderr)
        sys.exit(2)

    site = cfg[args.site]
    base_url = site.get("base_url") or f"https://{args.site}"
    page_tmpl = site.get("archivum", {}).get("page_template")
    if not page_tmpl:
        print("Hiányzik az archivum.page_template a YAML-ból.", file=sys.stderr)
        sys.exit(2)

    # Regexek – HVG: rovat + YYYYMMDD_ + slug
    article_regex = site.get("article_regex")
    rel_regex = site.get("relative_article_regex")
    if not article_regex:
        article_regex = r"https?://hvg\.hu/(?:[a-z0-9\-]+/)+(20\d{2})([01]\d)([0-3]\d)_[^\s\"'<>]+"
    if not rel_regex:
        rel_regex = r'href=[\'\"]/((?:[a-z0-9\-]+/)+(20\d{2})([01]\d)([0-3]\d)_[^\'\"<>]+)[\'\"]'

    article_re = compile_article_regex(article_regex)
    rel_re = re.compile(rel_regex, re.IGNORECASE)

    # Időablak
    if args.date_from or args.date_to:
        start = parse_iso_date(args.date_from) if args.date_from else None
        end_excl = parse_iso_date(args.date_to) if args.date_to else None
    elif args.years:
        today = date.today()
        end_excl = today + timedelta(days=1)
        start = date(today.year - args.years, today.month, today.day)
    else:
        # alap: 5 év
        today = date.today()
        end_excl = today + timedelta(days=1)
        start = date(today.year - 5, today.month, today.day)

    counters: Dict[str,int] = {
        "pages_fetched": 0,
        "page_fetch_errors": 0,
        "links_seen": 0,
        "dup_links": 0,
        "range_filtered": 0,
    }

    force_https = not args.no_force_https
    default_sort = "desc" if (args.date_from or args.date_to or args.years) else "asc"
    sort_dir = args.sort or default_sort

    with httpx.Client(headers={"User-Agent": UA}, follow_redirects=True, timeout=DEFAULT_TIMEOUT) as client:
        found = crawl_archivum(client, page_tmpl, start=start, end_excl=end_excl,
                               sleep=args.sleep, allow_missing=bool(args.allow_missing_date),
                               counters=counters, progress_every=args.progress_every,
                               max_pages=args.max_archivum_pages, article_re=article_re,
                               rel_re=rel_re, base_url=base_url, force_https=force_https)

    # dedup + rendezés
    uniq: Dict[str, FoundUrl] = {}
    for fu in found:
        key = canonicalize_url(fu.url, force_https=force_https)
        if key not in uniq:
            uniq[key] = FoundUrl(url=key, pubdate_guess=fu.pubdate_guess)
    found_unique = list(uniq.values())

    def sort_key(fu: FoundUrl):
        return (fu.pubdate_guess or date.min, fu.url)

    sorted_list = sorted(found_unique, key=sort_key, reverse=(sort_dir == "desc"))
    with_date = [fu for fu in sorted_list if fu.pubdate_guess is not None]
    without_date = [fu for fu in sorted_list if fu.pubdate_guess is None]
    sorted_list = with_date + without_date

    if args.limit is not None:
        sorted_list = sorted_list[:args.limit]

    # riport
    print("\n=== ÖSSZEGZŐ METRIKÁK ===")
    print(f"pages_fetched                 : {counters['pages_fetched']}")
    print(f"page_fetch_errors             : {counters['page_fetch_errors']}")
    print(f"links_seen                    : {counters['links_seen']}")
    print(f"dup_links                     : {counters['dup_links']}")
    print(f"range_filtered                : {counters['range_filtered']}")
    print(f"elfogadott_url_ossz           : {len(sorted_list)}")

    # CSV
    if args.report_csv:
        with open(args.report_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["url", "pubdate_guess"])
            for fu in sorted_list:
                w.writerow([fu.url, fu.pubdate_guess.isoformat() if fu.pubdate_guess else ""])
        print(f"CSV riport mentve: {args.report_csv}")

    # minta
    to_show = args.print if args.print is not None else min(30, len(sorted_list))
    if to_show:
        print(f"\n— Mintalista ({to_show} elem, rendezés: {sort_dir}) —")
        for fu in sorted_list[:to_show]:
            print(f"{(fu.pubdate_guess.isoformat() if fu.pubdate_guess else '').ljust(12)} {fu.url}")


if __name__ == "__main__":
    main()
