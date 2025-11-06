#!/usr/bin/env python3
# multi_archive_crawler.py
# Általános, több domainre konfigurálható archívum-crawler (sitemap nélkül).
# - archivum (listaoldal pagináció)
# - ym (éves/havi)
# - ymd (éves/havi/napi)
# Kimenet: részletes konzolriport + opcionális CSV. Nincs DB-írás.
#
# Újdonságok / javítások:
# - Relatív linkek felismerése és feloldása (urljoin + base_url a YAML-ból)
# - URL-kanonizálás (https kényszer alapból, host kisbetűs, trailing slash levágás)
# - Gazdag HTTP headerek (Accept, Accept-Language, Referer) a kevesebb 403-ért
# - --no-force-https kapcsoló (alap: https-re állítjuk)
# - progress log
# - egységes rendezés/limit a konzol és CSV kimenethez is
# - NameError fix: a no-force-https értéket paraméterként adjuk át a crawler függvényeknek
#
import argparse
import csv
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from typing import Dict, Iterable, Iterator, List, Optional, Set, Tuple
from urllib.parse import urlunsplit, urljoin, urlparse, urlunparse

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

UA = "MultiArchiveCrawler/1.1 (+https://example.org)"
DEFAULT_TIMEOUT = 20
DEFAULT_SLEEP = 0.2

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

def daterange_months(start: date, end_excl: date) -> Iterator[Tuple[int,int]]:
    y, m = start.year, start.month
    while True:
        cur = date(y, m, 1)
        if cur >= end_excl:
            break
        yield (y, m)
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
            return None
        ctype = r.headers.get("content-type","").lower()
        if "html" not in ctype and "xml" not in ctype:
            return None
        return r.text
    except Exception:
        return None

def compile_article_regex(pattern: str) -> re.Pattern:
    return re.compile(pattern, re.IGNORECASE)

def canonicalize_url(u: str, *, force_https: bool = True) -> str:
    try:
        pr = urlparse(u)
        scheme = pr.scheme or "https"
        netloc = pr.netloc.lower()
        path = pr.path or "/"
        if force_https:
            scheme = "https"
        # remove trailing slash (except root) – stabilabb duplumszűrés
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/")
        pr2 = pr._replace(scheme=scheme, netloc=netloc, path=path)
        return urlunparse(pr2)
    except Exception:
        return u

def extract_article_links(html: str, article_re: re.Pattern, rel_re: re.Pattern, base_url: str, *, force_https: bool) -> List[FoundUrl]:
    out: List[FoundUrl] = []
    seen: Set[str] = set()
    # Abszolút egyezések
    for m in article_re.finditer(html):
        dt = None
        try:
            y = int(m.group(1)); mo = int(m.group(2))
            try: d = int(m.group(3))
            except Exception: d = None
            if d is not None:
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
            y = int(m.group(1)); mo = int(m.group(2))
            try: d = int(m.group(3))
            except Exception: d = None
            if d is not None:
                dt = date(y, mo, d)
        except Exception:
            dt = None
        rel = m.group(0)
        # href="..."
        rel = re.sub(r'^href=\\"', '', rel).strip('"')
        url = urljoin(base_url.rstrip('/') + '/', rel)
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

def crawl_archivum(client: httpx.Client, page_tmpl: str, *, start: Optional[date], end_excl: Optional[date], sleep: float, allow_missing: bool, max_pages: Optional[int], counters: Dict[str,int], progress_every: int, article_re: re.Pattern, rel_re: re.Pattern, base_url: str, force_https: bool) -> List[FoundUrl]:
    found: List[FoundUrl] = []
    seen: Set[str] = set()
    page = 1
    empty_streak = 0
    while True:
        if max_pages is not None and page > max_pages:
            break
        if page % progress_every == 0:
            print(f"[archivum] page={page} fetched={counters['pages_fetched']} links_seen={counters['links_seen']}")
        url = page_tmpl.format(PAGE=page)
        time.sleep(sleep)
        html = fetch_text(client, url)
        counters["pages_fetched"] += 1
        if not html:
            counters["page_fetch_errors"] += 1
            empty_streak += 1
            if empty_streak >= 2:
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
                found.append(fu)
                seen.add(fu.url)
                new_on_page += 1
            else:
                counters["range_filtered"] += 1
        empty_streak = 0 if new_on_page > 0 else empty_streak + 1
        if empty_streak >= 2 and start is not None:
            break
        page += 1
    return found

def crawl_ym(client: httpx.Client, tmpl: str, *, start: date, end_excl: date, sleep: float, allow_missing: bool, counters: Dict[str,int], progress_every: int, reverse: bool, article_re: re.Pattern, rel_re: re.Pattern, base_url: str, force_https: bool) -> List[FoundUrl]:
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
        html = fetch_text(client, url)
        counters["pages_fetched"] += 1
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
                found.append(fu)
                seen.add(fu.url)
            else:
                counters["range_filtered"] += 1
    return found

def crawl_ymd(client: httpx.Client, tmpl: str, *, start: date, end_excl: date, sleep: float, allow_missing: bool, counters: Dict[str,int], progress_every: int, reverse: bool, max_days: Optional[int], article_re: re.Pattern, rel_re: re.Pattern, base_url: str, force_https: bool) -> List[FoundUrl]:
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
        html = fetch_text(client, url)
        counters["pages_fetched"] += 1
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
                found.append(fu)
                seen.add(fu.url)
            else:
                counters["range_filtered"] += 1
    return found

def main():
    ap = argparse.ArgumentParser(description="Többdomaines archívum-crawler (YAML konfiggal)")
    ap.add_argument("--config", default="news_sites.yaml", help="YAML konfig útvonala")
    ap.add_argument("--site", required=True, help="Domain kulcs a konfigból (pl. 444.hu, telex.hu)")
    ap.add_argument("--years", type=int, help="Hány évre visszamenőleg (alternatíva: --date-from/--date-to)")
    ap.add_argument("--date-from", help="Kezdő dátum (YYYY-MM-DD)")
    ap.add_argument("--date-to", help="Záró dátum (YYYY-MM-DD, kizáró)")
    ap.add_argument("--mode", choices=["auto","archivum","ym","ymd"], default="auto", help="Bejárási mód")
    ap.add_argument("--sleep", type=float, default=DEFAULT_SLEEP, help="Várakozás két kérés között (s)")
    ap.add_argument("--allow-missing-date", action="store_true", help="Ha a linkből nem nyerhető ki dátum, engedjük át")
    ap.add_argument("--max-archivum-pages", type=int, help="archivum mód: ennyi oldal után megállunk (ha nincs időablak)")
    ap.add_argument("--max-days", type=int, help="ymd mód: legfeljebb ennyi napot dolgozzunk fel")
    ap.add_argument("--report-csv", help="CSV export elérési út (url,pubdate_guess)")
    ap.add_argument("--sort", choices=["asc","desc"], help="Rendezés (alap: ha időablak van -> desc, különben asc)")
    ap.add_argument("--limit", type=int, help="Összesített lista vágása N elemre")
    ap.add_argument("--print", type=int, help="Ennyi rekordot írjunk a konzolra (alap: 30)")
    ap.add_argument("--progress-every", type=int, default=50, help="Haladási naplózás gyakorisága")
    ap.add_argument("--no-force-https", action="store_true", help="Ne kényszerítsük https-re az URL-eket (alap: https-t használunk)")
    args = ap.parse_args()

    # konfig betöltés
    try:
        with open(args.config, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    except Exception as e:
        print(f"Nem tudtam betölteni a konfigot: {e}", file=sys.stderr)
        sys.exit(2)

    if args.site not in cfg:
        print(f"A megadott --site nincs a konfigban: {args.site}", file=sys.stderr)
        print(f"Elérhető site-ok: {', '.join(cfg.keys())}", file=sys.stderr)
        sys.exit(2)

    site = cfg[args.site]
    article_regex = site.get("article_regex")
    rel_regex = site.get("relative_article_regex")
    base_url = site.get("base_url")

    if not article_regex:
        print("Hiányzik az article_regex a site-konfigból.", file=sys.stderr)
        sys.exit(2)
    if not rel_regex:
        # generikus relatív minta: opcionális rovat, majd YYYY/MM/DD/slug
        rel_regex = r'href=\"/(?:[a-z0-9\-]+/)?(20\d{2})/([01]\d)/([0-3]\d)/[^\"]+'
    if not base_url:
        base_url = f"https://{args.site}"

    article_re = compile_article_regex(article_regex)
    rel_re = re.compile(rel_regex, re.IGNORECASE)

    # időablak
    if args.date_from or args.date_to:
        start = parse_iso_date(args.date_from) if args.date_from else None
        end_excl = parse_iso_date(args.date_to) if args.date_to else None
    elif args.years:
        today = date.today()
        end_excl = today + timedelta(days=1)
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

    force_https = (not args.no_force_https)
    default_sort = "desc" if (args.date_from or args.date_to or args.years) else "asc"
    reverse_iter = True if (args.sort or default_sort) == "desc" else False

    found: List[FoundUrl] = []
    with httpx.Client(headers={"User-Agent": UA}, follow_redirects=True, timeout=DEFAULT_TIMEOUT) as client:
        if args.mode == "archivum" or args.mode == "auto":
            page_tmpl = site.get("archivum", {}).get("page_template")
            if page_tmpl:
                res = crawl_archivum(client, page_tmpl, start=start, end_excl=end_excl, sleep=args.sleep,
                                     allow_missing=args.allow_missing_date, max_pages=args.max_archivum_pages,
                                     counters=counters, progress_every=args.progress_every, article_re=article_re,
                                     rel_re=rel_re, base_url=base_url, force_https=force_https)
                found.extend(res)
            if args.mode == "auto" and len(found) < 200:
                ym_tmpl = site.get("ym", {}).get("template")
                if ym_tmpl:
                    res2 = crawl_ym(client, ym_tmpl, start=start or date(2013,1,1), end_excl=end_excl or (date.today()+timedelta(days=1)),
                                    sleep=args.sleep, allow_missing=args.allow_missing_date, counters=counters,
                                    progress_every=args.progress_every, reverse=reverse_iter, article_re=article_re,
                                    rel_re=rel_re, base_url=base_url, force_https=force_https)
                    found.extend(res2)
        elif args.mode == "ym":
            ym_tmpl = site.get("ym", {}).get("template")
            if not ym_tmpl:
                print("Hiányzik az ym.template a site-konfigból.", file=sys.stderr)
                sys.exit(2)
            found = crawl_ym(client, ym_tmpl, start=start or date(2013,1,1), end_excl=end_excl or (date.today()+timedelta(days=1)),
                             sleep=args.sleep, allow_missing=args.allow_missing_date, counters=counters,
                             progress_every=args.progress_every, reverse=reverse_iter, article_re=article_re,
                             rel_re=rel_re, base_url=base_url, force_https=force_https)
        elif args.mode == "ymd":
            ymd_tmpl = site.get("ymd", {}).get("template")
            if not ymd_tmpl:
                print("Hiányzik az ymd.template a site-konfigból.", file=sys.stderr)
                sys.exit(2)
            found = crawl_ymd(client, ymd_tmpl, start=start or date(2013,1,1), end_excl=end_excl or (date.today()+timedelta(days=1)),
                              sleep=args.sleep, allow_missing=args.allow_missing_date, counters=counters,
                              progress_every=args.progress_every, reverse=reverse_iter, max_days=args.max_days,
                              article_re=article_re, rel_re=rel_re, base_url=base_url, force_https=force_https)

    # deduplikálás (kanonizált URL-ek)
    uniq: Dict[str, FoundUrl] = {}
    for fu in found:
        key = canonicalize_url(fu.url, force_https=force_https)
        if key not in uniq:
            uniq[key] = FoundUrl(url=key, pubdate_guess=fu.pubdate_guess)
    found_unique = list(uniq.values())

    # riport
    print("\n=== ÖSSZEGZŐ METRIKÁK ===")
    print(f"pages_fetched                 : {counters['pages_fetched']}")
    print(f"page_fetch_errors             : {counters['page_fetch_errors']}")
    print(f"links_seen                    : {counters['links_seen']}")
    print(f"dup_links                     : {counters['dup_links']}")
    print(f"range_filtered                : {counters['range_filtered']}")
    print(f"elfogadott_url_ossz           : {len(found_unique)}")

    # rendezés/limit
    def sort_key(fu: FoundUrl):
        return (fu.pubdate_guess or date.min, fu.url)
    sort_dir = args.sort or default_sort
    sorted_list = sorted(found_unique, key=sort_key, reverse=(sort_dir=="desc"))
    with_date = [fu for fu in sorted_list if fu.pubdate_guess is not None]
    without_date = [fu for fu in sorted_list if fu.pubdate_guess is None]
    sorted_list = with_date + without_date
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

    # minta
    to_show = args.print if args.print is not None else min(30, len(sorted_list))
    if to_show:
        print(f"\n— Mintalista ({to_show} elem, rendezés: {sort_dir}) —")
        for fu in sorted_list[:to_show]:
            print(f"{(fu.pubdate_guess.isoformat() if fu.pubdate_guess else '').ljust(12)} {fu.url}")

if __name__ == "__main__":
    main()
