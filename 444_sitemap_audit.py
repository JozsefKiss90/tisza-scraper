#!/usr/bin/env python3
# 444_sitemap_audit.py
# Egydomaines (alapértelmezett: 444.hu) sitemap-audit és részletes konzolos riport.
# - Rekurzív bejárás sitemapindex -> (sitemapindex)* -> urlset
# - Dátumszűrés: --years vagy --date-from/--date-to (UTC)
# - lastmod hiánya kezelhető: --allow-missing-lastmod
# - Domain-szűrés: csak a megadott domain(ek) URL-jei
# - Nem-cikk útvonalak szűrése: --non-article opcióval (többször adható)
# - Limit per sitemap: --limit-per-sitemap
# - Részletes számlálók az összes kiesési okról
# - Kimenet: konzolos táblázat + opcionális CSV (--report-csv)

import argparse
import csv
import gzip
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Iterator, List, Optional, Tuple
from urllib.parse import urlparse, urlsplit, urlunsplit
import xml.etree.ElementTree as ET

try:
    import httpx
except Exception as e:
    print("A futtatáshoz szükséges a 'httpx' csomag: pip install httpx", file=sys.stderr)
    raise

UA = "SitemapAudit/1.0 (+https://example.org)"
DEFAULT_TIMEOUT = 20
DEFAULT_SLEEP = 0.2

@dataclass
class UrlEntry:
    url: str
    lastmod_raw: Optional[str]
    lastmod_ts: Optional[int]

def parse_date_iso(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    s2 = s.strip()
    try:
        dt = datetime.fromisoformat(s2.replace("Z", "+00:00"))
        return int(dt.astimezone(timezone.utc).timestamp())
    except Exception:
        pass
    try:
        # csak YYYY-MM-DD
        dt = datetime.strptime(s2[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None

def fetch_xml(client: httpx.Client, url: str) -> bytes:
    r = client.get(url, headers={"User-Agent": UA}, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    content = r.content
    if url.endswith(".gz"):
        try:
            content = gzip.decompress(content)
        except Exception:
            # lehet már kitömörítve jött
            pass
    return content

def iter_sitemap_nodes(xml_bytes: bytes) -> Iterator[Tuple[str, str, Optional[str]]]:
    """
    ('sitemap'|'url', loc, lastmod_raw)
    """
    root = ET.fromstring(xml_bytes)
    tag = root.tag.lower()
    if tag.endswith("sitemapindex"):
        for sm in root.findall(".//{*}sitemap"):
            loc = sm.find("{*}loc")
            lastmod = sm.find("{*}lastmod")
            yield ("sitemap", (loc.text.strip() if loc is not None else ""), lastmod.text.strip() if lastmod is not None else None)
    elif tag.endswith("urlset"):
        for u in root.findall(".//{*}url"):
            loc = u.find("{*}loc")
            lastmod = u.find("{*}lastmod")
            yield ("url", (loc.text.strip() if loc is not None else ""), lastmod.text.strip() if lastmod is not None else None)
    else:
        # namespace-naiv fallback
        for node in root.findall(".//sitemap"):
            loc = node.find("loc")
            lastmod = node.find("lastmod")
            yield ("sitemap", loc.text.strip() if loc is not None else "", lastmod.text.strip() if lastmod is not None else None)
        for node in root.findall(".//url"):
            loc = node.find("loc")
            lastmod = node.find("lastmod")
            yield ("url", loc.text.strip() if loc is not None else "", lastmod.text.strip() if lastmod is not None else None)

def discover_sitemaps_via_robots(client: httpx.Client, netloc: str) -> List[str]:
    """
    Visszaadja a robots.txt 'Sitemap:' sorokból talált URL-eket (https preferenciával).
    """
    robots_https = urlunsplit(("https", netloc, "/robots.txt", "", ""))
    robots_http  = urlunsplit(("http",  netloc, "/robots.txt", "", ""))
    found: List[str] = []
    for robots in (robots_https, robots_http):
        try:
            r = client.get(robots, headers={"User-Agent": UA}, timeout=DEFAULT_TIMEOUT)
            if r.status_code >= 400:
                continue
            for line in r.text.splitlines():
                if line.lower().startswith("sitemap:"):
                    url = line.split(":", 1)[1].strip()
                    found.append(url)
        except Exception:
            continue
    # duplikátum-eltávolítás https normalizálással
    out, seen = [], set()
    for u in found:
        u2 = u.replace("http://", "https://")
        if u2 not in seen:
            out.append(u2)
            seen.add(u2)
    return out

def allowed_domain(url: str, allowed_suffixes: List[str]) -> bool:
    if not allowed_suffixes:
        return True
    host = urlparse(url).netloc.lower()
    return any(host.endswith(suf) for suf in allowed_suffixes)

def walk_sitemap(client: httpx.Client, url: str, *, max_depth: int, sleep_sec: float, counters: Counter) -> Iterator[Tuple[str, Optional[str]]]:
    """
    Rekurzív bejárás: url -> xml -> (sitemap|url) -> yield url-ek lastmod_raw-val
    """
    stack = [(url, 0)]
    while stack:
        current, depth = stack.pop()
        try:
            time.sleep(sleep_sec)
            xml_bytes = fetch_xml(client, current)
        except Exception as e:
            counters["sitemap_fetch_errors"] += 1
            print(f"⚠️  Sitemap letöltési hiba: {current} ({e})")
            continue

        try:
            nodes = list(iter_sitemap_nodes(xml_bytes))
        except Exception as e:
            counters["sitemap_parse_errors"] += 1
            print(f"⚠️  XML parse hiba: {current} ({e})")
            continue

        # külön számoljuk az index és urlset bejegyzéseket
        if nodes and nodes[0][0] == "sitemap":
            counters["sitemapindex_entries"] += len([1 for k,_,_ in nodes if k=="sitemap"])
        if nodes and nodes[0][0] == "url":
            counters["urlset_entries"] += len([1 for k,_,_ in nodes if k=="url"])

        for kind, loc, lastmod_raw in nodes:
            if kind == "sitemap":
                if depth >= max_depth:
                    counters["sitemapindex_maxdepth_skipped"] += 1
                    continue
                stack.append((loc, depth + 1))
            else:
                yield (loc, lastmod_raw)

def main():
    ap = argparse.ArgumentParser(description="Egydomaines sitemap-audit és részletes riport (alap: 444.hu)")
    ap.add_argument("--domain", default="444.hu", help="Cél domain (alapértelmezett: 444.hu)")
    ap.add_argument("--start-sitemap", action="append", help="Kezdő sitemap URL (többször is megadható)")
    ap.add_argument("--years", type=int, help="Hány évre visszamenőleg kérjük az URL-eket (alternatíva a --date-from/--date-to-hoz)")
    ap.add_argument("--date-from", help="Kezdő dátum (YYYY-MM-DD)")
    ap.add_argument("--date-to", help="Záró dátum (YYYY-MM-DD, kizáró)")
    ap.add_argument("--allow-missing-lastmod", action="store_true", help="Ha nincs lastmod, akkor is számoljuk/engedjük át időszűrés mellett")
    ap.add_argument("--non-article", action="append", default=["/tag/", "/author/", "/category/", "/cimke/", "/szerzo/", "/tema/", "/kategoria/", "/rovat/"],
                    help="Nem-cikk URL-minta; többször is megadható (alapértékek adottak)")
    ap.add_argument("--limit-per-sitemap", type=int, help="Legfeljebb ennyi URL-t veszünk figyelembe sitemaponként")
    ap.add_argument("--max-depth", type=int, default=5, help="Rekurzió maximális mélysége sitemapindexekre (alap: 5)")
    ap.add_argument("--sleep", type=float, default=DEFAULT_SLEEP, help="Várakozás két kérés között (s; alap: 0.2)")
    ap.add_argument("--report-csv", help="Ha megadod, ide CSV-t mentünk az elfogadott URL-ekről (url,lastmod_iso)")
    args = ap.parse_args()

    # dátumablak számítása
    ts_from = ts_to = None
    if args.date_from or args.date_to:
        if args.date_from:
            ts_from = int(datetime.fromisoformat(args.date_from).replace(tzinfo=timezone.utc).timestamp())
        if args.date_to:
            ts_to = int(datetime.fromisoformat(args.date_to).replace(tzinfo=timezone.utc).timestamp())
    elif args.years:
        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        ts_to = int(now.timestamp())
        # egyszerű év-levágás; nem kezeli a szökőnapot külön, audit célra elég
        ts_from = int(now.replace(year=now.year - int(args.years)).timestamp())

    allowed_suffixes = [args.domain.lower()]

    counters = Counter()
    reasons = Counter()
    per_sitemap_seen = defaultdict(int)
    accepted: List[UrlEntry] = []

    with httpx.Client(headers={"User-Agent": UA}, follow_redirects=True, timeout=DEFAULT_TIMEOUT) as client:
        start_urls: List[str] = []
        if args.start_sitemap:
            start_urls.extend(args.start_sitemap)

        # robots.txt feltérképezés, ha nem adtak start sitemapot
        if not start_urls:
            robots_sitemaps = discover_sitemaps_via_robots(client, args.domain)
            start_urls.extend(robots_sitemaps)
            counters["robots_sitemaps"] = len(robots_sitemaps)
            if not robots_sitemaps:
                print("⚠️  A robots.txt nem adott vissza sitemapot; adj meg --start-sitemap paramétert!", file=sys.stderr)
                sys.exit(2)

        # Bejárás
        for sm in start_urls:
            print(f"→ Kezdő sitemap: {sm}")
            for loc, lastmod_raw in walk_sitemap(client, sm, max_depth=args.max_depth, sleep_sec=args.sleep, counters=counters):
                counters["urls_total_seen"] += 1
                host_ok = allowed_domain(loc, allowed_suffixes)
                if not host_ok:
                    reasons["other_domain"] += 1
                    continue
                if any(pat in loc for pat in (args.non_article or [])):
                    reasons["non_article_pattern"] += 1
                    continue

                lm_ts = parse_date_iso(lastmod_raw)
                # időszűrés
                if (ts_from is not None or ts_to is not None):
                    if lm_ts is None:
                        if args.allow_missing_lastmod:
                            reasons["missing_lastmod_allowed"] += 1
                            # átengedjük
                        else:
                            reasons["missing_lastmod_skipped"] += 1
                            continue
                    else:
                        if ts_from is not None and lm_ts < ts_from:
                            reasons["range_before_from"] += 1
                            continue
                        if ts_to is not None and lm_ts >= ts_to:
                            reasons["range_after_to"] += 1
                            continue

                # limit per sitemap (hozzávetőlegesen: a 'sm' kulcson számolunk)
                per_sitemap_seen[sm] += 1
                if args.limit_per_sitemap and per_sitemap_seen[sm] > args.limit_per_sitemap:
                    reasons["limit_per_sitemap_skipped"] += 1
                    continue

                accepted.append(UrlEntry(loc, lastmod_raw, lm_ts))

    # Riport

    def fmt_date(ts: Optional[int]) -> str:
        if ts is None:
            return ""
        return datetime.utcfromtimestamp(ts).isoformat() + "Z"

    print("\n=== ÖSSZEGZŐ METRIKÁK ===")
    key_order = [
        "robots_sitemaps",
        "sitemap_fetch_errors",
        "sitemap_parse_errors",
        "sitemapindex_entries",
        "sitemapindex_maxdepth_skipped",
        "urlset_entries",
        "urls_total_seen",
    ]
    for k in key_order:
        print(f"{k:30s}: {counters.get(k, 0)}")

    print("\n— Szűrési okok (kiesések/engedések) —")
    for k in [
        "other_domain",
        "non_article_pattern",
        "missing_lastmod_skipped",
        "missing_lastmod_allowed",
        "range_before_from",
        "range_after_to",
        "limit_per_sitemap_skipped",
    ]:
        if reasons.get(k, 0) or k in ("missing_lastmod_allowed",):
            print(f"{k:30s}: {reasons.get(k, 0)}")

    print(f"\nElfogadott URL-ek száma: {len(accepted)}")

    # Ha kell CSV
    if args.report_csv:
        with open(args.report_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["url", "lastmod_raw", "lastmod_iso"])
            for e in accepted:
                w.writerow([e.url, e.lastmod_raw or "", fmt_date(e.lastmod_ts)])
        print(f"CSV riport mentve: {args.report_csv}")

    # Minta: első 20 elfogadott URL
    show_n = min(20, len(accepted))
    if show_n:
        print("\n— Mintalista (első 20) —")
        for e in accepted[:show_n]:
            print(f"{fmt_date(e.lastmod_ts):25s}  {e.url}")

if __name__ == "__main__":
    main()
