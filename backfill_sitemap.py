# backfill_sitemap.py
# Sitemap-alapú archív backfill magyar hírportálokhoz
# - timestamp alapú dátumszűrés (UTC)
# - robots.txt fallback több sitemap URL-re
# - nem-cikk URL-ek (tag/author/category) kiszűrése
# - stabil cikk-letöltés: httpx -> trafilatura.extract
# - hibák nem állítják le a futást (try/except és continue)

import argparse
import gzip
import hashlib
import sqlite3
import time
from datetime import datetime, timezone
from urllib.parse import urlparse, urlsplit, urlunsplit
from pathlib import Path
import xml.etree.ElementTree as ET

import httpx
import trafilatura
import yaml
print(">>> RUNNING:", __file__)

DB_PATH = "news.sqlite"
UA = "TiszaScraper/1.0 (+https://example.org)"
DEFAULT_TIMEOUT = 20
DEFAULT_SLEEP = 1.0  # másodperc; udvarias rate-limit
MIN_CONTENT_LEN = 120  # rövid szövegeket átugorjuk

# Gyakori nem-cikk oldalak mintái (gyűjtők, szerzők, címkék stb.)
NON_ARTICLE_PATTERNS = (
    "/tag/", "/author/", "/category/", "/cimke/", "/szerzo/",
    "/tema/", "/kategoria/", "/rovat/", "/hirek/cimke/"
)


# --------------------- Segédfüggvények ---------------------

def canon_id(text: str) -> str:
    """Determinista azonosító link + cím alapján."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def parse_date_iso(s: str):
    """ISO dátum (esetleg Z/offset) -> UTC epoch (int). Ha nincs értelmes dátum, None."""
    if not s:
        return None
    try:
        # 2020-01-02T12:34:56+00:00 vagy ...Z
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return int(dt.astimezone(timezone.utc).timestamp())
    except Exception:
        pass
    # 2020-01-02 (offset nélkül → UTC-ként értelmezzük)
    try:
        dt = datetime.strptime(s[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None


def fetch_xml(client: httpx.Client, url: str) -> bytes:
    """Sitemap letöltése; ha .gz, akkor kitömörítés."""
    r = client.get(url, timeout=DEFAULT_TIMEOUT, headers={"User-Agent": UA})
    r.raise_for_status()
    content = r.content
    if url.endswith(".gz"):
        try:
            content = gzip.decompress(content)
        except Exception:
            # néha a szerver már kitömörítve küldi
            pass
    return content


def iter_sitemap_urls(xml_bytes: bytes):
    """
    Bejárja a sitemapot.
    - (<sitemapindex><sitemap><loc>...) -> ('sitemap', loc, lastmod)
    - (<urlset><url><loc>...)          -> ('url', loc, lastmod)
    """
    root = ET.fromstring(xml_bytes)
    tag = root.tag.lower()
    if tag.endswith("sitemapindex"):
        for sm in root.findall(".//{*}sitemap"):
            loc = sm.find("{*}loc")
            lastmod = sm.find("{*}lastmod")
            yield ("sitemap",
                   (loc.text.strip() if loc is not None else ""),
                   lastmod.text.strip() if lastmod is not None else None)
    elif tag.endswith("urlset"):
        for u in root.findall(".//{*}url"):
            loc = u.find("{*}loc")
            lastmod = u.find("{*}lastmod")
            yield ("url",
                   (loc.text.strip() if loc is not None else ""),
                   lastmod.text.strip() if lastmod is not None else None)
    else:
        # namespace nélküli fallback
        for u in root.findall(".//url") + root.findall(".//sitemap"):
            loc = u.find("loc")
            lastmod = u.find("lastmod")
            kind = "url" if u.tag.endswith("url") else "sitemap"
            yield (kind,
                   loc.text.strip() if loc is not None else "",
                   lastmod.text.strip() if lastmod is not None else None)


def allowed_domain(url: str, allowlist):
    if not allowlist:
        return True
    host = urlparse(url).netloc.lower()
    return any(host.endswith(d) for d in allowlist)


def discover_sitemaps_via_robots(client: httpx.Client, any_sitemap_url: str):
    """
    robots.txt-ből összegyűjti a Sitemap: sorokat.
    Mind HTTP, mind HTTPS kísérlet; végül https preferencia.
    """
    parts = urlsplit(any_sitemap_url)
    robots_https = urlunsplit(("https", parts.netloc, "/robots.txt", "", ""))
    robots_http  = urlunsplit(("http",  parts.netloc, "/robots.txt", "", ""))

    candidates = []
    for robots in (robots_https, robots_http):
        try:
            r = client.get(robots, timeout=DEFAULT_TIMEOUT, headers={"User-Agent": UA})
            if r.status_code >= 400:
                continue
            for line in r.text.splitlines():
                if line.lower().startswith("sitemap:"):
                    url = line.split(":", 1)[1].strip()
                    candidates.append(url)
        except Exception:
            pass

    # duplikátumok kiszűrése (https preferencia)
    uniq = []
    seen = set()
    for u in candidates:
        hu = u.replace("http://", "https://")
        if hu not in seen:
            uniq.append(hu)
            seen.add(hu)
    return uniq


def extract_article(url: str) -> str:
    """
    Stabilabb kinyerés:
    1) httpx-szel letöltjük a HTML-t kulturált User-Agenttel
    2) trafilatura.extract csak kinyeri a szöveget (fetch nélkül)
    """
    try:
        with httpx.Client(headers={"User-Agent": UA}, follow_redirects=True, timeout=DEFAULT_TIMEOUT) as c:
            r = c.get(url)
            r.raise_for_status()
            html = r.text
        text = trafilatura.extract(html, include_comments=False, include_tables=False) or ""
        return text.strip()
    except Exception as e:
        print(f"⚠️ Kinyerési hiba: {url} ({e})")
        return ""


def ensure_db(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    # tábla az rss_filter.py sémájához igazodva
    conn.execute("""
        CREATE TABLE IF NOT EXISTS items(
            id TEXT PRIMARY KEY,
            title TEXT,
            link TEXT,
            published TEXT,
            source TEXT,
            content TEXT,
            matched_tags TEXT,
            ts INTEGER
        )
    """)
    conn.commit()
    return conn


def insert_article(conn, title, link, published, source, content, ts=None):
    # DEBUG: futáskor is lásd, tényleg ezt a függvényt hívja-e
    # print(">>> insert_article CALLED FROM:", __file__)
    import hashlib, time
    from datetime import datetime

    key = hashlib.sha256(((link or "") + "|" + (title or "")).encode("utf-8")).hexdigest()
    if ts is None:
        ts = int(datetime.utcnow().timestamp())

    params = {
        "id": key,
        "title": title or "",
        "link": link or "",
        "published": published or "",
        "source": source or "",
        "content": content or "",
        "matched_tags": "",
        "ts": int(ts),
    }

    # DEBUG: ez 8 kell legyen, különben rögtön látod
    # print(">>> insert params len=", len(params), " keys=", list(params.keys()))

    conn.execute(
        """
        INSERT INTO items(
            id, title, link, published, source, content, matched_tags, ts
        ) VALUES (
            :id, :title, :link, :published, :source, :content, :matched_tags, :ts
        )
        """,
        params,
    )
    conn.commit()
    return True


def within_range(lastmod_ts, ts_from, ts_to):
    if lastmod_ts is None:
        return False
    if ts_from and lastmod_ts < ts_from:
        return False
    if ts_to and lastmod_ts >= ts_to:
        return False
    return True


# --------------------- Fő folyamat ---------------------

def backfill(config_path, years=None, date_from=None, date_to=None,
             per_sitemap_limit=None, sleep_sec=DEFAULT_SLEEP):
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    sitemaps = cfg.get("sitemaps", [])
    allow = set(cfg.get("domain_allowlist", []))
    conn = ensure_db(DB_PATH)

    # dátumablak (UTC epoch)
    if date_from:
        ts_from = int(datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc).timestamp())
        ts_to = int(datetime.fromisoformat(date_to).replace(tzinfo=timezone.utc).timestamp()) if date_to else None
    elif years:
        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        ts_to = int(now.timestamp())
        ts_from = int(now.replace(year=now.year - int(years)).timestamp())
    else:
        ts_from = None
        ts_to = None

    total_new = 0
    with httpx.Client(headers={"User-Agent": UA}, follow_redirects=True) as client:
        for sm_url in sitemaps:
            # sitemap index vagy direkt urlset
            try:
                sm_xml = fetch_xml(client, sm_url)
            except Exception as e:
                print(f"⚠️ Nem sikerült letölteni a sitemapet: {sm_url} ({e})")
                # robots.txt fallback: próbáljunk alternatív sitemapokat
                alt_list = discover_sitemaps_via_robots(client, sm_url)
                got = False
                for alt in alt_list:
                    try:
                        sm_xml = fetch_xml(client, alt)
                        print(f"ℹ️ Robots.txt alapján talált alternatív sitemap: {alt}")
                        got = True
                        break
                    except Exception as e2:
                        print(f"⚠️ Alternatív sitemap hiba: {alt} ({e2})")
                if not got:
                    continue

            # Jelöltek gyűjtése (URL-ek)
            bucket_urls = []
            try:
                for kind, loc, lastmod in iter_sitemap_urls(sm_xml):
                    if kind == "sitemap":
                        # fúrjunk le a napi/heti sitemapokra
                        try:
                            time.sleep(sleep_sec)
                            child_xml = fetch_xml(client, loc)
                        except Exception as e:
                            print(f"⚠️ Al-sitemap hiba: {loc} ({e})")
                            continue
                        try:
                            for kind2, loc2, lastmod2 in iter_sitemap_urls(child_xml):
                                if kind2 != "url":
                                    continue
                                if allow and not allowed_domain(loc2, allow):
                                    continue
                                if any(p in loc2 for p in NON_ARTICLE_PATTERNS):
                                    continue
                                lm = parse_date_iso(lastmod2)
                                if (ts_from or ts_to) and not within_range(lm, ts_from, ts_to):
                                    continue
                                bucket_urls.append((loc2, lm))
                                if per_sitemap_limit and len(bucket_urls) >= per_sitemap_limit:
                                    break
                        except Exception as e:
                            print(f"⚠️ Al-sitemap XML-parse hiba: {loc} ({e})")
                            continue
                    else:  # kind == "url"
                        if allow and not allowed_domain(loc, allow):
                            continue
                        if any(p in loc for p in NON_ARTICLE_PATTERNS):
                            continue
                        lm = parse_date_iso(lastmod)
                        if (ts_from or ts_to) and not within_range(lm, ts_from, ts_to):
                            continue
                        bucket_urls.append((loc, lm))
                        if per_sitemap_limit and len(bucket_urls) >= per_sitemap_limit:
                            break
            except Exception as e:
                print(f"⚠️ Fő sitemap XML-parse hiba: {sm_url} ({e})")
                continue

            print(f"ℹ️ {sm_url} — kandidált URL-ek: {len(bucket_urls)}")

            # letöltés/kinyerés/beszúrás
            for url, lm in bucket_urls:
                try:
                    time.sleep(sleep_sec)
                    content = extract_article(url)
                    if not content or len(content) < MIN_CONTENT_LEN:
                        continue

                    # egyszerű cím fallback: URL utolsó szegmense szépen
                    title = url.rstrip("/").split("/")[-1].replace("-", " ").strip().title()

                    ok = insert_article(
                        conn,
                        title=title,
                        link=url,
                        published=(datetime.utcfromtimestamp(lm).isoformat() + "Z" if lm else ""),
                        source=sm_url,
                        content=content,
                        ts=lm
                    )
                    if ok:
                        total_new += 1
                except Exception as e:
                    print(f"⚠️ URL feldolgozási hiba: {url} ({e})")
                    continue

            print(f"✅ {sm_url} — újonnan beszúrt cikkek (összes eddig): {total_new}")

    print(f"🎉 Összesen új cikk: {total_new}. Kész.")
    conn.close()


def main():
    ap = argparse.ArgumentParser(description="Sitemap-alapú archív backfill magyar hírportálokhoz")
    ap.add_argument("--config", "-c", required=True, help="YAML konfig (sitemaps, domain_allowlist)")
    ap.add_argument("--years", type=int, help="Hány évre visszamenőleg (alternatíva a --from/--to-hoz)")
    ap.add_argument("--from", dest="date_from", help="Kezdő dátum (YYYY-MM-DD)")
    ap.add_argument("--to", dest="date_to", help="Záró dátum (YYYY-MM-DD, kizáró)")
    ap.add_argument("--limit", type=int, help="Max. URL / sitemap (throttling/teszt)")
    ap.add_argument("--sleep", type=float, default=DEFAULT_SLEEP, help="Várakozás két kérés között (s)")
    args = ap.parse_args()

    backfill(args.config, years=args.years, date_from=args.date_from, date_to=args.date_to,
             per_sitemap_limit=args.limit, sleep_sec=args.sleep)


if __name__ == "__main__":
    main()
