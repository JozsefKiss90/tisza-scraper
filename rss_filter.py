import argparse, hashlib, re, sqlite3, time
from pathlib import Path
import feedparser, httpx, trafilatura, yaml
from urllib.parse import urlparse

def load_cfg(p): 
    with open(p, "r", encoding="utf-8") as f: 
        return yaml.safe_load(f)

def init_db(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("""CREATE TABLE IF NOT EXISTS items(
        id TEXT PRIMARY KEY,
        title TEXT,
        link TEXT,
        published TEXT,
        source TEXT,
        content TEXT,
        matched_tags TEXT,
        ts INTEGER
    )""")
    return conn

def canon_id(text): 
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def matches(text, cfg):
    t = (text or "").lower()

    def any_kw(keys): 
        return any(k.lower() in t for k in keys)

    def all_kw(keys):
        return all(k.lower() in t for k in keys)

    if cfg.get("include", {}).get("any") and not any_kw(cfg["include"]["any"]):
        return False, []
    if cfg.get("include", {}).get("all") and not all_kw(cfg["include"]["all"]):
        return False, []
    if cfg.get("exclude", {}).get("any") and any_kw(cfg["exclude"]["any"]):
        return False, []

    tags = []
    for pat in cfg.get("regex", {}).get("any", []):
        if re.search(pat, text or "", flags=re.I):
            tags.append(f"re:{pat}")

    return True, tags

def fetch_article_text(url, timeout=15):
    # próbálkozik: teljes cikk kinyerése (sok RSS csak kivonatot ad)
    try:
        downloaded = trafilatura.fetch_url(url, timeout=timeout)
        if not downloaded:
            return ""
        return trafilatura.extract(downloaded, include_comments=False, include_tables=False) or ""
    except Exception:
        return ""

def process_feed(feed_url, cfg):
    d = feedparser.parse(feed_url)
    out = []
    for e in d.entries:
        title = getattr(e, "title", "")
        link = getattr(e, "link", "")
        summ = getattr(e, "summary", "")
        published = getattr(e, "published", "") or getattr(e, "updated", "")
          # <<< --- IDE jön az új rész:
        cats = []
        if getattr(e, "tags", None):
            cats = [t.get("term", "") for t in e.tags if isinstance(t, dict)]
        base_text = " ".join([title or "", summ or "", " ".join(cats)])
        
        ok, tags = matches(base_text, cfg)
        content = ""

        # ha átment az első szűrőn vagy túl rövid a summary, próbáld a teljes cikket
        if ok or len(summ) < cfg.get("min_length", 0):
            article = fetch_article_text(link)
            if article:
                content = article
                ok2, tags2 = matches(" ".join([base_text, article]), cfg)
                ok = ok and ok2 if cfg.get("include", {}).get("all") else (ok or ok2)
                tags += tags2

        if ok:
            if not allowed_domain(link, cfg):
                continue
            out.append({
                "title": title, "link": link, "published": published,
                "source": feed_url, "content": content or summ, "tags": list(set(tags))
            })
    return out

def save_new(items, conn):
    cur = conn.cursor()
    inserted = 0
    for it in items:
        key = canon_id((it["link"] or "") + "|" + (it["title"] or ""))
        try:
            cur.execute("INSERT INTO items(id,title,link,published,source,content,matched_tags,ts) VALUES(?,?,?,?,?,?,?,?)",
                        (key, it["title"], it["link"], it["published"], it["source"], it["content"], ",".join(it["tags"]), int(time.time())))
            inserted += 1
        except sqlite3.IntegrityError:
            pass  # már megvan
    conn.commit()
    return inserted

def allowed_domain(url, cfg):
    allow = cfg.get("domain_allowlist")
    if not allow: 
        return True
    host = urlparse(url).netloc.lower()
    return any(host.endswith(d) for d in allow)

def main():
    ap = argparse.ArgumentParser(description="Magyar hírszűrő RSS alapon")
    ap.add_argument("--config", "-c", default="config.yaml")
    ap.add_argument("--print", action="store_true", help="Találatok kiírása konzolra")
    args = ap.parse_args()

    cfg = load_cfg(args.config)
    db_path = Path(cfg.get("store_path", "news.sqlite"))
    conn = init_db(db_path)

    total_new = 0
    for f in cfg["feeds"]:
        items = process_feed(f, cfg)
        added = save_new(items, conn)
        total_new += added

        if args.print:
            for it in items:
                print(f"[MATCH] {it['title']}  ({it['link']})  tags={it['tags']}")

    print(f"Kész. Új találatok: {total_new}")

if __name__ == "__main__":
    main()
