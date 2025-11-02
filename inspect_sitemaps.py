# inspect_sitemaps.py
import argparse
import sqlite3
from pathlib import Path
from datetime import datetime
import pandas as pd
import yaml

DB_DEFAULT = "news.sqlite"

def load_allowlist(config_path: str):
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    allow = cfg.get("domain_allowlist", []) or []
    # dedup + tisztítás
    allow = [d.strip().lower() for d in allow if d and isinstance(d, str)]
    allow = sorted(set(allow))
    return allow

def fetch_domain_df(conn, domain: str, since_ts: int | None, limit: int | None):
    where = ["source LIKE '%sitemap%'", "source LIKE ?"]
    params = [f"%{domain}%"]

    if since_ts is not None:
        where.append("ts >= ?")
        params.append(since_ts)

    sql = f"""
        SELECT
            datetime(ts,'unixepoch') AS date,
            title,
            link,
            source,
            label,
            round(label_score,3) AS label_score,
            cluster_id,
            substr(content,1,500) AS snippet
        FROM items
        WHERE {' AND '.join(where)}
        ORDER BY ts DESC
        {f'LIMIT {int(limit)}' if limit else ''}
    """
    rows = conn.execute(sql, params).fetchall()
    cols = ["date","title","link","source","label","label_score","cluster_id","snippet"]
    return pd.DataFrame(rows, columns=cols)

def sanitize_sheet(name: str) -> str:
    # Excel munkalapnév max 31 karakter és tiltott jelek nélkül
    bad = ['\\', '/', '?', '*', '[', ']', ':']
    for b in bad:
        name = name.replace(b, ' ')
    name = name[:31] or "Sheet"
    return name

def main():
    ap = argparse.ArgumentParser(description="Sitemap-ből származó cikkek exportja domainenként Excelbe")
    ap.add_argument("--db", default=DB_DEFAULT, help="SQLite adatbázis (alapértelmezés: news.sqlite)")
    ap.add_argument("--config", required=True, help="YAML konfig fájl (domain_allowlist szükséges)")
    ap.add_argument("--out", default="export/sitemaps_report.xlsx", help="Kimeneti Excel fájl")
    ap.add_argument("--since", help="Csak ettől a dátumtól (YYYY-MM-DD) újabb rekordok")
    ap.add_argument("--limit", type=int, help="Sorlimit domainenként (opcionális)")
    args = ap.parse_args()

    since_ts = None
    if args.since:
        since_ts = int(datetime.fromisoformat(args.since).timestamp())

    # betöltjük a domain_allowlistet a YAML-ból
    allow = load_allowlist(args.config)
    if not allow:
        print("⚠️ A konfigban nem találtam 'domain_allowlist' listát. Nincs mit exportálni.")
        return

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db)
    all_summ_rows = []

    with pd.ExcelWriter(args.out, engine="openpyxl") as xw:
        for domain in allow:
            df = fetch_domain_df(conn, domain, since_ts, args.limit)
            if df.empty:
                # üres munkalap helyett csak a Summaryban jelezzük
                all_summ_rows.append({"domain": domain, "rows": 0, "latest_date": None})
                continue

            # összefoglalóhoz stat
            latest_date = df["date"].iloc[0] if not df.empty else None
            all_summ_rows.append({"domain": domain, "rows": int(len(df)), "latest_date": latest_date})

            # írás külön munkalapra
            sheet = sanitize_sheet(domain)
            df.to_excel(xw, index=False, sheet_name=sheet)

        # Summary lap
        summ = pd.DataFrame(all_summ_rows, columns=["domain","rows","latest_date"]).sort_values(by="rows", ascending=False)
        summ.to_excel(xw, index=False, sheet_name="Summary")

    conn.close()
    print(f"✅ Kész: {args.out}")

if __name__ == "__main__":
    main()
