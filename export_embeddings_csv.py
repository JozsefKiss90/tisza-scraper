# export_embeddings_table.py
import argparse
import sqlite3
from pathlib import Path
import pandas as pd
import re, html

DB_PATH = "news.sqlite"

TAG_RE = re.compile(r"<[^>]+>")
WS_RE  = re.compile(r"\s+")

def clean_html(text: str) -> str:
    if not text:
        return ""
    # 1) HTML tagek lecsupaszítása
    txt = TAG_RE.sub(" ", text)
    # 2) HTML entitások visszaalakítása (&nbsp; -> space, stb.)
    txt = html.unescape(txt)
    # 3) sortörések, tabok, többszörös szóköz normalizálása
    txt = WS_RE.sub(" ", txt).strip()
    return txt

def fetch_df(conn, label=None, cluster=None, limit=None, order="time"):
    base = """
        SELECT 
            id,
            datetime(ts,'unixepoch') AS date,
            title,
            link,
            label,
            ROUND(label_score,3) AS label_score,
            cluster_id,
            matched_tags,
            source,
            content
        FROM items
    """
    args, where = [], []
    if label:
        where.append("label = ?"); args.append(label)
    if cluster is not None:
        where.append("cluster_id = ?"); args.append(cluster)
    if where:
        base += " WHERE " + " AND ".join(where)
    base += " ORDER BY " + ("label_score DESC" if order == "score" else "ts DESC")
    if limit:
        base += f" LIMIT {limit}"

    rows = conn.execute(base, args).fetchall()
    cols = ["id","date","title","link","label","label_score","cluster_id","matched_tags","source","content"]
    return pd.DataFrame(rows, columns=cols)

def main():
    ap = argparse.ArgumentParser(description="Embeddingelt cikkek táblázatos exportja (CSV/XLSX, HTML-mentesítés)")
    ap.add_argument("--db", default=DB_PATH, help="Adatbázis elérési út")
    ap.add_argument("--export", required=True, help="Célfájl: .csv vagy .xlsx")
    ap.add_argument("--label", choices=["kormánypárti","ellenzéki","semleges"], help="Szűrés címkére")
    ap.add_argument("--cluster", type=int, help="Szűrés klaszter ID-re")
    ap.add_argument("--limit", type=int, help="Sorlimit")
    ap.add_argument("--order", choices=["score","time"], default="time", help="Rendezés")
    ap.add_argument("--delimiter", default=";", help="CSV elválasztó (alapértelmezés: ';' Excelhez)")
    ap.add_argument("--snippet", type=int, default=400, help="Snippet hossza (karakter)")
    ap.add_argument("--strip-html", action="store_true", help="HTML tagek és sortörések eltávolítása (ajánlott)")
    ap.add_argument("--no-content", action="store_true", help="Ne exportálja a teljes content oszlopot, csak a snippetet")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    df = fetch_df(conn, args.label, args.cluster, args.limit, args.order)
    if df.empty:
        print("⚠️ Nincs találat a megadott szűrőkre."); return

    # HTML tisztítás + snippet
    if args.strip_html:
        df["content_clean"] = df["content"].fillna("").map(clean_html)
    else:
        # csak a sortöréseket normalizáljuk, hogy ne nyúljon szét Excelben
        df["content_clean"] = df["content"].fillna("").str.replace(r"\s+", " ", regex=True).str.strip()

    df["snippet"] = df["content_clean"].str.slice(0, args.snippet)

    # oszlop-sorrend
    cols = ["id","date","title","link","label","label_score","cluster_id","matched_tags","source","snippet"]
    if not args.no_content:
        cols += ["content_clean"]  # megtartjuk a tisztított teljes szöveget is
    df = df[cols]

    out = Path(args.export); out.parent.mkdir(parents=True, exist_ok=True)

    if out.suffix.lower() == ".csv":
        df.to_csv(out, index=False, sep=args.delimiter, encoding="utf-8-sig")
        print(f"✅ CSV export kész: {out}  (sep='{args.delimiter}')")
    elif out.suffix.lower() == ".xlsx":
        try:
            import openpyxl  # pip install openpyxl
        except ImportError:
            print("ℹ️  Az .xlsx exporthoz telepítsd: pip install openpyxl")
        # ne hagyjunk sortörést az értékekben, így Excel nem fog „felmagasodni”
        df.to_excel(out, index=False)
        print(f"✅ Excel export kész: {out}")
    else:
        print("❌ Ismeretlen kiterjesztés. Használd .csv vagy .xlsx kiterjesztést.")

if __name__ == "__main__":
    main()
