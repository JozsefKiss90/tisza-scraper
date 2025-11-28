#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Domain backfill több évre visszamenőleg, 30 napos batchekben.

Fő elvek:
- Minden batch:
  1) crawl (meta) -> master DB
  2) content backfill -> master DB (get_or_fetch_article)
  3) az adott 30 nap összes cikkének kimásolása külön batch-DB-be (almappába)
  4) részletes JSON riport mentése
- WAL + integrity_check minden batch végén
- Resume: ha a batch riportja megvan és success=true, skip
- Opcionális master DB backup batchenként

Példák:

# Telex, 1 év, 30 napos batchek, default útvonalak
python -m news_crawler.backfill_domain_batches --domain telex.hu --years 1

# Index, 3 év, 30 nap, saját master DB és kimeneti mappa, részletes loggal
python -m news_crawler.backfill_domain_batches \
    --domain index.hu --years 3 \
    --master-db index_master.sqlite \
    --outdir backfills/index.hu \
    -v

# HVG, 10 év, integritás-ellenőrzés + backup minden batch előtt
python -m news_crawler.backfill_domain_batches \
    --domain hvg.hu --years 10 \
    --backup-master --backup-prefix backups/hvg_master_
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any

# pakettszerű import + fallback (mint a meglévő scriptekben)
try:
    from .core import NewsCrawlerMVP
except Exception:
    here = Path(__file__).resolve()
    src_root = here.parents[1]
    if str(src_root) not in sys.path:
        sys.path.insert(0, str(src_root))
    from news_crawler.core import NewsCrawlerMVP  # type: ignore

import sqlite3


# --------------------------- Segédek ---------------------------

def ensure_dirs(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def set_safe_sqlite_pragmas(conn: sqlite3.Connection) -> None:
    """WAL + foreign_keys + normal sync."""
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA synchronous=NORMAL;")
    except Exception:
        pass


def integrity_ok(db_path: Path) -> bool:
    try:
        conn = sqlite3.connect(str(db_path))
        ok = conn.execute("PRAGMA integrity_check;").fetchone()
        conn.close()
        return bool(ok and ok[0] == "ok")
    except Exception:
        return False


def backup_file(src: Path, prefix: Optional[str] = None) -> Optional[Path]:
    if not src.exists():
        return None
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    if prefix:
        dst = Path(f"{prefix}{ts}.sqlite")
    else:
        dst = src.with_suffix(f".{ts}.bak.sqlite")
    ensure_dirs(dst)
    shutil.copy2(src, dst)
    return dst


def daterange_batches(start: date, end: date, step_days: int = 30) -> List[tuple[date, date]]:
    """Félnyitott intervallumokra bont: [start, end), 30 napos lépésekben."""
    out = []
    cur = start
    delta = timedelta(days=step_days)
    while cur < end:
        nxt = min(cur + delta, end)
        out.append((cur, nxt))
        cur = nxt
    return out


@dataclass
class BatchStats:
    domain: str
    df: str
    dt: str
    master_db: str
    batch_db: str
    started_at: str
    finished_at: Optional[str] = None
    seconds: Optional[float] = None
    crawl_upserts: int = 0
    content_success: int = 0
    content_errors: List[Dict[str, Any]] = None
    copied_to_batch: int = 0
    total_in_master_after: int = 0
    total_in_batch: int = 0
    integrity_master_ok: Optional[bool] = None
    integrity_batch_ok: Optional[bool] = None
    success: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "domain": self.domain,
            "date_from": self.df,
            "date_to": self.dt,
            "master_db": self.master_db,
            "batch_db": self.batch_db,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "seconds": self.seconds,
            "crawl_upserts": self.crawl_upserts,
            "content_success": self.content_success,
            "content_errors": self.content_errors or [],
            "copied_to_batch": self.copied_to_batch,
            "total_in_master_after": self.total_in_master_after,
            "total_in_batch": self.total_in_batch,
            "integrity_master_ok": self.integrity_master_ok,
            "integrity_batch_ok": self.integrity_batch_ok,
            "success": self.success,
        }


# --------------------------- Fő műveletek ---------------------------

def crawl_meta(app: NewsCrawlerMVP, domain: str, df: str, dt: str, verbose: bool) -> int:
    """
    Meta crawl: Pipeline.collect() domainre és dátumtartományra.
    NINCS rovat-szűrés (minden cikk).  :contentReference[oaicite:1]{index=1}
    """
    # Csak a target domain adaptere maradjon
    app.pipeline.adapters = [ad for ad in app.pipeline.adapters if getattr(ad, "domain", None) == domain]
    if not app.pipeline.adapters:
        print(f"[BATCH] Nincs adapter ehhez a domainhez: {domain}")
        return 0

    def _log(art, n):
        if verbose:
            print(f"[CRAWL {n:05d}] {art.source or '—'}  pub={art.published or '—'}  {art.link}")

    inserted = app.pipeline.collect(
        years=None,
        date_from=df,
        date_to=dt,
        predicate=None,   # nincs rovat-szűrés itt
        on_item=_log,
    )
    return int(inserted or 0)


def fill_content(app: NewsCrawlerMVP, domain: str, df: str, dt: str, limit: Optional[int], verbose: bool) -> tuple[int, List[Dict[str, Any]]]:
    """
    Tartalom backfill a megadott ablakra.
    Repository.get_or_fetch_article()-t használjuk, hibákat is gyűjtjük.  :contentReference[oaicite:2]{index=2}
    """
    conn = app.repo.conn
    cur = conn.cursor()
    sql = [
        "SELECT a.url",
        "FROM articles a",
        "JOIN sources s ON s.id = a.source_id",
        "WHERE s.domain = ?",
        "AND (a.content IS NULL OR a.content = '')",
        "AND a.published_date >= ?",
        "AND a.published_date < ?",
        "ORDER BY a.published_date ASC",
    ]
    rows = cur.execute(" ".join(sql), (domain, df, dt)).fetchall()
    if limit is not None:
        rows = rows[:limit]

    ok = 0
    errs: List[Dict[str, Any]] = []
    for i, r in enumerate(rows, 1):
        url = r["url"]
        try:
            art = app.repo.get_or_fetch_article(url, fetcher=app.fetcher)  # :contentReference[oaicite:3]{index=3}
            ok += 1
            if verbose:
                clen = len(art.content or "")
                print(f"[CONTENT {i:05d}] OK len={clen:5d}  {url}")
        except Exception as e:
            errs.append({"url": url, "error": str(e)})
            if verbose:
                print(f"[CONTENT {i:05d}] HIBA {url} -> {e}")
    return ok, errs


def copy_window_to_batch(master: NewsCrawlerMVP, batch: NewsCrawlerMVP, domain: str, df: str, dt: str) -> int:
    """
    Az adott (domain, [df, dt)) ablak összes cikkét (title+content) bemásolja a batch-DB-be.
    Repository.upsert()-tel teszünk át minden sort.  :contentReference[oaicite:4]{index=4}
    """
    mcur = master.repo.conn.cursor()
    rows = mcur.execute(
        """
        SELECT a.*
        FROM articles a
        JOIN sources s ON s.id = a.source_id
        WHERE s.domain = ?
          AND a.published_date >= ?
          AND a.published_date < ?
        ORDER BY a.published_date ASC
        """,
        (domain, df, dt),
    ).fetchall()

    # Row -> Article dataclass -> upsert (Repository kezeli a source_id mappinget is).  :contentReference[oaicite:5]{index=5}
    count = 0
    for row in rows:
        art = master.repo.row_to_article(row)  # title, url, content, published, label...  :contentReference[oaicite:6]{index=6}
        batch.repo.upsert(art)                 # stabil id + domain feloldás  :contentReference[oaicite:7]{index=7}
        count += 1
    return count


def count_domain_total(conn: sqlite3.Connection, domain: str) -> int:
    cur = conn.cursor()
    row = cur.execute(
        """
        SELECT COUNT(*) FROM articles a
        JOIN sources s ON s.id = a.source_id
        WHERE s.domain = ?
        """,
        (domain,),
    ).fetchone()
    return int(row[0]) if row else 0


# --------------------------- CLI & Main ---------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Domain backfill több évre visszamenőleg, 30 napos batchekben.")
    p.add_argument("--domain", required=True, help="pl. telex.hu / index.hu / 444.hu / hvg.hu")
    p.add_argument("--years", type=int, default=1, help="Visszamenőleges évek száma (alapértelmezés: 1)")
    p.add_argument("--batch-days", type=int, default=30, help="Batch ablak hossza napokban (alap: 30)")
    p.add_argument("--master-db", default=None, help="Master DB fájl. Alap: <domain>_master.sqlite")
    p.add_argument("--outdir", default=None, help="Batch DB-k és riportok gyökérmappája. Alap: backfills/<domain>/")
    p.add_argument("--resume", action="store_true", help="Már sikeres riporttal rendelkező batcheket kihagy.")
    p.add_argument("--force", action="store_true", help="A riport meglététől függetlenül újrafuttatja a batch-et.")
    p.add_argument("--backup-master", action="store_true", help="Minden batch előtt backupot készít a master DB-ről.")
    p.add_argument("--backup-prefix", default=None, help="Backup fájl prefix (pl. backups/telex_master_).")
    p.add_argument("--max-articles", type=int, default=None, help="Content backfill max cikk/batch (debug).")
    p.add_argument("-v", "--verbose", action="store_true", help="Részletes log.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    domain = args.domain.lower().lstrip("www.")
    today = date.today()
    end = today + timedelta(days=1)            # félnyitott intervallum vége: holnap
    start = end - timedelta(days=365 * args.years)

    master_db = Path(args.master_db or f"{domain}_master.sqlite").resolve()
    outdir = Path(args.outdir or f"backfills/{domain}").resolve()
    batches_dir = outdir / "batches"
    reports_dir = outdir / "reports"
    batches_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    print(f"[BATCH] domain={domain} years={args.years} range={start}..{end} master_db={master_db}")
    print(f"[BATCH] outdir={outdir}")

    # Ctrl+C barátságos megfogása
    stop_flag = {"stop": False}
    def _sigint(_sig, _frm):  # noqa
        stop_flag["stop"] = True
        print("\n[STOP] Megszakítás kérése érkezett – a batch a jelen lépés végén leáll.")
    signal.signal(signal.SIGINT, _sigint)

    # Master app előkészítése
    master_app = NewsCrawlerMVP(db_path=str(master_db))
    set_safe_sqlite_pragmas(master_app.repo.conn)

    # Batch intervallumok felosztása
    windows = daterange_batches(start, end, step_days=args.batch_days)

    for (df_d, dt_d) in windows:
        if stop_flag["stop"]:
            break

        df = df_d.isoformat()
        dt = dt_d.isoformat()
        tag = f"{df}_to_{dt}"
        batch_db = batches_dir / f"{domain}_{tag}.sqlite"
        report_path = reports_dir / f"{domain}_{tag}.json"

        # Resume/force logika
        if report_path.exists() and not args.force:
            try:
                prev = json.loads(report_path.read_text(encoding="utf-8"))
                if args.resume and prev.get("success"):
                    print(f"[SKIP] {tag} – már sikeres batch (resume).")
                    continue
            except Exception:
                pass

        print(f"\n[RUN] Batch {tag}")

        if args.backup_master and master_db.exists():
            b = backup_file(master_db, prefix=args.backup_prefix)
            if b:
                print(f"[BACKUP] Master backup: {b}")

        stats = BatchStats(
            domain=domain,
            df=df,
            dt=dt,
            master_db=str(master_db),
            batch_db=str(batch_db),
            started_at=datetime.utcnow().isoformat()+"Z",
            content_errors=[],
        )
        t0 = datetime.utcnow()

        # 1) Crawl (meta) -> master
        try:
            ins = crawl_meta(master_app, domain, df, dt, args.verbose)  # :contentReference[oaicite:8]{index=8}
            stats.crawl_upserts = int(ins)
            if args.verbose:
                print(f"[BATCH] crawl meta upserts ~{ins}")
        except Exception as e:
            print(f"[ERROR] crawl_meta: {e}")
            # továbbmegyünk: hátha van már adat a DB-ben

        # 2) Content backfill -> master
        try:
            ok, errs = fill_content(master_app, domain, df, dt, args.max_articles, args.verbose)  # :contentReference[oaicite:9]{index=9}
            stats.content_success = ok
            stats.content_errors = errs
            print(f"[BATCH] content backfill: ok={ok} errs={len(errs)}")
        except Exception as e:
            print(f"[ERROR] fill_content: {e}")

        # Integritás-ellenőrzés a masteren
        stats.integrity_master_ok = integrity_ok(master_db)
        if not stats.integrity_master_ok:
            print("[WARN] Master DB integrity_check != ok – érdemes azonnal megvizsgálni.")
            # Nem állunk meg automatikusan: a riport jelzi a problémát.

        # 3) Batch DB: bemásoljuk az ablak összes cikkét
        batch_app = NewsCrawlerMVP(db_path=str(batch_db))
        set_safe_sqlite_pragmas(batch_app.repo.conn)
        try:
            copied = copy_window_to_batch(master_app, batch_app, domain, df, dt)  # :contentReference[oaicite:10]{index=10}
            stats.copied_to_batch = copied
        except Exception as e:
            print(f"[ERROR] copy_window_to_batch: {e}")

        # Integritás-ellenőrzés a batchen
        stats.integrity_batch_ok = integrity_ok(batch_db)

        # 4) Összegző számok
        stats.total_in_master_after = count_domain_total(master_app.repo.conn, domain)
        stats.total_in_batch = count_domain_total(batch_app.repo.conn, domain)

        # lezárás
        t1 = datetime.utcnow()
        stats.finished_at = t1.isoformat()+"Z"
        stats.seconds = (t1 - t0).total_seconds()
        stats.success = bool(stats.integrity_master_ok and stats.integrity_batch_ok)

        # 5) Riport mentése
        ensure_dirs(report_path)
        report_path.write_text(json.dumps(stats.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[REPORT] {report_path}  success={stats.success}")

        # Takarítás
        try:
            batch_app.repo.close()
        except Exception:
            pass

    # Master zárása
    try:
        master_app.repo.close()
    except Exception:
        pass

    print("\n[DONE] Batches kész.")
    

if __name__ == "__main__":
    main()


'''
Használat – tipikus parancsok

Telex, 1 év (default beállításokkal):

python -m news_crawler.backfill_domain_batches --domain telex.hu --years 1 -v


Index, 3 év, saját master DB + saját kimeneti mappa, backup-pal:

python -m news_crawler.backfill_domain_batches \
  --domain index.hu \
  --years 3 \
  --master-db index_master.sqlite \
  --outdir backfills/index.hu \
  --backup-master --backup-prefix backups/index_master_ \
  -v


Folytatás (resume): ha korábban már végigment részenként, csak a hiányzó/sikertelen batcheket futtatja:

python -m news_crawler.backfill_domain_batches --domain hvg.hu --years 10 --resume


Újrafuttatás kényszerítve:

python -m news_crawler.backfill_domain_batches --domain telex.hu --years 1 --force
'''