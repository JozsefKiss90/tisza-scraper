from typing import List, Optional
import os
from pathlib import Path
from urllib.parse import urlparse  # +++

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel          # ⬅️ EZ HIÁNYZOTT
from .core import NewsCrawlerMVP  # NewsCrawler + Repo + Fetcher + SearchEngine


# ---- Alap DB + domain→DB mapping ----
DB_PATH = os.environ.get("NEWS_DB_PATH", "news.sqlite")
DB_PATH_ABS = Path(DB_PATH).resolve()

# Ide jönnek a domain-specifikus adatbázisok
DOMAIN_DB_MAP = {
    # ha más a fájlnév/útvonal, itt tudod átírni / env-ből felülírni
    "telex.hu": Path(os.environ.get("TELEX_DB_PATH", "telex_30d.sqlite")).resolve(),
    "index.hu": Path(os.environ.get("INDEX_DB_PATH", "index_30d.sqlite")).resolve(),
    "444.hu": Path(os.environ.get("FOURFOURFOUR_DB_PATH", "444_30d.sqlite")).resolve(),
    "hvg.hu": Path(os.environ.get("HVG_DB_PATH", "hvg_30d.sqlite")).resolve(),
}

print(f"[TISZA] Default DB (fallback): {DB_PATH_ABS}")
for dom, p in DOMAIN_DB_MAP.items():
    print(f"[TISZA] Domain DB mapping: {dom} -> {p}")

# -------------------------------------------------
# Globális állapot: több NewsCrawlerMVP instance cache-ben
# -------------------------------------------------
crawler_cache: dict[str, NewsCrawlerMVP] = {}


def normalize_domain(domain: Optional[str]) -> Optional[str]:
    if not domain:
        return None
    d = domain.lower()
    if d.startswith("www."):
        d = d[4:]
    return d


def get_crawler_for_domain(domain: Optional[str]) -> NewsCrawlerMVP:
    """
    Domain alapján eldönti, melyik SQLite fájlt kell használni,
    és ahhoz ad vissza (cache-elt) NewsCrawlerMVP példányt.
    """
    dom = normalize_domain(domain)
    db_path = DOMAIN_DB_MAP.get(dom, DB_PATH_ABS)  # ha nincs spec, a default DB-re esik vissza
    key = str(db_path)
    if key not in crawler_cache:
        print(f"[TISZA] Creating NewsCrawlerMVP for DB: {db_path} (domain={dom or 'DEFAULT'})")
        crawler_cache[key] = NewsCrawlerMVP(db_path=str(db_path))
    return crawler_cache[key]


# -------------------------------------------------
# FastAPI app config
# -------------------------------------------------
app = FastAPI(
    title="Tisza Politikai Hírfigyelő API",
    description="MVP API politikai cikkek kereséséhez és megjelenítéséhez.",
    version="0.1.0",
)

# CORS – hogy később bármilyen frontend (pl. külön React app) is tudja hívni
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # MVP-nél mindent engedünk, később szigorítható
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -------------------------------------------------
# Pydantic modellek (JSON válaszokhoz)
# -------------------------------------------------
class SearchResult(BaseModel):
    title: str
    url: str
    date: str
    label: Optional[str] = None
    label_score: Optional[float] = None
    rank: Optional[float] = None
    cluster_id: Optional[int] = None
    snippet: str


class ArticleResponse(BaseModel):
    title: str
    url: str
    content: str
    published: Optional[str] = None
    source: Optional[str] = None


# -------------------------------------------------
# Healthcheck
# -------------------------------------------------
@app.get("/api/health")
async def health() -> dict:
    return {"status": "ok"}


# -------------------------------------------------
# 1) Keresés CSAK az adatbázisban
# -------------------------------------------------
@app.get("/api/search", response_model=List[SearchResult])
async def search_db(
    q: Optional[str] = Query(
        None,
        description="Opcionális kulcsszó (title/content LIKE) az adatbázisban.",
    ),
    domain: Optional[str] = Query(
        None,
        description="Opcionális domain-szűrő (pl. 'telex.hu', 'index.hu').",
    ),
    date_from: Optional[str] = Query(
        None,
        description="Dátum 'tól' (YYYY-MM-DD).",
    ),
    date_to: Optional[str] = Query(
        None,
        description="Dátum 'ig' (YYYY-MM-DD).",
    ),
    limit: int = Query(200, ge=1, le=1000),
) -> List[SearchResult]:
    """
    Meta-alapú keresés/listázás *csak a már adatbázisban lévő cikkekre*.

    - domain + dátum intervallum opcionális
    - q opcionális kulcsszó (title/content LIKE)
    - NEM indít crawl-t, ha nincs találat.
    """

    # ...
    # ha fordítva vannak, cseréljük fel
    if date_from and date_to and date_from > date_to:
        date_from, date_to = date_to, date_from

    # a domain alapján kiválasztjuk a megfelelő DB-t
    crawler = get_crawler_for_domain(domain)

    rows = crawler.repo.search_by_meta(
        domain=domain,
        date_from=date_from,
        date_to=date_to,
        q=q,
        limit=limit,
    )

    print(
        f"[TISZA] /api/search domain={domain!r} "
        f"from={date_from!r} to={date_to!r} q={q!r} "
        f"limit={limit} -> {len(rows)} rows"
    )

    results: List[SearchResult] = []
    for r in rows:
        results.append(
            SearchResult(
                title=r.get("title") or "",
                url=r.get("link") or "",
                date=str(r.get("date") or ""),
                label=r.get("label"),
                label_score=r.get("label_score"),
                rank=r.get("rank"),
                cluster_id=r.get("cluster_id"),
                snippet=r.get("snippet") or "",
            )
        )
    return results


# -------------------------------------------------
# 2) Dinamikus crawling + scrapelés domain + intervallum alapján
# -------------------------------------------------
@app.get("/api/crawl_range", response_model=List[SearchResult])
async def crawl_range(
    domain: Optional[str] = Query(
        None,
        description="Domain, pl. 'telex.hu'. Ha üres, az összes adapter lefut.",
    ),
    date_from: Optional[str] = Query(
        None,
        description="Dátum 'tól' (YYYY-MM-DD).",
    ),
    date_to: Optional[str] = Query(
        None,
        description="Dátum 'ig' (YYYY-MM-DD).",
    ),
    limit: int = Query(200, ge=1, le=1000),
) -> List[SearchResult]:
    """
    Dinamikus archívum-letöltés domain + dátum intervallum alapján.

      1) Megnézi, van-e már adat a DB-ben.
      2) Ha nincs, lefuttatja a crawl_domain_range()-et.
      3) Utána a DB-ben lévő cikkeket listázza (kulcsszó NÉLKÜL).
    """

    # ha fordítva vannak, cseréljük fel
    if date_from and date_to and date_from > date_to:
        date_from, date_to = date_to, date_from

    # 🔥 ÚJ: domain → a megfelelő SQLite → a megfelelő NewsCrawlerMVP instance
    crawler = get_crawler_for_domain(domain)

    # 1) első kör: csak DB
    rows = crawler.repo.search_by_meta(
        domain=domain,
        date_from=date_from,
        date_to=date_to,
        q=None,
        limit=limit,
    )

    print(
      f"[TISZA] /api/crawl_range (pre-crawl) domain={domain!r} "
      f"from={date_from!r} to={date_to!r} "
      f"limit={limit} -> {len(rows)} rows"
    )
    # 2) ha nincs semmi, crawl
    if not rows:
        try:
            inserted = crawler.crawl_domain_range(
                domain=domain,
                years=10,
                date_from=date_from,
                date_to=date_to,
                verbose=True,
            )
            print(f"[TISZA] crawl_range(domain={domain}, "
                  f"from={date_from}, to={date_to}) -> inserted ~{inserted}")
        except Exception as e:
            print(f"[TISZA] crawl_range ERROR: {e}")

        # újra DB-ből kérdezünk
        rows = crawler.repo.search_by_meta(
            domain=domain,
            date_from=date_from,
            date_to=date_to,
            q=None,
            limit=limit,
        )

    # 3) Eredmények összeállítása
    results: List[SearchResult] = []
    for r in rows:
        results.append(
            SearchResult(
                title=r.get("title") or "",
                url=r.get("link") or "",
                date=str(r.get("date") or ""),
                label=r.get("label"),
                label_score=r.get("label_score"),
                rank=r.get("rank"),
                cluster_id=r.get("cluster_id"),
                snippet=r.get("snippet") or "",
            )
        )

    return results


# -------------------------------------------------
# Egy konkrét cikk lekérése (cache-eléssel)
# -------------------------------------------------
@app.get("/api/article", response_model=ArticleResponse)
async def get_article(url: str = Query(..., description="A cikk URL-je")) -> ArticleResponse:
    """
    Egy konkrét cikk betöltése:

      - ha a megfelelő domain-DB-ben már megvan a content -> onnan jön
      - ha nincs, akkor read_article() + mentés -> az adott domain DB-be menti
    """
    # Domain kinyerése az URL-ből (hogy el tudjuk dönteni, melyik DB-ben van)
    parsed = urlparse(url)
    domain = parsed.netloc  # pl. 'telex.hu' vagy 'index.hu'
    crawler = get_crawler_for_domain(domain)

    try:
        art = crawler.repo.get_or_fetch_article(url, fetcher=crawler.fetcher)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Hiba a cikk beolvasásakor: {e}")

    if not art:
        raise HTTPException(status_code=404, detail="Cikk nem található.")

    return ArticleResponse(
        title=art.title or "",
        url=art.link,
        content=art.content or "",
        published=art.published,
        source=art.source,
    )


# -------------------------------------------------
# Egyszerű UI: egyetlen HTML oldal
# -------------------------------------------------
INDEX_HTML = """
<!DOCTYPE html>
<html lang="hu">
<head>
  <meta charset="utf-8" />
  <title>Tisza – Politikai hírfigyelő MVP</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body {
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      margin: 0;
      padding: 0;
      background: #0f172a;
      color: #e5e7eb;
    }
    header {
      padding: 1rem 1.5rem;
      background: #020617;
      border-bottom: 1px solid #1f2937;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    header h1 {
      margin: 0;
      font-size: 1.2rem;
    }
    header span {
      font-size: 0.8rem;
      color: #9ca3af;
    }
    main {
      display: grid;
      grid-template-columns: minmax(0, 1.2fr) minmax(0, 2fr);
      gap: 1rem;
      padding: 1rem 1.5rem 1.5rem;
      height: calc(100vh - 64px);
      box-sizing: border-box;
    }
    .panel {
      background: #020617;
      border-radius: 0.75rem;
      border: 1px solid #1f2937;
      padding: 0.75rem;
      display: flex;
      flex-direction: column;
      min-height: 0;
    }
    .panel h2 {
      margin: 0 0 0.5rem 0;
      font-size: 0.95rem;
      color: #e5e7eb;
    }
    .search-row {
      display: flex;
      gap: 0.5rem;
      margin-bottom: 0.5rem;
      flex-wrap: wrap;
    }
    input[type="text"], select {
      padding: 0.4rem 0.6rem;
      border-radius: 0.5rem;
      border: 1px solid #374151;
      background: #020617;
      color: #e5e7eb;
      min-width: 0;
    }
    input[type="text"] {
      flex: 1;
    }
    button {
      padding: 0.4rem 0.8rem;
      border-radius: 999px;
      border: none;
      background: #2563eb;
      color: white;
      cursor: pointer;
      font-weight: 500;
      white-space: nowrap;
    }
    button:hover {
      background: #1d4ed8;
    }
    .results {
      flex: 1;
      overflow-y: auto;
      margin-top: 0.25rem;
      border-top: 1px solid #111827;
      padding-top: 0.25rem;
    }
    .result-item {
      padding: 0.4rem 0.3rem;
      border-radius: 0.5rem;
      cursor: pointer;
    }
    .result-item:hover {
      background: #111827;
    }
    .result-title {
      font-size: 0.9rem;
      color: #e5e7eb;
    }
    .result-meta {
      font-size: 0.75rem;
      color: #9ca3af;
      margin-top: 0.1rem;
    }
    .result-snippet {
      font-size: 0.8rem;
      color: #9ca3af;
      margin-top: 0.2rem;
    }
    .article {
      flex: 1;
      overflow-y: auto;
      margin-top: 0.25rem;
      border-top: 1px solid #111827;
      padding-top: 0.25rem;
      font-size: 0.9rem;
      line-height: 1.5;
    }
    .article h2 {
      font-size: 1.1rem;
      margin: 0 0 0.2rem 0;
    }
    .article-meta {
      font-size: 0.75rem;
      color: #9ca3af;
      margin-bottom: 0.75rem;
    }
    .article-body {
      white-space: pre-wrap;
    }
    .badge {
      display: inline-block;
      border-radius: 999px;
      padding: 0.05rem 0.6rem;
      font-size: 0.7rem;
      border: 1px solid #374151;
      color: #9ca3af;
      margin-left: 0.4rem;
    }
    .small {
      font-size: 0.7rem;
      color: #6b7280;
    }
    @media (max-width: 900px) {
      main {
        grid-template-columns: 1fr;
        height: auto;
      }
      .panel {
        height: 60vh;
      }
    }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Tisza – Politikai hírfigyelő</h1>
      <span>MVP · magyar hírportálok · politikai tartalom fókuszban</span>
    </div>
    <div class="small">
      <span>Backend: FastAPI · Adatbázis: SQLite</span>
    </div>
  </header>
  <main>
    <section class="panel">
      <h2>Keresés</h2>
      <div class="search-row">
        <input id="q" type="text" placeholder="Opcionális kulcsszó (pl. 'Orbán Viktor')" 
               onkeydown="if(event.key==='Enter'){doDbSearch();}" />
        <select id="domain">
          <option value="">Minden forrás</option>
          <option value="telex.hu">Telex</option>
          <option value="index.hu">Index</option>
          <option value="444.hu">444</option>
          <option value="hvg.hu">HVG</option>
        </select>
      </div>
      <div class="search-row">
        <input id="dateFrom" type="date" />
        <input id="dateTo" type="date" />
        <button onclick="doDbSearch()">DB keresés</button>
        <button onclick="doCrawlRange()">Intervallum letöltése</button>
      </div>
      <div class="small">
        A "DB keresés" csak a már eltárolt cikkekben keres (kulcsszó opcionális).
        Az "Intervallum letöltése" domain + dátum alapján lefuttatja az archívum-scrape-et (kulcsszó nélkül),
        és elmenti az új cikkeket az adatbázisba.
      </div>
      <div id="resultsInfo" class="small"></div>
      <div id="results" class="results"></div>
    </section>
    <section class="panel">
      <h2>Cikk</h2>
      <div id="article" class="article">
        <p class="small">Válassz egy cikket a bal oldali találati listából.</p>
      </div>
    </section>
  </main>

  <script>
    function renderResults(data) {
      const resultsEl = document.getElementById('results');
      const articleEl = document.getElementById('article');
      const infoEl = document.getElementById('resultsInfo');
      if (!data.length) {
        resultsEl.innerHTML = '<p class="small">Nincs találat.</p>';
        articleEl.innerHTML = '';
        return;
      }

      if (infoEl) infoEl.textContent = 'Találatok: ' + data.length + ' db';

      articleEl.innerHTML = '<p class="small">Válassz egy cikket a találatok közül.</p>';

      resultsEl.innerHTML = '';
      data.forEach((item, idx) => {
        const div = document.createElement('div');
        div.className = 'result-item';
        div.onclick = () => loadArticle(item.url);

        const title = document.createElement('div');
        title.className = 'result-title';
        title.textContent = item.title || '(cím nélküli cikk)';

        const meta = document.createElement('div');
        meta.className = 'result-meta';
        meta.textContent = (item.date || '') + ' · ' + item.url;

        const snip = document.createElement('div');
        snip.className = 'result-snippet';
        snip.textContent = item.snippet || '';

        div.appendChild(title);
        div.appendChild(meta);
        div.appendChild(snip);

        resultsEl.appendChild(div);
      });
    }

    async function doDbSearch() {
      const q = document.getElementById('q').value.trim();
      const domain = document.getElementById('domain').value.trim();
      const dateFrom = document.getElementById('dateFrom').value;
      const dateTo = document.getElementById('dateTo').value;
      const resultsEl = document.getElementById('results');
      const articleEl = document.getElementById('article');
      const infoEl = document.getElementById('resultsInfo');

      resultsEl.innerHTML = '';
      if (infoEl) infoEl.textContent = '';

      resultsEl.innerHTML = '';
      articleEl.innerHTML = '<p class="small">Várakozás a DB-keresés eredményére…</p>';

      const params = new URLSearchParams();
      params.append('limit', '1000');
      if (q) params.append('q', q);
      if (domain) params.append('domain', domain);
      if (dateFrom) params.append('date_from', dateFrom);
      if (dateTo) params.append('date_to', dateTo);

      if (!q && !domain && !dateFrom && !dateTo) {
      //  articleEl.innerHTML = '<p class="small">Adj meg legalább egy kulcsszót vagy szűrőt az adatbázis-kereséshez.</p>';
      //  return;
      }

      try {
        const resp = await fetch('/api/search?' + params.toString());
        if (!resp.ok) {
          resultsEl.innerHTML = '<p class="small">Hiba a DB-keresés során: ' + resp.statusText + '</p>';
          articleEl.innerHTML = '';
          return;
        }
        const data = await resp.json();
        console.log("API /api/search (db) response:", data);
        renderResults(data);
      } catch (err) {
        console.error(err);
        resultsEl.innerHTML = '<p class="small">Váratlan hiba a DB-keresés során.</p>';
        articleEl.innerHTML = '';
      }
    }

    async function doCrawlRange() {
      const domain = document.getElementById('domain').value.trim();
      const dateFrom = document.getElementById('dateFrom').value;
      const dateTo = document.getElementById('dateTo').value;
      const resultsEl = document.getElementById('results');
      const articleEl = document.getElementById('article');

      const infoEl = document.getElementById('resultsInfo');
      resultsEl.innerHTML = '';
      if (infoEl) infoEl.textContent = '';
      articleEl.innerHTML = '<p class="small">Intervallum lekérdezése és archívum letöltése…</p>';

      const params = new URLSearchParams();
      params.append('limit', '1000');
      if (domain) params.append('domain', domain);
      if (dateFrom) params.append('date_from', dateFrom);
      if (dateTo) params.append('date_to', dateTo);

      if (!domain && !dateFrom && !dateTo) {
        articleEl.innerHTML = '<p class="small">Az archívum letöltéséhez adj meg legalább domaint vagy dátum intervallumot.</p>';
        return;
      }

      try {
        const resp = await fetch('/api/crawl_range?' + params.toString());
        if (!resp.ok) {
          resultsEl.innerHTML = '<p class="small">Hiba az intervallum letöltésekor: ' + resp.statusText + '</p>';
          articleEl.innerHTML = '';
          return;
        }
        const data = await resp.json();
        console.log("API /api/crawl_range response:", data);
        renderResults(data);
      } catch (err) {
        console.error(err);
        resultsEl.innerHTML = '<p class="small">Váratlan hiba az intervallum letöltésekor.</p>';
        articleEl.innerHTML = '';
      }
    }

    async function loadArticle(url) {
      const articleEl = document.getElementById('article');
      articleEl.innerHTML = '<p class="small">Cikk betöltése…</p>';

      const params = new URLSearchParams({ url });

      try {
        const resp = await fetch('/api/article?' + params.toString());
        if (!resp.ok) {
          articleEl.innerHTML = '<p class="small">Hiba a cikk betöltésekor: ' + resp.statusText + '</p>';
          return;
        }
        const art = await resp.json();
        articleEl.innerHTML = '';

        const h2 = document.createElement('h2');
        h2.textContent = art.title || '(cím nélküli cikk)';

        const meta = document.createElement('div');
        meta.className = 'article-meta';
        meta.innerHTML = (art.published || '') + ' · ' + (art.source || '') +
          '<br/><span class="small">' + art.url + '</span>';

        const body = document.createElement('div');
        body.className = 'article-body';
        body.textContent = art.content || '[Nincs szöveg]';

        articleEl.appendChild(h2);
        articleEl.appendChild(meta);
        articleEl.appendChild(body);
      } catch (err) {
        console.error(err);
        articleEl.innerHTML = '<p class="small">Váratlan hiba a cikk betöltésekor.</p>';
      }
    }
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    """Egyszerű egyoldalas UI a kereséshez."""
    return HTMLResponse(INDEX_HTML)
