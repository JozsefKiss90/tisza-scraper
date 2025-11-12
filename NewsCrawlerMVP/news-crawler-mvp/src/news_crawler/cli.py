from .core import NewsCrawlerMVP
from typing import Optional, List, Dict, Any
from .search import SearchEngine
from .fetcher import Fetcher
# If you keep Query as a dataclass in search module:
from .search import Query

def main():
    app = NewsCrawlerMVP()
    
    # 1) Collect articles from archives
    inserted = app.crawl_all(years=10)
    print(f"Inserted records: ~{inserted}")

    # 2) Optional: Run embedding/tagging/clustering
    # app.reembed()

    # 3) Search for articles
    hits = app.search("Orbán Viktor", limit=25, order="bm25")
    for i, h in enumerate(hits, 1):
        print(f"{i:02d}. [{h.get('date')}] {h.get('title')} – {h.get('link')}")

if __name__ == "__main__":
    main()