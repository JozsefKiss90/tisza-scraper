# news_crawler/article_reader.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
from typing import List, Tuple, Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup  # type: ignore

# readability opcionális; ha nincs, fallback-olunk
try:
    from readability import Document  # type: ignore  # pip install readability-lxml
except Exception:
    Document = None  # type: ignore

from .fetcher import Fetcher


# --- Domain-specifikus CSS szelektorok a fallback kinyeréshez ---

DOMAIN_SELECTORS = {
    # jó eséllyel betaláló készletek (ha a Readability kevés)
    "telex.hu": [
        "article",
        ".article",
        '[itemprop="articleBody"]',
        ".article__body",
        ".rds-article",
        ".content",
    ],
    "index.hu": [
        "article",
        ".cikk-torzs",
        ".article-body",
        ".content",
        ".post-content",
    ],
    "444.hu": [
        "article",
        '[itemprop="articleBody"]',
        ".articleContent",
        ".post-content",
        ".content",
    ],
    "hvg.hu": [
        "article",
        ".article-content",
        ".article-body",
        ".content",
        ".post-content",
    ],
}


def domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def extract_title_fallback(soup: BeautifulSoup) -> str:
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        return og["content"].strip()
    if soup.title and soup.title.text:
        return soup.title.text.strip()
    h1 = soup.find("h1")
    return h1.get_text(" ", strip=True) if h1 else ""


def clean_text(text: str) -> str:
    # normál szóközök, összeomló whitespace
    text = re.sub(r"\u00A0", " ", text)  # no-break space
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_with_selectors(soup: BeautifulSoup, selectors: List[str]) -> str:
    node = None
    for sel in selectors:
        node = soup.select_one(sel)
        if node:
            break
    node = node or soup.body
    if not node:
        return ""
    parts: List[str] = []
    for p in node.find_all(["p", "li"]):
        t = p.get_text(" ", strip=True)
        if t:
            parts.append(t)
    return "\n".join(parts)


def extract_article(html: str, url: str) -> Tuple[str, str]:
    """
    Közös magfüggvény: HTML + URL -> (title, body).

    Ezt használhatja:
      - a CLI (print_article.py)
      - a backend / Repository (ha már van HTML, de újra akarod parszolni).
    """
    soup = BeautifulSoup(html, "html.parser")
    dom = domain_of(url)

    # 1) Readability, ha elérhető
    if Document is not None:
        try:
            doc = Document(html)
            art_html = doc.summary(html_partial=True)
            art_soup = BeautifulSoup(art_html, "html.parser")
            text = "\n".join(
                p.get_text(" ", strip=True)
                for p in art_soup.find_all(["p", "li"])
            )
            title = (doc.short_title() or "").strip() or extract_title_fallback(soup)
            if len(text) > 300:
                return title, clean_text(text)
        except Exception: 
            # ha bármi elhasal, megyünk tovább CSS-fallbackkel
            pass

    # 2) Domain-specifikus CSS fallback
    selectors = DOMAIN_SELECTORS.get(dom, [])
    text = extract_with_selectors(soup, selectors or ["article", ".content", ".post-content"])
    title = extract_title_fallback(soup)
    return title, clean_text(text)

def read_article(url: str, fetcher: Optional[Fetcher] = None) -> Tuple[str, str]:
    """
    Kényelmi függvény: URL -> (title, body).

    - A CLI is ezt hívja (print_article.py)
    - A backend / Repository is használhatja ugyanígy.
    """
    fetcher = fetcher or Fetcher()
    html = fetcher.get_text(url)
    if not html:
        return "", ""
    return extract_article(html, url)
