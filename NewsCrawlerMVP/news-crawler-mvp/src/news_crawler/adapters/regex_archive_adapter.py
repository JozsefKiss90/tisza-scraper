from __future__ import annotations

import abc, re, time, hashlib, os
from datetime import datetime, date, timedelta
from typing import Dict, Iterator, Optional, Tuple, List
from ..models import Article
from ..fetcher import Fetcher

class SourceAdapter(abc.ABC):
    domain: str
    def __init__(self, domain: str, fetcher: Optional[Fetcher] = None) -> None:
        self.domain = domain
        self.fetcher = fetcher or Fetcher()

    @abc.abstractmethod
    def iter_archive(self, years: int = 10, *, date_from: Optional[str] = None, date_to: Optional[str] = None, verbose: bool = False) -> Iterator[Article]:
        ...

    @abc.abstractmethod
    def name(self) -> str: ...

class RegexArchiveAdapter(SourceAdapter):
    """
    article_regex:
        kötelezően (1)=YYYY, (2)=MM, (3)=DD csoportokkal!
    page_templates:
        kulcsok közül bármelyik használható:
          - "archivum": "https://444.hu/archivum?page={PAGE}"
          - "ym":       "https://444.hu/{YYYY}/{MM}"
          - "ymd":      "https://444.hu/{YYYY}/{MM}/{DD}"
    Támogatott környezeti változók:
        CRAWL_MAX_PAGES (int, alap 200), CRAWL_SLEEP (float, s), CRAWL_VERBOSE (1/0)
    """
    def __init__(self, domain: str, article_regex: str, page_templates: Dict[str, str], fetcher: Optional[Fetcher] = None) -> None:
        super().__init__(domain, fetcher)
        self._article_re = re.compile(article_regex, re.IGNORECASE)
        self._pages = page_templates
        self._max_pages = int(os.getenv("CRAWL_MAX_PAGES", "200"))
        self._sleep = float(os.getenv("CRAWL_SLEEP", "0.2"))
        self._ym_max_pages  = int(os.getenv("CRAWL_YM_MAX_PAGES",  "8"))
        self._ymd_max_pages = int(os.getenv("CRAWL_YMD_MAX_PAGES", "8"))

    def name(self) -> str:
        return f"RegexArchiveAdapter<{self.domain}>"

    # --- segédek ---
    def _extract(self, html: str) -> List[Tuple[str, Optional[str]]]:
        out: List[Tuple[str, Optional[str]]] = []
        for m in self._article_re.finditer(html or ""):
            url = m.group(0)
            pub = None
            try:
                y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
                pub = f"{y:04d}-{mo:02d}-{d:02d}"
            except Exception:
                pass
            out.append((url, pub))
        return out

    def _fetch_text(self, url: str) -> Optional[str]:
        time.sleep(self._sleep)
        return self.fetcher.get_text(url)

    def _iter_archivum_urls(self) -> Iterator[str]:
        tmpl = self._pages.get("archivum")
        if not tmpl: return
        page = 1
        while page <= self._max_pages:
            yield tmpl.format(PAGE=page)
            page += 1

    def _iter_paged(self, tmpl: Optional[str], max_pages: int) -> Iterator[str]:
        if not tmpl:
            return
        for p in range(1, max_pages + 1):
            yield tmpl.format(PAGE=p)

    # YM iterátor (eddig csak egy URL/hó volt) – EGÉSZÍTSD KI:
    def _iter_ym_urls(self, start: date, end_excl: date, reverse: bool = True) -> Iterator[str]:
        tmpl = self._pages.get("ym")
        tmpl_paged = self._pages.get("ym_page")  # ÚJ
        if not tmpl and not tmpl_paged:
            return
        months = []
        cur = date(start.year, start.month, 1)
        while cur < end_excl:
            months.append((cur.year, cur.month))
            # next month
            if cur.month == 12:
                cur = date(cur.year + 1, 1, 1)
            else:
                cur = date(cur.year, cur.month + 1, 1)
        if reverse:
            months.reverse()
        for yy, mm in months:
            # 1) alap oldal
            if tmpl:
                yield tmpl.format(YYYY=yy, MM=f"{mm:02d}")
            # 2) lapozott oldalak
            if tmpl_paged:
                for url in self._iter_paged(tmpl_paged.format(YYYY=yy, MM=f"{mm:02d}", PAGE="{PAGE}"), self._ym_max_pages):
                    yield url

    # YMD iterátor – EGÉSZÍTSD KI:
    def _iter_ymd_urls(self, start: date, end_excl: date, reverse: bool = True, max_days: Optional[int] = None) -> Iterator[str]:
        tmpl = self._pages.get("ymd")
        tmpl_paged = self._pages.get("ymd_page")
        if not tmpl and not tmpl_paged:
            return
        days: List[date] = []
        d = start
        while d < end_excl:
            days.append(d)
            d += timedelta(days=1)
        if reverse:
            days.reverse()
        if max_days is not None:
            days = days[:max_days]  # <<< fontos: last-days ablak gyorsítva

        for dd in days:
            # 1) alap napi oldal
            if tmpl:
                yield tmpl.format(YYYY=dd.year, MM=f"{dd.month:02d}", DD=f"{dd.day:02d}")
            # 2) lapozott napi oldalak
            if tmpl_paged:
                for url in self._iter_paged(
                    tmpl_paged.format(YYYY=dd.year, MM=f"{dd.month:02d}", DD=f"{dd.day:02d}", PAGE="{PAGE}"),
                    self._ymd_max_pages
                ):
                    yield url


    def _within_range(self, pub: Optional[str], start: Optional[date], end_excl: Optional[date]) -> bool:
        if pub is None:       # szigorúan: ne engedjük át
            return False
        try:
            y, m, d = map(int, pub.split("-"))
            dt = date(y, m, d)
        except Exception:
            return False
        if start and dt < start: return False
        if end_excl and dt >= end_excl: return False
        return True

    def _yield_matches(self, matches: List[Tuple[str, Optional[str]]], seen: set, start: Optional[date], end_excl: Optional[date]) -> Iterator[Article]:
        for url, pub in matches:
            if url in seen: 
                continue
            if not self._within_range(pub, start, end_excl):
                continue
            seen.add(url)
            stable_id = hashlib.sha256(url.encode("utf-8")).hexdigest()
            yield Article(
                id=stable_id,
                title="",
                link=url,
                published=pub,
                source=self.domain,
                ts=int(time.time()),
            )

    # --- fő bejárás ---
    def iter_archive(self, years: int = 10, *, date_from: Optional[str] = None, date_to: Optional[str] = None, verbose: bool = False) -> Iterator[Article]:
        verbose = verbose or (os.getenv("CRAWL_VERBOSE") == "1")
        start: Optional[date] = datetime.fromisoformat(date_from).date() if date_from else None
        end_excl: Optional[date] = datetime.fromisoformat(date_to).date() if date_to else None
        if start is None and years:
            today = date.today()
            end_excl = end_excl or (today + timedelta(days=1))
            start = date(today.year - years, today.month, today.day)

        if start and not end_excl:
            end_excl = date.today() + timedelta(days=1)
        seen: set[str] = set()
        # 1) archivum pagináció
        if "archivum" in self._pages:
            page_i = 0
            for page_url in self._iter_archivum_urls():
                page_i += 1
                html = self._fetch_text(page_url)
                if not html:
                    if verbose: print(f"[{self.domain}] FAIL {page_i}: {page_url}")
                    continue
                matches = self._extract(html)
                if verbose: print(f"[{self.domain}] page {page_i}: {len(matches)} URLs  {page_url}")
                # korai leállás: ha minden link a start előtt van (és van start)
                if start:
                    all_old = True
                    for _, pub in matches:
                        if self._within_range(pub, start, None):  # csak alsó határ
                            all_old = False; break
                    if all_old and page_i > 1:
                        if verbose: print(f"[{self.domain}] STOP archivum at page {page_i} (< {start.isoformat()})")
                        break
                for art in self._yield_matches(matches, seen, start, end_excl):
                    yield art

        # 2) YM fallback – hónap oldalak (utolsó hónapok → elsőnek)
        if "ym" in self._pages and start and end_excl:
            for ym_url in self._iter_ym_urls(start, end_excl, reverse=True):
                html = self._fetch_text(ym_url)
                if not html:
                    if verbose: print(f"[{self.domain}] YM FAIL: {ym_url}")
                    continue
                matches = self._extract(html)
                if verbose: print(f"[{self.domain}] YM: {len(matches)} URLs  {ym_url}")
                for art in self._yield_matches(matches, seen, start, end_excl):
                    yield art

        # 3) YMD fallback – nap oldalak (kifejezetten “last N days”-hez)
        if "ymd" in self._pages and start and end_excl:
            # 444-hez elég pár nap vissza (gyors): környezeti CRAWL_MAX_DAYS (alap 14)
            max_days = int(os.getenv("CRAWL_MAX_DAYS", "14"))
            for ymd_url in self._iter_ymd_urls(start, end_excl, reverse=True, max_days=max_days):
                html = self._fetch_text(ymd_url)
                if not html:
                    if verbose: print(f"[{self.domain}] YMD FAIL: {ymd_url}")
                    continue
                matches = self._extract(html)
                if verbose: print(f"[{self.domain}] YMD: {len(matches)} URLs  {ymd_url}")
                for art in self._yield_matches(matches, seen, start, end_excl):
                    yield art
