#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
AI alapú kulcsszó- és témacímkézés (NER + topic tagging) cikkekhez.

Ez a modul *csak* az AI-tagging logikáért felel, hogy:

- legyen egy tiszta interfész (TaggingResult, BaseTagger),
- lehessen később bármilyen LLM-et / API-t mögé tenni (OpenAI, LLaMA, stb.),
- és könnyű legyen DB-ben tárolni az eredményt (articles.tags, articles.matched_tags).

Integrációs pontok:
- backfill_sections.py: tartalom backfill után -> tagger.tag_and_update_article(...)
- backfill_domain_batches.py: batch content után -> tagger.bulk_tag_articles_in_window(...)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional, Protocol, Tuple

from ..models import Article
from ..repository import Repository

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Adatszerkezetek: entitások, topicok, tagging eredmény
# ---------------------------------------------------------------------------

@dataclass
class Entity:
    """Felismert entitás NER alapján."""
    text: str
    type: str  # pl. PERSON / ORG / LOC / EVENT / MISC
    salience: float = 1.0  # relatív fontosság (0-1), nem kötelező


@dataclass
class TaggingResult:
    """
    AI tagging eredménye egy cikkre:
      - entities: névvel ellátott szereplők, szervezetek, helyek, események
      - topics: tematikus cimkék (pl. 'gazdaság', 'EU-politika', 'kampány', stb.)
      - keywords: szabad kulcsszavak (rugalmas, kereséshez)
    """
    entities: List[Entity]
    topics: List[str]
    keywords: List[str]

    def to_json_tags(self) -> str:
        """
        Általános, kereséshez használható "tags" mező.
        Javasolt forma: sima string-lista JSON-ben.
        """
        tags: List[str] = []

        # topic címkék
        tags.extend(self.topics)

        # entitásnevek
        tags.extend(e.text for e in self.entities)

        # kulcsszavak
        tags.extend(self.keywords)

        # Duplumok kiszűrése, sorrend megtartásával
        seen = set()
        uniq = []
        for t in tags:
            t = t.strip()
            if not t:
                continue
            if t.lower() in seen:
                continue
            seen.add(t.lower())
            uniq.append(t)

        return json.dumps(uniq, ensure_ascii=False)

    def to_json_matched_tags(self) -> str:
        """
        Strukturáltabb forma, ahol entitás + topic + keyword típus szerint is elérhető.

        Példa JSON:
        {
          "entities": [{"text": "Orbán Viktor", "type": "PERSON", "salience": 0.95}, ...],
          "topics": ["EU-politika", "kampány"],
          "keywords": ["NATO", "szankciók"]
        }
        """
        obj: Dict[str, Any] = {
            "entities": [asdict(e) for e in self.entities],
            "topics": self.topics,
            "keywords": self.keywords,
        }
        return json.dumps(obj, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Tagger interfész: bármilyen AI / szabály alapú rendszer mögé köthető
# ---------------------------------------------------------------------------

class BaseTagger(Protocol):
    """
    Absztrakt tagger interfész.

    Megvalósítási példák:
      - OpenAI alapú LLM tagger (NER + topic + keywords)
      - szabály alapú / kulcsszavas tagger
    """

    def tag_article(self, article: Article) -> TaggingResult:
        """
        Egyetlen cikkre generál AI-alapú címkézést.
        A `article.content` *nem* lehet üres, egyébként érdemes hibát dobni vagy üres eredményt adni.
        """
        ...


# ---------------------------------------------------------------------------
# Dummy / baseline tagger: szabály alapú, hogy legyen működő default
# ---------------------------------------------------------------------------

class SimpleHeuristicTagger:
    """
    Nagyon egyszerű, szabály alapú tagger baseline-ként.

    - kulcsszavakat a cím + tartalom leggyakoribb "hosszabb" szavaiból gyűjt
    - topicokat egyszerű kulcsszólisták alapján tippeli
    - entitásokat most nem próbál NER-rel meghatározni, csak nem-triviális szavakat gyűjt

    Ez *nem* helyettesít egy jó LLM-es megoldást, viszont:
      - nem igényel külső API-t
      - már most is használható debug / demo célokra
    """

    def __init__(self,
                 min_word_len: int = 5,
                 max_keywords: int = 15) -> None:
        self.min_word_len = min_word_len
        self.max_keywords = max_keywords

        # nagyon egyszerű topic kulcsszavak, később LLM veszi át a helyüket
        self.topic_keywords: Dict[str, List[str]] = {
            "gazdaság": ["infláció", "kamat", "adó", "gdp", "forint", "deviza", "válság", "beruházás"],
            "politika": ["választás", "kampány", "ellenzék", "kormány", "parlament", "párt"],
            "külföld": ["usa", "oroszország", "ukrajna", "europa", "eu", "nato", "brit", "francia"],
            "jog / korrupció": ["korrupció", "nyomozás", "bíróság", "ügyészség", "vád", "per"],
        }

    def _extract_keywords(self, text: str) -> List[str]:
        """
        Nyelvfüggetlen, nagyon egyszerű keyword extractor: hosszabb szavak, gyakoriság szerint.
        """
        import re
        words = re.findall(r"\w+", text.lower())
        freq: Dict[str, int] = {}
        for w in words:
            if len(w) < self.min_word_len:
                continue
            if w.isdigit():
                continue
            freq[w] = freq.get(w, 0) + 1

        # rendezés gyakoriság szerint, majd visszaalakítás "eredeti formát" nélkül (minden lower)
        sorted_words = sorted(freq.items(), key=lambda kv: kv[1], reverse=True)
        return [w for (w, _cnt) in sorted_words[: self.max_keywords]]

    def _guess_topics(self, text: str) -> List[str]:
        txt = text.lower()
        topics: List[str] = []
        for topic, kws in self.topic_keywords.items():
            if any(kw in txt for kw in kws):
                topics.append(topic)
        # duplumok kiszűrése
        seen = set()
        uniq = []
        for t in topics:
            if t in seen:
                continue
            seen.add(t)
            uniq.append(t)
        return uniq

    def tag_article(self, article: Article) -> TaggingResult:
        base_text = (article.title or "") + "\n\n" + (article.content or "")
        if not base_text.strip():
            # üres cikkre üres tagging
            return TaggingResult(entities=[], topics=[], keywords=[])

        keywords = self._extract_keywords(base_text)
        topics = self._guess_topics(base_text)

        # tényleges NER nélkül most az entitáslista üres marad
        entities: List[Entity] = []

        return TaggingResult(
            entities=entities,
            topics=topics,
            keywords=keywords,
        )

class HuSpacyNerTopicTagger:
    """
    HuSpaCy alapú NER + a meglévő heurisztikus topic/keyword logika.

    Ingyenes, lokálisan fut, csak a HuSpaCy és a modell kell hozzá:
      pip install huspacy
      pip install "hu_core_news_trf@https://huggingface.co/huspacy/hu_core_news_trf/resolve/main/hu_core_news_trf-any-py3-none-any.whl"
    """

    def __init__(
        self,
        model_name: str = "hu_core_news_trf",
        max_chars: int = 12000,
    ) -> None:
        self.max_chars = max_chars

        try:
            import huspacy  # type: ignore
        except Exception as e:
            raise RuntimeError(
                "HuSpacy használatához telepítsd a 'huspacy' csomagot és a modellt.\n"
                "pl.: pip install huspacy\n"
                "     pip install \"hu_core_news_trf@https://huggingface.co/"
                "huspacy/hu_core_news_trf/resolve/main/hu_core_news_trf-any-py3-none-any.whl\""
            ) from e

        try:
            self._nlp = huspacy.load(model_name)
        except Exception as e:
            raise RuntimeError(
                f"Nem sikerült betölteni a HuSpacy modellt: {model_name}. "
                f"Ellenőrizd, hogy telepítve van-e. Eredeti hiba: {e}"
            ) from e

        # újrahasznosítjuk a heurisztikus topic/keyword logikát
        self._heuristic = SimpleHeuristicTagger()

    def tag_article(self, article: Article) -> TaggingResult:
        # base_text = cím + tartalom
        base_text = (article.title or "") + "\n\n" + (article.content or "")
        if not base_text.strip():
            return TaggingResult(entities=[], topics=[], keywords=[])

        text = base_text
        if len(text) > self.max_chars:
            text = text[: self.max_chars]

        doc = self._nlp(text)

        # --- 1) NER → Entity lista ---
        # HuSpacy label-ek pl.: PER, ORG, LOC, MISC, GPE, stb.
        type_map = {
            "PER": "PERSON",
            "PERSON": "PERSON",
            "ORG": "ORG",
            "LOC": "LOC",
            "GPE": "LOC",
        }

        entities: List[Entity] = []
        for ent in doc.ents:
            text_clean = ent.text.strip()
            if not text_clean:
                continue
            norm_label = type_map.get(ent.label_, "MISC")
            # egyszerűsítés: minden entity salience=1.0 (ha kell, később finomítjuk)
            entities.append(
                Entity(
                    text=text_clean,
                    type=norm_label,
                    salience=1.0,
                )
            )

        # --- 2) Topic + keywords a meglévő heurisztikával ---
        topics = self._heuristic._guess_topics(text)
        keywords = self._heuristic._extract_keywords(text)

        return TaggingResult(
            entities=entities,
            topics=topics,
            keywords=keywords,
        )

# ---------------------------------------------------------------------------
# DB integráció: tags + matched_tags frissítése egy cikkre vagy batchre
# ---------------------------------------------------------------------------

def update_article_tags_in_db(repo: Repository,
                              article_id: str,
                              tagging: TaggingResult) -> None:
    """
    Egyetlen cikk tags + matched_tags mezőinek frissítése az articles táblában.

    - tags: egyszerű string-lista JSON-ben (topicok + entitások + kulcsszavak)
    - matched_tags: strukturált JSON (entities + topics + keywords külön)
    """
    tags_json = tagging.to_json_tags()
    matched_json = tagging.to_json_matched_tags()

    conn = repo.conn
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE articles
        SET tags = ?, matched_tags = ?, updated_at = strftime('%s','now')
        WHERE id = ?
        """,
        (tags_json, matched_json, article_id),
    )
    conn.commit()


def tag_article_and_update(repo: Repository,
                           article: Article,
                           tagger: BaseTagger) -> TaggingResult:
    """
    Convenience függvény:
      1) lefuttatja a tagger.tag_article()-t,
      2) az eredményt beírja a DB-be (articles.tags + matched_tags),
      3) visszaadja a TaggingResult-ot.
    """
    tagging = tagger.tag_article(article)
    if not article.id:
        # Repository.derive_id(article) is használható lenne, de az Article-nak általában már van id-je a DB-ben.
        logger.warning("tag_article_and_update: Article.id hiányzik, nem update-eljük a DB-t. url=%s", article.link)
        return tagging

    update_article_tags_in_db(repo, article.id, tagging)
    return tagging


def bulk_tag_missing_articles(repo: Repository,
                              tagger: BaseTagger,
                              domain: Optional[str] = None,
                              limit: Optional[int] = None,
                              verbose: bool = False) -> int:
    """
    Több cikk batch-szerű taggingje:

    - csak azokat választja, ahol a content NEM üres (már lescrapelt cikk),
    - de a tags IS NULL / üres,
    - opcionálisan domain-szűrővel,
    - opcionálisan limit-tel.

    VISSZAAD: hány cikket címkézett fel.
    """
    conn = repo.conn
    cur = conn.cursor()

    sql = [
        "SELECT a.*",
        "FROM articles a",
        "JOIN sources s ON s.id = a.source_id",
        "WHERE a.content IS NOT NULL AND a.content <> ''",
        "AND (a.tags IS NULL OR a.tags = '')",
    ]
    params: List[Any] = []

    if domain:
        sql.append("AND s.domain = ?")
        params.append(domain)

    sql.append("ORDER BY a.published_date ASC, a.id ASC")
    if limit is not None:
        sql.append("LIMIT ?")
        params.append(limit)

    rows = cur.execute(" ".join(sql), params).fetchall()

    n = 0
    for row in rows:
        art = repo.row_to_article(row)
        try:
            tagging = tagger.tag_article(art)
            update_article_tags_in_db(repo, art.id, tagging)
            n += 1
            if verbose:
                logger.info("Tagged article %s (%s) topics=%s",
                            art.id, art.link, tagging.topics)
        except Exception as e:
            logger.error("Hiba tagging közben: id=%s url=%s err=%s",
                         art.id, art.link, e)

    return n
