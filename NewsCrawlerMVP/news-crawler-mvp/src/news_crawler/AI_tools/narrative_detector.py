from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, asdict
from typing import List, Dict, Tuple, Optional
from collections import Counter
from datetime import datetime

import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans

from ..repository import Repository


# ---------- Adatstruktúrák ----------


@dataclass
class NarrativeArticle:
    id: int
    url: str
    title: str
    date: str        # 'YYYY-MM-DD'
    entities: List[str]
    topics: List[str]
    keywords: List[str]


@dataclass
class Narrative:
    id: int
    label: str
    description: str
    size: int
    date_from: str
    date_to: str
    top_entities: List[Tuple[str, int]]
    top_topics: List[Tuple[str, int]]
    top_keywords: List[Tuple[str, int]]
    article_ids: List[int]
    example_titles: List[str]  # ÚJ



# ---------- Embedding backend ----------


class SentenceTransformerEmbedder:
    """
    Egyszerű wrapper egy multilingual SentenceTransformer modellre.

    Ajánlott modellek (magyar támogatással):
      - sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2
      - distiluse-base-multilingual-cased-v2
    """

    def __init__(
        self,
        model_name: str = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
    ) -> None:
        self.model = SentenceTransformer(model_name)

    def encode(self, texts: List[str]) -> np.ndarray:
        # normalize_embeddings=True segít stabilabb clusteringben
        emb = self.model.encode(
            texts,
            show_progress_bar=True,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )
        return emb


# ---------- Fő detektor osztály ----------


class NarrativeDetector:
    """
    Full AI narratíva-detektor első MVP-je.

    Lépések:
      1) Cikkek betöltése DB-ből (NER + topics + keywords használata).
      2) Embedding generálása (SentenceTransformer).
      3) KMeans clustering (cluster ~ narratíva).
      4) Cluster summary (top entitások, topicok, kulcsszavak, időablak).
      5) Eredmény JSON reportban.
    """

    def __init__(
        self,
        repo: Repository,
        embedder: Optional[SentenceTransformerEmbedder] = None,
    ) -> None:
        self.repo = repo
        self.embedder = embedder or SentenceTransformerEmbedder()

    # ----- 1) Cikkek betöltése DB-ből -----

    def load_articles(
        self,
        domain: Optional[str],
        date_from: Optional[str],
        date_to: Optional[str],
        limit: Optional[int] = None,
    ) -> List[NarrativeArticle]:
        """
        Betölti azokat a cikkeket, amelyeknél:
          - van content
          - van matched_tags (NER + topic + keyword struktúra)
        """
        cur = self.repo.conn.cursor()

        sql = [
            "SELECT a.id, a.url, a.title, a.content, a.published_date,",
            "       a.matched_tags",
            "FROM articles a",
            "JOIN sources s ON s.id = a.source_id",
            "WHERE a.content IS NOT NULL AND a.content <> ''",
            "  AND a.matched_tags IS NOT NULL AND a.matched_tags <> ''",
        ]

        params: List[object] = []

        if domain:
            sql.append("  AND s.domain = ?")
            params.append(domain)

        if date_from:
            sql.append("  AND a.published_date >= ?")
            params.append(date_from)

        if date_to:
            sql.append("  AND a.published_date <= ?")
            params.append(date_to)

        sql.append("ORDER BY a.published_date ASC, a.id ASC")

        if limit is not None:
            sql.append("LIMIT ?")
            params.append(limit)

        rows = cur.execute(" ".join(sql), params).fetchall()

        articles: List[NarrativeArticle] = []
        for row in rows:
            mt_raw = row["matched_tags"]
            try:
                mt = json.loads(mt_raw) if mt_raw else {}
            except Exception:
                mt = {}

            entities = [e.get("text", "") for e in mt.get("entities", []) if e.get("text")]
            topics = mt.get("topics", []) or []
            keywords = mt.get("keywords", []) or []

            articles.append(
                NarrativeArticle(
                    id=row["id"],
                    url=row["url"],
                    title=row["title"] or "",
                    date=row["published_date"] or "",
                    entities=entities,
                    topics=topics,
                    keywords=keywords,
                )
            )

        return articles

    # ----- 2) Szöveg reprezentáció + embedding -----

    def _build_text_representation(self, art: NarrativeArticle) -> str:
        """
        Egy cikkből olyan stringet csinál, amit érdemes embed-elni.

        - title
        - ENTITIES: Orbán Viktor Magyar Péter ...
        - TOPICS: gazdaság politika ...
        - KEYWORDS: infláció választás ...

        A contentet most direkt kihagyjuk, mert hosszú és drága lenne;
        az NER + topics + keywords már tömörít valamennyit.
        Ha kell, később a content első 500-1000 karakterét is hozzávehetjük.
        """
        parts = [art.title]

        if art.entities:
            parts.append("ENTITIES: " + " ".join(art.entities))

        if art.topics:
            parts.append("TOPICS: " + " ".join(art.topics))

        if art.keywords:
            parts.append("KEYWORDS: " + " ".join(art.keywords[:20]))

        return "\n".join(parts)

    def embed_articles(self, articles: List[NarrativeArticle]) -> np.ndarray:
        texts = [self._build_text_representation(a) for a in articles]
        emb = self.embedder.encode(texts)
        return emb

    # ----- 3) Clustering (KMeans) -----

    def cluster_embeddings(
        self,
        emb: np.ndarray,
        min_cluster_size: int = 3,
        max_clusters: int = 50,
    ) -> np.ndarray:
        """
        KMeans clustering:

          - n_clusters ≈ sqrt(N) vagy max_clusters, ami kisebb
          - min_cluster_size alatt lévő cluster-eket később kidobjuk

        Visszatér: labels (shape: [N]), ahol -1 = zaj / túl kicsi cluster.
        """
        n = emb.shape[0]
        if n == 0:
            return np.array([], dtype=int)
        if n <= min_cluster_size:
            # minden egy cluster, ha nagyon kevés adat van
            return np.zeros(n, dtype=int)

        n_clusters = int(np.sqrt(n))
        n_clusters = max(2, min(max_clusters, n_clusters))

        km = KMeans(
            n_clusters=n_clusters,
            random_state=42,
            n_init="auto",
        )
        labels = km.fit_predict(emb)

        # min_cluster_size alattiak -> zaj (-1)
        counts = Counter(labels)
        labels_filtered = np.array(
            [lab if counts[lab] >= min_cluster_size else -1 for lab in labels],
            dtype=int,
        )
        return labels_filtered

    # ----- 4) Narratívák összeállítása -----

    def build_narratives(
        self,
        articles: List[NarrativeArticle],
        labels: np.ndarray,
    ) -> List[Narrative]:
        assert len(articles) == len(labels)
        n = len(articles)
        if n == 0:
            return []

        clusters: Dict[int, List[NarrativeArticle]] = {}
        for art, lab in zip(articles, labels):
            if lab == -1:
                continue
            clusters.setdefault(lab, []).append(art)

        narratives: List[Narrative] = []
        for cluster_id, arts in clusters.items():
            if not arts:
                continue

            # dátum intervallum
            dates = [a.date for a in arts if a.date]
            dates_sorted = sorted(dates)
            date_from = dates_sorted[0] if dates_sorted else ""
            date_to = dates_sorted[-1] if dates_sorted else ""

            # top entitások / topicok / keywords
            ent_counter = Counter()
            topic_counter = Counter()
            kw_counter = Counter()

            for a in arts:
                ent_counter.update(a.entities)
                topic_counter.update(a.topics)
                kw_counter.update(a.keywords)

            top_entities = ent_counter.most_common(10)
            top_topics = topic_counter.most_common(10)
            top_keywords = kw_counter.most_common(15)

            # címke generálása
            label = self._build_narrative_label(top_entities, top_topics, arts)
            # címek mint példák (max 3)
            example_titles = [a.title for a in arts[:3]]

            description = self._build_narrative_description(
                label, len(arts), date_from, date_to, top_entities, top_topics, example_titles
            )

            narratives.append(
                Narrative(
                    id=cluster_id,
                    label=label,
                    description=description,
                    size=len(arts),
                    date_from=date_from,
                    date_to=date_to,
                    top_entities=top_entities,
                    top_topics=top_topics,
                    top_keywords=top_keywords,
                    article_ids=[a.id for a in arts],
                    example_titles=example_titles,  # ÚJ
                )
            )

        # nagyobb clusterek előre
        narratives.sort(key=lambda n: n.size, reverse=True)
        return narratives

    def _build_narrative_label(
        self,
        top_entities: List[Tuple[str, int]],
        top_topics: List[Tuple[str, int]],
        arts: List[NarrativeArticle],
    ) -> str:
        if top_entities and top_topics:
            main_ent = top_entities[0][0]
            second_ent = top_entities[1][0] if len(top_entities) > 1 else None
            main_topic = top_topics[0][0]

            if second_ent:
                return f"{main_ent} és {second_ent} – {main_topic} narratíva"
            return f"{main_ent} – {main_topic} narratíva"

        if top_entities:
            ents = [e for e, _ in top_entities[:2]]
            return f"{', '.join(ents)} narratíva"

        if top_topics:
            topics = [t for t, _ in top_topics[:2]]
            return f"{' / '.join(topics)} narratíva"

        if arts:
            return arts[0].title[:80]

        return "Ismeretlen narratíva"

    def _build_narrative_description(
        self,
        label: str,
        size: int,
        date_from: str,
        date_to: str,
        top_entities: List[Tuple[str, int]],
        top_topics: List[Tuple[str, int]],
        example_titles: List[str],
    ) -> str:
        ents = [e for e, _ in top_entities[:3]]
        topics = [t for t, _ in top_topics[:3]]

        ents_str = ", ".join(ents) if ents else "nincs kiemelt szereplő"
        topics_str = ", ".join(topics) if topics else "nincs domináns téma"

        if date_from and date_to and date_from != date_to:
            date_span = f"{date_from} és {date_to} között"
        elif date_from:
            date_span = f"{date_from}-án/én"
        else:
            date_span = "ismeretlen időszakban"

        examples_part = ""
        if example_titles:
            # max 2 címet emeljünk ki
            ex = " — ".join(example_titles[:2])
            examples_part = f" Példa cikkek: {ex}."

        return (
            f"{label}: {size} cikk {date_span}. "
            f"Kiemelt szereplők: {ents_str}. Fő témák: {topics_str}.{examples_part}"
        )

# ---------- CLI ----------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Full AI narratíva-detektor (embedding + clustering) SQLite DB-ből."
    )
    p.add_argument("--db", required=True, help="SQLite DB fájl (pl. index_30d.sqlite)")
    p.add_argument("--domain", default=None, help="Domain szűrő (pl. index.hu, telex.hu)")
    p.add_argument("--date-from", default=None, help="Dátum-tól (YYYY-MM-DD)")
    p.add_argument("--date-to", default=None, help="Dátum-ig (YYYY-MM-DD)")
    p.add_argument("--limit", type=int, default=None, help="Max ennyi cikket elemez (debughoz).")
    p.add_argument(
        "--min-cluster-size",
        type=int,
        default=3,
        help="Minimum cikk/narratíva (ennél kisebb clusterek zajnak számítanak).",
    )
    p.add_argument(
        "--max-clusters",
        type=int,
        default=50,
        help="KMeans maximum cluster száma (felső korlát).",
    )
    p.add_argument(
        "--model",
        default="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
        help="SentenceTransformer modell neve.",
    )
    p.add_argument(
        "--out",
        default=None,
        help="Kimeneti JSON fájl (ha nincs megadva, stdout-ra ír).",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    repo = Repository(args.db)
    embedder = SentenceTransformerEmbedder(model_name=args.model)
    detector = NarrativeDetector(repo, embedder=embedder)

    if args.verbose:
        print(
            f"[NARRATIVE] Loading articles: domain={args.domain}, "
            f"from={args.date_from}, to={args.date_to}, limit={args.limit}"
        )

    arts = detector.load_articles(
        domain=args.domain,
        date_from=args.date_from,
        date_to=args.date_to,
        limit=args.limit,
    )

    if args.verbose:
        print(f"[NARRATIVE] Loaded {len(arts)} articles.")

    if not arts:
        print("[]")
        return

    emb = detector.embed_articles(arts)

    if args.verbose:
        print(f"[NARRATIVE] Embeddings shape: {emb.shape}")

    labels = detector.cluster_embeddings(
        emb,
        min_cluster_size=args.min_cluster_size,
        max_clusters=args.max_clusters,
    )

    if args.verbose:
        n_clusters = len({lab for lab in labels if lab != -1})
        print(f"[NARRATIVE] Found {n_clusters} clusters (labels != -1).")

    narratives = detector.build_narratives(arts, labels)

    data = [asdict(n) for n in narratives]

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=int)
        if args.verbose:
            print(f"[NARRATIVE] Saved {len(narratives)} narratives to {args.out}")
    else:
        print(json.dumps(data, ensure_ascii=False, indent=2, default=int))


if __name__ == "__main__":
    main()
