# embed_classify_summarize.py
import sqlite3, os, re, math, json
from pathlib import Path
from typing import List, Tuple
import numpy as np
from sentence_transformers import SentenceTransformer
import faiss
from sklearn.cluster import KMeans

DB_PATH = "news.sqlite"
MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
EMB_DIM = 384  # a fenti modell kimeneti dimenziója
BATCH = 32
MIN_CONTENT_LEN = 300  # nagyon rövid cikkek kiszűrése
K_CLUSTERS = 8         # klaszterek száma a „témacímkékhez” (tetszőlegesen állítható)

# --- segédfüggvények ---------------------------------------------------------

def normalize(v: np.ndarray) -> np.ndarray:
    n = np.linalg.norm(v, axis=1, keepdims=True) + 1e-12
    return v / n

def sent_split(text: str) -> List[str]:
    # egyszerű mondatdaraboló (HU-n is „elég jó”)
    return [s.strip() for s in re.split(r'(?<=[\.\!\?])\s+', text) if s.strip()]

def top_k_sentences(text: str, emb_model, k=5) -> List[str]:
    sents = sent_split(text)
    if not sents:
        return []
    # mondat-embeddingek + centroidközelség, a legreprezentatívabb mondatok
    embs = emb_model.encode(sents, convert_to_numpy=True, batch_size=16, show_progress_bar=False)
    embs = normalize(embs)
    centroid = normalize(np.mean(embs, axis=0, keepdims=True))
    sims = (embs @ centroid.T).ravel()
    idx = np.argsort(-sims)[:k]
    # eredeti sorrend jobb olvashatóságra
    idx_sorted = sorted(idx)
    return [sents[i] for i in idx_sorted]

def ensure_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    # új oszlopok: label, label_score, emb (BLOB), cluster_id, cluster_summary
    cur.execute("PRAGMA table_info(items)")
    cols = [r[1] for r in cur.fetchall()]
    if "label" not in cols:
        cur.execute("ALTER TABLE items ADD COLUMN label TEXT")
    if "label_score" not in cols:
        cur.execute("ALTER TABLE items ADD COLUMN label_score REAL")
    if "emb" not in cols:
        cur.execute("ALTER TABLE items ADD COLUMN emb BLOB")
    if "cluster_id" not in cols:
        cur.execute("ALTER TABLE items ADD COLUMN cluster_id INTEGER")
    if "cluster_summary" not in cols:
        cur.execute("ALTER TABLE items ADD COLUMN cluster_summary TEXT")
    conn.commit()

def fetch_items(conn: sqlite3.Connection) -> List[Tuple[str, str, str, str]]:
    # id, title, content, link
    rows = conn.execute(
        "SELECT id, title, content, link FROM items ORDER BY ts DESC"
    ).fetchall()
    return rows

def to_bytes(vec: np.ndarray) -> bytes:
    return vec.astype(np.float32).tobytes()

def from_bytes(b: bytes) -> np.ndarray:
    return np.frombuffer(b, dtype=np.float32)

# --- prototípusos osztályozás (kormánypárti vs. ellenzéki vs. semleges) -----

LABEL_DEFS = {
    "kormánypárti": [
        "kormány álláspontját támogató narratíva",
        "Fidesz-KDNP intézkedéseit pozitív színben feltüntető értelmezés",
        "kormányzati kommunikációt erősítő megfogalmazás"
    ],
    "ellenzéki": [
        "kormányt kritizáló vagy ellenzéki pártok üzeneteit erősítő narratíva",
        "kormányzati lépésekkel szemben kritikus tartalom",
        "közéleti visszásságokra rámutató ellenzéki hang"
    ],
    "semleges": [
        "tárgyszerű, kiegyensúlyozott, semleges hangvétel",
        "hírügynökségi jelleg, kommentár nélkül",
        "tényközlés állásfoglalás nélkül"
    ]
}

def build_label_prototypes(emb_model) -> dict:
    protos = {}
    for lab, prompts in LABEL_DEFS.items():
        vecs = emb_model.encode(prompts, convert_to_numpy=True, show_progress_bar=False)
        vecs = normalize(vecs)
        protos[lab] = np.mean(vecs, axis=0, keepdims=True)  # 1 x d
    return protos

def classify_doc(vec: np.ndarray, protos: dict) -> Tuple[str, float]:
    # cos hasonlóság a prototípusokhoz
    best_label, best_score = None, -1.0
    for lab, proto in protos.items():
        score = float((vec @ proto.T).ravel()[0])
        if score > best_score:
            best_label, best_score = lab, score
    return best_label, best_score

# --- fő folyamat -------------------------------------------------------------

def main():
    conn = sqlite3.connect(DB_PATH)
    ensure_schema(conn)

    model = SentenceTransformer(MODEL_NAME)
    items = fetch_items(conn)

    texts = []
    meta = []  # (id, title, link)
    for _id, title, content, link in items:
        text = (content or "").strip()
        if len(text) < MIN_CONTENT_LEN:
            # ha nagyon rövid a cikk, egészítsük ki a címmel
            text = f"{title or ''}. {text}"
        if not text.strip():
            continue
        texts.append(text)
        meta.append((_id, title or "", link or ""))

    if not texts:
        print("Nincs elég feldolgozható cikk.")
        return

    # 1) Embedding
    vecs = model.encode(texts, convert_to_numpy=True, batch_size=BATCH, show_progress_bar=True)
    vecs = normalize(vecs).astype(np.float32)

    # 2) FAISS index (koszinuszhoz IP + normalizált vektorok)
    index = faiss.IndexFlatIP(EMB_DIM)
    index.add(vecs)

    # 3) Prototípusos osztályozás
    protos = build_label_prototypes(model)
    for lab in protos:
        protos[lab] = normalize(protos[lab].astype(np.float32))

    labels, scores = [], []
    for i in range(vecs.shape[0]):
        lab, sc = classify_doc(vecs[i:i+1], protos)
        labels.append(lab)
        scores.append(sc)

    # 4) Klaszterezés (opcionális: témacímkék)
    km = KMeans(n_clusters=K_CLUSTERS, n_init="auto", random_state=42)
    cluster_ids = km.fit_predict(vecs)

    # 5) Klaszter-összefoglalók (centroid-közeli mondatok)
    #    csoportosítsunk, majd minden klaszterre csináljunk 5 mondatos kivonatot
    per_cluster_texts = {}
    for (cid, text) in zip(cluster_ids, texts):
        per_cluster_texts.setdefault(cid, []).append(text)

    cluster_summaries = {}
    for cid, tlist in per_cluster_texts.items():
        # egy nagy korpusz string a klaszterhez
        joined = "\n".join(tlist)
        # vegyünk 5 centroid-közeli mondatot (gyors, LLM nélküli)
        # trükk: mondatokat külön embedeljük és a klaszter centroidhoz mérjük
        sents = sent_split(joined)
        if not sents:
            cluster_summaries[cid] = ""
            continue
        s_vecs = model.encode(sents, convert_to_numpy=True, batch_size=64, show_progress_bar=False)
        s_vecs = normalize(s_vecs)
        centroid = normalize(np.mean(s_vecs, axis=0, keepdims=True))
        sims = (s_vecs @ centroid.T).ravel()
        idx = np.argsort(-sims)[:5]
        idx_sorted = sorted(idx)
        summary = " ".join([sents[i] for i in idx_sorted])
        cluster_summaries[cid] = summary

    # 6) Mentés a DB-be
    cur = conn.cursor()
    # id -> index térkép
    for (i, (_id, title, link)) in enumerate(meta):
        cur.execute(
            "UPDATE items SET label=?, label_score=?, emb=?, cluster_id=? WHERE id=?",
            (labels[i], float(scores[i]), to_bytes(vecs[i]), int(cluster_ids[i]), _id)
        )
    conn.commit()

    # klaszter-összefoglalók (ugyanaz a szöveg mehet több elemhez; egyszerűen itt a legfrissebbhez írjuk)
    for cid, summ in cluster_summaries.items():
        # csak „legutóbbi” egy rekordjára írjuk rá a summary-t, hogy legyen hova nézni
        cur.execute("SELECT id FROM items WHERE cluster_id=? ORDER BY ts DESC LIMIT 1", (int(cid),))
        row = cur.fetchone()
        if row:
            cur.execute("UPDATE items SET cluster_summary=? WHERE id=?", (summ, row[0]))
    conn.commit()

    print("Kész: embeddingek, címkék és klaszter-összefoglalók frissítve.")

if __name__ == "__main__":
    main()
