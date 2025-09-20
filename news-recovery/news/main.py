
import os, json, logging, signal, sys, html, time, unicodedata
from datetime import datetime, timezone
from urllib.parse import urlencode, quote_plus

import requests
import feedparser
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from sentence_transformers import SentenceTransformer
from dateutil import parser as dtparse
import re

# -------------------- Config --------------------
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:60118")
TOPIC_IN  = os.getenv("TOPIC_IN",  "rank.jobs")
TOPIC_OUT = os.getenv("TOPIC_OUT", "news.matches")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "news.matches.dlq")
GROUP_ID  = os.getenv("GROUP_ID",  "news-matcher-pt-v2")

EMB_MODEL     = os.getenv("EMB_MODEL", "intfloat/e5-base-v2")
EMB_MAX_CHARS = int(os.getenv("EMB_MAX_CHARS", "600"))
TOPN_RESULTS  = int(os.getenv("TOPN_RESULTS", "10"))
RSS_LIMIT     = int(os.getenv("RSS_LIMIT", "80"))
SEEK_TO_END   = os.getenv("SEEK_TO_END", "1") == "1"
SLEEP_BETWEEN = float(os.getenv("SLEEP_BETWEEN", "0.9"))  # ~1 req/s

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "20"))
HTTP_HEADERS = {
    "User-Agent": os.getenv(
        "HTTP_UA",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

HL, GL, CEID = "pt-BR", "BR", "BR:pt"

# -------------------- Embeddings --------------------
logging.info("Carregando modelo local de embeddings: %s", EMB_MODEL)
EMB = SentenceTransformer(EMB_MODEL)

def embed_text(text: str) -> np.ndarray:
    t = (text or "").strip() or "[empty]"
    if len(t) > EMB_MAX_CHARS:
        t = t[:EMB_MAX_CHARS]
    v = EMB.encode(f"query: {t}", normalize_embeddings=True)
    return np.asarray(v, dtype=np.float32)

def cosine(a, b) -> float:
    a = np.asarray(a, dtype=np.float32)
    b = np.asarray(b, dtype=np.float32)
    return float(np.dot(a, b))

# -------------------- Utils --------------------
def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

def ascii_fold(s: str) -> str:
    return ''.join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def build_gnews_url_from_query(query: str):
    """
    Query é frase pronta (pode ter aspas, OR, etc.). Apenas encode.
    Força PT-BR via HL/GL/CEID fixos.
    """
    q = (query or "").strip() or "notícia"
    params = {"q": q, "hl": HL, "gl": GL, "ceid": CEID}
    return "https://news.google.com/rss/search?" + urlencode(params, quote_via=quote_plus)

def build_gnews_url_from_keywords(keywords, city_hint=None):
    kws = [k.strip() for k in (keywords or []) if isinstance(k, str) and k.strip()]
    if not kws:
        kws = ["notícia"]
    def qterm(t: str) -> str:
        return f'"{t}"' if " " in t else t
    group = "(" + " OR ".join(qterm(k) for k in kws[:10]) + ")"
    q = group
    if city_hint:
        q += f' AND "{city_hint}"'
    params = {"q": q, "hl": HL, "gl": GL, "ceid": CEID}
    return "https://news.google.com/rss/search?" + urlencode(params, quote_via=quote_plus)

def fetch_gnews_items(url: str, limit=80):
    time.sleep(SLEEP_BETWEEN)
    r = requests.get(url, headers=HTTP_HEADERS, timeout=HTTP_TIMEOUT)
    r.raise_for_status()

    feed = feedparser.parse(r.content)
    items = []
    for e in feed.entries[:limit]:
        title = html.unescape(getattr(e, "title", "") or "")
        link  = getattr(e, "link", None)
        pub   = getattr(e, "published", None)
        src   = getattr(getattr(e, "source", None), "title", None)
        if not link or not title:
            continue
        items.append({"url": link, "title": title, "source": src, "published": pub})
    return items

def recency_score(published):
    try:
        dt = dtparse.parse(published) if published else None
        if not dt:
            return 0.5
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        age_days = max(0.0, (datetime.now(timezone.utc) - dt).total_seconds()/86400.0)
        return 1.0 / (1.0 + age_days)
    except Exception:
        return 0.5

TOKEN_RE = re.compile(r"[A-Za-zÀ-ÿ0-9'\-]{3,}")

def tokenize_query_terms(q: str):
    return [t.lower() for t in TOKEN_RE.findall(q or "")]

def keyword_score(keywords, query_terms, title):
    t = (title or "").lower()
    t_nf = ascii_fold(t)
    pool = []
    if keywords:
        pool.extend([k for k in keywords if isinstance(k, str) and k])
    if query_terms:
        pool.extend(query_terms)
    if not pool:
        return 0.0
    seen = set()
    hits = 0
    total = 0
    for k in pool:
        k0 = k.lower().strip()
        if not k0 or k0 in seen:
            continue
        seen.add(k0)
        total += 1
        if k0 in t or ascii_fold(k0) in t_nf:
            hits += 1
    return hits / max(1, total)

def rank_items(base_vec, keywords, query_terms, items, topn=10):
    ranked = []
    for it in items:
        title = it.get("title") or ""
        vec   = embed_text(title)
        sim   = cosine(base_vec, vec)
        kw    = keyword_score(keywords, query_terms, title)
        rec   = recency_score(it.get("published"))
        score = 0.55*sim + 0.15*kw + 0.30*rec
        ranked.append({**it, "scores": {"cosine": sim, "kw": kw, "recency": rec, "final": score}})
    ranked.sort(key=lambda x: x["scores"]["final"], reverse=True)
    return ranked[:topn]

# -------------------- Kafka I/O --------------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS.split(","),
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    linger_ms=10,
)
consumer = KafkaConsumer(
    TOPIC_IN,
    bootstrap_servers=KAFKA_BROKERS.split(","),
    value_deserializer=lambda v: v,
    enable_auto_commit=False,
    auto_offset_reset="latest",
    group_id=GROUP_ID,
    max_poll_records=50,
)

_running = True
def _graceful(*_):
    global _running
    _running = False
signal.signal(signal.SIGINT,  _graceful)
signal.signal(signal.SIGTERM, _graceful)

def parse_json_loose(raw: bytes):
    if raw is None:
        raise ValueError("mensagem vazia (None)")
    s = raw.decode("utf-8", "ignore").strip().lstrip("\ufeff")
    if not s:
        raise ValueError("mensagem vazia (apenas whitespace)")
    try:
        obj = json.loads(s)
        if isinstance(obj, str):
            return json.loads(obj)
        return obj
    except json.JSONDecodeError:
        inner = json.loads(s)
        return json.loads(inner) if isinstance(inner, str) else inner

# -------------------- Core --------------------
SCHEMA_VERSION = "news.matches.v1"

def process_message(raw_bytes):
    msg = parse_json_loose(raw_bytes)

    topic_id = msg.get("topicId")
    post_vec = msg.get("embedding")
    clean    = msg.get("cleanText", "") or ""
    lang     = (msg.get("lang") or "pt").lower()
    keywords = msg.get("keywords") or []

    news_queries = msg.get("newsQueries") or []
    news_query_primary = msg.get("newsQueryPrimary") or (news_queries[0] if news_queries else None)

    if not post_vec:
        post_vec = embed_text(clean)
    else:
        post_vec = np.asarray(post_vec, dtype=np.float32)

    queries = []
    if news_query_primary:
        queries.append(news_query_primary)
    for q in news_queries:
        if not q:
            continue
        if news_query_primary and q.strip() == news_query_primary.strip():
            continue
        queries.append(q)

    using_fallback_keywords = False
    if not queries:
        url = build_gnews_url_from_keywords(keywords)
        queries = [None]  # marcador: vamos buscar só por url já montada
        using_fallback_keywords = True
        logging.info("Sem newsQueries — usando fallback por keywords")

    clean_nf = ascii_fold(clean.lower())
    city_hint = "sao paulo" if ("sao paulo" in clean_nf or "são paulo" in clean.lower()) else None

    all_items = []
    seen_urls = set()
    query_urls = []

    if using_fallback_keywords:
        url = build_gnews_url_from_keywords(keywords, city_hint=city_hint)
        query_urls.append(url)
        logging.info("GN URL (fallback): %s", url)
        try:
            items = fetch_gnews_items(url, limit=RSS_LIMIT)
        except Exception as e:
            logging.warning("Falha ao buscar GN (fallback): %s", e)
            items = []
        for it in items:
            u = it.get("url")
            if u and u not in seen_urls:
                seen_urls.add(u)
                all_items.append(it)
    else:
        for q in queries:
            try:
                url = build_gnews_url_from_query(q)
                query_urls.append(url)
                logging.info("GN URL: %s", url)
                items = fetch_gnews_items(url, limit=RSS_LIMIT)
            except Exception as e:
                logging.warning("Falha ao buscar GN (q='%s'): %s", q, e)
                items = []
            for it in items:
                u = it.get("url")
                if u and u not in seen_urls:
                    seen_urls.add(u)
                    all_items.append(it)

    primary_terms = tokenize_query_terms(news_query_primary) if news_query_primary else []

    ranked = rank_items(post_vec, keywords, primary_terms, all_items, topn=TOPN_RESULTS)

    return {
        **msg,
        "news": {
            "source": "googlenews",
            "queryUrls": query_urls,
            "candidates": ranked,
            "matchedAt": now_iso()
        },
        "schemaVersion": SCHEMA_VERSION
    }

def main():
    logging.info("NEWS matcher v2 (PT-BR fixo) | in=%s out=%s | brokers=%s", TOPIC_IN, TOPIC_OUT, KAFKA_BROKERS)

    consumer.subscribe([TOPIC_IN])
    consumer.poll(timeout_ms=1000)
    if SEEK_TO_END:
        for tp in consumer.assignment():
            consumer.seek_to_end(tp)
        logging.info("Seek to end aplicado (SEEK_TO_END=1)")

    while _running:
        batch = consumer.poll(timeout_ms=1000)
        for tp, msgs in batch.items():
            for m in msgs:
                try:
                    if not m.value:
                        raise ValueError("mensagem vazia")
                    out = process_message(m.value)
                    key = (out.get("topicId") or "").encode() or None
                    producer.send(TOPIC_OUT, key=key, value=out).get(timeout=10)
                except Exception as e:
                    logging.warning("Falha ao processar %s@%s: %s", m.topic, m.offset, e)
                    try:
                        producer.send(DLQ_TOPIC, value={
                            "error": str(e),
                            "topic": m.topic,
                            "partition": m.partition,
                            "offset": m.offset,
                            "payload": (m.value or b"").decode("utf-8","replace"),
                            "ts": now_iso(),
                        }).get(timeout=10)
                    except Exception:
                        logging.exception("Falha ao enviar para DLQ")
        consumer.commit()
        producer.flush()

    logging.info("Encerrando...")
    consumer.close(5)
    producer.flush(); producer.close(5)

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("Erro fatal")
        sys.exit(1)
