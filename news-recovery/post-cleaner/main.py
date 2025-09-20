import os, re, json, hashlib, logging, signal, sys
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from langdetect import detect, LangDetectException
from sentence_transformers import SentenceTransformer
import requests
import emoji

# -------------------- Config --------------------
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:60118")
TOPIC_IN  = os.getenv("TOPIC_IN",  "raw.posts")
TOPIC_OUT = os.getenv("TOPIC_OUT", "rank.jobs")
GROUP_ID  = os.getenv("GROUP_ID",  "clean-embed-local-v1")

ST_MODEL = os.getenv("ST_MODEL", "intfloat/e5-base-v2")
EMB_MAX_CHARS = int(os.getenv("EMB_MAX_CHARS", "4000"))

OLLAMA_HOST  = os.getenv("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:3b")  # leve
MAX_QUERY_CH = int(os.getenv("MAX_QUERY_CH", "120"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# -------------------- Util --------------------
URL_RE     = re.compile(r"(https?://\S+)")
MENTION_RE = re.compile(r"@\w+")
WS_RE      = re.compile(r"\s+")
STOP_MIN   = {
    "the","and","http","https","com","www",
    "de","da","do","dos","das","uma","umas","um","uns",
    "para","por","com","nos","nas","no","na","em","que"
}

def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

def clean_text(raw: str) -> tuple[str, list[str]]:
    t = emoji.replace_emoji(raw or "", replace=" ")
    urls = URL_RE.findall(t)
    t = URL_RE.sub(" ", t)
    t = MENTION_RE.sub(" ", t)
    t = WS_RE.sub(" ", t).strip()
    return t, list(dict.fromkeys(urls))

def detect_lang_safe(text: str, fallback="pt") -> str:
    try:
        return detect(text) if text.strip() else fallback
    except LangDetectException:
        return fallback

def extract_keywords(text: str, max_k=12):
    hashtags = [h[1:].lower() for h in re.findall(r"#\w+", text)]
    tokens   = [w.lower() for w in re.findall(r"[A-Za-zÀ-ÿ\-']{3,}", text)]
    out, seen = [], set()
    for w in hashtags + tokens:
        if w in STOP_MIN:
            continue
        if w not in seen:
            seen.add(w); out.append(w)
        if len(out) >= max_k:
            break
    return out

def safe_for_embedding(raw_text: str, clean: str, max_chars: int) -> str:
    base = (clean or raw_text or "").strip()
    if not base:
        base = "[empty]"
    if len(base) > max_chars:
        base = base[:max_chars]
    return base

def _normalize_ws(s: str) -> str:
    return WS_RE.sub(" ", s).strip()

def _trim_query(q: str) -> str:
    q = _normalize_ws(q)
    if len(q) <= MAX_QUERY_CH:
        return q
    q = q[:MAX_QUERY_CH].rstrip()
    if q.count('"') % 2 == 1:
        q = q.rsplit('"', 1)[0]
    return q

STOP_PT = STOP_MIN | {
    "um","uma","uns","umas","de","do","da","dos","das","pra","pro","a","o","as","os",
    "se","em","no","na","nos","nas","ao","aos","à","às","é","ser","foi","são","esta","está",
    "hoje","ontem","amanhã"
}
PROPER_RE = re.compile(r"\b([A-ZÁÉÍÓÚÂÊÔÃÕÀÜ][\wÁÉÍÓÚÂÊÔÃÕÀÜà-ÿ'\-]+(?:\s+[A-ZÁÉÍÓÚÂÊÔÃÕÀÜ][\wÁÉÍÓÚÂÊÔÃÕÀÜà-ÿ'\-]+){0,3})\b")

def _tokenize_terms_pt(text: str) -> list[str]:
    stop = STOP_PT
    toks = [t.lower() for t in re.findall(r"[A-Za-zÀ-ÿ0-9'\-]{3,}", text)]
    out, seen = [], set()
    for t in toks:
        if t in stop:
            continue
        if t not in seen:
            seen.add(t); out.append(t)
        if len(out) >= 12:
            break
    return out

def _extract_proper_names(text: str) -> list[str]:
    names, seen = [], set()
    for m in PROPER_RE.finditer(text):
        n = m.group(1).strip()
        if len(n.split()) == 1 and len(n) < 4:
            continue
        low = n.lower()
        if low not in seen:
            seen.add(low); names.append(n)
        if len(names) >= 4:
            break
    return names

def _guess_date_hint_pt(created_iso: str) -> str:
    try:
        dt = datetime.fromisoformat(created_iso.replace("Z","+00:00"))
    except Exception:
        dt = datetime.now(timezone.utc)
    months_pt = ["jan","fev","mar","abr","mai","jun","jul","ago","set","out","nov","dez"]
    mon = months_pt[dt.month-1]
    return f"{mon} {dt.year}"

def _heuristic_queries_pt(clean_text: str, created_iso: str) -> list[str]:
    hashtags = [h[1:] for h in re.findall(r"#(\w+)", clean_text)]
    names    = _extract_proper_names(clean_text)
    terms    = _tokenize_terms_pt(clean_text)
    date_hint = _guess_date_hint_pt(created_iso)

    base_terms = names + hashtags + terms
    base_terms = base_terms[:8] if base_terms else terms[:8]

    exact_terms = base_terms[:6] if base_terms else terms[:6]
    exact_text = " ".join(exact_terms) if exact_terms else clean_text
    q1 = _trim_query(f"\"{exact_text}\"")

    main = base_terms[:4] if len(base_terms) >= 2 else terms[:4]
    q2 = " OR ".join([f"\"{t}\"" if " " in t else t for t in main])
    q2 = _trim_query(q2)

    expanded = base_terms[:6]
    q3 = _trim_query(f"{' '.join(expanded)} {date_hint}")

    out = [q for q in [q1, q2, q3] if q and len(q) >= 3]
    seen, uniq = set(), []
    for q in out:
        if q.lower() not in seen:
            seen.add(q.lower()); uniq.append(q)
    return uniq

EN_GUARD_RE = re.compile(r"\b(news|breaking|weather|storm|flood|city|the|of|in|and|with|from|on|for)\b", re.I)

def _ollama_chat(system_prompt: str, user_prompt: str, timeout_s: int = 12) -> str:
    url = f"{OLLAMA_HOST.rstrip('/')}/api/chat"
    payload = {
        "model": OLLAMA_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_prompt}
        ],
        "stream": False,
        "options": {"temperature": 0.2}
    }
    resp = requests.post(url, json=payload, timeout=timeout_s)
    resp.raise_for_status()
    data = resp.json()
    return data.get("message", {}).get("content", "") or ""

def _call_llm_for_queries_pt(clean_text: str, created_iso: str) -> list[str]:
    sys_prompt = (
        "Você é um gerador de consultas para o Google News em PT-BR. "
        "Sempre responda EXCLUSIVAMENTE em português do Brasil (PT-BR). "
        "A partir de um texto curto, produza de 2 a 3 consultas concisas (<=120 caracteres) "
        "que maximizem a chance de achar notícias relevantes. "
        "Use aspas para trechos exatos, OR para variações importantes e inclua uma dica temporal sucinta (mês e ano) quando fizer sentido. "
        "NÃO use palavras em inglês. NÃO use hashtags, emojis ou URLs. "
        "Saída APENAS em JSON com o campo 'queries': lista de strings."
    )
    date_hint = _guess_date_hint_pt(created_iso)
    user_prompt = (
        f"Texto: {clean_text}\n"
        f"Idioma: pt-BR\n"
        f"Dica temporal: {date_hint}\n"
        "Responda."
    )
    try:
        content = _ollama_chat(sys_prompt, user_prompt, timeout_s=15)
        try:
            j = json.loads(content)
        except Exception:
            m = re.search(r"\{.*\}", content, re.S)
            j = json.loads(m.group(0)) if m else {"queries":[]}
        queries = [ _trim_query(q) for q in j.get("queries", []) if isinstance(q, str) and q.strip() ]
        queries = [q for q in queries if not EN_GUARD_RE.search(q)]
        return queries[:3]
    except Exception:
        logging.exception("Ollama/LLM falhou; usando heurística")
        return []

def generate_search_queries(clean_text: str, _lang_ignored: str, created_iso: str) -> list[str]:
    q_llm = _call_llm_for_queries_pt(clean_text, created_iso)
    if q_llm:
        return q_llm
    return _heuristic_queries_pt(clean_text, created_iso)

# -------------------- Embeddings (LOCAL) --------------------
logging.info("Carregando modelo local de embeddings: %s", ST_MODEL)
EMB = SentenceTransformer(ST_MODEL)

def embed_local(text: str) -> list[float]:
    to_encode = f"query: {text}"
    vec = EMB.encode(to_encode, normalize_embeddings=True)
    return [float(x) for x in vec]

# -------------------- Kafka --------------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS.split(","),
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    linger_ms=10,
)
consumer = KafkaConsumer(
    TOPIC_IN,
    bootstrap_servers=KAFKA_BROKERS.split(","),
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    enable_auto_commit=False,
    auto_offset_reset="latest",
    group_id=GROUP_ID,
    max_poll_records=100,
)

_running = True
def _graceful(*_):
    global _running
    _running = False
signal.signal(signal.SIGINT,  _graceful)
signal.signal(signal.SIGTERM, _graceful)

# -------------------- Core --------------------
SCHEMA_VERSION = "rank.jobs.v2"

def transform(msg_in: dict) -> dict:
    """
    Entrada:
    {
      "postId": 1,
      "text": "texto cru do post",
      "createdAt": "2025-09-19T12:34:56Z",
      "authorId": 1
    }
    """
    post_id   = msg_in["post_id"]
    text      = msg_in.get("text","")
    created   = msg_in.get("created_at") or now_iso()
    author_id = msg_in.get("authorId")

    clean, urls = clean_text(text)
    lang = detect_lang_safe(clean, fallback="pt")
    keywords = extract_keywords(clean)

    emb_input = safe_for_embedding(text, clean, EMB_MAX_CHARS)
    emb_vec = embed_local(emb_input)

    topic_id = sha256(f"generic|{post_id}")

    news_queries = generate_search_queries(clean, lang, created)

    return {
        "topicId": topic_id,
        "postId": post_id,
        "authorId": author_id,
        "cleanText": clean,
        "lang": lang,
        "keywords": keywords,
        "urls": urls,
        "embeddingModel": f"st:{ST_MODEL}",
        "embedding": emb_vec,
        "createdAt": created,
        "ingestedAt": now_iso(),
        "schemaVersion": SCHEMA_VERSION,
        "newsQueries": news_queries,
        "newsQueryPrimary": news_queries[0] if news_queries else None,
    }

def main():
    logging.info(
        "Consumindo de %s, produzindo em %s | brokers=%s | ST=%s | Ollama=%s (%s)",
        TOPIC_IN, TOPIC_OUT, KAFKA_BROKERS, ST_MODEL, OLLAMA_MODEL, OLLAMA_HOST
    )
    while _running:
        batch = consumer.poll(timeout_ms=1000)
        for tp, msgs in batch.items():
            for m in msgs:
                try:
                    out = transform(m.value)
                    producer.send(TOPIC_OUT, key=out["topicId"].encode(), value=out)
                except Exception:
                    logging.exception("Falha ao processar mensagem offset=%s", m.offset)
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
