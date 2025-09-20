import os, json, asyncio, signal, time, re, hashlib
from typing import List, Optional
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
import numpy as np

# Kafka (async)
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# OpenAI (SDK novo). Se não tiver, o worker segue com fallback heurístico.
USE_OPENAI = True
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
try:
    from openai import OpenAI
    if not OPENAI_API_KEY:
        USE_OPENAI = False
        openai_client = None
    else:
        openai_client = OpenAI(api_key=OPENAI_API_KEY)
except Exception:
    USE_OPENAI = False
    openai_client = None

# ---------- Config ----------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP","localhost:9092")
TOPIC_IN  = os.getenv("TOPIC_IN","verify.jobs")      # onde chega seu evento
TOPIC_OUT = os.getenv("TOPIC_OUT","rag.results")     # veredito publicado
GROUP_ID  = os.getenv("GROUP_ID","verify-worker")

MODEL_NAME = os.getenv("LLM_MODEL","gpt-4o-mini")    # troque se quiser
TOPK = int(os.getenv("TOPK","6"))

# ---------- Utils ----------
def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

def clamp01(x: float) -> float:
    try:
        return float(max(0.0, min(1.0, x)))
    except Exception:
        return 0.0

def try_parse_pub(published: Optional[str]) -> Optional[str]:
    if not published:
        return None
    # RFC822 → ISO
    try:
        dt = parsedate_to_datetime(published)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00","Z")
    except Exception:
        pass
    # ISO direto
    try:
        dt = datetime.fromisoformat(published.replace("Z","+00:00"))
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00","Z")
    except Exception:
        return None

def days_between(iso_newer: Optional[str], iso_older: Optional[str]) -> Optional[float]:
    try:
        a = datetime.fromisoformat((iso_newer or "").replace("Z","+00:00"))
        b = datetime.fromisoformat((iso_older or "").replace("Z","+00:00"))
        return (a - b).total_seconds() / 86400.0
    except Exception:
        return None

def short(text: str, limit: int=1200) -> str:
    if not text: return ""
    t = re.sub(r"\s+"," ", text).strip()
    return t if len(t) <= limit else t[:limit] + "..."

def _domain_from_url(url: Optional[str]) -> str:
    if not url: return ""
    try:
        return url.split("/")[2]
    except Exception:
        return ""

def _stable_id(prefix: str, seed: str) -> str:
    h = hashlib.sha1((seed or "").encode("utf-8")).hexdigest()[:16]
    return f"{prefix}-{h}"

def _tokens(text: str) -> set:
    if not text: return set()
    # tokens simples, minúsculos, alfanuméricos
    toks = re.findall(r"[a-zA-ZÀ-ÿ0-9]{3,}", text.lower())
    return set(toks)

def _score_overlap(a: str, b: str) -> float:
    A = _tokens(a); B = _tokens(b)
    if not A or not B: return 0.0
    inter = len(A & B)
    return clamp01(inter / max(6, len(A)))  # normalização conservadora

# ---------- Coleta “post text” robusto ----------
POST_TEXT_KEYS = ("cleanText","rawText","text","content","message","body","htmlText","title")

def _best_post_text(root_like: dict) -> str:
    # tenta em root, root.post, root.page, root.pages[i].post etc.
    def grab_many(obj: dict) -> Optional[str]:
        for k in POST_TEXT_KEYS:
            v = (obj.get(k) or "").strip()
            if v: return v
        if obj.get("keywords"):
            kws = obj["keywords"]
            if isinstance(kws, list) and kws:
                return " ".join([str(x) for x in kws[:12]])
        return None

    # 1) raiz
    v = grab_many(root_like) or grab_many(root_like.get("post",{}) or {})
    if v: return v

    # 2) news/pages
    page = root_like.get("page") or {}
    v = grab_many(page) or grab_many(page.get("post",{}) or {})
    if v: return v

    # 3) se tiver pages, pega da primeira
    pages = root_like.get("pages") or []
    if pages:
        v = grab_many(pages[0]) or grab_many(pages[0].get("post",{}) or {})
        if v: return v

    # 4) por fim: usa título da 1ª notícia, se houver
    cands = _collect_candidates(root_like)
    if cands:
        t = (cands[0].get("title") or "").strip()
        if t: return t

    return ""

# ---------- Coleta candidates em vários formatos ----------
def _collect_candidates(container: dict) -> List[dict]:
    paths = [
        ("news","candidates"),
        ("candidates",),
        ("articles",),
        ("page","news","candidates"),
        ("page","candidates"),
        ("page","articles"),
        ("post","news","candidates"),
        ("post","candidates"),
        ("post","articles"),
    ]
    out = []
    for p in paths:
        cur = container
        ok = True
        for key in p:
            if not isinstance(cur, dict) or key not in cur:
                ok = False; break
            cur = cur[key]
        if ok and isinstance(cur, list):
            out.extend([x for x in cur if isinstance(x, dict)])
    return out

# ---------- Adaptador: qualquer evento -> verify.jobs.v1 ----------
def _cand_to_related(cand: dict, i: int, post_lang: str, post_id: str, news_source: str) -> dict:
    rag   = cand.get("rag") or {}
    url   = rag.get("finalUrl") or cand.get("url")
    title = (cand.get("title") or "").strip()
    src   = (cand.get("source") or "").strip() or _domain_from_url(url) or "unknown"
    pub   = try_parse_pub(cand.get("published") or cand.get("date") or cand.get("time"))

    # Texto do artigo (ignoramos embeddings de propósito)
    text  = (rag.get("text") or cand.get("snippet") or cand.get("summary") or cand.get("description") or cand.get("text") or "").strip()

    # metadados da fonte
    source_meta = {
        "name": src,
        "domain": _domain_from_url(url),
        "orgType": "news",
        "ifcn": False,
        "baseWeight": 0.60
    }

    return {
        "collectedFrom": news_source,
        "evidenceId": f"{post_id}-{i}",
        "source": source_meta,
        "url": url,
        "title": title,
        "lang": post_lang or "pt",
        "publishedAt": pub,
        "scoresFromProducer": cand.get("scores") or rag.get("scores") or {},
        # chunks apenas com texto; embeddings são ignorados
        "chunks": ([{"chunkId":"0","text": text}] if text else [])
    }

def convert_any_to_job(raw: dict) -> List[dict]:
    """
    Retorna uma lista de jobs normalizados (verify.jobs.v1).
    Se houver pages, um job por page; senão, um único job.
    """
    jobs = []
    schema = (raw.get("schemaVersion") or "").lower()

    pages = raw.get("pages")
    if pages and isinstance(pages, list) and pages:
        for page in pages:
            # post básico
            post_obj = page.get("post") or {}
            post_lang = (post_obj.get("lang") or page.get("lang") or raw.get("lang") or "pt").strip()
            post_id = (post_obj.get("postId") or page.get("postId") or raw.get("postId"))
            if not post_id:
                seed = (post_obj.get("externalId") or page.get("externalId") or json.dumps(page, ensure_ascii=False)[:512])
                post_id = _stable_id("post", seed)
            created = (post_obj.get("createdAt") or page.get("createdAt") or raw.get("createdAt") or now_utc_iso())

            # texto do post (robusto)
            post_text = _best_post_text({"page": page, "post": post_obj}) or ""

            # topic
            topic_id = (raw.get("topicId") or page.get("topicId") or _stable_id("topic", json.dumps(raw, ensure_ascii=False)[:512]))

            # candidates
            cands = _collect_candidates({"page": page, "post": post_obj}) or []
            related = []
            for i, cand in enumerate(cands):
                related.append(_cand_to_related(cand, i, post_lang, post_id, news_source=(page.get("news",{}).get("source") or raw.get("source") or "unknown")))

            # se já veio relatedArticles no page, adiciona também
            ra = page.get("relatedArticles") or []
            if isinstance(ra, list) and ra:
                for j, a in enumerate(ra):
                    # normaliza no formato esperado, ignorando embeddings
                    url = a.get("url")
                    title = a.get("title")
                    pub = try_parse_pub(a.get("publishedAt"))
                    src = (a.get("source",{}) or {}).get("name") or _domain_from_url(url) or "unknown"
                    text = ""
                    chs = a.get("chunks") or []
                    if chs and isinstance(chs, list) and chs and isinstance(chs[0], dict):
                        text = (chs[0].get("text") or "").strip()
                    elif a.get("body"):
                        text = (a.get("body") or "").strip()
                    related.append({
                        "collectedFrom": a.get("collectedFrom") or "unknown",
                        "evidenceId": f"{post_id}-ra-{j}",
                        "source": {
                            "name": src,
                            "domain": _domain_from_url(url),
                            "orgType": "news",
                            "ifcn": False,
                            "baseWeight": float(((a.get("source") or {}).get("baseWeight")) or 0.60)
                        },
                        "url": url,
                        "title": title,
                        "lang": post_lang,
                        "publishedAt": pub,
                        "scoresFromProducer": a.get("scoresFromProducer") or {},
                        "chunks": ([{"chunkId":"0","text": text}] if text else [])
                    })

            jobs.append({
                "schemaVersion": "verify.jobs.v1",
                "jobId": f"{topic_id}-{post_id}",
                "topicId": topic_id,
                "post": {
                    "postId": post_id,
                    "authorId": post_obj.get("authorId"),
                    "cleanText": post_text,
                    "lang": post_lang,
                    "keywords": (post_obj.get("keywords") or page.get("keywords") or []),
                    "createdAt": created
                },
                "relatedArticles": related
            })
        return jobs

    # Sem pages → um único job
    post_obj = raw.get("post") or {}
    post_lang = (post_obj.get("lang") or raw.get("lang") or "pt").strip()
    post_id = (post_obj.get("postId") or raw.get("postId"))
    if not post_id:
        seed = (post_obj.get("externalId") or raw.get("externalId") or json.dumps(raw, ensure_ascii=False)[:512])
        post_id = _stable_id("post", seed)
    created = (post_obj.get("createdAt") or raw.get("createdAt") or now_utc_iso())
    post_text = _best_post_text({"post": post_obj, **raw}) or ""

    topic_id = (raw.get("topicId") or _stable_id("topic", json.dumps(raw, ensure_ascii=False)[:512]))

    # candidates
    cands = _collect_candidates(raw) or []
    related = []
    for i, cand in enumerate(cands):
        related.append(_cand_to_related(cand, i, post_lang, post_id, news_source=(raw.get("news",{}).get("source") or raw.get("source") or "unknown")))

    # relatedArticles já pronto (se vier)
    ra = raw.get("relatedArticles") or []
    if isinstance(ra, list) and ra:
        for j, a in enumerate(ra):
            url = a.get("url")
            title = a.get("title")
            pub = try_parse_pub(a.get("publishedAt"))
            src = (a.get("source",{}) or {}).get("name") or _domain_from_url(url) or "unknown"
            text = ""
            chs = a.get("chunks") or []
            if chs and isinstance(chs, list) and chs and isinstance(chs[0], dict):
                text = (chs[0].get("text") or "").strip()
            elif a.get("body"):
                text = (a.get("body") or "").strip()
            related.append({
                "collectedFrom": a.get("collectedFrom") or "unknown",
                "evidenceId": f"{post_id}-ra-{j}",
                "source": {
                    "name": src,
                    "domain": _domain_from_url(url),
                    "orgType": "news",
                    "ifcn": False,
                    "baseWeight": float(((a.get("source") or {}).get("baseWeight")) or 0.60)
                },
                "url": url,
                "title": title,
                "lang": post_lang,
                "publishedAt": pub,
                "scoresFromProducer": a.get("scoresFromProducer") or {},
                "chunks": ([{"chunkId":"0","text": text}] if text else [])
            })

    return [{
        "schemaVersion": "verify.jobs.v1",
        "jobId": f"{topic_id}-{post_id}",
        "topicId": topic_id,
        "post": {
            "postId": post_id,
            "authorId": post_obj.get("authorId"),
            "cleanText": post_text,
            "lang": post_lang,
            "keywords": (post_obj.get("keywords") or raw.get("keywords") or []),
            "createdAt": created
        },
        "relatedArticles": related
    }]

# ---------- Seleção de evidências (IGNORA embeddings) ----------
def select_topk_text_only(job: dict, k: int = TOPK) -> List[dict]:
    post_text = job.get("post",{}).get("cleanText") or ""
    created_at = job.get("post",{}).get("createdAt") or now_utc_iso()

    cands: List[dict] = []
    for art in (job.get("relatedArticles") or []):
        baseW = float(((art.get("source") or {}).get("baseWeight")) or 0.60)
        pub   = art.get("publishedAt")
        fresh = 0.5
        if pub:
            d = days_between(created_at, pub)
            if d is not None:
                # mais novo → score maior; usa decaimento suave
                fresh = float(np.exp(-max(d,0)/130.0))

        # overlap de texto entre post e trecho/título
        excerpt = ""
        chs = art.get("chunks") or []
        if chs and isinstance(chs, list) and chs and isinstance(chs[0], dict):
            excerpt = chs[0].get("text") or ""
        text_for_match = (art.get("title") or "") + " " + (excerpt or "")
        overlap = _score_overlap(post_text, text_for_match)

        # score do produtor (se houver), normalizado
        prod = art.get("scoresFromProducer") or {}
        raw_final = prod.get("final") or prod.get("score") or 0.0
        try:
            rf = float(raw_final)
            if rf > 1.0: rf = rf/100.0  # comum vir em %
            rf = clamp01(rf)
        except Exception:
            rf = 0.0

        hybrid = 0.55*overlap + 0.25*baseW + 0.15*fresh + 0.05*rf

        art["_overlap"] = overlap
        art["_fresh"] = fresh
        art["_hybrid"] = hybrid
        cands.append(art)

    cands = sorted(cands, key=lambda z: z["_hybrid"], reverse=True)[:k]
    return cands

# ---------- LLM: Judge ----------
SYSTEM_PROMPT = (
"You are a rigorous Fact-Check Judge.\n"
"Respond ONLY with strict valid JSON, no markdown, no comments.\n"
'Schema: {"decision":"approved|rejected|inconclusive",'
'"stance":"supports|refutes|not_enough_evidence",'
'"confidence":0.0-1.0,'
'"explanation":"pt-BR, curta e objetiva, citando as razões com base nos trechos fornecidos",'
'"citations":[{"title":str,"url":str}],'
'"education_tip":"pt-BR, 1-2 frases com dicas práticas para evitar desinformação"}\n'
"Rules:\n"
"- Julgue apenas com base nas evidências fornecidas (títulos/trechos/links).\n"
"- Se refutam claramente o post: decision=rejected; stance=refutes.\n"
"- Se apoiam claramente: decision=approved; stance=supports.\n"
"- Se insuficientes/ambíguas: decision=inconclusive; stance=not_enough_evidence.\n"
"- Seja conservador quando as evidências forem fracas ou contraditórias.\n"
"- Não invente URLs; só use as fornecidas."
)

def build_user_prompt(post_text: str, evs: List[dict]) -> str:
    lines = []
    lines.append("POST DO USUÁRIO:")
    lines.append(post_text.strip() or "(sem texto fornecido; julgue apenas pelas evidências)")
    lines.append("\nEVIDÊNCIAS (top-K):")
    for i, e in enumerate(evs, 1):
        title = e.get("title","")
        src   = (e.get("source") or {}).get("name","")
        url   = e.get("url","")
        pub   = e.get("publishedAt") or ""
        excerpt = ""
        chs = e.get("chunks") or []
        if chs and isinstance(chs, list) and chs and isinstance(chs[0], dict):
            excerpt = short(chs[0].get("text") or "", 900)
        lines.append(f"{i}. [{src}] {title} ({pub})\nURL: {url}\nTrecho: {excerpt}\n")
    lines.append("TAREFA: Julgue a veracidade do POST com base nas evidências acima e responda estritamente no JSON do schema.")
    return "\n".join(lines)

def heuristic_fallback(post_text: str, evs: List[dict]) -> dict:
    blob = (post_text or "") + " " + " ".join([
        (e.get("title","") + " " + ((e.get("chunks") or [{}])[0].get("text") or "")) for e in evs
    ])
    blob = blob.lower()
    neg_tokens = ["falso","enganoso","boato","mentira","não procede","sem evidências","desmentido","fake"]
    pos_tokens = ["verdade","confirmado","procedente","comprovado"]
    decision = "inconclusive"; stance = "not_enough_evidence"; conf = 0.40
    if any(tok in blob for tok in neg_tokens):
        decision, stance, conf = "rejected", "refutes", 0.60
    elif any(tok in blob for tok in pos_tokens):
        decision, stance, conf = "approved", "supports", 0.60
    citations = [{"title": e.get("title",""), "url": e.get("url","")} for e in evs[:3] if e.get("url")]
    return {
        "decision": decision,
        "stance": stance,
        "confidence": conf,
        "explanation": "Julgamento heurístico simples a partir de palavras-sinal nas evidências.",
        "citations": citations,
        "education_tip": "Verifique data, contexto e fonte; evite conclusões de manchete."
    }

async def call_openai_judge(post_text: str, evs: List[dict]) -> dict:
    prompt = build_user_prompt(post_text, evs)
    try:
        resp = openai_client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role":"system","content":SYSTEM_PROMPT},
                {"role":"user","content":prompt}
            ],
            temperature=0.0,
            response_format={"type":"json_object"}
        )
        js = resp.choices[0].message.content
        data = json.loads(js)
        # sane defaults
        data["decision"]   = data.get("decision","inconclusive")
        data["stance"]     = data.get("stance","not_enough_evidence")
        data["confidence"] = float(data.get("confidence",0.5))
        data["explanation"]= data.get("explanation","")
        if not isinstance(data.get("citations",[]), list): data["citations"]=[]
        if "education_tip" not in data: data["education_tip"] = ""
        return data
    except Exception as e:
        return heuristic_fallback(post_text, evs)

# ---------- Pipeline de um job ----------
async def handle_job(producer: AIOKafkaProducer, raw: dict):
    t0 = time.time()

    # Normaliza → lista de jobs (1 ou N)
    jobs = convert_any_to_job(raw)
    for job in jobs:
        await _process_single_job(producer, job, t0)

async def _process_single_job(producer: AIOKafkaProducer, job: dict, t0: float):
    post = job.get("post") or {}
    post_text = (post.get("cleanText") or "").strip()
    post_id   = post.get("postId") or _stable_id("post", json.dumps(post, ensure_ascii=False)[:256])
    job_id    = job.get("jobId") or _stable_id("job", post_id + now_utc_iso())
    related   = job.get("relatedArticles") or []

    if not related:
        # tenta coletar de novo como fallback (caso job tenha vindo sem related)
        related = []
    # se mesmo assim não houver, publica INCONCLUSIVO mas com IDs e latência corretos
    if not related:
        out = {
            "version":"rag.results.v1",
            "jobId": job_id,
            "postId": post_id,
            "stance": "not_enough_evidence",
            "decision": "inconclusive",
            "confidence": 0.0,
            "scores": {
                "relevance_llm": None,
                "evidence_score": None,
                "freshness_avg": None
            },
            "explanation": "Não há evidências disponíveis para avaliar a veracidade do post do usuário.",
            "citations": [],
            "education_tip": "Sempre busque fontes confiáveis e verifique a informação antes de compartilhar.",
            "latency_ms": int((time.time()-t0)*1000),
            "model_used": MODEL_NAME if USE_OPENAI else "heuristic"
        }
        await send_out(producer, out)
        return

    # se o post_text vier vazio, cai para o título da 1ª evidência
    if not post_text:
        fallback_text = (related[0].get("title") or "") + " " + ((related[0].get("chunks") or [{}])[0].get("text") or "")
        post_text = fallback_text.strip()

    # 2) Seleciona top-K evidências (TEXTO-ONLY)
    topk = select_topk_text_only({"post": post, "relatedArticles": related}, k=TOPK)
    fresh_avg = float(np.mean([a.get("_fresh",0.5) for a in topk])) if topk else None
    best_ev_score = max([a.get("_hybrid",0.0) for a in topk]) if topk else None

    # 3) Chama o LLM (ou heurística)
    if USE_OPENAI:
        judge = await call_openai_judge(post_text, topk)
    else:
        judge = heuristic_fallback(post_text, topk)

    decision   = judge.get("decision","inconclusive")
    stance     = judge.get("stance","not_enough_evidence")
    confidence = float(judge.get("confidence",0.5))
    explanation= judge.get("explanation","")
    citations  = judge.get("citations",[])
    edu_tip    = judge.get("education_tip","")

    # 4) Monta saída final
    out = {
        "version":"rag.results.v1",
        "jobId": job_id,
        "postId": post_id,
        "stance": stance,
        "decision": decision,
        "confidence": round(confidence,3),
        "scores": {
            "relevance_llm": None,                # (opcional) 2ª passada de relevância
            "evidence_score": round(best_ev_score,3) if best_ev_score is not None else None,
            "freshness_avg": round(fresh_avg,3) if fresh_avg is not None else None
        },
        "explanation": explanation,
        "citations": citations,                   # [{title,url}]
        "education_tip": edu_tip,
        "latency_ms": int((time.time()-t0)*1000),
        "model_used": MODEL_NAME if USE_OPENAI else "heuristic"
    }

    await send_out(producer, out)

async def send_out(producer: AIOKafkaProducer, obj: dict):
    payload = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    await producer.send_and_wait(TOPIC_OUT, payload)

# ---------- Loop Kafka ----------
async def main():
    consumer = AIOKafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="latest",  # comece do fim; mude para "earliest" se quiser reprocessar
        value_deserializer=lambda v: v  # bytes -> tratamos manualmente
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: v
    )

    await consumer.start()
    await producer.start()
    print(f"[verify-worker] listening {KAFKA_BOOTSTRAP} topic_in={TOPIC_IN} -> topic_out={TOPIC_OUT}  model={MODEL_NAME}  openai={USE_OPENAI}  (IGNORING embeddings)")

    stop = asyncio.Event()

    def _graceful(*_):
        stop.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, _graceful)
        except NotImplementedError:
            pass

    try:
        while not stop.is_set():
            msg = await consumer.getone()
            try:
                raw = json.loads(msg.value.decode("utf-8"))
            except Exception as e:
                print("[warn] invalid json, skipping:", e)
                continue
            try:
                await handle_job(producer, raw)
            except Exception as e:
                print("[error] job failed:", e)
    finally:
        await consumer.stop()
        await producer.stop()
        print("[verify-worker] stopped")

if __name__ == "__main__":
    asyncio.run(main())
