try:
    from sentence_transformers import SentenceTransformer
except ImportError:
    SentenceTransformer = None
import os, sys, json, time, logging, signal, re, hashlib, base64
from datetime import datetime, timezone
from urllib.parse import urlparse, urljoin
from typing import Optional, Dict, Any, Tuple

import requests
from kafka import KafkaConsumer, KafkaProducer

try:
    import trafilatura
    from trafilatura.settings import use_config
except ImportError:
    raise SystemExit("Instale: pip install trafilatura kafka-python python-dateutil selenium")

# ==================== Config ====================
KAFKA_BROKERS   = os.getenv("KAFKA_BROKERS", "localhost:60118")
TOPIC_IN        = os.getenv("TOPIC_IN",  "news.matches")
TOPIC_OUT       = os.getenv("TOPIC_OUT", "verify.jobs")
DLQ_TOPIC       = os.getenv("DLQ_TOPIC", "verify.jobs.dlq")
GROUP_ID        = os.getenv("GROUP_ID",  "pages-writer-v1")

EMBED_MODEL     = os.getenv("EMBED_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
EMBED_ENABLE    = os.getenv("EMBED_ENABLE", "1") == "1"
EMBED_ROUND     = int(os.getenv("EMBED_ROUND", "6"))


SLEEP_BETWEEN   = float(os.getenv("SLEEP_BETWEEN", "0.6"))
HTTP_TIMEOUT    = float(os.getenv("HTTP_TIMEOUT", "18"))
HTTP_RETRIES    = int(os.getenv("HTTP_RETRIES", "2"))
MAX_REDIRECT_HOPS = int(os.getenv("MAX_REDIRECT_HOPS", "6"))

TRY_AMP         = os.getenv("TRY_AMP", "1") == "1"
MAX_HTML_MB     = float(os.getenv("MAX_HTML_MB", "5.0"))
MIN_TEXT_CH_OK  = int(os.getenv("MIN_TEXT_CH_OK", "400"))
MAX_TEXT_CH     = int(os.getenv("MAX_TEXT_CH", "250000"))

SELENIUM_ENABLE = os.getenv("SELENIUM_ENABLE", "1") == "1"
SELENIUM_PAGELOAD_TIMEOUT = int(os.getenv("SELENIUM_PAGELOAD_TIMEOUT", "25"))
SELENIUM_SCRIPT_TIMEOUT   = int(os.getenv("SELENIUM_SCRIPT_TIMEOUT", "20"))
SELENIUM_WAIT_TOTAL       = float(os.getenv("SELENIUM_WAIT_TOTAL", "10")) 
SELENIUM_POLL_INTERVAL    = float(os.getenv("SELENIUM_POLL_INTERVAL", "0.25"))

HTTP_HEADERS = {
    "User-Agent": os.getenv(
        "HTTP_UA",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger("trafilatura").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# ==================== Kafka ====================
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
    max_poll_records=30,
)

_running = True
def _graceful(*_):
    global _running
    _running = False
signal.signal(signal.SIGINT,  _graceful)
signal.signal(signal.SIGTERM, _graceful)

# ==================== Embeddings ====================
_embedder = None

def _ensure_embedder():
    global _embedder
    if not EMBED_ENABLE:
        return None
    if SentenceTransformer is None:
        logging.warning("sentence-transformers não disponível; embeddings desativados.")
        return None
    if _embedder is None:
        try:
            _embedder = SentenceTransformer(EMBED_MODEL)
            logging.info("Embedder carregado: %s", EMBED_MODEL)
        except Exception as e:
            logging.warning("Falha ao carregar modelo de embeddings (%s): %s", EMBED_MODEL, e)
            _embedder = None
    return _embedder

def embed_text(text: str):
    """
    Retorna lista[float] arredondada (ou []) se indisponível.
    Não faz chamadas de rede (modelo local).
    """
    if not text or not EMBED_ENABLE:
        return []
    emb = _ensure_embedder()
    if emb is None:
        return []
    try:
        vec = emb.encode(text, normalize_embeddings=True)  # float32 numpy array
        if EMBED_ROUND and EMBED_ROUND >= 0:
            return [round(float(x), EMBED_ROUND) for x in vec.tolist()]
        return vec.tolist()
    except Exception as e:
        logging.warning("Falha ao gerar embedding: %s", e)
        return []


# ==================== Selenium (uma instância global) ====================
driver = None
def selenium_start():
    """Inicializa um Chrome/Chromium headless reutilizável (Selenium 4+).
    O Selenium Manager baixa o driver automaticamente em muitos ambientes."""
    global driver
    if not SELENIUM_ENABLE or driver is not None:
        return

    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-features=NetworkService,NetworkServiceInProcess")
    opts.add_argument("--window-size=1280,1024")
    opts.add_argument(f"user-agent={HTTP_HEADERS['User-Agent']}")

    driver = webdriver.Chrome(options=opts)

    driver.set_page_load_timeout(SELENIUM_PAGELOAD_TIMEOUT)
    driver.set_script_timeout(SELENIUM_SCRIPT_TIMEOUT)
    logging.info("Selenium driver inicializado (headless).")

def selenium_stop():
    global driver
    if driver is not None:
        try:
            driver.quit()
        except Exception:
            pass
        driver = None
        logging.info("Selenium driver finalizado.")

# ==================== Utils ====================
session = requests.Session()
session.headers.update(HTTP_HEADERS)

def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

def parse_json_loose(raw: bytes):
    s = (raw or b"").decode("utf-8","ignore").strip().lstrip("\ufeff")
    if not s: raise ValueError("mensagem vazia")
    try:
        obj = json.loads(s)
        return json.loads(obj) if isinstance(obj, str) else obj
    except json.JSONDecodeError:
        inner = json.loads(s)
        return json.loads(inner) if isinstance(inner, str) else inner

def clamp_text(t: Optional[str], n: int) -> Optional[str]:
    if t is None: return None
    return t if len(t) <= n else t[:n]

def size_ok(content: bytes, max_mb: float) -> bool:
    return (len(content) / (1024*1024.0)) <= max_mb

def domain(u: str) -> str:
    try: return urlparse(u).netloc
    except: return ""

def is_news_google_articles(u: str) -> bool:
    try:
        p = urlparse(u)
        return p.netloc.startswith("news.google.") and p.path.startswith("/rss/articles/")
    except:
        return False

def _http_get(u: str, allow_redirects=True):
    last = None
    for i in range(HTTP_RETRIES + 1):
        try:
            r = session.get(u, timeout=HTTP_TIMEOUT, allow_redirects=allow_redirects)
            return (r.content or b""), str(r.url), r.status_code, dict(r.headers or {})
        except requests.RequestException as e:
            last = e
            time.sleep(0.35 + 0.25*i)
    logging.warning("GET falhou: %s (%s)", u, last)
    return None, None, None, None

META_REFRESH_RE = re.compile(r'<meta\s+http-equiv=["\']refresh["\']\s+content=["\'][^;]*;\s*url=([^"\']+)["\']', re.I)
AMP_LINK_RE     = re.compile(r'<link[^>]+rel=["\']amphtml["\'][^>]+href=["\']([^"\']+)["\']', re.I)

def maybe_find_amp(html_text: str, base_url: str) -> Optional[str]:
    m = AMP_LINK_RE.search(html_text or "")
    return urljoin(base_url, m.group(1)) if m else None

# ==================== Resolução via Selenium ====================
def resolve_with_selenium(u: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Abre a URL no Selenium e espera até que saia do domínio news.google.*
    Retorna (final_url, page_source) — ou (None, None) se não rolar.
    """
    if not SELENIUM_ENABLE:
        return None, None

    try:
        selenium_start()
    except Exception as e:
        logging.warning("Selenium indisponível (%s).", e)
        return None, None

    try:
        driver.get(u)
    except Exception as e:
        logging.info("Falha inicial ao carregar no Selenium (%s): %s", u, e)
        return None, None

    t0 = time.time()
    last_url = driver.current_url
    if not domain(last_url).startswith("news.google."):
        return last_url, driver.page_source

    while time.time() - t0 < SELENIUM_WAIT_TOTAL:
        time.sleep(SELENIUM_POLL_INTERVAL)
        try:
            cur = driver.current_url
            if cur != last_url:
                last_url = cur
            if not domain(cur).startswith("news.google."):
                return cur, driver.page_source
        except Exception:
            break

    try:
        anchors = driver.find_elements("css selector", "a[href]")
        for a in anchors:
            href = a.get_attribute("href") or ""
            if href and not domain(href).startswith("news.google."):
                driver.get(href)
                time.sleep(0.5)
                return driver.current_url, driver.page_source
    except Exception:
        pass

    return None, None

# ==================== Trafilatura ====================
tconfig = use_config()
tconfig.set("DEFAULT", "EXTRACTION_TIMEOUT", "0")
tconfig.set("DEFAULT", "MIN_EXTRACTED_SIZE", "300")
tconfig.set("DEFAULT", "STRICT", "0")
tconfig.set("DEFAULT", "RESPECT_ROBOTS_TXT", "0")

def extract_text(url: str, html_bytes_or_str) -> str:
    if isinstance(html_bytes_or_str, str):
        html_bytes = html_bytes_or_str.encode("utf-8", "ignore")
    else:
        html_bytes = html_bytes_or_str

    j = trafilatura.extract(
        filecontent=html_bytes,
        include_comments=False,
        include_tables=False,
        include_images=False,
        favor_precision=True,
        deduplicate=True,
        no_fallback=False,
        config=tconfig,
        url=url,
        output_format="json",
    )
    if j:
        try:
            obj = json.loads(j)
            return obj.get("text") or ""
        except Exception:
            return j if isinstance(j, str) else ""
    return ""

def get_clean_text_for_url(requested_url: str) -> Tuple[str, Optional[str]]:
    """
    Retorna (texto_limpo, final_url_para_log).
    - Se for Google News, tenta Selenium (JS), senão cai nos fallbacks.
    - Para não-GN, usa requests direto.
    """
    final_for_log = None

    if is_news_google_articles(requested_url) and SELENIUM_ENABLE:
        final_url, page_src = resolve_with_selenium(requested_url)
        if final_url and page_src:
            final_for_log = final_url
            text = extract_text(final_url, page_src)
            if len(text) >= MIN_TEXT_CH_OK:
                return clamp_text(text, MAX_TEXT_CH) or "", final_for_log
            html, final_url2, _, _ = _http_get(final_url, allow_redirects=True)
            if html and size_ok(html, MAX_HTML_MB):
                t2 = extract_text(final_url2 or final_url, html)
                if len(t2) > len(text):
                    return clamp_text(t2, MAX_TEXT_CH) or "", final_for_log
                return clamp_text(text, MAX_TEXT_CH) or "", final_for_log
        html, url_after, _, _ = _http_get(requested_url, allow_redirects=True)
        if html:
            s = html.decode("utf-8", "ignore")
            m = META_REFRESH_RE.search(s)
            if m:
                target = urljoin(url_after, m.group(1))
                html2, fin2, _, _ = _http_get(target, allow_redirects=True)
                if html2 and size_ok(html2, MAX_HTML_MB):
                    final_for_log = fin2 or target
                    t = extract_text(final_for_log, html2)
                    return clamp_text(t, MAX_TEXT_CH) or "", final_for_log
        return "", final_for_log

    html, final_url, _, _ = _http_get(requested_url, allow_redirects=True)
    if html is None:
        return "", None
    final_for_log = final_url

    if not size_ok(html, MAX_HTML_MB):
        return "", final_for_log

    text = extract_text(final_url, html)
    if len(text) >= MIN_TEXT_CH_OK:
        return clamp_text(text, MAX_TEXT_CH) or "", final_for_log

    if TRY_AMP:
        try:
            amp = maybe_find_amp(html.decode("utf-8","ignore"), final_url)
        except Exception:
            amp = None
        if amp:
            amp_html, amp_final, _, _ = _http_get(amp, allow_redirects=True)
            if amp_html and size_ok(amp_html, MAX_HTML_MB):
                t2 = extract_text(amp_final or amp, amp_html)
                if len(t2) > len(text):
                    return clamp_text(t2, MAX_TEXT_CH) or "", final_for_log

    fetched = trafilatura.fetch_url(final_url, config=tconfig)
    if fetched:
        j = trafilatura.extract(
            filecontent=fetched,
            include_comments=False,
            include_tables=False,
            include_images=False,
            favor_precision=True,
            deduplicate=True,
            no_fallback=False,
            config=tconfig,
            url=final_url,
            output_format="json",
        )
        if j:
            try:
                obj = json.loads(j)
                t3 = obj.get("text") or ""
            except Exception:
                t3 = j if isinstance(j, str) else ""
            if len(t3) > len(text):
                return clamp_text(t3, MAX_TEXT_CH) or "", final_for_log

    return clamp_text(text, MAX_TEXT_CH) or "", final_for_log

# ==================== Core ====================
SCHEMA_VERSION_OUT = "news.pages.v1"

def process_message(raw_bytes):
    in_msg = parse_json_loose(raw_bytes)

    out = {
        "topicId": in_msg.get("topicId"),
        "sourceBatch": in_msg.get("sourceBatch") or {},
        "pages": [],
        "createdAt": in_msg.get("createdAt") or now_iso(),
        "ingestedAt": now_iso(),
        "schemaVersion": SCHEMA_VERSION_OUT,
    }

    page_obj = dict(in_msg)
    if "news" not in page_obj or not isinstance(page_obj["news"], dict):
        page_obj["news"] = {}
    if "candidates" not in page_obj["news"] or not isinstance(page_obj["news"]["candidates"], list):
        page_obj["news"]["candidates"] = []

    for idx, c in enumerate(page_obj["news"]["candidates"]):
        if not isinstance(c, dict):
            continue
        url = c.get("url")
        if not url:
            c["rag"] = {"text": "",
            "finalUrl": final_url or url,
    "embedding": embed_text(text or "")}
            continue

        try:
            time.sleep(SLEEP_BETWEEN)
            text, final_url = get_clean_text_for_url(url)

            if final_url:
                logging.info("URL final [%d]: %s -> %s", idx, url, final_url)
            else:
                logging.info("URL final [%d]: %s -> (indefinida)", idx, url)

            c["rag"] = {"text": text or "",
            "finalUrl": final_url or url,
    "embedding": embed_text(text or "")}

        except Exception as e:
            logging.warning("Falha ao enriquecer %s: %s", url, e)
            c["rag"] = {"text": "",
            "finalUrl": final_url or url,
    "embedding": embed_text(text or "")}

    out["pages"].append(page_obj)
    return out

def main():
    logging.info("PAGES WRITER (Selenium) | in=%s out=%s | brokers=%s", TOPIC_IN, TOPIC_OUT, KAFKA_BROKERS)
    if SELENIUM_ENABLE:
        try:
            selenium_start()
        except Exception:
            logging.exception("Falha ao iniciar Selenium; seguindo sem ele.")
    consumer.subscribe([TOPIC_IN])
    consumer.poll(timeout_ms=1000)

    while _running:
        batch = consumer.poll(timeout_ms=1000)
        for tp, msgs in batch.items():
            for m in msgs:
                try:
                    if not m.value:
                        raise ValueError("mensagem vazia")
                    out = process_message(m.value)
                    key = (out.get("topicId") or "").encode() or None
                    producer.send(TOPIC_OUT, key=key, value=out).get(timeout=30)
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
                        }).get(timeout=15)
                    except Exception:
                        logging.exception("Falha ao enviar para DLQ")
        try:
            consumer.commit()
            producer.flush()
        except Exception as e:
            logging.warning("Commit/flush falhou: %s", e)

    logging.info("Encerrando...")
    try: consumer.close(5)
    except Exception: pass
    try: producer.flush(); producer.close(5)
    except Exception: pass
    selenium_stop()

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("Erro fatal")
        selenium_stop()
        sys.exit(1)

