from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy.orm import Session
from fastapi.concurrency import asynccontextmanager
from contextlib import asynccontextmanager
import schemas
import services
import threading
import logging
import sys

from kafka_subscriber import RAGResultSubscriber
from fastapi.middleware.cors import CORSMiddleware
from db.repository import init_db, get_db

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

# Instância do subscriber
rag_subscriber = RAGResultSubscriber()

def start_kafka_consumer():
    """Função para iniciar o consumer Kafka em thread separada"""
    try:
        rag_subscriber.start_consuming()
    except Exception as e:
        logger.error(f"Erro no consumer Kafka: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Evento de inicialização da aplicação"""
    try:
        # Inicializar banco de dados
        init_db()
        logger.info("Banco de dados inicializado")
        
        # Iniciar consumer Kafka em thread separada
        kafka_thread = threading.Thread(target=start_kafka_consumer, daemon=True)
        kafka_thread.start()
        logger.info("Consumer Kafka iniciado em thread separada")
        
        yield  # ← IMPORTANTE: isso permite que a app continue
        
    except Exception as e:
        logger.error(f"Erro na inicialização: {e}")
        raise

# Passe o lifespan para o FastAPI
app = FastAPI(
    title="RAG Analysis API",
    description="API para análise de textos usando RAG",
    version="1.0.0",
    lifespan=lifespan  # ← AGORA CORRETO
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    logger.info("Endpoint raiz acessado")
    return {"message": "Hello, FastAPI!"}

##### POSTS #####
@app.get("/posts/{post_id}")
def read_item(post_id: str, db: Session = Depends(get_db)):
    logger.info(f"Buscando post ID: {post_id}")
    post = services.PostService.get_post(db, post_id)
    if not post:
        logger.warning(f"Post não encontrado: {post_id}")
        raise HTTPException(status_code=404, detail="Post not found")
    return post

@app.post("/posts")
def create_post(request: schemas.PostBase, db: Session = Depends(get_db)):
    try:
        logger.info("Criando novo post")
        result = services.PostService.create_post(db, request)
        logger.info(f"Post criado com ID: {result.id}")
        return result
    except Exception as e:
        logger.error(f"Erro ao criar post: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao criar post: {str(e)}"
        )

@app.get("/posts")
def get_posts(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    logger.info(f"Listando posts - skip: {skip}, limit: {limit}")
    posts = services.PostService.get_posts(db, skip=skip, limit=limit)
    return posts

##### RAG #####
@app.get("/rag")
def get_rag_results(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    logger.info(f"Listando resultados RAG - skip: {skip}, limit: {limit}")
    results = services.RAGService.get_all_rag_results(db, skip=skip, limit=limit)
    return results

# Health check
@app.get("/health")
def health_check():
    logger.info("Health check executado")
    return {"status": "healthy", "service": "RAG API"}