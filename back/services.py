from sqlalchemy.orm import Session
from sqlalchemy import select
from typing import List, Optional, Dict
import time
import uuid
from db import models
from kafka_publisher import PostPublisher
import schemas

class PostService:
    @staticmethod
    def create_post(db: Session, post: schemas.PostBase) -> models.PostDB:
        db_post = models.PostDB(
            text=post.text
        )
        db.add(db_post)
        db.commit()
        db.refresh(db_post)
        PostPublisher.publish_post_created(db_post)
        return db_post

    @staticmethod
    def get_post(db: Session, post_id: str) -> Optional[models.PostDB]:
        return db.query(models.PostDB).filter(models.PostDB.id == post_id).first()

    @staticmethod
    def get_posts(db: Session, skip: int = 0, limit: int = 100) -> List[models.PostDB]:
        return db.query(models.PostDB).offset(skip).limit(limit).all()
    
class RAGService:
    @staticmethod
    def analyze_text(db: Session, text: str, model_version: str) -> schemas.RAGResultResponse:
        start_time = time.time()
        
        # Simulação de análise RAG (substitua por chamada real ao modelo)
        stance = "neutral"
        decision = "review"
        confidence = 0.75
        scores = {"support": 0.3, "oppose": 0.2, "neutral": 0.5}
        explanation = "The text is neutral based on the analysis."
        citations = ["Document A", "Document B"]
        latency_ms = int((time.time() - start_time) * 1000)
        model_used = model_version
        
        # Criação do post
        post_id = str(uuid.uuid4())
        db_post = models.PostDB(id=post_id, text=text)
        db.add(db_post)
        db.commit()
        db.refresh(db_post)
        
        # Criação do resultado RAG
        rag_result = models.RAGResultDB(
            post_id=post_id,
            version=model_version,
            stance=stance,
            decision=decision,
            confidence=confidence,
            scores=scores,
            explanation=explanation,
            citations=citations,
            latency_ms=latency_ms,
            model_used=model_used
        )
        db.add(rag_result)
        db.commit()
        db.refresh(rag_result)
        
        return schemas.RAGResultResponse.from_orm(rag_result)
    
    @staticmethod
    def register_rag_result(db: Session, rag_data: schemas.RAGResultCreate, job_id: str) -> models.RAGResultDB:
        db_rag = models.RAGResultDB(
            job_id=job_id,
            post_id=rag_data.postId,
            version=rag_data.version,
            stance=rag_data.stance,
            decision=rag_data.decision,
            confidence=rag_data.confidence,
            scores=rag_data.scores,
            explanation=rag_data.explanation,
            citations=rag_data.citations,
            latency_ms=rag_data.latency_ms,
            model_used=rag_data.model_used
        )
        db.add(db_rag)
        db.commit()
        db.refresh(db_rag)
        return db_rag
    
    @staticmethod
    def get_all_rag_results(db: Session) -> List[models.RAGResultDB]:
        return db.query(models.RAGResultDB).all()