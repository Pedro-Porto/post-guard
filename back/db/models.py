# models.py
from sqlalchemy import Column, Integer, String, Text, Float, DateTime, JSON, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import validates
import uuid
import logging
Base = declarative_base()

class PostDB(Base):
    __tablename__ = "posts"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    text = Column(Text, nullable=False)
    created_at = Column(DateTime, default=func.now())

class RAGResultDB(Base):
    __tablename__ = "rag_results"
    
    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(String(100), nullable=False, index=True)  # Adicionado job_id
    post_id = Column(String(36), nullable=False, index=True)
    version = Column(String(50), nullable=False)
    stance = Column(String(50), nullable=False)
    decision = Column(String(100), nullable=False)
    confidence = Column(Float, nullable=False)
    scores = Column(JSON, nullable=False)  # Armazena dict como JSON
    explanation = Column(Text, nullable=False)
    citations = Column(JSON, nullable=False)  # Armazena list como JSON
    latency_ms = Column(Integer, nullable=False)
    model_used = Column(String(100), nullable=False)
    created_at = Column(DateTime, default=func.now())

    