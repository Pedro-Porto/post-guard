from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
from typing import Dict, List, Optional
import uuid

class PostBase(BaseModel):
    text: str = Field(..., min_length=1, max_length=10000, description="Texto do post para análise")
    # Configuração para manter camelCase no JSON
    model_config = ConfigDict(
        populate_by_name=True,
        json_encoders={
            datetime: lambda v: v.isoformat()
        }
    )
class PostCreate(PostBase):
    text: str = Field(..., min_length=1, max_length=10000, description="Texto do post para análise")

class PostResponse(PostBase):
    id: str
    text: str
    created_at: datetime
    
    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True  # Permite usar alias e nome original
    )


from pydantic import BaseModel, Field, ConfigDict, validator
from datetime import datetime
from typing import Dict, List, Optional
import numpy as np

class RAGResultBase(BaseModel):
    version: str = Field(..., description="Versão do resultado RAG")
    jobId: str = Field(..., description="ID do job de processamento")
    postId: str = Field(..., description="ID do post analisado")
    stance: str = Field(..., description="Posição detectada")
    decision: str = Field(..., description="Decisão tomada")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confiança da predição")
    scores: Dict[str, float] = Field(..., description="Scores detalhados")
    explanation: str = Field(..., description="Explicação da decisão")
    citations: List[str] = Field(..., description="Citações/referências usadas")
    latency_ms: int = Field(..., ge=0, description="Latência em milissegundos")
    model_used: str = Field(..., description="Modelo utilizado")

    # Validador para scores
    @validator('scores')
    def validate_scores(cls, v):
        required_keys = ['relevance_llm', 'evidence_score', 'freshness_avg']
        for key in required_keys:
            if key not in v:
                raise ValueError(f"Score '{key}' é obrigatório")
        return v

    # Configuração para usar alias de campo
    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "example": {
                "version": "rag.results.v1",
                "jobId": "job_12345",
                "postId": "post_67890",
                "stance": "FAVOR",
                "decision": "APPROVED",
                "confidence": 0.85,
                "scores": {
                    "relevance_llm": 0.92,
                    "evidence_score": 0.88,
                    "freshness_avg": 0.95
                },
                "explanation": "O post apresenta argumentos relevantes com evidências recentes.",
                "citations": ["https://exemplo.com/ref1", "https://exemplo.com/ref2"],
                "latency_ms": 1250,
                "model_used": "gpt-4o-mini"
            }
        }
    )

class RAGResultCreate(RAGResultBase):
    pass

class RAGResultResponse(RAGResultBase):
    id: int
    created_at: datetime
    
    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        fields = {
            'jobId': {'alias': 'job_id'},
            'postId': {'alias': 'post_id'}
        }
    )
