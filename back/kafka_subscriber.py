# kafka_subscriber.py
from kafka_config import kafka_manager
from services import RAGService
from db.repository import get_db
from sqlalchemy.orm import Session
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class RAGResultSubscriber:
    def __init__(self):
        self.group_id = "rag-results-consumer-group"
        self.topic = "rag.results"
    
    def process_rag_result(self, message: Dict[str, Any]):
        """Processa mensagens de resultados RAG"""
        try:
            logger.info(f"Processando resultado RAG: {message}")
            
            # Validar campos obrigatórios
            required_fields = ['version', 'jobId', 'postId', 'stance', 'decision', 
                             'confidence', 'scores', 'explanation', 'citations', 
                             'latency_ms', 'model_used']
            
            for field in required_fields:
                if field not in message:
                    logger.error(f"Campo obrigatório faltando: {field}")
                    return
            
            # Obter sessão do banco
            db: Session = next(get_db())
            
            try:
                # Salvar resultado no banco
                rag_service = RAGService()
                
                result = rag_service.register_rag_result(db, message,message['jobId'])
                
                logger.info(f"Resultado RAG salvo com ID: {result.id}")
                
            except Exception as e:
                logger.error(f"Erro ao salvar resultado RAG: {e}")
                db.rollback()
                
        except Exception as e:
            logger.error(f"Erro ao processar mensagem RAG: {e}")
    
    def start_consuming(self):
        """Inicia o consumo de mensagens do tópico RAG"""
        logger.info(f"Iniciando consumo do tópico: {self.topic}")
        
        kafka_manager.subscribe_to_topic(
            topic=self.topic,
            group_id=self.group_id,
            callback=self.process_rag_result
        )