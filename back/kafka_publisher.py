# kafka_publisher.py
from kafka_config import kafka_manager
from db.models import PostDB
from schemas import PostBase
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class PostPublisher:
    @staticmethod
    def publish_post(post_data: PostBase) -> bool:
        """Publica um novo post no tópico raw.posts"""
        try:
            message = {
                "action": "post_created",
                "post_id": str(post_data.id) if hasattr(post_data, 'id') else None,
                "author_id": 42,  # Exemplo fixo, ajuste conforme necessário
                "text": post_data.text,
                "timestamp": post_data.created_at.isoformat() if hasattr(post_data, 'created_at') else None
            }
            
            success = kafka_manager.publish_message(
                topic="raw.posts",
                value=message,
                key=message.get("post_id")
            )
            
            if success:
                logger.info(f"Post publicado no Kafka: {message}")
            else:
                logger.error("Falha ao publicar post no Kafka")
                
            return success
            
        except Exception as e:
            logger.error(f"Erro ao publicar post: {e}")
            return False
    
    @staticmethod
    def publish_post_created(db_post: PostDB) -> bool:
        """Publica um post após ser criado no banco"""
        message = {
            "action": "post_created",
            "post_id": db_post.id,
            "text": db_post.text,
            "created_at": db_post.created_at.isoformat(),
            "event_type": "post_creation"
        }
        
        return kafka_manager.publish_message(
            topic="raw.posts",
            value=message,
            key=db_post.id
        )