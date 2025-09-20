# kafka_config.py
import json
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import logging
from typing import Dict, Any, Callable
import os

logger = logging.getLogger(__name__)

class KafkaManager:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.consumers = {}
        
    def create_producer(self):
        """Cria e retorna um producer Kafka"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3
            )
            logger.info(f"Producer Kafka conectado em {self.bootstrap_servers}")
            return self.producer
        except Exception as e:
            logger.error(f"Erro ao conectar producer Kafka: {e}")
            raise
    
    def create_consumer(self, group_id: str, auto_offset_reset: str = 'earliest'):
        """Cria e retorna um consumer Kafka"""
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=group_id,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                key_deserializer=lambda x: x.decode('utf-8') if x else None
            )
            logger.info(f"Consumer Kafka criado para grupo {group_id}")
            return consumer
        except Exception as e:
            logger.error(f"Erro ao criar consumer Kafka: {e}")
            raise
    
    def publish_message(self, topic: str, value: Dict[str, Any], key: str = None):
        """Publica mensagem no tópico Kafka"""
        if not self.producer:
            self.create_producer()
        
        try:
            future = self.producer.send(
                topic=topic,
                value=value,
                key=key
            )
            # Esperar confirmação
            future.get(timeout=10)
            logger.info(f"Mensagem publicada no tópico {topic}: {value}")
            return True
        except KafkaError as e:
            logger.error(f"Erro ao publicar mensagem no Kafka: {e}")
            return False
    
    def subscribe_to_topic(self, topic: str, group_id: str, callback: Callable):
        """Inscreve-se em um tópico e processa mensagens com callback"""
        consumer = self.create_consumer(group_id)
        consumer.subscribe([topic])
        
        logger.info(f"Inscrito no tópico {topic} com grupo {group_id}")
        
        try:
            for message in consumer:
                try:
                    logger.info(f"Mensagem recebida: {message.value}")
                    callback(message.value)
                except Exception as e:
                    logger.error(f"Erro ao processar mensagem: {e}")
                    # Continue processando outras mensagens
        except KeyboardInterrupt:
            logger.info("Consumer interrompido")
        finally:
            consumer.close()

# Instância global do Kafka Manager
kafka_manager = KafkaManager(
    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'andromeda.lasdpc.icmc.usp.br:60118')
)