import json
from confluent_kafka import Producer, KafkaException
from typing import Optional, Dict, Any
import logging

from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(
            f"Message delivered to topic {msg.topic()}, "
            f"partition {msg.partition()}, "
            f"offset {msg.offset()}"
        )


class KafkaMessageProducer:
    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.producer = None
        
    def connect(self) -> bool:
        try:
            logger.info(f"Connecting to Kafka at {self.config.KAFKA_BOOTSTRAP_SERVERS}")
            
            producer_config = {
                'bootstrap.servers': self.config.KAFKA_BOOTSTRAP_SERVERS,
                'acks': 'all',
                'retries': 3,
                'max.in.flight.requests.per.connection': 1,
                'compression.type': 'gzip',
                'linger.ms': 5,
                'batch.size': 16384,
            }
            
            self.producer = Producer(producer_config)
            logger.info("Successfully connected to Kafka")
            return True
        except KafkaException as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
    
    def send_message(
        self, 
        message: Dict[str, Any], 
        key: Optional[str] = None,
        topic: Optional[str] = None,
        callback: Optional[callable] = None
    ) -> bool:
        if not self.producer:
            logger.error("Producer not connected. Call connect() first.")
            return False
        
        target_topic = topic or self.config.KAFKA_TOPIC
        
        try:
            message_bytes = json.dumps(message).encode('utf-8')
            key_bytes = key.encode('utf-8') if key else None
            
            actual_callback = callback if callback else delivery_report
            
            self.producer.produce(
                topic=target_topic,
                key=key_bytes,
                value=message_bytes,
                on_delivery=actual_callback
            )
            
            return True
        except KafkaException as e:
            logger.error(f"Failed to send message: {e}")
            return False
        except TypeError as e:
            logger.error(f"Failed to serialize message: {e}")
            return False
    
    def send_messages_batch(
        self, 
        messages: list, 
        topic: Optional[str] = None
    ) -> int:
        success_count = 0
        for msg in messages:
            if isinstance(msg, dict):
                key = msg.get('key')
                value = msg.get('value', msg)
                if self.send_message(value, key, topic):
                    success_count += 1
            else:
                if self.send_message(msg, topic=topic):
                    success_count += 1
        
        logger.info(f"Successfully sent {success_count}/{len(messages)} messages")
        return success_count
    
    def flush(self, timeout: float = 10.0):
        if self.producer:
            remaining = self.producer.flush(timeout=timeout)
            if remaining > 0:
                logger.warning(f"{remaining} messages still in queue after flush")
            else:
                logger.info("Producer flushed successfully")
    
    def poll(self, timeout: float = 0.0):
        if self.producer:
            return self.producer.poll(timeout)
        return 0
    
    def close(self):
        if self.producer:
            self.flush()
            logger.info("Producer connection closed")


def main():
    import time
    
    producer = KafkaMessageProducer()
    if not producer.connect():
        logger.error("Failed to initialize producer")
        return
    
    try:
        for i in range(10):
            message = {
                "id": i,
                "timestamp": time.time(),
                "content": f"Test message {i}",
                "source": "kafka_producer.py"
            }
            producer.send_message(message, key=f"key-{i}")
            producer.poll(0.1)
            time.sleep(0.4)
        
        producer.flush()
        logger.info("All messages sent successfully")
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
