import kafka_python_313_fix

import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
from typing import Optional, Dict, Any
import logging

from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaMessageProducer:
    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.producer = None
        
    def connect(self) -> bool:
        try:
            logger.info(f"Connecting to Kafka at {self.config.KAFKA_BOOTSTRAP_SERVERS}")
            self.producer = KafkaProducer(
                bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info("Successfully connected to Kafka")
            return True
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error connecting to Kafka: {e}")
            return False
    
    def send_message(
        self, 
        message: Dict[str, Any], 
        key: Optional[str] = None,
        topic: Optional[str] = None
    ) -> bool:
        if not self.producer:
            logger.error("Producer not connected. Call connect() first.")
            return False
        
        target_topic = topic or self.config.KAFKA_TOPIC
        
        try:
            future = self.producer.send(
                target_topic,
                key=key,
                value=message
            )
            record_metadata = future.get(timeout=10)
            logger.info(
                f"Message sent to topic {record_metadata.topic}, "
                f"partition {record_metadata.partition}, "
                f"offset {record_metadata.offset}"
            )
            return True
        except KafkaError as e:
            logger.error(f"Failed to send message: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending message: {e}")
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
    
    def flush(self):
        if self.producer:
            self.producer.flush()
            logger.info("Producer flushed")
    
    def close(self):
        if self.producer:
            self.producer.flush()
            self.producer.close()
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
            time.sleep(0.5)
        
        producer.flush()
        logger.info("All messages sent successfully")
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
