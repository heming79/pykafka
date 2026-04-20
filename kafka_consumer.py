import json
import time
from datetime import datetime
from confluent_kafka import Consumer, KafkaException, KafkaError
import redis
from typing import Optional, Dict, Any, Callable, List
import logging
import threading

from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaMessageConsumer:
    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.consumer = None
        self.redis_client = None
        self.running = False
        self._message_count = 0
        self._lock = threading.Lock()
        
    def connect_kafka(self) -> bool:
        try:
            logger.info(f"Connecting to Kafka at {self.config.KAFKA_BOOTSTRAP_SERVERS}")
            
            consumer_config = {
                'bootstrap.servers': self.config.KAFKA_BOOTSTRAP_SERVERS,
                'group.id': self.config.KAFKA_GROUP_ID,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': True,
                'auto.commit.interval.ms': 5000,
                'fetch.min.bytes': 1,
                'fetch.max.wait.ms': 500,
            }
            
            self.consumer = Consumer(consumer_config)
            
            self.consumer.subscribe([self.config.KAFKA_TOPIC])
            
            logger.info("Successfully connected to Kafka")
            return True
        except KafkaException as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
    
    def connect_redis(self) -> bool:
        try:
            logger.info(f"Connecting to Redis at {self.config.REDIS_HOST}:{self.config.REDIS_PORT}")
            self.redis_client = redis.Redis(
                host=self.config.REDIS_HOST,
                port=self.config.REDIS_PORT,
                db=self.config.REDIS_DB,
                decode_responses=True
            )
            self.redis_client.ping()
            logger.info("Successfully connected to Redis")
            return True
        except redis.RedisError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            return False
    
    def connect(self) -> bool:
        return self.connect_kafka() and self.connect_redis()
    
    def _get_minute_key(self, timestamp: Optional[float] = None) -> str:
        if timestamp is None:
            timestamp = time.time()
        dt = datetime.fromtimestamp(timestamp)
        minute_str = dt.strftime("%Y-%m-%d_%H:%M")
        return f"{self.config.MESSAGE_COUNT_KEY_PREFIX}:{minute_str}"
    
    def increment_message_count(self, timestamp: Optional[float] = None) -> int:
        if not self.redis_client:
            logger.warning("Redis not connected, cannot increment count")
            return 0
        
        key = self._get_minute_key(timestamp)
        try:
            count = self.redis_client.incr(key)
            self.redis_client.expire(key, 86400)
            logger.debug(f"Message count for {key}: {count}")
            return count
        except redis.RedisError as e:
            logger.error(f"Failed to increment message count: {e}")
            return 0
    
    def get_minute_count(self, minute_str: Optional[str] = None) -> int:
        if not self.redis_client:
            logger.warning("Redis not connected, cannot get count")
            return 0
        
        if minute_str:
            key = f"{self.config.MESSAGE_COUNT_KEY_PREFIX}:{minute_str}"
        else:
            key = self._get_minute_key()
        
        try:
            count = self.redis_client.get(key)
            return int(count) if count else 0
        except redis.RedisError as e:
            logger.error(f"Failed to get message count: {e}")
            return 0
    
    def get_all_minute_counts(self) -> Dict[str, int]:
        if not self.redis_client:
            logger.warning("Redis not connected, cannot get counts")
            return {}
        
        try:
            pattern = f"{self.config.MESSAGE_COUNT_KEY_PREFIX}:*"
            keys = self.redis_client.keys(pattern)
            counts = {}
            for key in keys:
                count = self.redis_client.get(key)
                minute = key.split(':')[-1]
                counts[minute] = int(count) if count else 0
            return dict(sorted(counts.items()))
        except redis.RedisError as e:
            logger.error(f"Failed to get all message counts: {e}")
            return {}
    
    def _deserialize_message(self, msg) -> Optional[Dict[str, Any]]:
        try:
            value = msg.value()
            if value is None:
                return None
            return json.loads(value.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error(f"Failed to deserialize message: {e}")
            return None
    
    def _deserialize_key(self, msg) -> Optional[str]:
        try:
            key = msg.key()
            if key is None:
                return None
            return key.decode('utf-8')
        except UnicodeDecodeError as e:
            logger.error(f"Failed to deserialize key: {e}")
            return None
    
    def process_message(self, msg) -> bool:
        try:
            message_value = self._deserialize_message(msg)
            message_key = self._deserialize_key(msg)
            
            if message_value is None:
                logger.warning("Received message with None value")
                return False
            
            logger.info(f"Received message - Key: {message_key}, Value: {message_value}")
            
            timestamp = None
            if isinstance(message_value, dict):
                if 'timestamp' in message_value:
                    timestamp = message_value['timestamp']
            
            self.increment_message_count(timestamp)
            
            with self._lock:
                self._message_count += 1
            
            return True
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False
    
    def start_consuming(
        self, 
        message_handler: Optional[Callable[[Any], bool]] = None,
        timeout: Optional[float] = None
    ):
        if not self.consumer:
            logger.error("Consumer not connected. Call connect() first.")
            return
        
        self.running = True
        logger.info(f"Starting to consume messages from topic: {self.config.KAFKA_TOPIC}")
        
        try:
            start_time = time.time()
            while self.running:
                if timeout and time.time() - start_time > timeout:
                    logger.info(f"Timeout reached ({timeout}s), stopping consumer")
                    break
                
                msg = self.consumer.poll(
                    timeout=self.config.CONSUMER_POLL_TIMEOUT
                )
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info(
                            f"Reached end of partition {msg.partition()} "
                            f"at offset {msg.offset()}"
                        )
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                else:
                    if message_handler:
                        message_handler(msg)
                    else:
                        self.process_message(msg)
                            
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except KafkaException as e:
            logger.error(f"Kafka error: {e}")
        finally:
            self.running = False
            logger.info(f"Consumer stopped. Total messages processed: {self._message_count}")
    
    def stop(self):
        self.running = False
        logger.info("Consumer stop requested")
    
    def close(self):
        self.running = False
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer connection closed")
        if self.redis_client:
            self.redis_client.close()
            logger.info("Redis connection closed")


def main():
    consumer = KafkaMessageConsumer()
    if not consumer.connect():
        logger.error("Failed to initialize consumer")
        return
    
    try:
        logger.info("Starting consumer... (Press Ctrl+C to stop)")
        consumer.start_consuming()
    except KeyboardInterrupt:
        logger.info("Main thread interrupted")
    finally:
        consumer.close()
        
        logger.info("\n=== Message Statistics ===")
        counts = consumer.get_all_minute_counts()
        for minute, count in counts.items():
            logger.info(f"{minute}: {count} messages")


if __name__ == "__main__":
    main()
