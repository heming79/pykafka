import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "test_topic")
    KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "test_group")
    
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
    REDIS_DB = int(os.getenv("REDIS_DB", 0))
    
    MESSAGE_COUNT_KEY_PREFIX = os.getenv("MESSAGE_COUNT_KEY_PREFIX", "kafka:messages:minute")
    
    PRODUCER_BATCH_SIZE = int(os.getenv("PRODUCER_BATCH_SIZE", 100))
    CONSUMER_POLL_TIMEOUT = float(os.getenv("CONSUMER_POLL_TIMEOUT", 1.0))
