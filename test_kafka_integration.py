import pytest
import time
import threading
import json
from datetime import datetime
from unittest.mock import MagicMock, patch, PropertyMock
from confluent_kafka import KafkaException, KafkaError
from redis import RedisError

from config import Config
from kafka_producer import KafkaMessageProducer, delivery_report
from kafka_consumer import KafkaMessageConsumer


class TestConfig:
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
    KAFKA_TOPIC = "test_topic_integration"
    KAFKA_GROUP_ID = "test_group_integration"
    
    REDIS_HOST = "localhost"
    REDIS_PORT = 6379
    REDIS_DB = 1
    
    MESSAGE_COUNT_KEY_PREFIX = "test:kafka:messages:minute"
    
    PRODUCER_BATCH_SIZE = 10
    CONSUMER_POLL_TIMEOUT = 0.5


@pytest.fixture
def test_config():
    return TestConfig()


@pytest.fixture
def redis_client(test_config):
    import redis
    client = redis.Redis(
        host=test_config.REDIS_HOST,
        port=test_config.REDIS_PORT,
        db=test_config.REDIS_DB,
        decode_responses=True
    )
    client.flushdb()
    yield client
    client.flushdb()
    client.close()


class TestDeliveryReport:
    def test_delivery_report_success(self, capsys):
        mock_msg = MagicMock()
        mock_msg.topic.return_value = "test_topic"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 123
        
        delivery_report(None, mock_msg)
        
        captured = capsys.readouterr()
    
    def test_delivery_report_error(self, capsys):
        mock_err = MagicMock()
        mock_err.__str__.return_value = "Test error"
        
        delivery_report(mock_err, None)
        
        captured = capsys.readouterr()


class TestKafkaProducer:
    def test_producer_initialization(self, test_config):
        producer = KafkaMessageProducer(test_config)
        assert producer.config == test_config
        assert producer.producer is None
    
    def test_send_message_success(self, test_config):
        with patch('kafka_producer.Producer') as mock_producer_class:
            mock_producer = MagicMock()
            mock_producer_class.return_value = mock_producer
            
            producer = KafkaMessageProducer(test_config)
            producer.connect()
            
            test_message = {"id": 1, "content": "test message"}
            result = producer.send_message(test_message)
            
            assert result is True
            mock_producer.produce.assert_called_once()
            call_args = mock_producer.produce.call_args
            assert call_args[1]['topic'] == test_config.KAFKA_TOPIC
    
    def test_send_message_with_key(self, test_config):
        with patch('kafka_producer.Producer') as mock_producer_class:
            mock_producer = MagicMock()
            mock_producer_class.return_value = mock_producer
            
            producer = KafkaMessageProducer(test_config)
            producer.connect()
            
            test_message = {"id": 2, "content": "test message with key"}
            test_key = "test-key-123"
            producer.send_message(test_message, key=test_key)
            
            mock_producer.produce.assert_called_once()
            call_args = mock_producer.produce.call_args
            assert call_args[1]['key'] == test_key.encode('utf-8')
    
    def test_send_messages_batch(self, test_config):
        with patch('kafka_producer.Producer') as mock_producer_class:
            mock_producer = MagicMock()
            mock_producer_class.return_value = mock_producer
            
            producer = KafkaMessageProducer(test_config)
            producer.connect()
            
            messages = [
                {"id": i, "content": f"batch message {i}"} 
                for i in range(5)
            ]
            success_count = producer.send_messages_batch(messages)
            
            assert success_count == 5
            assert mock_producer.produce.call_count == 5
    
    def test_producer_flush(self, test_config):
        with patch('kafka_producer.Producer') as mock_producer_class:
            mock_producer = MagicMock()
            mock_producer.flush.return_value = 0
            mock_producer_class.return_value = mock_producer
            
            producer = KafkaMessageProducer(test_config)
            producer.connect()
            producer.flush()
            
            mock_producer.flush.assert_called_once()
    
    def test_producer_poll(self, test_config):
        with patch('kafka_producer.Producer') as mock_producer_class:
            mock_producer = MagicMock()
            mock_producer.poll.return_value = 2
            mock_producer_class.return_value = mock_producer
            
            producer = KafkaMessageProducer(test_config)
            producer.connect()
            result = producer.poll(0.5)
            
            assert result == 2
            mock_producer.poll.assert_called_once_with(0.5)
    
    def test_send_message_without_connection(self, test_config):
        producer = KafkaMessageProducer(test_config)
        
        result = producer.send_message({"test": "message"})
        assert result is False


class TestKafkaConsumer:
    def test_consumer_initialization(self, test_config):
        consumer = KafkaMessageConsumer(test_config)
        assert consumer.config == test_config
        assert consumer.consumer is None
        assert consumer.redis_client is None
        assert consumer.running is False
    
    def test_get_minute_key(self, test_config):
        consumer = KafkaMessageConsumer(test_config)
        
        test_timestamp = 1700000000
        key = consumer._get_minute_key(test_timestamp)
        
        assert key.startswith(test_config.MESSAGE_COUNT_KEY_PREFIX)
        assert ':' in key
        
        current_key = consumer._get_minute_key()
        assert current_key != key
    
    def test_increment_message_count(self, test_config, redis_client):
        consumer = KafkaMessageConsumer(test_config)
        consumer.redis_client = redis_client
        
        count1 = consumer.increment_message_count()
        assert count1 == 1
        
        count2 = consumer.increment_message_count()
        assert count2 == 2
        
        current_key = consumer._get_minute_key()
        stored_count = redis_client.get(current_key)
        assert int(stored_count) == 2
    
    def test_get_minute_count(self, test_config, redis_client):
        consumer = KafkaMessageConsumer(test_config)
        consumer.redis_client = redis_client
        
        current_key = consumer._get_minute_key()
        redis_client.set(current_key, 5)
        
        count = consumer.get_minute_count()
        assert count == 5
    
    def test_get_all_minute_counts(self, test_config, redis_client):
        consumer = KafkaMessageConsumer(test_config)
        consumer.redis_client = redis_client
        
        key1 = f"{test_config.MESSAGE_COUNT_KEY_PREFIX}:2024-01-01_10:00"
        key2 = f"{test_config.MESSAGE_COUNT_KEY_PREFIX}:2024-01-01_10:01"
        
        redis_client.set(key1, 10)
        redis_client.set(key2, 20)
        
        counts = consumer.get_all_minute_counts()
        
        assert len(counts) == 2
        assert "2024-01-01_10:00" in counts
        assert "2024-01-01_10:01" in counts
        assert counts["2024-01-01_10:00"] == 10
        assert counts["2024-01-01_10:01"] == 20
    
    def test_deserialize_message(self, test_config):
        consumer = KafkaMessageConsumer(test_config)
        
        mock_msg = MagicMock()
        test_value = {"id": 1, "content": "test"}
        mock_msg.value.return_value = json.dumps(test_value).encode('utf-8')
        
        result = consumer._deserialize_message(mock_msg)
        assert result == test_value
    
    def test_deserialize_message_none(self, test_config):
        consumer = KafkaMessageConsumer(test_config)
        
        mock_msg = MagicMock()
        mock_msg.value.return_value = None
        
        result = consumer._deserialize_message(mock_msg)
        assert result is None
    
    def test_deserialize_key(self, test_config):
        consumer = KafkaMessageConsumer(test_config)
        
        mock_msg = MagicMock()
        mock_msg.key.return_value = "test-key".encode('utf-8')
        
        result = consumer._deserialize_key(mock_msg)
        assert result == "test-key"
    
    def test_deserialize_key_none(self, test_config):
        consumer = KafkaMessageConsumer(test_config)
        
        mock_msg = MagicMock()
        mock_msg.key.return_value = None
        
        result = consumer._deserialize_key(mock_msg)
        assert result is None
    
    def test_consumer_without_redis_connection(self, test_config):
        consumer = KafkaMessageConsumer(test_config)
        
        count = consumer.increment_message_count()
        assert count == 0
        
        minute_count = consumer.get_minute_count()
        assert minute_count == 0
        
        all_counts = consumer.get_all_minute_counts()
        assert all_counts == {}
    
    def test_consumer_without_kafka_connection(self, test_config):
        consumer = KafkaMessageConsumer(test_config)
        
        consumer.start_consuming(timeout=1)
        
        assert consumer.running is False


class TestEdgeCases:
    def test_empty_message_batch(self, test_config):
        with patch('kafka_producer.Producer') as mock_producer_class:
            mock_producer = MagicMock()
            mock_producer_class.return_value = mock_producer
            
            producer = KafkaMessageProducer(test_config)
            producer.connect()
            
            success_count = producer.send_messages_batch([])
            assert success_count == 0
            mock_producer.produce.assert_not_called()
    
    def test_message_with_invalid_json(self, test_config):
        consumer = KafkaMessageConsumer(test_config)
        
        mock_msg = MagicMock()
        mock_msg.value.return_value = b"invalid json {{"
        
        result = consumer._deserialize_message(mock_msg)
        assert result is None


class TestConfigClass:
    def test_default_config(self):
        config = Config()
        
        assert config.KAFKA_BOOTSTRAP_SERVERS == "localhost:9092"
        assert config.KAFKA_TOPIC == "test_topic"
        assert config.KAFKA_GROUP_ID == "test_group"
        assert config.REDIS_HOST == "localhost"
        assert config.REDIS_PORT == 6379
        assert config.REDIS_DB == 0
    
    def test_test_config_override(self, test_config):
        assert test_config.KAFKA_TOPIC == "test_topic_integration"
        assert test_config.KAFKA_GROUP_ID == "test_group_integration"
        assert test_config.REDIS_DB == 1
