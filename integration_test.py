#!/usr/bin/env python
"""
Integration test for Kafka producer and consumer with Redis statistics.
This test requires Docker to be running and will start the necessary services.
"""

import time
import sys
import threading
import subprocess
import json
from datetime import datetime

try:
    import docker
    from docker.errors import DockerException, APIError
    DOCKER_AVAILABLE = True
except ImportError:
    DOCKER_AVAILABLE = False

from config import Config
from kafka_producer import KafkaMessageProducer
from kafka_consumer import KafkaMessageConsumer


class IntegrationTestConfig(Config):
    KAFKA_TOPIC = "integration_test_topic"
    KAFKA_GROUP_ID = "integration_test_group"
    REDIS_DB = 2
    MESSAGE_COUNT_KEY_PREFIX = "integration:kafka:messages:minute"


class TestRunner:
    def __init__(self):
        self.config = IntegrationTestConfig()
        self.docker_client = None
        self.producer = None
        self.consumer = None
        self.consumer_thread = None
        self.messages_consumed = []
        self.test_passed = False
    
    def check_docker(self) -> bool:
        if not DOCKER_AVAILABLE:
            print("ERROR: Docker Python library not installed.")
            print("Please install it with: pip install docker")
            return False
        
        try:
            self.docker_client = docker.from_env()
            self.docker_client.ping()
            print("✓ Docker is running")
            return True
        except DockerException as e:
            print(f"ERROR: Docker is not running or not accessible: {e}")
            return False
    
    def start_services(self) -> bool:
        print("\n=== Starting Docker Services ===")
        print("This may take a few minutes on first run...")
        
        try:
            result = subprocess.run(
                ["docker-compose", "up", "-d"],
                capture_output=True,
                text=True,
                cwd="."
            )
            
            if result.returncode != 0:
                print(f"ERROR: Failed to start services: {result.stderr}")
                return False
            
            print("✓ Docker services starting...")
            print("Waiting for services to be ready (this may take 30-60 seconds)...")
            
            self._wait_for_services()
            
            return True
            
        except FileNotFoundError:
            print("ERROR: docker-compose not found. Please ensure Docker Compose is installed.")
            return False
        except Exception as e:
            print(f"ERROR: Failed to start services: {e}")
            return False
    
    def _wait_for_services(self, timeout: int = 120) -> bool:
        import redis
        from confluent_kafka import Producer as ConfluentProducer
        
        start_time = time.time()
        kafka_ready = False
        redis_ready = False
        
        while time.time() - start_time < timeout:
            elapsed = int(time.time() - start_time)
            
            if not redis_ready:
                try:
                    r = redis.Redis(
                        host=self.config.REDIS_HOST,
                        port=self.config.REDIS_PORT,
                        socket_timeout=2
                    )
                    r.ping()
                    r.close()
                    redis_ready = True
                    print(f"✓ Redis is ready (elapsed: {elapsed}s)")
                except Exception:
                    pass
            
            if not kafka_ready:
                try:
                    p = ConfluentProducer({
                        'bootstrap.servers': self.config.KAFKA_BOOTSTRAP_SERVERS,
                        'socket.timeout.ms': 2000,
                        'socket.connection.setup.timeout.ms': 2000
                    })
                    p.list_topics(timeout=2)
                    kafka_ready = True
                    print(f"✓ Kafka is ready (elapsed: {elapsed}s)")
                except Exception:
                    pass
            
            if kafka_ready and redis_ready:
                return True
            
            time.sleep(2)
        
        print(f"ERROR: Timeout waiting for services after {timeout} seconds")
        return False
    
    def setup_clients(self) -> bool:
        print("\n=== Setting up Clients ===")
        
        self.producer = KafkaMessageProducer(self.config)
        if not self.producer.connect():
            print("ERROR: Failed to connect Kafka producer")
            return False
        print("✓ Kafka producer connected")
        
        self.consumer = KafkaMessageConsumer(self.config)
        if not self.consumer.connect():
            print("ERROR: Failed to connect Kafka consumer")
            return False
        print("✓ Kafka consumer connected")
        
        import redis
        r = redis.Redis(
            host=self.config.REDIS_HOST,
            port=self.config.REDIS_PORT,
            db=self.config.REDIS_DB
        )
        r.flushdb()
        r.close()
        print("✓ Test Redis database cleared")
        
        return True
    
    def message_handler(self, record):
        message_value = self.consumer._deserialize_message(record)
        if message_value:
            self.messages_consumed.append(message_value)
            print(f"  ↳ Consumed: {message_value.get('id', 'unknown')}")
            
            timestamp = None
            if isinstance(message_value, dict) and 'timestamp' in message_value:
                timestamp = message_value['timestamp']
            
            self.consumer.increment_message_count(timestamp)
    
    def run_test(self) -> bool:
        print("\n=== Running Integration Test ===")
        
        self.consumer_thread = threading.Thread(
            target=self.consumer.start_consuming,
            kwargs={
                'message_handler': self.message_handler,
                'timeout': 30
            },
            daemon=True
        )
        self.consumer_thread.start()
        print("✓ Consumer thread started")
        
        time.sleep(2)
        
        print("\n--- Sending Test Messages ---")
        test_messages = []
        for i in range(10):
            msg = {
                "id": i,
                "timestamp": time.time(),
                "content": f"Integration test message {i}",
                "test_type": "integration"
            }
            test_messages.append(msg)
            print(f"  → Sending: message {i}")
            self.producer.send_message(msg, key=f"test-key-{i}")
            self.producer.poll(0.1)
            time.sleep(0.2)
        
        self.producer.flush()
        print("\n✓ All messages sent")
        
        print("\n--- Waiting for consumer to process messages ---")
        max_wait = 15
        waited = 0
        while len(self.messages_consumed) < 10 and waited < max_wait:
            time.sleep(1)
            waited += 1
            print(f"  Waiting... consumed {len(self.messages_consumed)}/10 messages")
        
        self.consumer.stop()
        
        if self.consumer_thread.is_alive():
            self.consumer_thread.join(timeout=5)
        
        print(f"\n✓ Consumer stopped. Total consumed: {len(self.messages_consumed)}")
        
        print("\n--- Checking Redis Statistics ---")
        counts = self.consumer.get_all_minute_counts()
        print(f"Minute statistics found: {len(counts)} minute(s)")
        for minute, count in counts.items():
            print(f"  {minute}: {count} messages")
        
        total_count = sum(counts.values())
        print(f"\nTotal messages in Redis: {total_count}")
        
        print("\n=== Test Results ===")
        if len(self.messages_consumed) >= 10 and total_count >= 10:
            print("✓ TEST PASSED!")
            print(f"  - Messages sent: 10")
            print(f"  - Messages consumed: {len(self.messages_consumed)}")
            print(f"  - Messages in Redis: {total_count}")
            self.test_passed = True
            return True
        else:
            print("✗ TEST FAILED!")
            print(f"  - Expected: 10 messages")
            print(f"  - Consumed: {len(self.messages_consumed)}")
            print(f"  - In Redis: {total_count}")
            return False
    
    def cleanup(self):
        print("\n=== Cleanup ===")
        
        if self.producer:
            self.producer.close()
            print("✓ Producer closed")
        
        if self.consumer:
            self.consumer.close()
            print("✓ Consumer closed")
        
        if not self.test_passed:
            print("\nNote: Docker services are kept running for debugging.")
            print("To stop them, run: docker-compose down")
            print("To remove volumes as well: docker-compose down -v")
        else:
            print("\nDocker services are kept running for your convenience.")
            print("You can stop them with: docker-compose down")
    
    def run(self):
        print("=" * 60)
        print("Kafka + Redis Integration Test")
        print("=" * 60)
        
        if not self.check_docker():
            print("\nPlease ensure Docker is running and try again.")
            return False
        
        if not self.start_services():
            print("\nFailed to start Docker services.")
            return False
        
        if not self.setup_clients():
            print("\nFailed to setup clients.")
            self.cleanup()
            return False
        
        try:
            success = self.run_test()
        except Exception as e:
            print(f"\nERROR during test: {e}")
            import traceback
            traceback.print_exc()
            success = False
        
        self.cleanup()
        
        return success


def main():
    runner = TestRunner()
    success = runner.run()
    
    print("\n" + "=" * 60)
    if success:
        print("Integration test completed successfully!")
        sys.exit(0)
    else:
        print("Integration test failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
