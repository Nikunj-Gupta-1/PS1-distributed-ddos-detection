# file: src/streaming/kafka_consumer.py

import json
import logging
import os
from kafka import KafkaConsumer
from processor import add_to_queue

# Configure logging
logger = logging.getLogger(__name__)

def consume_raw_packets():
    """Consume raw packet data from Kafka topic"""
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
    topic = os.getenv("KAFKA_TOPIC", "raw_packets")
    group_id = os.getenv("KAFKA_GROUP_ID", "ddos-detection-job-server")
    
    logger.info(f"[Consumer] Starting raw packet consumer...")
    logger.info(f"[Consumer] Servers: {bootstrap_servers}")
    logger.info(f"[Consumer] Topic: {topic}")
    logger.info(f"[Consumer] Group ID: {group_id}")

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=group_id,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            max_poll_records=100,  # Process in batches for efficiency
            fetch_min_bytes=1024,  # Wait for at least 1KB
            fetch_max_wait_ms=500,  # But don't wait more than 500ms
        )
        logger.info(f"[Consumer] ✅ Connected to Kafka")
    except Exception as e:
        logger.error(f"[Consumer] ❌ Failed to connect to Kafka: {e}")
        raise

    packet_count = 0
    try:
        for msg in consumer:
            try:
                # Parse packet message
                packet_message = json.loads(msg.value.decode('utf-8'))
                
                packet_count += 1
                if packet_count % 100 == 0:
                    logger.info(f"[Consumer] Processed {packet_count} packets")
                
                # Add to processing queue
                add_to_queue(packet_message)
                
            except json.JSONDecodeError:
                logger.warning("[Consumer] ⚠️ Invalid JSON format, skipping message")
            except Exception as e:
                logger.error(f"[Consumer] Error processing message: {e}")
    
    except KeyboardInterrupt:
        logger.info("[Consumer] Shutting down...")
    finally:
        consumer.close()
        logger.info(f"[Consumer] Consumer closed. Total packets processed: {packet_count}")


# Backward compatibility
def consume_network_flows():
    """Legacy function name - redirects to new function"""
    logger.warning("[Consumer] Using legacy function name - consider updating to consume_raw_packets()")
    consume_raw_packets()
