# file: src/streaming/kafka_producer.py
import json
import logging
import time
import socket
from kafka import KafkaProducer
from kafka.errors import KafkaError


class OptimizedKafkaSender:
    """
    Optimized Kafka producer for distributed packet capture.
    Sends raw packet data with metadata for network distribution.
    """

    def __init__(self, servers, topic, device_id=None, linger_ms=5, batch_size=16384, acks=1):
        self.log = logging.getLogger("OptimizedKafkaSender")
        self.topic = topic
        self.device_id = device_id or self._get_device_id()
        self.stats = {
            'packets_sent': 0,
            'bytes_sent': 0,
            'errors': 0,
            'start_time': time.time()
        }
        
        # Optimized producer configuration for network distribution
        self.producer = KafkaProducer(
            bootstrap_servers=servers,
            value_serializer=self._serialize_packet_data,
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            
            # Performance optimizations
            linger_ms=linger_ms,  # Batch messages for 5ms
            batch_size=batch_size,  # 16KB batches
            buffer_memory=33554432,  # 32MB buffer
            max_request_size=1048576,  # 1MB max message size
            
            # Reliability settings
            acks=acks,  # Wait for leader acknowledgment
            retries=3,
            retry_backoff_ms=100,
            
            # Network optimization
            compression_type='snappy',  # Fast compression
            max_in_flight_requests_per_connection=5,
            
            # Timeout settings
            request_timeout_ms=30000,
            delivery_timeout_ms=120000,
        )
        
        self.log.info(f"Kafka producer initialized - Device: {self.device_id}, Servers: {servers}")

    def _get_device_id(self):
        """Generate unique device identifier"""
        hostname = socket.gethostname()
        return f"{hostname}_{int(time.time())}"

    def _serialize_packet_data(self, packet_data):
        """Serialize packet data with compression"""
        try:
            return json.dumps(packet_data, separators=(',', ':')).encode('utf-8')
        except Exception as e:
            self.log.error(f"Serialization error: {e}")
            return b'{}'

    def send_packet(self, raw_packet_bytes, packet_metadata):
        """
        Send raw packet data with metadata to Kafka.
        
        Args:
            raw_packet_bytes: Raw packet bytes from scapy
            packet_metadata: Extracted metadata (timestamp, src_ip, dst_ip, etc.)
        """
        try:
            # Create packet message
            packet_message = {
                'device_id': self.device_id,
                'timestamp': time.time() * 1_000_000,  # microseconds
                'capture_time': packet_metadata.get('capture_time', time.time()),
                'packet_data': raw_packet_bytes.hex(),  # Convert bytes to hex string
                'metadata': packet_metadata,
                'packet_size': len(raw_packet_bytes)
            }
            
            # Use source IP as partition key for flow locality
            partition_key = packet_metadata.get('src_ip', self.device_id)
            
            # Send to Kafka
            future = self.producer.send(
                self.topic,
                key=partition_key,
                value=packet_message
            )
            
            # Update stats
            self.stats['packets_sent'] += 1
            self.stats['bytes_sent'] += len(raw_packet_bytes)
            
            # Optional: Add callback for delivery confirmation
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)
            
            return True
            
        except Exception as exc:
            self.log.error(f"Kafka send failed: {exc}")
            self.stats['errors'] += 1
            return False

    def _on_send_success(self, record_metadata):
        """Callback for successful sends"""
        self.log.debug(f"Packet sent to partition {record_metadata.partition} offset {record_metadata.offset}")

    def _on_send_error(self, exception):
        """Callback for send errors"""
        self.log.error(f"Packet send failed: {exception}")
        self.stats['errors'] += 1

    def flush(self):
        """Force send all buffered messages"""
        try:
            self.producer.flush(timeout=10)
            return True
        except Exception as e:
            self.log.error(f"Flush failed: {e}")
            return False

    def get_stats(self):
        """Get producer statistics"""
        uptime = time.time() - self.stats['start_time']
        return {
            **self.stats,
            'uptime_seconds': uptime,
            'packets_per_second': self.stats['packets_sent'] / uptime if uptime > 0 else 0,
            'bytes_per_second': self.stats['bytes_sent'] / uptime if uptime > 0 else 0,
            'error_rate': self.stats['errors'] / max(self.stats['packets_sent'], 1)
        }

    def close(self):
        """Close producer and cleanup"""
        try:
            self.producer.flush(timeout=10)
            self.producer.close(timeout=10)
            self.log.info("Kafka producer closed successfully")
        except Exception as e:
            self.log.error(f"Error closing producer: {e}")


# Backward compatibility
class KafkaSender(OptimizedKafkaSender):
    """Legacy wrapper for backward compatibility"""
    
    def __init__(self, servers, topic, linger_ms=10, acks=1):
        super().__init__(servers, topic, linger_ms=linger_ms, acks=acks)
        self.log.warning("Using legacy KafkaSender - consider upgrading to OptimizedKafkaSender")

    def send(self, data: dict):
        """Legacy send method - converts dict to packet format"""
        fake_packet_bytes = json.dumps(data).encode('utf-8')
        metadata = {
            'src_ip': data.get('src_ip', '0.0.0.0'),
            'dst_ip': data.get('dst_ip', '0.0.0.0'),
            'protocol': data.get('protocol', 0),
            'legacy_format': True
        }
        return self.send_packet(fake_packet_bytes, metadata)

