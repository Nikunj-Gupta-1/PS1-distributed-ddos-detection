"""
Distributed packet-capture module for DDoS detection:

• Captures raw packets and sends them via Kafka to central job server
• Minimal processing on capture devices - just basic metadata extraction
• Central job server handles feature extraction and ML inference
• Supports multiple capture devices on the same network

Linux  ➜  AF_PACKET raw socket for high-throughput
macOS/other ➜ Scapy sniff()
"""

import os
import time
import socket
import logging
import platform
import uuid
from collections import defaultdict

from scapy.all import Ether, IP, TCP, UDP, sniff, raw
import netifaces

from src.streaming.kafka_producer import OptimizedKafkaSender


# ──────────────────────────────── PACKET METADATA EXTRACTOR ─────────────────────────────────
class PacketMetadataExtractor:
    """Extracts minimal metadata from packets for Kafka partitioning and routing"""

    @staticmethod
    def extract_metadata(pkt):
        """Extract basic metadata without heavy feature computation"""
        metadata = {
            'capture_time': time.time(),
            'packet_size': len(pkt),
            'has_ip': IP in pkt,
            'has_tcp': TCP in pkt,
            'has_udp': UDP in pkt,
        }

        if IP in pkt:
            ip_layer = pkt[IP]
            metadata.update({
                'src_ip': ip_layer.src,
                'dst_ip': ip_layer.dst,
                'protocol': ip_layer.proto,
                'ip_len': ip_layer.len,
                'ttl': ip_layer.ttl,
            })

            # Add port information if available
            if TCP in pkt:
                tcp_layer = pkt[TCP]
                metadata.update({
                    'src_port': tcp_layer.sport,
                    'dst_port': tcp_layer.dport,
                    'tcp_flags': tcp_layer.flags,
                    'tcp_seq': tcp_layer.seq,
                    'tcp_ack': tcp_layer.ack,
                })
            elif UDP in pkt:
                udp_layer = pkt[UDP]
                metadata.update({
                    'src_port': udp_layer.sport,
                    'dst_port': udp_layer.dport,
                    'udp_len': udp_layer.len,
                })

        return metadata


# ──────────────────────────────── DISTRIBUTED PACKET SNIFFER ─────────────────────────────────
class DistributedPacketSniffer:
    """
    Distributed packet sniffer that sends raw packets to Kafka.
    Designed for deployment on multiple devices across a network.
    """

    def __init__(self, interface, kafka_servers, kafka_topic, device_id=None):
        self.log = logging.getLogger("DistributedPacketSniffer")
        self.iface = interface or self._auto_iface()
        self.device_id = device_id or self._generate_device_id()
        
        # Initialize Kafka producer
        self.producer = OptimizedKafkaSender(
            servers=kafka_servers,
            topic=kafka_topic,
            device_id=self.device_id,
            linger_ms=5,  # Fast batching for real-time
            batch_size=32768,  # 32KB batches for network efficiency
        )
        
        # Metadata extractor
        self.metadata_extractor = PacketMetadataExtractor()
        
        # Statistics
        self.stats = {
            'packets_captured': 0,
            'packets_sent': 0,
            'bytes_captured': 0,
            'start_time': time.time(),
            'last_stats_time': time.time(),
        }

        self.log.info(f"Device ID: {self.device_id}")
        self.log.info(f"Interface: {self.iface}")
        self.log.info(f"Kafka servers: {kafka_servers}")
        self.log.info(f"Kafka topic: {kafka_topic}")

    def _auto_iface(self):
        """Auto-detect network interface"""
        for iface in netifaces.interfaces():
            if iface != "lo" and not iface.startswith("docker"):
                return iface
        return "lo"

    def _generate_device_id(self):
        """Generate unique device identifier"""
        hostname = socket.gethostname()
        mac = self._get_mac_address()
        return f"{hostname}_{mac}_{int(time.time())}"

    def _get_mac_address(self):
        """Get MAC address for unique identification"""
        try:
            import uuid
            mac = ':'.join(['{:02x}'.format((uuid.getnode() >> elements) & 0xff) 
                           for elements in range(0,2*6,2)][::-1])
            return mac.replace(':', '')
        except:
            return "unknown"

    def _should_capture_packet(self, pkt):
        """Filter packets to capture (basic filtering)"""
        # Only capture IP packets for now
        if IP not in pkt:
            return False
        
        # Skip loopback traffic
        if pkt[IP].src == '127.0.0.1' or pkt[IP].dst == '127.0.0.1':
            return False
            
        return True

    def _handle_packet(self, pkt):
        """Process each captured packet"""
        try:
            # Update capture stats
            self.stats['packets_captured'] += 1
            self.stats['bytes_captured'] += len(pkt)

            # Filter packets
            if not self._should_capture_packet(pkt):
                return

            # Extract metadata
            metadata = self.metadata_extractor.extract_metadata(pkt)
            
            # Get raw packet bytes
            raw_packet_bytes = raw(pkt)
            
            # Send to Kafka
            success = self.producer.send_packet(raw_packet_bytes, metadata)
            
            if success:
                self.stats['packets_sent'] += 1
            
            # Log stats periodically
            self._log_stats_if_needed()

        except Exception as e:
            self.log.error(f"Error handling packet: {e}")

    def _log_stats_if_needed(self):
        """Log statistics every 10 seconds"""
        now = time.time()
        if now - self.stats['last_stats_time'] >= 10:
            self._log_stats()
            self.stats['last_stats_time'] = now

    def _log_stats(self):
        """Log current statistics"""
        uptime = time.time() - self.stats['start_time']
        capture_rate = self.stats['packets_captured'] / uptime if uptime > 0 else 0
        send_rate = self.stats['packets_sent'] / uptime if uptime > 0 else 0
        
        kafka_stats = self.producer.get_stats()
        
        self.log.info(f"Stats - Captured: {self.stats['packets_captured']} "
                     f"({capture_rate:.1f}/s), Sent: {self.stats['packets_sent']} "
                     f"({send_rate:.1f}/s), Kafka errors: {kafka_stats['errors']}")

    def _linux_af_packet_loop(self):
        """Linux AF_PACKET capture loop"""
        ETH_P_ALL = 0x0003
        sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.ntohs(ETH_P_ALL))
        sock.bind((self.iface, 0))
        self.log.info(f"AF_PACKET socket ready (interface {self.iface})")

        try:
            while True:
                raw_data, _ = sock.recvfrom(65535)
                ether = Ether(raw_data)
                self._handle_packet(ether)
        except KeyboardInterrupt:
            self.log.info("Sniffer stopped by user")
        finally:
            sock.close()

    def _scapy_sniff_loop(self):
        """Scapy sniff loop for macOS and other platforms"""
        self.log.info(f"Starting Scapy sniff() loop on {self.iface}... Ctrl-C to stop")
        try:
            sniff(
                iface=self.iface,
                store=False,
                prn=self._handle_packet,
                filter="ip"  # Only IP packets
            )
        except KeyboardInterrupt:
            self.log.info("Sniffer stopped by user")

    def start_sniffing(self):
        """Start packet capture"""
        self.log.info("Starting distributed packet capture...")
        
        try:
            if platform.system() == "Linux":
                self._linux_af_packet_loop()
            else:
                self._scapy_sniff_loop()
        finally:
            self._cleanup()

    def _cleanup(self):
        """Cleanup resources"""
        self.log.info("Cleaning up...")
        
        # Flush any remaining packets
        self.producer.flush()
        
        # Log final stats
        self._log_stats()
        
        # Close producer
        self.producer.close()
        
        self.log.info("Cleanup complete")

    def get_stats(self):
        """Get comprehensive statistics"""
        kafka_stats = self.producer.get_stats()
        uptime = time.time() - self.stats['start_time']
        
        return {
            'device_id': self.device_id,
            'interface': self.iface,
            'uptime_seconds': uptime,
            'capture_stats': self.stats,
            'kafka_stats': kafka_stats,
            'capture_rate': self.stats['packets_captured'] / uptime if uptime > 0 else 0,
            'send_rate': self.stats['packets_sent'] / uptime if uptime > 0 else 0,
        }

