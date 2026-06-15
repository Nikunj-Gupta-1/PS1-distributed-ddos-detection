# file: packet_reconstructor.py

import time
import logging
import uuid
from collections import defaultdict
from statistics import mean, stdev
from scapy.all import Ether, IP, TCP, UDP
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class FlowStats:
    """Tracks statistics for one bidirectional flow (5-tuple)"""

    def __init__(self, first_packet_metadata):
        now = time.time()
        self.flow_id = str(uuid.uuid4())
        self.src_ip = first_packet_metadata.get('src_ip', '0.0.0.0')
        self.dst_ip = first_packet_metadata.get('dst_ip', '0.0.0.0')
        self.start_time = self.last_time = now
        self.primary_src = self.src_ip
        self.direction_set = True

        self.fwd_packets = self.bwd_packets = 0
        self.fwd_bytes = self.bwd_bytes = 0
        self.fwd_lengths = []
        self.bwd_lengths = []
        self.all_lengths = []

        self.fwd_iats = []
        self.bwd_iats = []
        self.last_fwd_ts = self.last_bwd_ts = 0.0

        self.fwd_psh = 0
        self.bwd_psh = 0
        self.fwd_urg = 0

        self.protocol = first_packet_metadata.get('protocol', 6)

    def update_with_packet_data(self, packet_metadata, packet_size):
        """Update flow stats with packet metadata"""
        now = time.time()
        
        # Determine direction
        src_ip = packet_metadata.get('src_ip', '0.0.0.0')
        fwd = src_ip == self.primary_src

        # Update packet stats
        if fwd:
            self.fwd_packets += 1
            self.fwd_bytes += packet_size
            self.fwd_lengths.append(packet_size)
            if self.last_fwd_ts:
                self.fwd_iats.append((now - self.last_fwd_ts) * 1_000_000)
            self.last_fwd_ts = now
        else:
            self.bwd_packets += 1
            self.bwd_bytes += packet_size
            self.bwd_lengths.append(packet_size)
            if self.last_bwd_ts:
                self.bwd_iats.append((now - self.last_bwd_ts) * 1_000_000)
            self.last_bwd_ts = now

        self.all_lengths.append(packet_size)

        # TCP flags from metadata
        if packet_metadata.get('has_tcp', False):
            tcp_flags = packet_metadata.get('tcp_flags', 0)
            if tcp_flags & 0x08:  # PSH
                if fwd:
                    self.fwd_psh += 1
                else:
                    self.bwd_psh += 1
            if tcp_flags & 0x20:  # URG
                if fwd:
                    self.fwd_urg += 1

        self.last_time = now

    def extract_features(self):
        """Extract 28 ML features from flow statistics"""
        duration = max(self.last_time - self.start_time, 1e-6)
        total_packets = self.fwd_packets + self.bwd_packets
        total_bytes = self.fwd_bytes + self.bwd_bytes
        all_iats = self.fwd_iats + self.bwd_iats

        def safe_mean(values):
            return mean(values) if values else 0.0

        def safe_stdev(values):
            return stdev(values) if len(values) > 1 else 0.0

        # Assemble 28-field feature vector
        features = {
            "request_id": self.flow_id,
            "src_ip": self.src_ip,
            "dst_ip": self.dst_ip,
            "timestamp": int(time.time() * 1_000_000),  # microseconds
            "protocol": self.protocol,
            "flow_duration": duration * 1_000_000,  # microseconds
            "total_fwd_packets": self.fwd_packets,
            "total_backward_packets": self.bwd_packets,
            "fwd_packet_length_max": max(self.fwd_lengths) if self.fwd_lengths else 0,
            "fwd_packet_length_min": min(self.fwd_lengths) if self.fwd_lengths else 0,
            "fwd_packet_length_mean": safe_mean(self.fwd_lengths),
            "packet_length_mean": safe_mean(self.all_lengths),
            "packet_length_std": safe_stdev(self.all_lengths),
            "flow_bytes_per_second": total_bytes / duration,
            "flow_packets_per_second": total_packets / duration,
            "flow_iat_mean": safe_mean(all_iats),
            "flow_iat_std": safe_stdev(all_iats),
            "flow_iat_max": max(all_iats) if all_iats else 0,
            "flow_iat_min": min(all_iats) if all_iats else 0,
            "fwd_iat_total": sum(self.fwd_iats),
            "fwd_iat_mean": safe_mean(self.fwd_iats),
            "fwd_iat_std": safe_stdev(self.fwd_iats),
            "fwd_iat_max": max(self.fwd_iats) if self.fwd_iats else 0,
            "fwd_iat_min": min(self.fwd_iats) if self.fwd_iats else 0,
            "bwd_iat_total": sum(self.bwd_iats),
            "bwd_iat_mean": safe_mean(self.bwd_iats),
            "bwd_iat_std": safe_stdev(self.bwd_iats),
            "bwd_iat_max": max(self.bwd_iats) if self.bwd_iats else 0,
            "bwd_iat_min": min(self.bwd_iats) if self.bwd_iats else 0,
            "fwd_psh_flags": self.fwd_psh,
            "bwd_psh_flags": self.bwd_psh,
            "fwd_urg_flags": self.fwd_urg,
        }

        return features


class PacketReconstructor:
    """
    Reconstructs packets from Kafka messages and maintains flow state.
    Extracts features when flows are complete or timeout.
    """

    def __init__(self, flow_timeout=60):
        self.flows = defaultdict(lambda: None)
        self.flow_timeout = flow_timeout
        self.stats = {
            'packets_processed': 0,
            'flows_created': 0,
            'flows_completed': 0,
            'reconstruction_errors': 0,
            'start_time': time.time()
        }
        logger.info(f"PacketReconstructor initialized with {flow_timeout}s flow timeout")

    def process_packet_message(self, packet_message) -> Optional[Dict]:
        """
        Process a packet message from Kafka and return flow features if ready.
        
        Args:
            packet_message: Dict containing packet data and metadata
            
        Returns:
            Dict with flow features if flow is complete, None otherwise
        """
        try:
            self.stats['packets_processed'] += 1
            
            # Extract packet data
            device_id = packet_message.get('device_id', 'unknown')
            packet_hex = packet_message.get('packet_data', '')
            metadata = packet_message.get('metadata', {})
            packet_size = packet_message.get('packet_size', 0)
            
            # Validate packet data
            if not packet_hex or not metadata.get('has_ip', False):
                logger.debug("Skipping non-IP packet")
                return None
            
            # Generate flow key
            flow_key = self._generate_flow_key(metadata)
            if not flow_key:
                return None
            
            # Get or create flow
            flow = self.flows.get(flow_key)
            if flow is None:
                flow = FlowStats(metadata)
                self.flows[flow_key] = flow
                self.stats['flows_created'] += 1
                logger.debug(f"Created new flow: {metadata.get('src_ip')} -> {metadata.get('dst_ip')}")
            
            # Update flow with packet
            flow.update_with_packet_data(metadata, packet_size)
            
            # Check if flow should be completed
            if self._should_complete_flow(flow):
                features = flow.extract_features()
                del self.flows[flow_key]
                self.stats['flows_completed'] += 1
                logger.debug(f"Completed flow: {features['src_ip']} -> {features['dst_ip']}")
                return features
            
            # Periodic cleanup
            if self.stats['packets_processed'] % 1000 == 0:
                self._cleanup_expired_flows()
                self._log_stats()
            
            return None
            
        except Exception as e:
            self.stats['reconstruction_errors'] += 1
            logger.error(f"Error processing packet message: {e}")
            return None

    def _generate_flow_key(self, metadata):
        """Generate flow key from packet metadata"""
        try:
            src_ip = metadata.get('src_ip')
            dst_ip = metadata.get('dst_ip')
            protocol = metadata.get('protocol', 0)
            src_port = metadata.get('src_port', 0)
            dst_port = metadata.get('dst_port', 0)
            
            if not src_ip or not dst_ip:
                return None
            
            # Create bidirectional flow key
            return tuple(sorted([(src_ip, src_port), (dst_ip, dst_port)]) + [protocol])
            
        except Exception as e:
            logger.error(f"Error generating flow key: {e}")
            return None

    def _should_complete_flow(self, flow):
        """Determine if flow should be completed and sent for ML inference"""
        # Complete flow if it has enough packets or has been active long enough
        total_packets = flow.fwd_packets + flow.bwd_packets
        flow_age = time.time() - flow.start_time
        
        return (total_packets >= 10 or  # Enough packets
                flow_age >= 30 or       # Active for 30 seconds
                total_packets >= 5 and flow_age >= 10)  # Some packets and some time

    def _cleanup_expired_flows(self):
        """Remove expired flows and extract features"""
        now = time.time()
        expired_flows = []
        
        for flow_key, flow in self.flows.items():
            if now - flow.last_time > self.flow_timeout:
                expired_flows.append((flow_key, flow))
        
        if expired_flows:
            logger.info(f"Cleaning up {len(expired_flows)} expired flows")
            
        for flow_key, flow in expired_flows:
            # Extract features from expired flow if it has any packets
            if flow.fwd_packets + flow.bwd_packets > 0:
                features = flow.extract_features()
                # Note: These expired flows could be processed separately if needed
                logger.debug(f"Expired flow: {features['src_ip']} -> {features['dst_ip']}")
            
            del self.flows[flow_key]
            self.stats['flows_completed'] += 1

    def _log_stats(self):
        """Log processing statistics"""
        uptime = time.time() - self.stats['start_time']
        pps = self.stats['packets_processed'] / uptime if uptime > 0 else 0
        
        logger.info(f"Reconstructor stats - Packets: {self.stats['packets_processed']} "
                   f"({pps:.1f}/s), Active flows: {len(self.flows)}, "
                   f"Completed: {self.stats['flows_completed']}, "
                   f"Errors: {self.stats['reconstruction_errors']}")

    def get_stats(self):
        """Get comprehensive statistics"""
        uptime = time.time() - self.stats['start_time']
        return {
            **self.stats,
            'active_flows': len(self.flows),
            'uptime_seconds': uptime,
            'packets_per_second': self.stats['packets_processed'] / uptime if uptime > 0 else 0,
        }

    def force_complete_all_flows(self):
        """Force completion of all active flows (for shutdown)"""
        completed_features = []
        
        for flow_key, flow in list(self.flows.items()):
            if flow.fwd_packets + flow.bwd_packets > 0:
                features = flow.extract_features()
                completed_features.append(features)
                self.stats['flows_completed'] += 1
        
        self.flows.clear()
        logger.info(f"Force completed {len(completed_features)} flows")
        return completed_features