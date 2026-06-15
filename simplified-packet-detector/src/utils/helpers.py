# file: src/utils/helpers.py
"""
Utility constants + math helpers
"""
from statistics import mean, stdev
from typing import List


FEATURE_NAMES = [
    "request_id",
    "src_ip",
    "dst_ip",
    "timestamp",
    "protocol",
    "flow_duration",
    "total_fwd_packets",
    "total_backward_packets",
    "fwd_packet_length_max",
    "fwd_packet_length_min",
    "fwd_packet_length_mean",
    "packet_length_mean",
    "packet_length_std",
    "flow_bytes_per_second",
    "flow_packets_per_second",
    "flow_iat_mean",
    "flow_iat_std",
    "flow_iat_max",
    "flow_iat_min",
    "fwd_iat_total",
    "fwd_iat_mean",
    "fwd_iat_std",
    "fwd_iat_max",
    "fwd_iat_min",
    "bwd_iat_total",
    "bwd_iat_mean",
    "bwd_iat_std",
    "bwd_iat_max",
    "bwd_iat_min",
    "fwd_psh_flags",
    "bwd_psh_flags",
    "fwd_urg_flags",
]


def safe_mean(values: List[float]) -> float:
    return mean(values) if values else 0.0


def safe_stdev(values: List[float]) -> float:
    return stdev(values) if len(values) > 1 else 0.0

