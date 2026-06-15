# file: src/streaming/ml_client.py

import os
import uuid
import time
import json
import logging
import requests
from typing import Optional, Dict

# Configure logging
logger = logging.getLogger(__name__)

ML_API_URL = os.getenv("ML_API_URL")
if not ML_API_URL:
    raise ValueError("ML_API_URL environment variable is required")

MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds
REQUEST_TIMEOUT = 10

REQUIRED_FIELDS = [
    "protocol", "flow_duration", "total_fwd_packets", "total_backward_packets",
    "fwd_packet_length_max", "fwd_packet_length_min", "fwd_packet_length_mean",
    "packet_length_mean", "packet_length_std", "flow_bytes_per_second",
    "flow_packets_per_second", "flow_iat_mean", "flow_iat_std", "flow_iat_max",
    "flow_iat_min", "fwd_iat_total", "fwd_iat_mean", "fwd_iat_std", "fwd_iat_max",
    "fwd_iat_min", "bwd_iat_total", "bwd_iat_mean", "bwd_iat_std", "bwd_iat_max",
    "bwd_iat_min", "fwd_psh_flags", "bwd_psh_flags", "fwd_urg_flags"
]

def validate_flow_data(flow_data: Dict) -> bool:
    """Validate that all required fields are present and numeric"""
    missing_fields = []
    for field in REQUIRED_FIELDS:
        if field not in flow_data:
            missing_fields.append(field)
        elif not isinstance(flow_data[field], (int, float)):
            logger.warning(f"[ML Client] Field '{field}' is not numeric: {type(flow_data[field])}")
    
    if missing_fields:
        logger.error(f"[ML Client] Missing required fields: {missing_fields}")
        return False
    return True

def send_to_ml_rest(flow_data: Dict) -> Optional[Dict]:
    """Send flow data to ML server with retry logic"""
    
    # Validate input
    if not validate_flow_data(flow_data):
        logger.error("[ML Client] ❌ Flow data validation failed")
        return None
    
    # Compose payload
    enriched_data = {
        "request_id": flow_data.get("request_id", str(uuid.uuid4())),
        "src_ip": flow_data.get("src_ip", "0.0.0.0"),
        "dst_ip": flow_data.get("dst_ip", "0.0.0.0"),
        "timestamp": flow_data.get("timestamp", int(time.time() * 1_000_000)),
    }
    
    for key in REQUIRED_FIELDS:
        enriched_data[key] = flow_data.get(key, 0)
    
    headers = {"Content-Type": "application/json"}
    
    # Retry logic
    for attempt in range(MAX_RETRIES):
        try:
            logger.debug(f"[ML Client] Attempt {attempt + 1}/{MAX_RETRIES} - Sending to {ML_API_URL}")
            
            response = requests.post(
                ML_API_URL,
                json=enriched_data,
                headers=headers,
                timeout=REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"[ML Client] ✅ Prediction - ID: {result.get('request_id')}, "
                          f"Result: {result.get('prediction')}, Confidence: {result.get('confidence'):.3f}")
                return result
            elif response.status_code >= 500:
                # Server error - retry
                logger.warning(f"[ML Client] Server error {response.status_code}, retrying...")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    continue
            else:
                # Client error - don't retry
                logger.error(f"[ML Client] Client error {response.status_code}: {response.text}")
                return None
        
        except requests.exceptions.Timeout:
            logger.warning(f"[ML Client] Request timeout (attempt {attempt + 1}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
        
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"[ML Client] Connection error (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
        
        except Exception as e:
            logger.error(f"[ML Client] Unexpected error: {e}")
            return None
    
    logger.error(f"[ML Client] ❌ Failed after {MAX_RETRIES} attempts")
    return None
