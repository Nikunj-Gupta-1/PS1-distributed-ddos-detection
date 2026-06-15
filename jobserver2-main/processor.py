# file: src/streaming/processor.py

import queue
import uuid
import time
import logging
from ml_client import send_to_ml_rest
from packet_reconstructor import PacketReconstructor

# Configure logging
logger = logging.getLogger(__name__)

QUEUE_SIZE = 10000
QUEUE_TIMEOUT = 5

processing_queue = queue.Queue(maxsize=QUEUE_SIZE)

# Global packet reconstructor
packet_reconstructor = PacketReconstructor(flow_timeout=60)

def add_to_queue(packet_message):
    """Add packet message to processing queue with backpressure handling"""
    try:
        processing_queue.put_nowait(packet_message)
        logger.debug("[Processor] Packet message added to queue")
    except queue.Full:
        logger.error(f"[Processor] ❌ Queue full ({QUEUE_SIZE} items), dropping packet - consider increasing workers")

def process_data():
    """Process packet messages from queue and extract flow features"""
    logger.info("[Processor] ✅ Packet processor thread started")
    
    while True:
        try:
            packet_message = processing_queue.get(timeout=QUEUE_TIMEOUT)
            logger.debug("[Processor] ✉️ Pulled packet message from queue")
        except queue.Empty:
            logger.debug("[Processor] ⏳ Queue empty, waiting...")
            continue

        try:
            # Process packet and extract features if flow is ready
            flow_features = packet_reconstructor.process_packet_message(packet_message)
            
            if flow_features:
                # We have a complete flow - send to ML server
                logger.info(f"[Processor] 🚦 Complete flow: {flow_features['src_ip']} → {flow_features['dst_ip']}")
                
                # Call ML prediction
                prediction = send_to_ml_rest(flow_features)

                if not prediction:
                    logger.warning("[Processor] ⚠️ No prediction received from ML server")
                elif prediction.get("prediction") == 1:
                    logger.warning(f"[Processor] 🚨 MALICIOUS DETECTED | "
                                 f"Flow: {flow_features['src_ip']} → {flow_features['dst_ip']} | "
                                 f"Confidence = {prediction.get('confidence', 0):.3f}")
                else:
                    logger.info(f"[Processor] ✅ BENIGN Flow | "
                              f"Flow: {flow_features['src_ip']} → {flow_features['dst_ip']} | "
                              f"Confidence = {prediction.get('confidence', 0):.3f}")
            else:
                # Packet added to flow, but flow not complete yet
                logger.debug("[Processor] Packet added to flow (not complete)")

        except Exception as e:
            logger.error(f"[Processor] ❌ Processing error: {e}", exc_info=True)

        finally:
            processing_queue.task_done()

def get_processor_stats():
    """Get processing statistics"""
    return {
        'queue_size': processing_queue.qsize(),
        'reconstructor_stats': packet_reconstructor.get_stats()
    }

def shutdown_processor():
    """Shutdown processor and complete remaining flows"""
    logger.info("[Processor] Shutting down processor...")
    
    # Force complete all active flows
    remaining_flows = packet_reconstructor.force_complete_all_flows()
    
    if remaining_flows:
        logger.info(f"[Processor] Processing {len(remaining_flows)} remaining flows...")
        for flow_features in remaining_flows:
            try:
                prediction = send_to_ml_rest(flow_features)
                if prediction and prediction.get("prediction") == 1:
                    logger.warning(f"[Processor] 🚨 FINAL MALICIOUS DETECTED | "
                                 f"Flow: {flow_features['src_ip']} → {flow_features['dst_ip']} | "
                                 f"Confidence = {prediction.get('confidence', 0):.3f}")
            except Exception as e:
                logger.error(f"[Processor] Error processing final flow: {e}")
    
    logger.info("[Processor] Processor shutdown complete")
