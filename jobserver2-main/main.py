# file: src/main.py

import time
import logging
import signal
import sys
from threading_manager import run_in_thread, shutdown_executor
from kafka_consumer import consume_raw_packets
from processor import process_data, shutdown_processor, get_processor_stats

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, shutting down...")
    
    # Shutdown processor first to complete flows
    shutdown_processor()
    
    # Then shutdown thread executor
    shutdown_executor()
    sys.exit(0)

def log_stats():
    """Log system statistics periodically"""
    while True:
        try:
            time.sleep(30)  # Log every 30 seconds
            stats = get_processor_stats()
            logger.info(f"System stats - Queue: {stats['queue_size']}, "
                       f"Active flows: {stats['reconstructor_stats']['active_flows']}, "
                       f"Packets/s: {stats['reconstructor_stats']['packets_per_second']:.1f}")
        except Exception as e:
            logger.error(f"Error logging stats: {e}")

if __name__ == "__main__":
    logger.info("[Main] Starting Distributed DDoS Detection Job Server...")
    logger.info("[Main] This server processes raw packets from multiple capture devices")
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Start the Kafka consumer for raw packets
        run_in_thread(consume_raw_packets)
        logger.info("[Main] Raw packet consumer thread started")

        # Start multiple processing threads
        num_workers = 4
        for i in range(num_workers):
            run_in_thread(process_data)
        logger.info(f"[Main] {num_workers} packet processor threads started")

        # Start stats logging thread
        run_in_thread(log_stats)
        logger.info("[Main] Statistics logging thread started")

        logger.info("[Main] ✅ All systems operational - processing packets from network devices")

        # Keep main thread alive
        while True:
            time.sleep(1)
    
    except Exception as e:
        logger.error(f"[Main] Fatal error: {e}", exc_info=True)
        shutdown_processor()
        shutdown_executor()
        sys.exit(1)
