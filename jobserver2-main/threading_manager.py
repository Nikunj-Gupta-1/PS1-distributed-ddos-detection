# file: src/streaming/threading_manager.py

import logging
import atexit
from concurrent.futures import ThreadPoolExecutor
import os

logger = logging.getLogger(__name__)

executor = ThreadPoolExecutor(max_workers=os.cpu_count() * 2)

def run_in_thread(task, *args, **kwargs):
    """Submit a task to run in a background thread"""
    future = executor.submit(task, *args, **kwargs)
    return future

def shutdown_executor():
    """Gracefully shutdown the thread pool"""
    logger.info("Shutting down thread pool executor...")
    executor.shutdown(wait=True, timeout=30)
    logger.info("Thread pool executor shut down complete")

# Register shutdown handler
atexit.register(shutdown_executor)
