#!/bin/bash
# Quick start script for central job server

set -e

# Configuration
KAFKA_SERVERS=${KAFKA_SERVERS:-"localhost:9092"}
ML_API_URL=${ML_API_URL:-"http://localhost:8000/predict"}
TOPIC=${TOPIC:-"raw_packets"}
WORKERS=${WORKERS:-4}

echo "🚀 Starting DDoS Detection Job Server (Central)"
echo "==============================================="
echo "Kafka Servers: $KAFKA_SERVERS"
echo "ML API URL: $ML_API_URL"
echo "Topic: $TOPIC"
echo "Workers: $WORKERS"
echo ""

# Check if we're in the right directory
if [ ! -f "jobserver2-main/main.py" ]; then
    echo "❌ Error: Please run this script from the project root directory"
    echo "   Current directory: $(pwd)"
    echo "   Expected files: jobserver2-main/main.py"
    exit 1
fi

# Check Python dependencies
echo "📦 Checking Python dependencies..."
cd jobserver2-main

if ! python -c "import kafka, requests, scapy" 2>/dev/null; then
    echo "⚠️  Installing missing dependencies..."
    pip install -r ../requirements.txt
fi

# Test Kafka connectivity
echo "🔗 Testing Kafka connectivity..."
python -c "
from kafka import KafkaConsumer
import sys
try:
    consumer = KafkaConsumer(bootstrap_servers=['$KAFKA_SERVERS'], request_timeout_ms=5000)
    consumer.close()
    print('✅ Kafka connection successful')
except Exception as e:
    print(f'❌ Kafka connection failed: {e}')
    print('   Please check:')
    print('   1. Kafka server is running')
    print('   2. Kafka topic \"$TOPIC\" exists')
    print('   3. Network connectivity')
    sys.exit(1)
"

# Test ML API connectivity
echo "🤖 Testing ML API connectivity..."
python -c "
import requests
import sys
try:
    response = requests.get('$ML_API_URL'.replace('/predict', '/health'), timeout=5)
    if response.status_code == 200:
        print('✅ ML API connection successful')
    else:
        print(f'⚠️  ML API returned status {response.status_code}')
except Exception as e:
    print(f'⚠️  ML API connection failed: {e}')
    print('   Job server will start anyway, but predictions may fail')
    print('   Please check ML server is running at: $ML_API_URL')
"

# Set environment variables
export KAFKA_BOOTSTRAP_SERVERS="$KAFKA_SERVERS"
export ML_API_URL="$ML_API_URL"
export KAFKA_TOPIC="$TOPIC"

echo ""
echo "🎯 Starting job server..."
echo "   Processing packets from capture devices"
echo "   Press Ctrl+C to stop"
echo ""

# Start job server
exec python main.py