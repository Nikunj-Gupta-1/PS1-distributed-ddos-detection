#!/bin/bash
# Quick start script for packet capture devices

set -e

# Configuration
KAFKA_SERVER=${KAFKA_SERVER:-"192.168.1.100:9092"}
DEVICE_ID=${DEVICE_ID:-""}
INTERFACE=${INTERFACE:-"auto"}
TOPIC=${TOPIC:-"raw_packets"}

echo "🚀 Starting DDoS Detection Packet Capture Device"
echo "================================================"
echo "Kafka Server: $KAFKA_SERVER"
echo "Device ID: ${DEVICE_ID:-"auto-generated"}"
echo "Interface: $INTERFACE"
echo "Topic: $TOPIC"
echo ""

# Check if we're in the right directory
if [ ! -f "simplified-packet-detector/main.py" ]; then
    echo "❌ Error: Please run this script from the project root directory"
    echo "   Current directory: $(pwd)"
    echo "   Expected files: simplified-packet-detector/main.py"
    exit 1
fi

# Check Python dependencies
echo "📦 Checking Python dependencies..."
cd simplified-packet-detector

if ! python -c "import scapy, kafka, yaml" 2>/dev/null; then
    echo "⚠️  Installing missing dependencies..."
    pip install -r ../requirements.txt
fi

# Test Kafka connectivity
echo "🔗 Testing Kafka connectivity..."
python -c "
from kafka import KafkaProducer
import sys
try:
    producer = KafkaProducer(bootstrap_servers=['$KAFKA_SERVER'], request_timeout_ms=5000)
    producer.close()
    print('✅ Kafka connection successful')
except Exception as e:
    print(f'❌ Kafka connection failed: {e}')
    print('   Please check:')
    print('   1. Kafka server is running on $KAFKA_SERVER')
    print('   2. Network connectivity to Kafka server')
    print('   3. Firewall settings (port 9092)')
    sys.exit(1)
"

# Check network interface
echo "🌐 Checking network interface..."
if [ "$INTERFACE" = "auto" ]; then
    echo "   Using auto-detection"
else
    if ! ip link show "$INTERFACE" >/dev/null 2>&1; then
        echo "❌ Error: Network interface '$INTERFACE' not found"
        echo "   Available interfaces:"
        ip link show | grep -E "^[0-9]+:" | cut -d: -f2 | sed 's/^ */     /'
        exit 1
    fi
    echo "   Using interface: $INTERFACE"
fi

# Check permissions for packet capture
echo "🔒 Checking packet capture permissions..."
if [ "$EUID" -ne 0 ] && [ "$(uname)" = "Linux" ]; then
    echo "⚠️  Warning: Running without root privileges on Linux"
    echo "   Packet capture may fail. Consider running with sudo:"
    echo "   sudo ./start_capture_device.sh"
    echo ""
    echo "   Continuing anyway (may work with proper capabilities)..."
fi

# Start packet capture
echo "📡 Starting packet capture..."
echo "   Press Ctrl+C to stop"
echo ""

# Build command
CMD="python main.py --kafka-servers $KAFKA_SERVER --topic $TOPIC --iface $INTERFACE"
if [ -n "$DEVICE_ID" ]; then
    CMD="$CMD --device-id $DEVICE_ID"
fi

echo "Running: $CMD"
echo ""

# Execute
exec $CMD