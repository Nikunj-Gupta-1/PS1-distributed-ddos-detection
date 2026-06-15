# Distributed DDoS Detection System - Deployment Guide

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Device 1      │    │   Device 2      │    │   Device N      │
│ Packet Capture  │    │ Packet Capture  │    │ Packet Capture  │
│                 │    │                 │    │                 │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          │         Raw Packets via Kafka               │
          │                      │                      │
          └──────────────────────┼──────────────────────┘
                                 │
                    ┌─────────────▼──────────────┐
                    │     Central Server         │
                    │                            │
                    │  ┌─────────────────────┐   │
                    │  │   Kafka Broker     │   │
                    │  └─────────────────────┘   │
                    │  ┌─────────────────────┐   │
                    │  │   Job Server       │   │
                    │  │ (Feature Extract)  │   │
                    │  └─────────────────────┘   │
                    └─────────────┬──────────────┘
                                  │
                                  │ ML API Requests
                                  │
                    ┌─────────────▼──────────────┐
                    │     Cloud ML Server        │
                    │   (AWS/GCP/Azure/Local)    │
                    │                            │
                    │  ┌─────────────────────┐   │
                    │  │   ML API Server    │   │
                    │  │ (DDoS Detection)   │   │
                    │  └─────────────────────┘   │
                    └────────────────────────────┘
```

## Components

### 1. Packet Capture Devices (Multiple)
- **Location**: Distributed across network (laptops, servers, IoT devices)
- **Function**: Capture raw packets and send to central Kafka
- **Code**: `simplified-packet-detector/`
- **Requirements**: Network access to central Kafka server

### 2. Central Job Server (Single)
- **Location**: Central server on the network
- **Function**: Receive packets, extract features, send to ML server
- **Code**: `jobserver2-main/`
- **Requirements**: Kafka broker, network access to ML server

### 3. ML Server (Single)
- **Location**: Cloud or central server
- **Function**: Provide ML predictions via REST API
- **Code**: `ml_server/`
- **Requirements**: Trained model, sufficient compute resources

## Network Setup

### Prerequisites
1. All devices on same network (WiFi/Ethernet)
2. Central server with static IP (e.g., 192.168.1.100)
3. Kafka installed on central server
4. Python 3.8+ on all devices

### Step 1: Setup Central Server

#### Install Kafka
```bash
# On central server (192.168.1.100)
# Install Java
sudo apt update
sudo apt install openjdk-11-jdk

# Download and install Kafka
wget https://downloads.apache.org/kafka/2.8.2/kafka_2.13-2.8.2.tgz
tar -xzf kafka_2.13-2.8.2.tgz
cd kafka_2.13-2.8.2

# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties &

# Start Kafka (configure for network access)
# Edit config/server.properties:
# listeners=PLAINTEXT://0.0.0.0:9092
# advertised.listeners=PLAINTEXT://192.168.1.100:9092
bin/kafka-server-start.sh config/server.properties &

# Create topic
bin/kafka-topics.sh --create --topic raw_packets --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1
```

#### Setup Job Server
```bash
# On central server
cd jobserver2-main

# Set environment variables
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export ML_API_URL=http://your-ml-server.com:8000/predict
export KAFKA_TOPIC=raw_packets

# Install dependencies
pip install -r ../requirements.txt

# Start job server
python main.py
```

### Step 2: Setup ML Server

#### Option A: Local ML Server (on central server)
```bash
cd ml_server
export ML_API_URL=http://localhost:8000/predict
python main.py
# Server runs on http://192.168.1.100:8000
```

#### Option B: Cloud ML Server
Deploy `ml_server/` to your cloud provider (AWS, GCP, Azure) and update `ML_API_URL`.

### Step 3: Setup Packet Capture Devices

#### On each capture device:
```bash
cd simplified-packet-detector

# Install dependencies
pip install -r ../requirements.txt

# Set Kafka server to central server
export KAFKA_BOOTSTRAP_SERVERS=192.168.1.100:9092

# Start packet capture
python main.py --kafka-servers 192.168.1.100:9092 --topic raw_packets

# Or with custom device ID
python main.py --kafka-servers 192.168.1.100:9092 --device-id laptop-001
```

## Configuration Examples

### Single Network Setup
```bash
# Central Server (192.168.1.100)
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
ML_API_URL=http://localhost:8000/predict

# Capture Devices
KAFKA_BOOTSTRAP_SERVERS=192.168.1.100:9092
```

### Cloud ML Setup
```bash
# Central Server
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
ML_API_URL=https://your-ml-api.amazonaws.com/predict

# Capture Devices
KAFKA_BOOTSTRAP_SERVERS=192.168.1.100:9092
```

### Multi-Server Kafka Setup
```bash
# For high availability, run Kafka on multiple servers
KAFKA_BOOTSTRAP_SERVERS=192.168.1.100:9092,192.168.1.101:9092,192.168.1.102:9092
```

## Testing the Setup

### 1. Test Kafka Connectivity
```bash
# From capture device, test Kafka connection
python -c "
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['192.168.1.100:9092'])
print('Kafka connection successful')
producer.close()
"
```

### 2. Test ML Server
```bash
# Test ML server API
curl -X POST http://192.168.1.100:8000/health
curl -X POST http://192.168.1.100:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"request_id":"test","src_ip":"1.1.1.1","dst_ip":"2.2.2.2","timestamp":1234567890,"protocol":6,"flow_duration":1000000,"total_fwd_packets":10,"total_backward_packets":5,"fwd_packet_length_max":1500,"fwd_packet_length_min":64,"fwd_packet_length_mean":800,"packet_length_mean":750,"packet_length_std":200,"flow_bytes_per_second":5000,"flow_packets_per_second":12,"flow_iat_mean":125000,"flow_iat_std":50000,"flow_iat_max":200000,"flow_iat_min":10000,"fwd_iat_total":1000000,"fwd_iat_mean":100000,"fwd_iat_std":30000,"fwd_iat_max":150000,"fwd_iat_min":50000,"bwd_iat_total":800000,"bwd_iat_mean":100000,"bwd_iat_std":25000,"bwd_iat_max":130000,"bwd_iat_min":70000,"fwd_psh_flags":1,"bwd_psh_flags":1,"fwd_urg_flags":0}'
```

### 3. Monitor Kafka Topics
```bash
# On central server, monitor packet flow
bin/kafka-console-consumer.sh --topic raw_packets --bootstrap-server localhost:9092
```

## Monitoring and Troubleshooting

### Check Kafka Topics
```bash
# List topics
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Check topic details
bin/kafka-topics.sh --describe --topic raw_packets --bootstrap-server localhost:9092

# Monitor consumer lag
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group ddos-detection-job-server
```

### Log Locations
- **Packet Capture**: Check console output for device stats
- **Job Server**: Check console for flow processing stats
- **ML Server**: Check console for prediction requests
- **Kafka**: Check `logs/` directory in Kafka installation

### Common Issues

#### 1. Kafka Connection Refused
- Check firewall on central server (port 9092)
- Verify `advertised.listeners` in Kafka config
- Test network connectivity: `telnet 192.168.1.100 9092`

#### 2. No Packets Captured
- Check network interface: `ip addr` or `ifconfig`
- Verify packet capture permissions (may need sudo)
- Check BPF filter settings

#### 3. ML Server Timeout
- Check ML server health endpoint
- Verify network connectivity to ML server
- Check ML server logs for errors

#### 4. High Memory Usage
- Reduce flow timeout in job server
- Increase number of processing workers
- Monitor Kafka consumer lag

## Performance Optimization

### For High Traffic Networks
1. **Increase Kafka partitions**: More parallel processing
2. **Add more job server workers**: Scale processing threads
3. **Use multiple Kafka brokers**: Distribute load
4. **Optimize batch sizes**: Larger batches for efficiency

### For Low Latency
1. **Reduce linger_ms**: Faster message sending
2. **Use gRPC for ML**: Lower overhead than REST
3. **Reduce flow timeout**: Faster feature extraction
4. **Local ML server**: Avoid network latency

### For Reliability
1. **Kafka replication**: Multiple broker copies
2. **Health checks**: Monitor all components
3. **Graceful shutdown**: Handle SIGTERM properly
4. **Error recovery**: Retry logic and dead letter queues

## Security Considerations

1. **Network Security**: Use VPN or isolated network
2. **Kafka Security**: Enable SASL/SSL authentication
3. **ML API Security**: Use API keys or OAuth
4. **Data Privacy**: Consider packet data sensitivity
5. **Access Control**: Limit device access to Kafka

## Scaling

### Horizontal Scaling
- **More capture devices**: Add devices to network
- **Multiple job servers**: Use different consumer groups
- **Kafka cluster**: Multiple brokers for availability
- **Load balanced ML**: Multiple ML server instances

### Vertical Scaling
- **More CPU/RAM**: For job server and ML server
- **Faster network**: Gigabit Ethernet for high traffic
- **SSD storage**: For Kafka logs and ML models