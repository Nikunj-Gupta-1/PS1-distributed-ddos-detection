# DDoS Detection System - Deployment Guide

## Quick Start

### 1. Set Environment Variables

```bash
# Copy example config
cp .env.example .env

# Edit .env with your values
export ML_API_URL=http://localhost:8000/predict
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=network_flows
```

### 2. Start ML Server

```bash
cd ml_server
python main.py
# Server runs on http://localhost:8000
# gRPC server runs on localhost:50051
```

### 3. Start Packet Detector

```bash
cd simplified-packet-detector
python main.py --iface auto --topic network_flows
```

### 4. Start Job Server

```bash
cd jobserver2-main
python main.py
```

## Architecture

```
Network Traffic
    ↓
Packet Detector (simplified-packet-detector/)
    ├─ Captures packets
    ├─ Extracts 28 features
    └─ Sends to Kafka
         ↓
    Job Server (jobserver2-main/)
    ├─ Consumes from Kafka
    ├─ Validates data
    ├─ Retries on failure
    └─ Sends to ML Server
         ↓
    ML Server (ml_server/)
    ├─ REST API: /predict, /predict/batch, /health
    └─ gRPC: Predict, PredictBatch, HealthCheck
```

## Configuration

### ML Server
- **Port**: 8000 (REST), 50051 (gRPC)
- **Model**: model4.pkl (best performing)
- **Features**: 28 network flow features
- **Output**: Binary classification (0=Benign, 1=Attack)

### Job Server
- **Workers**: 4 threads (configurable)
- **Queue Size**: 10,000 items
- **Retry Logic**: 3 attempts with 1s delay
- **Timeout**: 10 seconds per request

### Packet Detector
- **Interface**: auto-detect or specify (e.g., eth0, en0)
- **Flow Timeout**: 300 seconds
- **Pruning**: Every 100 packets

## Monitoring

### Health Checks

```bash
# REST API health
curl http://localhost:8000/health

# Expected response:
{
  "is_healthy": true,
  "model_status": "loaded",
  "version": "1.0.0",
  "uptime_seconds": 1234,
  "feature_count": 28
}
```

### Logs

All components use Python logging. Check logs for:
- Queue saturation warnings
- ML server connection failures
- Retry attempts
- Attack detections

### Metrics to Monitor

1. **Queue Depth**: If consistently high, increase workers
2. **Retry Rate**: If high, check ML server health
3. **Processing Latency**: Should be <100ms per flow
4. **Attack Detection Rate**: Baseline for your network

## Troubleshooting

### ML Server Won't Start
```bash
# Check if port 8000 is in use
lsof -i :8000

# Check model file exists
ls -la ml_server/model4.pkl
```

### Job Server Can't Connect to Kafka
```bash
# Verify Kafka is running
kafka-topics.sh --list --bootstrap-server localhost:9092

# Check KAFKA_BOOTSTRAP_SERVERS env var
echo $KAFKA_BOOTSTRAP_SERVERS
```

### Packet Detector Permission Denied
```bash
# May need sudo for packet capture
sudo python main.py --iface auto

# Or use tcpdump to verify interface
sudo tcpdump -i auto -c 5
```

### High Queue Saturation
```bash
# Increase workers in jobserver2-main/main.py
# Change: for i in range(4):
# To:     for i in range(8):  # or more
```

## Production Deployment

### Systemd Service Files

Create `/etc/systemd/system/ddos-ml-server.service`:
```ini
[Unit]
Description=DDoS Detection ML Server
After=network.target

[Service]
Type=simple
User=ddos
WorkingDirectory=/opt/ddos-detection/ml_server
Environment="ML_API_URL=http://localhost:8000/predict"
ExecStart=/usr/bin/python3 main.py
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Create `/etc/systemd/system/ddos-job-server.service`:
```ini
[Unit]
Description=DDoS Detection Job Server
After=network.target kafka.service ddos-ml-server.service

[Service]
Type=simple
User=ddos
WorkingDirectory=/opt/ddos-detection/jobserver2-main
Environment="ML_API_URL=http://localhost:8000/predict"
Environment="KAFKA_BOOTSTRAP_SERVERS=localhost:9092"
ExecStart=/usr/bin/python3 main.py
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### Enable and Start Services
```bash
sudo systemctl daemon-reload
sudo systemctl enable ddos-ml-server ddos-job-server
sudo systemctl start ddos-ml-server ddos-job-server
sudo systemctl status ddos-ml-server ddos-job-server
```

## Performance Tuning

### For High Traffic Networks
1. Increase job server workers: 8-16 threads
2. Increase queue size: 50,000 items
3. Use gRPC instead of REST (lower latency)
4. Consider batch predictions for throughput

### For Low Latency
1. Use gRPC API (50-100ms faster than REST)
2. Reduce flow timeout (faster memory cleanup)
3. Increase pruning frequency

### For Memory Efficiency
1. Reduce flow timeout (300s → 60s)
2. Increase pruning frequency (every 50 packets)
3. Monitor active flows count

## Security Considerations

1. **ML Server**: Currently uses insecure gRPC. Add TLS for production:
   ```python
   # In main.py, replace add_insecure_port with:
   grpc_server.add_secure_port('[::]:50051', grpc.ssl_server_credentials(...))
   ```

2. **Kafka**: Use SASL/SSL authentication in production

3. **Network**: Run on isolated network segment, restrict access to ports 8000 and 50051

4. **Logging**: Don't log sensitive IPs in production, use anonymization

## Backup and Recovery

1. **Model Backup**: Keep model4.pkl in version control
2. **Configuration**: Store .env in secure location
3. **Logs**: Archive logs regularly for compliance

## Scaling

### Horizontal Scaling
- Run multiple job server instances (different consumer groups)
- Use load balancer for ML server (stateless)
- Kafka handles distribution automatically

### Vertical Scaling
- Increase workers in job server
- Increase ML server batch size
- Use faster hardware for packet capture
