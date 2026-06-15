# lts-distributed-ddos-detection

A **distributed, multi-device DDoS detection pipeline** built on Kafka, FastAPI, and gRPC. The architecture separates packet capture (lightweight edge agents) from feature extraction and ML inference (centralised servers), enabling network-wide monitoring at scale. Built during **Practice School I (PS-1)**.

---

## How This Fits Into the Larger Work

This is the **third and most advanced phase** of the LTS project series — a complete redesign from single-machine to distributed:

```
lts-coursework-experiments         (early classifiers and Scapy experiments)
        ↓
lts-monolithic-ddos-detection      (single-machine sniffer + local inference)
        ↓
lts-distributed-ddos-detection    ← you are here

healthcare-prediction              (separate parallel project)
```

---

## Architecture

```
Capture Devices (multiple)
  └── simplified-packet-detector/
        │  Scapy sniffer → raw hex packets
        ▼
      Kafka broker  (raw_packets topic)
        ▼
Central Job Server
  └── jobserver2-main/
        │  Kafka consumer → flow reconstruction → 28 features
        ▼
ML Inference Server
  └── ml_server/
        │  FastAPI REST + gRPC
        │  Loads model4.pkl → prediction + confidence
        ▼
      Alert output / logs
```

---

## What's in This Repo

### `simplified-packet-detector/` — Edge capture agent
- `main.py` — Entry point; auto-detects network interface
- `src/capture/packet_capture.py` — `DistributedPacketSniffer`; minimal per-packet processing
- `src/streaming/kafka_producer.py` — `OptimizedKafkaSender`; batched, compressed Kafka publishing
- `src/utils/` — shared helpers
- `config/kafka_config.yaml` — Kafka network settings

### `jobserver2-main/` — Central processing server
- `main.py` — Starts consumer threads with signal-safe shutdown
- `kafka_consumer.py` — Consumes raw packets from Kafka
- `packet_reconstructor.py` — Rebuilds bidirectional flows; computes 28 features
- `processor.py` — Orchestrates flow → ML client pipeline
- `ml_client.py` — HTTP client to ML server with retry logic
- `threading_manager.py` — Thread pool lifecycle management

### `ml_server/` — Inference server
- `main.py` — Starts FastAPI + gRPC servers simultaneously
- `serve.py` — `MLModelService`; loads model, exposes `/predict` and `/health`
- `model_trainer.py` — Trains the API-compatible 28-feature ensemble model
- `prediction.proto` / `prediction_pb2*.py` — gRPC service definitions
- `model_training_summary.txt` — Training summary log

### Top-level
| File | Description |
|---|---|
| `.env.example` | Configuration template for all environment variables |
| `requirements.txt` | Python dependencies |
| `setup.sh` | One-shot environment setup script |
| `start_capture_device.sh` | Launches packet capture on an edge device |
| `start_job_server.sh` | Launches the central job server |

### Documents (`documents/`)
| Subfolder | Contents |
|---|---|
| `documents/technical/` | Deployment guides, changelogs, bug-fix explanations (5 Markdown files) |
| `documents/reports/` | Final PS-1 report, LTS project summary, similarity check, formatted report |
| `documents/presentations/` | Final project presentation, midsem presentation |
| `documents/admin/` | Submission receipt |

---

## Setup and Deployment

Copy `.env.example` to `.env` and fill in your values:
```bash
cp .env.example .env
# Set ML_API_URL and KAFKA_BOOTSTRAP_SERVERS
```

See `documents/technical/DEPLOYMENT.md` for the full single-machine setup guide and `documents/technical/DISTRIBUTED_DEPLOYMENT.md` for multi-device deployment.

**Quick start (local, all on one machine):**
```bash
# Terminal 1 — ML server
cd ml_server && python main.py

# Terminal 2 — Job server
bash start_job_server.sh

# Terminal 3 — Packet capture (requires root)
sudo bash start_capture_device.sh
```

---

## Data and Models Not Included

Large files are **not** in this repo. They live locally under `raw data collected for project/`:

| File | Size | Used by |
|---|---|---|
| `model4.pkl` | 51 MB | `ml_server/serve.py` (production model) |
| `model1.pkl` – `model3.pkl` | 48–59 MB each | Earlier training iterations |
| `CIC_DDoS2019_To_Use.csv` | 54 MB | `ml_server/model_trainer.py` |

The `ml_server/serve.py` resolves the model path dynamically. Update the constant if running from a different layout.

---

## See Also
- [`lts-coursework-experiments`](https://github.com/nikunj-gupta-1/lts-coursework-experiments) — precursor coursework and experiments
- [`lts-monolithic-ddos-detection`](https://github.com/nikunj-gupta-1/lts-monolithic-ddos-detection) — the monolithic predecessor
- [`healthcare-prediction`](https://github.com/nikunj-gupta-1/healthcare-prediction) — separate Streamlit ML dashboard
