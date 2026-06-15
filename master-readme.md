# PS-1 Project Overview — Real-Time DDoS Detection and Healthcare Prediction

This file describes the complete set of repositories produced during **Practice School I (PS-1), BITS Pilani**, completed at Leo Technosoft Pvt. Ltd. The work spans network security, machine learning, and distributed systems.

---

## Repositories at a Glance

| Repo | Description |
|---|---|
| [`lts-coursework-experiments`](#lts-coursework-experiments) | Early ML classifiers and live packet sniffing experiments |
| [`lts-monolithic-ddos-detection`](#lts-monolithic-ddos-detection) | Single-machine real-time DDoS detection with local ML inference |
| [`lts-distributed-ddos-detection`](#lts-distributed-ddos-detection) | Distributed pipeline: edge capture agents → Kafka → central ML server |
| [`healthcare-prediction`](#healthcare-prediction) | Streamlit dashboard for synthetic healthcare risk prediction |

---

## Project Evolution

The DDoS detection work followed a clear progressive arc:

```
[Phase 1] lts-coursework-experiments
  └── Learning phase: batch ML on intrusion datasets + Scapy packet sniffing trials

[Phase 2] lts-monolithic-ddos-detection
  └── Applied phase: real-time sniffer + feature extraction + local Random Forest inference
      running together on a single machine

[Phase 3] lts-distributed-ddos-detection
  └── Production phase: architecture split into microservices
        - Lightweight capture agents stream raw packets to Kafka
        - Central job server reconstructs flows and extracts 28 features
        - Cloud-capable ML server exposes REST + gRPC prediction API
```

The **healthcare-prediction** project ran in parallel, independently, as a separate ML engineering exercise using synthetic medical data.

---

## `lts-coursework-experiments`

**What it is:** The foundation of the DDoS detection work. Eight progressive Python assignments implementing Random Forest, XGBoost, LightGBM, and ensemble classifiers on cybersecurity intrusion data. Also contains early Scapy-based network packet sniffers.

**Key files:** `assign1first.py` – `assign8perp.py`, `SNA.py`, `scapy_trial.py`

**Dataset used (not in repo):** `cybersecurity_intrusion_data.csv`

---

## `lts-monolithic-ddos-detection`

**What it is:** The first working real-time DDoS detection system. A single Python process sniffs live network traffic using Scapy, computes 28 flow-level features from each bidirectional flow, and classifies traffic using a locally-loaded Random Forest model. Achieved high accuracy on the CIC-DDoS2019 benchmark.

**Key files:** `real_time_ddos_detection.py` (main), `train.py`, `predict_ddos.py`, `reducer.py`

**Data/models (not in repo):** `CIC_DDoS2019_To_Use.csv` (54 MB), `optimized_ddos_model.pkl` (37 MB)

---

## `lts-distributed-ddos-detection`

**What it is:** A complete architectural redesign for network-wide deployment. Three decoupled components:
1. **`simplified-packet-detector`** — lightweight edge agent; streams raw packets to Kafka
2. **`jobserver2-main`** — consumes from Kafka, reconstructs flows, calls ML server
3. **`ml_server`** — FastAPI + gRPC server serving an ensemble classifier via REST and gRPC

Supports multiple simultaneous capture devices and cloud-hosted ML inference.

**Key entry points:** `start_capture_device.sh`, `start_job_server.sh`, `ml_server/main.py`

**Config:** `.env.example` — copy to `.env` and fill in `ML_API_URL` and `KAFKA_BOOTSTRAP_SERVERS`

**Models (not in repo):** `model4.pkl` (51 MB, production model)

---

## `healthcare-prediction`

**What it is:** A standalone Streamlit web application for generating synthetic patient datasets and training binary risk classifiers. Configurable data distributions, feature weights, and model evaluation metrics displayed interactively. No external datasets required — all data is generated at runtime.

**Key entry point:** `streamlit run modeltrainer.py`

---

## Datasets and Model Binaries

**Raw datasets and trained model files are intentionally excluded from all repositories.**

They are large binary artifacts that do not belong in version control. On the local development machine they reside in:

```
LTS-Files/raw data collected for project/
├── cybersecurity_intrusion_data.csv    (725 KB)
├── CIC_DDoS2019_To_Use.csv             (54 MB)
├── CIC_DDoS2019_balanced_reduced.csv   (43 MB)
├── cydataset2.csv                      (96 MB)
├── NTD.csv                             (157 KB)
├── optimized_ddos_model.pkl            (37 MB)
├── ddos_detection_model.pkl            (255 MB)
├── improved_ddos_model.pkl             (37 MB)
└── model1.pkl – model4.pkl             (48–59 MB each)
```

The CIC-DDoS2019 dataset is publicly available from the Canadian Institute for Cybersecurity.

---

## Documents

Each repository contains a `documents/` subfolder with **project-specific copies** of relevant reports, presentations, and technical guides. The canonical originals live under `LTS-Files/Documents/` on the local machine.

Key shared documents:
- **PS-1 Final Report** — comprehensive write-up of all work (in `lts-distributed-ddos-detection/documents/reports/`)
- **Software Architecture Documents** — system design (in `lts-monolithic-ddos-detection/documents/reports/`)
- **Deployment guides** — setup instructions for the distributed system (in `lts-distributed-ddos-detection/documents/technical/`)

---

## Environment

All projects use **Python 3.10+**. Individual `requirements.txt` files are present in each repo. Create a virtual environment per repo:

```bash
python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
```

Packet sniffing (`lts-coursework-experiments`, `lts-monolithic-ddos-detection`) requires **root/admin privileges** and either `libpcap` (macOS/Linux) or Npcap (Windows).
