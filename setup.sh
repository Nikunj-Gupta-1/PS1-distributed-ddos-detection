#!/usr/bin/env bash
# complete-setup.sh – Complete environment setup for your software
# Installs Homebrew, system dependencies, and sets up your application, Kafka, Prometheus, Grafana, and Kafka Exporter

set -euo pipefail

echo "🚀 Starting complete environment setup..."

# Prompt for sudo password at the beginning
sudo -v

# Keep sudo alive throughout the script
while true; do sudo -n true; sleep 60; kill -0 "$$" || exit; done 2>/dev/null &

cd ~/

echo "📦 Updating System..."
sudo apt-get update
sudo apt-get install -y build-essential procps curl file git gnome-terminal wget

# Install Homebrew if missing
export NONINTERACTIVE=1
if ! command -v brew &>/dev/null; then
  echo "🍺 Installing Homebrew..."
  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
fi

# Initialize brew environment for current session
if [[ -x /home/linuxbrew/.linuxbrew/bin/brew ]]; then
  eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"
elif [[ -x /usr/local/bin/brew ]]; then
  eval "$(/usr/local/bin/brew shellenv)"
fi

echo "🔄 Updating brew and installing dependencies..."
brew update
brew install python3 libpcap kafka zookeeper prometheus grafana
brew cleanup

echo "🚀 Starting Zookeeper and Kafka services..."
brew services start zookeeper
sleep 3
brew services start kafka

echo "✅ Kafka and Zookeeper services started"

# Setup Python virtual environment
echo "🐍 Setting up Python virtual environment..."
cd ~/MyApp
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# --- Kafka Exporter setup ---
echo "📊 Setting up Kafka Exporter..."
KAFKA_EXPORTER_VERSION="1.7.0"
cd ~/
if [ ! -f "kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz" ]; then
    wget "https://github.com/danielqsj/kafka_exporter/releases/download/v${KAFKA_EXPORTER_VERSION}/kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz"
fi
tar -xf "kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64.tar.gz"
sudo mv "kafka_exporter-${KAFKA_EXPORTER_VERSION}.linux-amd64/kafka_exporter" /usr/local/bin/
sudo chmod +x /usr/local/bin/kafka_exporter

# --- Prometheus config for Kafka Exporter ---
echo "📝 Configuring Prometheus to scrape Kafka Exporter..."
PROM_FILE="$(brew --prefix)/etc/prometheus.yml"

cat <<EOF | sudo tee $PROM_FILE > /dev/null
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  - job_name: 'kafka'
    static_configs:
      - targets: ['localhost:9308']
    scrape_interval: 5s
EOF

echo "🔄 Restarting Prometheus service..."
brew services restart prometheus

# --- Grafana service ---
echo "🔄 Starting Grafana service..."
brew services start grafana

# --- Kafka Exporter as a systemd service ---
echo "🛠️  Creating systemd service for Kafka Exporter..."
cat <<'EOF' | sudo tee /etc/systemd/system/kafka_exporter.service > /dev/null
[Unit]
Description=Kafka Exporter
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/kafka_exporter --kafka.server=localhost:9092 --log.level=info --topic.whiteList=".*"
Restart=on-failure
User='"$(whoami)"'

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable kafka_exporter
sudo systemctl start kafka_exporter

echo "✅ Prometheus, Grafana, and Kafka Exporter configured and started"

# --- Your app run script, as before ---
echo "📝 Creating run script..."
cat > run.sh << 'EOS'
#!/bin/bash
# run.sh - Script to start your application

sudo -v  # Prompt for sudo password at start
sleep 2

echo "🔍 Starting packet detector..."
cd ~/MyApp/packet-detector
source ../venv/bin/activate
sudo ../venv/bin/python3 main.py &

sleep 2

echo "🖥️ Starting Kafka Listener for Attack flows"

gnome-terminal -- bash -c '
    cd ~/MyApp/packet-detector
    source ../venv/bin/activate
    ../venv/bin/python3 src/streaming/kafka_consumer.py
' &

sleep 2 

echo "🖥️ Starting job server in new terminal..."
gnome-terminal -- bash -c '
    echo "Starting Job Server..."
    cd ~/MyApp/jobserver
    source ../venv/bin/activate
    ../venv/bin/python3 main.py
    echo "Job server finished. Press Enter to close..."
    read
'

echo "✅ Both services started successfully!"
echo "📝 Packet detector is running in background"
echo "📝 Job server is running in the new terminal window"

wait
EOS

chmod +x run.sh

# --- Brew shell profile ---
echo "🔧 Adding Homebrew to shell profile..."
SHELL_PROFILE=""
if [[ -f ~/.bashrc ]]; then
    SHELL_PROFILE=~/.bashrc
elif [[ -f ~/.zshrc ]]; then
    SHELL_PROFILE=~/.zshrc
elif [[ -f ~/.profile ]]; then
    SHELL_PROFILE=~/.profile
fi

if [[ -n "$SHELL_PROFILE" ]]; then
    if ! grep -q 'linuxbrew' "$SHELL_PROFILE"; then
        echo '' >> "$SHELL_PROFILE"
        echo '# Homebrew' >> "$SHELL_PROFILE"
        echo 'eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"' >> "$SHELL_PROFILE"
    fi
fi

echo "✅ Environment setup complete!"
echo ""
echo "📋 Summary:"
echo "  • System packages installed"
echo "  • Homebrew installed and configured"
echo "  • Python virtual environment created"
echo "  • Kafka and Zookeeper services started"
echo "  • Kafka Exporter, Prometheus, Grafana installed and started"
echo "  • Run script created at ~/MyApp/run.sh"
echo ""
echo "🎯 To run your software:"
echo "  cd ~/MyApp && ./run.sh"
echo ""
echo "🔄 To restart services if needed:"
echo "  brew services restart zookeeper"
echo "  brew services restart kafka"
echo "  brew services restart prometheus"
echo "  brew services restart grafana"
echo "  sudo systemctl restart kafka_exporter"
echo ""
echo "📊 For monitoring:"
echo " • Prometheus: http://localhost:9090"
echo " • Grafana:    http://localhost:3000 (default login: admin/admin)"
echo ""

