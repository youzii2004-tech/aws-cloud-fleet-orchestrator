#!/bin/bash
# setup_worker.sh — Automated EC2 User-Data Bootstrap Script
# Provisions headless worker instance with Docker, Python 3, SSM Agent, and monitoring daemon.

set -euo pipefail

echo "=========================================="
echo "Starting Cloud Worker Bootstrap Procedure"
echo "=========================================="

# 1. Update OS Packages
apt-get update -y && apt-get upgrade -y
apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    python3 \
    python3-pip \
    python3-venv \
    unzip \
    jq

# 2. Ensure AWS Systems Manager (SSM) Agent is Active
if ! systemctl is-active --quiet amazon-ssm-agent; then
    echo "Installing AWS SSM Agent..."
    snap install amazon-ssm-agent --classic || true
    systemctl enable amazon-ssm-agent
    systemctl restart amazon-ssm-agent
fi

# 3. Install AWS CLI v2
if ! command -v aws &> /dev/null; then
    echo "Installing AWS CLI v2..."
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    unzip -q awscliv2.zip
    ./aws/install
    rm -rf aws awscliv2.zip
fi

# 4. Setup Worker Workspace
WORKER_DIR="/opt/cloud-fleet-worker"
mkdir -p "${WORKER_DIR}"/{logs,artifacts,references}
chmod -R 755 "${WORKER_DIR}"

echo "Worker bootstrap complete. Node ready for SSM task dispatch."
