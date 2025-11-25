#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== OpenGRIS Scaler Dev Container Setup ==="

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Docker not found. Installing Docker..."
    
    # Update system
    sudo apt-get update -y
    
    # Install prerequisites
    sudo apt-get install -y ca-certificates curl gnupg
    
    # Add Docker's official GPG key
    sudo install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    sudo chmod a+r /etc/apt/keyrings/docker.gpg
    
    # Add Docker repository
    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
      $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
      sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    
    # Install Docker
    sudo apt-get update -y
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    
    # Configure Docker to use /home/ubuntu for data storage
    echo "Configuring Docker to use /home/ubuntu/docker for data storage..."
    sudo mkdir -p /home/ubuntu/docker
    echo '{"data-root": "/home/ubuntu/docker"}' | sudo tee /etc/docker/daemon.json > /dev/null
    
    # Configure containerd to use /home/ubuntu for data storage
    echo "Configuring containerd to use /home/ubuntu/containerd for data storage..."
    sudo mkdir -p /home/ubuntu/containerd
    sudo mkdir -p /etc/containerd
    containerd config default | sudo tee /etc/containerd/config.toml > /dev/null
    sudo sed -i 's|/var/lib/containerd|/home/ubuntu/containerd|g' /etc/containerd/config.toml
    
    # Start services
    sudo systemctl start containerd
    sudo systemctl start docker
    sudo systemctl enable docker
    sudo systemctl enable containerd
    
    # Add ubuntu user to docker group
    sudo usermod -aG docker ubuntu
    
    echo "Docker installed successfully!"
else
    echo "Docker already installed: $(docker --version)"
fi

# Ensure Docker is running
if ! sudo systemctl is-active --quiet docker; then
    echo "Starting Docker service..."
    sudo systemctl start docker
fi

# Fix Docker socket permissions
sudo chown root:docker /var/run/docker.sock 2>/dev/null || true
sudo chmod 660 /var/run/docker.sock 2>/dev/null || true

# Check if user is in docker group
if ! groups | grep -q docker; then
    echo "Adding current user to docker group..."
    sudo usermod -aG docker $USER
    echo "WARNING: You need to log out and back in for group changes to take effect."
    echo "Run 'newgrp docker' to activate in current session, then re-run this script."
    exit 1
fi

# Check if image exists and if rebuild is needed
REBUILD="${REBUILD:-auto}"
if ! docker image inspect scaler-dev &> /dev/null; then
    echo "Building scaler-dev image..."
    docker build -f "$SCRIPT_DIR/Dockerfile" -t scaler-dev "$PROJECT_DIR"
elif [ "$REBUILD" = "yes" ] || [ "$REBUILD" = "y" ]; then
    echo "Rebuilding scaler-dev image..."
    docker build -f "$SCRIPT_DIR/Dockerfile" -t scaler-dev "$PROJECT_DIR"
elif [ "$REBUILD" = "auto" ]; then
    echo "scaler-dev image already exists"
    read -p "Rebuild image? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker build -f "$SCRIPT_DIR/Dockerfile" -t scaler-dev "$PROJECT_DIR"
    fi
else
    echo "Using existing scaler-dev image"
fi

# Stop existing container if running
if docker ps -a --format '{{.Names}}' | grep -q '^scaler-dev$'; then
    echo "Stopping existing scaler-dev container..."
    docker stop scaler-dev 2>/dev/null || true
    docker rm scaler-dev 2>/dev/null || true
fi

# Run container with SSH
echo "Starting scaler-dev container with SSH..."
docker run -d \
  --name scaler-dev \
  -p 2222:22 \
  -v "$PROJECT_DIR:/workspace" \
  -w /workspace \
  scaler-dev

echo ""
echo "=== Container Started Successfully ==="
echo "Container name: scaler-dev"
echo "SSH port: 2222"
echo "Project mounted at: /workspace"
echo ""
echo "Connect from local machine:"
echo "  ssh -p 2222 vscode@<EC2_IP>"
echo "  Password: vscode"
echo ""
echo "IntelliJ IDEA Remote Development:"
echo "  1. File → Remote Development → SSH"
echo "  2. Host: <EC2_IP>, Port: 2222, User: vscode"
echo "  3. Password: vscode"
echo "  4. Open project: /workspace"
echo ""
echo "To stop container: docker stop scaler-dev"
echo "To view logs: docker logs scaler-dev"
