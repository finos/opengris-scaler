#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== OpenGRIS Scaler Dev Container Setup ==="

# Check if we're already inside a container
if [ -f /.dockerenv ] || grep -q 'docker\|lxc' /proc/1/cgroup 2>/dev/null; then
    echo "Already running inside a container. Skipping Docker setup."
    echo "Project directory: $PROJECT_DIR"
    echo "Development environment ready!"
    exit 0
fi

# Check if we can use sudo
if ! sudo -n true 2>/dev/null; then
    echo "ERROR: This script requires sudo privileges."
    echo "Please run with a user that has sudo access or run 'sudo -v' first."
    exit 1
fi

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
if [ -S /var/run/docker.sock ]; then
    sudo chown root:docker /var/run/docker.sock 2>/dev/null || true
    sudo chmod 660 /var/run/docker.sock 2>/dev/null || true
fi

# Check if user is in docker group
if ! groups | grep -q docker; then
    echo "Adding current user to docker group..."
    if sudo usermod -aG docker $USER; then
        echo "User added to docker group successfully."
        echo "WARNING: You need to log out and back in for group changes to take effect."
        echo "Run 'newgrp docker' to activate in current session, then re-run this script."
        exit 1
    else
        echo "ERROR: Failed to add user to docker group"
        exit 1
    fi
fi

# Test Docker access
if ! docker ps >/dev/null 2>&1; then
    echo "ERROR: Cannot access Docker. Please check Docker installation and permissions."
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
  scaler-dev \
  tail -f /dev/null

# Wait for container to be ready
sleep 2

# Start SSH service
echo "Starting SSH service..."
docker exec scaler-dev service ssh start

# Setup SSH key for vscode user
echo "Setting up SSH key for vscode user..."
if [ -f "$HOME/.ssh/authorized_keys" ]; then
    echo "Copying SSH keys from ubuntu user's authorized_keys..."
    docker exec scaler-dev bash -c "mkdir -p /home/vscode/.ssh && chmod 700 /home/vscode/.ssh"
    docker cp "$HOME/.ssh/authorized_keys" scaler-dev:/home/vscode/.ssh/authorized_keys
    docker exec scaler-dev bash -c "chown -R vscode:vscode /home/vscode/.ssh && chmod 600 /home/vscode/.ssh/authorized_keys"
    echo "SSH keys installed successfully"
else
    echo "WARNING: No SSH keys found at $HOME/.ssh/authorized_keys"
    echo "Container SSH may not work without keys"
fi

# Setup AWS credentials for CodeCommit
echo "Setting up AWS credentials..."
if [ -f "$HOME/.aws/credentials" ]; then
    docker exec scaler-dev bash -c "mkdir -p /home/vscode/.aws"
    docker cp "$HOME/.aws/credentials" scaler-dev:/home/vscode/.aws/credentials
    docker cp "$HOME/.aws/config" scaler-dev:/home/vscode/.aws/config 2>/dev/null || true
    docker exec scaler-dev bash -c "chown -R vscode:vscode /home/vscode/.aws"
    echo "AWS credentials installed"
fi

# Configure git to use AWS CodeCommit credential helper
echo "Configuring git for CodeCommit..."
docker exec -u vscode scaler-dev bash -c "git config --global credential.helper '!aws codecommit credential-helper \$@'"
docker exec -u vscode scaler-dev bash -c "git config --global credential.UseHttpPath true"
echo "Git configured for CodeCommit"

# Verify SSH is working
echo "Verifying SSH connectivity..."
if docker exec scaler-dev ps aux | grep -q "[s]shd"; then
    echo "SSH service is running"
else
    echo "WARNING: SSH service may not be running properly"
fi

echo ""
echo "=== Container Started Successfully ==="
echo "Container name: scaler-dev"
echo "SSH port: 2222"
echo "Project mounted at: /workspace"
echo "Authentication: SSH key only (password disabled)"
echo ""
echo "Connect from local machine:"
echo "  ssh -p 2222 -i ~/.ssh/intellij-dev-key vscode@<EC2_IP>"
echo ""
echo "Clone CodeCommit repo:"
echo "  git clone https://git-codecommit.us-east-1.amazonaws.com/v1/repos/opengris-scaler"
echo ""
echo "IntelliJ IDEA Remote Development:"
echo "  1. File → Remote Development → SSH"
echo "  2. Host: <EC2_IP>, Port: 2222, User: vscode"
echo "  3. Auth: SSH key (intellij-dev-key)"
echo "  4. Open project: /workspace"
echo ""
echo "To stop container: docker stop scaler-dev"
echo "To view logs: docker logs scaler-dev"
