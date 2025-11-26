#!/bin/bash
# Run this script from your local machine to setup everything on EC2

set -e

EC2_HOST="${1:-3.209.188.168}"
SSH_KEY="${2:-/Users/blitvin/.ssh/intellij-dev-key.pem}"

echo "=== Remote EC2 Setup ==="
echo "EC2 Host: $EC2_HOST"
echo "SSH Key: $SSH_KEY"
echo ""

# Create directory structure on EC2 if it doesn't exist
echo "Creating directory structure on EC2..."
ssh -i "$SSH_KEY" ubuntu@$EC2_HOST "mkdir -p ~/IdeaProjects/opengris-scaler"

# Copy devcontainer files to EC2
echo "Copying devcontainer files to EC2..."
scp -i "$SSH_KEY" -r .devcontainer ubuntu@$EC2_HOST:~/IdeaProjects/opengris-scaler/

# Copy scripts directory (needed for Dockerfile)
echo "Copying scripts directory to EC2..."
scp -i "$SSH_KEY" -r scripts ubuntu@$EC2_HOST:~/IdeaProjects/opengris-scaler/

# Run setup script on EC2
echo "Running setup script on EC2..."
ssh -i "$SSH_KEY" ubuntu@$EC2_HOST "cd ~/IdeaProjects/opengris-scaler && REBUILD=yes bash .devcontainer/setup_and_run_remote_dev_docker.sh"

echo ""
echo "=== Setup Complete ==="
echo "Connect to container with IntelliJ IDEA:"
echo "  Host: $EC2_HOST"
echo "  Port: 2222"
echo "  User: vscode"
echo "  Auth: SSH key ($SSH_KEY without .pem extension)"
echo "  Project: /workspace"
