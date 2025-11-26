#!/bin/bash
# Launch a new dev instance from the launch template and attach EIP

set -e

LAUNCH_TEMPLATE_NAME="intellij-remote-dev"
EIP_ALLOCATION_ID="eipalloc-0b4441087cbd2407f"
AWS_REGION="us-east-1"

# AWS credentials


echo "=== Launching Dev Instance ==="

# Launch instance from template
echo "Launching instance from template: $LAUNCH_TEMPLATE_NAME..."
INSTANCE_ID=$(aws ec2 run-instances \
  --launch-template LaunchTemplateName=$LAUNCH_TEMPLATE_NAME \
  --query 'Instances[0].InstanceId' \
  --output text)

echo "Instance launched: $INSTANCE_ID"

# Wait for instance to be running
echo "Waiting for instance to be running..."
aws ec2 wait instance-running --instance-ids $INSTANCE_ID

# Associate EIP
echo "Associating EIP: $EIP_ALLOCATION_ID..."
aws ec2 associate-address \
  --instance-id $INSTANCE_ID \
  --allocation-id $EIP_ALLOCATION_ID

# Get EIP address
EIP_ADDRESS=$(aws ec2 describe-addresses \
  --allocation-ids $EIP_ALLOCATION_ID \
  --query 'Addresses[0].PublicIp' \
  --output text)

echo ""
echo "=== Instance Ready ==="
echo "Instance ID: $INSTANCE_ID"
echo "Public IP: $EIP_ADDRESS"
echo ""
echo "Wait ~30 seconds for SSH to be ready, then run:"
echo "  bash .devcontainer/remote_setup.sh $EIP_ADDRESS"
