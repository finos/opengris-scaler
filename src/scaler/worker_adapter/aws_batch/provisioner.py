"""
AWS Batch and S3 Provisioner.

Simple provisioning for AWS Batch compute environment, job queue,
job definition, and S3 bucket required for the Scaler AWS Batch adapter.
"""

import json
import logging
import os
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError

DEFAULT_PREFIX = "scaler-batch"
DEFAULT_CONFIG_FILE = ".scaler_aws_batch_config.json"


class AWSBatchProvisioner:
    """
    Provisions AWS resources for Scaler AWS Batch adapter.
    
    Creates:
        - S3 bucket for task payloads and results
        - IAM role for Batch jobs (with S3 access)
        - Batch compute environment (Fargate)
        - Batch job queue
        - Batch job definition
    """

    def __init__(
        self,
        aws_region: str = "us-east-1",
        prefix: str = DEFAULT_PREFIX,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
    ):
        self._region = aws_region
        self._prefix = prefix
        
        session_kwargs = {"region_name": aws_region}
        if aws_access_key_id and aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = aws_access_key_id
            session_kwargs["aws_secret_access_key"] = aws_secret_access_key
        
        self._session = boto3.Session(**session_kwargs)
        self._s3 = self._session.client("s3")
        self._batch = self._session.client("batch")
        self._iam = self._session.client("iam")
        self._sts = self._session.client("sts")
        
        self._account_id = self._sts.get_caller_identity()["Account"]

    def provision_all(
        self,
        container_image: str,
        vcpus: float = 1.0,
        memory_mb: int = 2048,
        max_vcpus: int = 256,
    ) -> dict:
        """
        Provision all required AWS resources.
        
        Args:
            container_image: Docker image for job definition (e.g., python:3.11-slim)
            vcpus: vCPUs per job (Fargate: 0.25, 0.5, 1, 2, 4)
            memory_mb: Memory per job in MB
            max_vcpus: Max vCPUs for compute environment
            
        Returns:
            dict with resource names/ARNs
        """
        logging.info(f"Provisioning AWS Batch resources with prefix '{self._prefix}'...")
        
        # 1. S3 bucket
        bucket_name = self.provision_s3_bucket()
        
        # 2. IAM role
        role_arn = self.provision_iam_role(bucket_name)
        
        # 3. Compute environment
        compute_env_arn = self.provision_compute_environment(max_vcpus)
        
        # 4. Job queue
        job_queue_arn = self.provision_job_queue(compute_env_arn)
        
        # 5. Job definition
        job_def_arn = self.provision_job_definition(
            container_image=container_image,
            role_arn=role_arn,
            vcpus=vcpus,
            memory_mb=memory_mb,
        )
        
        result = {
            "aws_region": self._region,
            "aws_account_id": self._account_id,
            "prefix": self._prefix,
            "s3_bucket": bucket_name,
            "s3_prefix": "scaler-tasks",
            "iam_role_arn": role_arn,
            "compute_environment_arn": compute_env_arn,
            "job_queue_arn": job_queue_arn,
            "job_queue_name": f"{self._prefix}-queue",
            "job_definition_arn": job_def_arn,
            "job_definition_name": f"{self._prefix}-job",
            "container_image": container_image,
            "vcpus": vcpus,
            "memory_mb": memory_mb,
        }
        
        logging.info("Provisioning complete!")
        return result

    @staticmethod
    def save_config(config: dict, config_file: str = DEFAULT_CONFIG_FILE):
        """Save provisioned config to file."""
        path = Path(config_file)
        with open(path, "w") as f:
            json.dump(config, f, indent=2)
        logging.info(f"Config saved to {path.absolute()}")

    @staticmethod
    def load_config(config_file: str = DEFAULT_CONFIG_FILE) -> dict:
        """Load provisioned config from file."""
        path = Path(config_file)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path.absolute()}")
        with open(path, "r") as f:
            return json.load(f)

    @staticmethod
    def print_export_commands(config: dict):
        """Print shell export commands for config values."""
        print(f"export SCALER_AWS_REGION=\"{config['aws_region']}\"")
        print(f"export SCALER_S3_BUCKET=\"{config['s3_bucket']}\"")
        print(f"export SCALER_JOB_QUEUE=\"{config['job_queue_name']}\"")
        print(f"export SCALER_JOB_DEFINITION=\"{config['job_definition_name']}\"")

    def provision_s3_bucket(self) -> str:
        """Create S3 bucket for task data."""
        bucket_name = f"{self._prefix}-{self._account_id}-{self._region}"
        
        try:
            if self._region == "us-east-1":
                self._s3.create_bucket(Bucket=bucket_name)
            else:
                self._s3.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": self._region}
                )
            logging.info(f"Created S3 bucket: {bucket_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
                logging.info(f"S3 bucket already exists: {bucket_name}")
            else:
                raise
        
        # Enable lifecycle to clean up old objects
        self._s3.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration={
                "Rules": [{
                    "ID": "cleanup-old-tasks",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "scaler-tasks/"},
                    "Expiration": {"Days": 1},
                }]
            }
        )
        
        return bucket_name

    def provision_iam_role(self, bucket_name: str) -> str:
        """Create IAM role for Batch jobs with S3 access."""
        role_name = f"{self._prefix}-job-role"
        
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "ecs-tasks.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }
        
        s3_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                "Resource": f"arn:aws:s3:::{bucket_name}/scaler-tasks/*"
            }]
        }
        
        try:
            response = self._iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description="IAM role for Scaler AWS Batch jobs",
            )
            role_arn = response["Role"]["Arn"]
            logging.info(f"Created IAM role: {role_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityAlreadyExists":
                role_arn = f"arn:aws:iam::{self._account_id}:role/{role_name}"
                logging.info(f"IAM role already exists: {role_name}")
            else:
                raise
        
        # Attach S3 policy
        policy_name = f"{self._prefix}-s3-policy"
        try:
            self._iam.put_role_policy(
                RoleName=role_name,
                PolicyName=policy_name,
                PolicyDocument=json.dumps(s3_policy),
            )
        except ClientError:
            pass  # Policy may already exist
        
        return role_arn

    def provision_compute_environment(self, max_vcpus: int = 256) -> str:
        """Create Fargate compute environment."""
        env_name = f"{self._prefix}-compute"
        
        try:
            response = self._batch.create_compute_environment(
                computeEnvironmentName=env_name,
                type="MANAGED",
                state="ENABLED",
                computeResources={
                    "type": "FARGATE",
                    "maxvCpus": max_vcpus,
                    "subnets": self._get_default_subnets(),
                    "securityGroupIds": self._get_default_security_group(),
                },
            )
            env_arn = response["computeEnvironmentArn"]
            logging.info(f"Created compute environment: {env_name}")
            
            # Wait for compute environment to be valid
            self._wait_for_compute_environment(env_name)
            
        except ClientError as e:
            if "already exists" in str(e):
                env_arn = f"arn:aws:batch:{self._region}:{self._account_id}:compute-environment/{env_name}"
                logging.info(f"Compute environment already exists: {env_name}")
            else:
                raise
        
        return env_arn

    def provision_job_queue(self, compute_env_arn: str) -> str:
        """Create job queue."""
        queue_name = f"{self._prefix}-queue"
        
        try:
            response = self._batch.create_job_queue(
                jobQueueName=queue_name,
                state="ENABLED",
                priority=1,
                computeEnvironmentOrder=[{
                    "order": 1,
                    "computeEnvironment": compute_env_arn,
                }],
            )
            queue_arn = response["jobQueueArn"]
            logging.info(f"Created job queue: {queue_name}")
        except ClientError as e:
            if "already exists" in str(e):
                queue_arn = f"arn:aws:batch:{self._region}:{self._account_id}:job-queue/{queue_name}"
                logging.info(f"Job queue already exists: {queue_name}")
            else:
                raise
        
        return queue_arn

    def provision_job_definition(
        self,
        container_image: str,
        role_arn: str,
        vcpus: float = 1.0,
        memory_mb: int = 2048,
    ) -> str:
        """Create job definition."""
        job_def_name = f"{self._prefix}-job"
        
        response = self._batch.register_job_definition(
            jobDefinitionName=job_def_name,
            type="container",
            platformCapabilities=["FARGATE"],
            containerProperties={
                "image": container_image,
                "command": ["python", "-m", "scaler.worker_adapter.aws_batch.batch_job_runner"],
                "jobRoleArn": role_arn,
                "executionRoleArn": role_arn,
                "resourceRequirements": [
                    {"type": "VCPU", "value": str(vcpus)},
                    {"type": "MEMORY", "value": str(memory_mb)},
                ],
                "networkConfiguration": {
                    "assignPublicIp": "ENABLED",
                },
            },
        )
        
        job_def_arn = response["jobDefinitionArn"]
        logging.info(f"Registered job definition: {job_def_name}")
        return job_def_arn

    def _get_default_subnets(self) -> list:
        """Get default VPC subnets."""
        ec2 = self._session.client("ec2")
        response = ec2.describe_subnets(
            Filters=[{"Name": "default-for-az", "Values": ["true"]}]
        )
        return [s["SubnetId"] for s in response["Subnets"]]

    def _get_default_security_group(self) -> list:
        """Get default security group."""
        ec2 = self._session.client("ec2")
        response = ec2.describe_security_groups(
            Filters=[{"Name": "group-name", "Values": ["default"]}]
        )
        return [response["SecurityGroups"][0]["GroupId"]]

    def _wait_for_compute_environment(self, env_name: str, timeout: int = 120):
        """Wait for compute environment to become VALID."""
        import time
        start = time.time()
        while time.time() - start < timeout:
            response = self._batch.describe_compute_environments(
                computeEnvironments=[env_name]
            )
            status = response["computeEnvironments"][0]["status"]
            if status == "VALID":
                return
            if status == "INVALID":
                raise RuntimeError(f"Compute environment {env_name} is INVALID")
            time.sleep(5)
        raise TimeoutError(f"Compute environment {env_name} did not become VALID")

    def cleanup(self):
        """Delete all provisioned resources."""
        logging.info("Cleaning up AWS resources...")
        
        queue_name = f"{self._prefix}-queue"
        env_name = f"{self._prefix}-compute"
        job_def_name = f"{self._prefix}-job"
        role_name = f"{self._prefix}-job-role"
        bucket_name = f"{self._prefix}-{self._account_id}-{self._region}"
        
        # Disable and delete job queue
        try:
            self._batch.update_job_queue(jobQueue=queue_name, state="DISABLED")
            self._batch.delete_job_queue(jobQueue=queue_name)
            logging.info(f"Deleted job queue: {queue_name}")
        except ClientError:
            pass
        
        # Disable and delete compute environment
        try:
            self._batch.update_compute_environment(computeEnvironment=env_name, state="DISABLED")
            self._batch.delete_compute_environment(computeEnvironment=env_name)
            logging.info(f"Deleted compute environment: {env_name}")
        except ClientError:
            pass
        
        # Deregister job definitions
        try:
            response = self._batch.describe_job_definitions(jobDefinitionName=job_def_name, status="ACTIVE")
            for job_def in response.get("jobDefinitions", []):
                self._batch.deregister_job_definition(jobDefinition=job_def["jobDefinitionArn"])
            logging.info(f"Deregistered job definitions: {job_def_name}")
        except ClientError:
            pass
        
        # Delete IAM role
        try:
            self._iam.delete_role_policy(RoleName=role_name, PolicyName=f"{self._prefix}-s3-policy")
            self._iam.delete_role(RoleName=role_name)
            logging.info(f"Deleted IAM role: {role_name}")
        except ClientError:
            pass
        
        # Delete S3 bucket (must be empty)
        try:
            paginator = self._s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket_name):
                for obj in page.get("Contents", []):
                    self._s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
            self._s3.delete_bucket(Bucket=bucket_name)
            logging.info(f"Deleted S3 bucket: {bucket_name}")
        except ClientError:
            pass
        
        logging.info("Cleanup complete!")


def main():
    """CLI for provisioning."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Provision AWS Batch resources for Scaler")
    parser.add_argument("action", choices=["provision", "cleanup", "show"], help="Action to perform")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument("--prefix", default=DEFAULT_PREFIX, help="Resource name prefix")
    parser.add_argument("--image", default="python:3.11-slim", help="Container image")
    parser.add_argument("--vcpus", type=float, default=1.0, help="vCPUs per job")
    parser.add_argument("--memory", type=int, default=2048, help="Memory per job (MB)")
    parser.add_argument("--max-vcpus", type=int, default=256, help="Max vCPUs for compute env")
    parser.add_argument("--config", default=DEFAULT_CONFIG_FILE, help="Config file path")
    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    
    if args.action == "show":
        try:
            config = AWSBatchProvisioner.load_config(args.config)
            print("\n=== Saved AWS Batch Config ===")
            for key, value in config.items():
                print(f"  {key}: {value}")
            print("\n=== Export Commands ===")
            AWSBatchProvisioner.print_export_commands(config)
        except FileNotFoundError as e:
            print(f"Error: {e}")
            print("Run 'provision' first to create resources.")
        return
    
    provisioner = AWSBatchProvisioner(aws_region=args.region, prefix=args.prefix)
    
    if args.action == "provision":
        result = provisioner.provision_all(
            container_image=args.image,
            vcpus=args.vcpus,
            memory_mb=args.memory,
            max_vcpus=args.max_vcpus,
        )
        AWSBatchProvisioner.save_config(result, args.config)
        print("\n=== Provisioned Resources ===")
        for key, value in result.items():
            print(f"  {key}: {value}")
        print("\n=== Export Commands (copy and run) ===")
        AWSBatchProvisioner.print_export_commands(result)
    else:
        provisioner.cleanup()


if __name__ == "__main__":
    main()
