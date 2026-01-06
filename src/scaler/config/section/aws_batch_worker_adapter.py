"""Configuration section for AWS Batch Worker Adapter."""

import dataclasses
from typing import Dict, Optional, Tuple

from scaler.config.types.object_storage_server import ObjectStorageConfig
from scaler.config.types.zmq import ZMQConfig


@dataclasses.dataclass
class AWSBatchWorkerAdapterConfig:
    """Configuration for AWS Batch Worker Adapter."""
    
    # Scaler configuration
    scheduler_address: ZMQConfig
    object_storage_address: Optional[ObjectStorageConfig] = None
    
    # AWS Batch configuration
    job_queue: str = ""
    job_definition: str = ""
    aws_region: str = "us-east-1"
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    
    # Job resource configuration
    vcpus: int = 1
    memory: int = 2048  # MB
    max_worker_groups: int = 10
    
    # Worker configuration
    base_concurrency: int = 1
    capabilities: Dict[str, int] = dataclasses.field(default_factory=dict)
    io_threads: int = 1
    task_queue_size: int = 1000
    heartbeat_interval_seconds: int = 2
    death_timeout_seconds: int = 60
    event_loop: str = "builtin"
    
    # Logging configuration
    logging_paths: Tuple[str, ...] = ("/dev/stdout",)
    logging_level: str = "INFO"
    logging_config_file: Optional[str] = None
    
    # Server configuration
    host: str = "0.0.0.0"
    port: int = 8080

    def __post_init__(self):
        """Validate configuration after initialization."""
        # TODO: Add configuration validation
        # Should validate:
        # - AWS credentials are valid
        # - Job queue and definition exist
        # - Resource limits are reasonable
        # - Network configuration is valid
        pass

    def validate_aws_configuration(self) -> bool:
        """
        Validate AWS-specific configuration.
        
        Returns:
            bool: True if configuration is valid
            
        Raises:
            ValueError: If configuration is invalid
        """
        # TODO: Implement AWS configuration validation
        # Should check:
        # - AWS credentials are provided or available via IAM
        # - AWS region is valid
        # - Job queue exists and is active
        # - Job definition exists and is valid
        # - Required permissions are available
        raise NotImplementedError("AWS configuration validation is not yet implemented")

    def validate_resource_limits(self) -> bool:
        """
        Validate resource limit configuration.
        
        Returns:
            bool: True if limits are valid
            
        Raises:
            ValueError: If limits are invalid
        """
        if self.vcpus <= 0:
            raise ValueError(f"vcpus must be positive, got {self.vcpus}")
        
        if self.memory <= 0:
            raise ValueError(f"memory must be positive, got {self.memory}")
        
        if self.max_worker_groups <= 0:
            raise ValueError(f"max_worker_groups must be positive, got {self.max_worker_groups}")
        
        if self.base_concurrency <= 0:
            raise ValueError(f"base_concurrency must be positive, got {self.base_concurrency}")
        
        # TODO: Add more sophisticated validation
        # - Check if resource limits are within AWS Batch limits
        # - Validate memory/vCPU combinations are supported
        # - Check against account limits
        
        return True

    def get_environment_variables(self) -> Dict[str, str]:
        """
        Get environment variables for AWS Batch jobs.
        
        Returns:
            Dict[str, str]: Environment variables to set in batch jobs
        """
        env_vars = {
            "SCALER_SCHEDULER_ADDRESS": self.scheduler_address.to_address(),
            "SCALER_BASE_CONCURRENCY": str(self.base_concurrency),
            "SCALER_TASK_QUEUE_SIZE": str(self.task_queue_size),
            "SCALER_HEARTBEAT_INTERVAL": str(self.heartbeat_interval_seconds),
            "SCALER_DEATH_TIMEOUT": str(self.death_timeout_seconds),
            "SCALER_EVENT_LOOP": self.event_loop,
            "SCALER_IO_THREADS": str(self.io_threads),
            "SCALER_LOGGING_LEVEL": self.logging_level,
        }
        
        if self.object_storage_address:
            env_vars["SCALER_OBJECT_STORAGE_ADDRESS"] = self.object_storage_address.to_string()
        
        if self.capabilities:
            # Convert capabilities dict to string format
            caps_str = ",".join(f"{k}={v}" for k, v in self.capabilities.items())
            env_vars["SCALER_CAPABILITIES"] = caps_str
        
        # AWS configuration
        env_vars["AWS_DEFAULT_REGION"] = self.aws_region
        
        if self.aws_access_key_id:
            env_vars["AWS_ACCESS_KEY_ID"] = self.aws_access_key_id
        
        if self.aws_secret_access_key:
            env_vars["AWS_SECRET_ACCESS_KEY"] = self.aws_secret_access_key
        
        return env_vars