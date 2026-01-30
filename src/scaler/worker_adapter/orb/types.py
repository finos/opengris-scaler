from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class ORBTemplate:
    template_id: str
    name: Optional[str] = None
    description: Optional[str] = None
    instance_type: Optional[str] = None
    image_id: Optional[str] = None
    max_instances: int = 1
    subnet_id: Optional[str] = None
    subnet_ids: List[str] = field(default_factory=list)
    security_group_ids: List[str] = field(default_factory=list)
    price_type: str = "ondemand"
    allocation_strategy: str = "lowest_price"
    max_price: Optional[float] = None
    instance_types: Dict[str, int] = field(default_factory=dict)
    primary_instance_type: Optional[str] = None
    network_zones: List[str] = field(default_factory=list)
    public_ip_assignment: Optional[bool] = None
    root_volume_size: Optional[int] = None
    root_volume_type: Optional[str] = None
    root_volume_iops: Optional[int] = None
    root_volume_throughput: Optional[int] = None
    storage_encryption: Optional[bool] = None
    encryption_key: Optional[str] = None
    key_pair_name: Optional[str] = None
    user_data: Optional[str] = None
    instance_profile: Optional[str] = None
    monitoring_enabled: Optional[bool] = None
    tags: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    provider_type: Optional[str] = None
    provider_name: Optional[str] = None
    provider_api: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    is_active: bool = True
    vm_type: Optional[str] = None
    vm_types: Dict[str, Any] = field(default_factory=dict)
    key_name: Optional[str] = None


@dataclass
class ORBMachine:
    id: str
    instance_id: str
    template_id: str
    request_id: Optional[str] = None
    provider_type: str = ""
    instance_type: str = ""
    image_id: str = ""
    private_ip: Optional[str] = None
    public_ip: Optional[str] = None
    subnet_id: Optional[str] = None
    security_group_ids: List[str] = field(default_factory=list)
    status: str = ""
    status_reason: Optional[str] = None
    launch_time: Optional[datetime] = None
    termination_time: Optional[datetime] = None
    tags: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    provider_data: Dict[str, Any] = field(default_factory=dict)
    version: int = 0
    created_at: Optional[datetime] = None


@dataclass
class ORBRequest:
    id: str
    request_id: str
    request_type: str
    provider_type: str
    template_id: str
    provider_instance: Optional[str] = None
    requested_count: int = 1
    desired_capacity: int = 1
    provider_name: Optional[str] = None
    provider_api: Optional[str] = None
    resource_ids: List[str] = field(default_factory=list)
    status: str = ""
    status_message: Optional[str] = None
    message: Optional[str] = None
    instance_ids: List[str] = field(default_factory=list)
    successful_count: int = 0
    failed_count: int = 0
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    error_details: Dict[str, Any] = field(default_factory=dict)
    provider_data: Dict[str, Any] = field(default_factory=dict)
    version: int = 0
