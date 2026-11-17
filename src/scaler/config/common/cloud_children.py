import dataclasses
from typing import Optional

from scaler.config.config_class import ConfigClass
from scaler.config.types.address import AddressConfig


@dataclasses.dataclass
class CloudChildrenConfig(ConfigClass):
    advertised_children_address: Optional[AddressConfig] = dataclasses.field(
        default=None,
        metadata=dict(
            short="-aca",
            help=(
                "address written into the units' start command for them to dial; defaults to children_address. "
                "Use this when the units reach this worker manager through NAT, e.g. an EC2 public IP."
            ),
        ),
    )
    advertised_otlp_endpoint: Optional[str] = dataclasses.field(
        default=None,
        metadata=dict(
            short="-aoe",
            help=(
                "OTLP/HTTP endpoint the units export telemetry to, in place of this worker manager's "
                "OTEL_EXPORTER_OTLP_ENDPOINT. Use this when the units reach the collector at another address, "
                "e.g. through NAT."
            ),
        ),
    )
