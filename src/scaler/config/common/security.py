import dataclasses
from typing import Optional

from scaler.config.config_class import ConfigClass


@dataclasses.dataclass
class SecurityConfig(ConfigClass):
    """TLS credentials used to secure Scaler's network sockets.

    Holds filesystem paths to a PEM certificate chain and its matching private key. Both are used by
    the binding side of a secure (e.g. ``tls://``) socket. Connecting sockets do not require
    credentials and may use a secure socket without them.
    """

    tls_cert: Optional[str] = dataclasses.field(
        default=None,
        metadata=dict(
            long="--tls-cert",
            short="-tc",
            help="path to the PEM certificate chain file used for secure (tls://) sockets",
        ),
    )
    tls_key: Optional[str] = dataclasses.field(
        default=None,
        metadata=dict(
            long="--tls-key", short="-tk", help="path to the PEM private key file matching the certificate chain"
        ),
    )

    def __post_init__(self):
        if (self.tls_cert is None) != (self.tls_key is None):
            raise ValueError("tls_cert and tls_key must be provided together")

    def has_credentials(self) -> bool:
        return self.tls_cert is not None and self.tls_key is not None
