import dataclasses
import re


@dataclasses.dataclass
class ObjectStorageConfig:
    host: str
    port: int

    def to_string(self) -> str:
        return f"tcp://{self.host}:{self.port}"

    @staticmethod
    def from_string(address: str) -> "ObjectStorageConfig":
        address_format = r"^tcp://([a-zA-Z0-9\.\-]+):([0-9]{1,5})$"
        match = re.compile(address_format).match(address)

        if not match:
            raise ValueError("object storage address has to be tcp://<host>:<port>")

        host = match.group(1)
        port = int(match.group(2))

        return ObjectStorageConfig(host=host, port=port)
