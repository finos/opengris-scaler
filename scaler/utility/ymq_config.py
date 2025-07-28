from dataclasses import dataclass

@dataclass
class YMQConfig:
    host: str
    port: int

    def to_address(self) -> str:
        return f"tcp://{self.host}:{self.port}"

    @staticmethod
    def from_string(address: str) -> 'YMQConfig':
        address = address.removeprefix("tcp://")

        try:
            host, port = address.split(':', 1)
        except ValueError as exc:
            raise TypeError(f"Invalid address format: {address}. Expected format is 'host:port'.") from exc

        return YMQConfig(host=host, port=int(port))
