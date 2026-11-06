from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.types.address import AddressConfig, SocketType
from scaler.utility.network_util import OS_PICKED_PORT


def local_children_address(worker_manager_config: WorkerManagerConfig) -> AddressConfig:
    """The address a manager binds for its local worker processes: the configured one, else an OS-picked loopback port.

    TCP rather than a Unix-domain socket, because Windows runs these managers too.
    """
    if worker_manager_config.children_address is not None:
        return worker_manager_config.children_address
    return AddressConfig(SocketType.tcp, "127.0.0.1", OS_PICKED_PORT)
