"""This is a basic example showing the minimal changes needed to start using Scaler for a Ray application."""

import ray

from scaler.cluster.combo import SchedulerClusterCombo

# this patches the ray module
from scaler.compat.ray import scaler_init


# this is an example and we don't have a real remote cluster here
# so for demonstration purposes we just start a local cluster
def start_remote_cluster() -> SchedulerClusterCombo:
    return SchedulerClusterCombo(n_workers=1)


def main(address: str):
    # explicitly init the scaler
    # we explicitly provide the address of the remote scheduler
    scaler_init(address=address)

    @ray.remote
    def my_function():
        return 1

    # this is executed by the remote scaler cluster
    future = my_function.remote()
    assert ray.get(future) == 1


if __name__ == "__main__":
    combo = start_remote_cluster()
    main(combo.get_address())

    combo.shutdown()
