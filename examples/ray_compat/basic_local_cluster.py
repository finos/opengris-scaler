"""This is a basic example showing the minimal changes needed to start using Scaler for a Ray application."""

import ray

# this patches the ray module
import scaler.compat.ray  # noqa: F401


def main():
    @ray.remote
    def my_function():
        return 1

    # Scaler is implicitly initialized on the first .remote() call using default settings.
    # See basic_remote_cluster.py for explicit initialization with custom settings.
    future = my_function.remote()
    assert ray.get(future) == 1

    # the implicitly-created cluster is globally-scoped
    # so we need to shut it down explicitly
    ray.shutdown()


if __name__ == "__main__":
    main()
