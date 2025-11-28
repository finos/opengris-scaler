from scaler.cluster.cluster import Cluster
from scaler.config.section.cluster import ClusterConfig
from scaler.utility.event_loop import register_event_loop


def main():
    cluster_config = ClusterConfig.parse("Scaler Standalone Compute Cluster", "cluster")
    register_event_loop(cluster_config.event_loop)
    cluster = Cluster(cluster_config)
    cluster.run()
