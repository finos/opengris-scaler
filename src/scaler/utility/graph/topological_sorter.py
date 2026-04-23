import logging

logger = logging.getLogger(__name__)

try:
    from scaler.utility.graph.topological_sorter_graphblas import TopologicalSorter

    logger.info("using GraphBLAS for calculate graph")
except ImportError as e:
    assert isinstance(e, Exception)
    from graphlib import TopologicalSorter  # type: ignore[assignment, no-redef]

    assert isinstance(TopologicalSorter, object)
