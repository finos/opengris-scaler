import argparse
import sys
from typing import Any, Dict, Type, Union, cast

from scaler.config.config_class import ConfigClass
from scaler.config.loading import _load_toml
from scaler.config.section.aws_hpc_worker_manager import AWSBatchWorkerManagerConfig
from scaler.config.section.ecs_worker_manager import ECSWorkerManagerConfig
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.section.symphony_worker_manager import SymphonyWorkerManagerConfig
from scaler.utility.event_loop import register_event_loop
from scaler.utility.logging.utility import setup_logger

_AnyWorkerManagerConfig = Union[
    NativeWorkerManagerConfig, SymphonyWorkerManagerConfig, ECSWorkerManagerConfig, AWSBatchWorkerManagerConfig
]

_TYPE_MAP: Dict[str, Type[ConfigClass]] = {
    NativeWorkerManagerConfig._tag: NativeWorkerManagerConfig,
    SymphonyWorkerManagerConfig._tag: SymphonyWorkerManagerConfig,
    ECSWorkerManagerConfig._tag: ECSWorkerManagerConfig,
    AWSBatchWorkerManagerConfig._tag: AWSBatchWorkerManagerConfig,
}


def main() -> None:
    # Pass 1: extract subcommand and --config before building the type-specific parser.
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--config", "-c")
    pre_parser.add_argument("subcommand")
    pre_args, remaining_argv = pre_parser.parse_known_args()

    wm_type = pre_args.subcommand
    if wm_type not in _TYPE_MAP:
        valid = ", ".join(_TYPE_MAP)
        print(f"scaler_worker_manager: unknown subcommand '{wm_type}', must be one of: {valid}", file=sys.stderr)
        sys.exit(1)

    # Load TOML and find the matching [[worker_manager]] entry.
    section_data: Dict[str, Any] = {}
    if pre_args.config:
        toml_data = _load_toml(pre_args.config)
        entries = toml_data.get("worker_manager", [])
        if isinstance(entries, dict):
            entries = [entries]
        matching = [e for e in entries if e.get("type") == wm_type]
        if not matching:
            print(f"scaler_worker_manager: no worker manager of type '{wm_type}' found in config", file=sys.stderr)
            sys.exit(1)
        if len(matching) > 1:
            print(
                f"scaler_worker_manager: {len(matching)} worker managers of type '{wm_type}' found in config,"
                " expected exactly one",
                file=sys.stderr,
            )
            sys.exit(1)
        section_data = matching[0]

    # Pass 2: parse the type-specific config using TOML defaults + remaining CLI args.
    config_cls = _TYPE_MAP[wm_type]
    wm_config = cast(
        _AnyWorkerManagerConfig,
        config_cls.parse_with_section("scaler_worker_manager", section_data, argv=remaining_argv),
    )

    setup_logger(wm_config.logging_config.paths, wm_config.logging_config.config_file, wm_config.logging_config.level)
    register_event_loop(wm_config.worker_config.event_loop)

    if isinstance(wm_config, NativeWorkerManagerConfig):
        from scaler.worker_manager_adapter.baremetal.native import NativeWorkerManager

        NativeWorkerManager(wm_config).run()
    elif isinstance(wm_config, SymphonyWorkerManagerConfig):
        from scaler.worker_manager_adapter.symphony.worker_manager import SymphonyWorkerManager

        SymphonyWorkerManager(wm_config).run()
    elif isinstance(wm_config, ECSWorkerManagerConfig):
        from scaler.worker_manager_adapter.aws_raw.ecs import ECSWorkerManager

        ECSWorkerManager(wm_config).run()
    elif isinstance(wm_config, AWSBatchWorkerManagerConfig):
        from scaler.worker_manager_adapter.aws_hpc.worker_manager import AWSHPCWorkerManager

        AWSHPCWorkerManager(wm_config).run()


if __name__ == "__main__":
    main()
