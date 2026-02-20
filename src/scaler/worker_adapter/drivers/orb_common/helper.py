import json
import os
import shutil
import subprocess
import tempfile
from os import path
from typing import Any, Dict, List, Optional

from scaler.utility.formatter import snakecase_dict
from scaler.worker_adapter.drivers.orb_common.exception import ORBException
from scaler.worker_adapter.drivers.orb_common.types import ORBMachine, ORBRequest, ORBTemplate


class ORBHelper:
    """Helper class to interact with the ORB CLI."""

    @staticmethod
    def _filter_data(cls: Any, data: Dict[str, Any]) -> Dict[str, Any]:
        """Filter data to match dataclass fields."""
        if not hasattr(cls, "__annotations__"):
            return data
        valid_keys = cls.__annotations__.keys()
        return {k: v for k, v in data.items() if k in valid_keys}

    class Templates:
        """API for managing compute templates."""

        def __init__(self, helper: "ORBHelper"):
            self.helper = helper

        def _command(self, command: List[str]) -> Any:
            return self.helper._command(["templates", *command])

        def list(self) -> List[ORBTemplate]:
            """List all templates."""
            data = self._command(["list"])
            # Handle case where list command returns a dict with items or a list
            if isinstance(data, list):
                items = data
            else:
                items = data.get("templates") or data.get("items") or []

            # Convert items to snake_case and filter
            result = []
            for item in items:
                snake_item = snakecase_dict(item)
                filtered_item = ORBHelper._filter_data(ORBTemplate, snake_item)
                result.append(ORBTemplate(**filtered_item))
            return result

        def create(self, config_file_path: Optional[str] = None, template_id: Optional[str] = None) -> ORBTemplate:
            """Create a new template from a JSON configuration file."""
            cmd = ["create"]
            if config_file_path:
                cmd.extend(["--file", config_file_path])
            if template_id:
                cmd.extend(["--template-id", template_id])

            data = self._command(cmd)
            # Handle possible nesting in creation response
            if isinstance(data, dict) and "templates" in data and data["templates"]:
                data = data["templates"][0]

            snake_data = snakecase_dict(data)
            return ORBTemplate(**ORBHelper._filter_data(ORBTemplate, snake_data))

        def delete(self, template_id: str) -> Dict[str, Any]:
            """Delete a template by ID."""
            return self._command(["delete", "--force", template_id])

    class Machines:
        """API for managing compute instances."""

        def __init__(self, helper: "ORBHelper"):
            self.helper = helper

        def _command(self, command: List[str]) -> Any:
            return self.helper._command(["machines", *command])

        def list(self) -> List[ORBMachine]:
            """List all machines."""
            data = self._command(["list"])
            if isinstance(data, list):
                items = data
            else:
                items = data.get("machines") or data.get("items") or []

            result = []
            for item in items:
                snake_item = snakecase_dict(item)
                filtered_item = ORBHelper._filter_data(ORBMachine, snake_item)
                result.append(ORBMachine(**filtered_item))
            return result

        def show(self, machine_id: str) -> ORBMachine:
            """Show details for a specific machine."""
            data = self._command(["show", machine_id])
            if isinstance(data, dict) and "machines" in data and data["machines"]:
                data = data["machines"][0]

            snake_data = snakecase_dict(data)
            return ORBMachine(**ORBHelper._filter_data(ORBMachine, snake_data))

        def request(
            self, template_id: str, count: int, wait: bool = False, timeout: Optional[int] = None
        ) -> ORBRequest:
            """Request new machines using a template."""
            cmd = ["request", template_id, str(count)]
            if wait:
                cmd.append("--wait")
            if timeout:
                cmd.extend(["--timeout", str(timeout)])
            data = self._command(cmd)
            # requestMachines usually returns requestId directly at top level or in a message
            # But handle list just in case
            if isinstance(data, dict) and "requests" in data and data["requests"]:
                data = data["requests"][0]

            snake_data = snakecase_dict(data)
            return ORBRequest(**ORBHelper._filter_data(ORBRequest, snake_data))

        def return_machines(self, machine_ids: List[str]) -> ORBRequest:
            """Return (terminate) one or more machines."""
            data = self._command(["return", *machine_ids])
            if isinstance(data, dict) and "requests" in data and data["requests"]:
                data = data["requests"][0]

            snake_data = snakecase_dict(data)
            return ORBRequest(**ORBHelper._filter_data(ORBRequest, snake_data))

    class Requests:
        """API for managing provisioning requests."""

        def __init__(self, helper: "ORBHelper"):
            self.helper = helper

        def _command(self, command: List[str]) -> Any:
            return self.helper._command(["requests", *command])

        def list(self) -> List[ORBRequest]:
            """List all requests."""
            data = self._command(["list"])
            if isinstance(data, list):
                items = data
            else:
                items = data.get("requests") or data.get("items") or []

            result = []
            for item in items:
                snake_item = snakecase_dict(item)
                filtered_item = ORBHelper._filter_data(ORBRequest, snake_item)
                result.append(ORBRequest(**filtered_item))
            return result

        def show(self, request_id: str) -> ORBRequest:
            """Show details for a specific request."""
            data = self._command(["show", request_id])
            if isinstance(data, dict) and "requests" in data and data["requests"]:
                data = data["requests"][0]

            snake_data = snakecase_dict(data)
            return ORBRequest(**ORBHelper._filter_data(ORBRequest, snake_data))

        def cancel(self, request_id: str) -> ORBRequest:
            """Cancel a provisioning request."""
            data = self._command(["cancel", request_id])
            if isinstance(data, dict) and "requests" in data and data["requests"]:
                data = data["requests"][0]

            snake_data = snakecase_dict(data)
            return ORBRequest(**ORBHelper._filter_data(ORBRequest, snake_data))

    def __init__(self, config_root_path: str):
        """Initialize the helper.

        :param config_root_path: The root directory containing ORB configuration to copy to a temp dir.
        """
        self._temp_dir = tempfile.TemporaryDirectory()
        self._cwd = self._temp_dir.name

        config_src = path.join(config_root_path, "config")
        config_dst = path.join(self._cwd, "config")

        shutil.copytree(
            config_src,
            config_dst,
            dirs_exist_ok=True,
            ignore=shutil.ignore_patterns(
                ".git", ".venv", ".mypy_cache", "build*", "dist", "__pycache__", "metrics", "logs"
            ),
        )

        os.makedirs(path.join(self._cwd, "logs"), exist_ok=True)

        self.templates = self.Templates(self)
        self.machines = self.Machines(self)
        self.requests = self.Requests(self)

    @property
    def cwd(self) -> str:
        """Return the working directory (temp dir)."""
        return self._cwd

    def _command(self, command: List[str]) -> Any:
        """Run an ORB CLI command and return the parsed JSON output."""
        # Set environment variables to point to the temp config directory
        env = os.environ.copy()
        config_dir = path.join(self._cwd, "config")
        logs_dir = path.join(self._cwd, "logs")

        env["HF_PROVIDER_CONFDIR"] = config_dir
        env["ORB_CONFIG_DIR"] = config_dir
        env["ORB_LOG_DIR"] = logs_dir
        env["HF_PROVIDER_LOGDIR"] = logs_dir

        cmd = ["orb", *command]

        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True, cwd=self._cwd, env=env)

            if not result.stdout.strip():
                return {}

            data = json.loads(result.stdout)

            if isinstance(data, dict) and "error" in data:
                raise ORBException(data)
            return data
        except subprocess.CalledProcessError as e:
            error_msg = f"Failed to run ORB command: {e}"
            if e.stdout:
                error_msg += f"\nSTDOUT:\n{e.stdout}"
            if e.stderr:
                error_msg += f"\nSTDERR:\n{e.stderr}"
            raise RuntimeError(error_msg) from e
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Failed to parse ORB command output as JSON: {e}\nOutput: {result.stdout}") from e
