import json
import subprocess
from os import path
from typing import Any, Dict, List, Optional

from scaler.worker_adapter.orb.exception import ORBException
from scaler.worker_adapter.orb.types import ORBMachine, ORBRequest, ORBTemplate


class ORBHelper:
    """Helper class to interact with the ORB CLI."""

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
            items = data if isinstance(data, list) else data.get("items", [])
            return [ORBTemplate(**item) for item in items]

        def create(self, file_path: str) -> ORBTemplate:
            """Create a new template from a file."""
            data = self._command(["create", "--file", path.abspath(file_path)])
            return ORBTemplate(**data)

        def delete(self, template_id: str) -> Dict[str, Any]:
            """Delete a template by ID."""
            return self._command(["delete", "--force", "--template-id", template_id])

    class Machines:
        """API for managing compute instances."""

        def __init__(self, helper: "ORBHelper"):
            self.helper = helper

        def _command(self, command: List[str]) -> Any:
            return self.helper._command(["machines", *command])

        def list(self) -> List[ORBMachine]:
            """List all machines."""
            data = self._command(["list"])
            items = data if isinstance(data, list) else data.get("items", [])
            return [ORBMachine(**item) for item in items]

        def show(self, machine_id: str) -> ORBMachine:
            """Show details for a specific machine."""
            data = self._command(["show", machine_id])
            return ORBMachine(**data)

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
            return ORBRequest(**data)

        def return_machines(self, machine_ids: List[str]) -> ORBRequest:
            """Return (terminate) one or more machines."""
            data = self._command(["return", *machine_ids])
            return ORBRequest(**data)

    class Requests:
        """API for managing provisioning requests."""

        def __init__(self, helper: "ORBHelper"):
            self.helper = helper

        def _command(self, command: List[str]) -> Any:
            return self.helper._command(["requests", *command])

        def list(self) -> List[ORBRequest]:
            """List all requests."""
            data = self._command(["list"])
            items = data if isinstance(data, list) else data.get("items", [])
            return [ORBRequest(**item) for item in items]

        def show(self, request_id: str) -> ORBRequest:
            """Show details for a specific request."""
            data = self._command(["show", request_id])
            return ORBRequest(**data)

        def cancel(self, request_id: str) -> ORBRequest:
            """Cancel a provisioning request."""
            data = self._command(["cancel", request_id])
            return ORBRequest(**data)

    def __init__(self, config_path: str):
        """Initialize the helper with the path to the ORB config file."""
        self._config_path = path.abspath(config_path)
        self.templates = self.Templates(self)
        self.machines = self.Machines(self)
        self.requests = self.Requests(self)

    def _command(self, command: List[str]) -> Any:
        """Run an ORB CLI command and return the parsed JSON output."""
        try:
            result = subprocess.run(
                ["orb", "--config", self._config_path, *command], check=True, capture_output=True, text=True
            )

            if not result.stdout.strip():
                return {}

            data = json.loads(result.stdout)

            if isinstance(data, dict) and "error" in data:
                raise ORBException(data)
            return data
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to run ORB command: {e}")
