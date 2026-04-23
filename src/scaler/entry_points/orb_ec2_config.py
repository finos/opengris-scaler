import argparse
import subprocess
import sys
from pathlib import Path


def _ec2_metadata(flag: str) -> str:
    try:
        result = subprocess.run(["ec2-metadata", flag], capture_output=True, text=True, check=True)
    except FileNotFoundError:
        print("error: ec2-metadata not found — this command must be run on an EC2 instance.", file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as exc:
        print(f"error: ec2-metadata {flag} failed: {exc.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    # Output format: "<key>: <value>"
    return result.stdout.split(":", 1)[1].strip()


def _build_config(
    public_ip: str,
    private_ip: str,
    scheduler_port: int,
    oss_port: int,
    gui_port: int,
    worker_manager_id: str,
    instance_type: str,
    aws_region: str,
    logging_level: str,
    image_id: str,
    python_version: str,
) -> str:
    if image_id:
        worker_image_lines = f'image_id = "{image_id}"'
    else:
        worker_image_lines = f'python_version = "{python_version}"\n' 'requirements_txt = """\nopengris-scaler\n"""'

    monitor_port = scheduler_port + 2

    return (
        f"[object_storage_server]\n"
        f'bind_address = "tcp://0.0.0.0:{oss_port}"\n'
        f"\n"
        f"[scheduler]\n"
        f'bind_address = "tcp://0.0.0.0:{scheduler_port}"\n'
        f'object_storage_address = "tcp://127.0.0.1:{oss_port}"\n'
        f'advertised_object_storage_address = "tcp://{public_ip}:{oss_port}"\n'
        f"\n"
        f"[[worker_manager]]\n"
        f'type = "orb_aws_ec2"\n'
        f'scheduler_address = "tcp://127.0.0.1:{scheduler_port}"\n'
        f'worker_manager_id = "{worker_manager_id}"\n'
        f'worker_scheduler_address = "tcp://{private_ip}:{scheduler_port}"\n'
        f'object_storage_address = "tcp://{private_ip}:{oss_port}"\n'
        f'instance_type = "{instance_type}"\n'
        f'aws_region = "{aws_region}"\n'
        f'logging_level = "{logging_level}"\n'
        f"{worker_image_lines}\n"
        f"\n"
        f"[gui]\n"
        f'monitor_address = "tcp://127.0.0.1:{monitor_port}"\n'
        f'gui_address = "0.0.0.0:{gui_port}"\n'
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a Scaler config.toml for ORB AWS EC2 deployment using ec2-metadata.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--output", default="config.toml", metavar="PATH", help="output file path")
    parser.add_argument("--scheduler-port", type=int, default=6788, metavar="PORT")
    parser.add_argument("--object-storage-port", type=int, default=6789, metavar="PORT")
    parser.add_argument("--worker-manager-id", default="wm-orb", metavar="ID")
    parser.add_argument("--instance-type", default="t3.medium", metavar="TYPE", help="EC2 instance type for workers")
    parser.add_argument("--aws-region", default="us-east-1", metavar="REGION")
    parser.add_argument("--logging-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    parser.add_argument(
        "--image-id", default="", metavar="AMI_ID", help="pre-built AMI for workers (skips Python install)"
    )
    parser.add_argument(
        "--python-version",
        default="3.14",
        metavar="VERSION",
        help="Python version to install on workers (ignored when --image-id is given)",
    )
    parser.add_argument("--gui-port", type=int, default=50001, metavar="PORT", help="port for the web GUI")
    args = parser.parse_args()

    public_ip = _ec2_metadata("--public-ipv4")
    private_ip = _ec2_metadata("--local-ipv4")

    config_content = _build_config(
        public_ip=public_ip,
        private_ip=private_ip,
        scheduler_port=args.scheduler_port,
        oss_port=args.object_storage_port,
        gui_port=args.gui_port,
        worker_manager_id=args.worker_manager_id,
        instance_type=args.instance_type,
        aws_region=args.aws_region,
        logging_level=args.logging_level,
        image_id=args.image_id,
        python_version=args.python_version,
    )

    output_path = Path(args.output)
    output_path.write_text(config_content)

    print(f"Config written to {output_path}")
    if not args.image_id:
        print(f"\nTo add Python packages for your workers, edit the requirements_txt field in {output_path}.")
    print("\nStart Scaler on this instance:")
    print(f"    scaler {output_path}")
    print(f"\nConnect from your local machine (scheduler port {args.scheduler_port}):")
    print("    from scaler import Client")
    print(f'    with Client(address="tcp://{public_ip}:{args.scheduler_port}") as client:')
    print("        ...")
    print(f"\nOpen the web GUI in your browser (ensure port {args.gui_port} is open in the EC2 security group):")
    print(f"    http://{public_ip}:{args.gui_port}")
