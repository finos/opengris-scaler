"""
OCI Container Instance Job Runner.

Runs inside OCI Container Instances to execute Scaler tasks.
Handles both inline (environment variable) and OCI Object Storage-based payloads,
with optional gzip compression.

This is a standalone script that does not depend on the full scaler package.
Runtime dependencies: cloudpickle, oci

Configuration is provided via environment variables set by OCIHPCTaskManager:
    TASK_ID        - Scaler task ID (hex string)
    OCI_NAMESPACE  - OCI Object Storage tenancy namespace
    OCI_BUCKET     - OCI Object Storage bucket name
    OCI_PREFIX     - OCI Object Storage key prefix (e.g., "scaler-tasks")
    OCI_OBJECT_KEY - Object key for the payload, or "none" for inline mode
    PAYLOAD_B64    - Base64-encoded inline payload (empty string when using Object Storage)
    COMPRESSED     - "1" if payload is gzip-compressed, "0" otherwise

Authentication inside the container uses OCI Resource Principals, which are
automatically available when running in an OCI Container Instance that belongs
to a Dynamic Group with the appropriate IAM policies.
"""

import base64
import gzip
import logging
import os
import signal
import sys
import traceback

COMPRESSION_THRESHOLD_BYTES: int = 4096


def signal_handler(signum, frame):
    """Log before exit on signal."""
    sig_name = signal.Signals(signum).name
    logging.error(f"Received signal {sig_name} ({signum})")
    sys.stdout.flush()
    sys.exit(128 + signum)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
        force=True,
    )
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)  # type: ignore[union-attr]

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)


def get_env(key: str, default: str = "") -> str:
    """Read an environment variable, treating the literal string "none" as empty."""
    value = os.environ.get(key, default)
    if value.lower() == "none":
        return ""
    return value


def build_oci_object_storage_client():
    """
    Build an OCI Object Storage client.

    Prefers Resource Principal auth (available inside OCI Container Instances
    with a suitably configured Dynamic Group). Falls back to the OCI config
    file (``~/.oci/config``) for local development and testing.
    """
    import oci

    try:
        signer = oci.auth.signers.get_resource_principals_signer()
        return oci.object_storage.ObjectStorageClient(config={}, signer=signer)
    except Exception as rp_exc:
        logging.warning(f"Resource Principal auth failed ({rp_exc}), falling back to config file")
        config = oci.config.from_file()
        return oci.object_storage.ObjectStorageClient(config)


def get_payload(
    object_storage_client,
    namespace: str,
    bucket: str,
    object_key: str,
    payload_b64: str,
    compressed: bool,
) -> bytes:
    """
    Fetch the task payload.

    Prefers the inline base64-encoded payload (set via environment variable for
    small tasks). Falls back to fetching from OCI Object Storage for larger tasks.
    """
    if payload_b64:
        payload = base64.b64decode(payload_b64)
        if compressed:
            payload = gzip.decompress(payload)
        return payload

    if not namespace or not bucket or not object_key:
        raise ValueError("No payload available: set PAYLOAD_B64 or provide OCI_NAMESPACE/OCI_BUCKET/OCI_OBJECT_KEY")

    logging.info(f"Fetching payload from Object Storage: {bucket}/{object_key}")
    response = object_storage_client.get_object(
        namespace_name=namespace,
        bucket_name=bucket,
        object_name=object_key,
    )
    payload = response.data.content

    if compressed or object_key.endswith(".gz"):
        payload = gzip.decompress(payload)

    # Clean up input object from Object Storage
    try:
        object_storage_client.delete_object(
            namespace_name=namespace,
            bucket_name=bucket,
            object_name=object_key,
        )
    except Exception as cleanup_exc:
        logging.warning(f"Failed to clean up input object {object_key}: {cleanup_exc}")

    return payload


def store_result(
    object_storage_client,
    result_bytes: bytes,
    namespace: str,
    bucket: str,
    prefix: str,
    instance_id: str,
) -> str:
    """
    Compress (if beneficial) and store the result in OCI Object Storage.

    The result key is derived from the Container Instance OCID so that the
    adapter can look it up after the container exits.
    """
    compressed = False
    if len(result_bytes) > COMPRESSION_THRESHOLD_BYTES:
        result_bytes = gzip.compress(result_bytes)
        compressed = True

    # The adapter uses the last segment of the OCID as the result key
    instance_suffix = instance_id.split(".")[-1] if instance_id else "unknown"
    result_key = f"{prefix}/results/{instance_suffix}.pkl"
    if compressed:
        result_key += ".gz"

    object_storage_client.put_object(
        namespace_name=namespace,
        bucket_name=bucket,
        object_name=result_key,
        put_object_body=result_bytes,
    )

    logging.info(f"Result stored: {bucket}/{result_key}")
    return result_key


def main() -> None:
    setup_logging()

    task_id = get_env("TASK_ID", "unknown")
    namespace = get_env("OCI_NAMESPACE")
    bucket = get_env("OCI_BUCKET")
    prefix = get_env("OCI_PREFIX", "scaler-tasks")
    object_key = get_env("OCI_OBJECT_KEY")
    payload_b64 = get_env("PAYLOAD_B64")
    compressed = get_env("COMPRESSED", "0") == "1"

    # OCI_RESOURCE_PRINCIPAL_RPST is set by OCI when running with Resource Principal auth
    instance_id = os.environ.get("OCI_RESOURCE_PRINCIPAL_RPST", "")
    if not instance_id:
        # Fallback: derive from hostname (set by OCI Container Instances)
        instance_id = os.environ.get("OCI_INSTANCE_ID", os.uname().nodename)

    logging.info(f"Starting task {task_id[:8]}...")
    logging.info(f"namespace={namespace}, bucket={bucket}, prefix={prefix}")

    object_storage_client = build_oci_object_storage_client()

    try:
        import cloudpickle

        payload_bytes = get_payload(
            object_storage_client=object_storage_client,
            namespace=namespace,
            bucket=bucket,
            object_key=object_key,
            payload_b64=payload_b64,
            compressed=compressed,
        )
        task_data = cloudpickle.loads(payload_bytes)

        logging.info(f"Task data loaded, keys: {list(task_data.keys())}")

        if "function" not in task_data or "arguments" not in task_data:
            raise ValueError("Task data missing 'function' and 'arguments' — reference mode not yet supported")

        func = task_data["function"]
        arguments = task_data["arguments"]

        logging.info(f"Executing function '{getattr(func, '__name__', 'unknown')}' with {len(arguments)} argument(s)")
        sys.stdout.flush()

        result = func(*arguments)
        logging.info(f"Function completed, result type: {type(result).__name__}")
        sys.stdout.flush()

        result_bytes = cloudpickle.dumps(result)
        logging.info(f"Result serialized ({len(result_bytes)} bytes), storing to Object Storage...")

        store_result(
            object_storage_client=object_storage_client,
            result_bytes=result_bytes,
            namespace=namespace,
            bucket=bucket,
            prefix=prefix,
            instance_id=instance_id,
        )

        logging.info(f"Task {task_id[:8]} completed successfully")

    except Exception as exc:
        logging.error(f"Task {task_id[:8]} failed: {exc}")
        traceback.print_exc()

        # Store the error so the adapter can surface it as a failed future
        try:
            import cloudpickle

            error_data = {"error": str(exc), "traceback": traceback.format_exc()}
            error_bytes = cloudpickle.dumps(error_data)
            store_result(
                object_storage_client=object_storage_client,
                result_bytes=error_bytes,
                namespace=namespace,
                bucket=bucket,
                prefix=prefix,
                instance_id=instance_id,
            )
        except Exception as store_exc:
            logging.error(f"Failed to store error result: {store_exc}")

        sys.exit(1)


if __name__ == "__main__":
    main()
