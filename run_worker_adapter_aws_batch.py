from scaler_aws_batch.entry_points.worker_adapter_aws_batch import main
from scaler_aws_batch.utility.debug import pdb_wrapped

if __name__ == "__main__":
    pdb_wrapped(main)()
