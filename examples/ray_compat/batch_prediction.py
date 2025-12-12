"""
This example was copied from https://docs.ray.io/en/latest/ray-core/examples/batch_prediction.html

Like in `highly_parallel.py`, only minimal changes are needed for the example to work on Scaler.
"""


import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import ray

# changed line 1/2
import scaler.compat.ray  # noqa: F401


def load_model():
    # A dummy model.
    def model(batch: pd.DataFrame) -> pd.DataFrame:
        # Dummy payload so copying the model will actually copy some data
        # across nodes.
        model.payload = np.zeros(100_000_000)  # type: ignore[attr-defined]
        return pd.DataFrame({"score": batch["passenger_count"] % 2 == 0})

    return model


def main():
    @ray.remote
    def make_prediction(model, shard_path):
        df = pq.read_table(shard_path).to_pandas()
        result = model(df)

        # Write out the prediction result.
        # NOTE: unless the driver will have to further process the
        # result (other than simply writing out to storage system),
        # writing out at remote task is recommended, as it can avoid
        # congesting or overloading the driver.
        # ...

        # Here we just return the size about the result in this example.
        return len(result)

    # 12 files, one for each remote task.
    input_files = [
        f"s3://anonymous@air-example-data/ursa-labs-taxi-data/downsampled_2009_full_year_data.parquet"
        f"/fe41422b01c04169af2a65a83b753e0f_{i:06d}.parquet"
        for i in range(12)
    ]

    # ray.put() the model just once to local object store, and then pass the
    # reference to the remote tasks.
    model = load_model()
    model_ref = ray.put(model)

    result_refs = []

    # Launch all prediction tasks.
    for file in input_files:
        # Launch a prediction task by passing model reference and shard file to it.
        # NOTE: it would be highly inefficient if you are passing the model itself
        # like make_prediction.remote(model, file), which in order to pass the model
        # to remote node will ray.put(model) for each task, potentially overwhelming
        # the local object store and causing out-of-disk error.
        result_refs.append(make_prediction.remote(model_ref, file))

    results = ray.get(result_refs)

    # Let's check prediction output size.
    for r in results:
        print("Prediction output size:", r)


if __name__ == "__main__":
    main()

    # changed line 2/2
    ray.shutdown()
