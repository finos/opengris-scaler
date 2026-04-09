# Running the Quant Research Sample Locally with Scaler

This document describes how to run the `order_flow_ray` feature engineering pipeline
from the [aws-samples/quant-research-sample-using-amazon-ecs-and-aws-batch](https://github.com/aws-samples/quant-research-sample-using-amazon-ecs-and-aws-batch)
repository entirely on your local machine using Scaler's built-in `SchedulerClusterCombo`.
No EC2, no S3, and no remote packaging are required.

## Prerequisites

Ray does not yet publish wheels for Python 3.14, so use a Python ≤3.13 environment.

```bash
# One-time environment setup (Python 3.13 example)
uv venv --python 3.13 .venv-313
source .venv-313/bin/activate
uv pip install -e .                          # installs scaler
uv pip install ray "polars[rtcompat]" boto3  # sample dependencies
# polars[rtcompat] is needed on CPUs without AVX2/FMA (e.g. older x86, VMs)
# use plain `polars` instead if your CPU supports AVX2
```

## Clone the upstream repo

```bash
git clone https://github.com/aws-samples/quant-research-sample-using-amazon-ecs-and-aws-batch.git
cd quant-research-sample-using-amazon-ecs-and-aws-batch/samples/order_flow_ray
```

## Apply the modifications

The patches below add local-filesystem data access and a Scaler-native entry point.
Apply each one in order.

### `src/data_preprocessing/data_access/__init__.py`

Lazy-load `S3TablesDataAccess` so `pyiceberg` is not required for local runs.

```diff
diff --git a/samples/order_flow_ray/src/data_preprocessing/data_access/__init__.py b/samples/order_flow_ray/src/data_preprocessing/data_access/__init__.py
--- a/samples/order_flow_ray/src/data_preprocessing/data_access/__init__.py
+++ b/samples/order_flow_ray/src/data_preprocessing/data_access/__init__.py
@@ -3,7 +3,11 @@
 
 from .base import DataAccess
 from .s3 import S3DataAccess
-from .s3tables import S3TablesDataAccess
 from .factory import DataAccessFactory
 
+try:
+    from .s3tables import S3TablesDataAccess
+except ImportError:
+    S3TablesDataAccess = None  # type: ignore[assignment,misc]
+
 __all__ = ["DataAccess", "S3DataAccess", "S3TablesDataAccess", "DataAccessFactory"]
```

---

### `src/data_preprocessing/data_access/local.py` (new file)

Local filesystem implementation of `DataAccess` using Polars.

```python
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import pathlib
from typing import List, Tuple

import polars as pl

from .base import DataAccess


class LocalDataAccess(DataAccess):
    """Local filesystem data access implementation using polars."""

    def list_files(self, path: str) -> List[Tuple[str, float]]:
        """Return [(abs_path, size_gb)] for all .parquet files under path."""
        root = pathlib.Path(path)
        result = []
        for p in sorted(root.rglob("*.parquet")):
            size_gb = p.stat().st_size / (1024**3)
            result.append((str(p), size_gb))
        return result

    def list_files_asynch(self, path: str, parallel_discovery_threshold: int = 20) -> List[Tuple[str, float]]:
        """Local filesystem discovery — no parallelism needed."""
        return self.list_files(path)

    def discover_files_asynch(
        self, path: str, sort_order: str = "asc", parallel_discovery_threshold: int = 20
    ) -> List[Tuple[str, float]]:
        """Discover files with sorting."""
        print(f"Discovering files in: {path}")
        files = self.list_files(path)
        files.sort(key=lambda x: x[1], reverse=(sort_order == "desc"))
        return files

    def read(self, path: str, **kwargs) -> pl.LazyFrame:
        """Read parquet from local path."""
        return pl.scan_parquet(path, **kwargs)

    def write(self, data: pl.LazyFrame | pl.DataFrame, path: str, **kwargs) -> None:
        """Write parquet to local path."""
        pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
        if isinstance(data, pl.LazyFrame):
            data.sink_parquet(path, **kwargs)
        else:
            data.write_parquet(path, **kwargs)

    def write_csv(self, data: pl.DataFrame, path: str) -> None:
        """Write CSV to local path."""
        pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
        data.write_csv(path)

    def write_inventory(self, inventory_name: str, files: List[Tuple[str, float]], metadata_path: str) -> None:
        """Write discovered files to inventory CSV."""
        df = pl.DataFrame({"file_path": [f[0] for f in files], "file_size": [f[1] for f in files]})
        out = pathlib.Path(metadata_path) / f"{inventory_name}_input_inventory.csv"
        out.parent.mkdir(parents=True, exist_ok=True)
        df.write_csv(str(out))
        print(f"Wrote inventory to {out}")

    def read_inventory(self, inventory_name: str, metadata_path: str) -> List[Tuple[str, float]]:
        """Read discovered files from inventory CSV."""
        inv = pathlib.Path(metadata_path) / f"{inventory_name}_input_inventory.csv"
        df = pl.scan_csv(str(inv)).collect()
        return [(row["file_path"], float(row["file_size"])) for row in df.to_dicts()]

    def group_files_by_size(self, files: List[Tuple[str, float]]) -> List[List[Tuple[str, float]]]:
        """Group files into subarrays where each subarray total size approximates the max file size."""
        if not files:
            return []

        max_size = max(size for _, size in files)

        groups: List[List[Tuple[str, float]]] = []
        current_group: List[Tuple[str, float]] = []
        current_size = 0.0

        for file_path, file_size in files:
            if current_size + file_size > max_size and current_group:
                groups.append(current_group)
                current_group = []
                current_size = 0.0
            current_group.append((file_path, file_size))
            current_size += file_size

        if current_group:
            groups.append(current_group)

        return groups
```

---

### `src/data_preprocessing/data_access/factory.py`

Register `LocalDataAccess` and lazy-load `S3TablesDataAccess`.

```diff
diff --git a/samples/order_flow_ray/src/data_preprocessing/data_access/factory.py b/samples/order_flow_ray/src/data_preprocessing/data_access/factory.py
--- a/samples/order_flow_ray/src/data_preprocessing/data_access/factory.py
+++ b/samples/order_flow_ray/src/data_preprocessing/data_access/factory.py
@@ -2,20 +2,28 @@
 # SPDX-License-Identifier: MIT-0
 
 from .base import DataAccess
+from .local import LocalDataAccess
 from .s3 import S3DataAccess
-from .s3tables import S3TablesDataAccess
 
 class DataAccessFactory:
     """Factory for creating data access implementations."""
-    
+
     _implementations = {
+        "local": LocalDataAccess,
         "s3": S3DataAccess,
-        "s3tables": S3TablesDataAccess
     }
+
+    @classmethod
+    def _ensure_s3tables(cls) -> None:
+        if "s3tables" not in cls._implementations:
+            from .s3tables import S3TablesDataAccess
+            cls._implementations["s3tables"] = S3TablesDataAccess
     
     @classmethod
     def create(cls, access_type: str, **kwargs) -> DataAccess:
         """Create data access implementation."""
+        if access_type == "s3tables":
+            cls._ensure_s3tables()
         if access_type not in cls._implementations:
             raise ValueError(f"Unknown access type: {access_type}")
         return cls._implementations[access_type](**kwargs)
```

---

### `src/pipeline/config.py`

Add `LocalLocation` storage location type.

```diff
diff --git a/samples/order_flow_ray/src/pipeline/config.py b/samples/order_flow_ray/src/pipeline/config.py
--- a/samples/order_flow_ray/src/pipeline/config.py
+++ b/samples/order_flow_ray/src/pipeline/config.py
@@ -42,6 +42,22 @@ class S3Location(StorageLocation):
         return self.path
 
 
+@dataclass(frozen=True)
+class LocalLocation(StorageLocation):
+    """Local filesystem storage location.
+
+    Args:
+        path: Absolute or relative local filesystem path
+    """
+    path: str
+
+    def get_access_type(self) -> str:
+        return 'local'
+
+    def get_path(self) -> str:
+        return self.path
+
+
 @dataclass(frozen=True)
 class S3TablesLocation(StorageLocation):
```

---

### `src/pipeline/pipeline.py`

Route `local` access type in `initialize()` and in the feature engineering worker.

```diff
diff --git a/samples/order_flow_ray/src/pipeline/pipeline.py b/samples/order_flow_ray/src/pipeline/pipeline.py
--- a/samples/order_flow_ray/src/pipeline/pipeline.py
+++ b/samples/order_flow_ray/src/pipeline/pipeline.py
@@ -50,7 +50,10 @@ class Pipeline:
         input_location = self.config.storage.get_step_input(first_step)
         
         # Initialize data access for input location
-        if input_location.get_access_type() == 's3tables':
+        access_type = input_location.get_access_type()
+        if access_type == 'local':
+            self.data_access = DataAccessFactory.create('local')
+        elif access_type == 's3tables':
             self.data_access = DataAccessFactory.create(
                 's3tables',
                 region=self.config.region,
@@ -64,11 +67,13 @@ class Pipeline:
                 region=self.config.region,
                 profile_name=self.config.profile_name
             )
-        
+
         # Initialize data access for normalized output (if normalization is enabled)
         if self.config.processing.normalization:
             norm_loc = self.config.storage.normalized
-            if norm_loc.get_access_type() == 's3tables':
+            if norm_loc.get_access_type() == 'local':
+                self.normalized_access = self.data_access
+            elif norm_loc.get_access_type() == 's3tables':
                 self.normalized_access = DataAccessFactory.create(
                     's3tables',
                     region=self.config.region,
@@ -579,12 +584,18 @@ class Pipeline:
                         import polars as pl
                         from data_preprocessing.data_access.factory import DataAccessFactory
                         from feature_engineering.order_flow import OrderFlowFeatureEngineering
-                        
+
                         # Extract data_type from path
                         data_type = 'level2q' if 'level2q' in file_path else 'trades'
-                        
-                        # Read from normalized
-                        data_access = DataAccessFactory.create('s3', region=region, profile_name=profile)
+
+                        # Create data access appropriate for input location
+                        input_access_type = features_loc_dict.get('access_type', 's3')
+                        if input_access_type == 'local':
+                            data_access = DataAccessFactory.create('local')
+                        else:
+                            data_access = DataAccessFactory.create('s3', region=region, profile_name=profile)
+
+                        # Read from repartitioned location
                         df = data_access.read(file_path)
                         input_row_count = df.select(pl.len()).collect().item()
```

---

### `src/scripts/convert_bmll_sample.py` (new file)

Converts the bundled BMLL CSV samples into the `repartitioned_v3` parquet layout expected
by the pipeline. See the schema notes in the module docstring — in particular:

- `ISOExchangeCode` is missing from the CSV and is derived from `MIC`.
- `ExchangeTicker` is derived as `Ticker + "_" + MIC`.
- `TimestampNanoseconds` is parsed by Polars as `Datetime`; it must be cast to `Int64`
  nanoseconds-since-epoch (the slope calculation divides by `1e9`).

```python
#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Convert the BMLL CSV sample files into the repartitioned_v3 parquet structure
expected by the feature engineering pipeline.

The output path layout mirrors what the normalization → repartition pipeline produces:

    <BASE>/repartitioned_v3/
        2025/01/16/trades/AMERICAS/A/XNAS-20250116.parquet
        2025/01/16/level2q/AMERICAS/A/XNAS-20250116.parquet

Usage:
    Set LOCAL_DATA_BASE to your data root (default: ~/data/order_flow), then:

        cd samples/order_flow_ray/samples/order_flow_ray/src
        python scripts/convert_bmll_sample.py
"""

import os
import pathlib

import polars as pl

SCRIPT_DIR = pathlib.Path(__file__).parent
SRC_DIR = SCRIPT_DIR.parent
SAMPLE_DIR = SRC_DIR / "BMLL_data_sample"

BASE = pathlib.Path(os.environ.get("LOCAL_DATA_BASE", pathlib.Path.home() / "data" / "order_flow"))
REPART_BASE = BASE / "repartitioned_v3"

TRADES_CSV = SAMPLE_DIR / "Trades_AMERICAS_Sample.csv"
L2Q_CSV = SAMPLE_DIR / "L2_Quotes_AMERICAS_Sample.csv"

DATE = "2025-01-16"
YEAR, MONTH, DAY = "2025", "01", "16"
REGION = "AMERICAS"
MIC = "XNAS"
FILENAME = f"{MIC}-{DATE.replace('-', '')}.parquet"


def convert_trades() -> None:
    print(f"Converting trades: {TRADES_CSV}")
    df = pl.read_csv(
        TRADES_CSV,
        try_parse_dates=True,
        null_values=["", "nan"],
        infer_schema_length=10000,
    )

    if "" in df.columns:
        df = df.drop("")

    if df["TradeDate"].dtype != pl.Date:
        df = df.with_columns(pl.col("TradeDate").cast(pl.Date))

    df = df.with_columns(
        [
            pl.lit("trades").alias("DataType"),
            pl.lit(REGION).alias("Region"),
            pl.lit(int(YEAR)).cast(pl.Int32).alias("Year"),
            pl.lit(int(MONTH)).cast(pl.Int8).alias("Month"),
            pl.lit(int(DAY)).cast(pl.Int8).alias("Day"),
            pl.col("MIC").alias("ISOExchangeCode"),
            (pl.col("Ticker") + pl.lit("_") + pl.col("MIC")).alias("ExchangeTicker"),
            pl.col("Ticker").str.slice(0, 1).str.to_uppercase().alias("TickerPrefix"),
        ]
    )

    for ticker_prefix, group_df in df.group_by("TickerPrefix"):
        prefix = ticker_prefix[0] if isinstance(ticker_prefix, tuple) else ticker_prefix
        out_path = REPART_BASE / YEAR / MONTH / DAY / "trades" / REGION / prefix / FILENAME
        out_path.parent.mkdir(parents=True, exist_ok=True)
        group_df.write_parquet(str(out_path))
        print(f"  Wrote {len(group_df):,} rows -> {out_path}")


def convert_l2q() -> None:
    print(f"Converting L2Q: {L2Q_CSV}")
    df = pl.read_csv(
        L2Q_CSV,
        try_parse_dates=True,
        null_values=["", "nan"],
        infer_schema_length=10000,
    )

    if "" in df.columns:
        df = df.drop("")

    if df["TradeDate"].dtype != pl.Date:
        df = df.with_columns(pl.col("TradeDate").cast(pl.Date))

    # TimestampNanoseconds is parsed as Datetime by polars; cast to Int64 nanoseconds
    if df["TimestampNanoseconds"].dtype != pl.Int64:
        df = df.with_columns(pl.col("TimestampNanoseconds").dt.timestamp("ns").alias("TimestampNanoseconds"))

    df = df.with_columns(
        [
            pl.lit("level2q").alias("DataType"),
            pl.lit(REGION).alias("Region"),
            pl.lit(int(YEAR)).cast(pl.Int32).alias("Year"),
            pl.lit(int(MONTH)).cast(pl.Int8).alias("Month"),
            pl.lit(int(DAY)).cast(pl.Int8).alias("Day"),
            pl.col("MIC").alias("ISOExchangeCode"),
            (pl.col("Ticker") + pl.lit("_") + pl.col("MIC")).alias("ExchangeTicker"),
            pl.col("Ticker").str.slice(0, 1).str.to_uppercase().alias("TickerPrefix"),
        ]
    )

    for ticker_prefix, group_df in df.group_by("TickerPrefix"):
        prefix = ticker_prefix[0] if isinstance(ticker_prefix, tuple) else ticker_prefix
        out_path = REPART_BASE / YEAR / MONTH / DAY / "level2q" / REGION / prefix / FILENAME
        out_path.parent.mkdir(parents=True, exist_ok=True)
        group_df.write_parquet(str(out_path))
        print(f"  Wrote {len(group_df):,} rows -> {out_path}")


def main() -> None:
    print(f"Data root: {BASE}")
    print(f"Output base: {REPART_BASE}\n")

    if not TRADES_CSV.exists():
        raise FileNotFoundError(f"Trades CSV not found: {TRADES_CSV}")
    if not L2Q_CSV.exists():
        raise FileNotFoundError(f"L2Q CSV not found: {L2Q_CSV}")

    convert_trades()
    print()
    convert_l2q()

    print(f"\nDone. Parquet files written to: {REPART_BASE}")
    print("\nTo run the feature engineering pipeline:")
    print(f"  LOCAL_DATA_BASE={BASE} python pipeline_workflow/order_flow_feature_engineering_local.py")


if __name__ == "__main__":
    main()
```

---

### `src/pipeline_workflow/order_flow_feature_engineering_local.py` (new file)

Entry point that starts an embedded Scaler cluster (scheduler + subprocess workers) and
runs the feature engineering pipeline entirely on local data.

```python
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Run feature engineering pipeline locally using Scaler's native worker manager.

No EC2, no S3, no remote packaging required.  Scaler starts local subprocess workers
and all data lives on the local filesystem.

Usage:
    Set BASE to the root of your local data directory, then run:

        cd samples/order_flow_ray/samples/order_flow_ray/src
        python pipeline_workflow/order_flow_feature_engineering_local.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import scaler.compat.ray  # patches ray module in-place
from scaler.compat.ray import scaler_init, remote as _scaler_remote

import ray
assert ray.remote is _scaler_remote, "ray.remote was not patched by Scaler compat layer"

from feature_engineering.order_flow import OrderFlowFeatureEngineering
from pipeline.config import (
    DataConfig,
    LocalLocation,
    PipelineConfig,
    ProcessingConfig,
    RayConfig,
    StorageConfig,
)
from pipeline.pipeline import Pipeline

# Root directory that contains your local data. Override with LOCAL_DATA_BASE env var.
BASE = os.environ.get("LOCAL_DATA_BASE", os.path.expanduser("~/data/order_flow"))


def main() -> None:
    # Start an embedded Scaler cluster (scheduler + workers as local subprocesses).
    # n_workers defaults to cpu_count(); pass n_workers=N to tune.
    scaler_init()

    config = PipelineConfig(
        region="us-east-1",  # unused for local storage, kept for config compatibility
        data=DataConfig(
            raw_data_path=f"{BASE}/repartitioned_v3",
            start_date="2025-01-16",
            end_date="2025-01-16",
            exchanges=["AMERICAS"],
            data_types=["trades", "level2q"],
        ),
        processing=ProcessingConfig(
            feature_engineering=OrderFlowFeatureEngineering(
                bar_duration_ms=250,
                max_section=7,
                group_filter=None,
            )
        ),
        storage=StorageConfig(
            raw_data=LocalLocation(f"{BASE}/raw"),
            normalized=LocalLocation(f"{BASE}/normalized"),
            repartitioned=LocalLocation(f"{BASE}/repartitioned_v3"),
            reconciliation=LocalLocation(f"{BASE}/reconciliation"),
            features=LocalLocation(f"{BASE}/features"),
            models=LocalLocation(f"{BASE}/models"),
            predictions=LocalLocation(f"{BASE}/predictions"),
            backtest=LocalLocation(f"{BASE}/backtest"),
            metadata=LocalLocation(f"{BASE}/metadata"),
        ),
        ray=RayConfig(runtime_env={}, flat_core_count=None),
    )

    print("Running Feature Engineering Pipeline (local mode)...")
    print(f"Data root: {BASE}")
    pipeline = Pipeline(config)
    results = pipeline.run()

    print("\n" + "=" * 80)
    print("Feature Engineering Pipeline Results")
    print("=" * 80)
    print(f"Total files processed: {len(results)}")

    successful = [r for r in results if r.get("message", "success") == "success"]
    failed = [r for r in results if r.get("message", "success") != "success"]

    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")

    if successful:
        total_input_rows = sum(r.get("input_row_count", 0) for r in successful)
        total_output_rows = sum(r.get("row_count", 0) for r in successful)
        print(f"\nTotal rows: {total_input_rows:,} -> {total_output_rows:,}")

        print("\nSample successful files:")
        for result in successful[:5]:
            input_path = result.get("input_path", "N/A")
            output_path = result.get("output_path", "N/A")
            input_rows = result.get("input_row_count", 0)
            output_rows = result.get("row_count", 0)
            print(f"  {input_path}")
            print(f"    -> {output_path}")
            print(f"    ({input_rows:,} -> {output_rows:,} rows)")

    if failed:
        print("\nFailed files:")
        for result in failed[:5]:
            print(f"  {result.get('input_path', 'N/A')}: {result.get('message', 'Unknown error')[:100]}")


if __name__ == "__main__":
    main()
```

---

## Run the pipeline

```bash
cd samples/order_flow_ray/samples/order_flow_ray/src

# One-time: convert bundled CSV samples to parquet
python scripts/convert_bmll_sample.py

# Run feature engineering on local Scaler cluster
python pipeline_workflow/order_flow_feature_engineering_local.py
```

To use a different data root:

```bash
LOCAL_DATA_BASE=/path/to/your/data python scripts/convert_bmll_sample.py
LOCAL_DATA_BASE=/path/to/your/data python pipeline_workflow/order_flow_feature_engineering_local.py
```

## Expected output

Run against the bundled BMLL sample data (AAPL/XNAS, 2025-01-16):

```
Discovered 6 files from ~/data/order_flow/repartitioned_v3
Grouped into 4 file groups

Feature_Engineering FINAL SUMMARY:
Total successful: 6/6  ✓
Total failed:     0/6

18,000 input rows → 5,642 feature rows
  trades/AMERICAS/A  1,000 →   550 rows
  trades/AMERICAS/C  1,000 →   569 rows
  trades/AMERICAS/S  1,000 →   332 rows
  level2q/AMERICAS/A 5,000 → 1,444 rows  (207 features, 7 sections)
  level2q/AMERICAS/C 5,000 → 2,221 rows
  level2q/AMERICAS/S 5,000 →   526 rows
```

Scaler starts local subprocess workers (one per CPU by default), distributes the task
groups, and shuts down cleanly.
