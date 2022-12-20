# Tutorial

This page covers brief tutorials and notes on how to use pycytominer-transform.

## CellProfiler CSV Output to Parquet

[CellProfiler](https://cellprofiler.org/) pipelines or projects may produce various CSV-based compartment output (for example, "Cells.csv", "Cytoplasm.csv", etc.).
Pycytominer-transform converts this data to Parquet from local or object-storage based locations.

Files with similar names nested within sub-folders will be concatenated by default (appended to the end of each data file) together and used to create a single Parquet file per compartment.
For example: if we have `folder/subfolder_a/cells.csv` and `folder/subfolder_b/cells.csv`, using `convert(source_path="folder", ...)` will result in `folder.cells.parquet` (unless `concat=False`).

For example, see below:

```python
from pycytominer_transform import convert

# using a local path with cellprofiler csv presets
convert(
    source_path="./tests/data/cellprofiler/csv_single",
    source_datatype="csv",
    dest_path=".",
    dest_datatype="parquet",
    preset="cellprofiler_csv",
)

# using an s3-compatible path with no signature for client
# and cellprofiler csv presets
convert(
    source_path="s3://s3path",
    source_datatype="csv",
    dest_path=".",
    dest_datatype="parquet",
    concat=True,
    preset="cellprofiler_csv",
    no_sign_request=True,
)
```
