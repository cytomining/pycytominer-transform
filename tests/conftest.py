"""
conftest.py for pytest
"""
import pathlib
import shutil
import subprocess
import tempfile
from typing import Any, Dict, Generator, List, Tuple

import boto3
import boto3.session
import duckdb
import pyarrow as pa
import pytest
from moto import mock_s3
from moto.server import ThreadedMotoServer
from pyarrow import csv, parquet
from pycytominer.cyto_utils.cells import SingleCells

from pycytominer_transform import column_sort


# note: we use name here to avoid pylint flagging W0621
@pytest.fixture(name="get_tempdir", scope="session")
def fixture_get_tempdir() -> Generator:
    """
    Provide temporary directory for testing
    """

    tmpdir = tempfile.mkdtemp()

    yield tmpdir

    shutil.rmtree(path=tmpdir, ignore_errors=True)


@pytest.fixture(name="data_dir_cellprofiler")
def fixture_data_dir_cellprofiler() -> str:
    """
    Provide a data directory for cellprofiler test data
    """

    return f"{pathlib.Path(__file__).parent}/data/cellprofiler"


@pytest.fixture(name="data_dirs_cytominerdatabase")
def fixture_data_dirs_cytominerdatabase() -> List[str]:
    """
    Provide a data directory for cytominer-database test data
    """

    basedir = f"{pathlib.Path(__file__).parent}/data/cytominer-database"

    return [
        f"{basedir}/data_a",
        f"{basedir}/data_b",
    ]


@pytest.fixture(name="cytominerdatabase_sqlite")
def fixture_cytominerdatabase_sqlite(
    get_tempdir: str,
    data_dirs_cytominerdatabase: List[str],
) -> List[str]:
    """
    Processed cytominer-database test data as sqlite data
    """

    output_paths = []
    for data_dir in data_dirs_cytominerdatabase:
        # example command for reference as subprocess below
        # cytominer-database ingest source_directory sqlite:///backend.sqlite -c ingest_config.ini
        output_path = f"sqlite:///{get_tempdir}/{pathlib.Path(data_dir).name}.sqlite"

        # run cytominer-database as command-line call
        subprocess.call(
            [
                "cytominer-database",
                "ingest",
                data_dir,
                output_path,
                "-c",
                f"{data_dir}/config_SQLite.ini",
            ]
        )
        # store the sqlite output file within list to be returned
        output_paths.append(output_path)

    return output_paths


@pytest.fixture()
def pycytominer_merge_single_cells_parquet(
    get_tempdir: str,
    cytominerdatabase_sqlite: List[str],
) -> List[str]:
    """
    Processed cytominer-database test sqlite data as
    pycytominer merged single cell parquet files
    """

    output_paths = []
    for sqlite_file in cytominerdatabase_sqlite:
        # build SingleCells from database and merge single cells into parquet file
        output_paths.append(
            SingleCells(
                sqlite_file,
                strata=["Metadata_Well"],
                image_cols=["TableNumber", "ImageNumber"],
            ).merge_single_cells(
                sc_output_file=f"{get_tempdir}/{pathlib.Path(sqlite_file).name}.parquet",
                output_type="parquet",
                join_on=["Image_Metadata_Well"],
            )
        )

    return output_paths


@pytest.fixture(name="example_tables")
def fixture_example_tables() -> Tuple[pa.Table, ...]:
    """
    Provide static example tables
    """

    table_image = pa.Table.from_pydict(
        {
            "ImageNumber": pa.array(["1", "1", "2", "2"]),
            "Image_Metadata_Plate": pa.array(["001", "001", "002", "002"]),
            "Image_Metadata_Well": pa.array(["A1", "A1", "A2", "A2"]),
        }
    )
    table_cytoplasm = pa.Table.from_pydict(
        {
            "ImageNumber": pa.array(["1", "1", "2", "2"]),
            "Cytoplasm_ObjectNumber": pa.array([1, 2, 1, 2]),
            "Cytoplasm_Feature_X": pa.array([0.1, 0.2, 0.1, 0.2]),
        }
    )
    table_cells = pa.Table.from_pydict(
        {
            "ImageNumber": pa.array(["1", "1", "2", "2"]),
            "Cells_ObjectNumber": pa.array([1, 2, 1, 2]),
            "Cells_Feature_Y": pa.array([0.01, 0.02, 0.01, 0.02]),
        }
    )
    table_nuclei_1 = pa.Table.from_pydict(
        {
            "ImageNumber": pa.array(
                [
                    "1",
                    "1",
                ]
            ),
            "Nuclei_ObjectNumber": pa.array(
                [
                    1,
                    2,
                ]
            ),
            "Nuclei_Feature_Z": pa.array(
                [
                    0.001,
                    0.002,
                ]
            ),
        }
    )

    table_nuclei_2 = pa.Table.from_pydict(
        {
            "ImageNumber": pa.array(["2", "2"]),
            "Nuclei_ObjectNumber": pa.array([1, 2]),
            "Nuclei_Feature_Z": pa.array([0.001, 0.002]),
        }
    )

    return table_image, table_cytoplasm, table_cells, table_nuclei_1, table_nuclei_2


@pytest.fixture(name="example_local_sources")
def fixture_example_local_sources(
    get_tempdir: str,
    example_tables: Tuple[pa.Table, ...],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Provide an example source
    """

    for table, number, name in zip(
        example_tables,
        range(0, len(example_tables)),
        ["image", "cytoplasm", "cells", "nuclei", "nuclei"],
    ):
        # build paths for output to land
        pathlib.Path(f"{get_tempdir}/example/{number}").mkdir(
            parents=True, exist_ok=True
        )
        # write example input
        csv.write_csv(table, f"{get_tempdir}/example/{number}/{name}.csv")
        # write example output
        parquet.write_table(table, f"{get_tempdir}/example/{number}.{name}.parquet")

    return {
        "image.csv": [
            {
                "source_path": pathlib.Path(f"{get_tempdir}/example/0/image.csv"),
                "destination_path": pathlib.Path(
                    f"{get_tempdir}/example/0.image.parquet"
                ),
            },
        ],
        "cytoplasm.csv": [
            {
                "source_path": pathlib.Path(f"{get_tempdir}/example/1/cytoplasm.csv"),
                "destination_path": pathlib.Path(
                    f"{get_tempdir}/example/1.cytoplasm.parquet"
                ),
            }
        ],
        "cells.csv": [
            {
                "source_path": pathlib.Path(f"{get_tempdir}/example/2/cells.csv"),
                "destination_path": pathlib.Path(
                    f"{get_tempdir}/example/2.cells.parquet"
                ),
            }
        ],
        "nuclei.csv": [
            {
                "source_path": pathlib.Path(f"{get_tempdir}/example/3/nuclei.csv"),
                "destination_path": pathlib.Path(
                    f"{get_tempdir}/example/3.nuclei.parquet"
                ),
            },
            {
                "source_path": pathlib.Path(f"{get_tempdir}/example/4/nuclei.csv"),
                "destination_path": pathlib.Path(
                    f"{get_tempdir}/example/4.nuclei.parquet"
                ),
            },
        ],
    }


@pytest.fixture(name="cellprofiler_merged_examplehuman")
def fixture_cellprofiler_merged_examplehuman(
    data_dir_cellprofiler: str,
) -> pa.Table:
    """
    Fixture for manually configured merged/joined result from
    CellProfiler ExampleHuman CSV Data
    """

    def col_renames(name: str, table: pa.Table):
        """
        Helper function to rename columns appropriately
        """
        return table.rename_columns(
            [
                f"Metadata_{colname}"
                if colname in ["ImageNumber", "ObjectNumber"]
                else f"Metadata_{name}_{colname}"
                if any(name in colname for name in ["Parent_Cells", "Parent_Nuclei"])
                else f"{name}_{colname}"
                if not (colname.startswith(name) or colname.startswith("Metadata_"))
                else colname
                for colname in table.column_names
            ]
        )

    # prepare simulated merge result from convert
    image_table = csv.read_csv(
        f"{data_dir_cellprofiler}/ExampleHuman/Image.csv"
    ).select(["ImageNumber"])
    cytoplasm_table = csv.read_csv(
        f"{data_dir_cellprofiler}/ExampleHuman/Cytoplasm.csv"
    )
    cells_table = csv.read_csv(f"{data_dir_cellprofiler}/ExampleHuman/Cells.csv")
    nuclei_table = csv.read_csv(f"{data_dir_cellprofiler}/ExampleHuman/Nuclei.csv")
    image_table = col_renames(name="Image", table=image_table)
    cytoplasm_table = col_renames(name="Cytoplasm", table=cytoplasm_table)
    cells_table = col_renames(name="Cells", table=cells_table)
    nuclei_table = col_renames(name="Nuclei", table=nuclei_table)

    control_result = (
        duckdb.connect()
        .execute(
            """
            SELECT
                *
            FROM
                image_table AS image
            LEFT JOIN cytoplasm_table AS cytoplasm ON
                cytoplasm.Metadata_ImageNumber = image.Metadata_ImageNumber
            LEFT JOIN cells_table AS cells ON
                cells.Metadata_ImageNumber = cytoplasm.Metadata_ImageNumber
                AND cells.Metadata_ObjectNumber = cytoplasm.Metadata_Cytoplasm_Parent_Cells
            LEFT JOIN nuclei_table AS nuclei ON
                nuclei.Metadata_ImageNumber = cytoplasm.Metadata_ImageNumber
                AND nuclei.Metadata_ObjectNumber = cytoplasm.Metadata_Cytoplasm_Parent_Nuclei
        """
        )
        .arrow()
    )

    # reversed order column check as col removals will change index order
    cols = []
    for i, colname in reversed(list(enumerate(control_result.column_names))):
        if colname not in cols:
            cols.append(colname)
        else:
            control_result = control_result.remove_column(i)

    # inner sorted alphabetizes any columns which may not be part of custom_sort
    # outer sort provides pycytominer-specific column sort order
    control_result = control_result.select(
        sorted(sorted(control_result.column_names), key=column_sort)
    )

    return control_result


@pytest.fixture(name="cellprofiler_merged_nf1data")
def fixture_cellprofiler_merged_nf1data(
    data_dir_cellprofiler: str,
) -> pa.Table:
    """
    Fixture for manually configured merged/joined result from
    CellProfiler NF1_SchwannCell SQLite Data
    """

    control_result = (
        duckdb.connect()
        # segmented executes below are used to parameterize the sqlite source
        # without using the same parameter in the select query (unnecessary after CALL)
        .execute(
            """
            /* install and load sqlite plugin for duckdb */
            INSTALL sqlite_scanner;
            LOAD sqlite_scanner;

            /* attach sqlite db to duckdb for full table awareness */
            CALL sqlite_attach(?);
            """,
            parameters=[
                f"{data_dir_cellprofiler}/NF1_SchwannCell_data/NF1_data.sqlite"
            ],
        )
        .execute(
            """
            /* perform query on sqlite tables through duckdb */
            WITH Per_Image_Filtered AS (
                SELECT
                    ImageNumber,
                    Image_Metadata_Well,
                    Image_Metadata_Plate
                FROM Per_Image
            )
            SELECT *
            FROM Per_Image_Filtered image
            LEFT JOIN Per_Cytoplasm cytoplasm ON
                image.ImageNumber = cytoplasm.ImageNumber
            LEFT JOIN Per_Cells cells ON
                cells.ImageNumber = cytoplasm.ImageNumber
                AND cells.Cells_Number_Object_Number = cytoplasm.Cytoplasm_Parent_Cells
            LEFT JOIN Per_Nuclei nuclei ON
                nuclei.ImageNumber = cytoplasm.ImageNumber
                AND nuclei.Nuclei_Number_Object_Number = cytoplasm.Cytoplasm_Parent_OrigNuclei
        """
        )
        .arrow()
        .drop_null()
    )

    # reversed order column check as col removals will change index order
    cols = []
    for i, colname in reversed(list(enumerate(control_result.column_names))):
        if colname not in cols:
            cols.append(colname)
        else:
            control_result = control_result.remove_column(i)

    control_result = control_result.rename_columns(
        [
            colname if colname != "ImageNumber" else "Metadata_ImageNumber"
            for colname in control_result.column_names
        ]
    )

    # inner sorted alphabetizes any columns which may not be part of custom_sort
    # outer sort provides pycytominer-specific column sort order
    control_result = control_result.select(
        sorted(sorted(control_result.column_names), key=column_sort)
    )

    return control_result


@pytest.fixture(scope="session", name="s3_session")
def fixture_s3_session() -> boto3.session.Session:
    """
    Yield a mocked boto session for s3 tests.

    Referenced from:
    https://docs.getmoto.org/en/latest/docs/getting_started.html
    and
    https://docs.getmoto.org/en/latest/docs/server_mode.html#start-within-python
    """

    # start a moto server for use in testing
    server = ThreadedMotoServer()
    server.start()

    with mock_s3():
        yield boto3.session.Session()


@pytest.fixture()
def example_s3_endpoint(
    s3_session: boto3.session.Session,
    example_local_sources: Dict[str, List[Dict[str, Any]]],
) -> str:
    """
    Create an mocked bucket which includes example sources

    Referenced with changes from:
    https://docs.getmoto.org/en/latest/docs/getting_started.html
    """
    # s3 is a fixture defined above that yields a boto3 s3 client.
    # Feel free to instantiate another boto3 S3 client -- Keep note of the region though.
    endpoint_url = "http://localhost:5000"
    bucket_name = "example"

    # create s3 client
    s3_client = s3_session.client("s3", endpoint_url=endpoint_url)

    # create a bucket for content to land in
    s3_client.create_bucket(Bucket=bucket_name)

    # upload each example file to the mock bucket
    for source_path in [
        source["source_path"]
        for group in example_local_sources.values()
        for source in group
    ]:
        s3_client.upload_file(
            Filename=str(source_path),
            Bucket=bucket_name,
            # mock nested directory structure within bucket per each file's parent
            Key=f"{source_path.parent.name}/{source_path.name}",
        )

    # return endpoint url for use in testing
    return endpoint_url
