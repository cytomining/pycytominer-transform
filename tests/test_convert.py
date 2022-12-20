"""
Tests for cyctominer_transform/convert.py
"""
import io
import itertools
import pathlib
from shutil import copy
from typing import Any, Dict, List, Tuple, cast

import pyarrow as pa
import pytest
from cloudpathlib import AnyPath
from prefect_dask.task_runners import DaskTaskRunner
from pyarrow import csv, parquet

from pycytominer_transform import (  # pylint: disable=R0801
    concat_join_sources,
    concat_source_group,
    config,
    convert,
    get_join_chunks,
    get_source_filepaths,
    infer_source_datatype,
    infer_source_group_common_schema,
    join_source_chunk,
    prepend_column_name,
    read_data,
    to_parquet,
    write_parquet,
)


def test_config():
    """
    Tests config to ensure proper values
    """

    # check that we have relevant keys for each preset
    for config_preset in config.values():
        assert sorted(
            [
                "CONFIG_NAMES_COMPARTMENTS",
                "CONFIG_NAMES_METADATA",
                "CONFIG_IDENTIFYING_COLUMNS",
                "CONFIG_CHUNK_SIZE",
                "CONFIG_CHUNK_COLUMNS",
                "CONFIG_JOINS",
            ]
        ) == sorted(config_preset.keys())


def test_get_source_filepaths(get_tempdir: str, data_dir_cellprofiler: str):
    """
    Tests get_source_filepaths
    """

    # test that no sources raises an exception
    empty_dir = pathlib.Path(f"{get_tempdir}/temp")
    empty_dir.mkdir(parents=True, exist_ok=True)
    with pytest.raises(Exception):
        single_dir_result = get_source_filepaths.fn(
            path=empty_dir,
            targets=["image", "cells", "nuclei", "cytoplasm"],
        )

    single_dir_result = get_source_filepaths.fn(
        path=pathlib.Path(f"{data_dir_cellprofiler}/ExampleHuman"),
        targets=["cells"],
    )
    # test that the single dir structure includes 1 unique key (for cells)
    assert len(set(single_dir_result.keys())) == 1

    single_dir_result = get_source_filepaths.fn(
        path=pathlib.Path(f"{data_dir_cellprofiler}/ExampleHuman"),
        targets=["image", "cells", "nuclei", "cytoplasm"],
    )
    # test that the single dir structure includes 4 unique keys
    assert len(set(single_dir_result.keys())) == 4


def test_read_data(get_tempdir: str):
    """
    Tests read_csv
    """

    # valid csv data (last row invalid rowcount)
    data = io.BytesIO("col_1,col_2,col_3,col_4\n1,0.1,a,True\n2,0.2,b,False".encode())
    table = csv.read_csv(input_file=data)
    destination = f"{get_tempdir}/sample.csv"

    csv.write_csv(data=table, output_file=destination)

    result = read_data.fn(source={"source_path": destination})

    assert isinstance(result, dict)
    assert sorted(list(result.keys())) == sorted(["source_path", "table"])
    assert result["table"].schema.equals(table.schema)
    assert result["table"].shape == table.shape

    # cover invalid number of rows read ignores
    destination_err = f"{get_tempdir}/sample.csv"
    with open(destination_err, "w", encoding="utf-8") as wfile:
        wfile.write("col_1,col_2,col_3,col_4\n1,0.1,a,True\n2,0.2,b,False,1")

    assert isinstance(read_data.fn(source={"source_path": destination_err}), Dict)


def test_prepend_column_name():
    """
    Tests prepend_column_name
    """

    # example cytoplasm csv table run
    result = prepend_column_name.fn(
        source={
            "table": pa.Table.from_pydict(
                {
                    "ImageNumber": [1, 2, 3, 1, 2, 3],
                    "ObjectNumber": [1, 1, 1, 2, 2, 2],
                    "Parent_Cells": [1, 1, 1, 2, 2, 2],
                    "Parent_Nuclei": [1, 1, 1, 2, 2, 2],
                    "field1": ["foo", "bar", "baz", "foo", "bar", "baz"],
                    "field2": [True, False, True, True, False, True],
                }
            )
        },
        source_group_name="Cytoplasm.csv",
        identifying_columns=[
            "ImageNumber",
            "ObjectNumber",
            "Parent_Cells",
            "Parent_Nuclei",
        ],
        metadata=["image"],
        targets=["image", "cells", "nuclei", "cytoplasm"],
    )

    # compare the results with what's expected for column names
    assert result["table"].column_names == [
        "Metadata_ImageNumber",
        "Metadata_ObjectNumber",
        "Metadata_Cytoplasm_Parent_Cells",
        "Metadata_Cytoplasm_Parent_Nuclei",
        "Cytoplasm_field1",
        "Cytoplasm_field2",
    ]

    # example cells sqlite table run
    result = prepend_column_name.fn(
        source={
            "table": pa.Table.from_pydict(
                {
                    "ImageNumber": [1, 2, 3, 1, 2, 3],
                    "Cells_Number_Object_Number": [1, 1, 1, 2, 2, 2],
                    "Parent_OrigNuclei": [1, 1, 1, 2, 2, 2],
                    "field1": ["foo", "bar", "baz", "foo", "bar", "baz"],
                    "field2": [True, False, True, True, False, True],
                }
            )
        },
        source_group_name="Per_Cells.sqlite",
        identifying_columns=[
            "ImageNumber",
            "Parent_Cells",
            "Parent_OrigNuclei",
        ],
        metadata=["image"],
        targets=["image", "cells", "nuclei", "cytoplasm"],
    )

    # compare the results with what's expected for column names
    assert result["table"].column_names == [
        "Metadata_ImageNumber",
        "Cells_Number_Object_Number",
        "Metadata_Cells_Parent_OrigNuclei",
        "Cells_field1",
        "Cells_field2",
    ]


def test_concat_source_group(
    get_tempdir: str,
    example_tables: Tuple[pa.Table, ...],
    example_local_sources: Dict[str, List[Dict[str, Any]]],
):
    """
    Tests concat_source_group
    """

    _, _, _, table_nuclei_1, table_nuclei_2 = example_tables

    # simulate concat
    concat_table = pa.concat_tables([table_nuclei_1, table_nuclei_2])

    result = concat_source_group.fn(
        source_group=example_local_sources["nuclei.csv"],
        dest_path=get_tempdir,
        common_schema=table_nuclei_1.schema,
    )
    assert len(result) == 1
    assert parquet.read_schema(result[0]["destination_path"]) == concat_table.schema
    assert (
        parquet.read_metadata(result[0]["destination_path"]).num_rows,
        parquet.read_metadata(result[0]["destination_path"]).num_columns,
    ) == concat_table.shape

    # add a mismatching source to group
    mismatching_table = pa.Table.from_pydict(
        {
            "color": pa.array(["blue", "red", "green", "orange"]),
        }
    )
    pathlib.Path(f"{get_tempdir}/example/5").mkdir(parents=True, exist_ok=True)
    csv.write_csv(mismatching_table, f"{get_tempdir}/example/5/nuclei.csv")
    parquet.write_table(mismatching_table, f"{get_tempdir}/example/5.nuclei.parquet")
    example_local_sources["nuclei.csv"].append(
        {
            "source_path": pathlib.Path(f"{get_tempdir}/example/5/nuclei.csv"),
            "destination_path": pathlib.Path(f"{get_tempdir}/example/5.nuclei.parquet"),
        }
    )

    with pytest.raises(Exception):
        concat_source_group.fn(
            source_group=example_local_sources["nuclei.csv"],
            dest_path=get_tempdir,
        )


def test_get_join_chunks(get_tempdir: str):
    """
    Tests get_join_chunks
    """

    # form test path
    test_path = f"{get_tempdir}/merge_chunks_test.parquet"

    # write test data to file
    parquet.write_table(
        table=pa.Table.from_pydict(
            {
                "id1": [1, 2, 3, 1, 2, 3],
                "id2": ["a", "a", "a", "b", "b", "b"],
                "field1": ["foo", "bar", "baz", "foo", "bar", "baz"],
                "field2": [True, False, True, True, False, True],
            }
        ),
        where=test_path,
    )

    result = get_join_chunks.fn(
        sources={"merge_chunks_test.parquet": [{"destination_path": test_path}]},
        metadata=["merge_chunks_test"],
        chunk_columns=["id1", "id2"],
        chunk_size=2,
    )

    # test that we have 3 chunks of merge columns
    assert len(result) == 3
    # test that we have only the columns we specified
    assert set(
        itertools.chain(
            *[list(chunk_item.keys()) for chunk in result for chunk_item in chunk]
        )
    ) == {"id1", "id2"}


def test_join_source_chunk(get_tempdir: str):
    """
    Tests get_join_chunks
    """

    # form test path a
    test_path_a = f"{get_tempdir}/example_a_merged.parquet"
    # form test path b
    test_path_b = f"{get_tempdir}/example_b_merged.parquet"

    # write test data to file
    parquet.write_table(
        table=pa.Table.from_pydict(
            {
                "id1": [1, 2, 3, 1, 2, 3],
                "id2": ["a", "a", "a", "b", "b", "b"],
                "field1": ["foo", "bar", "baz", "foo", "bar", "baz"],
            }
        ),
        where=test_path_a,
    )
    # write test data to file
    parquet.write_table(
        table=pa.Table.from_pydict(
            {
                "id1": [1, 2, 3, 1, 2, 3],
                "id2": ["a", "a", "a", "b", "b", "b"],
                "field2": [True, False, True, True, False, True],
            }
        ),
        where=test_path_b,
    )

    result = join_source_chunk.fn(
        sources={
            "example_a": [{"destination_path": test_path_a}],
            "example_b": [{"destination_path": test_path_b}],
        },
        dest_path=f"{get_tempdir}/destination.parquet",
        joins="""
            SELECT *
            FROM read_parquet('example_a_merged.parquet') as example_a
            JOIN read_parquet('example_b_merged.parquet') as example_b ON
                example_b.id1 = example_a.id1
                AND example_b.id2 = example_a.id2
        """,
        join_group=[{"id1": 1, "id2": "a"}, {"id1": 2, "id2": "a"}],
        drop_null=True,
    )

    assert isinstance(result, str)
    result_table = parquet.read_table(source=result)
    assert result_table.equals(
        other=pa.Table.from_pydict(
            {
                "id1": [1, 2],
                "id2": ["a", "a"],
                "field1": ["foo", "bar"],
                "field2": [True, False],
            },
            # use schema from result as a reference for col order
            schema=result_table.schema,
        )
    )


def test_concat_join_sources(get_tempdir: str):
    """
    Tests concat_join_sources
    """

    # create a test dir
    pathlib.Path(f"{get_tempdir}/concat_join/").mkdir(exist_ok=True)

    # form test paths
    test_path_a = f"{get_tempdir}/concat_join/join_chunks_test_a.parquet"
    test_path_b = f"{get_tempdir}/concat_join/join_chunks_test_b.parquet"
    test_path_a_join_chunk = f"{get_tempdir}/join_chunks_test_a.parquet"
    test_path_b_join_chunk = f"{get_tempdir}/join_chunks_test_b.parquet"

    # form test data
    test_table_a = pa.Table.from_pydict(
        {
            "id1": [
                1,
                2,
                3,
            ],
            "id2": [
                "a",
                "a",
                "a",
            ],
            "field1": [
                "foo",
                "bar",
                "baz",
            ],
            "field2": [
                True,
                False,
                True,
            ],
        }
    )
    test_table_b = pa.Table.from_pydict(
        {
            "id1": [1, 2, 3],
            "id2": ["b", "b", "b"],
            "field1": ["foo", "bar", "baz"],
            "field2": [True, False, True],
        }
    )

    # write test data to file
    parquet.write_table(
        table=test_table_a,
        where=test_path_a,
    )
    parquet.write_table(
        table=test_table_b,
        where=test_path_b,
    )

    # copy the files for testing purposes
    copy(test_path_a, test_path_a_join_chunk)
    copy(test_path_b, test_path_b_join_chunk)

    result = concat_join_sources.fn(
        dest_path=f"{get_tempdir}/example_concat_join.parquet",
        join_sources=[test_path_a_join_chunk, test_path_b_join_chunk],
        sources={
            "join_chunks_test_a.parquet": [{"destination_path": test_path_a}],
            "join_chunks_test_b.parquet": [{"destination_path": test_path_b}],
        },
    )

    # ensure the concatted result is what we expect
    assert parquet.read_table(source=result).equals(
        pa.concat_tables(tables=[test_table_a, test_table_b])
    )

    # ensure the test paths provided via sources were removed (unlinked)
    assert (pathlib.Path(test_path_a).exists() is False) and (
        pathlib.Path(test_path_a).exists() is False
    )


def test_write_parquet(get_tempdir: str):
    """
    Tests write_parquet
    """

    table = pa.Table.from_pydict(
        {"color": pa.array(["blue", "red", "green", "orange"])}
    )

    result = write_parquet.fn(
        source={
            "source_path": AnyPath(f"{get_tempdir}/example/colors.csv"),
            "table": table,
        },
        dest_path=f"{get_tempdir}/new_path",
        unique_name=False,
    )
    result_table = parquet.read_table(result["destination_path"])

    assert sorted(list(result.keys())) == sorted(["destination_path", "source_path"])
    assert result_table.schema == table.schema
    assert result_table.shape == table.shape
    result = write_parquet.fn(
        source={
            "source_path": pathlib.Path(f"{get_tempdir}/example/colors.csv"),
            "table": table,
        },
        dest_path=f"{get_tempdir}/new_path",
        unique_name=True,
    )
    assert (
        result["destination_path"].stem
        != pathlib.Path(f"{get_tempdir}/example/colors.csv").stem
    )


def test_infer_source_datatype():
    """
    Tests infer_source_datatype
    """

    data = {
        "sample_1.csv": [{"source_path": "stub"}],
        "sample_2.CSV": [{"source_path": "stub"}],
    }
    assert infer_source_datatype.fn(sources=data) == "csv"
    with pytest.raises(Exception):
        infer_source_datatype.fn(sources=data, source_datatype="parquet")

    data["sample_3.parquet"] = [{"source_path": "stub"}]
    assert (
        infer_source_datatype.fn(sources=data, source_datatype="parquet") == "parquet"
    )
    with pytest.raises(Exception):
        infer_source_datatype.fn(sources=data)


def test_to_parquet(
    get_tempdir: str, example_local_sources: Dict[str, List[Dict[str, Any]]]
):
    """
    Tests to_parquet
    """

    flattened_example_sources = list(
        itertools.chain(*list(example_local_sources.values()))
    )

    result = to_parquet(
        source_path=str(example_local_sources["image.csv"][0]["source_path"].parent),
        dest_path=get_tempdir,
        source_datatype=None,
        compartments=["cytoplasm", "cells", "nuclei"],
        metadata=["image"],
        identifying_columns=["imagenumber"],
        concat=False,
        join=False,
        joins=None,
        chunk_columns=None,
        chunk_size=None,
        infer_common_schema=False,
        drop_null=True,
    )

    flattened_results = list(itertools.chain(*list(result.values())))
    for i, flattened_result in enumerate(flattened_results):
        parquet_result = parquet.read_table(source=flattened_result["destination_path"])
        csv_source = csv.read_csv(flattened_example_sources[i]["source_path"])
        assert parquet_result.schema.equals(csv_source.schema)
        assert parquet_result.shape == csv_source.shape


def test_convert_s3_path(
    get_tempdir: str,
    example_local_sources: Dict[str, List[Dict[str, Any]]],
    example_s3_endpoint: str,
):
    """
    Tests convert with mocked s3 object storage endpoint
    """

    multi_dir_nonconcat_s3_result = convert(
        source_path="s3://example/",
        dest_path=f"{get_tempdir}/s3_test",
        dest_datatype="parquet",
        concat=False,
        join=False,
        joins=None,
        source_datatype="csv",
        compartments=["cytoplasm", "cells"],
        metadata=["image"],
        identifying_columns=["imagenumber"],
        # endpoint_url here will be used with cloudpathlib client(**kwargs)
        endpoint_url=example_s3_endpoint,
    )

    # compare each of the results using files from the source
    for control_path, test_path in zip(
        [
            source["destination_path"]
            for group in cast(Dict, multi_dir_nonconcat_s3_result).values()
            for source in group
        ],
        [
            source["destination_path"]
            for group in example_local_sources.values()
            for source in group
        ],
    ):
        parquet_control = parquet.read_table(control_path)
        parquet_result = parquet.read_table(test_path, schema=parquet_control.schema)

        assert parquet_result.schema.equals(parquet_control.schema)
        assert parquet_result.shape == parquet_control.shape


def test_infer_source_group_common_schema(
    example_local_sources: Dict[str, List[Dict[str, Any]]],
    example_tables: Tuple[pa.Table, ...],
):
    """
    Tests infer_source_group_common_schema
    """
    _, _, _, table_nuclei_1, _ = example_tables

    result = infer_source_group_common_schema.fn(
        source_group=example_local_sources["nuclei.csv"],
    )

    assert table_nuclei_1.schema.equals(pa.schema(result))


def test_convert_cytominerdatabase_csv(
    get_tempdir: str,
    data_dirs_cytominerdatabase: List[str],
    pycytominer_merge_single_cells_parquet: List[str],
):
    """
    Tests convert with cytominerdatabase csvs and processed
    csvs from cytominer-database to pycytominer merge_single_cells
    """

    for cytominerdatabase_dir, pycytominer_merge_dir in zip(
        data_dirs_cytominerdatabase, pycytominer_merge_single_cells_parquet
    ):
        # load control table, dropping tablenumber
        # and unlabeled objectnumber (no compartment specified)
        control_table = parquet.read_table(source=pycytominer_merge_dir).drop(
            [
                # tablenumber is not implemented within pycytominer-transform
                "Metadata_TableNumber",
                # objectnumber references are provided via cytoplasm parent object joins
                "Metadata_ObjectNumber",
                "Metadata_ObjectNumber_cells",
                "__index_level_0__",
            ]
        )
        # rename column to account for minor difference in processing
        control_table = control_table.rename_columns(
            [
                # rename based on compartment prefix name within pycytominer-transform format
                col if col != "Cells_Parent_Nuclei" else "Metadata_Cells_Parent_Nuclei"
                for col in control_table.schema.names
            ]
        )

        # load test table by reading parquet-based output from convert
        test_table = parquet.read_table(
            source=convert(
                source_path=cytominerdatabase_dir,
                dest_path=(
                    f"{get_tempdir}/{pathlib.Path(cytominerdatabase_dir).name}.test_table.parquet"
                ),
                dest_datatype="parquet",
                source_datatype="csv",
                merge=True,
                drop_null=False,
            ),
            schema=control_table.schema,
        )

        assert control_table.schema.equals(test_table.schema)
        assert control_table.num_columns == test_table.num_columns
        assert control_table.num_rows <= test_table.num_rows


def test_convert_cellprofiler_sqlite(
    get_tempdir: str, data_dir_cellprofiler: str, cellprofiler_merged_nf1data: pa.Table
):
    """
    Tests convert with cellprofiler sqlite exports
    """

    control_result = cellprofiler_merged_nf1data

    test_result = parquet.read_table(
        convert(
            source_path=(
                f"{data_dir_cellprofiler}/NF1_SchwannCell_data/NF1_data.sqlite"
            ),
            dest_path=f"{get_tempdir}/NF1_data.parquet",
            dest_datatype="parquet",
            source_datatype="sqlite",
            preset="cellprofiler_sqlite",
        )
    )

    # sort all values by the same columns
    # we do this due to the potential for inconsistently ordered results
    control_result = control_result.sort_by(
        [(colname, "ascending") for colname in control_result.column_names]
    )
    test_result = test_result.sort_by(
        [(colname, "ascending") for colname in test_result.column_names]
    )

    assert test_result.shape == control_result.shape
    assert test_result.equals(control_result)


def test_convert_cellprofiler_csv(
    get_tempdir: str,
    data_dir_cellprofiler: str,
    cellprofiler_merged_examplehuman: pa.Table,
):
    """
    Tests convert

    Note: uses default prefect task_runner from convert
    Dedicated tests for prefect-dask runner elsewhere.
    """

    # expects exception due to no compartments
    with pytest.raises(Exception):
        convert(
            source_path=f"{data_dir_cellprofiler}/ExampleHuman",
            dest_path=f"{get_tempdir}/ExampleHuman",
            dest_datatype="parquet",
            source_datatype="csv",
            compartments=[],
        )

    control_result = cellprofiler_merged_examplehuman

    test_result = parquet.read_table(
        convert(
            source_path=f"{data_dir_cellprofiler}/ExampleHuman",
            dest_path=f"{get_tempdir}/ExampleHuman",
            dest_datatype="parquet",
            source_datatype="csv",
            preset="cellprofiler_csv",
        )
    )

    # sort all values by the same columns
    # we do this due to the potential for inconsistently ordered results
    control_result = control_result.sort_by(
        [(colname, "ascending") for colname in control_result.column_names]
    )
    test_result = test_result.sort_by(
        [(colname, "ascending") for colname in test_result.column_names]
    )

    assert test_result.shape == control_result.shape
    assert test_result.equals(control_result)


def test_convert_dask_cellprofiler_csv(
    get_tempdir: str,
    data_dir_cellprofiler: str,
    cellprofiler_merged_examplehuman: pa.Table,
):
    """
    Tests convert

    Note: dedicated test for prefect-dask runner
    """

    control_result = cellprofiler_merged_examplehuman

    test_result = parquet.read_table(
        convert(
            source_path=f"{data_dir_cellprofiler}/ExampleHuman",
            dest_path=f"{get_tempdir}/ExampleHuman",
            dest_datatype="parquet",
            source_datatype="csv",
            preset="cellprofiler_csv",
            task_runner=DaskTaskRunner,
        )
    )

    # sort all values by the same columns
    # we do this due to the potential for inconsistently ordered results
    control_result = control_result.sort_by(
        [(colname, "ascending") for colname in control_result.column_names]
    )
    test_result = test_result.sort_by(
        [(colname, "ascending") for colname in test_result.column_names]
    )

    assert test_result.shape == control_result.shape
    assert test_result.equals(control_result)
