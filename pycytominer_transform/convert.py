"""
pycytominer-transform: convert - transforming data for use with pyctyominer.
"""

import itertools
import os
import pathlib
import uuid
from typing import Any, Dict, List, Literal, Optional, Tuple, Union, cast

import duckdb
import pyarrow as pa
from cloudpathlib import AnyPath
from prefect import flow, task, unmapped
from prefect.futures import PrefectFuture
from prefect.task_runners import BaseTaskRunner, ConcurrentTaskRunner
from pyarrow import csv, parquet

from pycytominer_transform.exceptions import SchemaException
from pycytominer_transform.presets import config
from pycytominer_transform.sources import gather_sources
from pycytominer_transform.utils import column_sort, duckdb_with_sqlite


@task
def read_data(source: Dict[str, Any]) -> Dict[str, Any]:
    """
    Read data from source.

    Args:
        source: Dict[str, Any]:
            Data containing filepath to csv file

    Returns:
        source: Dict[str, Any]
            Updated source (Dict[str, Any]) with source data in-memory
    """

    if AnyPath(source["source_path"]).suffix == ".csv":  # pylint: disable=no-member
        # define invalid row handler for rows which may be
        # somehow erroneous. See below for more details:
        # https://arrow.apache.org/docs/python/generated/pyarrow.csv.ParseOptions.html#pyarrow-csv-parseoptions
        def skip_erroneous_colcount(row):
            return "skip" if row.actual_columns != row.expected_columns else "error"

        # setup parse options
        parse_options = csv.ParseOptions(invalid_row_handler=skip_erroneous_colcount)

        # read csv using pyarrow lib and attach table data to source
        source["table"] = csv.read_csv(
            input_file=source["source_path"],
            parse_options=parse_options,
        )

    if AnyPath(source["source_path"]).suffix == ".sqlite":  # pylint: disable=no-member

        source["table"] = (
            duckdb_with_sqlite()
            .execute(
                """
                /* perform query on sqlite_master table for metadata on tables */
                SELECT * from sqlite_scan(?, ?)
                """,
                parameters=[str(source["source_path"]), str(source["table_name"])],
            )
            .arrow()
        )

    return source


@task
def prepend_column_name(
    source: Dict[str, Any],
    source_group_name: str,
    identifying_columns: Union[List[str], Tuple[str, ...]],
    metadata: Union[List[str], Tuple[str, ...]],
    targets: List[str],
) -> Dict[str, Any]:
    """
    Rename columns using the source group name, avoiding identifying columns.

    Args:
        source: Dict[str, Any]:
            Individual data source source which includes meta about source
            as well as Arrow table with data.
        source_group_name: str:
            Name of data source source group (for common compartments, etc).
        identifying_columns: Union[List[str], Tuple[str, ...]]:
            Column names which are used as ID's and as a result need to be
            ignored with regards to renaming.
        metadata: Union[List[str], Tuple[str, ...]]:
            List of source data names which are used as metadata
        targets: List[str]:
            List of source data names which are used as compartments

    Returns:
        Dict[str, Any]
            Updated source which includes the updated table column names
    """

    # stem of source group name
    # for example:
    #   targets: ['cytoplasm']
    #   source_group_name: 'Per_Cytoplasm.sqlite'
    #   source_group_name_stem: 'Cytoplasm'
    source_group_name_stem = targets[
        # return first result from generator below as index to targets
        next(
            i
            for i, val in enumerate(targets)
            # compare if value from targets in source_group_name stem
            if val.lower() in str(pathlib.Path(source_group_name).stem).lower()
        )
        # capitalize the result
    ].capitalize()

    # capture updated column names as new variable
    updated_column_names = []

    for column_name in source["table"].column_names:
        # if-condition for prepending source_group_name_stem to column name
        # where colname is not an identifying column
        # and where the column is not already prepended with source_group_name_stem
        # for example:
        #   source_group_name_stem: 'Cells'
        #   column_name: 'AreaShape_Area'
        #   updated_column_name: 'Cells_AreaShape_Area'
        if column_name not in identifying_columns and not column_name.startswith(
            source_group_name_stem
        ):
            updated_column_names.append(f"{source_group_name_stem}_{column_name}")

        # if-condition for prepending 'Metadata' and source_group_name_stem to column name
        # where colname is an identifying column
        # and where the colname does not already start with 'Metadata_'
        # and colname not in metadata list
        # and colname does not include 'ObjectNumber'
        # for example:
        #   source_group_name_stem: 'Cells'
        #   column_name: 'Parent_Nuclei'
        #   updated_column_name: 'Metadata_Cells_Parent_Nuclei'
        elif (
            column_name in identifying_columns
            and not column_name.startswith("Metadata_")
            and not any(item.capitalize() in column_name for item in metadata)
            and not "ObjectNumber" in column_name
        ):

            updated_column_names.append(
                f"Metadata_{source_group_name_stem}_{column_name}"
            )
        # if-condition for prepending 'Metadata' to column name
        # where colname doesn't already start with 'Metadata_'
        # and colname is in identifying columns list
        # for example:
        #   column_name: 'ObjectNumber'
        #   updated_column_name: 'Metadata_ObjectNumber'
        elif (
            not column_name.startswith("Metadata_")
            and column_name in identifying_columns
        ):
            updated_column_names.append(f"Metadata_{column_name}")

        # else we add the existing colname to the updated list as-is
        else:
            updated_column_names.append(column_name)

    # perform table column name updates
    source["table"] = source["table"].rename_columns(updated_column_names)

    return source


@task
def concat_source_group(
    source_group: List[Dict[str, Any]],
    dest_path: str = ".",
    common_schema: Optional[List[Tuple[str, str]]] = None,
) -> List[Dict[str, Any]]:
    """
    Concatenate group of sources together as unified dataset.

    For a reference to data concatenation within Arrow see the following:
    https://arrow.apache.org/docs/python/generated/pyarrow.concat_tables.html

    Args:
        source_group: List[Dict[str, Any]]:
            Data structure containing grouped data for concatenation.
        dest_path: Optional[str] (Default value = None)
            Optional destination path for concatenated sources.
        common_schema: List[Tuple[str, str]] (Default value = None)
            Common schema to use for concatenation amongst arrow tables
            which may have slightly different but compatible schema.

    Returns:
        List[Dict[str, Any]]
            Updated dictionary containing concatenated sources.
    """

    # if we have nothing to concat, return the source group
    if len(source_group) < 2:
        return source_group

    # check whether we already have a file as dest_path
    if pathlib.Path(dest_path).is_file():
        pathlib.Path(dest_path).unlink(missing_ok=True)

    concatted = [
        {
            # source path becomes parent's parent dir with the same filename
            "source_path": pathlib.Path(
                (
                    f"{source_group[0]['source_path'].parent.parent}"
                    f"/{source_group[0]['source_path'].stem}"
                )
            )
        }
    ]

    destination_path = pathlib.Path(
        (f"{dest_path}/" f"{source_group[0]['source_path'].stem}" ".parquet")
    )

    # if there's already a file remove it
    destination_path.unlink(missing_ok=True)

    if common_schema is not None:
        writer_schema = pa.schema(common_schema)

    # build a parquet file writer which will be used to append files
    # as a single concatted parquet file, referencing the first file's schema
    # (all must be the same schema)
    with parquet.ParquetWriter(str(destination_path), writer_schema) as writer:

        for table in [source["destination_path"] for source in source_group]:
            # if we haven't inferred the common schema
            # check that our file matches the expected schema, otherwise raise an error
            if common_schema is None and not writer_schema.equals(
                parquet.read_schema(table)
            ):
                raise SchemaException(
                    (
                        f"Detected mismatching schema for target concatenation group members:"
                        f" {str(source_group[0]['destination_path'])} and {str(table)}"
                    )
                )

            # read the file from the list and write to the concatted parquet file
            # note: we pass column order based on the first chunk file to help ensure schema
            # compatibility for the writer
            writer.write_table(parquet.read_table(table, schema=writer_schema))
            # remove the file which was written in the concatted parquet file (we no longer need it)
            pathlib.Path(table).unlink()

    # return the concatted parquet filename
    concatted[0]["destination_path"] = destination_path
    return concatted


@task
def write_parquet(
    source: Dict[str, Any], dest_path: str, unique_name: bool = False
) -> Dict[str, Any]:
    """
    Write parquet data using in-memory data.

    Args:
        source: Dict:
            Dictionary including in-memory data which will be written to parquet.
        dest_path: str:
            Destination path to write the parquet file to.
        unique_name: bool:  (Default value = False)
            Determines whether a unique name is necessary for the file.

    Returns:
        Dict[str, Any]
            Updated dictionary containing the destination path where parquet file
            was written.
    """

    # unlink the file if it exists
    if pathlib.Path(dest_path).is_file():
        pathlib.Path(dest_path).unlink()

    # make the dest_path dir if it doesn't already exist
    pathlib.Path(dest_path).mkdir(parents=True, exist_ok=True)

    # build a default destination path for the parquet output
    stub_name = str(source["source_path"].stem)
    if "table_name" in source.keys():
        stub_name = f"{source['table_name']}"
    destination_path = pathlib.Path(f"{dest_path}/{stub_name}.parquet")

    # build unique names to avoid overlaps
    if unique_name:
        destination_path = pathlib.Path(
            (
                f"{dest_path}/{str(source['source_path'].parent.name)}"
                f".{str(source['source_path'].stem)}.parquet"
            )
        )

    # write the table to destination path output
    parquet.write_table(table=source["table"], where=destination_path)

    # unset table
    del source["table"]

    # update the source to include the destination path
    source["destination_path"] = destination_path

    return source


@task
def get_join_chunks(
    sources: Dict[str, List[Dict[str, Any]]],
    metadata: Union[List[str], Tuple[str, ...]],
    chunk_columns: Union[List[str], Tuple[str, ...]],
    chunk_size: int,
) -> List[List[Dict[str, Any]]]:
    """
    Build groups of join keys for later join operations

    Args:
        sources: Dict[List[Dict[str, Any]]]:
            Grouped datasets of files which will be used by other functions.
        metadata: Union[List[str], Tuple[str, ...]]:
            List of source data names which are used as metadata
        chunk_columns: Union[List[str], Tuple[str, ...]]:
            Column names which appear in all compartments to use when performing join
        chunk_size: int:
            Size of join chunks which is used to limit data size during join ops

    Returns:
        List[List[Dict[str, Any]]]]:
            A list of lists with at most chunk size length that contain join keys
    """

    # fetch the compartment concat result as the basis for join groups
    for key, source in sources.items():
        if any(name.lower() in pathlib.Path(key).stem.lower() for name in metadata):
            first_result = source
            break

    # gather the workflow result for basis if it's not yet returned
    basis = (
        first_result.result()
        if isinstance(first_result, PrefectFuture)
        else first_result
    )

    # read only the table's chunk_columns
    join_column_rows = parquet.read_table(
        source=basis[0]["destination_path"], columns=chunk_columns
    ).to_pylist()

    # build and return the chunked join column rows
    return [
        join_column_rows[i : i + chunk_size]
        for i in range(0, len(join_column_rows), chunk_size)
    ]


@task
def join_source_chunk(
    sources: Dict[str, List[Dict[str, Any]]],
    dest_path: str,
    joins: str,
    join_group: List[Dict[str, Any]],
    drop_null: bool,
) -> str:
    """
    Join sources based on join group keys (group of specific join column values)

    Args:
        sources: Dict[str, List[Dict[str, Any]]]:
            Grouped datasets of files which will be used by other functions.
            Includes the metadata concerning location of actual data.
        dest_path: str:
            Destination path to write file-based content.
        joins: str:
            DuckDB-compatible SQL which will be used to perform the join
            operations using the join_group keys as a reference.
        join_group: List[Dict[str, Any]]:
            Group of joinable keys to be used as "chunked" filter
            of overall dataset.
        drop_null: bool:
            Whether to drop rows with null values within the resulting
            joined data.


    Returns:
        str
            Path to joined file which is created as a result of this function.
    """

    # replace with real location of sources for join sql
    for key, val in sources.items():
        if pathlib.Path(key).stem.lower() in joins.lower():
            joins = joins.replace(
                str(pathlib.Path(val[0]["destination_path"]).name.lower()),
                str(val[0]["destination_path"]),
            )

    # update the join groups to include unique values per table
    updated_join_group = []
    for key in sources.keys():
        updated_join_group.extend(
            [
                {
                    f"{str(pathlib.Path(key).stem)}.{join_key}": val
                    for join_key, val in chunk.items()
                }
                for chunk in join_group
            ]
        )

    # form where clause for sql joins to filter the results
    joins += (
        "WHERE ("
        + ") OR (".join(
            [
                " AND ".join(
                    [
                        # create groups of join column filters where values always
                        # are expected to equal those within the join_group together
                        f"{join_column} = {join_column_value}"
                        if not isinstance(join_column_value, str)
                        # account for string values
                        else (f"{join_column} = " f"'{join_column_value}'")
                        for join_column, join_column_value in chunk.items()
                    ]
                )
                for chunk in updated_join_group
            ]
        )
        + ")"
    )

    # perform compartment joins
    result = duckdb.connect().execute(joins).arrow()

    if drop_null:
        result = result.drop_null()

    # account for duplicate column names from joins
    cols = []
    # reversed order column check as col removals will change index order
    for i, colname in reversed(list(enumerate(result.column_names))):
        if colname not in cols:
            cols.append(colname)
        else:
            result = result.remove_column(i)

    # inner sorted alphabetizes any columns which may not be part of custom_sort
    # outer sort provides pycytominer-specific column sort order
    result = result.select(sorted(sorted(result.column_names), key=column_sort))

    result_file_path = (
        # store the result in the parent of the dest_path
        f"{str(pathlib.Path(dest_path).parent)}/"
        # use the dest_path stem in the name
        f"{str(pathlib.Path(dest_path).stem)}-"
        # give the join chunk result a unique to arbitrarily
        # differentiate from other chunk groups which are mapped
        # and before they are brought together as one dataset
        f"{str(uuid.uuid4().hex)}.parquet"
    )

    # write the result
    parquet.write_table(
        table=result,
        where=result_file_path,
    )

    return result_file_path


@task
def concat_join_sources(
    sources: Dict[str, List[Dict[str, Any]]],
    dest_path: str,
    join_sources: List[str],
) -> str:
    """
    Concatenate join sources from parquet-based chunks.

    For a reference to data concatenation within Arrow see the following:
    https://arrow.apache.org/docs/python/generated/pyarrow.concat_tables.html

    Args:
        sources: Dict[str, List[Dict[str, Any]]]:
            Grouped datasets of files which will be used by other functions.
            Includes the metadata concerning location of actual data.
        dest_path: str:
            Destination path to write file-based content.
        join_sources: List[str]:
            List of local filepath destination for join source chunks
            which will be concatenated.

    Returns:
        str
            Path to concatenated file which is created as a result of this function.
    """

    # remove the unjoined concatted compartments to prepare final dest_path usage
    # (we now have joined results)
    flattened_sources = list(itertools.chain(*list(sources.values())))
    for source in flattened_sources:
        pathlib.Path(source["destination_path"]).unlink(missing_ok=True)

    # remove dir if we have it
    if pathlib.Path(dest_path).is_dir():
        pathlib.Path(dest_path).rmdir()

    # also remove any pre-existing files which may already be at file destination
    pathlib.Path(dest_path).unlink(missing_ok=True)

    # write the concatted result as a parquet file
    parquet.write_table(
        table=pa.concat_tables(
            tables=[parquet.read_table(table_path) for table_path in join_sources]
        ),
        where=dest_path,
    )

    # remove join chunks as we have the final result
    for table_path in join_sources:
        pathlib.Path(table_path).unlink()

    # return modified sources format to indicate the final result
    # and retain the other source data for reference as needed
    return dest_path


@task
def infer_source_group_common_schema(
    source_group: List[Dict[str, Any]]
) -> List[Tuple[str, str]]:
    """
    Infers a common schema for group of parquet files which may have
    similar but slightly different schema or data. Intended to assist with
    data concatenation and other operations.

    Args:
        source_group: List[Dict[str, Any]]:
            Group of one or more data sources which includes metadata about
            path to parquet data.

    Returns:
        List[Tuple[str, str]]
            A list of tuples which includes column name and PyArrow datatype.
            This data will later be used as the basis for forming a PyArrow schema.
    """

    # read first file for basis of schema and column order for all others
    common_schema = parquet.read_schema(source_group[0]["destination_path"])

    # infer common basis of schema and column order for all others
    for schema in [
        parquet.read_schema(source["destination_path"]) for source in source_group
    ]:

        # account for completely equal schema
        if schema.equals(common_schema):
            continue

        # gather field names from schema
        schema_field_names = [item.name for item in schema]

        # reversed enumeration because removing indexes ascendingly changes schema field order
        for index, field in reversed(list(enumerate(common_schema))):

            # check whether field name is contained within writer basis, remove if not
            # note: because this only checks for naming, we defer to initially detected type
            if field.name not in schema_field_names:

                common_schema = common_schema.remove(index)

            # check if we have a nulltype and non-nulltype conflict, deferring to non-nulltype
            elif pa.types.is_null(field.type) and not pa.types.is_null(
                schema.field(field.name).type
            ):
                common_schema = common_schema.set(
                    index, field.with_type(schema.field(field.name).type)
                )

            # check if we have an integer to float challenge and enable later casting
            elif pa.types.is_integer(field.type) and pa.types.is_floating(
                schema.field(field.name).type
            ):
                common_schema = common_schema.set(index, field.with_type(pa.float64()))

    if len(list(common_schema.names)) == 0:
        raise SchemaException(
            (
                "No common schema basis to perform concatenation for source group."
                " All columns mismatch one another within the group."
            )
        )

    # return a python-native list of tuples with column names and str types
    return list(
        zip(
            common_schema.names,
            [str(schema_type) for schema_type in common_schema.types],
        )
    )


@flow
def to_parquet(  # pylint: disable=too-many-arguments, too-many-locals
    source_path: str,
    dest_path: str,
    source_datatype: Optional[str],
    metadata: Union[List[str], Tuple[str, ...]],
    compartments: Union[List[str], Tuple[str, ...]],
    identifying_columns: Union[List[str], Tuple[str, ...]],
    concat: bool,
    join: bool,
    joins: Optional[str],
    chunk_columns: Optional[Union[List[str], Tuple[str, ...]]],
    chunk_size: Optional[int],
    infer_common_schema: bool,
    drop_null: bool,
    **kwargs,
) -> Union[Dict[str, List[Dict[str, Any]]], str]:
    """
    Export data to parquet.

    Args:
        source_path: str:
            str reference to read source files from.
            Note: may be local or remote object-storage
            location using convention "s3://..." or similar.
        dest_path: str:
            Path to write files to.
            Note: this may only be a local path.
        source_datatype: Optional[str]: (Default value = None)
            Source datatype to focus on during conversion.
        metadata: Union[List[str], Tuple[str, ...]]:
            Metadata names to use for conversion.
        compartments: Union[List[str], Tuple[str, ...]]: (Default value = None)
            Compartment names to use for conversion.
        identifying_columns: Union[List[str], Tuple[str, ...]]:
            Column names which are used as ID's and as a result need to be
            ignored with regards to renaming.
        concat: bool:
            Whether to concatenate similar files together.
        join: bool:
            Whether to join the compartment data together into one dataset
        joins: str:
            DuckDB-compatible SQL which will be used to perform the join operations.
        chunk_columns: Optional[Union[List[str], Tuple[str, ...]]],
            Column names which appear in all compartments to use when performing join
        chunk_size: Optional[int],
            Size of join chunks which is used to limit data size during join ops
        infer_common_schema: bool:  (Default value = True)
            Whether to infer a common schema when concatenating sources.
        drop_null: bool:
            Whether to drop null results.

    Returns:
        Union[Dict[str, List[Dict[str, Any]]], str]:
            Grouped sources which include metadata about destination filepath
            where parquet file was written or a string filepath for the joined
            result.
    """

    # gather sources to be processed
    sources = gather_sources(
        source_path=source_path,
        source_datatype=source_datatype,
        targets=list(metadata) + list(compartments),
        **kwargs,
    )

    if not isinstance(sources, Dict):
        sources = sources.result()

    results = {}
    # for each group of sources, map writing parquet per file
    for source_group_name, source_group in sources.items():

        # read data from source groups
        source_group = read_data.map(source=source_group)

        # rename cols to include compartment or meta names
        renamed_source_group = prepend_column_name.map(
            source=source_group,
            targets=unmapped(list(metadata) + list(compartments)),
            source_group_name=unmapped(source_group_name),
            identifying_columns=unmapped(identifying_columns),
            metadata=unmapped(metadata),
        )

        # map for writing parquet files with list of files via sources
        results[source_group_name] = write_parquet.map(
            source=renamed_source_group,
            dest_path=unmapped(dest_path),
            # if the source group has more than one source, we will need a unique name
            # arg set to true or false based on evaluation of len(source_group)
            unique_name=unmapped(len(renamed_source_group) >= 2),
        )

        if concat and infer_common_schema:
            common_schema = infer_source_group_common_schema(
                source_group=results[source_group_name]
            )

        # if concat or join, concat the source groups
        # note: join implies a concat, but concat does not imply a join
        if concat or join:
            # build a new concatenated source group
            results[source_group_name] = concat_source_group.submit(
                source_group=results[source_group_name],
                dest_path=dest_path,
                common_schema=common_schema,
            )

    # conditional section for merging
    # note: join implies a concat, but concat does not imply a join
    if join:

        # map joined results based on the join groups gathered above
        # note: after mapping we end up with a list of strings (task returns str)
        join_sources_result = join_source_chunk.map(
            # gather the result of concatted sources prior to
            # join group merging as each mapped task run will need
            # full concat results
            sources=unmapped(
                {
                    key: value.result() if isinstance(value, PrefectFuture) else value
                    for key, value in results.items()
                }
            ),
            dest_path=unmapped(dest_path),
            joins=unmapped(joins),
            # get merging chunks by join columns
            join_group=get_join_chunks(
                sources=results,
                chunk_columns=chunk_columns,
                chunk_size=chunk_size,
                metadata=metadata,
            ),
            drop_null=unmapped(drop_null),
        )

        # concat our join chunks together as one cohesive dataset
        # return results in common format which includes metadata
        # for lineage and debugging
        results = concat_join_sources(
            dest_path=dest_path,
            join_sources=(
                join_sources_result.result()
                if isinstance(join_sources_result, PrefectFuture)
                else join_sources_result
            ),
            sources=results,
        )

    return (
        {
            key: value.result()
            if isinstance(value, PrefectFuture)
            else [
                inner_result.result()
                if isinstance(inner_result, PrefectFuture)
                else inner_result
                for inner_result in value
            ]
            if not isinstance(value, Dict)
            else value
            for key, value in results.items()
        }
        if isinstance(results, dict)
        else results
    )


def convert(  # pylint: disable=too-many-arguments,too-many-locals
    source_path: str,
    dest_path: str,
    dest_datatype: Literal["parquet"],
    source_datatype: Optional[str] = None,
    metadata: Union[List[str], Tuple[str, ...]] = cast(
        list, config["cellprofiler_csv"]["CONFIG_NAMES_METADATA"]
    ),
    compartments: Union[List[str], Tuple[str, ...]] = cast(
        list, config["cellprofiler_csv"]["CONFIG_NAMES_COMPARTMENTS"]
    ),
    identifying_columns: Union[List[str], Tuple[str, ...]] = cast(
        list, config["cellprofiler_csv"]["CONFIG_IDENTIFYING_COLUMNS"]
    ),
    concat: bool = True,
    join: bool = True,
    joins: Optional[str] = cast(str, config["cellprofiler_csv"]["CONFIG_JOINS"]),
    chunk_columns: Optional[Union[List[str], Tuple[str, ...]]] = cast(
        list, config["cellprofiler_csv"]["CONFIG_CHUNK_COLUMNS"]
    ),
    chunk_size: Optional[int] = cast(
        int, config["cellprofiler_csv"]["CONFIG_CHUNK_SIZE"]
    ),
    infer_common_schema: bool = True,
    drop_null: bool = True,
    preset: Optional[str] = None,
    task_runner: BaseTaskRunner = ConcurrentTaskRunner,
    log_level: str = "ERROR",
    **kwargs,
) -> Union[Dict[str, List[Dict[str, Any]]], str]:
    """
    Convert file-based data from various sources to Pycytominer-compatible standards.

    Note: source paths may be local or remote object-storage location
    using convention "s3://..." or similar.

    Args:
        source_path: str:
            str reference to read source files from.
            Note: may be local or remote object-storage location
            using convention "s3://..." or similar.
        dest_path: str:
            Path to write files to.
            Note: this may only be a local path.
        dest_datatype: Literal["parquet"]:
            Destination datatype to write to.
        source_datatype: Optional[str]:  (Default value = None)
            Source datatype to focus on during conversion.
        metadata: Union[List[str], Tuple[str, ...]]:
            Metadata names to use for conversion.
        compartments: Union[List[str], Tuple[str, str, str, str]]:
            (Default value = DEFAULT_COMPARTMENTS)
            Compartment names to use for conversion.
        identifying_columns: Union[List[str], Tuple[str, ...]]:
            Column names which are used as ID's and as a result need to be
            ignored with regards to renaming.
        concat: bool:  (Default value = True)
            Whether to concatenate similar files together.
        join: bool:  (Default value = True)
            Whether to join the compartment data together into one dataset
        joins: str: (Default value = presets.config["cellprofiler_csv"]["CONFIG_JOINS"]):
            DuckDB-compatible SQL which will be used to perform the join operations.
        chunk_columns: Optional[Union[List[str], Tuple[str, ...]]]
            (Default value = DEFAULT_CHUNK_COLUMNS)
            Column names which appear in all compartments to use when performing join
        chunk_size: Optional[int] (Default value = DEFAULT_CHUNK_SIZE)
            Size of join chunks which is used to limit data size during join ops
        infer_common_schema: bool: (Default value = True)
            Whether to infer a common schema when concatenating sources.
        drop_null: bool (Default value = True)
            Whether to drop nan/null values from results
        preset: str (Default value = None)
            an optional group of presets to use based on common configurations
        task_runner: BaseTaskRunner: (Default value = ConcurrentTaskRunner)
            Prefect task runner to use with flows.
        log_level: str: (Default value = "ERROR"):
            Log level for Prefect flow and task operations.

    Returns:
        Union[Dict[str, List[Dict[str, Any]]], str]
            Grouped sources which include metadata about destination filepath
            where parquet file was written or str of joined result filepath.

    Example:

        .. code-block:: python

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

            # using local path with cellprofiler sqlite presets
            convert(
                source_path="example.sqlite",
                dest_path="example.parquet",
                dest_datatype="parquet",
                merge=True,
                preset="cellprofiler_sqlite",
            )
    """

    # log level overrides for prefect
    os.environ["PREFECT_LOGGING_LEVEL"] = log_level
    os.environ["PREFECT_LOGGING_ROOT_LEVEL"] = log_level
    os.environ["PREFECT_LOGGING_HANDLERS_CONSOLE_LEVEL"] = log_level
    os.environ["PREFECT_LOGGING_HANDLERS_CONSOLE_FLOW_RUNS_LEVEL"] = log_level
    os.environ["PREFECT_LOGGING_HANDLERS_CONSOLE_TASK_RUNS_LEVEL"] = log_level
    os.environ["PREFECT_LOGGING_SERVER_LEVEL"] = log_level

    # optionally load preset configuration for arguments
    if preset is not None:
        metadata = cast(list, config[preset]["CONFIG_NAMES_METADATA"])
        compartments = cast(list, config[preset]["CONFIG_NAMES_COMPARTMENTS"])
        identifying_columns = cast(list, config[preset]["CONFIG_IDENTIFYING_COLUMNS"])
        joins = cast(str, config[preset]["CONFIG_JOINS"])
        chunk_columns = cast(list, config[preset]["CONFIG_CHUNK_COLUMNS"])
        chunk_size = cast(int, config[preset]["CONFIG_CHUNK_SIZE"])

    # send sources to be written to parquet if selected
    if dest_datatype == "parquet":
        output = to_parquet.with_options(task_runner=task_runner)(
            source_path=source_path,
            dest_path=dest_path,
            source_datatype=source_datatype,
            metadata=metadata,
            compartments=compartments,
            identifying_columns=identifying_columns,
            concat=concat,
            join=join,
            joins=joins,
            chunk_columns=chunk_columns,
            chunk_size=chunk_size,
            infer_common_schema=infer_common_schema,
            drop_null=drop_null,
            **kwargs,
        )

    return output
