"""
pycytominer-transform: sources - tasks and flows related to
source data and metadata for performing conversion work.
"""

import pathlib
from typing import Any, Dict, List, Optional, Union

from cloudpathlib import AnyPath, CloudPath
from prefect import flow, task

from pycytominer_transform.exceptions import DatatypeException, NoInputDataException
from pycytominer_transform.utils import duckdb_with_sqlite


@task
def build_path(
    path: Union[str, pathlib.Path, AnyPath], **kwargs
) -> Union[pathlib.Path, Any]:
    """
    Build a path client or return local path.

    Args:
        path: Union[pathlib.Path, Any]:
            Path to seek filepaths within.
        **kwargs: Any
            keyword arguments to be used with
            Cloudpathlib.CloudPath.client

    Returns:
        Union[pathlib.Path, Any]
            A local pathlib.Path or Cloudpathlib.AnyPath type path.
    """

    processed_path = AnyPath(path)

    # set the client for a CloudPath
    if isinstance(processed_path, CloudPath):
        processed_path.client = processed_path.client.__class__(**kwargs)

    return processed_path


@task
def get_source_filepaths(
    path: Union[pathlib.Path, AnyPath],
    targets: List[str],
    source_datatype: Optional[str] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Gather dataset of filepaths from a provided directory path.

    Args:
        path: Union[pathlib.Path, Any]:
            Path to seek filepaths within.
        targets: List[str]:
            Compartment and metadata names to seek within the provided path.
        source_datatype: Optional[str]  (Default value = None)
            Data type for source data files.


    Returns:
        Dict[str, List[Dict[str, Any]]]
            Data structure which groups related files based on the compartments.
    """

    if source_datatype == "sqlite" or (path.is_file() and path.suffix == ".sqlite"):
        return {
            f"{table_name}.sqlite": [{"table_name": table_name, "source_path": path}]
            for table_name in duckdb_with_sqlite()
            .execute(
                """
                /* perform query on sqlite_master table for metadata on tables */
                SELECT name as table_name
                from sqlite_scan(?, 'sqlite_master')
                where type='table'
                """,
                parameters=[str(path)],
            )
            .arrow()["table_name"]
            .to_pylist()
            if any(target.lower() in table_name.lower() for target in targets)
        }

    # gathers files from provided path using compartments as a filter
    sources = [
        {"source_path": file}
        for file in path.glob("**/*")
        if file.is_file()
        and (
            targets is None
            or str(file.stem).lower() in [target.lower() for target in targets]
        )
    ]

    # if we collected no files above, raise exception
    if len(sources) < 1:
        raise NoInputDataException(f"No input data to process at path: {str(path)}")

    grouped_sources = {}

    # group files together by similar filename for later data operations
    for unique_source in set(source["source_path"].name for source in sources):
        grouped_sources[unique_source.capitalize()] = [
            source for source in sources if source["source_path"].name == unique_source
        ]

    return grouped_sources


@task
def infer_source_datatype(
    sources: Dict[str, List[Dict[str, Any]]], source_datatype: Optional[str] = None
) -> str:
    """
    Infers and optionally validates datatype (extension) of files.

    Args:
        sources: Dict[str, List[Dict[str, Any]]]:
            Grouped datasets of files which will be used by other functions.
        source_datatype: Optional[str]:  (Default value = None)
            Optional source datatype to validate within the context of
            detected datatypes.

    Returns:
        str
            A string of the datatype detected or validated source_datatype.
    """

    # gather file extension suffixes
    suffixes = list(set((group.split(".")[-1]).lower() for group in sources))

    # if we don't have a source datatype and have more than one suffix
    # we can't infer which file type to read.
    if source_datatype is None and len(suffixes) > 1:
        raise DatatypeException(
            f"Detected more than one inferred datatypes from source path: {suffixes}"
        )

    # if we have a source datatype and it isn't within the detected suffixes
    # we will have no files to process.
    if source_datatype is not None and source_datatype not in suffixes:
        raise DatatypeException(
            (
                f"Unable to find source datatype {source_datatype} "
                "within files. Detected datatypes: {suffixes}"
            )
        )

    # if we haven't set a source datatype and need to rely on the inferred one
    # set it so it may be returned
    if source_datatype is None:
        source_datatype = suffixes[0]

    return source_datatype


@task
def filter_source_filepaths(
    sources: Dict[str, List[Dict[str, Any]]], source_datatype: str
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Filter source filepaths based on provided source_datatype

    Args:
        sources: Dict[str, List[Dict[str, Any]]]
            Grouped datasets of files which will be used by other functions.
        source_datatype: str
            Source datatype to use for filtering the dataset.

    Returns:
        Dict[str, List[Dict[str, Any]]]
            Data structure which groups related files based on the datatype.
    """

    return {
        filegroup: [
            file
            for file in files
            # ensure the filesize is greater than 0
            if file["source_path"].stat().st_size > 0
            # ensure the datatype matches the source datatype
            and file["source_path"].suffix == f".{source_datatype}"
        ]
        for filegroup, files in sources.items()
    }


@flow
def gather_sources(
    source_path: str,
    source_datatype: Optional[str] = None,
    targets: Optional[List[str]] = None,
    **kwargs,
) -> Dict[str, List[Dict[str, Any]]]:

    """
    Flow for gathering data sources for conversion

    Args:
        source_path: str:
            Where to gather file-based data from.
        source_datatype: Optional[str]:  (Default value = None)
            The source datatype (extension) to use for reading the tables.
        targets: Optional[List[str]]:  (Default value = None)
            The source file names to target within the provided path.

    Returns:
        Dict[str, List[Dict[str, Any]]]
            Data structure which groups related files based on the compartments.
    """

    source_path = build_path(path=source_path, **kwargs)

    # gather filepaths which will be used as the basis for this work
    sources = get_source_filepaths(
        path=source_path, source_datatype=source_datatype, targets=targets
    )

    # infer or validate the source datatype based on source filepaths
    source_datatype = infer_source_datatype(
        sources=sources, source_datatype=source_datatype
    )

    # filter source filepaths to inferred or source datatype
    return filter_source_filepaths(sources=sources, source_datatype=source_datatype)
