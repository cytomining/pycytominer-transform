"""
Presets for common pycytominer-transform configurations.
"""

config = {
    "cellprofiler_csv": {
        # names of source table compartments (for ex. cells.csv, etc.)
        "CONFIG_NAMES_COMPARTMENTS": ("cells", "nuclei", "cytoplasm"),
        # names of source table metadata (for ex. image.csv, etc.)
        "CONFIG_NAMES_METADATA": ("image",),
        # column names in any compartment or metadata tables which contain
        # unique names to avoid renaming
        "CONFIG_IDENTIFYING_COLUMNS": (
            "ImageNumber",
            "ObjectNumber",
            "Metadata_Well",
            "Metadata_Plate",
            "Parent_Cells",
            "Parent_Nuclei",
        ),
        # chunk size to use for join operations to help with possible performance issues
        # note: this number is an estimate and is may need changes contingent on data
        # and system used by this library.
        "CONFIG_CHUNK_SIZE": 1000,
        # chunking columns to use along with chunk size for join operations
        "CONFIG_CHUNK_COLUMNS": ("Metadata_ImageNumber",),
        # compartment and metadata joins performed using DuckDB SQL
        # and modified at runtime as needed
        "CONFIG_JOINS": """
            WITH Image_Filtered AS (
                SELECT
                    /* seeks columns by name, avoiding failure if some do not exist */
                    COLUMNS('^Metadata_ImageNumber$|^Image_Metadata_Well$|^Image_Metadata_Plate$')
                FROM
                    read_parquet('image.parquet')
                )
            SELECT
                *
            FROM
                Image_Filtered AS image
            LEFT JOIN read_parquet('cytoplasm.parquet') AS cytoplasm ON
                cytoplasm.Metadata_ImageNumber = image.Metadata_ImageNumber
            LEFT JOIN read_parquet('cells.parquet') AS cells ON
                cells.Metadata_ImageNumber = cytoplasm.Metadata_ImageNumber
                AND cells.Metadata_ObjectNumber = cytoplasm.Metadata_Cytoplasm_Parent_Cells
            LEFT JOIN read_parquet('nuclei.parquet') AS nuclei ON
                nuclei.Metadata_ImageNumber = cytoplasm.Metadata_ImageNumber
                AND nuclei.Metadata_ObjectNumber = cytoplasm.Metadata_Cytoplasm_Parent_Nuclei
            """,
    },
    "cellprofiler_sqlite": {
        # names of source table compartments (for ex. cells.csv, etc.)
        "CONFIG_NAMES_COMPARTMENTS": ("cells", "nuclei", "cytoplasm"),
        # names of source table metadata (for ex. image.csv, etc.)
        "CONFIG_NAMES_METADATA": ("image",),
        # column names in any compartment or metadata tables which contain
        # unique names to avoid renaming
        "CONFIG_IDENTIFYING_COLUMNS": (
            "ImageNumber",
            "Metadata_Well",
            "Parent_Cells",
            "Parent_OrigNuclei",
        ),
        # chunk size to use for join operations to help with possible performance issues
        # note: this number is an estimate and is may need changes contingent on data
        # and system used by this library.
        "CONFIG_CHUNK_SIZE": 1000,
        # chunking columns to use along with chunk size for join operations
        "CONFIG_CHUNK_COLUMNS": ("Metadata_ImageNumber",),
        # compartment and metadata joins performed using DuckDB SQL
        # and modified at runtime as needed
        "CONFIG_JOINS": """
            WITH Per_Image_Filtered AS (
                SELECT
                    Metadata_ImageNumber,
                    Image_Metadata_Well,
                    Image_Metadata_Plate
                FROM
                    read_parquet('per_image.parquet')
                )
            SELECT
                *
            FROM
                Per_Image_Filtered AS per_image
            LEFT JOIN read_parquet('per_cytoplasm.parquet') AS per_cytoplasm ON
                per_cytoplasm.Metadata_ImageNumber = per_image.Metadata_ImageNumber
            LEFT JOIN read_parquet('per_cells.parquet') AS per_cells ON
                per_cells.Metadata_ImageNumber = per_cytoplasm.Metadata_ImageNumber
                AND per_cells.Cells_Number_Object_Number = per_cytoplasm.Cytoplasm_Parent_Cells
            LEFT JOIN read_parquet('per_nuclei.parquet') AS per_nuclei ON
                per_nuclei.Metadata_ImageNumber = per_cytoplasm.Metadata_ImageNumber
                AND per_nuclei.Nuclei_Number_Object_Number = per_cytoplasm.Cytoplasm_Parent_OrigNuclei
            """,
    },
}
"""
Configuration presets for pycytominer-transform
"""
