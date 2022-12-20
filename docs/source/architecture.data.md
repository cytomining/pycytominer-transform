# Data Architecture

Documentation covering data architecture for pyctyominer-transform.

## Sources

Data sources for pyctyominer-transform are measurement data created from other cell biology image analysis tools.

See below for a brief overview of these sources and data types.

- [CellProfiler](https://github.com/CellProfiler/CellProfiler) generates [CSV](https://en.wikipedia.org/wiki/Comma-separated_values) and [SQLite](https://www.sqlite.org/) databases data.
- [Cytominer-database](https://github.com/cytomining/cytominer-database) generates [SQLite](https://www.sqlite.org/) databases (which includes table data based on CellProfiler CSV's mentioned above).
- [DeepProfiler](https://github.com/cytomining/DeepProfiler) generates [NPZ](https://numpy.org/doc/stable/reference/routines.io.html?highlight=npz%20format#numpy-binary-files-npy-npz) data.

## Data structure

### Compartments and Images

Data are organized into tables of generally two categories:

- __Images__: Image tables generally include data about the image itself, including metadata.
- __Compartments__: Compartment data tables (such as Cytoplasm, Cells, Nuclei, Actin, or others) includes measurements specific to an aspect or part of cells found within an image.

### Identifying or Key Fields

Identifying or key fields for image and compartment tables may include the following:

- __ImageNumber__: Provides specificity on what image is being referenced (there may be many).
- __ObjectNumber__: Provides specificity for a specific compartment object within an ImageNumber.
- __Parent_Cells__: Provides a related Cell compartment ObjectNumber. This field is canonically referenced from the Cytoplasm compartment for joining Cytoplasm and Cell compartment data. (see [Cytoplasm Compartment Data Relationships](architecture.data.md#cytoplasm-compartment-data-relationships) below for greater detail)
- __Parent_Nuclei__: Provides a related Nuclei compartment ObjectNumber. This field is canonically referenced from the Cytoplasm compartment for joining Cytoplasm and Cell compartment data. (see [Cytoplasm Compartment Data Relationships](architecture.data.md#cytoplasm-compartment-data-relationships) below for greater detail)

## Relationships

### Image Data Relationships

```{mermaid}
erDiagram
    Image ||--o{ Cytoplasm : includes
    Image ||--o{ Cells : includes
    Image ||--o{ Nuclei : includes
    Image ||--o{ Others : includes
```

The above diagram shows an example of image data relationships found within the data that is used by pycytominer-transform.
Namely: Each image may include zero or many compartment objects (Cytoplasm, Cells, Nuclei, etc)objects.

### Cytoplasm Compartment Data Relationships

```{mermaid}
erDiagram
    Cytoplasm {
        integer ObjectNumber "Cytoplasm object number within image"
        integer Parent_Cells "Related Cells compartment object number"
        integer Parent_Nuclei "Related Nuclei compartment object number"
    }
    Cells {
        integer ObjectNumber "Cells object number within image"
    }
    Nuclei {
        integer ObjectNumber "Nuclei object number within image"
    }
    Cells ||--|| Cytoplasm : related-to
    Nuclei ||--|| Cytoplasm : related-to
```

The above diagram shows canonical relationships of the Cytoplasm compartment data to other compartments found within the data that is used by pycytominer-transform.
Each Cytoplasm object is related to Cells via the Parent_Cells field and Nuclei via the Parent_Nuclei field.
These Parent\_\* fields are ObjectNumbers in their respective compartments.
