get_compile_data Function
=========================

.. automodule:: sendqsarpy.get_compiledata
   :members:
   :undoc-members:
   :show-inheritance:

Overview
--------

The `get_compile_data` function processes data from the SEND database to produce a cleaned and ranked dataset for further QSAR model development. This function includes filtering, data manipulation, and ranking procedures to align the dataset with experimental requirements.

Function Signature
-------------------

.. autofunction:: get_compile_data

Parameters
----------

The following parameters are expected as input to the function:

- **data (pd.DataFrame)**: The input dataset, typically loaded from the SEND database.
- **Species (str)**: The species to filter for (e.g., 'rat').
- **pp (pd.DataFrame)**: Pooling data containing pool IDs and related information.
- **pooldef (pd.DataFrame)**: Definitions of pool IDs, including their associated subjects and studies.
- **tx (pd.DataFrame)**: Treatment data containing dose levels and associated parameters.
- **bw (pd.DataFrame)**: Body weight data to help with additional filtering.

Returns
-------

A cleaned and ranked dataset (`pd.DataFrame`) that includes the following columns:

- `STUDYID`: Study identifier.
- `USUBJID`: Unique subject identifier.
- `Species`: The species associated with the dataset.
- `SEX`: The sex of the subjects.
- `ARMCD`: The dose ranking assigned to the subjects ('vehicle', 'HD', 'Intermediate', or 'Both').
- `SETCD`: Set code for experimental groups.

Usage Example
-------------

Below is an example of how to use the `get_compile_data` function:

.. code-block:: python

    import pandas as pd
    from sendqsarpy.get_compiledata import get_compile_data

    # Load your datasets
    recovery_cleaned_data = pd.read_csv('recovery_cleaned_data.csv')
    pp_data = pd.read_csv('pp_data.csv')
    pooldef_data = pd.read_csv('pooldef_data.csv')
    tx_data = pd.read_csv('tx_data.csv')
    bw_data = pd.read_csv('bw_data.csv')

    # Call the function
    cleaned_data = get_compile_data(
        data=recovery_cleaned_data,
        Species='rat',
        pp=pp_data,
        pooldef=pooldef_data,
        tx=tx_data,
        bw=bw_data
    )

    # Inspect the results
    print(cleaned_data.head())

Notes
-----

- Ensure the input data follows the expected schema (columns and data types).
- This function assumes the SEND database schema for input data tables.
- For troubleshooting or custom dataset processing, refer to the source code and modify as needed.

Related Links
-------------

- `SENDQSARpy Documentation <index.html>`_
- `Getting Started <getting_started.html>`_
