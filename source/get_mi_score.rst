get_mi_score Function
======================

.. module:: my_module_name
.. currentmodule:: my_module_name

This function iterates over columns in the `mi_CompileData2` DataFrame, applies severity scoring, and adjusts values based on study-specific incidence thresholds. The results are updated in the `ScoredData` DataFrame and `mi_CompileData2`.

Function Details
-----------------

.. function:: get_mi_score(mi_CompileData2, ScoredData, MIIncidence)

    Processes severity scoring and incidence-based adjustments for columns in a dataset.

    **Parameters**

    :param mi_CompileData2: pandas.DataFrame
        The main DataFrame containing the data to be processed.
        Must include columns for `STUDYID`, `SEX`, and other severity-related columns.

    :param ScoredData: pandas.DataFrame
        A DataFrame used to store the results of severity scoring.

    :param MIIncidence: pandas.DataFrame
        A DataFrame containing incidence data for different treatments and findings.
        Must include columns `Treatment`, `Sex`, `Finding`, and `Count`.

    **Returns**

    None. Updates `mi_CompileData2` and `ScoredData` in-place.

    **Raises**

    :raises ValueError:
        If multiple unique `STUDYID` values are found in `ScoredData`.

    **Steps**

    1. **Severity Scoring**:
       - Scores severity for each column in `mi_CompileData2` starting from the 6th column.
       - Scoring rules:
         - 5 if value equals 5.
         - 3 if value is greater than 3.
         - 2 if value equals 3.
         - 1 if value is greater than 0 but less than 3.
         - 0 otherwise.

    2. **Subset Filtering**:
       - Filters data by `STUDYID` and `SEX` to process subsets for incidence calculations.

    3. **Incidence Adjustment**:
       - Calculates an adjusted incidence score based on the `Count` column in `MIIncidence`.
       - Updates values in `ScoredData` and `mi_CompileData2` where the calculated incidence score is higher than the current severity score.

    4. **Debugging Information**:
       - Prints intermediate outputs to verify processing steps.

    **Notes**

    This function assumes that `mi_CompileData2` and `MIIncidence` have the required structure and columns. Debugging outputs are included for development purposes.

Example Usage
-------------

.. code-block:: python

    import pandas as pd
    import numpy as np

    # Sample data
    mi_CompileData2 = pd.DataFrame({
        'STUDYID': ['Study1'] * 4,
        'SEX': ['M', 'F', 'M', 'F'],
        'ARMCD': ['HD', 'LD', 'HD', 'LD'],
        'Col7': [5, 4, 3, 0],
        'Col8': [3, 2, 0, 5],
    })

    ScoredData = mi_CompileData2.copy()
    MIIncidence = pd.DataFrame({
        'Treatment': ['Study1 HD', 'Study1 LD'],
        'Sex': ['M', 'F'],
        'Finding': ['Col7', 'Col8'],
        'Count': [0.8, 0.4],
    })

    IncidenceOverideCount = 0

    # Call the function
    get_mi_score(mi_CompileData2, ScoredData, MIIncidence)

    # Check results
    print(ScoredData)

Changelog
---------

- **v1.0**: Initial version with scoring and incidence adjustment logic.
