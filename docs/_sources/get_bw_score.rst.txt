.. _get_bw_score:

get_bw_score
============

.. function:: get_bw_score(bw, master_compiledata=None, tK_animals_df=None, return_individual_scores=False, return_zscore_by_USUBJID=False)

   Processes body weight data for study subjects, calculates z-scores, and enforces mutual exclusivity of certain return options.

   **Parameters:**

   - **bw** (*pandas.DataFrame*):  
     A DataFrame containing body weight data with columns `STUDYID`, `USUBJID`, `BWTESTCD`, `BWSTRESN`, `VISITDY`, and optionally `BWDY`.

   - **master_compiledata** (*pandas.DataFrame, optional*):  
     A DataFrame containing metadata for subjects, including `USUBJID`, `ARMCD`, `SETCD`, and `SEX`. Defaults to `None`.

   - **tK_animals_df** (*pandas.DataFrame, optional*):  
     A DataFrame containing information about animals to exclude, such as toxicokinetic (TK) animals. Defaults to `None`.

   - **return_individual_scores** (*bool, optional*):  
     If `True`, individual scores for each subject will be calculated and returned. Defaults to `False`.

   - **return_zscore_by_USUBJID** (*bool, optional*):  
     If `True`, z-scores by `USUBJID` will be calculated and returned. Defaults to `False`.

   **Raises:**

   - **ValueError**:  
     If both `return_individual_scores` and `return_zscore_by_USUBJID` are set to `True`.

   **Returns:**

   - **pandas.DataFrame**:  
     A DataFrame containing processed body weight data, filtered subjects, and calculated z-scores for high-dose (HD) subjects.

   **Notes:**

   This function performs the following steps:
   - Replaces missing `VISITDY` values with corresponding `BWDY` values.
   - Selects `TERMBW` rows or those with the maximum `VISITDY > 5` if `TERMBW` is not available.
   - Filters out toxicokinetic (TK) animals using `master_compiledata`.
   - Joins body weight data with additional metadata and calculates z-scores for each subject in the "HD" group.

   Example usage::

       result = get_bw_score(
           bw=bw_data,
           master_compiledata=master_data,
           tK_animals_df=tk_animals,
           return_individual_scores=False,
           return_zscore_by_USUBJID=True
       )

   The resulting DataFrame includes columns such as `STUDYID`, `USUBJID`, `SEX`, and `BWZSCORE`.

