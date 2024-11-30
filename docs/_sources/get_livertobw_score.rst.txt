get_livertobw_score
====================

.. function:: get_livertobw_score(studyid=None, path_db=None, fake_study=False, use_xpt_file=False, master_compiledata=None, bwzscore_BW=None, return_individual_scores=False, return_zscore_by_USUBJID=False)

   Calculates the liver-to-body weight (liverToBW) z-scores and related metrics for a given study.

   The function processes either `.xpt` files or data from an SQLite database. It returns either individual scores, z-scores for each subject (by `USUBJID`), or average z-scores for the study, depending on the specified parameters.

   **Parameters:**

   - **studyid** (*str*, optional): The study ID. If `None`, the study ID is not used (default is `None`).
   - **path_db** (*str*): Path to the database or directory containing the `.xpt` files.
   - **fake_study** (*bool*, optional): If `True`, it uses a fake study setup for testing purposes (default is `False`).
   - **use_xpt_file** (*bool*, optional): If `True`, data is read from `.xpt` files instead of the SQLite database (default is `False`).
   - **master_compiledata** (*DataFrame*, optional): A DataFrame containing master compile data for the study. If `None`, it will be fetched from the `get_compile_data` function.
   - **bwzscore_BW** (*DataFrame*, optional): A DataFrame containing body weight z-scores. If `None`, it will be fetched from the `get_bw_score` function.
   - **return_individual_scores** (*bool*, optional): If `True`, the function returns individual liverToBW z-scores for each subject. Cannot be used with `return_zscore_by_USUBJID` (default is `False`).
   - **return_zscore_by_USUBJID** (*bool*, optional): If `True`, the function returns the liverToBW z-scores for each subject by `USUBJID`. Cannot be used with `return_individual_scores` (default is `False`).

   **Returns:**

   - If `return_individual_scores` is `True`, returns a DataFrame with individual z-scores for each subject in the "HD" arm.
   - If `return_zscore_by_USUBJID` is `True`, returns a DataFrame with liverToBW z-scores by `USUBJID` for the "HD" arm.
   - Otherwise, returns a DataFrame with the average liverToBW z-scores for the study.

   **Raises:**

   - **ValueError**: If both `return_individual_scores` and `return_zscore_by_USUBJID` are `True` at the same time.

   **Example:**

   To calculate and return the average liver-to-body weight z-scores for the "HD" arm:
   
   .. code-block:: python

      result = get_livertobw_score(
          studyid="STUDY123",
          path_db="/path/to/database",
          fake_study=False,
          use_xpt_file=False,
          master_compiledata=None,
          bwzscore_BW=None,
          return_individual_scores=False,
          return_zscore_by_USUBJID=False
      )

   To get individual liver-to-body weight z-scores for each subject in the "HD" arm:
   
   .. code-block:: python

      result = get_livertobw_score(
          studyid="STUDY123",
          path_db="/path/to/database",
          fake_study=False,
          use_xpt_file=False,
          master_compiledata=None,
          bwzscore_BW=None,
          return_individual_scores=True,
          return_zscore_by_USUBJID=False
      )
   
   To get liver-to-body weight z-scores by `USUBJID` for the "HD" arm:
   
   .. code-block:: python

      result = get_livertobw_score(
          studyid="STUDY123",
          path_db="/path/to/database",
          fake_study=False,
          use_xpt_file=False,
          master_compiledata=None,
          bwzscore_BW=None,
          return_individual_scores=False,
          return_zscore_by_USUBJID=True
      )
