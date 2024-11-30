.. _get_lb_score:

get_lb_score
============

.. function:: get_lb_score(lb_tk_recovery_filtered_armcd, lbtestcd_groups)

   Calculate Z-scores and assign toxicity scores for multiple LBTESTCD groups.

   This function processes lab data to compute Z-scores based on the deviation of values in the HD group compared to the vehicle group. It assigns toxicity scores and organizes the results into separate DataFrames for each group.

   :param lb_tk_recovery_filtered_armcd: 
       A pandas DataFrame containing lab data. Must include columns: 
       ``STUDYID``, ``LBTESTCD``, ``LBSTRESN``, and ``ARMCD``.
   :type lb_tk_recovery_filtered_armcd: pandas.DataFrame

   :param lbtestcd_groups: 
       A dictionary where keys are group names (e.g., "ALT", "AST") and values are lists of corresponding ``LBTESTCD`` codes.
   :type lbtestcd_groups: dict

   :return: 
       A dictionary with group names as keys and their respective DataFrames as values. Each DataFrame includes the columns:
       
       - ``STUDYID``: The study ID.
       - ``avg_<group_name>_zscore``: The average Z-score for the group, with toxicity scores applied.

   :rtype: dict

   **Example Usage**::

       import pandas as pd

       # Sample lab data
       data = {
           "STUDYID": ["S1", "S1", "S1", "S2", "S2"],
           "LBTESTCD": ["SERUM | ALT", "PLASMA | ALT", "WHOLE BLOOD | ALT", "SERUM | ALT", "PLASMA | ALT"],
           "LBSTRESN": [1.2, 1.5, 2.0, 1.8, 2.2],
           "ARMCD": ["vehicle", "HD", "HD", "vehicle", "HD"]
       }
       df = pd.DataFrame(data)

       # Define LBTESTCD groups
       lbtestcd_groups = {
           "ALT": ["SERUM | ALT", "PLASMA | ALT", "WHOLE BLOOD | ALT"],
           "AST": ["SERUM | AST", "PLASMA | AST", "WHOLE BLOOD | AST"]
       }

       # Calculate Z-scores
       results = get_lb_score(df, lbtestcd_groups)

       # Access results for the ALT group
       alt_results = results["ALT"]
       print(alt_results)

   **Notes**:

   - The function assumes the presence of a "vehicle" group in the ``ARMCD`` column for baseline calculations.
   - Z-scores are calculated as the absolute deviation from the mean, scaled by the standard deviation.
   - Toxicity scores are assigned as follows:
     
     - ``3`` for Z-scores >= 3
     - ``2`` for Z-scores >= 2
     - ``1`` for Z-scores >= 1
     - ``0`` for Z-scores < 1
