import pandas as pd
import numpy as np

def get_col_harmonized_scores_df(liver_score_data_frame, round_values=False):
    # Create a copy of the input DataFrame
    liver_scores = liver_score_data_frame.copy()

    # Replace spaces, commas, and slashes in column names with dots
    liver_scores.columns = liver_scores.columns.str.replace(' ', '.').str.replace(',', '.').str.replace('/', '.')

    # Replace NA values with 0
    liver_scores.fillna(0, inplace=True)

    # Identify columns with periods
    findings2replace_index = [i for i, col in enumerate(liver_scores.columns) if '.' in col]

    # Identify unique column names without periods
    fn2replace = set(liver_scores.columns.str.upper()) - {liver_scores.columns[i].upper() for i in findings2replace_index}

    # Remove specific columns from processing
    remove_columns = {'STUDYID', 'UNREMARKABLE', 'THIKENING', 'POSITIVE'}
    fn2replace = fn2replace - remove_columns

    # Harmonize synonym columns
    for finding in fn2replace:
        synonyms = [col for col in liver_scores.columns if finding in col.upper()]
        for synonym in synonyms:
            indices = liver_scores[synonym] > 0
            liver_scores.loc[indices, finding] = liver_scores.loc[indices, [finding, synonym]].max(axis=1)

    # Remove synonym columns
    liver_scores.drop(columns=[liver_scores.columns[i] for i in findings2replace_index], inplace=True)

    # Remove unwanted endpoints
    remove_endpoints = {'Infiltrate', 'UNREMARKABLE', 'THIKENING', 'POSITIVE'}
    liver_scores = liver_scores.drop(columns=[col for col in liver_scores.columns if col in remove_endpoints], errors='ignore')

    if round_values:
        # Identify columns with "avg_" or "liver"
        zscore_index = liver_scores.filter(regex='avg_|liver').columns

        # Apply rounding rules to zscore_index columns
        for col in zscore_index:
            liver_scores[col] = liver_scores[col].apply(np.floor)
            liver_scores.loc[liver_scores[col] > 5, col] = 5

        # Identify columns starting with an uppercase letter
        histo_index = [col for col in liver_scores.columns if col[0].isupper()]
        histo_index = histo_index[1:]  # Exclude the first column

        # Apply ceiling to histo_index columns
        for col in histo_index:
            liver_scores[col] = liver_scores[col].apply(np.ceil)

    # Reorder columns by their sum, descending
    column_sums = liver_scores.iloc[:, 1:].sum(axis=0).sort_values(ascending=False)
    reordered_columns = [liver_scores.columns[0]] + column_sums.index.tolist()
    liver_scores = liver_scores[reordered_columns]

    return liver_scores
