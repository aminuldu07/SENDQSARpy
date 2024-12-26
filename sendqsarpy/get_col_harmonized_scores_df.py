import pandas as pd
import numpy as np
import re
import pdb  # Import the debugger


def get_col_harmonized_scores_df(liver_score_data_frame, 
                                 round_values=False):
    # Create a copy of the input DataFrame
    liver_scores = liver_score_data_frame.copy()

    # Replace spaces, commas, and slashes in column names with dots
    liver_scores.columns = liver_scores.columns.str.replace(' ', '.').str.replace(',', '.').str.replace('/', '.')

    # Replace NA values with 0
    liver_scores.fillna(0, inplace=True)

    # Identify columns with periods
    findings2replace_index = [i for i, col in enumerate(liver_scores.columns) if '.' in col]

    # Exclude columns at findings2replaceIndex and get their names
    remaining_columns = [col for i, col in enumerate(liver_scores.columns) if i not in findings2replace_index]

    # Convert to uppercase and remove duplicates
    # Identify unique column names without periods
    fn2replace = list(set([col.upper() for col in remaining_columns]))

    # Store column names with periods
    f2replace = [liver_scores.columns[i] for i in findings2replace_index ]

    # Remove specific columns from processing
    remove_columns = ['STUDYID', 'UNREMARKABLE', 'THIKENING', 'POSITIVE']
    fn2replace = [item for item in fn2replace if item not in remove_columns]

    for finding in fn2replace:
        # Find synonyms using case-insensitive regex
        synonyms = [f for f in f2replace if re.search(finding, f, re.IGNORECASE)]

        for synonym in synonyms:
            # Get indices where the liver_scores[synonym] > 0
            index = liver_scores[synonym][liver_scores[synonym] > 0].index

            for i in index:
                # Update liver_scores[finding][i] if the condition is met
                # If the value in the synonym column is greater than the value in the finding column, update
                if liver_scores.at[i, synonym] > liver_scores.at[i, finding]:
                    liver_scores.at[i, finding] = liver_scores.at[i, synonym]
    # Remove synonym columns
    #liver_scores = liver_scores.drop(columns=findings2replace_index)
    print(liver_scores.shape)
    liver_scores = liver_scores.drop(liver_scores.columns[findings2replace_index], axis=1)

    # Rename "liver_scores" to "Data"
    Data = liver_scores

    # Remove unwanted endpoints
    remove_endpoints = {'INFILTRATE', 'UNREMARKABLE', 'THIKENING', 'POSITIVE'}
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

