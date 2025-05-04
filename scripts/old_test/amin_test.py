import sqlite3
import pandas as pd
from sendqsarpy.get_liver_om_lb_mi_tox_score_list import get_liver_om_lb_mi_tox_score_list
from sendqsarpy.get_col_harmonized_scores_df  import get_col_harmonized_scores_df
import pdb 
# 1. Define the path to the SQLite database
#path_db = 'C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/DATABASES/fake_merged_liver_not_liver.db'

# Set the parameters you want to test
db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/DATABASES/TestDB.db"

path_db=db_path
 
#studyid_or_studyids = 
studyid_or_studyids = ["2170017"]


# # 2. Establish a connection to the database
# connection = sqlite3.connect(path_db)

# # 3. Execute the SQL query to retrieve the STUDYID column from the 'dm' table
# query = "SELECT STUDYID FROM dm"
# studyid_data = pd.read_sql_query(query, connection)

# # 4. Extract unique STUDYID values
# unique_studyids = studyid_data['STUDYID'].unique()


# unique_studyids = '11094'
# # 5. Close the database connection
# connection.close()

# # 6. Assign the unique IDs to a variable
# studyid_or_studyids = unique_studyids

# Call the function with equivalent arguments
fake80_liver_scores = get_liver_om_lb_mi_tox_score_list(
    studyid_or_studyids=studyid_or_studyids,
    path_db=path_db,
    fake_study=False,
    use_xpt_file=False,
    output_individual_scores=True,
    output_zscore_by_USUBJID=False
)

#  # Apply the function
# column_harmonized_liverscr_df = get_col_harmonized_scores_df(
#      liver_score_data_frame=fake80_liver_scores,
#      round_values=True
#  )

# Create a copy of the input DataFrame
liver_scores = fake80_liver_scores.copy()

# Replace spaces, commas, and slashes in column names with dots
liver_scores.columns = liver_scores.columns.str.replace(' ', '.').str.replace(',', '.').str.replace('/', '.')

# Replace NA values with 0
liver_scores.fillna(0, inplace=True)

# Identify columns with periods
findings2replace_index = [i for i, col in enumerate(liver_scores.columns) if '.' in col]

#breakpoint()

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

#breakpoint()
# Remove synonym columns
liver_scores.drop(columns=[liver_scores.columns[i] for i in findings2replace_index], inplace=True)

# Remove unwanted endpoints
remove_endpoints = {'Infiltrate', 'UNREMARKABLE', 'THIKENING', 'POSITIVE'}
liver_scores = liver_scores.drop(columns=[col for col in liver_scores.columns if col in remove_endpoints], errors='ignore')

# if round_values:
#     # Identify columns with "avg_" or "liver"
#     zscore_index = liver_scores.filter(regex='avg_|liver').columns

#     # Apply rounding rules to zscore_index columns
#     for col in zscore_index:
#         liver_scores[col] = liver_scores[col].apply(np.floor)
#         liver_scores.loc[liver_scores[col] > 5, col] = 5

#     # Identify columns starting with an uppercase letter
#     histo_index = [col for col in liver_scores.columns if col[0].isupper()]
#     histo_index = histo_index[1:]  # Exclude the first column

#     # Apply ceiling to histo_index columns
#     for col in histo_index:
#         liver_scores[col] = liver_scores[col].apply(np.ceil)

# #breakpoint()
# # Reorder columns by their sum, descending
# column_sums = liver_scores.iloc[:, 1:].sum(axis=0).sort_values(ascending=False)
# reordered_columns = [liver_scores.columns[0]] + column_sums.index.tolist()
# liver_scores = liver_scores[reordered_columns]


