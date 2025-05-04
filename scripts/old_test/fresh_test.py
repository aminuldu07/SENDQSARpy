# -*- coding: utf-8 -*-
"""
Created on Sun Dec 22 15:36:52 2024

@author: MdAminulIsla.Prodhan
"""
# import sys
# import os
# #import pdb  # Import the pdb debugger
#
# # Add the top-level directory (where sendqsarpy is located) to the system path
# #sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
#
# # Now you can import the function from your package
# from sendqsarpy.get_liver_om_lb_mi_tox_score_list import get_liver_om_lb_mi_tox_score_list
# from sendqsarpy.get_col_harmonized_scores_df  import get_col_harmonized_scores_df
# # Set the parameters you want to test
# db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/DATABASES/TestDB.db"
#
# #studyid_or_studyids =
# studyid_or_studyids = ["5003635"]
#
# # Call the function you want to debug
# lb_mi_litb_score = get_liver_om_lb_mi_tox_score_list(
#     studyid_or_studyids=studyid_or_studyids,
#     path_db=db_path,
#     fake_study=False,
#     use_xpt_file=False,
#     output_individual_scores=True,
#     output_zscore_by_USUBJID=False
# )

#print(lb_mi_litb_score)  # Observe the result

#-------------------------------------------------------------------
import sqlite3
import pandas as pd
from sendqsarpy.get_liver_om_lb_mi_tox_score_list import get_liver_om_lb_mi_tox_score_list
#from sendqsarpy.get_col_harmonized_scores_df  import get_col_harmonized_scores_df
import pdb 
# 1. Define the path to the SQLite database
path_db = 'C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/DATABASES/FAKE_DATA/fake_merged_liver_not_liver.db'

# 2. Establish a connection to the database
connection = sqlite3.connect(path_db)

# import sqlite3
# conn = sqlite3.connect(path_db)
# cursor = conn.cursor()
# cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
# tables = cursor.fetchall()
# print(tables)
# conn.close()


# 3. Execute the SQL query to retrieve the STUDYID column from the 'dm' table
query = "SELECT STUDYID FROM dm"
studyid_data = pd.read_sql_query(query, connection)

# 4. Extract unique STUDYID values
unique_studyids = studyid_data['STUDYID'].unique()

#unique_studyids = ['11094']

# 5. Close the database connection
connection.close()

# 6. Assign the unique IDs to a variable
studyid_or_studyids = unique_studyids

# Call the function with equivalent arguments
fake80_liver_scores = get_liver_om_lb_mi_tox_score_list(
    studyid_or_studyids=studyid_or_studyids,
    path_db=path_db,
    fake_study=True,
    use_xpt_file=False,
    output_individual_scores=True,
    output_zscore_by_USUBJID=False
)
print(fake80_liver_scores.shape)
# # Apply the function
# column_harmonized_liverscr_df = get_col_harmonized_scores_df(
#     liver_score_data_frame=fake80_liver_scores,
#     round_values=True
# )




# # Create a copy of the input DataFrame
# liver_scores = fake80_liver_scores.copy()

# # Replace spaces, commas, and slashes in column names with dots
# liver_scores.columns = liver_scores.columns.str.replace(' ', '.').str.replace(',', '.').str.replace('/', '.')

# # Replace NA values with 0
# liver_scores.fillna(0, inplace=True)

# # Identify columns with periods
# findings2replace_index = [i for i, col in enumerate(liver_scores.columns) if '.' in col]

# #breakpoint()

# # Identify unique column names without periods
# fn2replace = set(liver_scores.columns.str.upper()) - {liver_scores.columns[i].upper() for i in findings2replace_index}

# # Remove specific columns from processing
# remove_columns = {'STUDYID', 'UNREMARKABLE', 'THIKENING', 'POSITIVE'}
# fn2replace = fn2replace - remove_columns

# # Harmonize synonym columns
# for finding in fn2replace:
#     synonyms = [col for col in liver_scores.columns if finding in col.upper()]
#     for synonym in synonyms:
#         indices = liver_scores[synonym] > 0
#         liver_scores.loc[indices, finding] = liver_scores.loc[indices, [finding, synonym]].max(axis=1)



















