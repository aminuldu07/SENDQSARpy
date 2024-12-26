import sqlite3
import pandas as pd
#from sendqsarpy.get_compile_data import get_compile_data
from sendqsarpy.get_mi_score import get_mi_score
from sendqsarpy.get_liver_om_lb_mi_tox_score_list import get_liver_om_lb_mi_tox_score_list
from sendqsarpy.get_col_harmonized_scores_df  import get_col_harmonized_scores_df
from sendqsarpy.prepare_data_and_tune_hyperparameters import prepare_data_and_tune_hyperparameters

import pdb 
# 1. Define the path to the SQLite database
path_db = 'C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/DATABASES/fake_merged_liver_not_liver.db'
studyid = "10663"

# # Set the parameters you want to test
# path_db = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/DATABASES/TestDB.db"
#
# #studyid_or_studyids =
# studyid = "2170017"

mi_score = get_mi_score(studyid=studyid,
                 path_db=path_db,
                 fake_study=True,
                 use_xpt_file=False,
                 master_compiledata=None,
                 return_individual_scores=True,
                 return_zscore_by_USUBJID=False)
print(mi_score.shape)
# studyid_or_studyids=["2170017"]

# for single studyid
#studyid_or_studyids=["10663"]

# For multiple studyids
# 1. Define the path to the SQLite database
path_db = 'C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/DATABASES/fake_merged_liver_not_liver.db'

# 2. Establish a connection to the database
connection = sqlite3.connect(path_db)

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

fake80_liver_scores = get_liver_om_lb_mi_tox_score_list(
    studyid_or_studyids=studyid_or_studyids,
    path_db=path_db,
    fake_study=True,
    use_xpt_file=False,
    output_individual_scores=True,
    output_zscore_by_USUBJID=False
)
column_harmonized_liverscr_df = get_col_harmonized_scores_df(liver_score_data_frame = fake80_liver_scores,
                                                              round_values = True)

# print(column_harmonized_liverscr_df)
#
# # #Enable Python Console Execution
# # import code
# # code.interact(local=locals())
# Read the CSV file into a DataFrame
fake_80_metadata = pd.read_csv("C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/DATABASES/fake_80_MD.csv",
                               header=0,  # This is the default and indicates the first row is the header
                               dtype=str)  # Use dtype=str to ensure all columns are read as strings (like R's stringsAsFactors = FALSE)

rfData_and_best_m = prepare_data_and_tune_hyperparameters(scores_df = column_harmonized_liverscr_df,
                                          studyid_metadata=fake_80_metadata ,
                                          Impute=True,
                                          Round=True,
                                          reps=1,
                                          holdback=0.25,
                                          Undersample=True,
                                          hyperparameter_tuning=False,
                                          error_correction_method=None)