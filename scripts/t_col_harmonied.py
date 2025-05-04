import sqlite3
import pandas as pd
from sendqsarpy.get_liver_om_lb_mi_tox_score_list import get_liver_om_lb_mi_tox_score_list
from sendqsarpy.get_col_harmonized_scores_df import get_col_harmonized_scores_df
from sendqsarpy.prepare_data_and_tune_hyperparameters import prepare_data_and_tune_hyperparameters
#from sendqsarpy.get_col_harmonized_scores_df  import get_col_harmonized_scores_df
import pdb
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

# now call the col-harmonization function
print("----------------------------------------------------------------")
data_col_harmonized =  get_col_harmonized_scores_df(liver_score_data_frame = fake80_liver_scores ,
                                 round_values=False)

print(data_col_harmonized.shape)
print(data_col_harmonized)
print(" data col harmonized finished")

studyid_metadata = pd.read_csv
mldata = prepare_data_and_tune_hyperparameters(scores_df = data_col_harmonized,
                                          studyid_metadata = ,
                                          Impute=False,
                                          Round=False,
                                          reps=0,
                                          holdback=0.75,
                                          Undersample=False,
                                          hyperparameter_tuning=False,
                                          error_correction_method=None)