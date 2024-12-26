import sqlite3
import pandas as pd
from sendqsarpy.get_liver_om_lb_mi_tox_score_list import get_liver_om_lb_mi_tox_score_list
from sendqsarpy.get_col_harmonized_scores_df  import get_col_harmonized_scores_df
import pdb 
# 1. Define the path to the SQLite database
#path_db = 'C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/DATABASES/fake_merged_liver_not_liver.db'

#from sendqsarpy.get_compile_data import get_compile_data
from sendqsarpy.get_mi_score import get_mi_score
# Set the parameters you want to test
db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/DATABASES/TestDB.db"

path_db=db_path
 
#studyid_or_studyids =
studyid = "2170017"

mi_score = get_mi_score(studyid=studyid,
                 path_db=db_path,
                 fake_study=False,
                 use_xpt_file=False,
                 master_compiledata=None,
                 return_individual_scores=True,
                 return_zscore_by_USUBJID=False)
print(mi_score.shape)
studyid_or_studyids=["2170017"]
fake80_liver_scores = get_liver_om_lb_mi_tox_score_list(
    studyid_or_studyids=studyid_or_studyids,
    path_db=path_db,
    fake_study=False,
    use_xpt_file=False,
    output_individual_scores=True,
    output_zscore_by_USUBJID=False
)
column_harmonized_liverscr_df = get_col_harmonized_scores_df(liver_score_data_frame = fake80_liver_scores,
                                                              round_values = True)