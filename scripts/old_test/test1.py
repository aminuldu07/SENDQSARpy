import sqlite3
import pandas as pd
from sendqsarpy.get_liver_om_lb_mi_tox_score_list import get_liver_om_lb_mi_tox_score_list
db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/DATABASES/TestDB.db"
# 2. Establish a connection to the database
connection = sqlite3.connect(path_db)

#studyid_or_studyids =
studyid_or_studyids = ["5003635"]

# Call the function you want to debug
lb_mi_litb_score = get_liver_om_lb_mi_tox_score_list(
    studyid_or_studyids=studyid_or_studyids,
    path_db=db_path,
    fake_study=False,
    use_xpt_file=False,
    output_individual_scores=True,
    output_zscore_by_USUBJID=False
)