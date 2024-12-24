# -*- coding: utf-8 -*-
"""
Created on Sun Dec 22 15:36:52 2024

@author: MdAminulIsla.Prodhan
"""
import sys
import os
#import pdb  # Import the pdb debugger

# Add the top-level directory (where sendqsarpy is located) to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Now you can import the function from your package
from sendqsarpy.get_liver_om_lb_mi_tox_score_list import get_liver_om_lb_mi_tox_score_list

# Set the parameters you want to test
studyid_or_studyids = ["5003635"]
db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/DATABASES/TestDB.db"



# Call the function you want to debug
lb_mi_litb_score = get_liver_om_lb_mi_tox_score_list(
    studyid_or_studyids=studyid_or_studyids,
    path_db=db_path,
    fake_study=False,
    use_xpt_file=False,
    output_individual_scores=True,
    output_zscore_by_USUBJID=False
)

#print(lb_mi_litb_score)  # Observe the result

