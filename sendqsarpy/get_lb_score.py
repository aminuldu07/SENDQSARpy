# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 08:55:40 2024

@author: MdAminulIsla.Prodhan
"""

import os
import sqlite3
import pandas as pd
import pyreadstat
from get_compiledata import get_compile_data

def get_lb_score(studyid=None, 
                 path_db=None, 
                 fake_study=False, 
                 use_xpt_file=False, 
                 master_compiledata=None, 
                 return_individual_scores=False, 
                 return_zscore_by_USUBJID=False):

    studyid = str(studyid)
    path = path_db

    # Helper function to fetch data from SQLite database
    def fetch_domain_data(db_connection, domain_name, studyid):
        domain_name = domain_name.upper()
        query_statement = f"SELECT * FROM {domain_name} WHERE STUDYID = ?"
        query_result = pd.read_sql_query(query_statement, db_connection, params=(studyid,))
        return query_result

    # Get the required domain data
    if use_xpt_file:
        # Read data from .xpt files using pyreadstat
        lb, meta = pyreadstat.read_xport(os.path.join(path, 'lb.xpt'))
    else:
        # Establish a connection to the SQLite database
        db_connection = sqlite3.connect(path)
        lb = fetch_domain_data(db_connection, 'lb', studyid)
        db_connection.close()

    # Check the lb data frame
    organ_testcd_list = {
        'LIVER': [
            'SERUM | ALT', 'SERUM | AST', 'SERUM | ALP', 'SERUM | GGT', 'SERUM | BILI',
            'SERUM | ALB', 'PLASMA | ALT', 'PLASMA | AST', 'PLASMA | ALP', 'PLASMA | GGT',
            'PLASMA | BILI', 'PLASMA | ALB', 'WHOLE BLOOD | ALT', 'WHOLE BLOOD | AST',
            'WHOLE BLOOD | ALP', 'WHOLE BLOOD | GGT', 'WHOLE BLOOD | BILI', 'WHOLE BLOOD | ALB'
        ]
    }

    # Make LBData DataFrame to hold information
    lb_data = pd.DataFrame(columns=["STUDYID", "USUBJID", "LBSPEC", "LBTESTCD", "LBSTRESN", "VISITDY"])

    # Filter the data for the current STUDYID
    study_data_lb = lb

    # Check if LBDY column exists and process accordingly
    if "LBDY" in study_data_lb.columns:
        lbd = study_data_lb[study_data_lb["LBDY"] >= 1][["STUDYID", "USUBJID", "LBSPEC", "LBTESTCD", "LBSTRESN", "LBDY"]]
        lbd.rename(columns={"LBDY": "VISITDY"}, inplace=True)

        # Convert LBCAT to LBSPEC if LBSPEC is NA
        if lbd["LBSPEC"].isna().all():
            lbd["LBSPEC"] = study_data_lb.loc[study_data_lb["LBDY"] >= 1, "LBCAT"]
            lbd["LBSPEC"] = lbd["LBSPEC"].replace(
                {"HEMATOLOGY": "WHOLE BLOOD", "Hematology": "WHOLE BLOOD", "hematology": "WHOLE BLOOD",
                 "CLINICAL CHEMISTRY": "SERUM", "Clinical Chemistry": "SERUM",
                 "URINALYSIS": "URINE", "Urinalysis": "URINE"})
    else:
        # If LBDY column does not exist, handle accordingly
        lbd = study_data_lb[study_data_lb["VISITDY"] >= 1][["STUDYID", "USUBJID", "LBSPEC", "LBTESTCD", "LBSTRESN", "VISITDY"]]

    # Add to lb_data
    lb_data = pd.concat([lb_data, lbd], ignore_index=True).dropna()

    # Concatenate LBSPEC and LBTESTCD
    lb_data["LBTESTCD"] = lb_data["LBSPEC"] + " | " + lb_data["LBTESTCD"]

    # Remove rows not matching tests from organ_testcd_list
    test_cleaned_lb_data = lb_data[lb_data["LBTESTCD"].isin(organ_testcd_list['LIVER'])]

    # Create a new DataFrame with the row having the max VISITDY for each USUBJID and LBTESTCD combination
    max_visitdy_df = (test_cleaned_lb_data.groupby(["USUBJID", "LBTESTCD"])
                      .apply(lambda x: x.loc[x["VISITDY"].idxmax()])
                      .reset_index(drop=True))

    # If master_compiledata is not provided, fetch it
    if master_compiledata is None:
        master_compiledata = get_compile_data(studyid=studyid if not use_xpt_file else None, 
                                              path_db=path_db, fake_study=fake_study, 
                                              use_xpt_file=use_xpt_file)

    # Filter out TK and Recovery animals using master_compiledata
    lb_tk_recovery_filtered = max_visitdy_df[max_visitdy_df["USUBJID"].isin(master_compiledata["USUBJID"])]

    # Perform an inner join to match USUBJID and get ARMCD
    lb_tk_recovery_filtered_armcd = lb_tk_recovery_filtered.merge(
        master_compiledata[["USUBJID", "ARMCD"]], on="USUBJID", how="inner")

        
    # helper funciton for calculation the zscore for each enzymes 
  
    def calculate_zscores(lb_tk_recovery_filtered_armcd, lbtestcd_groups):
        """
        Calculate Z-scores and assign toxicity scores for multiple LBTESTCD groups.

        Parameters:
            lb_tk_recovery_filtered_armcd (pd.DataFrame): The input DataFrame with lab data.
            lbtestcd_groups (dict): A dictionary mapping group names to LBTESTCD lists.

        Returns:
            dict: A dictionary with group names as keys and processed DataFrames as values.
        """
        result = {}
        
        for group_name, lbtestcd_list in lbtestcd_groups.items():
            # Filter data for the given LBTESTCD list
            df_filtered = lb_tk_recovery_filtered_armcd[
                lb_tk_recovery_filtered_armcd["LBTESTCD"].isin(lbtestcd_list)
            ]

            # Calculate mean and standard deviation for 'vehicle' group
            df_filtered[f"mean_vehicle_{group_name.lower()}"] = df_filtered.groupby("STUDYID")["LBSTRESN"].transform(
                lambda x: x[df_filtered["ARMCD"] == "vehicle"].mean())
            df_filtered[f"sd_vehicle_{group_name.lower()}"] = df_filtered.groupby("STUDYID")["LBSTRESN"].transform(
                lambda x: x[df_filtered["ARMCD"] == "vehicle"].std())
            
            # Compute Z-score
            df_filtered[f"{group_name.lower()}_zscore"] = abs(
                (df_filtered["LBSTRESN"] - df_filtered[f"mean_vehicle_{group_name.lower()}"]) /
                df_filtered[f"sd_vehicle_{group_name.lower()}"]
            )
            
            # Calculate final Z-score for 'HD' group
            final_zscore = (df_filtered[df_filtered["ARMCD"] == "HD"]
                            .groupby("STUDYID")
                            .agg(avg_zscore=(f"{group_name.lower()}_zscore", "mean"))
                            .reset_index())
            
            # Assign toxicity scores
            final_zscore["avg_zscore"] = final_zscore["avg_zscore"].apply(
                lambda x: 3 if x >= 3 else (2 if x >= 2 else (1 if x >= 1 else 0))
            )
            
            # Rename the avg_zscore column to include the group name
            final_zscore = final_zscore.rename(columns={"avg_zscore": f"avg_{group_name.lower()}_zscore"})

           # Select only STUDYID and the new avg_zscore column
            final_zscore = final_zscore[["STUDYID", f"avg_{group_name.lower()}_zscore"]]

            
            result[group_name] = final_zscore
        
        return result

       # Example Usage
    lbtestcd_groups = {
     "ALT": ['SERUM | ALT', 'PLASMA | ALT', 'WHOLE BLOOD | ALT'],
     "AST": ['SERUM | AST', 'PLASMA | AST', 'WHOLE BLOOD | AST'],
     "ALP": ['SERUM | ALP', 'PLASMA | ALP', 'WHOLE BLOOD | ALP'],
     "GGT": ['SERUM | GGT', 'PLASMA | GGT', 'WHOLE BLOOD | GGT'],
     "BILI": ['SERUM | BILI', 'PLASMA | BILI', 'WHOLE BLOOD | BILI'],
     "ALB": ['SERUM | ALB', 'PLASMA | ALB', 'WHOLE BLOOD | ALB']
     }

    # Call the function
    results = calculate_zscores(lb_tk_recovery_filtered_armcd, lbtestcd_groups)


    # Return the processed data
    return results



#Example usage
# Later in the script, where you want to call the function:
db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/2023-2024_projects/FAKE_DATABASES/fake_merged_liver_not_liver.db"

# Call the function
fake_T_xpt_F_lb_score = get_lb_score(studyid="28738",
                                         path_db=db_path, 
                                         fake_study=True, 
                                         use_xpt_file=False, 
                                         master_compiledata=None, 
                                         return_individual_scores=False, 
                                         return_zscore_by_USUBJID=False)
   
serum_alt_results = fake_T_xpt_F_lb_score["ALT"]
serum_ast_results = fake_T_xpt_F_lb_score["AST"]
serum_alp_results = fake_T_xpt_F_lb_score["ALP"]
serum_ggt_results = fake_T_xpt_F_lb_score["GGT"]
serum_bili_results = fake_T_xpt_F_lb_score["BILI"]
serum_alb_results = fake_T_xpt_F_lb_score["ALB"]






# #studyid="28738", path_db = db_path, fake_study=True, use_xpt_file=False)
# #(db_path, selected_studies)

# # Later in the script, where you want to call the function:
# db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/2023-2024_projects/FAKE_DATABASES/single_fake_xpt_folder/FAKE28738"

# # Call the function
# fake_T_xpt_T_lb_score = get_lb_score(studyid=None,
#                                          path_db=db_path, 
#                                          fake_study=True, 
#                                          use_xpt_file=True, 
#                                          master_compiledata=None, 
#                                          return_individual_scores=False, 
#                                          return_zscore_by_USUBJID=False)


# #(studyid=None, path_db = db_path, fake_study =True, use_xpt_file=True)

# # Later in the script, where you want to call the function:
# db_path = "C:\\Users\\MdAminulIsla.Prodhan\\OneDrive - FDA\\Documents\\TestDB.db"
# #selected_studies = "28738"  # Example list of selected studies

# # Call the function
# real_sqlite_lb_score = get_lb_score(studyid="876", path_db = db_path, fake_study=False, use_xpt_file=False)


# # Later in the script, where you want to call the function:
# db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/2023-2024_projects/FAKE_DATABASES/real_xpt_dir/IND051292_1017-3581"

# # Call the function
# real_xpt_lb_score = get_lb_score(studyid=None, path_db = db_path, fake_study =False, use_xpt_file=True)

##########################################################################################################
#############################################################################################################
##############################################################################################################
# # 1. Z-score calculation for 'SERUM | ALT'
# df_serum_alt = lb_tk_recovery_filtered_armcd[
#     lb_tk_recovery_filtered_armcd["LBTESTCD"].isin(['SERUM | ALT', 'PLASMA | ALT', 'WHOLE BLOOD | ALT'])]

# df_serum_alt["mean_vehicle_alt"] = df_serum_alt.groupby("STUDYID")["LBSTRESN"].transform(
#     lambda x: x[df_serum_alt["ARMCD"] == "vehicle"].mean())
# df_serum_alt["sd_vehicle_alt"] = df_serum_alt.groupby("STUDYID")["LBSTRESN"].transform(
#     lambda x: x[df_serum_alt["ARMCD"] == "vehicle"].std())
# df_serum_alt["alt_zscore"] = abs((df_serum_alt["LBSTRESN"] - df_serum_alt["mean_vehicle_alt"]) / df_serum_alt["sd_vehicle_alt"])

# serum_alt_final_zscore = (df_serum_alt[df_serum_alt["ARMCD"] == "HD"]
#                           .groupby("STUDYID")
#                           .agg(avg_alt_zscore=("alt_zscore", "mean"))
#                           .reset_index())
# serum_alt_final_zscore["avg_alt_zscore"] = serum_alt_final_zscore["avg_alt_zscore"].apply(
#     lambda x: 3 if x >= 3 else (2 if x >= 2 else (1 if x >= 1 else 0)))

# # 2. Z-score calculation for 'SERUM | AST'
# df_serum_ast = lb_tk_recovery_filtered_armcd[
#     lb_tk_recovery_filtered_armcd["LBTESTCD"].isin(['SERUM | AST', 'PLASMA | AST', 'WHOLE BLOOD | AST'])]

# df_serum_ast["mean_vehicle_ast"] = df_serum_ast.groupby("STUDYID")["LBSTRESN"].transform(
#     lambda x: x[df_serum_ast["ARMCD"] == "vehicle"].mean())
# df_serum_ast["sd_vehicle_ast"] = df_serum_ast.groupby("STUDYID")["LBSTRESN"].transform(
#     lambda x: x[df_serum_ast["ARMCD"] == "vehicle"].std())
# df_serum_ast["ast_zscore"] = abs((df_serum_ast["LBSTRESN"] - df_serum_ast["mean_vehicle_ast"]) / df_serum_ast["sd_vehicle_ast"])

# serum_ast_final_zscore = (df_serum_ast[df_serum_ast["ARMCD"] == "HD"]
#                           .groupby("STUDYID")
#                           .agg(avg_ast_zscore=("ast_zscore", "mean"))
#                           .reset_index())
# serum_ast_final_zscore["avg_ast_zscore"] = serum_ast_final_zscore["avg_ast_zscore"].apply(
#     lambda x: 3 if x >= 3 else (2 if x >= 2 else (1 if x >= 1 else 0)))

# # 3. Z-score calculation for 'SERUM | ALP'
# df_serum_alp = lb_tk_recovery_filtered_armcd[
#     lb_tk_recovery_filtered_armcd["LBTESTCD"].isin(['SERUM | ALP', 'PLASMA | ALP', 'WHOLE BLOOD | ALP'])]

# df_serum_alp["mean_vehicle_alp"] = df_serum_alp.groupby("STUDYID")["LBSTRESN"].transform(
#     lambda x: x[df_serum_alp["ARMCD"] == "vehicle"].mean())
# df_serum_alp["sd_vehicle_alp"] = df_serum_alp.groupby("STUDYID")["LBSTRESN"].transform(
#     lambda x: x[df_serum_alp["ARMCD"] == "vehicle"].std())
# df_serum_alp["alp_zscore"] = abs((df_serum_alp["LBSTRESN"] - df_serum_alp["mean_vehicle_alp"]) / df_serum_alp["sd_vehicle_alp"])

# serum_alp_final_zscore = (df_serum_alp[df_serum_alp["ARMCD"] == "HD"]
#                           .groupby("STUDYID")
#                           .agg(avg_alp_zscore=("alp_zscore", "mean"))
#                           .reset_index())
# serum_alp_final_zscore["avg_alp_zscore"] = serum_alp_final_zscore["avg_alp_zscore"].apply(
#     lambda x: 3 if x >= 3 else (2 if x >= 2 else (1 if x >= 1 else 0)))