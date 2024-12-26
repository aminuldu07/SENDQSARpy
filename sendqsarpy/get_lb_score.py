# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 08:55:40 2024

@author: MdAminulIsla.Prodhan
"""

import os
import sqlite3
import pandas as pd
import numpy as np
import pyreadstat
from .get_compile_data import get_compile_data

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
    LB_tk_recovery_filtered_ARMCD = lb_tk_recovery_filtered.merge(
        master_compiledata[["USUBJID", "ARMCD"]], on="USUBJID", how="inner")

    
    # 1. Z-score calculation for 'SERUM | ALT'------------------------------
    df_serum_alt = LB_tk_recovery_filtered_ARMCD[
        LB_tk_recovery_filtered_ARMCD["LBTESTCD"].isin(['SERUM | ALT', 'PLASMA | ALT', 'WHOLE BLOOD | ALT'])]

    df_serum_alt["mean_vehicle_alt"] = df_serum_alt.groupby("STUDYID")["LBSTRESN"].transform(
        lambda x: x[df_serum_alt["ARMCD"] == "vehicle"].mean())
    df_serum_alt["sd_vehicle_alt"] = df_serum_alt.groupby("STUDYID")["LBSTRESN"].transform(
        lambda x: x[df_serum_alt["ARMCD"] == "vehicle"].std())
    df_serum_alt["alt_zscore"] = abs((df_serum_alt["LBSTRESN"] - df_serum_alt["mean_vehicle_alt"]) / df_serum_alt["sd_vehicle_alt"])

    serum_alt_final_zscore = (df_serum_alt[df_serum_alt["ARMCD"] == "HD"]
                              .groupby("STUDYID")
                              .agg(avg_alt_zscore=("alt_zscore", "mean"))
                              .reset_index())
    serum_alt_final_zscore["avg_alt_zscore"] = serum_alt_final_zscore["avg_alt_zscore"].apply(
        lambda x: 3 if x >= 3 else (2 if x >= 2 else (1 if x >= 1 else 0)))

    # 2. Z-score calculation for 'SERUM | AST'------------------------------
    df_serum_ast = LB_tk_recovery_filtered_ARMCD[
        LB_tk_recovery_filtered_ARMCD["LBTESTCD"].isin(['SERUM | AST', 'PLASMA | AST', 'WHOLE BLOOD | AST'])]

    df_serum_ast["mean_vehicle_ast"] = df_serum_ast.groupby("STUDYID")["LBSTRESN"].transform(
        lambda x: x[df_serum_ast["ARMCD"] == "vehicle"].mean())
    df_serum_ast["sd_vehicle_ast"] = df_serum_ast.groupby("STUDYID")["LBSTRESN"].transform(
        lambda x: x[df_serum_ast["ARMCD"] == "vehicle"].std())
    df_serum_ast["ast_zscore"] = abs((df_serum_ast["LBSTRESN"] - df_serum_ast["mean_vehicle_ast"]) / df_serum_ast["sd_vehicle_ast"])

    serum_ast_final_zscore = (df_serum_ast[df_serum_ast["ARMCD"] == "HD"]
                              .groupby("STUDYID")
                              .agg(avg_ast_zscore=("ast_zscore", "mean"))
                              .reset_index())
    serum_ast_final_zscore["avg_ast_zscore"] = serum_ast_final_zscore["avg_ast_zscore"].apply(
        lambda x: 3 if x >= 3 else (2 if x >= 2 else (1 if x >= 1 else 0)))

    # 3. Z-score calculation for 'SERUM | ALP'-------------------------------
    df_serum_alp = LB_tk_recovery_filtered_ARMCD[
        LB_tk_recovery_filtered_ARMCD["LBTESTCD"].isin(['SERUM | ALP', 'PLASMA | ALP', 'WHOLE BLOOD | ALP'])]

    df_serum_alp["mean_vehicle_alp"] = df_serum_alp.groupby("STUDYID")["LBSTRESN"].transform(
        lambda x: x[df_serum_alp["ARMCD"] == "vehicle"].mean())
    df_serum_alp["sd_vehicle_alp"] = df_serum_alp.groupby("STUDYID")["LBSTRESN"].transform(
        lambda x: x[df_serum_alp["ARMCD"] == "vehicle"].std())
    df_serum_alp["alp_zscore"] = abs((df_serum_alp["LBSTRESN"] - df_serum_alp["mean_vehicle_alp"]) / df_serum_alp["sd_vehicle_alp"])

    serum_alp_final_zscore = (df_serum_alp[df_serum_alp["ARMCD"] == "HD"]
                              .groupby("STUDYID")
                              .agg(avg_alp_zscore=("alp_zscore", "mean"))
                              .reset_index())
    serum_alp_final_zscore["avg_alp_zscore"] = serum_alp_final_zscore["avg_alp_zscore"].apply(
        lambda x: 3 if x >= 3 else (2 if x >= 2 else (1 if x >= 1 else 0)))
    
    # 4. Z-score calculation for 'SERUM | GGT'------------------------------
    df_serum_ggt = LB_tk_recovery_filtered_ARMCD[
        LB_tk_recovery_filtered_ARMCD["LBTESTCD"].isin(['SERUM | GGT', 'PLASMA | GGT', 'WHOLE BLOOD | GGT'])]

    df_serum_ggt["mean_vehicle_ggt"] = df_serum_ggt.groupby("STUDYID")["LBSTRESN"].transform(
        lambda x: x[df_serum_ggt["ARMCD"] == "vehicle"].mean())
    df_serum_ggt["sd_vehicle_ggt"] = df_serum_ggt.groupby("STUDYID")["LBSTRESN"].transform(
        lambda x: x[df_serum_ggt["ARMCD"] == "vehicle"].std())
    df_serum_ggt["ggt_zscore"] = abs((df_serum_ggt["LBSTRESN"] - df_serum_ggt["mean_vehicle_ggt"]) / df_serum_ggt["sd_vehicle_ggt"])

    serum_ggt_final_zscore = (df_serum_ggt[df_serum_ggt["ARMCD"] == "HD"]
                              .groupby("STUDYID")
                              .agg(avg_ggt_zscore=("ggt_zscore", "mean"))
                              .reset_index())
    serum_ggt_final_zscore["avg_ggt_zscore"] = serum_ggt_final_zscore["avg_ggt_zscore"].apply(
        lambda x: 3 if x >= 3 else (2 if x >= 2 else (1 if x >= 1 else 0)))
    
    # 5. Z-score calculation for 'SERUM | BILI'------------------------------
    df_serum_bili = LB_tk_recovery_filtered_ARMCD[
        LB_tk_recovery_filtered_ARMCD["LBTESTCD"].isin(['SERUM | BILI', 'PLASMA | BILI', 'WHOLE BLOOD | BILI'])]

    df_serum_bili["mean_vehicle_bili"] = df_serum_bili.groupby("STUDYID")["LBSTRESN"].transform(
        lambda x: x[df_serum_bili["ARMCD"] == "vehicle"].mean())
    df_serum_bili["sd_vehicle_bili"] = df_serum_bili.groupby("STUDYID")["LBSTRESN"].transform(
        lambda x: x[df_serum_bili["ARMCD"] == "vehicle"].std())
    df_serum_bili["bili_zscore"] = abs((df_serum_bili["LBSTRESN"] - df_serum_bili["mean_vehicle_bili"]) / df_serum_bili["sd_vehicle_bili"])

    serum_bili_final_zscore = (df_serum_bili[df_serum_bili["ARMCD"] == "HD"]
                              .groupby("STUDYID")
                              .agg(avg_bili_zscore=("bili_zscore", "mean"))
                              .reset_index())
    serum_bili_final_zscore["avg_bili_zscore"] = serum_bili_final_zscore["avg_bili_zscore"].apply(
        lambda x: 3 if x >= 3 else (2 if x >= 2 else (1 if x >= 1 else 0)))
    
    
    
    # 6. Z-score calculation for 'SERUM | ALB'------------------------------
    df_serum_alb = LB_tk_recovery_filtered_ARMCD[
        LB_tk_recovery_filtered_ARMCD["LBTESTCD"].isin(['SERUM | ALB', 'PLASMA | ALB', 'WHOLE BLOOD | ALB'])]

    df_serum_alb["mean_vehicle_alb"] = df_serum_alb.groupby("STUDYID")["LBSTRESN"].transform(
        lambda x: x[df_serum_alb["ARMCD"] == "vehicle"].mean())
    df_serum_alb["sd_vehicle_alb"] = df_serum_alb.groupby("STUDYID")["LBSTRESN"].transform(
        lambda x: x[df_serum_alb["ARMCD"] == "vehicle"].std())
    df_serum_alb["alb_zscore"] = abs((df_serum_alb["LBSTRESN"] - df_serum_alb["mean_vehicle_alb"]) / df_serum_alb["sd_vehicle_alb"])

    serum_alb_final_zscore = (df_serum_alb[df_serum_alb["ARMCD"] == "HD"]
                              .groupby("STUDYID")
                              .agg(avg_alb_zscore=("alb_zscore", "mean"))
                              .reset_index())
    serum_alb_final_zscore["avg_alb_zscore"] = serum_alb_final_zscore["avg_alb_zscore"].apply(
        lambda x: 3 if x >= 3 else (2 if x >= 2 else (1 if x >= 1 else 0)))

    # Perform a full join (similar to SQL FULL OUTER JOIN) for the dataframes
    LB_zscore_merged_df = pd.merge(serum_alb_final_zscore, serum_ast_final_zscore, on="STUDYID", how="outer")
    LB_zscore_merged_df = pd.merge(LB_zscore_merged_df, serum_alp_final_zscore, on="STUDYID", how="outer")
    LB_zscore_merged_df = pd.merge(LB_zscore_merged_df, serum_alt_final_zscore, on="STUDYID", how="outer")
    LB_zscore_merged_df = pd.merge(LB_zscore_merged_df, serum_bili_final_zscore, on="STUDYID", how="outer")
    LB_zscore_merged_df = pd.merge(LB_zscore_merged_df, serum_ggt_final_zscore, on="STUDYID", how="outer")
    
    # Replace Inf, -Inf, and NaN with NaN in the selected columns
    selected_cols = ["avg_alb_zscore", "avg_ast_zscore", "avg_alp_zscore", 
                     "avg_alt_zscore", "avg_bili_zscore", "avg_ggt_zscore"]
    
    # Replace infinite values with NaN
    LB_zscore_merged_df[selected_cols] = LB_zscore_merged_df[selected_cols].applymap(
        lambda x: np.nan if np.isinf(x) or pd.isna(x) else x
    )
    
    
    # Enforce mutual exclusivity: If both are TRUE, raise an error
    if return_individual_scores and return_zscore_by_USUBJID:
        raise ValueError("Error: Both 'return_individual_scores' and 'return_zscore_by_USUBJID' cannot be TRUE at the same time.")
    
    

    if return_individual_scores:
        # Master LB list
        master_LB_list = pd.DataFrame({
        "STUDYID": [np.nan],
        "avg_alb_zscore": [np.nan],
        "avg_ast_zscore": [np.nan],
        "avg_alp_zscore": [np.nan],
        "avg_alt_zscore": [np.nan],
        "avg_bili_zscore": [np.nan],
        "avg_ggt_zscore": [np.nan]
        })

        # Add LB_zscore_merged_df to master dataframe
        master_LB_list = pd.concat([master_LB_list, LB_zscore_merged_df], ignore_index=True)

        # Remove the first row
        master_lb_scores = master_LB_list.iloc[1:]

    elif return_zscore_by_USUBJID:
        # Calculate the zscore for all USUBJIDs
        df_lb_for_zscore = LB_tk_recovery_filtered_ARMCD
        
        # Group by STUDYID and calculate mean and standard deviation for 'vehicle' group
        zscore_lb = df_lb_for_zscore.copy()
        zscore_lb["mean_vehicle"] = zscore_lb.groupby("STUDYID")["LBSTRESN"].transform(
            lambda x: x[zscore_lb.loc[x.index, "ARMCD"] == "vehicle"].mean(skipna=True)
            )
        zscore_lb["sd_vehicle"] = zscore_lb.groupby("STUDYID")["LBSTRESN"].transform(
            lambda x: x[zscore_lb.loc[x.index, "ARMCD"] == "vehicle"].std(skipna=True)
            )
            
        # Calculate the z-score
        zscore_lb["LB_zscore"] = (zscore_lb["LBSTRESN"] - zscore_lb["mean_vehicle"]) / zscore_lb["sd_vehicle"]
        
        # Filter for 'HD' group
        LB_zscore_by_USUBJID_HD = zscore_lb[zscore_lb["ARMCD"] == "HD"].copy()
        
        # Replace infinite z-scores with NaN
        LB_zscore_by_USUBJID_HD["LB_zscore"].replace([np.inf, -np.inf], np.nan, inplace=True)
        
        # Convert z-scores to absolute values
        LB_zscore_by_USUBJID_HD["LB_zscore"] = LB_zscore_by_USUBJID_HD["LB_zscore"].abs()
        
        # Map z-scores to toxicity scores
        LB_zscore_by_USUBJID_HD["LB_zscore"] = LB_zscore_by_USUBJID_HD["LB_zscore"].apply(
            lambda x: 3 if x >= 3 else (2 if x >= 2 else (1 if x >= 1 else 0))
            )
        
        # Select final columns and convert to a regular DataFrame
        LB_zscore_by_USUBJID_HD = LB_zscore_by_USUBJID_HD[["STUDYID", "USUBJID", "LB_zscore"]]
        
    else:
        # Handle case when return_individual_scores == False and return_zscore_by_USUBJID == False
        # Calculate the average for each row, ignoring NaN values
        LB_zscore_merged_df["avg_all_LB_zscores"] = LB_zscore_merged_df[selected_cols].mean(axis=1, skipna=True)

        # Select the specific columns for calculation
        LB_all_liver_zscore_averaged = LB_zscore_merged_df[["STUDYID", "avg_all_LB_zscores"]].copy()

        # Assign the new variable
        LB_final_score = LB_all_liver_zscore_averaged

        # Rename column
        averaged_LB_score = LB_final_score.rename(columns={"avg_all_LB_zscores": "LB_score_avg"})

        # Ensure it is a regular DataFrame
        averaged_LB_score = averaged_LB_score.reset_index(drop=True)

    
    ####################################################
    # Check if individual scores are requested
    if return_individual_scores:

        # Master LB list
        master_LB_list = pd.DataFrame({
            'STUDYID': [None], 'avg_alb_zscore': [None], 'avg_ast_zscore': [None], 'avg_alp_zscore': [None],
            'avg_alt_zscore': [None], 'avg_bili_zscore': [None], 'avg_ggt_zscore': [None]
            })
        
        # Add LB_zscore_merged_df to master dataframe
        master_LB_list = pd.concat([master_LB_list, LB_zscore_merged_df], ignore_index=True)
        
        # Remove the first row
        master_lb_scores = master_LB_list.drop(index=0)
        
        # If z-scores by USUBJID are requested
    elif return_zscore_by_USUBJID:
        
        # Sub-setting for 'SERUM | ALT' data frame for "LBzScore Calculation"
        df_lb_for_zscore = LB_tk_recovery_filtered_ARMCD
            
        # Calculate the z-score
        zscore_lb = df_lb_for_zscore.groupby('STUDYID').apply(
            lambda group: group.assign(
                mean_vehicle=group.loc[group['ARMCD'] == 'vehicle', 'LBSTRESN'].mean(),
                sd_vehicle=group.loc[group['ARMCD'] == 'vehicle', 'LBSTRESN'].std()
                )
            )
        
        zscore_lb['LB_zscore'] = (zscore_lb['LBSTRESN'] - zscore_lb['mean_vehicle']) / zscore_lb['sd_vehicle']
        
        # Filter for HD and process LB z-scores
        LB_zscore_by_USUBJID_HD = zscore_lb[zscore_lb['ARMCD'] == 'HD'].copy()
        LB_zscore_by_USUBJID_HD['LB_zscore'] = LB_zscore_by_USUBJID_HD['LB_zscore'].replace(
            [float('inf'), float('-inf')], None
            )
        LB_zscore_by_USUBJID_HD['LB_zscore'] = LB_zscore_by_USUBJID_HD['LB_zscore'].abs()
        
        LB_zscore_by_USUBJID_HD = LB_zscore_by_USUBJID_HD[['STUDYID', 'USUBJID', 'LB_zscore']]
        LB_zscore_by_USUBJID_HD['LB_zscore'] = LB_zscore_by_USUBJID_HD['LB_zscore'].apply(
            lambda x: 3 if x >= 3 else (2 if x >= 2 else (1 if x >= 1 else 0))
            )
        
        # Convert to regular DataFrame
        LB_zscore_by_USUBJID_HD = LB_zscore_by_USUBJID_HD.reset_index(drop=True)
        
        # Handle case when neither individual scores nor z-scores by USUBJID are requested
    else:
        
        # Calculate the average for each row, ignoring NA values
        LB_zscore_merged_df['avg_all_LB_zscores'] = LB_zscore_merged_df[selected_cols].mean(axis=1, skipna=True)
        
        # Select the specific columns for calculation
        LB_all_liver_zscore_averaged = LB_zscore_merged_df[['STUDYID', 'avg_all_LB_zscores']]
        
        # Assign the new variables
        LB_final_score = LB_all_liver_zscore_averaged
        
        # Create "LB_df" for FOUR_Liver_Score
        averaged_LB_score = LB_final_score.rename(columns={'avg_all_LB_zscores': 'LB_score_avg'})
        
        averaged_LB_score = averaged_LB_score.reset_index(drop=True)

    # Check conditions and return based on input flags
    if return_individual_scores:
        # If individual scores are requested, return master_lb_scores
        return master_lb_scores

    elif return_zscore_by_USUBJID:
        # If z-scores by USUBJID are requested, return LB_zscore_by_USUBJID_HD
        return LB_zscore_by_USUBJID_HD

    else:
        # Handle the case where neither individual scores nor z-scores by USUBJID are requested
        return averaged_LB_score

