# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 08:55:40 2024

@author: MdAminulIsla.Prodhan
"""

import os
import sqlite3
import pandas as pd
import pyreadstat

def get_lb_score(studyid=None, path_db=None, fake_study=False, use_xpt_file=False, 
                 master_compiledata=None, return_individual_scores=False, return_zscore_by_USUBJID=False):

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

    # Filter out TK and Recovery animals
    lb_tk_recovery_filtered = max_visitdy_df[max_visitdy_df["USUBJID"].isin(master_compiledata["USUBJID"])]

    # Perform an inner join to match USUBJID and get ARMCD
    lb_tk_recovery_filtered_armcd = lb_tk_recovery_filtered.merge(
        master_compiledata[["USUBJID", "ARMCD"]], on="USUBJID", how="inner")

    # Z-score calculation for 'SERUM | ALT'
    df_serum_alt = lb_tk_recovery_filtered_armcd[
        lb_tk_recovery_filtered_armcd["LBTESTCD"].isin(['SERUM | ALT', 'PLASMA | ALT', 'WHOLE BLOOD | ALT'])]
    
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

    # Repeat for 'SERUM | AST' and 'SERUM | ALP' as needed...

    # Return the processed data
    return serum_alt_final_zscore

# Ensure to replace `get_compile_data` with the appropriate function in your code.
