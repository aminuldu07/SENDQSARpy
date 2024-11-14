# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 08:47:16 2024

@author: MdAminulIsla.Prodhan
"""

import pandas as pd
import pyreadstat
import sqlite3
import numpy as np


def get_livertobw_score(studyid=None, 
                        path_db=None, 
                        fake_study=False, 
                        use_xpt_file=False,
                        master_compiledata=None, 
                        bwzscore_BW=None,
                        return_individual_scores=False, 
                        return_zscore_by_USUBJID=False):
    
    # Helper function to fetch data from SQLite database
    def fetch_domain_data(db_connection, domain_name, studyid):
        domain_name = domain_name.upper()
        query_statement = f"SELECT * FROM {domain_name} WHERE STUDYID = ?"
        query_result = pd.read_sql_query(query_statement, db_connection, params=(studyid,))
        return query_result

    # Read data from .xpt files or SQLite database
    if use_xpt_file:
        # Reading data using pyreadstat
        om, meta = pyreadstat.read_xpt(f"{path_db}/om.xpt")
    else:
        # Establish a connection to the SQLite database
        db_connection = sqlite3.connect(path_db)
        # Fetch data for required domains
        om = fetch_domain_data(db_connection, 'om', studyid)
        # Close the database connection
        db_connection.close()

    # Get master_compiledata if not provided
    if master_compiledata is None:
        studyid = None if use_xpt_file else studyid
        master_compiledata = get_compile_data(studyid=studyid, path_db=path_db, fake_study=fake_study,
                                              use_xpt_file=use_xpt_file)

    # Get bwzscore_BW if not provided
    if bwzscore_BW is None:
        studyid = None if use_xpt_file else studyid
        bwzscore_BW = get_bw_score(studyid=studyid, path_db=path_db, fake_study=fake_study,
                                   use_xpt_file=use_xpt_file, master_compiledata=master_compiledata,
                                   return_individual_scores=True, return_zscore_by_USUBJID=False)

    # Initialize OrganWeights_Liver DataFrame
    OrganWeights_Liver = pd.DataFrame(columns=["USUBJID", "OMSPEC", "OMSTRESN", "OMTEST"])

    # Extract data for the current STUDYID
    StudyData_current_liver = om

    # Pull index of the LIVER data
    Studyidx_liver = StudyData_current_liver['OMSPEC'].str.contains("LIVER", case=False, na=False)
    
    # Pull relevant OM Data for LIVER
    OMD_liver = StudyData_current_liver.loc[Studyidx_liver, ["USUBJID", "OMSPEC", "OMSTRESN", "OMTEST"]]

    # Append to the OrganWeights_Liver DataFrame
    OrganWeights_Liver = pd.concat([OrganWeights_Liver, OMD_liver], ignore_index=True)

    # Filter the OrganWeights_Liver DataFrame for Weight
    OrganWeights_Liver_Weight = OrganWeights_Liver[OrganWeights_Liver["OMTEST"] == "Weight"]

    # Select specific columns
    OrganWeights_Liver_Weight_Selected_Col = OrganWeights_Liver_Weight[["USUBJID", "OMSTRESN"]]

    # Filter for USUBJID in master_compiledata
    OrganWeights_Liver_filtered = OrganWeights_Liver_Weight_Selected_Col[
        OrganWeights_Liver_Weight_Selected_Col["USUBJID"].isin(master_compiledata["USUBJID"])
    ]

    # Perform a left join to match USUBJID and get ARMCD
    OrganWeights_Liver_with_ARMCD = OrganWeights_Liver_filtered.merge(
        master_compiledata[["STUDYID", "USUBJID", "ARMCD"]], on="USUBJID", how="left"
    )

    # Add "BodyWeight" data and calculate liverToBW
    OrganWeights_Liver_to_BWeight = OrganWeights_Liver_with_ARMCD.merge(
        bwzscore_BW[["USUBJID", "finalbodyweight"]], on="USUBJID", how="left"
    )
    OrganWeights_Liver_to_BWeight["liverToBW"] = OrganWeights_Liver_to_BWeight["OMSTRESN"] / OrganWeights_Liver_to_BWeight["finalbodyweight"]

    # Calculate liverToBW z-score
    liver_zscore_df = OrganWeights_Liver_to_BWeight.groupby("STUDYID").apply(
        lambda x: x.assign(
            liverToBW=lambda df: df["liverToBW"].replace([np.inf, -np.inf], np.nan),
            mean_vehicle_liverToBW=x.loc[x["ARMCD"] == "vehicle", "liverToBW"].mean(skipna=True),
            sd_vehicle_liverToBW=x.loc[x["ARMCD"] == "vehicle", "liverToBW"].std(skipna=True)
        )
    ).reset_index(drop=True)
    
    liver_zscore_df["liverToBW_zscore"] = abs((liver_zscore_df["liverToBW"] - liver_zscore_df["mean_vehicle_liverToBW"]) / liver_zscore_df["sd_vehicle_liverToBW"])
    liver_zscore_df = liver_zscore_df[["STUDYID", "USUBJID", "liverToBW_zscore", "ARMCD"]]

    # Filter for "HD" ARMCD
    HD_liver_zscore = liver_zscore_df[liver_zscore_df["ARMCD"] == "HD"]

    # Error handling for mutual exclusivity
    if return_individual_scores and return_zscore_by_USUBJID:
        raise ValueError("Error: Both 'return_individual_scores' and 'return_zscore_by_USUBJID' cannot be TRUE at the same time.")

    if return_individual_scores:
        HD_liver_zscore_df = HD_liver_zscore.groupby("STUDYID").agg(
            avg_liverToBW_zscore=("liverToBW_zscore", lambda x: abs(x.replace([np.inf, -np.inf], np.nan).mean(skipna=True)))
        ).reset_index()
        HD_liver_zscore_df["avg_liverToBW_zscore"] = HD_liver_zscore_df["avg_liverToBW_zscore"].apply(
            lambda x: 3 if x >= 3 else 2 if x >= 2 else 1 if x >= 1 else 0
        )
        return HD_liver_zscore_df

    elif return_zscore_by_USUBJID:
        liverTOBW_zscore_by_USUBJID_HD = HD_liver_zscore.copy()
        liverTOBW_zscore_by_USUBJID_HD["liverToBW_zscore"] = liverTOBW_zscore_by_USUBJID_HD["liverToBW_zscore"].apply(
            lambda x: 3 if x >= 3 else 2 if x >= 2 else 1 if x >= 1 else 0
        )
        return liverTOBW_zscore_by_USUBJID_HD[["STUDYID", "USUBJID", "liverToBW_zscore"]]

    else:
        averaged_liverToBW_df = HD_liver_zscore.groupby("STUDYID").agg(
            avg_liverToBW_zscore=("liverToBW_zscore", lambda x: abs(x.replace([np.inf, -np.inf], np.nan).mean(skipna=True)))
        ).reset_index()
        averaged_liverToBW_df["avg_liverToBW_zscore"] = averaged_liverToBW_df["avg_liverToBW_zscore"].apply(
            lambda x: 3 if x >= 3 else 2 if x >= 2 else 1 if x >= 1 else 0
        )
        return averaged_liverToBW_df
