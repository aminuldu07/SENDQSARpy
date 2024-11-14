# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 09:03:37 2024

@author: MdAminulIsla.Prodhan
"""

import os
import pandas as pd
import pyreadstat
import sqlite3
import numpy as np
import re

def get_mi_score(studyid=None, path_db=None, fake_study=False, use_xpt_file=False, 
                 master_compiledata=None, return_individual_scores=False, 
                 return_zscore_by_USUBJID=False):

    # Convert studyid to string if provided
    studyid = str(studyid) if studyid is not None else None
    path = path_db

    # Helper function to fetch data from SQLite database
    def fetch_domain_data(db_connection, domain_name, studyid):
        domain_name = domain_name.upper()
        query_statement = f"SELECT * FROM {domain_name} WHERE STUDYID = ?"
        query_result = pd.read_sql_query(query_statement, db_connection, params=(studyid,))
        return query_result

    # Fetch domain data
    if use_xpt_file:
        # Read data from .xpt files
        mi, _ = pyreadstat.read_xport(os.path.join(path, 'mi.xpt'))
        dm, _ = pyreadstat.read_xport(os.path.join(path, 'dm.xpt'))
    else:
        # Connect to the SQLite database
        db_connection = sqlite3.connect(path)
        mi = fetch_domain_data(db_connection, 'mi', studyid)
        dm = fetch_domain_data(db_connection, 'dm', studyid)
        db_connection.close()

    print(f"The dimension of 'dm' domain is: {dm.shape}")
    print(f"The dimension of 'mi' domain is: {mi.shape}")

    # Check if 'mi' DataFrame is empty
    if mi.empty:
        unique_study_ids = dm['STUDYID'].unique()
        warning_messages = ["The 'mi' data frame is empty"] * len(unique_study_ids)
        mi_empty_df = pd.DataFrame({'STUDYID': unique_study_ids, 'Warning': warning_messages})
        return mi_empty_df

    # Initialize the MI_final_score DataFrame
    MI_final_score = pd.DataFrame({'STUDYID': mi['STUDYID'].unique(), 'avg_MI_score': np.nan})

    # Create DataFrame to hold MI Information for the study
    MIData = pd.DataFrame(columns=["USUBJID", "MISTRESC", "MISEV", "MISPEC"])

    # Filter for liver-specific MI data
    MBD = mi[mi['MISPEC'].str.contains("LIVER", case=False, na=False)][['USUBJID', 'MISTRESC', 'MISEV', 'MISPEC']]
    MIData = pd.concat([MIData, MBD], ignore_index=True)

    # Replace empty strings with NaN and fill NAs in MISEV with '0'
    MIData['MISEV'].replace('', np.nan, inplace=True)
    MIData['MISEV'].fillna('0', inplace=True)
    MIData.dropna(inplace=True)

    # Convert MISTRESC to uppercase
    MIData['MISTRESC'] = MIData['MISTRESC'].str.upper()

    # Severity conversion using regex replacements
    severity_replacements = {
        r"\b1\s*OF\s*4\b": "2", r"\b2\s*OF\s*4\b": "3", r"\b3\s*OF\s*4\b": "4", 
        r"\b4\s*OF\s*4\b": "5", "1 OF 5": "1", "MINIMAL": "1", "2 OF 5": "2", 
        "MILD": "2", "3 OF 5": "3", "MODERATE": "3", "4 OF 5": "4", 
        "MARKED": "4", "5 OF 5": "5", "SEVERE": "5"
    }
    for pattern, replacement in severity_replacements.items():
        MIData['MISEV'] = MIData['MISEV'].str.replace(pattern, replacement, regex=True)

    # Convert MISEV to an ordered categorical type
    MIData['MISEV'] = pd.Categorical(MIData['MISEV'], categories=["0", "1", "2", "3", "4", "5"], ordered=True)

    # Set MISPEC values to "LIVER"
    MIData['MISPEC'] = "LIVER"

    # Factor replacements for MISTRESC
    replacements = {
        "CELL DEBRIS": "CELLULAR DEBRIS", "INFILTRATION, MIXED CELL": "Infiltrate",
        "INFILTRATION, MONONUCLEAR CELL": "Infiltrate", "INFILTRATION, MONONUCLEAR CELL": "Infiltrate",
        "FIBROSIS": "Fibroplasia/Fibrosis"
    }
    MIData['MISTRESC'].replace(replacements, inplace=True)

    # Remove empty MISTRESC values
    MIData = MIData[MIData['MISTRESC'] != '']

    # Filter out recovery and tk animals using master_compiledata
    if master_compiledata is None:
        master_compiledata = get_compile_data(studyid=studyid, path_db=path_db, 
                                              fake_study=fake_study, use_xpt_file=use_xpt_file)

    tk_recovery_less_MIData = MIData[MIData['USUBJID'].isin(master_compiledata['USUBJID'])]

    # Merge with master_compiledata to get ARMCD
    MIData_cleaned = pd.merge(tk_recovery_less_MIData, 
                              master_compiledata[['STUDYID', 'USUBJID', 'ARMCD', 'SETCD']], 
                              on='USUBJID', how='left')

    # Create MIIncidencePRIME DataFrame
    MIIncidencePRIME = MIData_cleaned[['USUBJID', 'MISTRESC', 'MISPEC']]

    # Pivot the data to create mi_CompileData
    MIData_cleaned_SColmn = MIData_cleaned.pivot_table(index='USUBJID', columns='MISTRESC', values='MISEV', fill_value="0")
    mi_CompileData = pd.merge(master_compiledata, MIData_cleaned_SColmn, on='USUBJID', how='left')

    # Remove the 'NORMAL' column if it exists
    if 'NORMAL' in mi_CompileData.columns:
        mi_CompileData.drop(columns='NORMAL', inplace=True)

    # Convert columns 7 to the last to numeric
    mi_CompileData.iloc[:, 6:] = mi_CompileData.iloc[:, 6:].apply(pd.to_numeric, errors='coerce')

    return mi_CompileData
