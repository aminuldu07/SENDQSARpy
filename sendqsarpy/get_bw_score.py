# -*- coding: utf-8 -*-
"""
Created on Tue Nov 12 09:03:58 2024

@author: MdAminulIsla.Prodhan
"""

import sqlite3
import pandas as pd
#import numpy as np
import pyreadstat

def get_bw_score(studyid=None,
                 path_db=None, 
                 fake_study=False, 
                 use_xpt_file=False, 
                 master_compiledata=None, 
                 return_individual_scores=False, 
                 return_zscore_by_USUBJID=False):
    
    studyid = str(studyid)
    path = path_db

    # Function to query the database by domain
    def fetch_domain_data(db_connection, domain_name, studyid):
        domain_name = domain_name.upper()
        query_statement = f'SELECT * FROM {domain_name} WHERE STUDYID = ?'
        query_result = pd.read_sql_query(query_statement, db_connection, params=(studyid,))
        return query_result

    if use_xpt_file:
        # Read data from .xpt files
        bw, meta = pyreadstat.read_xport(f"{path}/bw.xpt")
    else:
        # Connect to the SQLite database
        db_connection = sqlite3.connect(path)
        # Fetch data for required domains
        bw = fetch_domain_data(db_connection, 'bw', studyid)
        # Close the database connection
        db_connection.close()

    # Check for the existence of BWDY and VISITDY columns
    if "BWDY" not in bw.columns and "VISITDY" not in bw.columns:
        raise ValueError("Both 'BWDY' and 'VISITDY' columns are absent in the 'bw' data frame.")
    elif "BWDY" in bw.columns and "VISITDY" not in bw.columns:
        bw['VISITDY'] = bw['BWDY']
    elif "BWDY" not in bw.columns and "VISITDY" in bw.columns:
        bw['BWDY'] = bw['VISITDY']

    # Ensure numeric conversion
    bw['BWSTRESN'] = pd.to_numeric(bw['BWSTRESN'], errors='coerce')
    bw['VISITDY'] = pd.to_numeric(bw['VISITDY'], errors='coerce')
    bw['BWDY'] = pd.to_numeric(bw['BWDY'], errors='coerce')

    #.................. "BodyWeight_zScore" .....calculation........
    #................... Initial BW weight calculation..............
    # Initialize data frames
    StudyInitialWeights = pd.DataFrame(columns=["STUDYID", "USUBJID", "BWSTRESN", "VISITDY"])
    
    # Initialize dataframe for unmatched USUBJIDs
    UnmatchedUSUBJIDs = pd.DataFrame(columns=["USUBJID"])

    # Get unique USUBJIDs in the current study
    unique_subjids = bw['USUBJID'].unique()

    for currentUSUBJID in unique_subjids:
        # Initialize an empty dataframe for this subject
        
        # Data (all rows) for the current USUBJID
        subj_data = bw[bw['USUBJID'] == currentUSUBJID].copy()
        
        # for any row if  VISITDY column data is empty replace it with the corresponding values from BWDY column
        subj_data['VISITDY'] = subj_data['VISITDY'].fillna(subj_data['BWDY'])

        # 1. Check if VISITDY == 1 is present
        SubjectInitialWeight = subj_data[subj_data['VISITDY'] == 1][["STUDYID", "USUBJID", "BWSTRESN", "VISITDY"]]
        
        # 2. If no initial weight with VISITDY == 1,  try VISITDY < 0
        if SubjectInitialWeight.empty:
            negative_visits = subj_data[subj_data['VISITDY'] < 0]
            if not negative_visits.empty:
                closest_row = negative_visits.loc[(negative_visits['VISITDY'].abs()).idxmin()]
                SubjectInitialWeight = closest_row[["STUDYID", "USUBJID", "BWSTRESN", "VISITDY"]].to_frame().T
                
        # 3. If no initial weight with VISITDY == 1 VISITDY < 0 , try 1<VISITDY<=5
        if SubjectInitialWeight.empty:
            five_visitdy = subj_data[(subj_data['VISITDY'] > 1) & (subj_data['VISITDY'] <= 5)]
            if not five_visitdy.empty:
                closest_row_five = five_visitdy.loc[five_visitdy['VISITDY'].idxmin()]
                SubjectInitialWeight = closest_row_five[["STUDYID", "USUBJID", "BWSTRESN", "VISITDY"]].to_frame().T
                
        # 4. If no rows, if VISITDY  >5 , set BWSTRESN value 0
        if SubjectInitialWeight.empty:
            null_visitdy_large_bw = subj_data[subj_data['VISITDY'] > 5]
            if not null_visitdy_large_bw.empty:
                null_visitdy_large_bw['BWSTRESN'] = 0
                closest_row_null_visitdy = null_visitdy_large_bw.loc[null_visitdy_large_bw['VISITDY'].idxmin()]
                SubjectInitialWeight = closest_row_null_visitdy[["STUDYID", "USUBJID", "BWSTRESN", "VISITDY"]].to_frame().T
        
        # If SubjectInitialWeight is still empty, add currentUSUBJID to UnmatchedUSUBJIDs
        if SubjectInitialWeight.empty:
            UnmatchedUSUBJIDs = UnmatchedUSUBJIDs.append({"USUBJID": currentUSUBJID}, ignore_index=True)

        StudyInitialWeights = pd.concat([StudyInitialWeights, SubjectInitialWeight], ignore_index=True)

    # Remove the first row if initialized with NaNs
    StudyInitialWeights.dropna(subset=["STUDYID"], inplace=True)

    # Check for duplicates
    duplicates_exist = StudyInitialWeights['USUBJID'].duplicated().any()
    if duplicates_exist:
        print("There are duplicate USUBJID values in StudyInitialWeights")
        duplicate_usubjids = StudyInitialWeights['USUBJID'][StudyInitialWeights['USUBJID'].duplicated()]
        print(duplicate_usubjids.tolist())
    else:
        print("No duplicate USUBJID values found in StudyInitialWeights")

    StudyInitialWeights = StudyInitialWeights.drop_duplicates(subset='USUBJID')

    # Initialize the StudyBodyWeights data frame
    StudyBodyWeights = pd.DataFrame(columns=["STUDYID", "USUBJID", "BWTESTCD", "BWSTRESN", "VISITDY"])
    BodyWeights_UnmatchedUSUBJIDs = pd.DataFrame(columns=["USUBJID"])

    unique_bw_subjids = bw['USUBJID'].unique()

    for current_bw_USUBJID in unique_bw_subjids:
        subj_bw_data = bw[bw['USUBJID'] == current_bw_USUBJID].copy()
        subj_bw_data['VISITDY'] = subj_bw_data['VISITDY'].fillna(subj_bw_data['BWDY'])

        SubjectBodyWeight = subj_bw_data[subj_bw_data['BWTESTCD'] == "TERMBW"][["STUDYID", "USUBJID", "BWTESTCD", "BWSTRESN", "VISITDY"]]

        if SubjectBodyWeight.empty:
            positive_bw_VISITDY = subj_bw_data[subj_bw_data['VISITDY'] > 5]
            if not positive_bw_VISITDY.empty:
                max_VISITDY = positive_bw_VISITDY['VISITDY'].idxmax()
                SubjectBodyWeight = positive_bw_VISITDY.loc[max_VISITDY][["STUDYID", "USUBJID", "BWTESTCD", "BWSTRESN", "VISITDY"]].to_frame().T

        if SubjectBodyWeight.empty:
            BodyWeights_UnmatchedUSUBJIDs = BodyWeights_UnmatchedUSUBJIDs.append({"USUBJID": current_bw_USUBJID}, ignore_index=True)

        StudyBodyWeights = pd.concat([StudyBodyWeights, SubjectBodyWeight], ignore_index=True)

    StudyBodyWeights.dropna(subset=["STUDYID"], inplace=True)

    stbw_duplicates_exist = StudyBodyWeights['USUBJID'].duplicated().any()
    if stbw_duplicates_exist:
        print("There are duplicate USUBJID values in StudyBodyWeights")
        stbw_duplicate_usubjids = StudyBodyWeights['USUBJID'][StudyBodyWeights['USUBJID'].duplicated()]
        print(stbw_duplicate_usubjids.tolist())
    else:
        print("No duplicate USUBJID values found in StudyBodyWeights")

    StudyBodyWeights = StudyBodyWeights.drop_duplicates(subset='USUBJID')

    unique_StudyBodyWeights_USUBJID = StudyBodyWeights['USUBJID'].nunique()

    # Return results as needed
    return StudyInitialWeights, StudyBodyWeights, unique_StudyBodyWeights_USUBJID




#Example usage
# Later in the script, where you want to call the function:
db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/2023-2024_projects/FAKE_DATABASES/fake_merged_liver_not_liver.db"

# Call the function
fake_T_xpt_F_bw_score = get_bw_score(studyid="28738",
                                         path_db=db_path, 
                                         fake_study=True, 
                                         use_xpt_file=False, 
                                         master_compiledata=None, 
                                         return_individual_scores=False, 
                                         return_zscore_by_USUBJID=False)
   
      
    #studyid="28738", path_db = db_path, fake_study=True, use_xpt_file=False)
#(db_path, selected_studies)

# Later in the script, where you want to call the function:
db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/2023-2024_projects/FAKE_DATABASES/single_fake_xpt_folder/FAKE28738"

# Call the function
fake_T_xpt_T_bw_score = get_bw_score(studyid=None,
                                         path_db=db_path, 
                                         fake_study=True, 
                                         use_xpt_file=True, 
                                         master_compiledata=None, 
                                         return_individual_scores=False, 
                                         return_zscore_by_USUBJID=False)


#(studyid=None, path_db = db_path, fake_study =True, use_xpt_file=True)

# Later in the script, where you want to call the function:
db_path = "C:\\Users\\MdAminulIsla.Prodhan\\OneDrive - FDA\\Documents\\TestDB.db"
#selected_studies = "28738"  # Example list of selected studies

# Call the function
real_sqlite_bw_score = get_bw_score(studyid="876", path_db = db_path, fake_study=False, use_xpt_file=False)


# Later in the script, where you want to call the function:
db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/2023-2024_projects/FAKE_DATABASES/real_xpt_dir/IND051292_1017-3581"

# Call the function
real_xpt_bw_score = get_bw_score(studyid=None, path_db = db_path, fake_study =False, use_xpt_file=True)
