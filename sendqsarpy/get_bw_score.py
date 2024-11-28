# -*- coding: utf-8 -*-
"""
Created on Tue Nov 12 09:03:58 2024

@author: MdAminulIsla.Prodhan
"""

import sqlite3
import pandas as pd
import numpy as np
import pyreadstat
from get_compiledata import get_compile_data

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
        # If BWDY is present and VISITDY is absent, create VISITDY with the values of BWDY
        bw['VISITDY'] = bw['BWDY']
    elif "BWDY" not in bw.columns and "VISITDY" in bw.columns:
        # If VISITDY is present and BWDY is absent, create BWDY with the values of VISITDY
        bw['BWDY'] = bw['VISITDY']
    # If both are present, do nothing
    
    # Ensuring "BWSTRESN", "VISITDY", "BWDY" columns are numeric
    bw['BWSTRESN'] = pd.to_numeric(bw['BWSTRESN'], errors='coerce')
    bw['VISITDY'] = pd.to_numeric(bw['VISITDY'], errors='coerce')
    bw['BWDY'] = pd.to_numeric(bw['BWDY'], errors='coerce')

    #.................. "BodyWeight_zScore" .....calculation........
    #................... Initial BW weight calculation..............
    #...............................................................
    # Initialize data frames
    StudyInitialWeights = pd.DataFrame(columns=["STUDYID",
                                                "USUBJID", 
                                                "BWSTRESN", 
                                                "VISITDY"])
    
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
                # If there are rows where 1 < VISITDY <= 5, choose the one with the minimum VISITDY value
                closest_row_five = five_visitdy.loc[five_visitdy['VISITDY'].idxmin()]
                SubjectInitialWeight = closest_row_five[["STUDYID", "USUBJID", "BWSTRESN", "VISITDY"]].to_frame().T
                
        # 4. If no rows, if VISITDY  >5 , set BWSTRESN value 0
        if SubjectInitialWeight.empty:
            null_visitdy_large_bw = subj_data[subj_data['VISITDY'] > 5]
            if not null_visitdy_large_bw.empty:
                # Set BWSTRESN to 0 for the rows that meet the condition
                null_visitdy_large_bw['BWSTRESN'] = 0
                
                # Choose the row with the minimum VISITDY value greater than 5
                closest_row_null_visitdy = null_visitdy_large_bw.loc[null_visitdy_large_bw['VISITDY'].idxmin()]
                SubjectInitialWeight = closest_row_null_visitdy[["STUDYID", "USUBJID", "BWSTRESN", "VISITDY"]].to_frame().T
        
        # If SubjectInitialWeight is still empty, add currentUSUBJID to UnmatchedUSUBJIDs
        if SubjectInitialWeight.empty:
            UnmatchedUSUBJIDs = UnmatchedUSUBJIDs.append({"USUBJID": currentUSUBJID}, ignore_index=True)

        StudyInitialWeights = pd.concat([StudyInitialWeights, SubjectInitialWeight], ignore_index=True)

    # Remove the first row if initialized with NaNs
    #StudyInitialWeights.dropna(subset=["STUDYID"], inplace=True)

    # Check for the presence of duplicate "USUBJID" in "StudyInitialWeights"
    duplicates_exist = StudyInitialWeights['USUBJID'].duplicated().any()
    
    # Output result
    if duplicates_exist:
        print("There are duplicate USUBJID values in StudyInitialWeights")
        duplicate_usubjids = StudyInitialWeights['USUBJID'][StudyInitialWeights['USUBJID'].duplicated()]
        print(duplicate_usubjids.tolist())
    else:
        print("No duplicate USUBJID values found in StudyInitialWeights")

    # Duplicate rows handling
    # Removing duplicates based on specific column(s)
    # only the first occurrence of each unique USUBJID will be kept, and subsequent duplicates will be removed
    StudyInitialWeights = StudyInitialWeights.drop_duplicates(subset='USUBJID')

    #.......................................................................
    # ..........Final day "StudyBodyWeights" calculation.....................

    #......(StudyBodyWeights)-(TERMBW)-(BoDY Weigt) calculation...............
    
    
    # Initialize the StudyBodyWeights data frame
    StudyBodyWeights = pd.DataFrame(columns=["STUDYID",
                                             "USUBJID",
                                             "BWTESTCD",
                                             "BWSTRESN",
                                             "VISITDY"])
    
    # Initialize dataframe for unmatched USUBJIDs
    BodyWeights_UnmatchedUSUBJIDs = pd.DataFrame(columns=["USUBJID"])
    

    # Get unique USUBJIDs in the current study    
    unique_bw_subjids = bw['USUBJID'].unique()

    for current_bw_USUBJID in unique_bw_subjids:
        # Initialize an empty dataframe for this subject
        
        # Data (all rows) for the current USUBJID
        subj_bw_data = bw[bw['USUBJID'] == current_bw_USUBJID].copy()
        
        # for any row if  VISITDY column data is empty replace it with the corresponding values from BWDY column
        subj_bw_data['VISITDY'] = subj_bw_data['VISITDY'].fillna(subj_bw_data['BWDY'])

        # 1. Check if BWTESTCD == TERMBW is present        
        SubjectBodyWeight = subj_bw_data[subj_bw_data['BWTESTCD'] == "TERMBW"][["STUDYID", "USUBJID", "BWTESTCD", "BWSTRESN", "VISITDY"]]

        
        # If BWTESTCD == TERMBW not present,
        # 2. If no  BWTESTCD == TERMBW,try  VISITDY > 5"
        # should we do that ..............
        if SubjectBodyWeight.empty:
            positive_bw_VISITDY = subj_bw_data[subj_bw_data['VISITDY'] > 5]
            if not positive_bw_VISITDY.empty:
                # choose the one with the maximum VISITDY value
                max_VISITDY = positive_bw_VISITDY['VISITDY'].idxmax()
                SubjectBodyWeight = positive_bw_VISITDY.loc[max_VISITDY][["STUDYID", "USUBJID", "BWTESTCD", "BWSTRESN", "VISITDY"]].to_frame().T

        
        # If SubjectInitialWeight is still empty, add currentUSUBJID to UnmatchedUSUBJIDs
        if SubjectBodyWeight.empty:
            BodyWeights_UnmatchedUSUBJIDs = BodyWeights_UnmatchedUSUBJIDs.append({"USUBJID": current_bw_USUBJID}, ignore_index=True)

        
        # Store Values to "StudyBodyWeights" data frame
        StudyBodyWeights = pd.concat([StudyBodyWeights, SubjectBodyWeight], ignore_index=True)

    #remove the NA values 
    StudyBodyWeights.dropna(subset=["STUDYID"], inplace=True)

    
    #Check for the presence of duplicate "USUBJID" in "StudyBodyWeights"
    stbw_duplicates_exist = StudyBodyWeights['USUBJID'].duplicated().any()
    if stbw_duplicates_exist:
        print("There are duplicate USUBJID values in StudyBodyWeights")
        stbw_duplicate_usubjids = StudyBodyWeights['USUBJID'][StudyBodyWeights['USUBJID'].duplicated()]
        print(stbw_duplicate_usubjids.tolist())
    else:
        print("No duplicate USUBJID values found in StudyBodyWeights")

    # Duplicate "StudyBodyWeights" rows handling

    # Removing duplicates based on specific column(s)
    # only the first occurrence of each unique USUBJID will be kept, and subsequent duplicates will be removed
    # StudyBodyWeights <- StudyBodyWeights[!duplicated(StudyBodyWeights$USUBJID), ]

    # number of unique USUBJID
    #StudyBodyWeights = StudyBodyWeights.drop_duplicates(subset='USUBJID')
    unique_StudyBodyWeights_USUBJID = StudyBodyWeights['USUBJID'].nunique()
    
    #<><><><><><><><><><><><><><><><>... Remove TK animals and Recovery animals......<><><><><><>.............
    #<><><><><><><><> master_compiledata is free of TK animals and Recovery animals<><><><><><><><><><><><><><> 
    if master_compiledata is None:
        studyid = None if use_xpt_file else studyid

        master_compiledata = get_compile_data( studyid=studyid,
                                              path_db=path_db,
                                              fake_study=fake_study,
                                              use_xpt_file=use_xpt_file
                                              )




    # Subtract TK animals from StudyBodyWeights using `master_compiledata`
    #Substract TK animals from the "StudyInitialWeights" and StudyBodyWeights" data frame
    #tk_less_StudyBodyWeights <- StudyBodyWeights[!(StudyBodyWeights$USUBJID %in% tK_animals_df$USUBJID),]
    tk_less_StudyBodyWeights = StudyBodyWeights[StudyBodyWeights['USUBJID'].isin(master_compiledata['USUBJID'])]

    # Similarly handle StudyInitialWeights (if applicable)
    # Substract TK animals from the "StudyInitialWeights" data frame
    #tk_less_StudyInitialWeights <- StudyInitialWeights[!(StudyInitialWeights$USUBJID %in% tK_animals_df$USUBJID),]
    tk_less_StudyInitialWeights = StudyInitialWeights[StudyInitialWeights['USUBJID'].isin(master_compiledata['USUBJID'])]
    
    # Rename columns in StudyInitialWeights by adding "_Init" suffix
    tk_less_StudyInitialWeights = tk_less_StudyInitialWeights.add_suffix('_Init')
    tk_less_StudyInitialWeights = tk_less_StudyInitialWeights.rename(columns={'USUBJID_Init': 'USUBJID'})

    # Inner join on USUBJID
    #  Inner join"StudyInitialWeights" and StudyBodyWeights"
    # an inner join on the USUBJID column for TK_less (StudyInitialWeights & StudyBodyWeights )
    joined_BW_df = pd.merge(tk_less_StudyBodyWeights, tk_less_StudyInitialWeights, on="USUBJID")

    # Select specific columns from joined_BW_df
    BW_df_selected_column = joined_BW_df[["USUBJID", "STUDYID", "BWSTRESN", "BWSTRESN_Init"]]

    # Merge with ARMCD, SETCD, SEX
    # Add "ARMCD","SETCD","SEX" to "selected_df"
    STUDYID_less_master_compiledata = master_compiledata[["USUBJID", "ARMCD", "SETCD", "SEX"]]
    BW_df_merged_ARMCD = pd.merge(BW_df_selected_column, STUDYID_less_master_compiledata, on="USUBJID")

    
    # # BW z-score calculation --------------------------------------------

    # Create the finalbodyweight column
    bwzscore_BW_df = BW_df_merged_ARMCD.copy()
    bwzscore_BW_df['finalbodyweight'] = abs(bwzscore_BW_df['BWSTRESN'] - bwzscore_BW_df['BWSTRESN_Init'])

    # Group by STUDYID and calculate mean and standard deviation for the 'vehicle' group
    grouped = bwzscore_BW_df.groupby('STUDYID')
    bwzscore_BW_df['mean_vehicle'] = grouped['finalbodyweight'].transform(
    lambda x: np.mean(x[bwzscore_BW_df.loc[x.index, 'ARMCD'] == 'vehicle'])
    )
    bwzscore_BW_df['sd_vehicle'] = grouped['finalbodyweight'].transform(
    lambda x: np.std(x[bwzscore_BW_df.loc[x.index, 'ARMCD'] == 'vehicle'])
    )

    # Calculate BWZSCORE
    bwzscore_BW_df['BWZSCORE'] = (
    (bwzscore_BW_df['finalbodyweight'] - bwzscore_BW_df['mean_vehicle']) /
    bwzscore_BW_df['sd_vehicle']
    )

    # Remove mean_vehicle and sd_vehicle columns
    bwzscore_BW = bwzscore_BW_df.drop(columns=['mean_vehicle', 'sd_vehicle'])

    # Filter for "HD" and select specific columns
    HD_BWzScore = bwzscore_BW[bwzscore_BW['ARMCD'] == 'HD'][['STUDYID', 'USUBJID', 'SEX', 'BWZSCORE']]

    # Convert to a DataFrame (if needed)
    HD_BWzScore = HD_BWzScore.reset_index(drop=True)

    # Display the resulting DataFrame
    print(HD_BWzScore)


    # # BW z-score calculation --------------------------------------------
    # # Create the finalbodyweight column in merged_recovery_tk_cleaned_dose_ranked_df data frame
    # BW_df_merged_ARMCD['finalbodyweight'] = abs(BW_df_merged_ARMCD['BWSTRESN'] - BW_df_merged_ARMCD['BWSTRESN_Init'])
    # mean_vehicle = BW_df_merged_ARMCD[BW_df_merged_ARMCD['ARMCD'] == 'vehicle']['finalbodyweight'].mean()
    # sd_vehicle = BW_df_merged_ARMCD[BW_df_merged_ARMCD['ARMCD'] == 'vehicle']['finalbodyweight'].std()

    # BW_df_merged_ARMCD['BWZSCORE'] = (BW_df_merged_ARMCD['finalbodyweight'] - mean_vehicle) / sd_vehicle

    # # Filter for "HD" and select columns
    # HD_BWzScore = BW_df_merged_ARMCD[BW_df_merged_ARMCD['ARMCD'] == 'HD'][["STUDYID", "USUBJID", "SEX", "BWZSCORE"]]

    # # Output final DataFrame
    # HD_BWzScore = HD_BWzScore.reset_index(drop=True)
    # print(HD_BWzScore)
 
    
    
    
    # # Grouped BW z-score calculation -------------------------------------
    # BW_df_merged_ARMCD['finalbodyweight'] = abs(BW_df_merged_ARMCD['BWSTRESN'] - BW_df_merged_ARMCD['BWSTRESN_Init'])

    # # Grouped by STUDYID to calculate mean and std
    # grouped = BW_df_merged_ARMCD.groupby('STUDYID')
    # BW_df_merged_ARMCD['mean_vehicle'] = grouped.apply(lambda g: g[g['ARMCD'] == 'vehicle']['finalbodyweight'].mean()).values
    # BW_df_merged_ARMCD['sd_vehicle'] = grouped.apply(lambda g: g[g['ARMCD'] == 'vehicle']['finalbodyweight'].std()).values

    # BW_df_merged_ARMCD['BWZSCORE'] = (BW_df_merged_ARMCD['finalbodyweight'] - BW_df_merged_ARMCD['mean_vehicle']) / BW_df_merged_ARMCD['sd_vehicle']

    # # Filter for "HD" and select columns
    # HD_BWzScore = BW_df_merged_ARMCD[BW_df_merged_ARMCD['ARMCD'] == 'HD'][["STUDYID", "USUBJID", "SEX", "BWZSCORE"]]

    # # Output final DataFrame
    # HD_BWzScore = HD_BWzScore.reset_index(drop=True)
    # print(HD_BWzScore)
    
    # Enforce mutual exclusivity
    if return_individual_scores and return_zscore_by_USUBJID:
        raise ValueError("Error: Both 'return_individual_scores' and 'return_zscore_by_USUBJID' cannot be True at the same time.")

    if return_individual_scores:
        # Individual scores are simply retained
        bwzscore_BW = bwzscore_BW.copy()

    elif return_zscore_by_USUBJID:
        # Filter for HD, group by STUDYID, handle infinite values, and map BWZSCORE into categories
        BW_zscore_by_USUBJID_HD = (
        bwzscore_BW[bwzscore_BW['ARMCD'] == 'HD']  # Step 1: Filter for HD
        .groupby('STUDYID')  # Step 2: Group by STUDYID
        .apply(lambda group: group.assign(
            BWZSCORE=group['BWZSCORE']
            .replace([float('inf'), -float('inf')], None)  # Replace infinite values with NA
            .abs()  # Take the absolute value
        ))
        .reset_index(drop=True)
        )
        # Map BWZSCORE into categories
        BW_zscore_by_USUBJID_HD['BWZSCORE'] = BW_zscore_by_USUBJID_HD['BWZSCORE'].apply(
        lambda x: 3 if x >= 3 else (2 if x >= 2 else (1 if x >= 1 else 0))
        )
        BW_zscore_by_USUBJID_HD = BW_zscore_by_USUBJID_HD[['STUDYID', 'USUBJID', 'SEX', 'BWZSCORE']]

    else:
        # Case when return_individual_scores == False and return_zscore_by_USUBJID == False
        averaged_HD_BWzScore = (
        HD_BWzScore[['STUDYID', 'BWZSCORE']]  # Select relevant columns
        .groupby('STUDYID', as_index=False)  # Group by STUDYID
        .agg(BWZSCORE_avg=('BWZSCORE', lambda x: x.abs().mean(skipna=True)))  # Calculate average, ignoring NAs
        )
        # Map BWZSCORE_avg into categories
        averaged_HD_BWzScore['BWZSCORE_avg'] = averaged_HD_BWzScore['BWZSCORE_avg'].apply(
        lambda x: 3 if x >= 3 else (2 if x >= 2 else (1 if x >= 1 else 0))
        )

    # Return based on score_in_list_format
    if return_individual_scores:
        result = bwzscore_BW
    elif return_zscore_by_USUBJID:
        result = BW_zscore_by_USUBJID_HD
    else:
        # Handle case when return_individual_scores == False and return_zscore_by_USUBJID == False
        result = averaged_HD_BWzScore

    # Return the result
    return result



#Example usage

# Call the function
db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/2023-2024_projects/FAKE_DATABASES/fake_merged_liver_not_liver.db"
fake_T_xpt_F_bw_score = get_bw_score(studyid="28738",
                                         path_db=db_path, 
                                         fake_study=True, 
                                         use_xpt_file=False, 
                                         master_compiledata=None, 
                                         return_individual_scores=False, 
                                         return_zscore_by_USUBJID=False)

# Call the function
db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/2023-2024_projects/FAKE_DATABASES/single_fake_xpt_folder/FAKE28738"
fake_T_xpt_T_bw_score = get_bw_score(studyid=None,
                                         path_db=db_path, 
                                         fake_study=True, 
                                         use_xpt_file=True, 
                                         master_compiledata=None, 
                                         return_individual_scores=False, 
                                         return_zscore_by_USUBJID=False)


# Call the function for SEND SQLite database
db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/TestDB.db"
real_sqlite_bw_score = get_bw_score(studyid="5003635",
                                         path_db=db_path, 
                                         fake_study=False, 
                                         use_xpt_file=False, 
                                         master_compiledata=None, 
                                         return_individual_scores=False, 
                                         return_zscore_by_USUBJID=False)

# Call the function for SEND XPT data
db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/2023-2024_projects/FAKE_DATABASES/real_xpt_dir/IND051292_1017-3581"
real_XPT_bw_score = get_bw_score(studyid=None,
                                         path_db=db_path, 
                                         fake_study=False, 
                                         use_xpt_file=True, 
                                         master_compiledata=None, 
                                         return_individual_scores=False, 
                                         return_zscore_by_USUBJID=False)