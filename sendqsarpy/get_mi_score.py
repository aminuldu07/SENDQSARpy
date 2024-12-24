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
#import re
from .get_compile_data import get_compile_data

def get_mi_score(studyid=None, 
                 path_db=None, 
                 fake_study=False,
                 use_xpt_file=False, 
                 master_compiledata=None,
                 return_individual_scores=False, 
                 return_zscore_by_USUBJID=False):

    # Convert studyid to string if provided
    #studyid = str(studyid) if studyid is not None else None
    studyid = str(studyid)
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
        dm, _ = pyreadstat.read_xport(os.path.join(path, 'dm.xpt'), encoding="latin1")
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
        r"\b1\s*OF\s*4\b": "2",
        r"\b2\s*OF\s*4\b": "3",
        r"\b3\s*OF\s*4\b": "4", 
        r"\b4\s*OF\s*4\b": "5",
        "1 OF 5": "1",
        "MINIMAL": "1",
        "2 OF 5": "2", 
        "MILD": "2", 
        "3 OF 5": "3",
        "MODERATE": "3",
        "4 OF 5": "4", 
        "MARKED": "4", 
        "5 OF 5": "5",
        "SEVERE": "5"
    }
    for pattern, replacement in severity_replacements.items():
        MIData['MISEV'] = MIData['MISEV'].str.replace(pattern, replacement, regex=True)

    # Convert MISEV to an ordered categorical type
    MIData['MISEV'] = pd.Categorical(MIData['MISEV'], categories=["0", "1", "2", "3", "4", "5"], ordered=True)
    
    # Replace NA values in MISEV with "0"
    MIData['MISEV'] = MIData['MISEV'].fillna("0")
    
    # Set MISPEC values to "LIVER"
    # Make all the MISPEC value = LIVER
    # replace all the value to Liver which will replace "LIVER/GALLBLADDER" to "LIVER"
    MIData['MISPEC'] = "LIVER"

    # Factor replacements for MISTRESC
    #Combine Levels on Findings
    replacements = {
        "CELL DEBRIS": "CELLULAR DEBRIS",
        "INFILTRATION, MIXED CELL": "Infiltrate",
        "INFILTRATION, MONONUCLEAR CELL": "Infiltrate",
        "INFILTRATION, MONONUCLEAR CELL": "Infiltrate",
        "FIBROSIS": "Fibroplasia/Fibrosis"
    }
    MIData['MISTRESC'].replace(replacements, inplace=True)
    
    # Check any empty MISRESC column
    empty_strings_count = (MIData['MISTRESC'] == "").sum()
    print(f"Number of empty strings in MISTRESC: {empty_strings_count}")
    
    # Remove empty MISTRESC values
    MIData = MIData[MIData['MISTRESC'] != '']

    # Filter out recovery and tk animals using master_compiledata
    #---master_compiledata is free of TK animals and Recovery animals-----
    if master_compiledata is None:
        master_compiledata = get_compile_data(studyid=studyid, path_db=path_db, 
                                              fake_study=fake_study, use_xpt_file=use_xpt_file)

    # Filtering the tk animals and the recovery animals
    tk_recovery_less_MIData = MIData[MIData['USUBJID'].isin(master_compiledata['USUBJID'])]

    # Merge with master_compiledata to get ARMCD
    # Perform a left join to match USUBJID and get ARMCD
    tk_recovery_less_MIData_with_ARMCD = pd.merge(tk_recovery_less_MIData, 
                              master_compiledata[['STUDYID', 'USUBJID', 'ARMCD', 'SETCD']], 
                              on='USUBJID', how='left')
    
    # "MIData_cleaned"equivalent to "tk_recovery_less_MIData_with_ARMCD"
    
    ########## MI Data ###############
    MIData_cleaned = tk_recovery_less_MIData_with_ARMCD 
    
         
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    #~~~~~~~~~~~~Merge Severity MI Data into Compile Data~~~~~~~~~~~~~~~~~~~
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
    # Create MIIncidencePRIME DataFrame
    MIIncidencePRIME = MIData_cleaned[['USUBJID', 'MISTRESC', 'MISPEC']]
    
    #test_MIIncidencePRIME = MIIncidencePRIME.copy()
    
    #Severity = pd.merge(master_compiledata[['STUDYID', 'USUBJID', 'Species', 'ARMCD']], MIData_cleaned, on=['STUDYID', 'USUBJID','ARMCD'], how='inner')

    MIData_cleaned_SColmn = MIData_cleaned[['USUBJID', 'MISTRESC', 'MISEV']].copy()

    # Reshape the data (pivot the dataframe)

    MIData_cleaned_SColmn = MIData_cleaned.pivot_table(index='USUBJID', columns='MISTRESC', values='MISEV', aggfunc='first')

    # Fill NAs with "0"
    MIData_cleaned_SColmn.fillna('0', inplace=True)

    # Merge MIData_cleaned_SColmn with master_compiledata on 'USUBJID'
    mi_CompileData = pd.merge(master_compiledata, MIData_cleaned_SColmn, on='USUBJID', how='inner')

    # Backup for checking purpose
    final_working_compile_data_bef_normal = mi_CompileData.copy()
    print(final_working_compile_data_bef_normal.shape)
    
    # Remove Normal MI Results
    if 'NORMAL' in mi_CompileData.columns:
     mi_CompileData.drop(columns='NORMAL', inplace=True)

    # Backup after removal
    final_working_compile_data_afer_normal = mi_CompileData.copy()
    print(final_working_compile_data_afer_normal.shape)

    # Convert columns 7 to the last column to numeric
    mi_CompileData.iloc[:, 6:] = mi_CompileData.iloc[:, 6:].apply(pd.to_numeric, errors='coerce')

    # Print MIIncidencePRIME and mi_CompileData
    # print_MIIncidencePRIME = MIIncidencePRIME.copy()
    # print_mi_CompileData = mi_CompileData.copy()
        

    # Check if mi_CompileData has more than 6 columns
    if mi_CompileData.shape[1] > 6:
        # Merge MIIncidencePRIME with unique values from mi_CompileData
        MIIncidencePRIME = MIIncidencePRIME.merge(
        mi_CompileData[['STUDYID', 'USUBJID', 'ARMCD']].drop_duplicates(), 
        on='USUBJID'
        )
        
        # Get column names of MIIncidencePRIME
        column_MIIncidencePRIME = pd.DataFrame(MIIncidencePRIME.columns, columns=['Column Names'])
        print(column_MIIncidencePRIME)
        
        # Subset MIIncidencePRIME
        MIIncidence = MIIncidencePRIME[['STUDYID', 'USUBJID', 'MISTRESC', 'ARMCD']]
        
        test_MIIncidence = MIIncidence.copy()
        print(test_MIIncidence.shape)
        
        # Initialize GroupIncid
        GroupIncid = pd.DataFrame({
            'Treatment': pd.Series(dtype='str'),
            'Sex': pd.Series(dtype='str'),
            'Finding': pd.Series(dtype='str'),
            'Count': pd.Series(dtype='float')
            })
        
        # Iterate over SEX categories
        for sex in ['M', 'F']:
            StudyMI = MIIncidence
            #StudyMI = MIIncidence[MIIncidence['STUDYID'] == MIIncidence['STUDYID'].unique()[0]].copy()
            
            # Filter data for the current sex
            sexSubjects = mi_CompileData.loc[mi_CompileData['SEX'] == sex, 'USUBJID']
            
            StudyMI = StudyMI[StudyMI['USUBJID'].isin(sexSubjects)]
            
            # Initialize StudyGroupIncid
            StudyGroupIncid = pd.DataFrame({
            'Treatment': pd.Series(dtype='str'),
            'Sex': pd.Series(dtype='str'),
            'Finding': pd.Series(dtype='str'),
            'Count': pd.Series(dtype='float')
            })
            
            # Iterate over ARMCD groups
            for dose in StudyMI['ARMCD'].unique():
                doseMI = StudyMI[StudyMI['ARMCD'] == dose]

                # Calculate incidence for each finding
                Incid = doseMI['MISTRESC'].str.upper().value_counts().reset_index()
                Incid.columns = ['Finding', 'Count']
                Incid['Count'] /= doseMI['USUBJID'].nunique()  # Normalize by unique USUBJID count
                Incid['Treatment'] = f"{StudyMI['STUDYID'].unique()[0]} {dose}"
                Incid['Sex'] = sex
                StudyGroupIncid = pd.concat([StudyGroupIncid, Incid], ignore_index=True)

            # Adjust Count for vehicle baseline
            for finding in StudyGroupIncid['Finding'].unique():
                findingIndex = StudyGroupIncid[StudyGroupIncid['Finding'] == finding].index
                vehicleIndex = findingIndex[StudyGroupIncid.loc[findingIndex, 'Treatment'].str.contains('Vehicle', na=False)]
                if not vehicleIndex.empty:
                    baseline = StudyGroupIncid.loc[vehicleIndex, 'Count'].iloc[0]
                    StudyGroupIncid.loc[findingIndex, 'Count'] -= baseline

            # Remove negative values                        
            negative_index = StudyGroupIncid[StudyGroupIncid['Count'] < 0].index  # Find indices where Count is less than 0
            if len(negative_index) > 0:
                StudyGroupIncid.loc[negative_index, 'Count'] = 0  # Set those rows in 'Count' column to 0
            
            # Combine results
            #StudyGroupIncid.loc[StudyGroupIncid['Count'] < 0, 'Count'] = 0
            GroupIncid = pd.concat([GroupIncid, StudyGroupIncid], ignore_index=True)

                        # Remove rows with NA in Treatment
                        #GroupIncid = GroupIncid.dropna(subset=['Treatment'])
        
            # Remove rows with NA in Treatment
            remove_index = GroupIncid[GroupIncid['Treatment'].isna()].index  # Find indices where Treatment is NA
            if len(remove_index) > 0:
                GroupIncid = GroupIncid.drop(remove_index)  # Remove rows where Treatment is NA
        
        #MIIncidence = GroupIncid
        MIIncidence = GroupIncid.copy()
        # Create mi_CompileData2 and initialize ScoredData
        mi_CompileData2 = mi_CompileData.copy()
        ScoredData = mi_CompileData2.iloc[:, :6].copy()
        IncidenceOverideCount = 0
        
        # Extract column names starting from the 7th column
        colNames = list(mi_CompileData2.columns[6:])
        print(colNames)
        # Override colNames with "INFILTRATION"
        #colNames = ["INFILTRATION"]


        ## Iterate over each column for scoring and adjustments
        #for colName in mi_CompileData2.columns[6:]:
        for colName in colNames:
            # Initialize the column in ScoredData
            ScoredData[colName] = pd.NA

            print(f"Processing column: {colName}")  # Debug: Check current column
            ScoredData[colName] = np.where(mi_CompileData2[colName] == 5, 5,
                                       np.where(mi_CompileData2[colName] > 3, 3,
                                                np.where(mi_CompileData2[colName] == 3, 2,
                                                         np.where(mi_CompileData2[colName] > 0, 1, 0))))
            print(f"Severity scoring complete for {colName}.")  # Debug
            # Ensure scoring updates `mi_CompileData2` correctly
            mi_CompileData2[colName] = ScoredData[colName]
            
            # Debug: Compare the scored values in ScoredData and mi_CompileData2
            print(f"ScoredData[{colName}]: {ScoredData[colName].unique()}")
            print(f"mi_CompileData2[{colName}]: {mi_CompileData2[colName].unique()}")

            # Check incidence percentage by SEX
            for sex in ['M', 'F']:
                # Subset study data by STUDYID and SEX
                studyDataIndex = mi_CompileData2[
                (mi_CompileData2['STUDYID'] == ScoredData['STUDYID'].unique()[0]) & 
                (mi_CompileData2['SEX'] == sex)
                ].index
                StudyData = mi_CompileData2.loc[studyDataIndex]
                print(f"StudyData subset for SEX={sex}: {StudyData.shape[0]} rows.")  # Debug

                # Subset MIIncidence data by STUDYID and SEX
                MIIncidStudy = MIIncidence[
                (MIIncidence['Treatment'].str.contains(ScoredData['STUDYID'].unique()[0])) & 
                (MIIncidence['Sex'] == sex)
                ]
                print(f"MIIncidStudy subset for SEX={sex}: {MIIncidStudy.shape[0]} rows.")  # Debug
                
                for Dose2 in StudyData['ARMCD'].unique():
                    DoseSev = StudyData[StudyData['ARMCD'] == Dose2]
                    DoseIncid = MIIncidStudy[MIIncidStudy['Treatment'].str.endswith(Dose2)]
                    print(f"Processing Dose2={Dose2}. DoseSev rows: {DoseSev.shape[0]}, DoseIncid rows: {DoseIncid.shape[0]}")  # Debug
                    
                    if colName in DoseIncid['Finding'].values:
                        findingIndex = DoseIncid[DoseIncid['Finding'] == colName].index
                        if findingIndex.empty:
                            print(f"No matching findings for {colName} in DoseIncid.")  # Debug
                            continue
                
                        Incid = DoseIncid.loc[findingIndex, 'Count'].values[0]

                        Incid = np.where(Incid >= 0.75, 5,
                                     np.where(Incid >= 0.5, 3,
                                              np.where(Incid >= 0.25, 2,
                                                       np.where(Incid >= 0.1, 1, 0))))
                        # Debug: Show calculated incidence value
                        print(f"Calculated Incid for {colName} in Dose2={Dose2}: {Incid}")

                        swapIndex = DoseSev.index[(DoseSev[colName] < Incid) & (DoseSev[colName] > 0)]
                        if not swapIndex.empty:
                            print(f"Rows to update for {colName} in Dose2={Dose2}: {len(swapIndex)}")  # Debug
                            DoseSev.loc[swapIndex, colName] = Incid
                            ScoredData.loc[swapIndex, colName] = DoseSev.loc[swapIndex, colName]
                            IncidenceOverideCount += 1
                            print(f"IncidenceOverideCount updated: {IncidenceOverideCount}")  # Debug
                            
                        else:
                            print(f"No rows to update for {colName} in Dose2={Dose2}.")  # Debug
                                    

        # Subset and manipulate ScoredData
        ScoredData_subset_HD = ScoredData[ScoredData['ARMCD'] == "HD"].copy()

        if ScoredData_subset_HD.shape[1] == 7:
            ScoredData_subset_HD['highest_score'] = ScoredData_subset_HD.iloc[:, 6]
        else:
            ScoredData_subset_HD['highest_score'] = ScoredData_subset_HD.iloc[:, 6:].max(axis=1)

 
        # Subset the ScoredData where ARMCD == "HD"
        ScoredData_subset_HD = ScoredData[ScoredData['ARMCD'] == "HD"].copy()

        # Convert columns from the 7th to the last to numeric
        for col in ScoredData_subset_HD.columns[6:]:
            ScoredData_subset_HD[col] = pd.to_numeric(ScoredData_subset_HD[col], errors='coerce')

        # Check the number of columns
        num_cols_ScoredData_subset_HD = ScoredData_subset_HD.shape[1]

        # If number of columns is 7, assign highest_score as the value of the 7th column
        if num_cols_ScoredData_subset_HD == 7:
            ScoredData_subset_HD['highest_score'] = ScoredData_subset_HD.iloc[:, 6]
        else:
        # If number of columns is more than 7, get the max value from column 7 to the end
            ScoredData_subset_HD['highest_score'] = ScoredData_subset_HD.iloc[:, 6:].max(axis=1)

        # Move the highest_score column to be the third column
        cols = list(ScoredData_subset_HD.columns)
        cols = cols[:2] + ['highest_score'] + cols[2:-1]
        ScoredData_subset_HD = ScoredData_subset_HD[cols]

        # Enforce mutual exclusivity: If both are TRUE, raise an error
        if return_individual_scores and return_zscore_by_USUBJID:
            raise ValueError("Error: Both 'return_individual_scores' and 'return_zscore_by_USUBJID' cannot be TRUE at the same time.")

        if return_individual_scores:
            # Get all the severity as individual scores in a list
            mi_scoredata_hd = ScoredData_subset_HD

            # Average calculation for each of the columns from the 8th onward
            col_8th_to_end = mi_scoredata_hd.iloc[:, 7:]
            mean_col_8th_to_end = col_8th_to_end.mean().to_dict()

            # Define an empty list
            empty_mi_score_list = {}

            # Add a 'STUDYID' element to the empty list with value
            empty_mi_score_list['STUDYID'] = ScoredData_subset_HD['STUDYID'].unique()[0] # Use the first unique value
            # Append elements from mean_col_8th_to_end
            mi_score_final_list = {**empty_mi_score_list, **mean_col_8th_to_end}

            print(mi_score_final_list)
        
            # Convert the list to a DataFrame
            mi_score_final_list_df = pd.DataFrame([mi_score_final_list])

        elif return_zscore_by_USUBJID:
            MI_score_by_USUBJID_HD = ScoredData_subset_HD

        else:
            # Averaged z-score per STUDYID for 'MI'
            # Step 1: Filter for HD
            MI_final_score = ScoredData_subset_HD[ScoredData_subset_HD['ARMCD'] == "HD"].copy()

            # Step 2: Convert highest_score to numeric
            MI_final_score['highest_score'] = pd.to_numeric(MI_final_score['highest_score'], errors='coerce')

            # Step 3: Group by STUDYID
            MI_final_score = MI_final_score.groupby('STUDYID', as_index=False).agg(
                avg_MI_score=('highest_score', 'mean')
                )

            # Step 4: Rename avg_MI_score to MI_score_avg
            averaged_MI_score = MI_final_score.rename(columns={'avg_MI_score': 'MI_score_avg'})

    else:
        # Return based on return_individual_scores
        if return_individual_scores:
            # Create an empty DataFrame using pandas
            mi_score_final_list_df = pd.DataFrame()  
        elif return_zscore_by_USUBJID:
            # Create an empty DataFrame
            MI_score_by_USUBJID_HD = pd.DataFrame()  
        else:
            # Create an empty DataFrame
            averaged_MI_score = pd.DataFrame()  
    # Return based on return_individual_scores
    if return_individual_scores:
        # Return the DataFrame for individual scores
        return mi_score_final_list_df  

    elif return_zscore_by_USUBJID:
        # Return the DataFrame for z-scores by USUBJID
        return MI_score_by_USUBJID_HD  
    else:
        # Return the DataFrame for averaged scores
        return averaged_MI_score          
            
    