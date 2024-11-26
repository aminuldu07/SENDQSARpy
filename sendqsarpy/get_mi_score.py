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
from get_compiledata import get_compile_data

def get_mi_score(studyid=None, 
                 path_db=None, 
                 fake_study=False,
                 use_xpt_file=False, 
                 master_compiledata=None,
                 return_individual_scores=False, 
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
    
    Severity = pd.merge(master_compiledata[['STUDYID', 'USUBJID', 'Species', 'ARMCD']], MIData_cleaned, on=['STUDYID', 'USUBJID','ARMCD'], how='inner')

    MIData_cleaned_SColmn = MIData_cleaned[['USUBJID', 'MISTRESC', 'MISEV']].copy()

    # Reshape the data (pivot the dataframe)
    MIData_cleaned_SColmn = MIData_cleaned.pivot_table(index='USUBJID', columns='MISTRESC', values='MISEV', aggfunc='first')

    # Fill NAs with "0"
    MIData_cleaned_SColmn.fillna('0', inplace=True)

    # Merge MIData_cleaned_SColmn with master_compiledata on 'USUBJID'
    mi_CompileData = pd.merge(master_compiledata, MIData_cleaned_SColmn, on='USUBJID', how='inner')

    # Backup for checking purpose
    final_working_compile_data_bef_normal = mi_CompileData.copy()

    # Remove Normal MI Results
    if 'NORMAL' in mi_CompileData.columns:
     mi_CompileData.drop(columns='NORMAL', inplace=True)

    # Backup after removal
    final_working_compile_data_afer_normal = mi_CompileData.copy()

    # Convert columns 7 to the last column to numeric
    mi_CompileData.iloc[:, 6:] = mi_CompileData.iloc[:, 6:].apply(pd.to_numeric, errors='coerce')

    # Print MIIncidencePRIME and mi_CompileData
    print_MIIncidencePRIME = MIIncidencePRIME.copy()
    print_mi_CompileData = mi_CompileData.copy()
    
    

    # Check if mi_CompileData has more than 6 columns
    if mi_CompileData.shape[1] > 6:
        # Merge MIIncidencePRIME with unique values from mi_CompileData
        MIIncidencePRIME = MIIncidencePRIME.merge(
        mi_CompileData[['STUDYID', 'USUBJID', 'ARMCD']].drop_duplicates(), 
        on='USUBJID'
        )
        
        # Get column names of MIIncidencePRIME
        column_MIIncidencePRIME = pd.DataFrame(MIIncidencePRIME.columns, columns=['Column Names'])
        
        # Subset MIIncidencePRIME
        MIIncidence = MIIncidencePRIME[['STUDYID', 'USUBJID', 'MISTRESC', 'ARMCD']]
        test_MIIncidence = MIIncidence.copy()
        
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
        
        MIIncidence = GroupIncid

        # Create mi_CompileData2 and initialize ScoredData
        mi_CompileData2 = mi_CompileData.copy()
        ScoredData = mi_CompileData2.iloc[:, :6].copy()
        IncidenceOverideCount = 0

        ## Iterate over each column for scoring and adjustments
        for colName in mi_CompileData2.columns[6:]:
            ScoredData[colName] = np.where(mi_CompileData2[colName] == 5, 5,
                                       np.where(mi_CompileData2[colName] > 3, 3,
                                                np.where(mi_CompileData2[colName] == 3, 2,
                                                         np.where(mi_CompileData2[colName] > 0, 1, 0))))
            mi_CompileData2[colName] = ScoredData[colName]

        # Check incidence percentage by SEX
        for sex in ['M', 'F']:
            studyDataIndex = mi_CompileData2[
                (mi_CompileData2['STUDYID'] == ScoredData['STUDYID'].unique()[0]) & 
                (mi_CompileData2['SEX'] == sex)
            ].index
            StudyData = mi_CompileData2.loc[studyDataIndex]

            MIIncidStudy = MIIncidence[
                (MIIncidence['Treatment'].str.contains(ScoredData['STUDYID'].unique()[0])) & 
                (MIIncidence['Sex'] == sex)
            ]

            for Dose2 in StudyData['ARMCD'].unique():
                DoseSev = StudyData[StudyData['ARMCD'] == Dose2]
                DoseIncid = MIIncidStudy[MIIncidStudy['Treatment'].str.endswith(Dose2)]

                if colName in DoseIncid['Finding'].values:
                    findingIndex = DoseIncid[DoseIncid['Finding'] == colName].index
                    Incid = DoseIncid.loc[findingIndex, 'Count'].values[0]

                    Incid = np.where(Incid >= 0.75, 5,
                                     np.where(Incid >= 0.5, 3,
                                              np.where(Incid >= 0.25, 2,
                                                       np.where(Incid >= 0.1, 1, 0))))

                    swapIndex = DoseSev.index[(DoseSev[colName] < Incid) & (DoseSev[colName] > 0)]
                    if not swapIndex.empty:
                        DoseSev.loc[swapIndex, colName] = Incid
                        ScoredData.loc[swapIndex, colName] = DoseSev.loc[swapIndex, colName]
                        IncidenceOverideCount += 1

    # Subset and manipulate ScoredData
    ScoredData_subset_HD = ScoredData[ScoredData['ARMCD'] == "HD"].copy()

    if ScoredData_subset_HD.shape[1] == 7:
        ScoredData_subset_HD['highest_score'] = ScoredData_subset_HD.iloc[:, 6]
    else:
        ScoredData_subset_HD['highest_score'] = ScoredData_subset_HD.iloc[:, 6:].max(axis=1)
# # If mi_CompileData has more than 6 columns
    # if mi_CompileData.shape[1] > 6:
    #     #Calculate Incidence per group for MI Data
    #     # Merge MIIncidencePRIME with mi_CompileData
    #     MIIncidencePRIME = pd.merge(MIIncidencePRIME, mi_CompileData[['STUDYID', 'USUBJID', 'ARMCD']].drop_duplicates(), on='USUBJID', how='left')

    #     # Directly subset the data frame
    #     MIIncidence = MIIncidencePRIME[['STUDYID', 'USUBJID', 'MISTRESC', 'ARMCD']]

    #     test_MIIncidence = MIIncidence.copy()

    #     # Create a data frame 
    #     GroupIncid = pd.DataFrame(columns=['Treatment',
    #                                        'Sex',
    #                                        'Finding',
    #                                        'Count'])

    #     # Iterate over each sex category
    #     for sex in ['M', 'F']:
    #         # Filter data for the current study
    #         # Filter rows where STUDYID matches the unique values of STUDYID
    #         StudyMI = MIIncidence[MIIncidence['STUDYID'].isin(MIIncidence['STUDYID'].unique())]

    #        # StudyMI = MIIncidence[MIIncidence['SEX'] == sex]

    #         # " StudyGroupIncid" data frame createion
    #         StudyGroupIncid = pd.DataFrame(columns=['Treatment',
    #                                                 'Sex',
    #                                                 'Finding',
    #                                                 'Count'])

    #         # Filter data for the current sex
    #         sexSubjects = mi_CompileData[mi_CompileData['SEX'] == sex]['USUBJID']
    #         sexIndex = StudyMI['USUBJID'].isin(sexSubjects)
    #         StudyMI = StudyMI[sexIndex]
    #         #StudyMI = StudyMI[StudyMI['USUBJID'].isin(sexSubjects)] ?????

    #         # Iterate over unique treatment arms (ARMCD)
    #         for dose in StudyMI['ARMCD'].unique():
    #             doseMI = StudyMI[StudyMI['ARMCD'] == dose]

    #             # Calculate the incidence for each finding
    #             #Incid = doseMI['MISTRESC'].str.upper().value_counts(normalize=True).reset_index()
    #             Incid = doseMI['MISTRESC'].value_counts(normalize=True).reset_index()
    #             Incid.columns = ['Finding', 'Count']
    #             #Incid['Treatment'] = f"{StudyMI['STUDYID'].unique()[0]} {dose}"
    #             Incid['Treatment'] = f"{StudyMI['STUDYID'].iloc[0]} {dose}"
    #             Incid['Sex'] = sex

    #             #StudyGroupIncid = pd.concat([StudyGroupIncid, Incid], ignore_index=True)
    #             StudyGroupIncid = pd.concat([StudyGroupIncid, Incid])
                

    #         # Remove vehicle baseline
    #         for finding in StudyGroupIncid['Finding'].unique():
    #             #findingIndex = StudyGroupIncid['Finding'] == finding
    #             findingIndex = StudyGroupIncid[StudyGroupIncid['Finding'] == finding].index
    #             #vehicleIndex = StudyGroupIncid.loc[findingIndex, 'Treatment'].str.contains('Vehicle', na=False)
    #             vehicleIndex = StudyGroupIncid[StudyGroupIncid['Treatment'].str.contains('Vehicle')].index
    #             if not vehicleIndex.empty:
    #             #if vehicleIndex.any():
    #                 #baseline = StudyGroupIncid.loc[findingIndex & vehicleIndex, 'Count'].values[0]
    #                 baseline = StudyGroupIncid.loc[vehicleIndex, 'Count'].values[0]
    #                 #StudyGroupIncid.loc[findingIndex, 'Count'] -= baseline
    #                 StudyGroupIncid.loc[findingIndex, 'Count'] -= baseline
                    
    #                 # when findings in HD group less than the vehicle group
    #                 # Replace negative values with 0
    #                 negativeIndex = StudyGroupIncid['Count'] < 0
    #                 StudyGroupIncid.loc[negativeIndex, 'Count'] = 0
    #                 #StudyGroupIncid.loc[StudyGroupIncid['Count'] < 0, 'Count'] = 0  # Correct negative counts

    #                 # Combine results
    #                 # GroupIncid = pd.concat([GroupIncid, StudyGroupIncid], ignore_index=True)
    #                 GroupIncid = pd.concat([GroupIncid, StudyGroupIncid])
                    
    #                 removeIndex = GroupIncid['Treatment'].isna()
    #                 if removeIndex.any():
    #                     GroupIncid = GroupIncid[~removeIndex].reset_index(drop=True)
    #                     #GroupIncid = GroupIncid.dropna(subset=['Treatment'])

    MIIncidence = GroupIncid.copy()

    # Create a copy of mi_CompileData named mi_CompileData2
    mi_CompileData2 = mi_CompileData.copy()

    # Initialize ScoredData with the first 6 columns of "mi_CompileData2"
    ScoredData = mi_CompileData2.iloc[:, :6].copy()
    
    # Create a copy of mi_CompileData named mi_CompileData2
    mi_CompileData2 = mi_CompileData.copy()

    # Initialize ScoredData with the first 6 columns of "mi_CompileData2"
    ScoredData = mi_CompileData2.iloc[:, :6].copy()

    # Initialize a counter for incidence overrides
    IncidenceOverideCount = 0

    # Iterate over each column for scoring and adjustments
    for colName in mi_CompileData2.columns[6:]:
    # Score severity using nested np.where
        ScoredData[colName] = np.where(mi_CompileData2[colName] == 5, 5,
                                   np.where(mi_CompileData2[colName] > 3, 3,
                                            np.where(mi_CompileData2[colName] == 3, 2,
                                                     np.where(mi_CompileData2[colName] > 0, 1, 0))))
    mi_CompileData2[colName] = ScoredData[colName]

    # Iterate over sex categories
    for sex in ['M', 'F']:
        studyDataIndex = mi_CompileData2[
            (mi_CompileData2['STUDYID'] == ScoredData['STUDYID'].unique()[0]) & 
            (mi_CompileData2['SEX'] == sex)
        ].index
        StudyData = mi_CompileData2.loc[studyDataIndex]

        MIIncidIndex = MIIncidence[
            (MIIncidence['Treatment'].str.contains(ScoredData['STUDYID'].unique()[0])) & 
            (MIIncidence['Sex'] == sex)
        ].index
        MIIncidStudy = MIIncidence.loc[MIIncidIndex]

        # Iterate over unique treatment arms (ARMCD)
        for Dose2 in StudyData['ARMCD'].unique():
            DoseSev = StudyData[StudyData['ARMCD'] == Dose2]
            DoseIncid = MIIncidStudy[MIIncidStudy['Treatment'].str.split().str[-1] == Dose2]

            if colName in DoseIncid['Finding'].values:
                findingIndex = DoseIncid[DoseIncid['Finding'] == colName].index
                Incid = DoseIncid.loc[findingIndex, 'Count'].values[0]

                Incid = np.where(Incid >= 0.75, 5,
                                np.where(Incid >= 0.5, 3,
                                         np.where(Incid >= 0.25, 2,
                                                  np.where(Incid >= 0.1, 1, 0))))

                swapIndex = DoseSev.index[
                    (DoseSev[colName] < Incid) & (DoseSev[colName] > 0)
                ]
                if not swapIndex.empty:
                    DoseSev.loc[swapIndex, colName] = Incid
                    ScoredData.loc[swapIndex, colName] = DoseSev.loc[swapIndex, colName]
                    IncidenceOverideCount += 1

    # Subset the ScoredData for "HD" group
    ScoredData_subset_HD = ScoredData[ScoredData['ARMCD'] == "HD"]



    ###############################################################################################
    # # Iterate over each column for scoring and adjustments
    # for colName in mi_CompileData2.columns[6:]:
    #     ScoredData[colName] = np.where(mi_CompileData2[colName] == 5, 5,
    #                                    np.where(mi_CompileData2[colName] > 3, 3,
    #                                             np.where(mi_CompileData2[colName] == 3, 2,
    #                                                      np.where(mi_CompileData2[colName] > 0, 1, 0))))

    #     mi_CompileData2[colName] = ScoredData[colName]

    #     # Iterate over each sex and treatment arm for incidence adjustments
    #     for sex in ['M', 'F']:
    #         StudyData = mi_CompileData2[(mi_CompileData2['SEX'] == sex) & (mi_CompileData2['STUDYID'] == ScoredData['STUDYID'].iloc[0])]

    #         MIIncidStudy = MIIncidence[(MIIncidence['Sex'] == sex) & (MIIncidence['Treatment'].str.contains(ScoredData['STUDYID'].iloc[0]))]

    #         for dose in StudyData['ARMCD'].unique():
    #             DoseSev = StudyData[StudyData['ARMCD'] == dose]
    #             DoseIncid = MIIncidStudy[MIIncidStudy['Treatment'].str.contains(dose)]

    #             if colName in DoseIncid['Finding'].values:
    #                 findingIndex = DoseIncid[DoseIncid['Finding'] == colName].index[0]
    #                 Incid = DoseIncid.loc[findingIndex, 'Count']
    #                 Incid = 5 if Incid >= 0.75 else (3 if Incid >= 0.5 else (2 if Incid >= 0.25 else (1 if Incid >= 0.1 else 0)))

    #                 swapIndex = DoseSev[DoseSev[colName] < Incid].index
    #                 if not swapIndex.empty:
    #                     DoseSev.loc[swapIndex, colName] = Incid
    #                     ScoredData.loc[swapIndex, colName] = DoseSev.loc[swapIndex, colName]
    #################################################################################################

    # Subset the ScoredData
    #ScoredData_subset_HD = ScoredData[ScoredData['ARMCD'] == "HD"]
    
    # # Create MIIncidencePRIME DataFrame
    # MIIncidencePRIME = MIData_cleaned[['USUBJID', 'MISTRESC', 'MISPEC']]

    # # Pivot the data to create mi_CompileData
    # MIData_cleaned_SColmn = MIData_cleaned.pivot_table(index='USUBJID', columns='MISTRESC', values='MISEV', fill_value="0")
    # mi_CompileData = pd.merge(master_compiledata, MIData_cleaned_SColmn, on='USUBJID', how='left')

    # # Remove the 'NORMAL' column if it exists
    # if 'NORMAL' in mi_CompileData.columns:
    #     mi_CompileData.drop(columns='NORMAL', inplace=True)

    # # Convert columns 7 to the last to numeric
    # mi_CompileData.iloc[:, 6:] = mi_CompileData.iloc[:, 6:].apply(pd.to_numeric, errors='coerce')

    return mi_CompileData




##############################################################################################

#Example usage
# Later in the script, where you want to call the function:
db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/2023-2024_projects/FAKE_DATABASES/fake_merged_liver_not_liver.db"

# Call the function
fake_T_xpt_F_mi_score = get_mi_score(studyid="28738",
                                         path_db=db_path, 
                                         fake_study=True, 
                                         use_xpt_file=False, 
                                         master_compiledata=None, 
                                         return_individual_scores=False, 
                                         return_zscore_by_USUBJID=False)