
# from get_mi_score.py" file~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
    
    ##########################################################################################################
           # MIIncidence = GroupIncid.copy()

           # # Create a copy of mi_CompileData named mi_CompileData2
           # mi_CompileData2 = mi_CompileData.copy()

           # # Initialize ScoredData with the first 6 columns of "mi_CompileData2"
           # ScoredData = mi_CompileData2.iloc[:, :6].copy()
       
           # # # Create a copy of mi_CompileData named mi_CompileData2
           # # mi_CompileData2 = mi_CompileData.copy()

           # # # Initialize ScoredData with the first 6 columns of "mi_CompileData2"
           # # ScoredData = mi_CompileData2.iloc[:, :6].copy()

           # # Initialize a counter for incidence overrides
           # IncidenceOverideCount = 0

           # # Iterate over each column for scoring and adjustments
           # for colName in mi_CompileData2.columns[6:]:
           #     # Score severity using nested np.where
           #     ScoredData[colName] = np.where(mi_CompileData2[colName] == 5, 5,
           #                            np.where(mi_CompileData2[colName] > 3, 3,
           #                                     np.where(mi_CompileData2[colName] == 3, 2,
           #                                              np.where(mi_CompileData2[colName] > 0, 1, 0))))
           #     mi_CompileData2[colName] = ScoredData[colName]

           #     # Iterate over sex categories
           #     for sex in ['M', 'F']:
           #         studyDataIndex = mi_CompileData2[
           #         (mi_CompileData2['STUDYID'] == ScoredData['STUDYID'].unique()[0]) & 
           #         (mi_CompileData2['SEX'] == sex)
           #         ].index
           #         StudyData = mi_CompileData2.loc[studyDataIndex]

           #         MIIncidIndex = MIIncidence[
           #         (MIIncidence['Treatment'].str.contains(ScoredData['STUDYID'].unique()[0])) & 
           #         (MIIncidence['Sex'] == sex)
           #         ].index
           #         MIIncidStudy = MIIncidence.loc[MIIncidIndex]

           #         # Iterate over unique treatment arms (ARMCD)
           #         for Dose2 in StudyData['ARMCD'].unique():
           #             DoseSev = StudyData[StudyData['ARMCD'] == Dose2]
           #             DoseIncid = MIIncidStudy[MIIncidStudy['Treatment'].str.split().str[-1] == Dose2]

           #         if colName in DoseIncid['Finding'].values:
           #             findingIndex = DoseIncid[DoseIncid['Finding'] == colName].index
           #             Incid = DoseIncid.loc[findingIndex, 'Count'].values[0]

           #             Incid = np.where(Incid >= 0.75, 5,
           #                         np.where(Incid >= 0.5, 3,
           #                                  np.where(Incid >= 0.25, 2,
           #                                           np.where(Incid >= 0.1, 1, 0))))

           #             swapIndex = DoseSev.index[
           #             (DoseSev[colName] < Incid) & (DoseSev[colName] > 0)
           #             ]
           #             if not swapIndex.empty:
           #                 DoseSev.loc[swapIndex, colName] = Incid
           #                 ScoredData.loc[swapIndex, colName] = DoseSev.loc[swapIndex, colName]
           #                 IncidenceOverideCount += 1

           # # Subset the ScoredData for "HD" group
           # #ScoredData_subset_HD = ScoredData[ScoredData['ARMCD'] == "HD"]
       
           # # # Subset and manipulate ScoredData
           # # ScoredData_subset_HD = ScoredData[ScoredData['ARMCD'] == "HD"].copy()

           # # if ScoredData_subset_HD.shape[1] == 7:
           #     #     ScoredData_subset_HD['highest_score'] = ScoredData_subset_HD.iloc[:, 6]
           #     # else:
           #     #     ScoredData_subset_HD['highest_score'] = ScoredData_subset_HD.iloc[:, 6:].max(axis=1)

   ######################################################################################################################