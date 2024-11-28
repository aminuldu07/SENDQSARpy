
# from get_bw_score.py" file~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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