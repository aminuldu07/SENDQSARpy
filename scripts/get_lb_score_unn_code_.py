
# from get_lb_score.py" file~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# serum_alt_results = fake_T_xpt_F_lb_score["ALT"]
# serum_ast_results = fake_T_xpt_F_lb_score["AST"]
# serum_alp_results = fake_T_xpt_F_lb_score["ALP"]
# serum_ggt_results = fake_T_xpt_F_lb_score["GGT"]
# serum_bili_results = fake_T_xpt_F_lb_score["BILI"]
# serum_alb_results = fake_T_xpt_F_lb_score["ALB"]

# #studyid="28738", path_db = db_path, fake_study=True, use_xpt_file=False)
# #(db_path, selected_studies)

# # Later in the script, where you want to call the function:
# db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/2023-2024_projects/FAKE_DATABASES/single_fake_xpt_folder/FAKE28738"

# # Call the function
# fake_T_xpt_T_lb_score = get_lb_score(studyid=None,
#                                          path_db=db_path,
#                                          fake_study=True,
#                                          use_xpt_file=True,
#                                          master_compiledata=None,
#                                          return_individual_scores=False,
#                                          return_zscore_by_USUBJID=False)


# #(studyid=None, path_db = db_path, fake_study =True, use_xpt_file=True)

# # Later in the script, where you want to call the function:
# db_path = "C:\\Users\\MdAminulIsla.Prodhan\\OneDrive - FDA\\Documents\\TestDB.db"
# #selected_studies = "28738"  # Example list of selected studies

# # Call the function
# real_sqlite_lb_score = get_lb_score(studyid="876", path_db = db_path, fake_study=False, use_xpt_file=False)


# # Later in the script, where you want to call the function:
# db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/2023-2024_projects/FAKE_DATABASES/real_xpt_dir/IND051292_1017-3581"

# # Call the function
# real_xpt_lb_score = get_lb_score(studyid=None, path_db = db_path, fake_study =False, use_xpt_file=True)

##############################################################################################################
#################################### lb zscore calculation helper funciton ###################################
##############################################################################################################

# # helper funciton for calculation the zscore for each enzymes 
  
    # def calculate_zscores(lb_tk_recovery_filtered_armcd, lbtestcd_groups):
    #     """
    #     Calculate Z-scores and assign toxicity scores for multiple LBTESTCD groups.

    #     Parameters:
    #         lb_tk_recovery_filtered_armcd (pd.DataFrame): The input DataFrame with lab data.
    #         lbtestcd_groups (dict): A dictionary mapping group names to LBTESTCD lists.

    #     Returns:
    #         dict: A dictionary with group names as keys and processed DataFrames as values.
    #     """
    #     result = {}
        
    #     for group_name, lbtestcd_list in lbtestcd_groups.items():
    #         # Filter data for the given LBTESTCD list
    #         df_filtered = lb_tk_recovery_filtered_armcd[
    #             lb_tk_recovery_filtered_armcd["LBTESTCD"].isin(lbtestcd_list)
    #         ]

    #         # Calculate mean and standard deviation for 'vehicle' group
    #         df_filtered[f"mean_vehicle_{group_name.lower()}"] = df_filtered.groupby("STUDYID")["LBSTRESN"].transform(
    #             lambda x: x[df_filtered["ARMCD"] == "vehicle"].mean())
    #         df_filtered[f"sd_vehicle_{group_name.lower()}"] = df_filtered.groupby("STUDYID")["LBSTRESN"].transform(
    #             lambda x: x[df_filtered["ARMCD"] == "vehicle"].std())
            
    #         # Compute Z-score
    #         df_filtered[f"{group_name.lower()}_zscore"] = abs(
    #             (df_filtered["LBSTRESN"] - df_filtered[f"mean_vehicle_{group_name.lower()}"]) /
    #             df_filtered[f"sd_vehicle_{group_name.lower()}"]
    #         )
            
    #         # Calculate final Z-score for 'HD' group
    #         final_zscore = (df_filtered[df_filtered["ARMCD"] == "HD"]
    #                         .groupby("STUDYID")
    #                         .agg(avg_zscore=(f"{group_name.lower()}_zscore", "mean"))
    #                         .reset_index())
            
    #         # Assign toxicity scores
    #         final_zscore["avg_zscore"] = final_zscore["avg_zscore"].apply(
    #             lambda x: 3 if x >= 3 else (2 if x >= 2 else (1 if x >= 1 else 0))
    #         )
            
    #         # Rename the avg_zscore column to include the group name
    #         final_zscore = final_zscore.rename(columns={"avg_zscore": f"avg_{group_name.lower()}_zscore"})

    #        # Select only STUDYID and the new avg_zscore column
    #         final_zscore = final_zscore[["STUDYID", f"avg_{group_name.lower()}_zscore"]]

            
    #         result[group_name] = final_zscore
#     return result

    #    # Example Usage
    # lbtestcd_groups = {
    #  "ALT": ['SERUM | ALT', 'PLASMA | ALT', 'WHOLE BLOOD | ALT'],
    #  "AST": ['SERUM | AST', 'PLASMA | AST', 'WHOLE BLOOD | AST'],
    #  "ALP": ['SERUM | ALP', 'PLASMA | ALP', 'WHOLE BLOOD | ALP'],
    #  "GGT": ['SERUM | GGT', 'PLASMA | GGT', 'WHOLE BLOOD | GGT'],
    #  "BILI": ['SERUM | BILI', 'PLASMA | BILI', 'WHOLE BLOOD | BILI'],
    #  "ALB": ['SERUM | ALB', 'PLASMA | ALB', 'WHOLE BLOOD | ALB']
    #  }

    # # Call the function
    # results = calculate_zscores(lb_tk_recovery_filtered_armcd, lbtestcd_groups)


    # Return the processed data
    #return results
    #return serum_bili_final_zscore