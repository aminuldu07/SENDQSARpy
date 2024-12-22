# -*- coding: utf-8 -*-
"""
Created on Sun Dec 22 07:58:03 2024

@author: MdAminulIsla.Prodhan
"""

import pandas as pd
from .get_compile_data import get_compile_data
from .get_bw_score import get_bw_score
from .get_livertobw_score import get_livertobw_score
from .get_lb_score import get_lb_score
from .get_mi_score import get_mi_score

def get_liver_om_lb_mi_tox_score_list(
    studyid_or_studyids=None,
    path_db=None,
    fake_study=False,
    use_xpt_file=False,
    output_individual_scores=False,
    output_zscore_by_USUBJID=False
):
    # Enforce mutual exclusivity: If both are TRUE, throw an error
    if output_individual_scores and output_zscore_by_USUBJID:
        raise ValueError("Both 'output_individual_scores' and 'output_zscore_by_USUBJID' cannot be True at the same time.")
    
    # if output_individual_scores:
    #     # Initialize master DataFrames and error tracking
    #     master_data = initialize_dataframes(output_individual_scores, output_zscore_by_USUBJID)
    #     error_studies = []
    
    
    if output_individual_scores:
        # Master bwzscore

        # Master liverToBW DataFrame
        master_liverToBW = pd.DataFrame(columns=["STUDYID", "avg_liverToBW_zscore"])

        # Master LB list
        master_lb_score_six = pd.DataFrame(columns=[
            "STUDYID", "avg_alb_zscore", "avg_ast_zscore", "avg_alp_zscore",
            "avg_alt_zscore", "avg_bili_zscore", "avg_ggt_zscore"
            ])

        # Master MI DataFrame
        master_mi_df = pd.DataFrame()

        # Initialize an empty list to store the names of studies with errors
        error_studies = []

        # Initialize the master error DataFrame to store the details of errors
        master_error_df = pd.DataFrame(columns=["STUDYID", "Block", "ErrorMessage"])

    elif output_zscore_by_USUBJID:
        # Master liverToBW list
        master_liverToBW = []

        # Master LB score list
        master_lb_score = []

        # Master MI score list
        master_mi_score = []

        # Initialize an empty list to store the names of studies with errors
        error_studies = []

        # Initialize the master error DataFrame to store the details of errors
        master_error_df = pd.DataFrame(columns=["STUDYID", "Block", "ErrorMessage"])

    else:
        # Create FOUR_Liver_Score_avg DataFrame for "LiverToBodyWeight", "LB", and "MI" scores
        FOUR_Liver_Score_avg = pd.DataFrame(columns=[
        "STUDYID", "BWZSCORE_avg", "liverToBW_avg", "LB_score_avg", "MI_score_avg"
        ])

        # Initialize an empty list to store the names of studies with errors
        error_studies = []

        # Initialize the master error DataFrame to store the details of errors
        master_error_df = pd.DataFrame(columns=["STUDYID", "Block", "ErrorMessage"])
   
    
   #---------------------------------------------------------------------
    # iterate over studyid or each xpt folder
    # Process each study
    for studyid in studyid_or_studyids:
        print(studyid)
        if use_xpt_file:
            
            # giving the path of the xpt folder
            path_db = studyid
            
        
        # Initialize a flag variable at the start of each iteration
        first_block_success = True

        try:
            # Set 'studyid' to None if using an XPT file; otherwise, keep the original value
            studyid = None if use_xpt_file else studyid
            
            # Call the "get_compile_data" function to get the master_compiledata
            output_get_compile_data = get_compile_data(
                studyid=studyid,
                path_db=path_db,  # Path to the database
                fake_study=fake_study,
                use_xpt_file=use_xpt_file
                )

            # Get the "master_compiledata" DataFrame from the output
            master_compiledata = output_get_compile_data

            # Create a copy of master_compiledata for diagnostic purposes
            master_compiledata_copy = master_compiledata

            # Indicate the first block succeeded
            #first_block_success = True
            
        except Exception as e:
            # Handling errors
            print(f"Error in master Compilation Data calculation: {str(e)}")

            # Log the error
            error_block1 = pd.DataFrame({
                "STUDYID": [path_db if use_xpt_file else studyid],
                "Block": ["compiledata"],
                "ErrorMessage": [str(e)]
                })

            # Append the error information to the master_error_df
            #master_error_df = pd.concat([master_error_df, error_block1], ignore_index=True)

            # Set the flag to indicate the first block failed
            #first_block_success = False
            
            # Append the error to the master_error_df
            if 'master_error_df' in globals():
                master_error_df.append(error_block1)
            else:
                master_error_df = [error_block1]

        # Set the flag to False to indicate the first block failed
        first_block_success = False

        # Check the flag to decide whether to proceed to the next iteration of the loop
        if not first_block_success:
            # Append STUDYID to the error_studies list
            #Error_studies.append(studyid)
            error_studies.append(studyid)
            # Skip to the next iteration
            continue

        # End of master_compiledata calculation
        
        #---------------------------------------------------------------------
        #----------------------score_accumulation_df--------------------------
        #This block for "Adding a new row for the current STUDYID in FOUR_Liver_Score"

        try:
            # Initialize the "FOUR_Liver_Score_avg"
            # when output_individual_scores == False and output_zscore_by_USUBJID == False
            if not output_individual_scores and not output_zscore_by_USUBJID:
                new_row_in_four_liver_scr_avg = {
                    "STUDYID": master_compiledata["STUDYID"].unique()[0],
                    "BWZSCORE_avg": None,
                    "liverToBW_avg": None,
                    "LB_score_avg": None,
                    "MI_score_avg": None,
                    }

            # Append the new row to FOUR_Liver_Score_avg
            if 'FOUR_Liver_Score_avg' in globals():
                FOUR_Liver_Score_avg.append(new_row_in_four_liver_scr_avg)
            else:
                FOUR_Liver_Score_avg = [new_row_in_four_liver_scr_avg]

        except Exception as e:
            # Handling errors of the secondary operation
            print(f"Error in FOUR_Liver_Score: {str(e)}")

            # Log the error
            error_block_flscrdf = {
                "STUDYID": path_db if use_xpt_file else studyid,
                "Block": "FOUR_Liver_Score",
                "ErrorMessage": str(e),
                }

            # Append the error to master_error_df
            if 'master_error_df' in globals():
                master_error_df.append(error_block_flscrdf)
            else:
                master_error_df = [error_block_flscrdf]

        # End of score accumulation

        # ------------------ Calculation of BodyWeight_zScore --------------------------
        
        try:
            if output_individual_scores:
                # Set 'studyid' to None if using an XPT file, otherwise keep the original value
                studyid = None if use_xpt_file else studyid

                bwzscore_BW = get_bw_score( studyid=studyid,
                                            path_db=path_db,
                                            fake_study=fake_study,
                                            use_xpt_file=use_xpt_file,
                                            master_compiledata=master_compiledata,
                                            return_individual_scores=True,
                                            return_zscore_by_USUBJID=False
                                            )
                # bwzscore_BW <- as.data.frame(bwzscore_BW)
                # master_bwzscore_BW <- rbind(master_bwzscore_BW, bwzscore_BW )

            elif output_zscore_by_USUBJID:
                # Set 'studyid' to None if using an XPT file, otherwise keep the original value
                studyid = None if use_xpt_file else studyid

                BW_zscore_by_USUBJID_HD = get_bw_score(studyid=studyid,
                                                       path_db=path_db,
                                                       fake_study=fake_study,
                                                       use_xpt_file=use_xpt_file,
                                                       master_compiledata=master_compiledata,
                                                       return_individual_scores=False,
                                                       return_zscore_by_USUBJID=True
                                                       )

                # Convert to a pandas DataFrame if needed
                BW_zscore_by_USUBJID_HD = pd.DataFrame(BW_zscore_by_USUBJID_HD)
        
            else:
                # Set 'studyid' to None if using an XPT file, otherwise keep the original value
                studyid = None if use_xpt_file else studyid

                averaged_HD_BWzScore = get_bw_score(studyid=studyid,
                                                path_db=path_db,
                                                fake_study=fake_study,
                                                use_xpt_file=use_xpt_file,
                                                master_compiledata=master_compiledata,
                                                return_individual_scores=False,
                                                return_zscore_by_USUBJID=False
                                                )

            print(averaged_HD_BWzScore)

            # Extract the liverToBW value for the current STUDYID
            calculated_BWzScore_value = averaged_HD_BWzScore.loc[
            averaged_HD_BWzScore["STUDYID"] == master_compiledata["STUDYID"].unique()[0],
            "BWZSCORE_avg"
            ].values[0]

            # Update the BWZSCORE_avg in FOUR_Liver_Score_avg for the current STUDYID
            FOUR_Liver_Score_avg.loc[
            FOUR_Liver_Score_avg["STUDYID"] == master_compiledata["STUDYID"].unique()[0],
            "BWZSCORE_avg"
            ] = calculated_BWzScore_value

        except Exception as e:
            # Handle errors
            print(f"Error in BodyWeight_zScore calculation: {str(e)}")

            # Log the error
            error_block2 = {
                "STUDYID": path_db if use_xpt_file else studyid,
                "Block": "BWZscore",
                "ErrorMessage": str(e),
                }

            # Append to master_error_df
            if 'master_error_df' in globals():
                master_error_df.append(error_block2)
            else:
                master_error_df = [error_block2]

# --------------------------- "OM_DATA" (Liver Organ to Body Weight zScore) -----

        try:
            # Set 'studyid' to None if using an XPT file, otherwise keep the original value
            studyid = None if use_xpt_file else studyid

            if output_individual_scores:
                # When output_individual_scores == True
                # Calculate liver-to-body-weight scores
                HD_liver_zscore_df = get_livertobw_score(studyid=studyid,
                                                         path_db=path_db,
                                                         fake_study=fake_study,
                                                         use_xpt_file=use_xpt_file,
                                                         master_compiledata=master_compiledata,
                                                         bwzscore_BW=bwzscore_BW,
                                                         return_individual_scores=True,
                                                         return_zscore_by_USUBJID=False
                                                         )

                # Convert to pandas DataFrame and append to master_liverToBW
                HD_liver_zscore_df = pd.DataFrame(HD_liver_zscore_df)
                master_liverToBW = pd.concat([master_liverToBW, HD_liver_zscore_df])

            elif output_zscore_by_USUBJID:
                # Set 'studyid' to None if using an XPT file, otherwise keep the original value
                studyid = None if use_xpt_file else studyid
                
               
                bwzscore_BW = get_bw_score(studyid=studyid,
                                           path_db=path_db,
                                           fake_study=fake_study,
                                           use_xpt_file=use_xpt_file,
                                           master_compiledata=master_compiledata,
                                           return_individual_scores=True,
                                           return_zscore_by_USUBJID=False
                                           )

                # Calculate liver-to-body-weight scores by USUBJID
                liverTOBW_zscore_by_USUBJID_HD = get_livertobw_score(studyid=studyid,
                                                                     path_db=path_db,
                                                                     fake_study=fake_study,
                                                                     use_xpt_file=use_xpt_file,
                                                                     master_compiledata=master_compiledata,
                                                                     bwzscore_BW=bwzscore_BW,
                                                                     return_individual_scores=False,
                                                                     return_zscore_by_USUBJID=True
                                                                     )

                # Convert to pandas DataFrame
                liverTOBW_zscore_by_USUBJID_HD = pd.DataFrame(liverTOBW_zscore_by_USUBJID_HD)
                liverTOBW_study_identifier = liverTOBW_zscore_by_USUBJID_HD["STUDYID"].unique()[0]

                # Append to master_liverToBW using study identifier
                master_liverToBW[str(liverTOBW_study_identifier)] = liverTOBW_zscore_by_USUBJID_HD

            else:
                # When neither output_individual_scores nor output_zscore_by_USUBJID is True
                bwzscore_BW = get_bw_score(studyid=studyid,
                                           path_db=path_db,
                                           fake_study=fake_study,
                                           use_xpt_file=use_xpt_file,
                                           master_compiledata=master_compiledata,
                                           return_individual_scores=True,
                                           return_zscore_by_USUBJID=False
                                           )

                # Calculate liver-to-body-weight scores by USUBJID
                averaged_liverToBW_df = get_livertobw_score(studyid=studyid,
                                                            path_db=path_db,
                                                            fake_study=fake_study,
                                                            use_xpt_file=use_xpt_file,
                                                            master_compiledata=master_compiledata,
                                                            bwzscore_BW=bwzscore_BW,
                                                            return_individual_scores=False,
                                                            return_zscore_by_USUBJID=False
                                                            )

                # Rename column for FOUR_Liver_Score_avg
                liverToBW_df = averaged_liverToBW_df.rename(columns={"avg_liverToBW_zscore": "liverToBW_avg"})

                # Extract liverToBW value for current STUDYID
                calculated_liverToBW_value = liverToBW_df.loc[
                    liverToBW_df["STUDYID"] == master_compiledata["STUDYID"].unique()[0],
                    "liverToBW_avg"
                    ].values[0]

                # Update the liverToBW_avg in FOUR_Liver_Score_avg
                print(calculated_liverToBW_value)
                FOUR_Liver_Score_avg.loc[
                    FOUR_Liver_Score_avg["STUDYID"] == master_compiledata["STUDYID"].unique()[0],
                    "liverToBW_avg"
                    ] = calculated_liverToBW_value

        except Exception as e:
            # Handle errors
            print(f"Error in Liver_Organ to Body Weight zScore: {str(e)}")

            # Log the error
            error_block3 = {
                "STUDYID": path_db if use_xpt_file else studyid,
                "Block": "LiverToBW",
                "ErrorMessage": str(e),
                }

        # Append to master_error_df
            if 'master_error_df' in globals():
                master_error_df = pd.concat([master_error_df, pd.DataFrame([error_block3])])
            else:
                master_error_df = pd.DataFrame([error_block3])

        # <><><><><><><><><><><><><><><><><><> "LB" zscoring <><><><><><><><><><><><><>
  
        try:
            # Set 'studyid' to None if using an XPT file, otherwise keep the original value
            studyid = None if use_xpt_file else studyid

            if output_individual_scores:
                # Get LB scores for individual outputs
                master_lb_scores = get_lb_score(studyid=studyid,
                                            path_db=path_db,
                                            fake_study=fake_study,
                                            use_xpt_file=use_xpt_file,
                                            master_compiledata=master_compiledata,
                                            return_individual_scores=True,
                                            return_zscore_by_USUBJID=False
                                            )

                # Append to master_lb_score_six
                master_lb_score_six = pd.concat([master_lb_score_six, master_lb_scores])

            elif output_zscore_by_USUBJID:
                # Get LB z-scores by USUBJID
                LB_zscore_by_USUBJID_HD = get_lb_score(studyid=studyid,
                                                   path_db=path_db,
                                                   fake_study=fake_study,
                                                   use_xpt_file=use_xpt_file,
                                                   master_compiledata=master_compiledata,
                                                   return_individual_scores=False,
                                                   return_zscore_by_USUBJID=True
                                                   )

                # Extract unique study identifier
                lb_study_identifier = LB_zscore_by_USUBJID_HD["STUDYID"].unique()[0]

                # Append to master_lb_score with the study identifier as key
                master_lb_score[str(lb_study_identifier)] = LB_zscore_by_USUBJID_HD

            else:
                # Get averaged LB score
                averaged_LB_score = get_lb_score(studyid=studyid,
                                             path_db=path_db,
                                             fake_study=fake_study,
                                             use_xpt_file=use_xpt_file,
                                             master_compiledata=master_compiledata,
                                             return_individual_scores=False,
                                             return_zscore_by_USUBJID=False
                                             )

                # Extract the LB score value for the current STUDYID
                calculated_LB_value = averaged_LB_score.loc[
                    averaged_LB_score["STUDYID"] == master_compiledata["STUDYID"].unique()[0],
                    "LB_score_avg"
                    ].values[0]

                # Update the LB_score_avg in FOUR_Liver_Score_avg for the current STUDYID
                FOUR_Liver_Score_avg.loc[
                    FOUR_Liver_Score_avg["STUDYID"] == master_compiledata["STUDYID"].unique()[0],
                    "LB_score_avg"
                    ] = calculated_LB_value

        except Exception as e:
                # Handling errors
                print(f"Error in LB zscoring: {str(e)}")

                # Log the error
                error_block4 = {
                    "STUDYID": path_db if use_xpt_file else studyid,
                    "Block": "LB",
                    "ErrorMessage": str(e)
                    }

                # Append to master_error_df
                if 'master_error_df' in globals():
                    master_error_df = pd.concat([master_error_df, pd.DataFrame([error_block4])])
                else:
                    master_error_df = pd.DataFrame([error_block4])

# <><><><><><><><><><><><><><><><><><> "MI" zscoring <><><><><><><><><><><><><>


        try:
            # Initialize 'studyid' to None if using an XPT file, otherwise keep its original value
            studyid = None if use_xpt_file else studyid

            if output_individual_scores:
                # Get MI scores for individual outputs
                mi_score_final_list_df = get_mi_score(studyid=studyid,
                                                      path_db=path_db,
                                                      fake_study=fake_study,
                                                      use_xpt_file=use_xpt_file,
                                                      master_compiledata=master_compiledata,
                                                      return_individual_scores=True,
                                                      return_zscore_by_USUBJID=False
                                                      )

                # Append to master_mi_df
                master_mi_df = pd.concat([master_mi_df, mi_score_final_list_df])

            elif output_zscore_by_USUBJID:
                # Get MI z-scores by USUBJID
                MI_score_by_USUBJID_HD = get_mi_score(studyid=studyid,
                                                      path_db=path_db,
                                                      fake_study=fake_study,
                                                      use_xpt_file=use_xpt_file,
                                                      master_compiledata=master_compiledata,
                                                      return_individual_scores=False,
                                                      return_zscore_by_USUBJID=True
                                                      )

                # Extract unique study identifier
                mi_study_identifier = MI_score_by_USUBJID_HD["STUDYID"].unique()[0]

                # Append to master_mi_score with the study identifier as the key
                master_mi_score[str(mi_study_identifier)] = MI_score_by_USUBJID_HD

            else:
                # Get averaged MI score
                averaged_MI_score = get_mi_score(studyid=studyid,
                                                 path_db=path_db,
                                                 fake_study=fake_study,
                                                 use_xpt_file=use_xpt_file,
                                                 master_compiledata=master_compiledata,
                                                 return_individual_scores=False,
                                                 return_zscore_by_USUBJID=False
                                                 )

                # Extract the MI score value for the current STUDYID
                calculated_MI_value = averaged_MI_score.loc[
                averaged_MI_score["STUDYID"] == master_compiledata["STUDYID"].unique()[0],
                "MI_score_avg"
                ].values[0]

                # Update the MI_score_avg in FOUR_Liver_Score_avg for the current STUDYID
                FOUR_Liver_Score_avg.loc[
                FOUR_Liver_Score_avg["STUDYID"] == master_compiledata["STUDYID"].unique()[0],
                "MI_score_avg"
                ] = calculated_MI_value

        except Exception as e:
            # Log the error
            error_block5 = {
                "STUDYID": path_db if use_xpt_file else studyid,
                "Block": "MI",
                "ErrorMessage": str(e)
                }

        # Append the error to master_error_df
        if 'master_error_df' in globals():
            master_error_df = pd.concat([master_error_df, pd.DataFrame([error_block5])])
        else:
            master_error_df = pd.DataFrame([error_block5])


    if output_individual_scores:
            # Perform a full join (merge) to keep all rows from each data frame
            combined_output_individual_scores = master_liverToBW.merge(
            master_lb_score_six, on="STUDYID", how="outer"
            ).merge(
                master_mi_df, on="STUDYID", how="outer"
            )

    elif output_zscore_by_USUBJID:
            # Concatenate data frames to combine them row-wise
            combined_liverToBW = pd.concat(master_liverToBW, ignore_index=True)
            combined_lb_score = pd.concat(master_lb_score, ignore_index=True)
            combined_mi_score = pd.concat(master_mi_score, ignore_index=True)

            # Merge the first two data frames on STUDYID and USUBJID
            combined_df = combined_liverToBW.merge(
                combined_lb_score, on=["STUDYID", "USUBJID"], how="outer"
                )

            # Merge the result with the third data frame on STUDYID and USUBJID
            final_output_zscore_by_USUBJID = combined_df.merge(
                combined_mi_score, on=["STUDYID", "USUBJID"], how="outer"
                )

    else:
        # Round all columns from the second column onward to two decimal places
        FOUR_Liver_Score_avg.iloc[:, 1:] = FOUR_Liver_Score_avg.iloc[:, 1:].round(2)

    if output_individual_scores:
        return combined_output_individual_scores

    elif output_zscore_by_USUBJID:
        return final_output_zscore_by_USUBJID

    else:
        return FOUR_Liver_Score_avg






