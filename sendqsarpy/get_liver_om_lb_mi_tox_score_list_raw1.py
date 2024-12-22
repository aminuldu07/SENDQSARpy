# -*- coding: utf-8 -*-
"""
Created on Sun Dec 22 07:58:03 2024

@author: MdAminulIsla.Prodhan
"""

import pandas as pd

def get_liver_om_lb_mi_tox_score_list(
    studyid_or_studyids=None,
    path_db=None,
    fake_study=False,
    use_xpt_file=False,
    output_individual_scores=False,
    output_zscore_by_USUBJID=False
):
    if output_individual_scores and output_zscore_by_USUBJID:
        raise ValueError("Both 'output_individual_scores' and 'output_zscore_by_USUBJID' cannot be True at the same time.")

    if output_individual_scores:
        master_liverToBW = pd.DataFrame(columns=['STUDYID', 'avg_liverToBW_zscore'])
        master_lb_score_six = pd.DataFrame(columns=[
            'STUDYID', 'avg_alb_zscore', 'avg_ast_zscore', 'avg_alp_zscore',
            'avg_alt_zscore', 'avg_bili_zscore', 'avg_ggt_zscore'
        ])
        master_mi_df = pd.DataFrame()
        error_studies = []
        master_error_df = pd.DataFrame(columns=['STUDYID', 'Block', 'ErrorMessage'])
    elif output_zscore_by_USUBJID:
        master_liverToBW = []
        master_lb_score = []
        master_mi_score = []
        error_studies = []
        master_error_df = pd.DataFrame(columns=['STUDYID', 'Block', 'ErrorMessage'])
    else:
        FOUR_Liver_Score_avg = pd.DataFrame(columns=['STUDYID', 'BWZSCORE_avg', 'liverToBW_avg', 'LB_score_avg', 'MI_score_avg'])
        error_studies = []
        master_error_df = pd.DataFrame(columns=['STUDYID', 'Block', 'ErrorMessage'])

    for studyid in studyid_or_studyids:
        print(studyid)
        if use_xpt_file:
            path_db = studyid
            print(path_db)

        first_block_success = True

        try:
            studyid = None if use_xpt_file else studyid
            output_get_compile_data = get_compile_data(studyid=studyid, path_db=path_db, fake_study=fake_study, use_xpt_file=use_xpt_file)
            master_compiledata = output_get_compile_data
            master_compiledata_copy = master_compiledata.copy()
        except Exception as e:
            print(f"Error in master Compilation Data calculation: {e}")
            error_block1 = {
                "STUDYID": path_db if use_xpt_file else studyid,
                "Block": "compiledata",
                "ErrorMessage": str(e)
            }
            master_error_df = pd.concat([master_error_df, pd.DataFrame([error_block1])], ignore_index=True)
            first_block_success = False

        if not first_block_success:
            error_studies.append(studyid)
            continue

        try:
            if not output_individual_scores and not output_zscore_by_USUBJID:
                new_row_in_four_liver_scr_avg = {
                    "STUDYID": master_compiledata['STUDYID'].iloc[0],
                    "BWZSCORE_avg": None,
                    "liverToBW_avg": None,
                    "LB_score_avg": None,
                    "MI_score_avg": None
                }
                FOUR_Liver_Score_avg = pd.concat([FOUR_Liver_Score_avg, pd.DataFrame([new_row_in_four_liver_scr_avg])], ignore_index=True)
        except Exception as e:
            print(f"Error in FOUR_Liver_Score: {e}")
            error_block_flscrdf = {
                "STUDYID": path_db if use_xpt_file else studyid,
                "Block": "FOUR_Liver_Score",
                "ErrorMessage": str(e)
            }
            master_error_df = pd.concat([master_error_df, pd.DataFrame([error_block_flscrdf])], ignore_index=True)

    return {
        "error_studies": error_studies,
        "master_error_df": master_error_df
    }

# Placeholder for get_compile_data function
def get_compile_data(studyid, path_db, fake_study, use_xpt_file):
    # Example implementation for demonstration purposes
    return pd.DataFrame({"STUDYID": [studyid], "SomeData": [1]})

#-----------------------------------------------------------------------------
try:
    # BodyWeight_zScore Calculation
    if output_individual_scores:
        studyid = None if use_xpt_file else studyid

        bwzscore_BW = get_bw_score(
            studyid=studyid,
            path_db=path_db,
            fake_study=fake_study,
            use_xpt_file=use_xpt_file,
            master_compiledata=master_compiledata,
            return_individual_scores=True,
            return_zscore_by_USUBJID=False
        )

    elif output_zscore_by_USUBJID:
        studyid = None if use_xpt_file else studyid

        BW_zscore_by_USUBJID_HD = get_bw_score(
            studyid=studyid,
            path_db=path_db,
            fake_study=fake_study,
            use_xpt_file=use_xpt_file,
            master_compiledata=master_compiledata,
            return_individual_scores=False,
            return_zscore_by_USUBJID=True
        )

    else:
        studyid = None if use_xpt_file else studyid

        averaged_HD_BWzScore = get_bw_score(
            studyid=studyid,
            path_db=path_db,
            fake_study=fake_study,
            use_xpt_file=use_xpt_file,
            master_compiledata=master_compiledata,
            return_individual_scores=False,
            return_zscore_by_USUBJID=False
        )
        print(averaged_HD_BWzScore)

        calculated_BWzScore_value = averaged_HD_BWzScore.loc[
            averaged_HD_BWzScore["STUDYID"] == master_compiledata["STUDYID"].iloc[0],
            "BWZSCORE_avg"
        ].values[0]

        FOUR_Liver_Score_avg.loc[
            FOUR_Liver_Score_avg["STUDYID"] == master_compiledata["STUDYID"].iloc[0],
            "BWZSCORE_avg"
        ] = calculated_BWzScore_value

except Exception as e:
    error_block2 = {
        "STUDYID": path_db if use_xpt_file else studyid,
        "Block": "BWZscore",
        "ErrorMessage": str(e)
    }
    master_error_df = pd.concat([master_error_df, pd.DataFrame([error_block2])], ignore_index=True)
    print(f"Error in BodyWeight_zScore calculation: {str(e)}")

try:
    # OM_DATA: Liver Organ to Body Weight zScore Calculation
    studyid = None if use_xpt_file else studyid

    if output_individual_scores:
        HD_liver_zscore_df = get_livertobw_score(
            studyid=studyid,
            path_db=path_db,
            fake_study=fake_study,
            use_xpt_file=use_xpt_file,
            master_compiledata=master_compiledata,
            bwzscore_BW=bwzscore_BW,
            return_individual_scores=True,
            return_zscore_by_USUBJID=False
        )
        master_liverToBW = pd.concat([master_liverToBW, HD_liver_zscore_df], ignore_index=True)

    elif output_zscore_by_USUBJID:
        bwzscore_BW = get_bw_score(
            studyid=studyid,
            path_db=path_db,
            fake_study=fake_study,
            use_xpt_file=use_xpt_file,
            master_compiledata=master_compiledata,
            return_individual_scores=True,
            return_zscore_by_USUBJID=False
        )

        liverTOBW_zscore_by_USUBJID_HD = get_livertobw_score(
            studyid=studyid,
            path_db=path_db,
            fake_study=fake_study,
            use_xpt_file=use_xpt_file,
            master_compiledata=master_compiledata,
            bwzscore_BW=bwzscore_BW,
            return_individual_scores=False,
            return_zscore_by_USUBJID=True
        )

        study_identifier = liverTOBW_zscore_by_USUBJID_HD["STUDYID"].unique()[0]
        master_liverToBW[str(study_identifier)] = liverTOBW_zscore_by_USUBJID_HD

    else:
        bwzscore_BW = get_bw_score(
            studyid=studyid,
            path_db=path_db,
            fake_study=fake_study,
            use_xpt_file=use_xpt_file,
            master_compiledata=master_compiledata,
            return_individual_scores=True,
            return_zscore_by_USUBJID=False
        )

        averaged_liverToBW_df = get_livertobw_score(
            studyid=studyid,
            path_db=path_db,
            fake_study=fake_study,
            use_xpt_file=use_xpt_file,
            master_compiledata=master_compiledata,
            bwzscore_BW=bwzscore_BW,
            return_individual_scores=False,
            return_zscore_by_USUBJID=False
        )

        calculated_liverToBW_value = averaged_liverToBW_df.loc[
            averaged_liverToBW_df["STUDYID"] == master_compiledata["STUDYID"].iloc[0],
            "avg_liverToBW_zscore"
        ].values[0]

        FOUR_Liver_Score_avg.loc[
            FOUR_Liver_Score_avg["STUDYID"] == master_compiledata["STUDYID"].iloc[0],
            "liverToBW_avg"
        ] = calculated_liverToBW_value

except Exception as e:
    error_block3 = {
        "STUDYID": path_db if use_xpt_file else studyid,
        "Block": "LiverToBW",
        "ErrorMessage": str(e)
    }
    master_error_df = pd.concat([master_error_df, pd.DataFrame([error_block3])], ignore_index=True)
    print(f"Error in Liver Organ to Body Weight zScore calculation: {str(e)}")

try:
    # LB zscoring
    studyid = None if use_xpt_file else studyid

    if output_individual_scores:
        master_lb_scores = get_lb_score(
            studyid=studyid,
            path_db=path_db,
            fake_study=fake_study,
            use_xpt_file=use_xpt_file,
            master_compiledata=master_compiledata,
            return_individual_scores=True,
            return_zscore_by_USUBJID=False
        )
        master_lb_score_six = pd.concat([master_lb_score_six, master_lb_scores], ignore_index=True)

    elif output_zscore_by_USUBJID:
        LB_zscore_by_USUBJID_HD = get_lb_score(
            studyid=studyid,
            path_db=path_db,
            fake_study=fake_study,
            use_xpt_file=use_xpt_file,
            master_compiledata=master_compiledata,
            return_individual_scores=False,
            return_zscore_by_USUBJID=True
        )
        lb_study_identifier = LB_zscore_by_USUBJID_HD["STUDYID"].unique()[0]
        master_lb_score[str(lb_study_identifier)] = LB_zscore_by_USUBJID_HD

    else:
        averaged_LB_score = get_lb_score(
            studyid=studyid,
            path_db=path_db,
            fake_study=fake_study,
            use_xpt_file=use_xpt_file,
            master_compiledata=master_compiledata,
            return_individual_scores=False,
            return_zscore_by_USUBJID=False
        )
        calculated_LB_value = averaged_LB_score.loc[
            averaged_LB_score["STUDYID"] == master_compiledata["STUDYID"].iloc[0],
            "LB_score_avg"
        ].values[0]
        FOUR_Liver_Score_avg.loc[
            FOUR_Liver_Score_avg["STUDYID"] == master_compiledata["STUDYID"].iloc[0],
            "LB_score_avg"
        ] = calculated_LB_value

except Exception as e:
    error_block4 = {
        "STUDYID": path_db if use_xpt_file else studyid,
        "Block": "LB",
        "ErrorMessage": str(e)
    }
    master_error_df = pd.concat([master_error_df, pd.DataFrame([error_block4])], ignore_index=True)
    print(f"Error in LB zscoring: {str(e)}")

try:
    # MI zscoring
    studyid = None if use_xpt_file else studyid

    if output_individual_scores:
        mi_score_final_list_df = get_mi_score(
            studyid=studyid,
            path_db=path_db,
            fake_study=fake_study,
            use_xpt_file=use_xpt_file,
            master_compiledata=master_compiledata,
            return_individual_scores=True,
            return_zscore_by_USUBJID=False
        )
        master_mi_df = pd.concat([master_mi_df, mi_score_final_list_df], ignore_index=True)

    elif output_zscore_by_USUBJID:
        MI_score_by_USUBJID_HD = get_mi_score(
            studyid=studyid,
            path_db=path_db,
            fake_study=fake_study,
            use_xpt_file=use_xpt_file,
            master_compiledata=master_compiledata,
            return_individual_scores=False,
            return_zscore_by_USUBJID=True
        )
        mi_study_identifier = MI_score_by_USUBJID_HD["STUDYID"].unique()[0]
        master_mi_score[str(mi_study_identifier)] = MI_score_by_USUBJID_HD

    else:
        averaged_MI_score = get_mi_score(
            studyid=studyid,
            path_db=path_db,
            fake_study=fake_study,
            use_xpt_file=use_xpt_file,
            master_compiledata=master_compiledata,
            return_individual_scores=False,
            return_zscore_by_USUBJID=False
        )
        calculated_MI_value = averaged_MI_score.loc[
            averaged_MI_score["STUDYID"] == master_compiledata["STUDYID"].iloc[0],
            "MI_score_avg"
        ].values[0]
        FOUR_Liver_Score_avg.loc[
            FOUR_Liver_Score_avg["STUDYID"] == master_compiledata["STUDYID"].iloc[0],
            "MI_score_avg"
        ] = calculated_MI_value

except Exception as e:
    error_block5 = {
        "STUDYID": path_db if use_xpt_file else studyid,
        "Block": "MI",
        "ErrorMessage": str(e)
    }
    master_error_df = pd.concat([master_error_df, pd.DataFrame([error_block5])], ignore_index=True)
    print(f"Error in MI zscoring: {str(e)}")

# Combine outputs
if output_individual_scores:
    combined_output_individual_scores = master_liverToBW.merge(
        master_lb_score_six, on="STUDYID", how="outer"
    ).merge(
        master_mi_df, on="STUDYID", how="outer"
    )
    result = combined_output_individual_scores

elif output_zscore_by_USUBJID:
    combined_liverToBW = pd.concat(master_liverToBW.values(), ignore_index=True)
    combined_lb_score = pd.concat(master_lb_score.values(), ignore_index=True)
    combined_mi_score = pd.concat(master_mi_score.values(), ignore_index=True)

    combined_df = combined_liverToBW.merge(
        combined_lb_score, on=["STUDYID", "USUBJID"], how="outer"
    ).merge(
        combined_mi_score, on=["STUDYID", "USUBJID"], how="outer"
    )
    result = combined_df

else:
    FOUR_Liver_Score_avg.iloc[:, 1:] = FOUR_Liver_Score_avg.iloc[:, 1:].round(2)
    result = FOUR_Liver_Score_avg

return result



