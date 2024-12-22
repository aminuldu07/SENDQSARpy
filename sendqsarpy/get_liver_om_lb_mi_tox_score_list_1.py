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

    # Initialize variables based on output options
    if output_individual_scores:
        master_liverToBW = pd.DataFrame(columns=['STUDYID', 'avg_liverToBW_zscore'])
        master_lb_score_six = pd.DataFrame(columns=[
            'STUDYID', 'avg_alb_zscore', 'avg_ast_zscore', 'avg_alp_zscore',
            'avg_alt_zscore', 'avg_bili_zscore', 'avg_ggt_zscore'
        ])
        master_mi_df = pd.DataFrame()
        master_error_df = pd.DataFrame(columns=['STUDYID', 'Block', 'ErrorMessage'])

    elif output_zscore_by_USUBJID:
        master_liverToBW, master_lb_score, master_mi_score = [], [], []
        master_error_df = pd.DataFrame(columns=['STUDYID', 'Block', 'ErrorMessage'])

    else:
        FOUR_Liver_Score_avg = pd.DataFrame(columns=['STUDYID', 'BWZSCORE_avg', 'liverToBW_avg', 'LB_score_avg', 'MI_score_avg'])
        master_error_df = pd.DataFrame(columns=['STUDYID', 'Block', 'ErrorMessage'])

    error_studies = []

    # Process each study ID
    for studyid in studyid_or_studyids:
        print(f"Processing STUDYID: {studyid}")
        if use_xpt_file:
            path_db = studyid

        try:
            studyid = None if use_xpt_file else studyid
            master_compiledata = get_compile_data(studyid, path_db, fake_study, use_xpt_file)
        except Exception as e:
            print(f"Error in master Compilation Data calculation: {e}")
            master_error_df = log_error(master_error_df, studyid, "compiledata", e)
            error_studies.append(studyid)
            continue

        try:
            if not output_individual_scores and not output_zscore_by_USUBJID:
                new_row = {
                    "STUDYID": master_compiledata['STUDYID'].iloc[0],
                    "BWZSCORE_avg": None,
                    "liverToBW_avg": None,
                    "LB_score_avg": None,
                    "MI_score_avg": None
                }
                FOUR_Liver_Score_avg = pd.concat([FOUR_Liver_Score_avg, pd.DataFrame([new_row])], ignore_index=True)
        except Exception as e:
            print(f"Error in FOUR_Liver_Score processing: {e}")
            master_error_df = log_error(master_error_df, studyid, "FOUR_Liver_Score", e)

    # Process BodyWeight zScore
    try:
        process_bodyweight_zscore(
            studyid, path_db, fake_study, use_xpt_file, output_individual_scores,
            output_zscore_by_USUBJID, master_compiledata, FOUR_Liver_Score_avg
        )
    except Exception as e:
        master_error_df = log_error(master_error_df, studyid, "BWZscore", e)

    # Process Liver to Body Weight zScore
    try:
        process_livertobw_zscore(
            studyid, path_db, fake_study, use_xpt_file, output_individual_scores,
            output_zscore_by_USUBJID, master_compiledata, master_liverToBW, FOUR_Liver_Score_avg
        )
    except Exception as e:
        master_error_df = log_error(master_error_df, studyid, "LiverToBW", e)

    # Process LB zScoring
    try:
        process_lb_zscore(
            studyid, path_db, fake_study, use_xpt_file, output_individual_scores,
            output_zscore_by_USUBJID, master_compiledata, master_lb_score_six, FOUR_Liver_Score_avg
        )
    except Exception as e:
        master_error_df = log_error(master_error_df, studyid, "LB", e)

    # Process MI zScoring
    try:
        process_mi_zscore(
            studyid, path_db, fake_study, use_xpt_file, output_individual_scores,
            output_zscore_by_USUBJID, master_compiledata, master_mi_df, FOUR_Liver_Score_avg
        )
    except Exception as e:
        master_error_df = log_error(master_error_df, studyid, "MI", e)

    # Combine outputs
    if output_individual_scores:
        result = combine_individual_scores(master_liverToBW, master_lb_score_six, master_mi_df)
    elif output_zscore_by_USUBJID:
        result = combine_zscore_by_usubjid(master_liverToBW, master_lb_score, master_mi_score)
    else:
        FOUR_Liver_Score_avg.iloc[:, 1:] = FOUR_Liver_Score_avg.iloc[:, 1:].round(2)
        result = FOUR_Liver_Score_avg

    return {
        "error_studies": error_studies,
        "master_error_df": master_error_df,
        "result": result
    }

def get_compile_data(studyid, path_db, fake_study, use_xpt_file):
    # Placeholder function
    return pd.DataFrame({"STUDYID": [studyid], "SomeData": [1]})

def log_error(error_df, studyid, block, error):
    error_entry = {
        "STUDYID": studyid,
        "Block": block,
        "ErrorMessage": str(error)
    }
    return pd.concat([error_df, pd.DataFrame([error_entry])], ignore_index=True)

def process_bodyweight_zscore(*args):
    # Placeholder function for BodyWeight zScore processing
    pass

def process_livertobw_zscore(*args):
    # Placeholder function for Liver to Body Weight zScore processing
    pass

def process_lb_zscore(*args):
    # Placeholder function for LB zScoring
    pass

def process_mi_zscore(*args):
    # Placeholder function for MI zScoring
    pass

def combine_individual_scores(liverToBW, lb_score_six, mi_df):
    return liverToBW.merge(lb_score_six, on="STUDYID", how="outer").merge(mi_df, on="STUDYID", how="outer")

def combine_zscore_by_usubjid(liverToBW, lb_score, mi_score):
    combined_liverToBW = pd.concat(liverToBW, ignore_index=True)
    combined_lb_score = pd.concat(lb_score, ignore_index=True)
    combined_mi_score = pd.concat(mi_score, ignore_index=True)
    return combined_liverToBW.merge(combined_lb_score, on=["STUDYID", "USUBJID"], how="outer").merge(combined_mi_score, on=["STUDYID", "USUBJID"], how="outer")
