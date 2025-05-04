from sendqsarpy.get_bw_score import get_bw_score

# Call the function
db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/DATABASES/fake_merged_liver_not_liver.db"
fake_T_xpt_F_bw_score = get_bw_score(studyid="28738",
                                         path_db=db_path, 
                                         fake_study=True, 
                                         use_xpt_file=False, 
                                         master_compiledata=None, 
                                         return_individual_scores=False, 
                                         return_zscore_by_USUBJID=False)


# Call the function
db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/DATABASES/single_fake_xpt_folder/FAKE28738"
fake_T_xpt_T_bw_score = get_bw_score(studyid=None,
                                         path_db=db_path, 
                                         fake_study=True, 
                                         use_xpt_file=True, 
                                         master_compiledata=None, 
                                         return_individual_scores=False, 
                                         return_zscore_by_USUBJID=False)


# Call the function for SEND SQLite database
db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/DATABASES/TestDB.db"
real_sqlite_bw_score = get_bw_score(studyid="5003635",
                                         path_db=db_path, 
                                         fake_study=False, 
                                         use_xpt_file=False, 
                                         master_compiledata=None, 
                                         return_individual_scores=False, 
                                         return_zscore_by_USUBJID=False)

# Call the function for SEND XPT data
db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/DATABASES/real_xpt_dir/IND051292_1017-3581"
real_XPT_bw_score = get_bw_score(studyid=None,
                                         path_db=db_path, 
                                         fake_study=False, 
                                         use_xpt_file=True, 
                                         master_compiledata=None, 
                                         return_individual_scores=False, 
                                         return_zscore_by_USUBJID=False)