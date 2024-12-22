# -*- coding: utf-8 -*-
"""
Created on Sun Dec 22 07:51:59 2024

@author: MdAminulIsla.Prodhan
"""

import pandas as pd
import sqlite3

def get_treatment_group_amin(studyid=None, 
                             path_db=None, 
                             fake_study=False, 
                             use_xpt_file=False, 
                             master_compiledata=None, 
                             return_individual_scores=False, 
                             return_zscore_by_USUBJID=False):
    # Initialize results
    list_return = {}
    four = []  # For studies with exactly 4 treatment groups
    path = path_db

    # Helper function to fetch data from SQLite database
    def fetch_domain_data(db_connection, domain_name, studyid):
        domain_name = domain_name.upper()
        query_statement = f"SELECT * FROM {domain_name} WHERE STUDYID = ?"
        query_result = pd.read_sql_query(query_statement, db_connection, params=(studyid,))
        return query_result

    # Read data
    if use_xpt_file:
        import pyreadstat  # For reading .xpt files
        mi, _ = pyreadstat.read_xport(f"{path}/mi.xpt")
        dm, _ = pyreadstat.read_xport(f"{path}/dm.xpt")
    else:
        with sqlite3.connect(path) as db_connection:
            tx = fetch_domain_data(db_connection, "tx", studyid)
            ts = fetch_domain_data(db_connection, "ts", studyid)
            ds = fetch_domain_data(db_connection, "ds", studyid)
            dm = fetch_domain_data(db_connection, "dm", studyid)
            pc = fetch_domain_data(db_connection, "pc", studyid)
            pp = fetch_domain_data(db_connection, "pp", studyid)
            pooldef = fetch_domain_data(db_connection, "pooldef", studyid)

    # Identify unique treatment groups (SETCD) and study species
    number_of_setcd = dm['SETCD'].unique()
    print(number_of_setcd)

    st_species = ts.loc[ts['TSPARMCD'] == 'SPECIES', 'TSVAL'].values

    # Initialize groups
    recovery_group = []
    not_rat_recovery_group = []
    treatment_group = []
    not_rat_treatment_group = []

    Dose_Level_df = pd.DataFrame(columns=["STUDYID", "Dose_Level", "Dose_Units", "Treatment_SETCD"])

    if len(st_species) != 0:
        if st_species[0] in ["RAT", "MOUSE"]:
            setcd_dm_tk_less = dm[dm['USUBJID'].isin(pc['USUBJID'])][["STUDYID", "USUBJID", "SETCD"]]
            tk_group = setcd_dm_tk_less['SETCD'].unique()

            not_tk_group = [x for x in number_of_setcd if x not in tk_group]

            for set_cd in not_tk_group:
                subjid = dm[dm['SETCD'] == set_cd]['USUBJID'].unique()
                dsdecod = ds[ds['USUBJID'].isin(subjid)]['DSDECOD'].str.lower().unique()

                if "recovery sacrifice" in dsdecod:
                    recovery_group.append(set_cd)
                elif any(x in dsdecod for x in ["terminal sacrifice", "moribund sacrifice", 
                                                "removed from study alive", "non-moribund sacrifice"]):
                    treatment_group.append(set_cd)

            print(recovery_group)
            print(treatment_group)

            for trtm_setcd in treatment_group:
                tx_trtm_setcd = tx[tx['SETCD'] == trtm_setcd]
                dose_level = tx_trtm_setcd[tx_trtm_setcd['TXPARMCD'] == "TRTDOS"]['TXVAL'].values
                dose_units = tx_trtm_setcd[tx_trtm_setcd['TXPARMCD'] == "TRTDOSU"]['TXVAL'].values

                dose_level_df = pd.DataFrame({
                    "STUDYID": tx['STUDYID'].unique(),
                    "Dose_Level": dose_level,
                    "Dose_Units": dose_units,
                    "Treatment_SETCD": trtm_setcd
                })
                Dose_Level_df = pd.concat([Dose_Level_df, dose_level_df], ignore_index=True)

        else:
            for not_rat_set_cd in number_of_setcd:
                not_rat_subjid = dm[dm['SETCD'] == not_rat_set_cd]['USUBJID'].unique()
                not_rat_dsdecod = ds[ds['USUBJID'].isin(not_rat_subjid)]['DSDECOD'].str.lower().unique()

                if "recovery sacrifice" in not_rat_dsdecod:
                    not_rat_recovery_group.append(not_rat_set_cd)
                elif any(x in not_rat_dsdecod for x in ["terminal sacrifice", "moribund sacrifice", 
                                                        "removed from study alive"]):
                    not_rat_treatment_group.append(not_rat_set_cd)

            print(not_rat_recovery_group)
            print(not_rat_treatment_group)

            for not_rat_trtm_setcd in not_rat_treatment_group:
                not_rat_tx_trtm_setcd = tx[tx['SETCD'] == not_rat_trtm_setcd]
                not_rat_dose_level = not_rat_tx_trtm_setcd[not_rat_tx_trtm_setcd['TXPARMCD'] == "TRTDOS"]['TXVAL'].values
                not_rat_dose_units = not_rat_tx_trtm_setcd[not_rat_tx_trtm_setcd['TXPARMCD'] == "TRTDOSU"]['TXVAL'].values

                not_rat_dose_level_df = pd.DataFrame({
                    "STUDYID": tx['STUDYID'].unique(),
                    "Dose_Level": not_rat_dose_level,
                    "Dose_Units": not_rat_dose_units,
                    "Treatment_SETCD": not_rat_trtm_setcd
                })
                Dose_Level_df = pd.concat([Dose_Level_df, not_rat_dose_level_df], ignore_index=True)

    if "RAT" in st_species:
        return Dose_Level_df
    else:
        return Dose_Level_df
