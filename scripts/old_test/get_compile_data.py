import sqlite3
import pandas as pd
import numpy as np
import pyreadstat
#import re
import os
#from pathlib import Path

def get_compile_data(studyid=None, path_db=None, fake_study=False, use_xpt_file=False):
    # Convert studyid to string
    studyid = str(studyid)
    path = path_db

    # Function to fetch domain data from SQLite database
    def fetch_domain_data(db_connection, domain_name, studyid):
        domain_name = domain_name.upper()
        query_statement = f"SELECT * FROM {domain_name} WHERE STUDYID = ?"
        query_result = pd.read_sql_query(query_statement, db_connection, params=(studyid,))
        return query_result

    if fake_study and not use_xpt_file:
        # Connect to SQLite database
        db_connection = sqlite3.connect(path)

        # Fetch data for 'dm' and 'ts' domains
        dm = fetch_domain_data(db_connection, 'dm', studyid)
        ts = fetch_domain_data(db_connection, 'ts', studyid)

        # Close the database connection
        db_connection.close()

        # Convert 'dm' and 'ts' to pandas DataFrames
        dm = pd.DataFrame(dm)
        ts = pd.DataFrame(ts)

        # Fetch species value from 'ts' table where 'TSPARMCD' equals 'SPECIES'
        species = ts.loc[ts['TSPARMCD'] == 'SPECIES', 'TSVAL'].values[0]

        # Select specific columns from 'dm'
        dm = dm[['STUDYID', 'USUBJID', 'SPECIES', 'SEX', 'ARMCD', 'ARM', 'SETCD']]
        
        # Add the 'Species' column
        dm['Species'] = dm['SPECIES']
        
        # update the 'Species" column of dm with "species" variable 
        dm['Species'] = species
        
        # remove columns "SPECIES", "ARMCD"
        dm.drop(columns=['SPECIES', 'ARMCD'], inplace=True)

        # Rename ARM to ARMCD (if ARMCD is needed)
        dm = dm.rename(columns={'ARM': 'ARMCD'})
        
        # rearrange the columns of dm 
        dm = dm[['STUDYID', 'USUBJID', 'Species', 'SEX', 'ARMCD', 'SETCD']]
        
        # Update 'ARMCD' to 'vehicle' where it equals 'Control'
        dm['ARMCD'] = dm['ARMCD'].replace('Control', 'vehicle')

        # Filter 'dm' to include only rows where 'ARMCD' is 'vehicle' or 'HD'
        dm = dm[dm['ARMCD'].isin(['vehicle', 'HD'])]

        return dm

    elif fake_study and use_xpt_file:
        # Read XPT files using pandas
        #dm = pd.read_sas(os.path.join(path, 'dm.xpt'), format='xport')
        #ts = pd.read_sas(os.path.join(path, 'ts.xpt'), format='xport')
        
        # Read data from .xpt files using pyreadstat
        dm, meta = pyreadstat.read_xport(os.path.join(path, 'dm.xpt'))
        ts, meta = pyreadstat.read_xport(os.path.join(path, 'ts.xpt'))

        # Convert to DataFrame
        dm = pd.DataFrame(dm)
        ts = pd.DataFrame(ts)
        
        # Decode TSPARMCD column to strings
        #ts['TSPARMCD'] = ts['TSPARMCD'].str.decode('utf-8')
        
        # Decode all object columns to UTF-8
        for col in dm.select_dtypes(include=['object']):
         dm[col] = dm[col].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)
        
        # Decode all object columns to UTF-8
        for col in ts.select_dtypes(include=['object']):
         ts[col] = ts[col].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)

        # Fetch species value from 'ts'
        species = ts.loc[ts['TSPARMCD'] == 'SPECIES', 'TSVAL'].values[0]

        # Select specific columns and add 'Species' to 'dm'
        dm = dm[['STUDYID', 'USUBJID', 'SEX', 'ARMCD', 'ARM', 'SETCD']]
        
        # update the 'Species" column of dm with "species" variable 
        dm['Species'] = species

       # remove column "ARMCD"
        dm.drop(columns=['ARMCD'], inplace=True)
        
        # Rename ARM to ARMCD
        dm = dm.rename(columns={'ARM': 'ARMCD'})
        
        # Select specific columns and add 'Species' to 'dm'
        dm = dm[['STUDYID', 'USUBJID', 'Species', 'SEX', 'ARMCD', 'SETCD']]
        
        # Update 'ARMCD' to 'vehicle' where it equals 'Control'
        dm['ARMCD'] = dm['ARMCD'].replace('Control', 'vehicle')
        
        # Filter 'dm' to include only rows where 'ARMCD' is 'vehicle' or 'HD'
        dm = dm[dm['ARMCD'].isin(['vehicle', 'HD'])]

        return dm

    elif not fake_study and not use_xpt_file:
        # Connect to SQLite database
        db_connection = sqlite3.connect(path)

        # Fetch data for all relevant domains
        bw = fetch_domain_data(db_connection, 'bw', studyid)
        dm = fetch_domain_data(db_connection, 'dm', studyid)
        ds = fetch_domain_data(db_connection, 'ds', studyid)
        ts = fetch_domain_data(db_connection, 'ts', studyid)
        tx = fetch_domain_data(db_connection, 'tx', studyid)
        pp = fetch_domain_data(db_connection, 'pp', studyid)
        pooldef = fetch_domain_data(db_connection, 'pooldef', studyid)

        # Close the database connection
        db_connection.close()

    elif not fake_study and use_xpt_file:
                
        # Read data from .xpt files using pyreadstat
        bw, meta = pyreadstat.read_xport(os.path.join(path, 'bw.xpt'))
        dm, meta = pyreadstat.read_xport(os.path.join(path, 'dm.xpt'), encoding='latin1')
        ds, meta = pyreadstat.read_xport(os.path.join(path, 'ds.xpt'))
        ts, meta = pyreadstat.read_xport(os.path.join(path, 'ts.xpt'), encoding='latin1')
        tx, meta = pyreadstat.read_xport(os.path.join(path, 'tx.xpt'), encoding='latin1')
        pp, meta = pyreadstat.read_xport(os.path.join(path, 'pp.xpt'))
        pooldef, meta = pyreadstat.read_xport(os.path.join(path, 'pooldef.xpt'))
      

    # Decode all object columns to UTF-8
    for col in bw.select_dtypes(include=['object']):
     bw[col] = bw[col].apply(lambda x: x.decode('iso-8859-1', errors='ignore') if isinstance(x, bytes) else x)
     
    # Decode all object columns to UTF-8
    for col in dm.select_dtypes(include=['object']):
     dm[col] = dm[col].apply(lambda x: x.decode('iso-8859-1', errors='ignore') if isinstance(x, bytes) else x)
     
     # Decode all object columns to UTF-8
     for col in ds.select_dtypes(include=['object']):
      ds[col] = ds[col].apply(lambda x: x.decode('iso-8859-1', errors='ignore') if isinstance(x, bytes) else x)

    # Decode all object columns to UTF-8
    for col in ts.select_dtypes(include=['object']):
     ts[col] = ts[col].apply(lambda x: x.decode('iso-8859-1', errors='ignore') if isinstance(x, bytes) else x)
     
    # Decode all object columns to UTF-8
    for col in tx.select_dtypes(include=['object']):
     tx[col] = tx[col].apply(lambda x: x.decode('iso-8859-1', errors='ignore') if isinstance(x, bytes) else x)
     
    # Decode all object columns to UTF-8
    for col in pp.select_dtypes(include=['object']):
     pp[col] = pp[col].apply(lambda x: x.decode('iso-8859-1', errors='ignore') if isinstance(x, bytes) else x)
     
    # Decode all object columns to UTF-8
    for col in pooldef.select_dtypes(include=['object']):
     pooldef[col] = pooldef[col].apply(lambda x: x.decode('iso-8859-1', errors='ignore') if isinstance(x, bytes) else x)
     
    # Compilation of DM Data
    compile_data = pd.DataFrame(columns=['STUDYID', 'Species', 'USUBJID', 'SEX', 'ARMCD', 'SETCD'])

    # Pull species and other details from 'ts'
    species = ts.loc[ts['TSPARMCD'] == "SPECIES", 'TSVAL'].values[0]
    duration = ts.loc[ts['TSPARMCD'] == "DOSDUR", 'TSVAL'].values[0]
    

    # Convert duration to days
    if "W" in duration:
        days = int(''.join(filter(str.isdigit, duration))) * 7
    elif "M" in duration:
        days = int(''.join(filter(str.isdigit, duration))) * 30
    else:
        days = int(''.join(filter(str.isdigit, duration)))
    duration = f"{days}D"

    # Create DM Data
    studyid_unique = ts['STUDYID'].unique()[0]
    
    # Create DM Data    
    dm_data = pd.DataFrame({
        'STUDYID': [studyid_unique] * len(dm),
        'Species': [species] * len(dm),
        'USUBJID': dm['USUBJID'],
        'SEX': dm['SEX'],
        'ARMCD': dm['ARMCD'],
        'SETCD': dm['SETCD']
    })

    # Append DM Data to compile_data
    compile_data = pd.concat([compile_data, dm_data], ignore_index=True).dropna()

    # Create a copy of CompileData which will not
    # changes with changing the CompileData
    compile_data_copy = pd.DataFrame(compile_data)

    # Step-2 :: # REMOVE THE RECOVERY ANIMALS from "CompileData"...<>"Recovery
    #  animals" cleaning.. using "DS domain"

    # Filter for specific "DSDECOD" values and keep the mentioned four
    filtered_ds = ds[ds['DSDECOD'].isin([
    'TERMINAL SACRIFICE',
    'MORIBUND SACRIFICE',
    'REMOVED FROM STUDY ALIVE',
    'NON-MORIBUND SACRIFICE'
    ])]
      
   # Filter "compile_data" to keep rows where USUBJID is in "filtered_ds"
   # meaning removing recovery animals
    recovery_cleaned_compile_data = compile_data[compile_data['USUBJID'].isin(filtered_ds['USUBJID'])]
 
        
    # Step-3: REMOVE THE TK ANIMALS IF SPECIES IS RAT from the 
    # "recovery_cleaned_CompileData"


    # Initialize an empty DataFrame to store the results
    tK_animals_df = pd.DataFrame(columns=['PP_PoolID', 'STUDYID', 'USUBJID', 'POOLID'])

    # Initialize a DataFrame to keep track of studies with no POOLID
    no_poolid_studies = pd.DataFrame(columns=['STUDYID'])

    # check if the current study is a 'rat' study
    # Check if the species is a rat (case-insensitive)
    
    Species_lower = species.lower()

    if "rat" in Species_lower:
    # Create TK individuals for "Rat" studies
    # Retrieve unique pool IDs (TKPools) from the pp table
     TKPools = pp['POOLID'].unique()

    # Check if TKPools is not empty
     if len(TKPools) > 0:
        # For each pool ID in TKPools, retrieve corresponding rows from pooldef table
        for pool_id in TKPools:
            pooldef_data = pooldef[pooldef['POOLID'] == pool_id]

            # Create a temporary DataFrame if pooldef_data is not empty
            if not pooldef_data.empty:
                temp_df = pd.DataFrame({
                    'PP_PoolID': [pool_id] * len(pooldef_data),
                    'STUDYID': pooldef_data['STUDYID'],
                    'USUBJID': pooldef_data['USUBJID'],
                    'POOLID': pooldef_data['POOLID']
                })

                # Append the temporary DataFrame to the results DataFrame
                tK_animals_df = pd.concat([tK_animals_df, temp_df], ignore_index=True)
     else:
        # Retrieve STUDYID for the current study
        current_study_id = bw['STUDYID'].iloc[0]

        # Add study to no_poolid_studies DataFrame
        no_poolid_studies = pd.concat([no_poolid_studies, pd.DataFrame({'STUDYID': [current_study_id]})], ignore_index=True)
    else:
    # Create an empty DataFrame named "tK_animals_df"
     tK_animals_df = pd.DataFrame(columns=['PP_PoolID', 'STUDYID', 'USUBJID', 'POOLID'])

    
    # Step 1: Subtract "TK_animals_df" data from "recovery_cleaned_compile_data"
    cleaned_compile_data = recovery_cleaned_compile_data[
        ~recovery_cleaned_compile_data['USUBJID'].isin(tK_animals_df['USUBJID'])
    ]

    # Step 2: Filter "tx" table for specific TXPARMCD values
    cleaned_compile_data_filtered_tx = tx[tx['TXPARMCD'] == "TRTDOS"]

    # Step 3: Create a unified separator pattern
    clean_pattern = r";|\||-|/|:|,"

    # Step 4: Split and expand the TXVAL column, and add row_state
    cleaned_compile_data_filtered_tx.loc[:,'is_split'] = cleaned_compile_data_filtered_tx['TXVAL'].str.contains(clean_pattern)
    cleaned_compile_data_filtered_tx.loc[:,'TXVAL'] = cleaned_compile_data_filtered_tx['TXVAL'].str.split(clean_pattern)
    clean_tx_expanded = cleaned_compile_data_filtered_tx.explode('TXVAL')
    clean_tx_expanded['TXVAL'] = pd.to_numeric(clean_tx_expanded['TXVAL'], errors='coerce')
    clean_tx_expanded['row_state'] = np.where(clean_tx_expanded['is_split'], 'new_row', 'old_row')
    clean_tx_expanded = clean_tx_expanded.drop(columns='is_split')

    # Step 5: Adding dose_ranking
    dose_ranking = pd.DataFrame()
    
    dose_ranking_prob_study = pd.DataFrame()

    study_data = clean_tx_expanded

    if True:  # Always runs
     study_data = clean_tx_expanded
        # Check if all TXVAL values are NA for the STUDYID
     if study_data['TXVAL'].isna().all():
      dose_ranking_prob_study = pd.concat([dose_ranking_prob_study, study_data], ignore_index=True)
        # Check if all SETCD values are the same for the STUDYID
     elif study_data['SETCD'].nunique() == 1:
      dose_ranking_prob_study = pd.concat([dose_ranking_prob_study, study_data], ignore_index=True)
     else:
      # Process for lowest TXVAL
      lowest_txval = study_data['TXVAL'].min(skipna=True)
      lowest_data = study_data[study_data['TXVAL'] == lowest_txval].sort_values(by='SETCD')

      if len(lowest_data) == 1:
       dose_ranking = pd.concat([dose_ranking, lowest_data], ignore_index=True)
      else:
       # Select the first old_row if available, else the first new_row
       selected_lowest = lowest_data[lowest_data['row_state'] == 'old_row'].head(1)
       if len(selected_lowest) > 0:
        dose_ranking = pd.concat([dose_ranking, selected_lowest], ignore_index=True)
       else :
         selected_lowest = lowest_data[lowest_data['row_state'] == 'new_row'].head(1)
         dose_ranking = pd.concat([dose_ranking, selected_lowest], ignore_index=True) 
       
    # Process for highest TXVAL
     highest_txval = study_data['TXVAL'].max(skipna=True)
     highest_data = study_data[study_data['TXVAL'] == highest_txval].sort_values(by='SETCD')

     if len(highest_data) == 1:
      dose_ranking = pd.concat([dose_ranking, highest_data], ignore_index=True)
     elif len(highest_data) > 1:
      selected_highest = highest_data[highest_data['row_state'] == 'old_row'].head(1)
      #if len(selected_highest > 0):
      if not selected_highest.empty:
       dose_ranking = pd.concat([dose_ranking, selected_highest], ignore_index=True)
      else:
       selected_highest = highest_data[highest_data['row_state'] == 'new_row'].head(1)
       #if len(selected_highest> 0):
       if not selected_highest.empty:
        dose_ranking = pd.concat([dose_ranking, selected_highest], ignore_index=True)
      
        # if not selected_highest.empty:
        #             dose_ranking = pd.concat([dose_ranking, selected_highest], ignore_index=True)
        #         else:
        #             selected_highest = highest_data[highest_data['row_state'] == 'new_row'].head(1)
        #             if not selected_highest.empty:
        #                 dose_ranking = pd.concat([dose_ranking, selected_highest], ignore_index=True)

    # Step 6: ADD DOSE_RANKING column in "selected_rows" data frame
    
    # Assuming `dose_ranking` is your DataFrame
    # Group by 'STUDYID'
    grouped = dose_ranking.groupby('STUDYID')

    # Add 'MinTXVAL' and 'MaxTXVAL' columns using transform
    dose_ranking['MinTXVAL'] = grouped['TXVAL'].transform('min')
    dose_ranking['MaxTXVAL'] = grouped['TXVAL'].transform('max')

   # Define the DOSE_RANKING column based on conditions
    dose_ranking['DOSE_RANKING'] = 'Intermediate'  # Default value
    dose_ranking.loc[
    (dose_ranking['TXVAL'] == dose_ranking['MinTXVAL']) & 
    (dose_ranking['TXVAL'] == dose_ranking['MaxTXVAL']), 
    'DOSE_RANKING'
    ] = 'Both'
    
    dose_ranking.loc[
    dose_ranking['TXVAL'] == dose_ranking['MinTXVAL'], 
    'DOSE_RANKING'
    ] = 'vehicle'
    
    dose_ranking.loc[
    dose_ranking['TXVAL'] == dose_ranking['MaxTXVAL'], 
    'DOSE_RANKING'
    ] = 'HD'

   # Drop the 'MinTXVAL' and 'MaxTXVAL' columns
    dose_ranking = dose_ranking.drop(columns=['MinTXVAL', 'MaxTXVAL'])

   # Reset the index if needed (since `groupby` does not modify the DataFrame structure)
    dose_ranked_selected_rows = dose_ranking.reset_index(drop=True)

    
    
    # DOSE_RANKED_selected_rows = (
    #     dose_ranking
    #     .groupby('STUDYID')
    #     .apply(lambda df: df.assign(
    #         MinTXVAL=df['TXVAL'].min(),
    #         MaxTXVAL=df['TXVAL'].max(),
    #         DOSE_RANKING=np.where(
    #             (df['TXVAL'] == df['MinTXVAL']) & (df['TXVAL'] == df['MaxTXVAL']), "Both",
    #             np.where(df['TXVAL'] == df['MinTXVAL'], "vehicle",
    #                      np.where(df['TXVAL'] == df['MaxTXVAL'], "HD", "Intermediate"))
    #         )
    #     ))
    #     .reset_index(drop=True)
    #     .drop(columns=['MinTXVAL', 'MaxTXVAL'])
    # )

    # Step 7: Merging "DOSE_RANKED_selected_rows" and "cleaned_CompileData"
    dose_rank_comp_data = pd.merge(cleaned_compile_data, dose_ranked_selected_rows, on=['STUDYID', 'SETCD'])

    # Step 8: Rename and select specific columns
    master_compiledata1 = dose_rank_comp_data[['STUDYID', 'USUBJID', 'Species', 'SEX', 'DOSE_RANKING', 'SETCD']]

    # Step 9: Rename "DOSE_RANKING" column to "ARMCD"
    master_compiledata = master_compiledata1.rename(columns={'DOSE_RANKING': 'ARMCD'})

    return master_compiledata

###########################################################################
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Example usage

# Call the function
db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/DATABASES/fake_merged_liver_not_liver.db"
fake_T_xpt_F_compile_data = get_compile_data(studyid="28738",
                                             path_db = db_path,
                                             fake_study=True,
                                             use_xpt_file=False)

# # Call the function
# db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/2023-2024_projects/FAKE_DATABASES/single_fake_xpt_folder/FAKE28738"
# fake_T_xpt_T_compile_data = get_compile_data(studyid=None,
#                                              path_db = db_path,
#                                              fake_study =True,
#                                              use_xpt_file=True)
#
# # Call the function
# db_path = "C:\\Users\\MdAminulIsla.Prodhan\\OneDrive - FDA\\Documents\\TestDB.db"
# real_sqlite_compile_data = get_compile_data(studyid="876",
#                                             path_db = db_path,
#                                             fake_study=False,
#                                             use_xpt_file=False)
#
#
# # Call the function
# db_path = "C:/Users/MdAminulIsla.Prodhan/OneDrive - FDA/Documents/2023-2024_projects/FAKE_DATABASES/real_xpt_dir/IND051292_1017-3581"
# real_xpt_compile_data = get_compile_data(studyid=None,
#                                          path_db = db_path,
#                                          fake_study =False,
#                                          use_xpt_file=True)

