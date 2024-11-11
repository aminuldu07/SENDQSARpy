import sqlite3
import pandas as pd
import os
from pathlib import Path

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
        dm = pd.read_sas(os.path.join(path, 'dm.xpt'), format='xport')
        ts = pd.read_sas(os.path.join(path, 'ts.xpt'), format='xport')

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

        # Further data manipulation can be added here...

    elif not fake_study and use_xpt_file:
        # Read XPT files using pandas
        bw = pd.read_sas(os.path.join(path, 'bw.xpt'), format='xport')
        dm = pd.read_sas(os.path.join(path, 'dm.xpt'), format='xport')
        ds = pd.read_sas(os.path.join(path, 'ds.xpt'), format='xport')
        ts = pd.read_sas(os.path.join(path, 'ts.xpt'), format='xport')
        tx = pd.read_sas(os.path.join(path, 'tx.xpt'), format='xport')
        pp = pd.read_sas(os.path.join(path, 'pp.xpt'), format='xport')
        pooldef = pd.read_sas(os.path.join(path, 'pooldef.xpt'), format='xport')

        # Further data manipulation can be added here...

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

    # Further filtering and cleaning can be done here...

    return compile_data



# # Later in the script, where you want to call the function:
# db_path = "C:\\Users\\MdAminulIsla.Prodhan\\OneDrive - FDA\\Documents\\2023-2024_projects\\FAKE_DATABASES\\single_fake_xpt_folder\\FAKE28738"
# #selected_studies = "28738"  # Example list of selected studies

# # Call the function
# compile_data = get_compile_data(studyid=None, path_db = db_path, fake_study =True, use_xpt_file=True)
# #(db_path, selected_studies)


# Later in the script, where you want to call the function:
db_path = "C:\\Users\\MdAminulIsla.Prodhan\\OneDrive - FDA\\Documents\\TestDB.db"
#selected_studies = "28738"  # Example list of selected studies

# Call the function
compile_data = get_compile_data(studyid="5003635", path_db = db_path, fake_study=False, use_xpt_file=False)
#(db_path, selected_studies)
