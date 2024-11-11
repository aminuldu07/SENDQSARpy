import sqlite3
import pandas as pd
import os
from pathlib import Path

# Function to fetch domain data from SQLite database
def fetch_domain_data(db_connection, domain_name, studyid):
    domain_name = domain_name.upper()
    query_statement = f"SELECT * FROM {domain_name} WHERE STUDYID = ?"
    query_result = pd.read_sql_query(query_statement, db_connection, params=(studyid,))
    return query_result

# get the parameter
db_path = "C:\\Users\\MdAminulIsla.Prodhan\\OneDrive - FDA\\Documents\\2023-2024_projects\\FAKE_DATABASES\\fake_merged_liver_not_liver.db"
studyid = "28738"  # Example list of selected studies

# Connect to SQLite database
db_connection = sqlite3.connect(db_path)

# Fetch data for 'dm' and 'ts' domains
dm = fetch_domain_data(db_connection, 'dm', studyid)
ts = fetch_domain_data(db_connection, 'ts', studyid)

# Close the database connection
db_connection.close()

# Convert 'dm' and 'ts' to pandas DataFrames
dm = pd.DataFrame(dm)
ts = pd.DataFrame(ts)
# Select specific columns from 'dm'
dm = dm[['STUDYID', 'USUBJID', 'SPECIES', 'SEX', 'ARMCD', 'ARM', 'SETCD']]

#species = ts.loc[ts['TSPARMCD'] == 'SPECIES', 'TSVAL'].values[0]
dm['Species'] = dm['SPECIES']

# Fetch species value from 'ts' table where 'TSPARMCD' equals 'SPECIES'
species = ts.loc[ts['TSPARMCD'] == 'SPECIES', 'TSVAL'].values[0]

dm['Species'] = species
dm.drop(columns=['SPECIES', 'ARMCD'], inplace=True)
dm = dm.rename(columns={'ARM': 'ARMCD'})
    # Update 'ARMCD' to 'vehicle' where it equals 'Control'
dm['ARMCD'] = dm['ARMCD'].replace('Control', 'vehicle')
# Filter 'dm' to include only rows where 'ARMCD' is 'vehicle' or 'HD'
dm = dm[dm['ARMCD'].isin(['vehicle', 'HD'])]

