# %% [markdown]
# <h1 style = "color : navy;"> QSAR Model Building_MF</h1>

# %% [markdown]
# # QSAR Model building for predicting durg toxicity from SEND data
# ### The overall workflows are as follow : 
#      - Get the Negative Control Animal Body wight (NC) data from the SEND database using sql query
#         - remove the NUll values from the BWSTRESN  column 
#         - NC has '0' dose level for the TRTDOSLEVEL' column 
#         
#     - Get the Animal Body wight (AB) data from the SEND database using sql query
#         - remove the '0' dose level from the 'TRTDOSLEVEL' column 
#         - remove the 'multiple dose level' listed in a single row from the 'TRTDOSLEVEL' column
#         - resolve the COMMA issues in 'TRTDOSLEVEL' column
#         - remove the NUll values from the BWSTRESN and 'TRTDOSLEVEL' column
#         - select the high does for each STUDYID 
#         
#     - zscore calculation from AB and NC data
#         
#     - Get the SMILES data from the GSRS data 
#         - merge two differnt GSRS file on Approval ID 
#         - create APPID column by pasting 'APP_type' and 'App_number' columns 
#     
#      - Merge zscore data and GSRS data 
#          - merge zscore and GSRS data on 'APPID' column 
#          - get unique "SMILES" with highest "zscore" value
#          - create the unique SMILES data frame
#     
#     - Molecular descriptor (MD) calculation 
#         - molecular descriptor calculation using mordred library
#         - molecular descriptor calculation using pybimed library
#         - selection of the appropriate MD for QSAR modeling
#     
#     - Molecular descriptor (MD) data manipulation for QSAR modeling
#         - merge the descriptor data frame with the unique "SMILES" with highest "zscore" data frame 
#         - assign the zscore value as toxic ( zscore < -2.00 )  and non_toxic ( zscore > -2.00 ) 
#         - Now the target value is categorical value and MD values are numerical
#      
#     - QSAR Model building by differnt machine learning technique
#         - random forest model
#         - neural network 
#         - svm 

# %% [markdown]
# ## Suppress any warnings for all the upcoming cells
# - use this function with the "ignore" argument to ignore all warnings

# %%
import warnings
warnings.filterwarnings('ignore')

# If there is a specific type of warning that you want to ignore, you can specify this type in the filterwarnings function
warnings.filterwarnings('ignore', category=UserWarning) # You would replace UserWarning with the specific warning class
                                                        # you wish to ignore.

# For the case of ignoring warnings from specific libraries, you can add the module parameter:
warnings.filterwarnings('ignore', module='numpy')  # Ignore warnings from numpy


# %% [markdown]
# <h1 style ="color : blue;">  SQL query code for getting AB and NC body weight data  </h1>  
#   <ul style ="color : red;">
#     <li> IND147206 (export)</li>
#     <ul>

# %% [markdown]
# #### SQL query code for getting AB and NC body weight data 
# 
# - INDAB07032023 (export) AB_MF
# 
# SELECT  ID.APPID, ID.STUDYID, DS.DSDECOD, DM.SEX,DM.SPECIES, BW.USUBJID, BW.BWTESTCD, BW.BWSTRESN, BW.BWSTRESU, TS.TSVAL AS TSSPECIES, TX.TXVAL AS TRTDOSLEVEL
# FROM (((((ID INNER JOIN DS ON (DS.STUDYID = ID.STUDYID AND DS.USUBJID = DM.USUBJID ) )
#          INNER JOIN TS ON TS.STUDYID = ID.STUDYID)
#         INNER JOIN TX ON (TX.STUDYID = ID.STUDYID AND TX.SETCD = DM.SETCD ) )
#        INNER JOIN DM ON DM.STUDYID = ID.STUDYID)
#       INNER JOIN BW ON (BW.STUDYID = ID.STUDYID AND BW.USUBJID= DM.USUBJID))
# WHERE TX.TXPARMCD == "TRTDOS" AND TS.TSPARMCD LIKE '%SPECIES%' AND DS.DSDECOD == "TERMINAL SACRIFICE" AND BW.BWTESTCD == "TERMBW";
# 
# - INDNC07032023 (export) NC_MF 
# 
# SELECT ID.APPID, ID.STUDYID, DS.DSDECOD, DM.SEX,DM.SPECIES, BW.USUBJID, BW.BWTESTCD, BW.BWSTRESN, BW.BWSTRESU, TS.TSVAL AS TSSPECIES, TX.TXVAL AS TRTDOSLEVEL, TX.TXPARMCD
# FROM (((((ID INNER JOIN DS ON (DS.STUDYID = ID.STUDYID AND DS.USUBJID = DM.USUBJID ) )
#          INNER JOIN TS ON TS.STUDYID = ID.STUDYID)
#         INNER JOIN TX ON (TX.STUDYID = ID.STUDYID AND TX.SETCD = DM.SETCD ) )
#        INNER JOIN DM ON DM.STUDYID = ID.STUDYID)
#       INNER JOIN BW ON (BW.STUDYID = ID.STUDYID AND BW.USUBJID= DM.USUBJID))
# WHERE TX.TXPARMCD == "TCNTRL" AND TS.TSPARMCD LIKE '%SPECIES%' AND DS.DSDECOD == "TERMINAL SACRIFICE" AND BW.BWTESTCD == "TERMBW";

# %% [markdown]
# <h1 style = " color : red ;" > Required library</h1>

# %%
# Required library 
import pandas as pd
import numpy as np

# %% [markdown]
# <h1 style ="color : blue;">  Cleaning NC file ("NC_MF.csv") </h1>  

# %%
# Read the CSV file
NC_MF = pd.read_csv("NC_MF.csv")

# print the data frame
print(NC_MF.shape)
NC_MF.head(2)

# %%
# Convert the 'BWSTRESN' column to numeric
NC_MF['BWSTRESN'] = pd.to_numeric(NC_MF['BWSTRESN'])

# Remove rows where the BWSTRESN column has the value "NULL"
#NC_MF_cleanf = NC_MF[NC_MF['BWSTRESN'] != "NULL"] ## can not remove empty 'BWSTRESN'

# Remove rows where the BWSTRESN column is empty
NC_MF_cleanf = NC_MF[NC_MF['BWSTRESN'].notnull()] 

print(NC_MF_cleanf.shape)
NC_MF_cleanf.head(1)

# %% [markdown]
# <h4 style ="color : navy;">  Write the dfAB_cleanf as csv file </h4> 
#    <ul style="color:red;">
#     <li>NC_MF_cleanf.to_csv("NC_MF_cleanf.csv", index = False)</li>
#     </ul>

# %%
# # Write dfNC_cleanf to a CSV file
# NC_MF_cleanf.to_csv('NC_MF_cleanf.csv', index=False)

# %% [markdown]
# <h2 style ="color : navy;"> Cleaning Animal Body Weight  File ("AB_MF.csv") </h2> 

# %%
# Reading and cleaning Animal Body weight  file ("AB_MF.csv")
df = pd.read_csv('AB_MF.csv')

# Print the data frame

print(df.shape)
df.head(1)

# %% [markdown]
# ## Manually cleaning the "AB_MF.csv" data file
# - "INDAB07032023_manually_cleaned.csv"data were manually cleaned by deleting some rows by following some set criteria
# - Deleted rows having TRTDOSLEVEL '0' ,  multiple values and text values 

# %% [markdown]
# ## Issues with  'AB_MF_Manually_cleaned.csv' data 
# 
# -  some "species" values are null"
# - 'some' BWSTRESN values are null
# -  some data in TRTDOSLEVEl column has "comma" 
# -  "some" TRTDOSLEVEL vaues are NULL as those vaues contains "comma" like "1,000"

# %% [markdown]
# ### Read the Mamually cleaned data frame 'AB_MF_Manually_cleaned.csv'
# - read the comma contained string value as numeric
# - replace the single comma and keep remaining in "TRTDOSLEVEL" column
# - thousands=',' make the comma as numeric
# - Read the "IND147206AB_cleaned.csv" in a way to convert COMMASlike "1,000" as 1000

# %% [markdown]
# In a pandas DataFrame, numbers formatted with commas, like "1,000", are often treated as strings because the comma is not recognized in numeric data types. Therefore, if "1,000" is currently a float64 in your DataFrame, it's likely that it's actually being stored as NaN (since pd.to_numeric() with errors='coerce' would convert unparseable values to NaN).
# 
# To confirm, you could view a subset of your data where 'TRTDOSLEVEL' is NaN:

# %%
# read the manully celaned csv file 
dfAB_MF = pd.read_csv('AB_MF_Manually_cleaned.csv', thousands=',') 

# print the data frame
print(dfAB_MF.shape)
dfAB_MF.head(1)

# %% [markdown]
# <h3 style ="color : navy;"> Check the data type of "TRTDOSLEVEL" column </h3> 

# %%
# Check the data type
str_count = dfAB_MF['TRTDOSLEVEL'].apply(lambda x: isinstance(x, str)).sum()

print(f"Number of string data types in 'TRTDOSLEVEL': {str_count}")




# %% [markdown]
# <h4 style ="color : navy;"> Check the data type of a 'specific row' in "TRTDOSLEVEL" column </h4> 

# %%
# Check the data type
row_index = dfAB_MF[dfAB_MF['USUBJID'] == 'H21075RD1-4005'].index[0]
trtdoslevel_dtype = type(dfAB_MF.loc[row_index, 'TRTDOSLEVEL'])

print(f"Data type of TRTDOSLEVEL: {trtdoslevel_dtype}")


row_index = dfAB_MF2[dfAB_MF2['USUBJID'] == 'H21075RD1-4005'].index[0]
trtdoslevel_dtype = dfAB_MF2.loc[row_index, 'TRTDOSLEVEL'].dtype

print(f"Data type of TRTDOSLEVEL: {trtdoslevel_dtype}")

# %% [markdown]
# <h4 style ="color : navy;"> Desired columns selections  </h4> 

# %%
#Create a new DataFrame dfAB_MF1 from dfAB_MF with only the selected columns
selected_cols = ["APPID", "STUDYID", "USUBJID", "BWSTRESN", "BWSTRESU", "SEX", "TSSPECIES", "TRTDOSLEVEL"]
dfAB_MF1 = dfAB_MF[selected_cols]

# print the data frame
print(dfAB_MF1.shape)
dfAB_MF1.head(1)

# %%
has_null_TRTDOSLEVEL = dfAB_MF1['TRTDOSLEVEL'].isnull()
num_null_TRTDOSLEVEL = has_null_TRTDOSLEVEL.sum()

print(f"Has null values: {has_null_TRTDOSLEVEL.any()}")  # This will print True if there are any null values, False otherwise
print(f"Number of null values: {num_null_TRTDOSLEVEL}")  # This will print the number of null values

# %% [markdown]
# <h4 style ="color : navy;"> Converting 'BWSTRESN' and 'TRTDOSLEVEL' columns to numeric  </h4> 

# %%
# Create dfAB by copying dfAB1
dfAB_MF2 = dfAB_MF1.copy()

# Convert 'BWSTRESN' and 'TRTDOSLEVEL' columns to numeric
dfAB_MF2['BWSTRESN'] = pd.to_numeric(dfAB_MF2['BWSTRESN'], errors='coerce')
dfAB_MF2['TRTDOSLEVEL'] = pd.to_numeric(dfAB_MF2['TRTDOSLEVEL'], errors='coerce')

# print the data frame
print(dfAB_MF2.shape)
dfAB_MF2.head(1)

# %% [markdown]
# <h4 style ="color : navy;"> Check the presence of Null Values in 'TRTDOSLEVEL' column  </h4> 

# %%
has_null_TRTDOSLEVEL = dfAB_MF2['TRTDOSLEVEL'].isnull()
num_null_TRTDOSLEVEL = has_null_TRTDOSLEVEL.sum()

print(f"Has null values: {has_null_TRTDOSLEVEL.any()}")  # This will print True if there are any null values, False otherwise
print(f"Number of null values: {num_null_TRTDOSLEVEL}")  # This will print the number of null values

# %% [markdown]
# <h4 style ="color : navy;"> Check the presence of 'string' data types in 'TRTDOSLEVEL' column  </h4> 

# %%
str_count = dfAB_MF2['TRTDOSLEVEL'].apply(lambda x: isinstance(x, str)).sum()

print(f"Number of string data types in 'TRTDOSLEVEL': {str_count}")

# %% [markdown]
# #### Check the presence of Null Values in 'BWSTRESN' column

# %%
has_null_BWSTRESN = dfAB_MF2['BWSTRESN'].isnull()
num_null_BWSTRESN = has_null_BWSTRESN.sum()

print(f"Has null values: {has_null_BWSTRESN.any()}")  # This will print True if there are any null values, False otherwise
print(f"Number of null values: {num_null_BWSTRESN}")  # This will print the number of null values

# %% [markdown]
# <h4 style ="color : navy;">  'Subset' function to remove rows where the BWSTRESN column has the "NULL" value </h4> 
#    <ul style="color:red;">
#     <li>remove 'null' values from the 'BWSTRESN' column if present</li>
#     </ul>

# %%
# # subset dfAB2 to remove Null values
# dfAB_cleanf = dfAB_MF2.dropna(subset=['BWSTRESN'])
# print(dfAB_cleanf.shape)

# %% [markdown]
#  <h4 style ="color : navy;">  Create the dfAB_MF_cleanf </h4> 

# %%
dfAB_MF_cleanf  = dfAB_MF2

# print the data frame
print(dfAB_MF_cleanf.shape)
dfAB_MF_cleanf.head(2)

# %% [markdown]
# #### Check if the 'BWSTRESN' column has any null values and count them

# %%
has_null_BWSTRESN = dfAB_MF_cleanf['BWSTRESN'].isnull()
num_null_BWSTRESN = has_null_BWSTRESN.sum()

print(f"Has null values: {has_null_BWSTRESN.any()}")  # This will print True if there are any null values, False otherwise
print(f"Number of null values: {num_null_BWSTRESN}")  # This will print the number of null values

# %% [markdown]
# #### Check if the 'TRTDOSLEVEL' column has any null values and count them

# %%
# Check if the 'TRTDOSLEVEL' column has any null values and count them

has_null_TRTDOSLEVEL = dfAB_MF_cleanf['TRTDOSLEVEL'].isnull()
num_null_TRTDOSLEVEL = has_null_TRTDOSLEVEL.sum()

print(f"Has null values: {has_null_TRTDOSLEVEL.any()}")  # This will print True if there are any null values, False otherwise
print(f"Number of null values: {num_null_TRTDOSLEVEL}")  # This will print the number of null values

# %% [markdown]
# <h2 style ="color : navy;">  Write the dfAB_MF_cleanf as csv file </h2> 
#    <ul style="color:red;">
#     <li>dfAB_MF_cleanf.to_csv("dfAB_MF_cleanf.csv", index = False)</li>
#     </ul>

# %%
# # Write the dfAB_MF2 as csv file
#dfAB_MF_cleanf.to_csv("dfAB_MF_cleanf.csv", index = False)

# %% [markdown]
#  <h2 style ="color : navy;"> 'High Dose' Selection Calculation </h2> 

# %% [markdown]
# #### Read 'dfAB_cleanf.csv' file 

# %%
# Read the CSV file
dfAB_MF_cleanf = pd.read_csv ("dfAB_MF_cleanf.csv")
print(dfAB_MF_cleanf.shape)
dfAB_MF_cleanf.head(2)

# %% [markdown]
# <h2 style ="color : navy;">  High dose selection </h2> 
# <ul style ="color : blue;"> 
#     <li> High dose can be selected two ways</li>
#     <ul style ="color : red;">
#         <li> Without prior segregation of Gender</li>
#         <li> With prior segregation of Gender</li>  
#     </ul>
#     </ul>

# %% [markdown]
# <h2 style = "color : maroon;"> High dose selection without 'Prior Gender Segregation'</h2>
# <ul> 
#     <li>This approch select max dose for each STUDYID disregarding the SEX</li>

# %%
# # Assign dfAB_MF_cleanf data frame to a new variable 

# df = dfAB_MF_cleanf

#  # Initialize an empty DataFrame to store the results
# highdose_dfAB_MF_sex_unspecified = pd.DataFrame()

#  # Loop over each unique STUDYID
# for unique_studyid in df['STUDYID'].unique():
  
#      # Subset the DataFrame to only include rows with the current STUDYID
#      subset_df = df[df['STUDYID'] == unique_studyid]
  
#      # Get the index of the row with the highest TRTDOSLEVEL value
#      max_index = subset_df['TRTDOSLEVEL'].idxmax()
  
#      # Extract the row with the highest TRTDOSLEVEL value
#      max_row = subset_df.loc[max_index]
  
#      # Append the max_row to the result_df DataFrame
#      highdose_dfAB_MF_sex_unspecified = highdose_dfAB_MF_sex_unspecified.append(max_row)

# # Print the result DataFrame
print(highdose_dfAB_MF_sex_unspecified.shape)
highdose_dfAB_MF_sex_unspecified.head(2)



# %% [markdown]
# <h2 style = "color : maroon;"> High dose selection by Prior Gender Segregation</h2>
# <ul> 
#     <li> This approach select max dose for each STUDYID and each 'SEX"</li>

# %%
# # Assign dfAB_MF_cleanf data frame to a new variable
# df = dfAB_MF_cleanf

# # Subset the DataFrame for male and female
# df_male = df[df['SEX'] == 'M']
# df_female = df[df['SEX'] == 'F']

# # Initialize empty DataFrames to store the results
# highdose_dfAB_MF_male = pd.DataFrame()
# highdose_dfAB_MF_female = pd.DataFrame()

# # Loop over each unique STUDYID in the male subset
# for unique_studyid_male in df_male['STUDYID'].unique():
#     # Subset the DataFrame to only include rows with the current STUDYID in the male subset
#     subset_df_male = df_male[df_male['STUDYID'] == unique_studyid_male]
#     # Get the index of the row with the highest TRTDOSLEVEL value in the male subset
#     max_index_male = subset_df_male['TRTDOSLEVEL'].idxmax()
#     # Extract the row with the highest TRTDOSLEVEL value in the male subset
#     max_row_male = subset_df_male.loc[max_index_male]
#     # Append the max_row_male to the male result DataFrame
#     highdose_dfAB_MF_male = highdose_dfAB_MF_male.append(max_row_male)

# # Loop over each unique STUDYID in the female subset
# for unique_studyid_female in df_female['STUDYID'].unique():
#     # Subset the DataFrame to only include rows with the current STUDYID in the female subset
#     subset_df_female = df_female[df_female['STUDYID'] == unique_studyid_female]
#     # Get the index of the row with the highest TRTDOSLEVEL value in the female subset
#     max_index_female = subset_df_female['TRTDOSLEVEL'].idxmax()
#     # Extract the row with the highest TRTDOSLEVEL value in the female subset
#     max_row_female = subset_df_female.loc[max_index_female]
#     # Append the max_row_female to the female result DataFrame
#     highdose_dfAB_MF_female = highdose_dfAB_MF_female.append(max_row_female)

# # Concatenate the male and female DataFrames to create the final DataFrame
# highdose_dfAB_MF_sex_specified = pd.concat([highdose_dfAB_MF_male, highdose_dfAB_MF_female])

# # Print the shape of the final DataFrame
print("Final DataFrame shape:", highdose_dfAB_MF_sex_specified.shape)
highdose_dfAB_MF_sex_specified.head(1)


# The above code can also be written as below........................................................................
#.....................................................................................................................
# # Assign dfAB_MF_cleanf data frame to a new variable
# df = dfAB_MF_cleanf

# # Initialize empty DataFrames to store the results
# highdose_dfAB_MF_male = pd.DataFrame()
# highdose_dfAB_MF_female = pd.DataFrame()

# # Loop over each unique STUDYID
# for unique_studyid in df['STUDYID'].unique():
  
#     # Subset the DataFrame to only include rows with the current STUDYID
#     subset_df = df[df['STUDYID'] == unique_studyid]
  
#     # Separate the subset DataFrame by SEX column
#     subset_df_male = subset_df[subset_df['SEX'] == 'M']
#     subset_df_female = subset_df[subset_df['SEX'] == 'F']
  
#     # Check if the subset DataFrames are not empty
#     if not subset_df_male.empty:
#         # Get the index of the row with the highest TRTDOSLEVEL value for M
#         max_index_male = subset_df_male['TRTDOSLEVEL'].idxmax()
#         # Extract the row with the highest TRTDOSLEVEL value for M
#         max_row_male = subset_df_male.loc[max_index_male]
#         # Append the max_row_male to the male result DataFrame
#         highdose_dfAB_MF_male = highdose_dfAB_MF_male.append(max_row_male)
  
#     if not subset_df_female.empty:
#         # Get the index of the row with the highest TRTDOSLEVEL value for F
#         max_index_female = subset_df_female['TRTDOSLEVEL'].idxmax()
#         # Extract the row with the highest TRTDOSLEVEL value for F
#         max_row_female = subset_df_female.loc[max_index_female]
#         # Append the max_row_female to the female result DataFrame
#         highdose_dfAB_MF_female = highdose_dfAB_MF_female.append(max_row_female)

# # Concatenate the male and female DataFrames to create the final DataFrame
# final_df = pd.concat([highdose_dfAB_MF_male, highdose_dfAB_MF_female])

# # Print the shape of the final DataFrame
# print("Final DataFrame shape:", final_df.shape)


# %% [markdown]
# <h4 style = "color : maroon;"> High dose selection from" High dose selection by Prior Gender Segregation"</h4>
# <ul style = "color : maroon;"> 
#     <li> This approach select max dose for each STUDYID without considering the SEX</li>
# <ul> 
#     

# %%
# # Assign dfAB_MF_cleanf data frame to a new variable 

# df = highdose_dfAB_MF_sex_specified

#  # Initialize an empty DataFrame to store the results
# highdose_dfAB_MF_fpgs = pd.DataFrame()

#  # Loop over each unique STUDYID
# for unique_studyid in df['STUDYID'].unique():
  
#      # Subset the DataFrame to only include rows with the current STUDYID
#      subset_df = df[df['STUDYID'] == unique_studyid]
  
#      # Get the index of the row with the highest TRTDOSLEVEL value
#      max_index = subset_df['TRTDOSLEVEL'].idxmax()
  
#      # Extract the row with the highest TRTDOSLEVEL value
#      max_row = subset_df.loc[max_index]
  
#      # Append the max_row to the result_df DataFrame
#      highdose_dfAB_MF_fpgs = highdose_dfAB_MF_fpgs.append(max_row)

# # Print the result DataFrame
# print(highdose_dfAB_MF_fpgs.shape)
# highdose_dfAB_MF_fpgs


# %%


# %% [markdown]
# <h3 style ="color : navy;">  Write the highdose_dfAB_MF_sex_specified as csv file </h3> 
#    <ul style="color:red;">
#     <li>highdose_dfAB_MF_sex_specified.to_csv("highdose_dfAB_MF_sex_specified.csv", index = False)</li>
#     </ul>

# %%
# # Write the DataFrame to a CSV file
# highdose_dfAB_MF_sex_specified.to_csv('highdose_dfAB_MF_sex_specified.csv', index=False)

# %% [markdown]
# <h3 style ="color : navy;"> "ZSCORES" calculations for each unique STUDYID </h3> 
#   <ul style="color:red;">
#     <li> zscores calculation requires</li>
#     <ul style="color:navy;">
#     <li>highdose_dfAB_MF_sex_specified from AB_MF data</li>
#      <li>Cleaned Negative control "NC_MF_cleanf" data</li>   
#     </ul>
#    </ul>

# %% [markdown]
# ### Read the cleaned and final  NC data ("dfNC__no_NaN.csv")

# %%
# Read the cleaned and final NC data 
df1 =  pd.read_csv("NC_MF_cleanf.csv")
print(df1.shape)

# %% [markdown]
# ### Read the final AB High Dose Selection data

# %%
# read final AB High Dose Selection data
df2 = pd.read_csv ('highdose_dfAB_MF_sex_specified.csv')
print(df2.shape)

# %% [markdown]
# ### Grouping NC DATA for 'MEAN' and 'SD' calculation on 'BWSTRESN'
# - If any STUDYID has only one rows , it's SD was 'NaN" 
# - rows containning NaN values were removed

# %%
# # Group by 'STUDYID' and calculate the mean and standard deviation of 'BWSTRESN'
# df1_grouped = df1.groupby('STUDYID').agg({'BWSTRESN': ['count', 'mean', 'std']})

# # Reset the column names
# df1_grouped.columns = ['count', 'mean_BWSTRESN', 'sd_BWSTRESN']

# # Filter rows where count is more than 1
# df1_grouped = df1_grouped[df1_grouped['count'] > 1]

# # Drop the 'count' column
# df1_grouped = df1_grouped.drop(columns=['count'])

# # Remove rows with NaN values
# df1_grouped = df1_grouped.dropna()

# print(df1_grouped.shape)

# %%


# %%
# Group by 'STUDYID' and 'SEX' and calculate the mean and standard deviation of 'BWSTRESN'
df1_grouped = df1.groupby(['STUDYID', 'SEX']).agg({'BWSTRESN': ['count', 'mean', 'std']})

# Reset the column names
df1_grouped.columns = ['count', 'mean_BWSTRESN', 'sd_BWSTRESN']

# Filter rows where count is more than 1
df1_grouped = df1_grouped[df1_grouped['count'] > 1]

# Drop the 'count' column
df1_grouped = df1_grouped.drop(columns=['count'])

# Remove rows with NaN values
df1_grouped = df1_grouped.dropna()

print(df1_grouped.shape)
df1_grouped.head(1)

# %% [markdown]
# ### Joining grouped_NC (df1_grouped) data with  highdose_dfAB data ( Join AB and NC data)
# - joined on STUDYID

# %%
# Join df1_grouped and df2 on 'STUDYID' and 'SEX'
joined_df = pd.merge(df1_grouped, df2, on=['STUDYID', 'SEX'], how='inner')

print(joined_df.shape)
joined_df.head(3)

# %%
# # Join df1_grouped and df2 on 'STUDYID'
# joined_df = pd.merge(df1_grouped, df2, on='STUDYID', how='inner')

# print(joined_df.shape)

# %% [markdown]
# ### Zscore calcualtion

# %%
# Create a new DataFrame
zscore_df = joined_df.copy()

# Compute zscore
joined_df['zscore'] = (joined_df['BWSTRESN'] - joined_df['mean_BWSTRESN']) / joined_df['sd_BWSTRESN']

# Filter out rows where 'zscore' is NaN, -Inf, or Inf
joined_df = joined_df[np.isfinite(joined_df['zscore'])]

# Reorganize the columns of the joined_df
zscore_df = joined_df[['APPID', 'SEX', 'STUDYID', 'USUBJID', 'zscore', 'mean_BWSTRESN', 'sd_BWSTRESN', 'BWSTRESN', 'BWSTRESU', 'TSSPECIES', 'TRTDOSLEVEL']]

print(zscore_df.shape)
zscore_df.head(2)


# %% [markdown]
# ### Write zscore_df as a CSV file

# %%
# # Write to CSV file
# zscore_df.to_csv("zscore.csv", index=False)

# %% [markdown]
# ### Check any null value in 'zscore' column

# %%
# Check if the 'zscore' column has any null values and count them

has_null_zscore = zscore_df['zscore'].isnull()
num_null_zscore = has_null_zscore.sum()

print(f"Has null values: {has_null_zscore.any()}")  # This will print True if there are any null values, False otherwise
print(f"Number of null values: {num_null_zscore}")  # This will print the number of null values

# %% [markdown]
# <h1 style ="color : maroon;">  Merging "GSRS data"  with 'zscore' data on 'APPID' </h1> 
# <ul style="color:blue;">
#     <li>'GSRS' and 'zscore' does not have common column </li>
#     </ul>

# %% [markdown]
# <h1 style ="color : navy;">  Issues with  "GSRS data"   </h1> 
# <ul style="color:blue;">
#     <li>GSRS has multiple data files</li>
#     <li>GSRS data doesn't have APPID column while 'zscore' does</li>
#     <li>APPID column can be created from GSRS data</li>
#     </ul>

# %% [markdown]
# <h3 style="color: black;">GSRS data processing for generating 'APPID' column</h3>
# <ul style="color: navy;">
#     <li>Below mentioned two GSRS data files required for 'APPID' column generation:</li>
#     <ul>
#         <li>BrowseApplications-13-04-2023_7-08-00.csv</li>
#         <li>export-13-04-2023_7-08-24.csv</li>
#     </ul>
# </ul>
# 

# %% [markdown]
# <h3 style ="color : blue;"> <><><> Processing <><><> "BrowseApplications-13-04-2023_7-08-00.csv" <><><><><>> </h3>  

# %%
# read the data frame
data1_APPID = pd.read_csv("BrowseApplications-13-04-2023_7-08-00.csv")
print(data1_APPID.shape)

# %% [markdown]
# #### Select the required columns

# %%
#Create a new DataFrame data2_APPID from data1_APPID with only the selected columns

selected_cols = ["APPLICANT_INGREDIENT_NAME", "APPROVAL_ID", "APP_TYPE", "APP_NUMBER"]
data2_APPID = data1_APPID[selected_cols]
print(data2_APPID.shape)

# %% [markdown]
# ####  'APPID' column generation by pasting 'APP_TYPE'&	'APP_NUMBER' columns

# %%
# 'APPID' column generation by pasting 'APP_TYPE'&	'APP_NUMBER' columns

data2_APPID['APPID'] = data2_APPID['APP_TYPE'].astype(str) + data2_APPID['APP_NUMBER'].astype(str)
print(data2_APPID.shape)
data2_APPID.head(2)

# %% [markdown]
# #### Select the required columns

# %%
# column selection from data2_APPID
data_APPID = data2_APPID[["APPID", "APPROVAL_ID"]]
print (data_APPID.shape)

# %% [markdown]
# <h2 style ="color : navy;">  Write the data_APPID as csv file </h2> 
#    <ul style="color:red;">
#     <li>data_APPID.to_csv("data_APPID.csv", index = False)</li>
#     </ul>
# 

# %%
# write the data_APPID as csv file

# data_APPID.to_csv("data_APPID.csv", index = False)

# %% [markdown]
# <h2 style ="color : magenta;">  <><><> Processing <><><> "export-13-04-2023_7-08-24.csv" <><><><><><> </h2> 

# %%
# read the data frame
data2_smiles2 = pd.read_csv("export-13-04-2023_7-08-24_cleaned.csv")

# print the data frame
print(data2_smiles2.shape)
data2_smiles2.head(1)

# %% [markdown]
# #### Select the required columns

# %%
# column selection from data2_APPID
data2_smiles = data2_smiles2[["APPROVAL_ID", "SMILES", "INCHIKEY", "PUBCHEM"]]

# print the data frame
print (data2_smiles.shape)
data2_smiles.head(1)

# %% [markdown]
# <h2 style ="color : magenta;">  Merge "BrowseApplications_" & "export_" files on Approval_ID </h2> 

# %%
# Merge "BrowseApplications_" & "export_" files on Approval_ID

merged_on_Approval_ID = pd.merge(data_APPID, data2_smiles, on = "APPROVAL_ID", how="inner")

# print data frame
print(merged_on_Approval_ID.shape)
merged_on_Approval_ID.head(1)

# %%
#Write the 
#merged_on_Approval_ID.to_csv("merged_on_Approval_ID.csv", index = False)

# %% [markdown]
# <h2 style ="color : red;"> ~Inner join 'zscore' and ''GSRS_merged' data frame on 'APPID'~ </h2> 
# 
# <ul style="color:brown;">
#     <li>merging 'zscore' data with the 'gsrsmerged' data </li>
#     </ul>
# 
# 

# %% [markdown]
# <h3 style ="color : navy;"> Read "zscore.csv" for merging </h3> 

# %%
# read the data frame
fzscores1 = pd.read_csv("zscore.csv")

# print the data frame 
print(fzscores1.shape)
fzscores1.head(1)

# %% [markdown]
# <h3 style ="color : navy;"> Select the required columns </h3> 

# %%
# column selection from zscore.csv
fzscores = fzscores1[["APPID","SEX", "zscore","TSSPECIES", "STUDYID"]]

# Print
print(fzscores.shape)
fzscores.head(1)

# %% [markdown]
# <h3 style ="color : navy;"> Read the 'gsrsmerged' data frame for merging </h3> 

# %%
## merging data based on a column ( APPID column)
gsrsmerged =  pd.read_csv("merged_on_Approval_ID_cleaned.csv")

# print
print(gsrsmerged.shape)
gsrsmerged.head(1)

# %% [markdown]
# <h2 style ="color :  maroon;"> Merging on = "APPID" column</h2> 

# %%
# merge two data frames using the merge() function 
finalmerged1 = pd.merge(gsrsmerged, fzscores, on="APPID")

# print
print(finalmerged1.shape)
finalmerged1.head(1)

# %% [markdown]
# <h2 style ="color :  brown;"> Re-organize the columns </h2> 

# %%
# Reorganize the column of the finalmerged1
finalmerged2 =  finalmerged1 [["STUDYID","SMILES","zscore","TSSPECIES", "SEX"]]

# Print
print(finalmerged2.shape)
finalmerged2.head(1)

# %% [markdown]
# [<h3 style ="color :  brown;"> Filtering rows having no 'smile string' in "SMILES" column </h3> 

# %% [markdown]
# ##
# #finalmerged1.to_csv("finalmerged1.csv", index = False)

# %%
## Filter to remove the rows not having SMILES 
finalmerged = finalmerged2.dropna(subset=['SMILES'])

# print
print(finalmerged.shape)
finalmerged.head(1)

# %% [markdown]
# <h2 style ="color : navy;">  Write the finalmerged as csv file </h2> 
#    <ul style="color:red;">
#     <li>finalmerged.to_csv("merged_Smiles_zscores3054.csv", index = False)</li>
#     </ul>
# 

# %%
# write the finalmerged as csv file 
#finalmerged.to_csv("merged_Smiles_zscores3054.csv", index = False)

# %% [markdown]
# <h2 style ="color : navy;">  Incorporating calculated descriptor in "merged_Smiles_zscores3054.csv" </h2> 
#    <ul style="color:red;">
#     <li>preciously calculated descriptors are merged here</li>
#     </ul>

# %% [markdown]
# <h3 style ="color :  red;"> Change the column name "Unnamed: 0" to "SMILES1"  </h3> 
#  <ul style="color:red;">
#     <li>during csv file creation, if index = False, </li>
#     <li>created data frame's first column will be "Unnamed: 0" </li>
#     </ul>

# %% [markdown]
# <h2 style ="color : navy;">  Merging data frame on  "SMILES" column </h2> 
#    <ul style="color:red;">
#     <li>.................................................</li>
#     </ul>

# %% [markdown]
# <h2 style ="color : navy;">  Create data frame having "unique SMILES" column from merged_Smiles_zscores3054.csv </h2> 
#    <ul style="color:red;">
#     <li>UNIQUE "SMILES"</li>
#     </ul>

# %%
# only UNIQUE Smiles file cration
merged_Smiles_zscores3054 = pd.read_csv("merged_Smiles_zscores3054.csv")

unique_merged_Smiles_zscores3054 = merged_Smiles_zscores3054["SMILES"].unique()
unique_merged_Smiles_zscores3054_df = pd.DataFrame(unique_merged_Smiles_zscores3054, columns=['SMILES'])

# print
print(unique_merged_Smiles_zscores3054.shape)
unique_merged_Smiles_zscores3054_df.head(1)


# %% [markdown]
# <h2 style ="color : navy;">  Write the unique_merged_Smiles_zscores3054_df as csv file </h2> 
#    <ul style="color:red;">
#     <li>unique_merged_Smiles_zscores3054_df.to_csv("unique_merged_Smiles_zscores3054_df.csv", index = False)</li>
#     </ul>

# %%
# write the smiles_1541p as csv file 
#unique_merged_Smiles_zscores3054_df.to_csv("unique_merged_Smiles_zscores3054_df_582.csv", index = False)

# %% [markdown]
# <h2 style ="color : navy;">  Calculate Molecular Descriptors using "unique_merged_Smiles_zscores3054_df" file </h2> 
#    <ul style="color: brown;">
#     <li>molecular descriptor were calculated by employing docker image</li>
#     <li>molecular descriptor were calculated by mordred package </li>
#     <li>molecular descriptor were calculated by pybiomed package </li>
#     </ul>

# %% [markdown]
# <h2 style ="color : black;">  Calculated Molecular Descriptors file descriptons </h2> 
#    <ul style="color:red;">
#     <li>from mordred- "unique_merged_Smiles_zscores3054_df_582_desc_mordred.csv"</li>
#     <li>from pybiomed- "........................."</li>
#     </ul>

# %% [markdown]
# <h3 style ="color : purple;">  Concatenate "merged_Smiles_zscores3054.csv" and   "unique_merged_Smiles_zscores3054_df_582_desc_mordred.csv" </h3> 
# <ul style="color:red;">
#     <li>concatenate to get the file having..... </li>
#         <ul style="color:red;">
#         <li>SMILES</li>
#         <li>zscore</li>
#         <li>species</li>
#         <li>STUDYID</li>
#         <li> Molecular descriptors</li>
#          </ul>
#     </ul>

# %% [markdown]
# <h3 style ="color :  brown;"> Read the "merged_Smiles_zscores3054.csv" data frame </h3> 

# %%
# read the data frame 
merged_Smiles_zscores3054 = pd.read_csv("merged_Smiles_zscores3054.csv")

# print the data frame
print(merged_Smiles_zscores3054.shape)
merged_Smiles_zscores3054.head(1)

# %% [markdown]
# <h3 style ="color :  brown;"> Read the "unique_merged_Smiles_zscores3054_df_582_desc_mordred.csv" data frame </h3> 

# %%
# read the data frame
unique_merged_Smiles_zscores3054_df_582_desc_mordred = pd.read_csv  ("unique_merged_Smiles_zscores3054_df_582_desc_mordred.csv")

# print
print(unique_merged_Smiles_zscores3054_df_582_desc_mordred.shape)
unique_merged_Smiles_zscores3054_df_582_desc_mordred.head(1)

# %% [markdown]
# <h3 style ="color :  red;"> Change the column name "Unnamed: 0" to "SMILES"  </h3> 
#  <ul style="color:red;">
#     <li>during csv file creation, if index = False, </li>
#     <li>created data frame's first column will be "Unnamed: 0" </li>
#     </ul>

# %%
# change the column name "Unnamed: 0" to "SMILES1" 
unique_merged_Smiles_zscores3054_df_582_desc_mordred.rename(columns={'Unnamed: 0': 'SMILES'}, inplace=True)

# print the data frame 
print(unique_merged_Smiles_zscores3054_df_582_desc_mordred.shape)
unique_merged_Smiles_zscores3054_df_582_desc_mordred.head(1)

# %% [markdown]
# <h3 style ="color :  brown;"> Concatenate two dataframes </h3> 

# %%
# concatenate two dataframes "merged_Smiles_zscores1541p" and "smiles_1541p_desc_mordred"
concatenated_df = pd.merge(merged_Smiles_zscores3054, unique_merged_Smiles_zscores3054_df_582_desc_mordred, on= 'SMILES')

# print
print(concatenated_df.shape)
concatenated_df.head(10)

# %% [markdown]
# <h3 style ="color :  red;"> Reshuffle "concatenated_df "  </h3> 
#  <ul style="color:red;">
#     <li>reshuffling makes downword data processing easy </li>
#     </ul>

# %%
# Reshuffling the 'concatenated_df'
reshuffled_df = concatenated_df[['STUDYID','SMILES','zscore', 'TSSPECIES', 'SEX'] + list(concatenated_df.columns[5:])]

# print the reshuffled data frmae
print(reshuffled_df.shape)
reshuffled_df.head(1)

# %% [markdown]
# <h3 style ="color : navy;">  Write the reshuffled_df as csv file </h3> 
#    <ul style="color:red;">
#     <li>reshuffled_df.to_csv("final_merged_Smiles_zscores3054_desc_mordred.csv", index = False)</li>
#     </ul>

# %%
# # write the "for_species_selection_smiles1541_desc_mordred.csv" file 
# reshuffled_df.to_csv("final_merged_Smiles_zscores3054_desc_mordred.csv.csv", index = False)

# %% [markdown]
#  <h1 style="color:red;">IMPORTANT POITNS TO BE CONSIDERED</h1>
#    <ul style="color:blue;">
#     <li>select SPECIES before selecting the max zscore value</li>
#      <li>getting the unique smiles followed by max zscore selection gives smallernumber of rows</li>
#     </ul>

# %% [markdown]
# <h2 style="color:brown;">         .......... SELECT THE SPECIES  ......................</h2>
# <ul style="color:blue;">
#     <li>"for_species_selection_smiles1541_desc_mordred.csv" has all the species</li>
#      <li>select the species based on the experimental design</li>
#     </ul>

# %% [markdown]
# <h2 style="color:navy;">Select the SPECIES "RAT"</h2>
# <ul style="color:blue;">
#     <li>Column 'TSSPECIES' = "RAT"</li>
#     </ul>

# %%
# # select the rat species only

# df_rat = df[df['TSSPECIES'] == 'RAT'].copy()

# # print the data frame 
# print(df_rat.shape)
# df_rat.head(2)

# %% [markdown]
# <h2 style="color:navy;">Get unique "SMILES" with highest "absolute" "zscore" value</h2>
# <ul style="color:red;"> The reasons for selecting 'absolute highest zscore' <ul>
#     <ul style="color:black;"> 
#     <li>groupby "SMILES" and then select the highst absolute zscore</li>
#      <li>only zscore selection can select "0" zscore</li>
#     <li>only zscore selection will select preferentially the "positive" zscore</li>
#       <li>only zscore selection will create unbalanced "zscore" distribution</li>
#     </ul>

# %% [markdown]
# <h2 style="color:purple;">Select the rows having highest 'absolute zscore' value</h2>

# %%
# Group by 'SMILES', 'TSSPECIES', and 'SEX' and keep the one with the absolute "max 'zscore'"
SMILES_SPECIES_SEX_abs_max_zscore = reshuffled_df.groupby(['SMILES', 'TSSPECIES', 'SEX']).apply(lambda x: x.loc[abs(x.zscore).idxmax()]).reset_index(drop=True)

# Print the shape of the data frame
print(SMILES_SPECIES_SEX_abs_max_zscore.shape)
SMILES_SPECIES_SEX_abs_max_zscore.head(1)


# %% [markdown]
# <h3 style="color:brown;">Select the desired columns by droping one columns</h3>

# %%
# select the desired columns by droping two columns
SMILES_SPECIES_SEX_abs_max_zscore = SMILES_SPECIES_SEX_abs_max_zscore.drop(['STUDYID'], axis=1)

# print the data frame
print(SMILES_SPECIES_SEX_abs_max_zscore.shape)
SMILES_SPECIES_SEX_abs_max_zscore.head(1)

# %% [markdown]
# <h3 style ="color : navy;">  Write the 'SMILES_SPECIES_SEX_abs_max_zscore' as csv file </h3> 
#    <ul style="color:red;">
#     <li>SMILES_SPECIES_SEX_abs_max_zscore.to_csv("SMILES_SPECIES_SEX_abs_max_zscore.csv", index = False)</li>
#     </ul>

# %%
# # # Write to CSV file
# SMILES_SPECIES_SEX_abs_max_zscore.to_csv("SMILES_SPECIES_SEX_abs_max_zscore.csv", index=False)

# %% [markdown]
# <h1 style="color:red;">Data Manipulation for machine learning model building</h1>

# %% [markdown]
# <h3 style="color:navy;">Read the "SMILES_SPECIES_SEX_abs_max_zscore" data frame</h3>


