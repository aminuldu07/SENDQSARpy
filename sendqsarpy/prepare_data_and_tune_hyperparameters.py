# -*- coding: utf-8 -*-
"""
Created on Tue Dec 24 07:07:28 2024

@author: MdAminulIsla.Prodhan
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import accuracy_score

def prepare_data_and_tune_hyperparameters(scores_df,
                                          studyid_metadata,
                                          Impute=False,
                                          Round=False,
                                          reps=0,
                                          holdback=0.75,
                                          Undersample=False,
                                          hyperparameter_tuning=False,
                                          error_correction_method=None): # # Choose: "Flip" or "Prune" or "None"
    # Prepare input data
    scores_df = scores_df.copy()
    studyid_metadata = studyid_metadata.copy()
    studyid_metadata['STUDYID'] = studyid_metadata['STUDYID'].astype(str)
    merged_data = pd.merge(studyid_metadata, scores_df, on='STUDYID', how='left')

    # Prepare rfData
    rfData = merged_data.drop(columns=['STUDYID'])
    rfData['Target_Organ'] = rfData['Target_Organ'].replace({'Liver': 1, 'not_Liver': 0}).astype('category')

    # Impute missing values
    if Impute:
        # Impute missing values in all columns except the first (Target_Organ
        imputer = SimpleImputer(strategy='mean')
        rfData.iloc[:, 1:] = imputer.fit_transform(rfData.iloc[:, 1:])
        if Round:
            # Select columns with "avg_" or "liver" in their names
            zscore_cols = rfData.filter(regex='avg_|liver', axis=1).columns
            rfData[zscore_cols] = rfData[zscore_cols].apply(np.floor) # Floor values
            rfData[zscore_cols] = rfData[zscore_cols].clip(upper=5) # Cap at 5

            # Select columns starting with uppercase letters, excluding the first column
            histo_cols = rfData.columns[1:][rfData.columns[1:].str[0].str.isupper()]
            rfData[histo_cols] = rfData[histo_cols].apply(np.ceil) # Ceiling values

    count = 0
    for rep in range(reps):
        print(f"{(rep + 1) / reps * 100:.2f}% Complete...")

        # Train-test split
        if holdback == 1:
            test_idx = np.random.choice(rfData.index, 1, replace=False)
            train = rfData.drop(index=test_idx)
            test = rfData.loc[test_idx]
        else:
            #train, test = train_test_split(rfData, test_size=holdback, stratify=rfData['Target_Organ'])
            ind = np.random.choice(
                [1, 2], size=len(rfData), replace=True, p=[1 - holdback, holdback]
            )
            # Split the data into train and test sets based on the indices
            train = rfData[ind == 1]
            test = rfData[ind == 2]

            # Extract the indices of the test set
            testIndex = np.where(ind == 2)[0]

        # Undersampling
        if Undersample:
            pos = train[train['Target_Organ'] == 1]
            neg = train[train['Target_Organ'] == 0]
            neg_sample = neg.sample(n=len(pos), replace=(len(neg) < len(pos)))
            train = pd.concat([pos, neg_sample])

        # Hyperparameter tuning
        if hyperparameter_tuning:
            param_grid = {'n_estimators': [100, 200, 500], 'max_features': ['sqrt', 'log2']}
            rf = RandomForestClassifier(random_state=42)
            grid_search = GridSearchCV(rf, param_grid, cv=3, scoring='accuracy')
            grid_search.fit(train.iloc[:, 1:], train['Target_Organ'])
            best_m = grid_search.best_params_['max_features']
        else:
            best_m = 4

        # Train RF model
        rf = RandomForestClassifier(max_features=best_m, n_estimators=500, random_state=42)
        rf.fit(train.iloc[:, 1:], train['Target_Organ'])

        # Error correction
        if error_correction_method in {'Flip', 'Prune'}:
            test_probs = rf.predict_proba(test.iloc[:, 1:])[:, 1]
            threshold = 0.5
            flip_idx = test.index[
                (test['Target_Organ'] == 1) & (test_probs < threshold) |
                (test['Target_Organ'] == 0) & (test_probs > 1 - threshold)
            ]
            if error_correction_method == 'Flip':
                rfData.loc[flip_idx, 'Target_Organ'] = 1 - rfData.loc[flip_idx, 'Target_Organ']
            elif error_correction_method == 'Prune':
                rfData = rfData.drop(index=flip_idx)

            count += 1

    return {'rfData': rfData, 'best_m': best_m}


# #----------------------------------------------------------------
# import pandas as pd
# import numpy as np
# from sklearn.ensemble import RandomForestRegressor
# from sklearn.impute import SimpleImputer
#
# # Assuming `rfData` is your dataset and 'Target_Organ' is the target variable
# if Impute:
#     # Separate features and target variable
#     X = rfData.drop(columns=['Target_Organ'])
#     y = rfData['Target_Organ']
#
#     # Handle missing values in the features
#     X_filled = SimpleImputer(strategy="mean").fit_transform(X)  # Fill NaNs with mean
#
#     # Random forest model for imputation
#     rf = RandomForestRegressor(n_estimators=100, random_state=42)
#     rf.fit(X_filled, y)
#
#     # Predict and fill missing values in the target column
#     y_pred = rf.predict(X_filled)
#     rfData['Target_Organ'] = y.fillna(pd.Series(y_pred, index=y.index))
#
#     # After imputation, proceed with rounding if enabled
#     if Round:
#         # Identify columns with "avg_" or "liver"
#         zscoreIndex = [
#             col for col in rfData.columns if "avg_" in col or "liver" in col
#         ]
#
#         # Apply floor rounding and cap values at 5
#         for col in zscoreIndex:
#             rfData[col] = np.floor(rfData[col])
#             rfData[col] = np.where(rfData[col] > 5, 5, rfData[col])
#
#         # Identify columns starting with uppercase letters, excluding the first column
#         histoIndex = [
#             col for col in rfData.columns[1:]  # Skip the first column
#             if col[0].isupper()
#         ]
#
#         # Apply ceiling rounding to these columns
#         for col in histoIndex:
#             rfData[col] = np.ceil(rfData[col])
