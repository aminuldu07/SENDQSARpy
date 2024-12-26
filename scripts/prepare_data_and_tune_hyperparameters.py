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
        imputer = SimpleImputer(strategy='mean')
        rfData.iloc[:, 1:] = imputer.fit_transform(rfData.iloc[:, 1:])
        if Round:
            zscore_cols = rfData.filter(regex='avg_|liver', axis=1).columns
            rfData[zscore_cols] = rfData[zscore_cols].apply(np.floor)
            rfData[zscore_cols] = rfData[zscore_cols].clip(upper=5)

            histo_cols = rfData.columns[1:][rfData.columns[1:].str[0].str.isupper()]
            rfData[histo_cols] = rfData[histo_cols].apply(np.ceil)

    count = 0
    for rep in range(reps):
        print(f"{(rep + 1) / reps * 100:.2f}% Complete...")

        # Train-test split
        if holdback == 1:
            test_idx = np.random.choice(rfData.index, 1, replace=False)
            train = rfData.drop(index=test_idx)
            test = rfData.loc[test_idx]
        else:
            train, test = train_test_split(rfData, test_size=holdback, stratify=rfData['Target_Organ'])

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
            best_m = 'sqrt'

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
