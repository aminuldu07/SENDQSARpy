from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score, roc_curve
import matplotlib.pyplot as plt

def get_auc_curve(rf_data=None, best_m=None, path_db=None, studyid_metadata_path=None, 
                  fake_study=True, round_values=True, undersample=True):
    """
    Train a Random Forest model, calculate AUC, and plot the ROC curve.
    
    Parameters:
        rf_data (pd.DataFrame): Input data for training. If None, data is generated dynamically.
        best_m (int): The `max_features` hyperparameter for Random Forest. If None, determined dynamically.
        path_db (str): Path to the SQLite database. Required if `rf_data` or `best_m` is None.
        studyid_metadata_path (str): Path to the CSV file containing study ID metadata. Required if `rf_data` or `best_m` is None.
        fake_study (bool): Whether to use fake study IDs. Default is True.
        round_values (bool): Whether to round numerical values. Default is True.
        undersample (bool): Whether to perform undersampling. Default is True.
    
    Returns:
        None. Prints the AUC and plots the ROC curve.
    """
    if rf_data is None or best_m is None:
        if path_db is None or studyid_metadata_path is None:
            raise ValueError("Both 'path_db' and 'studyid_metadata_path' must be provided if 'rf_data' or 'best_m' is None.")
        
        # Generate data and best_m using get_rfData_and_best_m
        result = get_rfData_and_best_m(
            path_db=path_db,
            studyid_metadata_path=studyid_metadata_path,
            fake_study=fake_study,
            round_values=round_values,
            undersample=undersample
        )
        rf_data = result["rfData"]
        best_m = result["best_m"]
    
    # Train a Random Forest model
    rf_model = RandomForestClassifier(max_features=best_m, n_estimators=500, random_state=42)
    X = rf_data.drop(columns=["Target_Organ"])
    y = rf_data["Target_Organ"]
    rf_model.fit(X, y)
    
    # Predict probabilities
    y_pred_prob = rf_model.predict_proba(X)[:, 1]
    
    # Calculate AUC
    auc = roc_auc_score(y, y_pred_prob)
    print(f"AUC: {auc}")
    
    # Plot ROC curve
    fpr, tpr, _ = roc_curve(y, y_pred_prob)
    plt.figure()
    plt.plot(fpr, tpr, color="red", lw=2, label=f"ROC Curve (AUC = {auc:.3f})")
    plt.plot([0, 1], [0, 1], color="gray", linestyle="--", lw=2)
    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")
    plt.title("ROC Curve")
    plt.legend(loc="lower right")
    plt.show()
