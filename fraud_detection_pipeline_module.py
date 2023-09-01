import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.preprocessing import StandardScaler


def clean_fraud_detection_new_data(rawdata):
    """
    ETL function to get data from a csv file and return a dataframe

    Read the csv file from a url, do some feature engineering, saving the processed in a database, and split data into train/test sets for modeling purpose.

    Args:
        input_path (str, optional): _description_. Defaults to "data/PS_20174392719_1491204439457_log_v2.csv".

    Returns:
        _type_: _description_
    """
    data = rawdata.rename(
        columns={
            "nameorig": "origin",
            "oldbalanceorg": "sender_old_balance",
            "newbalanceorig": "sender_new_balance",
            "namedest": "destination",
            "oldbalancedest": "receiver_old_balance",
            "newbalancedest": "receiver_new_balance",
        }
    ).drop(columns=["step"], axis="columns")
    cols = data.columns.tolist()
    new_position = 3
    cols.insert(new_position, cols.pop(cols.index("destination")))
    data = data[cols]
    data["origin_code"] = np.nan
    data.loc[data.origin.str.contains("C") & data.destination.str.contains("C"), "origin_code"] = "CC"
    data.loc[data.origin.str.contains("C") & data.destination.str.contains("M"), "origin_code"] = "CM"
    data.loc[data.origin.str.contains("M") & data.destination.str.contains("C"), "origin_code"] = "MC"
    data.loc[data.origin.str.contains("M") & data.destination.str.contains("C"), "origin_code"] = "MM"
    data.drop(columns=["origin", "destination"], axis="columns", inplace=True)
    data = pd.get_dummies(data, drop_first=True)
    X = data
    sc = StandardScaler()
    X = sc.fit_transform(X)
    return X


def split_fraud_detection_data():
    print("new function as 2023-09-01T13:59:31.806963")


def get_fraud_detection_model(X_train, y_train):
    random_forest_classifier = RandomForestClassifier(n_estimators=15, random_state=42)
    random_forest_classifier.fit(X_train, y_train)
    return random_forest_classifier


def get_fraud_detection_predictions(random_forest_classifier, X_test):
    rfc_pred = random_forest_classifier.predict(X_test)
    return rfc_pred


def get_fraud_detection_performance_report(rfc_pred, y_test):
    report = classification_report(y_test, rfc_pred, labels=[0, 1], target_names=["Not Fraud", "Fraud"])
    return report
