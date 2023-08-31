import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
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
    data.loc[
        data.origin.str.contains("C") & data.destination.str.contains("C"),
        "origin_code",
    ] = "CC"
    data.loc[
        data.origin.str.contains("C") & data.destination.str.contains("M"),
        "origin_code",
    ] = "CM"
    data.loc[
        data.origin.str.contains("M") & data.destination.str.contains("C"),
        "origin_code",
    ] = "MC"
    data.loc[
        data.origin.str.contains("M") & data.destination.str.contains("C"),
        "origin_code",
    ] = "MM"
    data.drop(columns=["origin", "destination"], axis="columns", inplace=True)
    data = pd.get_dummies(data, drop_first=True)
    X = data
    sc = StandardScaler()
    X = sc.fit_transform(X)
    return X


def split_fraud_detection_data(data):
    """
    ETL function to get data from a csv file and return a dataframe

    Read the csv file from a url, do some feature engineering, saving the processed in a database, and split data into train/test sets for modeling purpose.

    Args:
        input_path (str, optional): _description_. Defaults to "data/PS_20174392719_1491204439457_log_v2.csv".

    Returns:
        _type_: _description_
    """
    X = data.drop(labels="isfraud", axis=1)
    y = data["isfraud"]
    split_data = train_test_split(X, y, test_size=0.3, stratify=data.isfraud)
    sc = StandardScaler()
    X_train = sc.fit_transform(split_data[0])
    X_test = sc.fit_transform(split_data[1])
    y_train = split_data[2]
    y_test = split_data[3]
    data = {"X_train": X_train, "X_test": X_test, "y_train": y_train, "y_test": y_test}
    return data


def get_fraud_detection_model(X_train, y_train):
    random_forest_classifier = RandomForestClassifier(n_estimators=15, random_state=42)
    random_forest_classifier.fit(X_train, y_train)
    return random_forest_classifier


def get_fraud_detection_predictions(random_forest_classifier, X_test):
    rfc_pred = random_forest_classifier.predict(X_test)
    return rfc_pred


def get_fraud_detection_performance_report(rfc_pred, y_test):
    report = classification_report(
        y_test, rfc_pred, labels=[0, 1], target_names=["Not Fraud", "Fraud"]
    )
    return report


# def main():
#     data = get_fraud_detection_data()
#     random_forest_classifier = get_fraud_detection_model(data=data)
#     rfc_pred = get_fraud_detection_predictions(random_forest_classifier=random_forest_classifier, data=data)
#     report = get_fraud_detection_performance_report(rfc_pred=rfc_pred, data=data)


# if __name__ == "__main__":
#     main()
