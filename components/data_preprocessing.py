def data_preprocessing(ingested_data):
    import os

    import numpy as np
    import pandas as pd
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import (LabelEncoder, OneHotEncoder,
                                       StandardScaler)

    data = ingested_data.drop(columns=["RowNumber", "CustomerId", "Surname"], axis=1)
    X = data.iloc[:, :-1]
    y = data.iloc[:, -1:]
    le = LabelEncoder()
    ohe = OneHotEncoder()
    X["Gender"] = le.fit_transform(X["Gender"])
    geo_df = pd.DataFrame(ohe.fit_transform(X[["Geography"]]).toarray())
    geo_df.columns = ohe.get_feature_names_out(["Geography"])
    X = X.join(geo_df)
    X.drop(columns=["Geography"], axis=1, inplace=True)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    sc = StandardScaler()
    X_train = sc.fit_transform(X_train)
    X_test = sc.transform(X_test)
    data_path = "data/processed"
    if not os.path.exists(data_path):
        os.makedirs(data_path)
    x_train_path = f"{data_path}/X_train.npy"
    x_test_path = f"{data_path}/X_test.npy"
    y_train_path = f"{data_path}/y_train.npy"
    y_test_path = f"{data_path}/y_test.npy"
    np.save(x_train_path, X_train)
    np.save(x_test_path, X_test)
    np.save(y_train_path, y_train)
    np.save(y_test_path, y_test)
    processed_data = {"X_train": x_train_path, "X_test": x_test_path, "y_train": y_train_path, "y_test": y_test_path}
    return processed_data


def data_preprocessing_raw(ingested_data):
    import pandas as pd
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import (LabelEncoder, OneHotEncoder,
                                       StandardScaler)

    data = ingested_data.drop(columns=["RowNumber", "CustomerId", "Surname"], axis=1)
    X = data.iloc[:, :-1]
    y = data.iloc[:, -1:]
    le = LabelEncoder()
    ohe = OneHotEncoder()
    X["Gender"] = le.fit_transform(X["Gender"])
    geo_df = pd.DataFrame(ohe.fit_transform(X[["Geography"]]).toarray())
    geo_df.columns = ohe.get_feature_names_out(["Geography"])
    X = X.join(geo_df)
    X.drop(columns=["Geography"], axis=1, inplace=True)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    sc = StandardScaler()
    X_train = sc.fit_transform(X_train)
    X_test = sc.transform(X_test)
    processed_data = {"X_train": X_train, "X_test": X_test, "y_train": y_train, "y_test": y_test}
    return processed_data
