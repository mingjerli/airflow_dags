def model_training(processed_data):
    import numpy as np
    from joblib import dump
    from sklearn.linear_model import LinearRegression

    x_train_path = processed_data["X_train"]
    y_train_path = processed_data["y_train"]
    X_train = np.load(x_train_path)
    y_train = np.load(y_train_path)
    model = LinearRegression().fit(X_train, y_train)
    model_path = "model.joblib"
    dump(model, model_path)
    return model_path
