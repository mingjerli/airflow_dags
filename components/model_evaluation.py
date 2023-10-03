def model_evaluation(model_path, processed_data):
    import numpy as np
    from joblib import load

    x_test_path = processed_data["X_test"]
    y_test_path = processed_data["y_test"]
    X_test = np.load(x_test_path)
    y_test = np.load(y_test_path)
    model = load(model_path)
    result = model.score(X_test, y_test)
    return result


def direct_model_evaluation(model, processed_data):
    X_test = processed_data["X_test"]
    y_test = processed_data["y_test"]
    result = model.score(X_test, y_test)
    return result
