def data_ingestion(data):
    import pandas as pd

    ingested_data = pd.read_csv(data)
    return ingested_data
