import os

import dill
import s3fs


def pickle_s3(obj, s3_path):
    """Pickle an object to S3."""
    storage_options = {"key": os.environ["AWS_ACCESS_KEY_ID"], "secret": os.environ["AWS_SECRET_ACCESS_KEY"]}
    s3 = s3fs.S3FileSystem(**storage_options)
    with s3.open(s3_path, "wb") as f:
        dill.dump(obj, f)


def unpickle_s3(s3_path):
    """Unpickle an object from S3."""
    storage_options = {"key": os.environ["AWS_ACCESS_KEY_ID"], "secret": os.environ["AWS_SECRET_ACCESS_KEY"]}
    s3 = s3fs.S3FileSystem(**storage_options)
    with s3.open(s3_path, "rb") as f:
        return dill.load(f)
