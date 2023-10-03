import dill
import s3fs


def pickle_s3(obj, s3_path):
    """Pickle an object to S3."""
    s3 = s3fs.S3FileSystem()
    with s3.open(s3_path, "wb") as f:
        dill.dump(obj, f)


def unpickle_s3(s3_path):
    """Unpickle an object from S3."""
    s3 = s3fs.S3FileSystem()
    with s3.open(s3_path, "rb") as f:
        return dill.load(f)
