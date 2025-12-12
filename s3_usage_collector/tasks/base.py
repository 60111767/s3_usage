from s3_usage_collector.api.s3client import S3Client
from s3_usage_collector.data.config import Config


class BaseCollector:

    def __init__(self):
        self.s3_client = S3Client(access_key=Config.S3_ACCESS_KEY, secret_key=Config.S3_SECRET_KEY, endpoint=Config.S3_ENDPOINT)
