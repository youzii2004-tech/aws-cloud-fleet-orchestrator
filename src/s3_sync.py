"""
s3_sync.py — S3 asset staging and synchronization pipeline.
Handles pre-flight reference uploads and post-execution artifact ingest.
"""
import os
import logging
from pathlib import Path
from typing import Optional, List
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger("S3Sync")


class S3SyncManager:
    def __init__(self, bucket_name: str, aws_profile: Optional[str] = None, region: str = "us-east-1"):
        self.bucket_name = bucket_name
        session = boto3.Session(profile_name=aws_profile, region_name=region)
        self.s3 = session.client("s3")

    def upload_file(self, local_path: Path, s3_key: str) -> bool:
        """Uploads a local asset to the target S3 bucket."""
        try:
            logger.info(f"Uploading {local_path} to s3://{self.bucket_name}/{s3_key}...")
            self.s3.upload_file(str(local_path), self.bucket_name, s3_key)
            return True
        except ClientError as e:
            logger.error(f"S3 upload error: {e}")
            return False

    def download_file(self, s3_key: str, local_path: Path) -> bool:
        """Downloads a remote S3 artifact to a local directory."""
        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            logger.info(f"Downloading s3://{self.bucket_name}/{s3_key} to {local_path}...")
            self.s3.download_file(self.bucket_name, s3_key, str(local_path))
            return True
        except ClientError as e:
            logger.error(f"S3 download error: {e}")
            return False

    def list_artifacts(self, prefix: str) -> List[str]:
        """Lists artifact keys matching prefix."""
        try:
            resp = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            return [obj["Key"] for obj in resp.get("Contents", [])]
        except ClientError as e:
            logger.error(f"Failed to list S3 objects: {e}")
            return []
