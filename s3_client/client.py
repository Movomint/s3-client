# s3_client.py
import base64
import os
import re
import uuid
import mimetypes
import urllib.parse
import boto3
from typing import Literal, Optional

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

Env = Literal["local", "dev", "stage", "prod"]
ENVS = {"local", "dev", "stage", "prod"}

# Only non-local buckets exist
BUCKET_BY_ENV = {
    "dev":   "movomint-dev",
    "stage": "movomint-stage",
    "prod":  "movomint-prod",
}

class S3Client:
    def __init__(self, env: Env):
        if env not in ENVS:
            raise ValueError(f"env must be one of {sorted(ENVS)}; got {env!r}")

        self.env: Env = env
        self.region = "us-east-1"

        # No bucket/client in local
        self.bucket: Optional[str] = None if env == "local" else BUCKET_BY_ENV[env]
        self.client = None if env == "local" else boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=self.region,
        )

    def _key(self, name: str, category: str, subpath: str = "") -> str:
        """
        Key format: <env>/<category>/[subpath/]<filename-uuid>.<ext>
        Example:    movomint-stage/ingested/test-a1b2c3d4.pdf
                    movomint-prod/generated/pick-tickets/pt-0001-9f8e7d6c.pdf
        """
        root, ext = os.path.splitext(name)
        unique = f"{root}-{uuid.uuid4().hex[:8]}{ext}"

        parts = [category.strip("/")]
        if subpath:
            parts.append(subpath.strip("/"))
        parts.append(unique)
        return "/".join(parts)

    def upload(self, data: bytes, name: str, category: str, subpath: str) -> str | None:
        key = self._key(name, category, subpath)
        content_type = mimetypes.guess_type(name)[0] or "application/octet-stream"

        if self.client is None or self.bucket is None:
            print("[LOCAL] Skipping saving files to S3")
            return None

        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )
        # URL-encode the key to handle special characters like spaces, #, etc.
        encoded_key = urllib.parse.quote(key, safe='/')
        return f"https://{self.bucket}.s3.{self.region}.amazonaws.com/{encoded_key}"

    def download(self, s3_url: str) -> tuple[str, str]:
        if self.client is None or self.bucket is None:
            raise ValueError("Cannot download from S3 in local environment")
        
        parsed = urllib.parse.urlparse(s3_url)
        bucket = parsed.hostname.split('.')[0]
        key = urllib.parse.unquote(parsed.path.lstrip('/'))
        
        # Download the object
        response = self.client.get_object(Bucket=bucket, Key=key)
        data = response['Body'].read()
        filename_with_uuid = key.split('/')[-1] # Get filename from key
        
        # Remove UUID from filename to get original name
        match = re.match(r'^(.+)-[0-9a-f]{8}(\.\w+)?$', filename_with_uuid)
        if match:
            name = match.group(1) + (match.group(2) or '')
        else:
            name = filename_with_uuid

        base64_data = base64.b64encode(data).decode('utf-8')        
        return (name, base64_data)
