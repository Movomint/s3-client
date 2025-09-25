# s3_client.py
import os
import uuid
import mimetypes
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
        return f"https://{self.bucket}.s3.{self.region}.amazonaws.com/{key}"
