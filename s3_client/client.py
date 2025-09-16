import os
import uuid
import mimetypes
import boto3

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


class S3Client:
    def __init__(self, env: str, bucket: str, region: str):
        self.env = env  # expected: "dev" | "stage" | "prod" (or "local" to skip)
        self.bucket = bucket
        self.region = region

        self.client = None if self.env == "local" else boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=self.region,
        )

    def _key(self, name: str, category: str, subpath: str = "") -> str:
        """
        Key format: <env>/<category>/[subpath/]<filename-uuid>.<ext>
        Example:    stage/ingested/test-a1b2c3d4.pdf
                    prod/generated/pick-tickets/pt-0001-9f8e7d6c.pdf
        """
        root, ext = os.path.splitext(name)
        unique = f"{root}-{uuid.uuid4().hex[:8]}{ext}"

        parts = [self.env.strip("/"), category.strip("/")]
        if subpath:
            parts.append(subpath.strip("/"))
        parts.append(unique)
        return "/".join(parts)

    def upload(self, data: bytes, name: str, category: str, subpath: str) -> str | None:
        key = self._key(name, category, subpath)
        content_type = mimetypes.guess_type(name)[0] or "application/octet-stream"

        if self.client is None:
            print("[LOCAL] Skipping saving files to S3")
            return None

        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )
        return f"https://{self.bucket}.s3.{self.region}.amazonaws.com/{key}"
