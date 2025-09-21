import pytest

from s3_client import S3Client

@pytest.mark.parametrize("env,expected_bucket", [
    ("dev",   "movomint-dev"),
    ("stage", "movomint-stage"),
    ("prod",  "movomint-prod"),
])
def test_init_valid_envs(env, expected_bucket):
    c = S3Client(env)
    assert c.env == env
    assert c.bucket == expected_bucket
    assert c.region == "us-east-1"

@pytest.mark.parametrize("bad_env", ["", "production", "DEV", "qa", None])
def test_init_invalid_env_raises(bad_env):
    with pytest.raises(ValueError):
        S3Client(bad_env)  # type: ignore[arg-type]

