# s3-client

Tiny Python helper for saving files to Amazon S3 with a simple, predictable key layout.

- **Environments:** `dev`, `stage`, `prod`, or `local`
- **Key format:** `<env>/<category>/[subpath/]<filename-uuid>.<ext>`
- **Behavior:** in `local` env, uploads are skipped (returns `None`)

---

## Install

Requires Python 3.9+ and `boto3`.

```bash
pip install boto3

