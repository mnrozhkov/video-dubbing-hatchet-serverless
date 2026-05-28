#!/usr/bin/env python3
"""Download sample videos, populate a local batch, and optionally upload to S3.

Examples:
    # Download NASA clip, create 100 local copies, upload to S3 prefix demo-100/
    python scripts/download_samples.py nasa --sample-size 100 --s3-prefix demo-100

    # Download only (no batch, no upload)
    python scripts/download_samples.py nasa

    # Tears of Steel, 50-file batch, upload
    python scripts/download_samples.py tears-of-steel --sample-size 50 --s3-prefix demo-50
"""

from pipeline.download import main

if __name__ == "__main__":
    main()
