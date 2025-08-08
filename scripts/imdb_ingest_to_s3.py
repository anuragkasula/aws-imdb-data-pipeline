import os
import boto3
import requests

# Set your S3 bucket and base prefix
BUCKET = 'imdb-data-raw-ak'
PREFIX = 'raw/'

# List all IMDb .tsv.gz files
IMDB_FILES = [
    "name.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz"
    "title.basics.tsv.gz",
    "title.ratings.tsv.gz",
    "title.akas.tsv.gz",
    "title.principals.tsv.gz"
]
IMDB_URL = "https://datasets.imdbws.com/"

# Create a tmp directory for downloads if not exist
tmp_dir = os.path.join(os.getcwd(), "tmp")
os.makedirs(tmp_dir, exist_ok=True)

s3 = boto3.client('s3')

for file in IMDB_FILES:
    print(f"Downloading {file}...")
    r = requests.get(IMDB_URL + file, stream=True)
    local_file = os.path.join(tmp_dir, file)
    with open(local_file, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    # Upload to its own folder for easy Athena DDL
    s3_folder = f"{PREFIX}{file.replace('.tsv.gz','')}/"
    print(f"Uploading {file} to S3 folder {s3_folder}...")
    s3.upload_file(local_file, BUCKET, s3_folder + file)
    print(f"Uploaded {file} to s3://{BUCKET}/{s3_folder}{file}")
    # Optionally clean up
    os.remove(local_file)
    print(f"Deleted local file {local_file}")

print("All new IMDb files downloaded and uploaded to S3 successfully!")
