"""
DAG: imdb_raw_ingest
Downloads IMDb datasets to S3 raw ONLY when the remote has changed.

- Compares HTTP Last-Modified / Content-Length (and ETag when available)
- Writes to s3://<bucket>/<RAW_PREFIX>/<dataset_dir>/run_date=YYYY-MM-DD/<file>
- Updates a control file per IMDb object under .../_remote_state/<file>.json
- Writes _MANIFEST.json and _SUCCESS under s3://<bucket>/<RAW_PREFIX>/run_date=YYYY-MM-DD/
- Maintains a 'latest/' pointer per dataset_dir for convenience
"""

from __future__ import annotations

import os
import json
import hashlib
import tempfile
from datetime import timedelta, datetime, timezone
from typing import List, Dict, Optional
from collections import Counter

import boto3
import botocore
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import get_current_context

# --------- Config via env ---------
S3_RAW_BUCKET = os.environ["S3_RAW_BUCKET"]                     # e.g. imdb-data-raw-ak  (REQUIRED)
RAW_PREFIX    = os.environ.get("RAW_PREFIX", "imdb/raw")        # e.g. imdb/raw
CONTROL_PREFIX= os.environ.get("CONTROL_PREFIX", f"{RAW_PREFIX.rstrip('/')}/_remote_state")
IMDB_BASE_URL = os.environ.get("IMDB_BASE_URL", "https://datasets.imdbws.com/")
IMDB_FILES    = [f.strip() for f in os.environ.get(
    "IMDB_FILES",
    "title.basics.tsv.gz,title.ratings.tsv.gz,title.akas.tsv.gz,title.crew.tsv.gz,title.episode.tsv.gz,title.principals.tsv.gz,name.basics.tsv.gz"
).split(",") if f.strip()]
WRITE_LATEST  = os.environ.get("WRITE_LATEST", "true").lower() == "true"
AWS_REGION    = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_retry": False,
    "email_on_failure": False,
}

def _s3():
    return boto3.client("s3", region_name=AWS_REGION)

def _s3_key_exists(bucket: str, key: str) -> bool:
    s3 = _s3()
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in {"404", "NotFound", "NoSuchKey"}:
            return False
        raise

def _dataset_dir(fname: str) -> str:
    # title.basics.tsv.gz -> title_basics ; name.basics.tsv.gz -> name_basics
    return fname.replace(".tsv.gz", "").replace(".", "_")

def _get_remote_meta(url: str) -> Dict[str, Optional[str]]:
    """HEAD the remote file; fall back to GET if needed."""
    def _normalize(headers):
        last_modified = headers.get("Last-Modified")
        etag = headers.get("ETag")
        length = headers.get("Content-Length")
        return {
            "last_modified": last_modified,
            "etag": etag.strip('"') if etag else None,
            "content_length": int(length) if length is not None else None,
        }

    r = requests.head(url, allow_redirects=True, timeout=30)
    r.raise_for_status()
    meta = _normalize(r.headers)
    if not meta["last_modified"] or meta["content_length"] is None:
        g = requests.get(url, stream=True, timeout=30)
        g.raise_for_status()
        meta = _normalize(g.headers)
        g.close()
    return meta

def _read_control(bucket: str, fname: str) -> Optional[Dict]:
    key = f"{CONTROL_PREFIX.rstrip('/')}/{fname}.json"
    s3 = _s3()
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except botocore.exceptions.ClientError as e:
        if e.response.get("Error", {}).get("Code") in {"404", "NoSuchKey"}:
            return None
        if e.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404:
            return None
        raise

def _write_control(bucket: str, fname: str, control: Dict):
    key = f"{CONTROL_PREFIX.rstrip('/')}/{fname}.json"
    _s3().put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(control, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

def _upload_file(bucket: str, key: str, local_path: str, metadata: Dict[str, str]):
    _s3().upload_file(
        local_path, bucket, key,
        ExtraArgs={"ContentType": "application/gzip", "Metadata": metadata}
    )

def _copy_to(bucket: str, src_key: str, dst_key: str):
    _s3().copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": src_key},
        Key=dst_key,
        MetadataDirective="REPLACE",
        ContentType="application/gzip",
    )
    return dst_key

with DAG(
    dag_id="imdb_raw_ingest",
    description="Download IMDb datasets to S3 raw only when IMDb has new data.",
    default_args=default_args,
    start_date=days_ago(1),
    schedule=None,     # trigger manually or from parent DAG
    catchup=False,
    tags=["imdb", "raw", "s3"],
) as dag:

    @task
    def plan() -> List[Dict]:
        """
        Return a LIST (not a dict) so we can map directly over it.
        Each item includes run_date and a fully qualified run_key in S3.
        """
        run_date = datetime.now(timezone.utc).date().isoformat()
        items: List[Dict] = []
        for fname in IMDB_FILES:
            ds = _dataset_dir(fname)
            run_prefix = f"{RAW_PREFIX.rstrip('/')}/{ds}/run_date={run_date}/"
            items.append({
                "fname": fname,
                "dataset_dir": ds,
                "url": IMDB_BASE_URL + fname,
                "run_date": run_date,
                "run_key": f"{run_prefix}{fname}",
            })
        return items

    @task
    def decide_and_fetch(item: Dict) -> Dict:
        """
        For each file:
          * Get remote headers
          * Load control (last seen) from S3
          * If unchanged -> return 'unchanged'
          * Else download, write to run partition, update control, and update 'latest'
        """
        bucket   = S3_RAW_BUCKET
        fname    = item["fname"]
        url      = item["url"]
        ds       = item["dataset_dir"]
        run_key  = item["run_key"]
        run_date = item["run_date"]

        remote = _get_remote_meta(url)
        last_mod = remote["last_modified"]
        c_len    = remote["content_length"]
        etag     = remote["etag"]

        control = _read_control(bucket, fname) or {}
        if control.get("last_modified") == last_mod and control.get("content_length") == c_len:
            return {
                "file": fname, "status": "unchanged",
                "last_modified": last_mod, "content_length": c_len,
                "s3_key": control.get("s3_key"), "run_date": run_date
            }

        # Idempotency guard: if this run_key already exists, don't re-upload.
        if _s3_key_exists(bucket, run_key):
            _write_control(bucket, fname, {
                "last_modified": last_mod,
                "content_length": c_len,
                "etag": etag,
                "s3_key": run_key,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })
            latest_key = None
            if WRITE_LATEST:
                latest_key = _copy_to(bucket, run_key, f"{RAW_PREFIX.rstrip('/')}/{ds}/latest/{fname}")
            return {
                "file": fname, "status": "present", "s3_key": run_key, "latest_key": latest_key,
                "last_modified": last_mod, "content_length": c_len, "run_date": run_date
            }

        # Download and upload
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
        h = hashlib.md5()

        with requests.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        h.update(chunk)
                        f.write(chunk)

        meta = {
            "source": "imdbws",
            "file": fname,
            "run_date": run_date,
            "md5": h.hexdigest(),
            "remote_last_modified": last_mod or "",
            "remote_content_length": str(c_len) if c_len is not None else "",
            "remote_etag": etag or "",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }
        _upload_file(bucket, run_key, tmp_path, meta)
        try:
            os.remove(tmp_path)
        except FileNotFoundError:
            pass

        # Update control and 'latest' pointer
        _write_control(bucket, fname, {
            "last_modified": last_mod,
            "content_length": c_len,
            "etag": etag,
            "s3_key": run_key,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })
        latest_key = None
        if WRITE_LATEST:
            latest_key = _copy_to(bucket, run_key, f"{RAW_PREFIX.rstrip('/')}/{ds}/latest/{fname}")

        return {
            "file": fname, "status": "updated", "s3_key": run_key, "latest_key": latest_key,
            "last_modified": last_mod, "content_length": c_len, "run_date": run_date
        }

    @task
    def finalize(results: list):
        """
        results: list from the mapped decide_and_fetch task.
        It may contain dicts (preferred) or strings/JSON-strings (older code).
        This function normalizes before counting & writing the manifest.
        """

        # 1) Normalize results to list[dict]
        norm = []
        for r in (results or []):
            if isinstance(r, dict):
                norm.append(r)
            elif isinstance(r, (bytes, bytearray)):
                try:
                    obj = json.loads(r.decode("utf-8"))
                    norm.append(obj if isinstance(obj, dict) else {"status": str(obj)})
                except Exception:
                    norm.append({"status": r.decode("utf-8", "ignore")})
            elif isinstance(r, str):
                # try JSON first, else treat as a status string
                try:
                    obj = json.loads(r)
                    norm.append(obj if isinstance(obj, dict) else {"status": str(obj)})
                except Exception:
                    norm.append({"status": r})
            else:
                norm.append({"status": str(r)})

        # 2) Counts
        c = Counter(d.get("status") for d in norm)

        # 3) Build manifest (adjust bucket/keys to your constants)
        ctx = get_current_context()
        ds = ctx["ds"]  # YYYY-MM-DD
        run_id = ctx["dag_run"].run_id

        base_prefix   = f"{RAW_PREFIX.rstrip('/')}/run_date={ds}/"
        manifest_key  = f"{base_prefix}_MANIFEST.json"
        success_key   = f"{base_prefix}_SUCCESS"

        manifest = {
            "run": {"dag_run_id": run_id, "ts": ctx["ts"]},
            "counts": dict(c),
            "files": norm,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.put_object(
            Bucket=S3_RAW_BUCKET,
            Key=manifest_key,
            Body=json.dumps(manifest, indent=2, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        s3.put_object(Bucket=S3_RAW_BUCKET, Key=success_key, Body=b"")
        print(f"✔ Wrote manifest to s3://{S3_RAW_BUCKET}/{manifest_key}")
        print(f"✔ Wrote success marker to s3://{S3_RAW_BUCKET}/{success_key}")
        
    items = plan()
    results = decide_and_fetch.expand(item=items)
    finalize(results)