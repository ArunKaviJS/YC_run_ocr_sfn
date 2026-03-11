### textract_service.py
import boto3
import base64
import time
import json
import re
import traceback
from typing import List, Dict, Any, Optional, Tuple
from botocore.exceptions import ClientError
from uuid import uuid4
from mongo import (
    try_claim_processing,
    fetch_job_record,
    set_job_started,
    set_job_succeeded,
    set_job_failed,
    get_textract_job_collection
)
from datetime import datetime, timezone
import os
import anthropic
from dotenv import load_dotenv

load_dotenv()
# -----------------------------
# Configuration
# -----------------------------
TEMP_BUCKET_PREFIX = "yellow-temp-"
S3_SOURCE_REGION = "ap-south-1"

# ============================================================
# S3 / Textract Helpers
# ============================================================

def get_random_textract_client():
    """Select a random AWS region for Textract and return client + temp bucket name."""
    region = S3_SOURCE_REGION
    print(f"🌍 Using region: {region}")
    textract = boto3.client("textract", region_name=region)
    temp_bucket = f"{TEMP_BUCKET_PREFIX}{region}"
    return textract, region, temp_bucket


def copy_to_temp_bucket(source_bucket: str, source_key: str, temp_bucket: str, region: str) -> Optional[str]:
    """Copy file to temporary bucket in the same region."""
    s3_dest = boto3.client("s3", region_name=region)
    try:
        temp_key = f"{uuid4().hex}_{source_key.split('/')[-1]}"
        print(f"📤 Copying file to temp bucket: s3://{temp_bucket}/{temp_key}")
        s3_dest.copy_object(
            Bucket=temp_bucket,
            CopySource={"Bucket": source_bucket, "Key": source_key},
            Key=temp_key
        )
        return temp_key
    except ClientError as e:
        print(f"⚠️ S3 ClientError: {e}")
        return None
    except Exception as e:
        print(f"⚠️ Unexpected S3 copy error: {e}")
        return None


def cleanup_temp_bucket(temp_bucket: str, temp_key: Optional[str], region: str):
    """Delete temporary object after processing completes."""
    if not temp_key:
        return
    try:
        print(f"🗑️ Cleaning up temp file: s3://{temp_bucket}/{temp_key}")
        boto3.client("s3", region_name=region).delete_object(Bucket=temp_bucket, Key=temp_key)
    except Exception as e:
        print(f"⚠️ Cleanup failed: {e}")


# ============================================================
# Claude OCR
# ============================================================

def claude_ocr(file_path: str) -> str:
    """
    Use Claude Sonnet 4.6 to extract all text from a local file (PDF or image).
    Downloads file from S3 first if needed — caller must provide local path.
    """
    client = anthropic.Anthropic(api_key=os.getenv("CLAUDE_ANTHROPIC"))

    with open(file_path, "rb") as f:
        file_bytes = f.read()

    encoded_file = base64.b64encode(file_bytes).decode("utf-8")

    # Detect media type
    lower_path = file_path.lower()
    if lower_path.endswith(".pdf"):
        media_type = "application/pdf"
        content_type = "document"
    elif lower_path.endswith(".png"):
        media_type = "image/png"
        content_type = "image"
    elif lower_path.endswith(".jpg") or lower_path.endswith(".jpeg"):
        media_type = "image/jpeg"
        content_type = "image"
    else:
        raise ValueError(f"Unsupported file type: {file_path}")

    print(f"🤖 Sending file to Claude OCR ({media_type})...")

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=8096,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": content_type,
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": encoded_file,
                        },
                    },
                    {
                        "type": "text",
                        "text": """
                                Extract all text from this document exactly as written. Preserve tables, line breaks, and formatting as much as possible.
                                CRITICAL RULES FOR NUMERIC FIELDS (marks, scores, roll numbers, register numbers, ID numbers, totals, grades expressed as numbers):

                                In handwritten or low-quality scanned documents, certain characters are commonly misread. Apply the following corrections ONLY in positions where a digit is expected (numeric fields):

                                - "O" or "o" → 0  (letter O confused with zero)
                                - "D" → 0  (letter D confused with zero)
                                - "Q" → 0  (letter Q confused with zero)
                                - "b" → 6  (letter b confused with six)
                                - "S" or "s" → 5  (letter S confused with five)
                                - "I" or "l" or "|" or "\" → 1  (letter I/l or pipe/backslash confused with one)
                                - "Z" or "z" → 2  (letter Z confused with two)
                                - "B" → 8  (letter B confused with eight)
                                - "G" → 6  (letter G confused with six)
                                - " " (space within a number) → remove the space (e.g. "1 1 0" → "110")

                                HOW TO IDENTIFY NUMERIC FIELDS:
                                - Any column or field labeled: Marks, Score, Total, Max, Min, Grade (if numeric), Roll No, Register No, Reg No, ID, Enrollment No, Seat No, Percentage, Rank, Year, Age.
                                - Any cell in a marks table where surrounding cells contain digits.
                                - Multi-digit sequences that are clearly IDs or roll numbers.

                                DO NOT apply these corrections to:
                                - Names of students, subjects, schools, or places.
                                - Text fields, addresses, or descriptions.
                                - Single letters that represent grades (e.g. A, B, C, F) unless they are clearly a misread digit in a numeric column.

                                Return the corrected, clean extracted text.
                        
                        """
                    },
                ],
            }
        ],
    )

    extracted_text = response.content[0].text
    print(f"✅ Claude OCR complete. Extracted {len(extracted_text)} characters.")
    return extracted_text


# ============================================================
# Main Orchestrator
# ============================================================

def run_claude(bucket: str, key: str, file_id: str, region: str = "ap-south-1") -> Dict[str, Any]:
    """
    Distributed-safe Claude OCR runner:
    - Uses Mongo locking (_id = fileId)
    - Reuses completed jobs when available
    - Downloads file from S3 and extracts text using Claude Sonnet 4.6
    """
    local_filepath = None
    owner_id = str(uuid4())[:8]
    col = get_textract_job_collection()

    try:
        # --- Try to claim (acts as distributed lock) ---
        claimed = try_claim_processing(file_id, owner_id)
        if not claimed:
            existing = fetch_job_record(file_id)
            if existing:
                status = existing.get("status")
                print(f"ℹ️ Existing Claude OCR job found for {file_id}: {status}")

                if status == "SUCCEEDED":
                    print("✅ Using cached Claude OCR result from Mongo")
                    return existing.get("result", {})

                elif status in ["IN_PROGRESS", "CLAIMED"]:
                    print("⏳ Waiting for existing job to finish...")
                    for _ in range(10):
                        time.sleep(3)
                        existing = fetch_job_record(file_id)
                        status = existing.get("status")
                        if status == "SUCCEEDED":
                            print("✅ Job completed by another process")
                            return existing.get("result", {})
                    raise Exception("Job already claimed but not finished (timeout)")

                elif status == "FAILED":
                    print("⚠️ Retrying after previous failed attempt")
                    col.update_one(
                        {"_id": file_id},
                        {
                            "$set": {
                                "status": "CLAIMED",
                                "owner": owner_id,
                                "result": None,
                                "updatedAt": datetime.utcnow(),
                            },
                            "$inc": {"attempts": 1},
                        },
                    )
                    print(f"🔁 Re-claimed file {file_id} after failure")
            else:
                raise Exception("Could not find or claim textract_jobs record.")

        # --- Download file from S3 to local ---
        local_filepath = f"/tmp/{os.path.basename(key)}"
        s3 = boto3.client("s3", region_name=region)
        s3.download_file(bucket, key, local_filepath)
        print(f"✅ Downloaded file to {local_filepath}")

        # --- Mark job as started ---
        set_job_started(file_id, job_id="CLAUDE_OCR")

        # --- Process via Claude OCR ---
        print("🧠 Starting Claude OCR text extraction...")
        extracted_text = claude_ocr(local_filepath)

        if not extracted_text:
            raise Exception("Claude OCR returned no text output")

        # Estimate page count from file size (rough heuristic) or set to 1
        page_count = 1

        print(f"🔹 Extracted text length: {len(extracted_text)} characters")
        print("extracted text",extracted_text)


        set_job_succeeded(file_id, extracted_text)
        print("✅ Claude OCR job succeeded and stored in Mongo")

        return extracted_text

    except Exception as e:
        print(f"❌ run_chunkr (Claude OCR) error: {e}")
        traceback.print_exc()
        set_job_failed(file_id, str(e))
        return {}

    finally:
        try:
            if local_filepath and os.path.exists(local_filepath):
                os.remove(local_filepath)
                print(f"🧹 Cleaned up local file: {local_filepath}")
        except Exception as ce:
            print(f"⚠️ Cleanup error: {ce}")