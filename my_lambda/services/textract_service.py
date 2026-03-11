import boto3
import random
import time
import json
import re
import traceback
from typing import List, Dict, Any, Optional, Tuple
from botocore.exceptions import ClientError
from trp import Document

# -----------------------------
# Configuration
# -----------------------------
TEXTRACT_REGIONS = [
    "us-east-2", "us-west-1", "us-west-2",
    "eu-west-1", "eu-central-1", "ap-southeast-1"
]
TEMP_BUCKET_PREFIX = "yellow-temp-"
S3_SOURCE_REGION = "ap-south-1"


# ============================================================
# S3 / Textract Helpers
# ============================================================
def get_random_textract_client():
    """Select a random AWS region for Textract and return client + temp bucket name."""
    region = random.choice(TEXTRACT_REGIONS)
    print(f"🌍 Using Textract in region: {region}")
    textract = boto3.client("textract", region_name=region)
    temp_bucket = f"{TEMP_BUCKET_PREFIX}{region}"
    return textract, region, temp_bucket


def copy_to_temp_bucket(source_bucket: str, source_key: str, temp_bucket: str, region: str) -> Optional[str]:
    """Copy file to temporary bucket in the same region."""
    s3_dest = boto3.client("s3", region_name=region)
    try:
        temp_key = source_key.split("/")[-1]
        print(f"📤 Copying file to temp bucket: s3://{temp_bucket}/{temp_key}")
        s3_dest.copy_object(Bucket=temp_bucket, CopySource={"Bucket": source_bucket, "Key": source_key}, Key=temp_key)
        return temp_key
    except ClientError as e:
        print(f"⚠️ S3 ClientError: {e}")
        return None
    except Exception as e:
        print(f"⚠️ Unexpected S3 copy error: {e}")
        return None


def cleanup_temp_bucket(temp_bucket: str, temp_key: Optional[str], region: str):
    """Delete temporary object after Textract completes."""
    if not temp_key:
        return
    try:
        print(f"🗑️ Cleaning up temp file: s3://{temp_bucket}/{temp_key}")
        boto3.client("s3", region_name=region).delete_object(Bucket=temp_bucket, Key=temp_key)
    except Exception as e:
        print(f"⚠️ Cleanup failed: {e}")


# ============================================================
# Textract Normalization (TRP Based)
# ============================================================
def normalize_textract_response(textract_output: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize raw Textract JSON using TRP into a clean structure:
    {
      "tables": [...],
      "forms": [...],
      "lines": [...]
    }
    """
    print("🔄 Normalizing Textract JSON using TRP...")
    doc = Document(textract_output)
    normalized = {"tables": [], "forms": [], "lines": []}

    # Tables
    for page in doc.pages:
        for table in page.tables:
            table_data = []
            for row in table.rows:
                table_data.append([cell.text.strip() if cell.text else "" for cell in row.cells])
            normalized["tables"].append(table_data)

        # Key-Value Pairs
        form_data = {}
        for field in page.form.fields:
            key = field.key.text.strip() if field.key else None
            val = field.value.text.strip() if field.value else None
            if key:
                form_data[key] = val
        if form_data:
            normalized["forms"].append(form_data)

        # Lines
        for line in page.lines:
            normalized["lines"].append(line.text.strip())

    print(f"✅ Normalization complete: {len(normalized['tables'])} tables, "
          f"{len(normalized['forms'])} form groups, {len(normalized['lines'])} lines")
    return normalized


# ============================================================
# Marks Extraction Logic
# ============================================================
def _safe_to_int(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    value = value.strip()
    digits = re.sub(r"[^\d]", "", value)
    return int(digits) if digits.isdigit() else None


def _is_checkmark(value: Optional[str]) -> bool:
    if not value:
        return False
    value = value.strip()
    return value.upper() == "SELECTED" or value in ["✓", "/", "\\", "√", "×", "x"]


def extract_marks(tables: List[List[List[str]]]) -> Dict[str, Any]:
    """Extract structured marks data from tables."""
    print("🚀 Extracting marks from tables...")
    if not tables:
        return {"PartA": [], "PartB_C": [], "Totals": {}, "metadata": {}}

    final_data = {
        "PartA": [],
        "PartB_C": [],
        "Totals": {},
        "metadata": {
            "total_questions_part_a": 0,
            "answered_questions_part_a": 0,
            "total_questions_part_bc": 0,
            "answered_questions_part_bc": 0,
        },
    }

    marks_table = max(tables, key=len)  # assume largest table is marks table

    for idx, row in enumerate(marks_table):
        if idx == 0:
            continue

        # ---- Part A (Questions 1-10) ----
        try:
            q_no = _safe_to_int(row[0]) if len(row) > 0 else None
            mark_val = _safe_to_int(row[2]) if len(row) > 2 else None
            answered = _is_checkmark(row[1]) if len(row) > 1 else False

            if q_no and 1 <= q_no <= 10:
                final_data["PartA"].append({"Q_No": str(q_no), "Marks": mark_val, "Answered": answered})
                final_data["metadata"]["total_questions_part_a"] += 1
                if answered:
                    final_data["metadata"]["answered_questions_part_a"] += 1
        except Exception:
            continue

        # ---- Part B & C (Questions 11-16) ----
        try:
            q_no = _safe_to_int(row[3]) if len(row) > 3 else None
            sub_part = row[4].strip().lower() if len(row) > 4 and row[4] else ""
            total_marks = _safe_to_int(row[11]) if len(row) > 11 else None

            if q_no and 11 <= q_no <= 16 and sub_part in ["a", "b"]:
                if total_marks is None:
                    # fallback: sum individual columns
                    total_marks = sum(
                        _safe_to_int(row[i]) or 0 for i in [6, 8, 10] if len(row) > i
                    )

                answered = any(_is_checkmark(row[i]) for i in [5, 7, 9] if len(row) > i)
                q_id = f"{q_no}{sub_part}"

                if not any(item["Q_No"] == q_id for item in final_data["PartB_C"]):
                    final_data["PartB_C"].append({"Q_No": q_id, "Marks": total_marks, "Answered": answered})
                    final_data["metadata"]["total_questions_part_bc"] += 1
                    if answered:
                        final_data["metadata"]["answered_questions_part_bc"] += 1
        except Exception:
            continue

        # ---- Totals Row ----
        try:
            if row[0] and "total" in row[0].lower():
                if len(row) > 2:
                    final_data["Totals"]["PartA_Total"] = _safe_to_int(row[2])
                if len(row) > 11:
                    final_data["Totals"]["PartBC_Total"] = _safe_to_int(row[11])
        except Exception:
            continue

    # ---- Grand Total Fallback ----
    if "GrandTotal" not in final_data["Totals"]:
        part_a_total = final_data["Totals"].get("PartA_Total", 0) or 0
        part_bc_total = final_data["Totals"].get("PartBC_Total", 0) or 0
        final_data["Totals"]["GrandTotal"] = part_a_total + part_bc_total

    print("✅ Marks extraction complete.")
    return final_data


# ============================================================
# Main Orchestrator
# ============================================================
def run_textract(bucket: str, key: str, textract_client, temp_bucket: str, region: str) -> Dict[str, Any]:
    temp_key = None
    try:
        temp_key = copy_to_temp_bucket(bucket, key, temp_bucket, region)
        if not temp_key:
            raise Exception("Failed to copy to temp bucket")

        # Start Textract Job
        print("📄 Starting Textract Document Analysis...")
        start_resp = textract_client.start_document_analysis(
            DocumentLocation={"S3Object": {"Bucket": temp_bucket, "Name": temp_key}},
            FeatureTypes=["TABLES", "FORMS"],
        )
        job_id = start_resp["JobId"]
        print(f"🎯 Job ID: {job_id}")

        # Poll Textract Job
        status = "IN_PROGRESS"
        while status == "IN_PROGRESS":
            time.sleep(5)
            resp = textract_client.get_document_analysis(JobId=job_id)
            status = resp.get("JobStatus", status)
            print(f"... Status: {status}")
            if status in ["SUCCEEDED", "FAILED"]:
                job_output = resp
                break

        if status != "SUCCEEDED":
            raise Exception(f"Textract job failed with status {status}")

        # Collect all pages
        results = resp.get("Blocks", [])
        next_token = resp.get("NextToken")
        while next_token:
            next_resp = textract_client.get_document_analysis(JobId=job_id, NextToken=next_token)
            results.extend(next_resp.get("Blocks", []))
            next_token = next_resp.get("NextToken")

        # Wrap into a JSON structure
        textract_json = {"Blocks": results, "DocumentMetadata": resp.get("DocumentMetadata", {})}

        # ---- Normalize using TRP ----
        normalized_data = normalize_textract_response(textract_json)

        # ---- Extract Marks ----
        marks_data = extract_marks(normalized_data.get("tables", []))

        final_output = {
            "normalized_data": normalized_data,
            "marks_data": marks_data
        }

        print("✅ Final Textract pipeline complete.")
        return final_output

    except Exception as e:
        print(f"❌ Error in run_textract: {e}")
        traceback.print_exc()
        return {}
    finally:
        cleanup_temp_bucket(temp_bucket, temp_key, region)