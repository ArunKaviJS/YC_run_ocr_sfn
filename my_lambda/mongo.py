from pymongo import MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError
from bson import ObjectId
from datetime import datetime, timezone
from uuid import uuid4
from typing import List, Dict, Any
import traceback
import json
import re
from collections import defaultdict
import os 
from config import MONGO_URI, DB_NAME, FILE_DETAILS_COLLECTION, REQUESTED_FIELDS_COLLECTION, CREDIT_COLLECTION
from services.azure_llm import AzureLLMAgent, RequestedField
from dotenv import load_dotenv

load_dotenv()

# --- Initialize MongoDB Client ---
mongo_client = MongoClient(MONGO_URI)


# ======================================================
# 📄 Textract Job Management
# ======================================================

def get_textract_job_collection():
    db = mongo_client[DB_NAME]
    return db["tb_OCR_reuse"]


def try_claim_processing(file_id, owner):
    """Attempt to claim a file for processing (insert new job record)."""
    try:
        col=get_textract_job_collection()
        col.insert_one({
            "_id": ObjectId(file_id),  # _id serves as file_id
            "owner": owner,
            "status": "CLAIMED",
            "result": None,
            "attempts": 0,
            "createdAt": datetime.utcnow(),
            "updatedAt": datetime.utcnow(),
        })
        print(f"✅ Claimed file {file_id}")
        return True
    except DuplicateKeyError:
        print(f"⚠️ File {file_id} already exists, reusing existing job.")
        return False


def fetch_job_record(file_id: str):
    col = get_textract_job_collection()
    return col.find_one({"_id": ObjectId(file_id)})


def set_job_started(file_id: str, job_id: str):
    col = get_textract_job_collection()
    return col.find_one_and_update(
        {"_id": file_id},
        {
            "$set": {
                "status": "IN_PROGRESS",
                "updatedAt": datetime.utcnow(),
            },
            "$inc": {"attempts": 1},
        },
        return_document=ReturnDocument.AFTER,
    )


def set_job_succeeded(file_id: str, result):
    col = get_textract_job_collection()
    return col.find_one_and_update(
        {"_id": ObjectId(file_id)},
        {
            "$set": {
                "status": "SUCCEEDED",
                "result": result,
                "updatedAt": datetime.utcnow(),
            }
        },
        return_document=ReturnDocument.AFTER,
    )


def set_job_failed(file_id: str, error_msg: str):
    col = get_textract_job_collection()
    return col.find_one_and_update(
        {"_id": ObjectId(file_id)},
        {
            "$set": {
                "status": "FAILED",
                "error": error_msg,
                "updatedAt": datetime.utcnow(),
            }
        },
        return_document=ReturnDocument.AFTER,
    )


# ======================================================
# 🗃️ Mongo Utilities
# ======================================================

def get_mongo_collection(collection_name):
    """Returns a MongoDB collection handle."""
    db = mongo_client[DB_NAME]
    return db[collection_name]


# ======================================================
# 📋 Requested Fields Handling
# ======================================================

def fetch_requested_fields(user_id, cluster_id):
    """
    Fetch and normalize requested fields for a given user and cluster.

    - For 'fieldType' = 'field', return as-is.
    - For 'fieldType' = 'table', expand each subfield inside 'tableData'
      as individual entries with reference to their table name.
    """
    collection = get_mongo_collection("tb_clusters")
    query = {"userId": ObjectId(user_id), "_id": ObjectId(cluster_id)}
    projection = {"requestedFields": 1, "_id": 0}
    doc = collection.find_one(query, projection)

    if not doc:
        print(f"⚠️ No requested fields found for user {user_id}, cluster {cluster_id}")
        return []

    requested_fields = doc.get("requestedFields", [])
    normalized_fields = []

    for field in requested_fields:
        field_type = field.get("fieldType", "field")
        field_name = field.get("fieldName", "").strip()
        if not field_name:
            continue  # skip invalid fields

        if field_type == "field":
            normalized_fields.append({
                "fieldType": "field",
                "fieldName": field_name,
                "fieldDatatype": field.get("fieldDatatype", "String"),
                "fieldDescription": field.get("fieldDescription", ""),
                "fieldExample": field.get("fieldExample", ""),
            })
        elif field_type == "table":
            table_name = field_name
            table_data = field.get("tableData", [])
            if not table_data:
                print(f"⚠️ Table '{table_name}' has no columns defined.")
                continue

            for col in table_data:
                col_name = col.get("fieldName", "").strip()
                if not col_name:
                    continue
                normalized_fields.append({
                    "fieldType": "table",
                    "tableName": table_name,
                    "fieldName": col_name,
                    "fieldDatatype": col.get("fieldDatatype", "String"),
                    "fieldDescription": col.get("fieldDescription", ""),
                    "fieldExample": col.get("fieldExample", ""),
                })

    return normalized_fields


# ======================================================
# 📄 Extracted Text Retrieval
# ======================================================

def fetch_extracted_text(user_id, cluster_id, file_id):
    collection = get_mongo_collection("tb_file_details")
    query = {
        "_id": ObjectId(file_id),
        "userId": ObjectId(user_id),
        "clusterId": ObjectId(cluster_id),
    }
    projection = {
        "extractedField": 1,
        "originalS3File": 1,
        "pageCount": 1,
        "originalFile": 1,
        "normalized_data": 1,
    }

    doc = collection.find_one(query, projection)
    if not doc:
        return None, None, None, None

    return (
        doc.get("extractedField"),
        doc.get("originalS3File"),
        doc.get("pageCount"),
        doc.get("normalized_data"),
    )


def mark_file_as_failed(doc_id):
    collection = get_mongo_collection(FILE_DETAILS_COLLECTION)
    collection.update_one(
        {"_id": ObjectId(doc_id)},
        {"$set": {
            "processingStatus": "Failed",
            "updatedAt": datetime.now(timezone.utc),
        }}
    )


# ======================================================
# 🧭 Job Status Management
# ======================================================

def update_job_status(job_id: str, status: str, summary: dict = None, message: str = None):
    """Insert or update job status."""
    collection = get_mongo_collection("job_status")
    doc = {
        "job_id": job_id,
        "status": status,
        "updatedAt": datetime.now(timezone.utc),
    }
    if summary:
        doc["summary"] = summary
    if message:
        doc["message"] = message

    collection.update_one({"job_id": job_id}, {"$set": doc}, upsert=True)


def fetch_job_status(job_id: str):
    """Retrieve job status by job_id."""
    collection = get_mongo_collection("job_status")
    return collection.find_one({"job_id": job_id}, {"_id": 0})

def extract_all_fields_and_tables(field_schema: list[dict], content: str, agent, context=None):
    """
    Build a rich prompt that includes both field and table instructions.
    For each field/table column we append an extra instruction that tells the LLM to
    carefully interpret the field using its description and return the expected type
    (string/number/date/etc) according to fieldDatatype.
    """

    # ---- Split schemas ----
    field_items = [f for f in field_schema if f["fieldType"] == "field"]
    table_items = [f for f in field_schema if f["fieldType"] == "table"]

    # ---- Build field instructions ----
    # For each field include: name, datatype, description and the extra instruction line.
    field_text_lines = []
    for f in field_items:
        extra = (
            f"with {f['fieldName']} - carefully understand what is meant by "
            f"the description ({f['fieldDescription']}) for this field and return "
            f"the output in the type expected ({f['fieldDatatype']}) "
            "(e.g., string, number, date, etc.)."
        )
        field_text_lines.append(
            f"- **{f['fieldName']}** ({f['fieldDatatype']}): {f['fieldDescription']}\n  {extra}"
        )
    field_text = "\n".join(field_text_lines)

    # ---- Build table instructions grouped by tableName ----
    tables = defaultdict(list)
    for r in table_items:
        tables[r["tableName"]].append(r)

    table_text = ""
    for table_name, cols in tables.items():
        table_text += f"\n### Table: {table_name}\n"
        for c in cols:
            extra = (
                f"with {c['fieldName']} - carefully understand what is meant by "
                f"the description ({c['fieldDescription']}) for this column and return "
                f"the output in the type expected ({c['fieldDatatype']}) "
                "(e.g., string, number, date, etc.)."
            )
            table_text += f"- **{c['fieldName']}** ({c['fieldDatatype']}): {c['fieldDescription']}\n  {extra}\n"

    # ---- Build universal prompt ----
    prompt = f"""
        You are an extremely accurate information extraction model.

        Extract ALL requested fields and ALL table rows across ALL pages (even if table continues on multiple pages).

        ========================================================
        FIELDS TO EXTRACT (OUTPUT AS SIMPLE KEY: VALUE)
        ========================================================
        {field_text}

        ========================================================
        TABLES TO EXTRACT (OUTPUT AS ITEMS ARRAY)
        ========================================================
        (Return format example:
        "TableName": {{
            "fieldType": "table",
            "items": [
                {{ "col1": "...", "col2": "...", ... }},
                ...
            ]
        }}
        )

        {table_text}

        ========================================================
        MULTI-PAGE TABLE CONTINUATION RULES
        ========================================================

        1. Tables may span multiple pages.
        2. If a table appears on one page with column headers and next pages contain rows
        with the same number of columns or similar pattern, treat them as continuation.
        3. If column headers repeat again later with the same structure → still continuation.
        4. Only treat as NEW table if structure or number of columns changes.
        5. Continue extracting rows until:
        - New table header appears OR
        - Document ends.
        6. If a row matches the pattern of previous table rows (same number of columns),
        extract it even if it is outside Markdown formatting.
        7. Remove noise like: “Page 2 of 5”, “[Signature]”, footers, headers.
        8. Keep extracting ALL rows across ALL pages.

        ========================================================
        FIELD EXTRACTION RULES
        ========================================================
        For each field:
        - Understand the fieldDescription deeply.
        - Return the value in EXACT type expected by fieldDatatype.
        - If no value found → return null.
        - Output nothing outside JSON.

        ========================================================
        TABLE COLUMN RULES
        ========================================================
        For each table column:
        - Use fieldDescription to understand the meaning.
        - Extract values in correct datatype (string, number, date, etc.).
        - Extract ALL rows, including across multiple pages.

        ========================================================
        JSON OUTPUT RULES
        ========================================================

        ⚠ OUTPUT ONLY VALID JSON.  
        NO markdown, NO explanation, NO notes.  

        Format:
        {{
        "fieldName1": value or null,
        "fieldName2": value or null,
        "TableName": {{
                "fieldType": "table",
                "items": [
                    {{
                        "col1": "...",
                        "col2": "..."
                    }},
                    ...
                ]
        }}
        }}

        ========================================================
        CONTENT TO ANALYZE
        ========================================================
        {content}

        Now return the FINAL JSON ONLY:
            """.strip()

    # ---- Call LLM ----
    try:
        raw = agent.complete(prompt, context=context) if context else agent.complete(prompt)
    except Exception as e:
        print("❌ LLM failed:", e)
        return None

    # ---- Clean JSON ----
    match = re.search(r'\{[\s\S]*\}', raw)
    if match:
        raw = match.group(0)

    try:
        return json.loads(raw)
    except Exception as e:
        print("❌ JSON parsing error:", e)
        return {"raw": raw}


def extract_field_values_with_llm_forfields(field: list[dict], lines: list[str], agent, context=None):
    """
    Build a dynamic extraction prompt and use an LLM agent to extract field values from given content.

    - Forces pure JSON output (no markdown, no commentary)
    - Automatically fills null for missing values
    """

    # --- Step 1: Build prompt ---
    field_instructions = []
    for f in field:
        field_instructions.append(
            f"- **{f['fieldName']}** ({f['fieldDatatype']}): {f['fieldDescription']}"
        )
    fields_text = "\n".join(field_instructions)
    content_text = "\n".join(lines)

    # --- Stronger prompt enforcing strict JSON output ---
    prompt = f"""
You are an information extraction model.

Extract the requested field values from the following content and return ONLY valid JSON.

---
### 🧩 Fields to Extract:
{fields_text}

---
### 📘 Content:
{content_text}

---
### 🧾 OUTPUT INSTRUCTIONS:
- Return ONLY valid JSON — no markdown, no explanations.
- Each key in JSON must match the fieldName exactly.
- If a field value is not present in the text, return **null** for that key.
- Do NOT include additional text before or after the JSON.
- Example output:
{{
{', '.join([f'"{f["fieldName"]}": "example value or null"' for f in field])}
}}

Now return the extracted JSON:
""".strip()

    # --- Step 2: Run extraction ---
    extracted_values = []
    key = " | ".join([f["fieldName"] for f in field])

    try:
        value = agent.complete(prompt, context=context) if context else agent.complete(prompt)
    except Exception as e:
        print(f"⚠️ LLM extraction failed for {key}: {e}")
        extracted_values.append("NA")
        return extracted_values

    # --- Step 3: Clean and parse JSON safely ---
    raw_output = value.strip()
    # Try to isolate JSON if model adds any prefix/suffix accidentally
    match = re.search(r'\{[\s\S]*\}', raw_output)
    if match:
        raw_output = match.group(0)

    try:
        parsed = json.loads(raw_output)
        # Ensure all requested fields exist
        for f in field:
            if f["fieldName"] not in parsed:
                parsed[f["fieldName"]] = None
        extracted_values.append(parsed)
    except Exception as e:
        print(f"⚠️ JSON parse error for {key}: {e}")
        extracted_values.append({"raw": raw_output})

    return extracted_values

def extract_table_fields_with_llm(structured_tables: str, table_fields: list[dict], agent, context=None):
    """
    Extract values for specific tables and fields from structured markdown tables using an LLM.
    
    - structured_tables: string containing multiple markdown-style tables.
    - table_fields: list of dicts describing what to extract.
    - agent: LLM agent instance (e.g., AzureLLMAgent).
    """

    # --- Step 1: Group fields by table name ---
    tables_map = defaultdict(list)
    for f in table_fields:
        tables_map[f["tableName"]].append(f)

    all_results = {}

    # --- Step 2: Loop over each table schema ---
    for table_name, fields in tables_map.items():
        field_instructions = [
            f"- **{f['fieldName']}** ({f['fieldDatatype']}): {f['fieldDescription']}"
            for f in fields
        ]
        fields_text = "\n".join(field_instructions)

        prompt = f"""
You are a data extraction expert.

Given the following structured tables, extract information for the table named **"{table_name}"**.

---
### 📊 Structured Tables:
{structured_tables}

---
### 🧩 Fields to Extract for "{table_name}":
{fields_text}

---
### 🧾 Output Format (JSON):
Return **only valid JSON**, no markdown or commentary.

If multiple rows exist, return them as a list of JSON objects — one per row.

If a column or value is not found, use null for that field.

Example:
{{
  "{table_name}": [
    {{
      {', '.join([f'"{f["fieldName"]}": "value or null"' for f in fields])}
    }},
    ...
  ]
}}

Now, extract the JSON for table "{table_name}" only.
""".strip()

        # --- Step 3: Run LLM ---
        try:
            value = agent.complete(prompt, context=context) if context else agent.complete(prompt)
        except Exception as e:
            print(f"⚠️ LLM extraction failed for table {table_name}: {e}")
            all_results[table_name] = "NA"
            continue

        # --- Step 4: Clean and safely parse JSON ---
        raw_output = value.strip()
        match = re.search(r'\{[\s\S]*\}', raw_output)
        if match:
            raw_output = match.group(0)

        try:
            parsed = json.loads(raw_output)
            # Ensure all required field keys exist
            if table_name in parsed:
                for row in parsed[table_name]:
                    for f in fields:
                        if f["fieldName"] not in row:
                            row[f["fieldName"]] = None
            else:
                # If LLM returned rows directly instead of wrapped in table_name
                parsed = {table_name: parsed if isinstance(parsed, list) else [parsed]}
            all_results[table_name] = parsed[table_name]
        except Exception as e:
            print(f"⚠️ JSON parse error for table {table_name}: {e}")
            all_results[table_name] = {"raw": raw_output}

    return all_results

# ======================================================
# 🧾 Update Extracted Values to MongoDB
# ======================================================
def normalize_to_nested(value):
    """
    Normalize any value into the format [["x"]] or [["NA"]].
    """
    if value is None:
        return [["NA"]]
    if isinstance(value, list):
        # already a list, ensure nested [["x"]] pattern
        normalized = []
        for v in value:
            if isinstance(v, list):
                normalized.append([str(x) for x in v])
            else:
                normalized.append([str(v)])
        return normalized
    else:
        return [[str(value)]]


from datetime import datetime, timezone
from bson import ObjectId
import json

def update_extraction_results_to_mongo(user_id, cluster_id, doc_id, field_results=None, table_results=None, full_text=None):
    """
    ✅ Combined function to store both field_results and table_results into MongoDB.
    Handles None values gracefully for field_results and table_results.

    Args:
        user_id (str)
        cluster_id (str)
        doc_id (str)
        field_results (list[dict] | None)
        table_results (dict | None)
        full_text (str | dict | None)
    """

    collection = get_mongo_collection("tb_file_details")
    filter_query = {"_id": ObjectId(doc_id)}

    # -------------------------------
    # 🧩 Normalize field results
    # -------------------------------
    merged_fields = {}
    if field_results:
        for result in field_results:
            merged_fields.update(result or {})

    normalized_fields = {
        field_name: str(value).strip() if value is not None else ""
        for field_name, value in merged_fields.items()
    } if merged_fields else None

    # -------------------------------
    # 📊 Normalize table results
    # -------------------------------
    normalized_tables = {}
    if table_results:
        for table_name, rows in table_results.items():
            items = []
            for row in rows:
                normalized_row = {field_name: str(value).strip() for field_name, value in row.items()}
                items.append(normalized_row)
            normalized_tables[table_name] = {
                                "fieldType": "table",
                                "items": items
                            }
    else:
        normalized_tables = None

    # -------------------------------
    # 🔄 Combine both into single list
    # -------------------------------
    combined_values = {}
    if normalized_fields is not None:
        combined_values.update(normalized_fields)
    if normalized_tables is not None:
        combined_values.update(normalized_tables)

    # -------------------------------
    # 🗄️ Prepare MongoDB update
    # -------------------------------
    if not isinstance(full_text, str):
        try:
            full_text = json.dumps(full_text, ensure_ascii=False)
        except Exception:
            full_text = str(full_text)
    print('***mongo storage****')
    print(combined_values)
    print('***')
    update_query = {
        "$set": {
            "extractedValues": combined_values or None,
            "updatedExtractedValues": combined_values or None,
            "processingStatus": "Completed",
            "extractedText": full_text or "",
            "updatedAt": datetime.now(timezone.utc),
        }
    }

    result = collection.update_one(filter_query, update_query, upsert=True)
    print("✅ Combined field + table results stored successfully in MongoDB.")

    return {
        "status": "success" if result.modified_count > 0 else "no-change",
        "storedData": combined_values or None,
    }


def update_extraction_results_to_mongo_single(user_id, cluster_id, doc_id, final_result, full_text=None):
    """
    Stores unified final_result (from extract_all_fields_and_tables)
    as a SINGLE combined JSON structure under extractedValues.
    """

    collection = get_mongo_collection("tb_file_details")
    filter_query = {"_id": ObjectId(doc_id)}

    # -------------------------------
    # 🧩 Normalize single final_result JSON
    # -------------------------------
    normalized_results = {}

    for key, value in final_result.items():
        # fieldType: field → store direct value
        if isinstance(value, str) or value is None:
            normalized_results[key] = value or ""

        # fieldType: table → items list
        elif isinstance(value, dict) and value.get("fieldType") == "table":
            items = []
            for row in value.get("items", []):
                normalized_row = {k: (str(v).strip() if v is not None else "") for k, v in row.items()}
                items.append(normalized_row)

            normalized_results[key] = {
                "fieldType": "table",
                "items": items
            }

        # unexpected formats → convert to string safely
        else:
            normalized_results[key] = json.dumps(value)

    # -------------------------------
    # 🔄 Convert full_content to text
    # -------------------------------
    if not isinstance(full_text, str):
        try:
            full_text = json.dumps(full_text, ensure_ascii=False)
        except Exception:
            full_text = str(full_text)

    print("****** Unified Mongo Store ******")
    print(normalized_results)
    print("********************************")

    update_query = {
        "$set": {
            "extractedValues": normalized_results,
            "updatedExtractedValues": normalized_results,
            "processingStatus": "Completed",
            "extractedText": full_text or "",
            "updatedAt": datetime.now(timezone.utc)
        }
    }

    result = collection.update_one(filter_query, update_query, upsert=True)
    print("✅ Unified extractor results stored successfully in MongoDB.")

    return {
        "status": "success" if result.modified_count > 0 else "no-change",
        "storedData": normalized_results
    }

# ======================================================
# 💳 Credit Management
# ======================================================
mongo_client = MongoClient(MONGO_URI)

mongo_database = os.getenv("DB_NAME")
db = mongo_client[mongo_database]

# Direct collection access (no helper function)
tb_credits = db["tb_credits"]

tb_file_details=db["tb_file_details"]


# ---------------------------------------------------------
# 1️⃣ UPDATE CREDIT RECORD (Replace old insert_debit_credit)
# ---------------------------------------------------------

def update_debit_credit(
    user_id, 
    cluster_id, 
    file_id, 
    credits_to_deduct, 
    job_id, 
    credit_id,
    message="Success"     # <-- added optional parameter for successMessage
):
    """
    Updates the credit record ('type' and 'updatedAt') using creditId.
    Also updates file details after successful debit update.
    """

    if not credit_id:
        raise ValueError("creditId is required but missing")

    try:
        # ---------------- OLD LOGIC (unchanged) ----------------
        result = tb_credits.update_one(
            {"_id": ObjectId(credit_id)},
            {
                "$set": {
                    "updatedAt": datetime.now(timezone.utc),
                    "type": "debited"
                }
            }
        )

        if result.matched_count == 0:
            raise LookupError(f"No credit record found for creditId={credit_id}")

        print(f"✅ Updated credit record {credit_id}")

        # ---------------- NEW FILE DETAILS UPDATE ----------------
        if file_id:
            file_update_result = tb_file_details.update_one(
                {"_id": ObjectId(file_id)},
                {
                    "$set": {
                        "processingStatus": "Completed",
                        "successMessage": message,
                        "updatedAt": datetime.utcnow().isoformat() + "Z"
                    }
                }
            )

            print(f"✅ Marked SUCCESS for file {file_id}, modified: {file_update_result.modified_count}")
        else:
            print("⚠️ file_id is missing, skipping file details update.")

        return {
            "status": "success",
            "creditId": credit_id,
            "fileId": file_id
        }

    except Exception as e:
        print(f"❌ Error updating creditId={credit_id}: {e}")
        traceback.print_exc()
        return {"status": "error", "message": str(e)}


# ---------------------------------------------------------
# 2️⃣ DELETE CREDIT RECORD
# ---------------------------------------------------------

def delete_credit_record(credit_id, file_id=None):
    """
    Deletes a credit record by creditId.
    Also updates file details status to 'Failed' if file_id is provided.
    Raises errors when creditId missing or not found.
    """

    if not credit_id:
        raise ValueError("creditId is required but missing")

    try:
        # ---------- OLD LOGIC (unchanged) ----------
        result = tb_credits.update_one(
            {"_id": ObjectId(credit_id)},
            {
                "$set": {
                    "status": "0",
                    "updatedAt": datetime.utcnow().isoformat() + "Z"
                }
            }
        )

        if result.matched_count == 0:
            raise LookupError(f"No credit record found for creditId={credit_id}")

        print(f"🗑️ Soft-deleted credit record {credit_id} (status set to 0)")

        # ---------- NEW FILE-DETAILS UPDATE ----------
        if file_id:
            file_update_result = tb_file_details.update_one(
                {"_id": ObjectId(file_id)},
                {
                    "$set": {
                        "processingStatus": "Failed",
                        "updatedAt": datetime.utcnow().isoformat() + "Z"
                    }
                }
            )

            print(f"⚠️ Marked FAILED for file {file_id}, modified: {file_update_result.modified_count}")
        else:
            print("⚠️ file_id not provided, skipping file update.")

        return {
            "status": "success",
            "creditId": credit_id,
            "fileId": file_id
        }

    except Exception as e:
        print(f"❌ Error deleting creditId={credit_id}: {e}")
        traceback.print_exc()
        return {"status": 'error', "message": str(e)}

