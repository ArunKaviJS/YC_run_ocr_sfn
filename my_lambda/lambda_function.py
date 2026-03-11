import os
import json
import traceback
from dotenv import load_dotenv
from bson import ObjectId

from textract_service import run_claude, get_random_textract_client
from mongo import mark_file_as_failed, update_job_status,get_mongo_collection,update_debit_credit,delete_credit_record

load_dotenv()
S3_BUCKET_NAME='yellow-checks-test'

def get_original_filename_from_mongo(file_id: str) -> str:
    """
    Fetch original filename from MongoDB using file_id.
    """
    collection = get_mongo_collection("tb_file_details")
    doc = collection.find_one({"_id": ObjectId(file_id)})

    if not doc:
        raise ValueError(f"No file found in MongoDB for file_id: {file_id}")

    return doc.get("originalS3File")  # or doc["originalS3File"]

def lambda_handler(event, context):
    """
    Run Chunkr text extraction (Mongo-safe orchestration).
    Expected Input:
        {
            "userId": "...",
            "clusterId": "...",
            "fileId": "...",
            "creditId": "...",
            "fileName": "document.pdf",
            "jobId": "optional"
        }
    """
    try:
        print("📥 Incoming event:", json.dumps(event, indent=2))

        # --- Required fields ---
        user_id = event["userId"]
        cluster_id = event["clusterId"]
        file_id = event["fileId"]
        credit_id = event.get("creditId")
        filename = get_original_filename_from_mongo(file_id)
        region="ap-south-1"
        bucket = event.get("bucket", S3_BUCKET_NAME)
        print(bucket)
        file_oid = ObjectId(file_id)
        user_oid = ObjectId(user_id)
        cluster_oid = ObjectId(cluster_id)
        credit_oid = ObjectId(credit_id)

        # --- Build S3 key ---
        s3_key = f"{user_id}/{cluster_id}/raw/{filename}"
        print(f"🔹 S3 Key: {s3_key}")

        textract, textract_region, temp_bucket = get_random_textract_client()

        print(temp_bucket)

        # --- Get random region + temp bucket ---
        

        # --- Run Chunkr (instead of Textract) ---
        try:
            print("Running Claude extraction...")
            extraction_result = run_claude(bucket, s3_key,file_id, region)
            print("Claude extraction...",extraction_result)
        except Exception as e:
            if credit_id:
                print(f"💳 Deleting credit record {credit_id} due to MongoDB update failure")
                delete_credit_record(credit_oid,file_oid)
            print("❌ Exception in RunChunkr:", str(e))

            
        # --- Validate output ---
        if not extraction_result :
            print("❌ run_chunkr returned unexpected result; aborting.")
            if credit_id:
                print(f"💳 Deleting credit record {credit_id} due to MongoDB update failure")
                delete_credit_record(credit_oid,file_oid)
                
            mark_file_as_failed(file_id)
            #update_job_status(job_id, status="error", message="Chunkr extraction failed")
            return {
                **event,
                "status": "Failed",
                "creditId": credit_id,
                "clusterId": cluster_id,
                "userId": user_id,
                "fileId": file_id,
                "filename": filename,
                "normalized_data": None,
            }

        # --- Extract details ---
        

        #print(f"✅ Chunkr completed successfully — {page_count} pages processed.")

        # --- Return consistent payload ---
        return {
            **event,
            "status": "success",
            "creditId": credit_id,
            "clusterId": cluster_id,
            "userId": user_id,
            "fileId": file_id,
            "filename": filename,
            
            "normalized_data": extraction_result,
        }

    except Exception as e:
        print("❌ Exception in RunChunkr:", str(e))
        traceback.print_exc()
        mark_file_as_failed(event.get("fileId"))
        if credit_id:
                print(f"💳 Deleting credit record {credit_id} due to MongoDB update failure")
                delete_credit_record(credit_oid,file_oid)
                
        #update_job_status(event.get("jobId"), status="error", message=str(e))
        return {
                **event,
                "status": "Failed",
                "creditId": credit_id,
                "clusterId": cluster_id,
                "userId": user_id,
                "fileId": file_id,
                "filename": filename,
                "normalized_data": None,
            }
