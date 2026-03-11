import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# AWS configurations
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "yellow-checks-test")

# MongoDB configurations
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
FILE_DETAILS_COLLECTION = os.getenv("FILE_DETAILS")
REQUESTED_FIELDS_COLLECTION = os.getenv("REQUESTED_FIELDS")
CREDIT_COLLECTION = os.getenv("CREDIT")

# Azure OpenAI configurations
AZURE_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION")
AZURE_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")

