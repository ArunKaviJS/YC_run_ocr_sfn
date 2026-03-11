import os
import asyncio
from chunkr_ai import Chunkr
from chunkr_ai.models import Configuration, LlmProcessing, FallbackStrategy, SegmentationStrategy, ErrorHandlingStrategy
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError

load_dotenv()
CHUNKR_API_KEY = os.getenv("CHUNKR_API_KEY")

# -------------------------
# Helper functions
# -------------------------
def extract_text_from_chunk(chunk_obj):
    """
    Extract clean text from a Chunkr chunk, avoiding duplicate text from segments and content.
    """
    # Prefer the highest-level textual content available
    if hasattr(chunk_obj, "llm") and chunk_obj.llm:
        chunk_text = chunk_obj.llm
    elif hasattr(chunk_obj, "content") and chunk_obj.content:
        chunk_text = chunk_obj.content
    elif hasattr(chunk_obj, "segments") and chunk_obj.segments:
        segment_texts = [seg.content for seg in chunk_obj.segments if hasattr(seg, "content") and seg.content]
        chunk_text = "\n".join(segment_texts)
    elif hasattr(chunk_obj, "html") and chunk_obj.html:
        chunk_text = chunk_obj.html
    else:
        chunk_text = ""

    # Strip HTML tags
    if "<" in chunk_text and ">" in chunk_text:
        soup = BeautifulSoup(chunk_text, "html.parser")
        chunk_text = soup.get_text(separator="\n")

    return chunk_text.strip()



async def process_file_async(filepath):
    client = Chunkr(api_key=CHUNKR_API_KEY)
    config = Configuration(
        segmentation_strategy=SegmentationStrategy.PAGE,
        error_handling=ErrorHandlingStrategy.CONTINUE,
        llm_processing=LlmProcessing(
            llm_model_id="gemini-pro-2.5",
            fallback_strategy=FallbackStrategy.none(),
            temperature=0.0
        ),
        expires_in=3600,
        segment_processing={
            "page": {
                "crop_image": "All",
                "format": "Markdown",
                "strategy": "LLM",
                "extended_context": True,
                "description": True,
            },
        }
    )
    print(f"🔹 Uploading file to Chunkr: {filepath}")
    task = await client.upload(filepath, config)
    print("🔹 File uploaded, polling for result...")
    task = await task.poll()
    
    chunks = task.output.chunks if hasattr(task.output, "chunks") else []
    print(f"🔹 Total chunks found: {len(chunks)}")

    page_texts = []

    for i, c in enumerate(chunks, start=1):
        page_text = extract_text_from_chunk(c)
        if page_text.strip():
            labeled_page = f"=====  CHUNKR PAGE NUMBER {i} =====\n{page_text.strip()}"
            page_texts.append(labeled_page)

    # remove exact duplicates while preserving order
    page_texts = list(dict.fromkeys(page_texts))
    all_text = "\n\n".join(page_texts)
    page_count = len(page_texts)
    
    await client.close()
    return all_text, page_count