from dotenv import load_dotenv
import os
import json
import time
from pathlib import Path
from llama_parse import LlamaParse
from llama_index.core import VectorStoreIndex, Settings
from llama_index.llms.gemini import Gemini
from llama_index.embeddings.gemini import GeminiEmbedding
from datetime import datetime
import asyncio
import re

def get_email_from_path(path):
    """Extract email from the path"""
    # Path format: data/equity/email@domain.com/contract_notes/
    parts = Path(path).parts
    try:
        eq_idx = parts.index('equity')
        if eq_idx + 1 < len(parts):
            email = parts[eq_idx + 1]
            return email
    except ValueError:
        # Try getting email from parent directory if path structure is different
        if isinstance(path, Path):
            try:
                return path.parent.parent.name
            except:
                return None
    return None

def create_output_directory(input_path):
    """Create corresponding output directory structure"""
    # Get email from path
    email = get_email_from_path(input_path)
    if not email:
        raise ValueError(f"Could not extract email from path: {input_path}")

    # Create output path: data/equity/email@domain.com/processed/
    base_path = Path("data/equity") / email / "processed"
    base_path.mkdir(parents=True, exist_ok=True)
    return base_path

async def setup_gemini():
    """Initialize Gemini LLM and embedding models."""
    llm = Gemini(
        api_key=os.getenv("GOOGLE_API_KEY"), model_name="models/gemini-pro"
    )
    embed_model = GeminiEmbedding(
        api_key=os.getenv("GOOGLE_API_KEY"), model_name="models/embedding-001"
    )
    Settings.llm = llm
    Settings.embed_model = embed_model

    return llm, embed_model

async def parse_pdf(input_file, output_dir, parser):
    """Parse a single PDF file asynchronously."""
    parsed_file = output_dir / f"{input_file.stem}_parsed.md"

    if parsed_file.exists():
        return []
    try:
        documents = await parser.aload_data(input_file)
        if documents:
            full_text = "\n".join([doc.text for doc in documents])
            with open(parsed_file, "w") as f:
                f.write(full_text)
                return documents
        else:
            return []

    except Exception as e:
        return []

def clean_json(json_string):
    """
    Loads a JSON string (if it's a string), removes null values,
    and handles any nested JSON strings within.

    Args:
        json_string (str or dict or list): The JSON data, which can be a
            string to be parsed or a pre-parsed dictionary or list.

    Returns:
        dict or list: The cleaned JSON data with null values removed.
    """
    if isinstance(json_string, str):
        try:
            data = json.loads(json_string)
        except:
            return json_string  # Return as is if not a valid JSON string
    else:
        data = json_string

    if isinstance(data, dict):
        cleaned_data = {}
        for k, v in data.items():
            if v is not None:
                cleaned_value = clean_json(v)  # Recursive call for nested structures
                if cleaned_value is not None:
                    cleaned_data[k] = cleaned_value
        return cleaned_data if cleaned_data else None

    elif isinstance(data, list):
        cleaned_data = []
        for item in data:
            cleaned_item = clean_json(item)  # Recursive call for nested structures
            if cleaned_item is not None:
                cleaned_data.append(cleaned_item)
        return cleaned_data if cleaned_data else None

    else:
        return data

def clean_investment_summary(summary_json):
    """
    Cleans the investment summary JSON by:
    1. Removing unnecessary text from 'trade_date'.
    2. Formatting 'buy_details' and 'sell_details' as proper JSON strings.
    3. Formatting 'pay_obligation' as a proper JSON string.
    4. Removing null values.

    Args:
        summary_json (dict): The investment summary JSON.

    Returns:
        dict: The cleaned investment summary JSON.
    """

    cleaned_summary = {}
    if "investment_summary" in summary_json:
        inv_summary = summary_json["investment_summary"]
        cleaned_summary["investment_summary"] = {}

        # 1. Clean 'trade_date'
        cleaned_summary["investment_summary"]["trade_date"] = inv_summary.get(
            "trade_date", ""
        ).replace("The provided context does not mention the Trade Date.", "")

        # 2. Clean 'UCC' (just copy it as is)
        cleaned_summary["investment_summary"]["UCC"] = inv_summary.get("UCC", "")

        # 3. Clean 'buy_details' and 'sell_details'
        for key in ["buy_details", "sell_details"]:
            if key in inv_summary:
                try:
                    # Replace escaped ₹ and remove newlines, assuming JSON-like structure within a string
                    cleaned_json_str = (
                        inv_summary[key].replace("\\u20b9", "₹").replace("\n", "")
                    )
                    # Parse the cleaned string as JSON and remove null values recursively
                    cleaned_summary["investment_summary"][key] = clean_json(
                        cleaned_json_str
                    )
                except:
                    # If parsing fails, keep the original string
                    cleaned_summary["investment_summary"][key] = inv_summary[key]

        # 4. Clean 'pay_obligation'
        if "pay_obligation" in inv_summary:
            try:
                # Remove ```json and ``` and newlines, assuming JSON-like structure within a string
                cleaned_json_str = (
                    inv_summary["pay_obligation"]
                    .replace("```json", "")
                    .replace("```", "")
                    .replace("\n", "")
                )
                # Parse the cleaned string as JSON and remove null values recursively
                cleaned_summary["investment_summary"]["pay_obligation"] = clean_json(
                    cleaned_json_str
                )
            except:
                # If parsing fails, keep the original string
                cleaned_summary["investment_summary"]["pay_obligation"] = inv_summary[
                    "pay_obligation"
                ]

    return cleaned_summary

def extract_investment_summary(query_engine, output_dir, pdf_file):
    """Extract investment summary and scheme-wise details."""
    summary_file = output_dir / f"{pdf_file.stem}_summary.json"

    if summary_file.exists():
        return None

    summary_queries = {
        "trade_date": "What is the Trade Date mentioned in the document, search for Trade Date: <date>?",
        "UCC": "What is the UCC (Unique Client Code) mentioned in the document?",
        "buy_details": """
                        Provide a nested JSON containing all buy details found under any table with 'Equity' as header across all pages. 
                        Include 'Security / Contract Description', 'Quantity', 'Gross Rate/ Trade Price Per unit(₹)', 
                        'Brokerage per unit(₹)', 'Net rate per unit(₹)', and 'Net Total (Before Levies) (₹)' for each buy 
                        transaction (where Buy(B) / Sell(S) is 'B'). Remove any '/n' characters from the output.
                        """,
        "sell_details": """
                        Provide a nested JSON containing all sell details found under any table with 'Equity' as header across all pages. 
                        Include 'Security / Contract Description', 'Quantity', 'Gross Rate/ Trade Price Per unit(₹)', 
                        'Brokerage per unit(₹)', 'Net rate per unit(₹)', and 'Net Total (Before Levies) (₹)' for each sell 
                        transaction (where Buy(B) / Sell(S) is 'S'). Remove any '/n' characters from the output.
                        """,
        "pay_obligation": """
                        Provide a nested JSON containing the row labels and the 'NET TOTAL' column from the table that includes 
                        the rows 'Pay in/Pay out obligation' and 'Net amount receivable/(payable by client)'. Exclude columns 
                        'Equity' and 'Futures and Options' if present. Include all rows in the table. Alsoalculate the sum of all values in the 'NET TOTAL' 
                        column except for the 'Pay in/Pay out obligation' row, and name this sum 'additional expenses'. Remove any '/n' characters from the output.
                        """,
    }

    summary = {}
    for key, query in summary_queries.items():
        response = query_engine.query(query)
        summary[key] = str(response)
        time.sleep(6)  # Be mindful of rate limits

    final_summary = {"investment_summary": summary}

    # Clean the summary using the cleaning function
    cleaned_final_summary = clean_investment_summary(final_summary)

    with open(summary_file, "w") as f:
        json.dump(cleaned_final_summary, f, indent=4)  # Save the cleaned summary

    return cleaned_final_summary

async def process_single_pdf(pdf_file, parser, llm, embed_model):
    """Processes a single PDF file."""
    output_dir = create_output_directory(pdf_file.parent)
    documents = await parse_pdf(pdf_file, output_dir, parser)

    if not documents:
        return

    index = VectorStoreIndex.from_documents(documents, llm=llm, embed_model=embed_model)
    query_engine = index.as_query_engine()

    extract_investment_summary(query_engine, output_dir, pdf_file)

async def main():
    load_dotenv()
    base_dir = Path("data/equity")
    for email_dir in base_dir.glob("*"):
        if not email_dir.is_dir():
            continue

        contract_notes_dir = email_dir / "contract_notes"
        if not contract_notes_dir.exists():
            continue

        pdf_paths = list(contract_notes_dir.glob("*.pdf"))

        if not pdf_paths:
            continue

        parser = LlamaParse(
            result_type="markdown",
            # parsing_instruction="""The document is a transaction statement for stock transactions containing tables which need to be extracted""",
            # premium_mode=True,
            is_formatting_instruction=False,
            invalidate_cache=True,
            # do_not_cache=True,
            verbose=True,
        )
        llm, embed_model = await setup_gemini()

        tasks = [
            process_single_pdf(pdf_file, parser, llm, embed_model)
            for pdf_file in pdf_paths
        ]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())