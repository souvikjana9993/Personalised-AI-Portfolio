from dotenv import load_dotenv
import os
import json
import time
import logging
from pathlib import Path
from llama_parse import LlamaParse
from llama_index.core import VectorStoreIndex, Settings
from llama_index.llms.gemini import Gemini
from llama_index.embeddings.gemini import GeminiEmbedding
from datetime import datetime
import asyncio

def setup_logger():
    """Configure logging"""
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'nps_parser_{timestamp}.log'

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    return logging.getLogger('NPS_Parser')

def get_email_from_path(path):
    """Extract email from the path"""
    logger = logging.getLogger('NPS_Parser')
    try:
        # Path format: data/nps/email@domain.com/transactions/
        parts = Path(path).parts
        nps_idx = parts.index('nps')
        if nps_idx + 1 < len(parts):
            email = parts[nps_idx + 1]
            logger.info(f"Extracted email from path: {email}")
            return email
    except Exception as e:
        logger.error(f"Failed to extract email from path {path}: {str(e)}", exc_info=True)
    return None

def create_output_directory(input_path):
    """Create corresponding output directory structure"""
    logger = logging.getLogger('NPS_Parser')
    try:
        # Get email from path
        email = get_email_from_path(input_path)
        if not email:
            raise ValueError(f"Could not extract email from path: {input_path}")

        # Create output path: data/nps/email@domain.com/processed/
        base_path = Path("data/nps") / email / "processed"
        base_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"Created output directory for {email}: {base_path}")
        return base_path
    except Exception as e:
        logger.error(f"Failed to create output directory for {input_path}: {str(e)}", exc_info=True)
        raise

async def setup_gemini():
    """Initialize Gemini LLM and embedding models."""
    logger = logging.getLogger('NPS_Parser')
    try:
        logger.info("Setting up Gemini models")
        llm = Gemini(
            api_key=os.getenv('GOOGLE_API_KEY'),
            model_name="models/gemini-pro"
        )
        embed_model = GeminiEmbedding(
            api_key=os.getenv('GOOGLE_API_KEY'),
            model_name="models/embedding-001"
        )
        Settings.llm = llm
        Settings.embed_model = embed_model
        logger.info("Successfully initialized Gemini models")
        return llm, embed_model
    except Exception as e:
        logger.error(f"Failed to setup Gemini models: {str(e)}", exc_info=True)
        raise

async def parse_pdf(input_file, output_dir, parser):
    """Parse a single PDF file asynchronously."""
    logger = logging.getLogger('NPS_Parser')
    parsed_file = output_dir / f'{input_file.stem}_parsed.md'

    if parsed_file.exists():
        logger.info(f"Parsed file already exists: {parsed_file}. Skipping parsing.")
        return []

    try:
        logger.info(f"Starting to parse PDF: {input_file}")
        documents = await parser.aload_data(input_file)

        if documents:
            with open(parsed_file, 'w') as f:
                f.write(documents[0].text)
            logger.info(f"Successfully parsed PDF and saved to: {parsed_file}")
            return documents
        else:
            logger.error(f"Failed to load file {input_file}. No documents parsed.")
            return []

    except Exception as e:
        logger.error(f"Error parsing {input_file}: {str(e)}", exc_info=True)
        return []

def extract_investment_summary(query_engine, output_dir, pdf_file):
    """Extract investment summary and scheme-wise details."""
    logger = logging.getLogger('NPS_Parser')
    summary_file = output_dir / f'{pdf_file.stem}_summary.json'

    if summary_file.exists():
        logger.info(f"Summary file already exists: {summary_file}. Skipping extraction.")
        return None

    try:
        logger.info("Starting investment summary extraction")

        summary_queries = {
            "value_of_holdings": "What is the Value of your Holdings (Investments) amount?",
            "total_contribution": "What is the Total Contribution amount?",
            "as_on_date":"What is the as on date in dd-mm-yyyy for Total Contribution amount value?",
            "total_withdrawal": "What is the Total Withdrawal amount?",
            "total_notional_gain": "What is the Total Notional Gain/Loss amount?",
            "withdrawal_deduction": "What is the Withdrawal/deduction in units towards intermediary charges amount?",
            "return_on_investment_xirr": "What is the Return on Investment XIRR percentage?",
            "return_on_investment": "What is the Return on Investment percentage for the selected period?"
        }

        scheme_queries = {
            "scheme_E": {
                "value": "What is the Value of Holdings for HDFC PENSION MANAGEMENT COMPANY LIMITED SCHEME E?",
                "total_units": "What is the Total Units for HDFC PENSION MANAGEMENT COMPANY LIMITED SCHEME E?",
                "nav": "What is the NAV for HDFC PENSION MANAGEMENT COMPANY LIMITED SCHEME E?"
            },
            "scheme_C": {
                "value": "What is the Value of Holdings for HDFC PENSION MANAGEMENT COMPANY LIMITED SCHEME C?",
                "total_units": "What is the Total Units for HDFC PENSION MANAGEMENT COMPANY LIMITED SCHEME C?",
                "nav": "What is the NAV for HDFC PENSION MANAGEMENT COMPANY LIMITED SCHEME C?"
            },
            "scheme_G": {
                "value": "What is the Value of Holdings for HDFC PENSION MANAGEMENT COMPANY LIMITED SCHEME G?",
                "total_units": "What is the Total Units for HDFC PENSION MANAGEMENT COMPANY LIMITED SCHEME G?",
                "nav": "What is the NAV for HDFC PENSION MANAGEMENT COMPANY LIMITED SCHEME G?"
            }
        }

        summary = {}
        for key, query in summary_queries.items():
            logger.debug(f"Executing query for {key}")
            response = query_engine.query(query)
            summary[key] = str(response)
            time.sleep(6) # Wait for 6 seconds after each query

        schemes_summary = {}
        for scheme_key, queries in scheme_queries.items():
            logger.debug(f"Processing scheme: {scheme_key}")
            scheme_data = {}
            for key, query in queries.items():
                logger.debug(f"Executing query for {scheme_key}.{key}")
                response = query_engine.query(query)
                scheme_data[key] = str(response)
                time.sleep(6)  # Wait for 6 seconds after each query
            schemes_summary[scheme_key] = scheme_data

        final_summary = {
            "investment_summary": summary,
            "scheme_wise_summary": schemes_summary
        }

        with open(summary_file, 'w') as f:
            json.dump(final_summary, f, indent=4)
        logger.info(f"Saved summary to: {summary_file}")

        logger.info("Successfully completed investment summary extraction")
        return final_summary
    except Exception as e:
        logger.error(f"Error extracting investment summary: {str(e)}", exc_info=True)
        raise

async def process_single_pdf(pdf_file, parser, llm, embed_model):
    """Processes a single PDF file."""
    logger = logging.getLogger('NPS_Parser')
    logger.info(f"\n{'='*50}\nProcessing {pdf_file}\n{'='*50}")

    try:
        output_dir = create_output_directory(pdf_file.parent)
        documents = await parse_pdf(pdf_file, output_dir, parser)

        if not documents:
            logger.warning(f"Skipping {pdf_file} due to parsing error")
            return

        logger.info("Creating vector index")
        index = VectorStoreIndex.from_documents(
            documents, llm=llm, embed_model=embed_model
        )
        query_engine = index.as_query_engine()

        extract_investment_summary(query_engine, output_dir, pdf_file)

    except Exception as e:
        logger.error(f"Error processing {pdf_file}: {str(e)}", exc_info=True)

async def main():
    logger = setup_logger()

    try:
        logger.info("Starting NPS PDF processing pipeline")
        load_dotenv()
        logger.info("Loaded environment variables")

        base_dir = Path("data/nps")
        pdf_paths = []
        for email_dir in base_dir.glob("*"):
            if email_dir.is_dir():
                transaction_dir = email_dir / "transactions"
                if transaction_dir.exists():
                    pdfs = list(transaction_dir.glob("*.pdf"))
                    pdf_paths.extend(pdfs)
                    logger.info(f"Found {len(pdfs)} PDFs in {email_dir}")

        logger.info(f"Total PDFs found: {len(pdf_paths)}")

        logger.info("Initializing LlamaParse")
        parser = LlamaParse(
            result_type="markdown",
            parsing_instruction="""The document is a holdings report for NPS investments. It generally will have tables. I need data in 3 tables, Investment Summary, 
            Investment Details Scheme wise summary and Contribution/Redemption Details during the selected period. Keep the tabular data clean and structured so that 
            I can extract it easily""",
        )

        llm, embed_model = await setup_gemini()

        tasks = [
            process_single_pdf(pdf_file, parser, llm, embed_model)
            for pdf_file in pdf_paths
        ]
        await asyncio.gather(*tasks)

        logger.info("Completed processing all PDF files")

    except Exception as e:
        logger.error(f"Fatal error in main process: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(main())