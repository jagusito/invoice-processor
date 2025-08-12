# parsers/headers/digital_realty_header.py
"""
Digital Realty Header Router - FIXED VERSION
Routes to appropriate USA (Telx) or UK (Digital London) header parser
NO FALLBACKS - must clearly identify branch or return empty
"""

import os
import logging
from parsers.headers import digital_realty_usa_header, digital_realty_uk_header

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_header(pdf_path: str):
    """
    Route to appropriate Digital Realty header parser based on filename
    NO FALLBACKS - must clearly identify branch
    """
    filename = os.path.basename(pdf_path).lower()
    
    # UK branch detection
    if 'interxion' in filename:
        logger.info("🇬🇧 Routing to Digital London Ltd (UK) header parser")
        return digital_realty_uk_header.extract_header(pdf_path)
    
    # USA branch detection
    elif 'digitalrealty' in filename:
        logger.info("🇺🇸 Routing to Telx - New York, LLC (USA) header parser")
        return digital_realty_usa_header.extract_header(pdf_path)
    
    # Content-based detection as secondary method
    else:
        logger.info("🔍 Filename unclear, checking invoice content...")
        try:
            import fitz
            doc = fitz.open(pdf_path)
            first_page_text = doc[0].get_text()
            doc.close()
            
            # Check for UK indicators
            uk_indicators = ['Digital London', 'Interxion', 'GBP', 'United Kingdom', 'UK']
            usa_indicators = ['Telx', 'New York', 'USD', 'United States', 'USA']
            
            uk_score = sum(1 for indicator in uk_indicators if indicator in first_page_text)
            usa_score = sum(1 for indicator in usa_indicators if indicator in first_page_text)
            
            if uk_score > usa_score:
                logger.info("🇬🇧 Content indicates UK branch - routing to Digital London Ltd")
                return digital_realty_uk_header.extract_header(pdf_path)
            elif usa_score > uk_score:
                logger.info("🇺🇸 Content indicates USA branch - routing to Telx - New York, LLC")
                return digital_realty_usa_header.extract_header(pdf_path)
            else:
                logger.warning("⚠️ Cannot determine Digital Realty branch from filename or content")
                return pd.DataFrame()  # Return empty instead of fallback
        
        except Exception as e:
            logger.error(f"❌ Error during content-based branch detection: {e}")
            return pd.DataFrame()  # Return empty instead of fallback