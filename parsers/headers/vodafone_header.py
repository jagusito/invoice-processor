# parsers/headers/vodafone_header.py
"""
Vodafone Multi-Branch Header Router
Routes to appropriate branch header parser based on filename and content
"""

import os
import fitz
import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_header(pdf_path: str) -> pd.DataFrame:
    """
    Route to appropriate Vodafone branch header parser
    
    Args:
        pdf_path: Path to Vodafone PDF invoice
        
    Returns:
        DataFrame with header information from appropriate branch parser
    """
    try:
        filename = os.path.basename(pdf_path).lower()
        logger.info(f"🔄 Routing Vodafone header parser for: {filename}")
        
        # Method 1: Filename-based routing (fastest)
        if 'uk' in filename or 'britain' in filename:
            logger.info("🇬🇧 Routing to Vodafone UK header parser (filename)")
            return route_to_uk_parser(pdf_path)
        elif 'png' in filename or 'papua' in filename:
            logger.info("🇵🇬 Routing to Vodafone PNG header parser (filename)")
            return route_to_png_parser(pdf_path)
        
        # Method 2: Content-based routing (fallback)
        try:
            doc = fitz.open(pdf_path)
            first_page_text = doc[0].get_text()
            doc.close()
            
            # Check for UK-specific patterns
            if any(pattern in first_page_text for pattern in [
                'Your registered address:', 
                'Vodafone Business UK',
                'United Kingdom',
                'GBP'
            ]):
                logger.info("🇬🇧 Routing to Vodafone UK header parser (content)")
                return route_to_uk_parser(pdf_path)
            
            # Check for PNG-specific patterns  
            elif any(pattern in first_page_text for pattern in [
                'Papua New Guinea',
                'Vodafone Papua New Guinea',
                'PGK'
            ]):
                logger.info("🇵🇬 Routing to Vodafone PNG header parser (content)")
                return route_to_png_parser(pdf_path)
                
        except Exception as e:
            logger.warning(f"Content analysis failed: {e}")
        
        # Method 3: Default fallback to UK (most common)
        logger.info("🔄 Defaulting to Vodafone UK header parser")
        return route_to_uk_parser(pdf_path)
        
    except Exception as e:
        logger.error(f"❌ Error in Vodafone header routing: {e}")
        return pd.DataFrame()

def route_to_uk_parser(pdf_path: str) -> pd.DataFrame:
    """Route to Vodafone UK header parser"""
    try:
        from parsers.headers import vodafone_uk_header
        return vodafone_uk_header.extract_header(pdf_path)
    except ImportError as e:
        logger.error(f"❌ Failed to import Vodafone UK header parser: {e}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"❌ Error in Vodafone UK header parser: {e}")
        return pd.DataFrame()

def route_to_png_parser(pdf_path: str) -> pd.DataFrame:
    """Route to Vodafone PNG header parser"""
    try:
        # NOTE: Placeholder for PNG parser - to be implemented
        from parsers.headers import vodafone_png_header
        return vodafone_png_header.extract_header(pdf_path)
    except ImportError:
        logger.warning("⚠️ Vodafone PNG header parser not yet implemented - falling back to UK parser")
        return route_to_uk_parser(pdf_path)
    except Exception as e:
        logger.error(f"❌ Error in Vodafone PNG header parser: {e}")
        logger.info("🔄 Falling back to UK parser")
        return route_to_uk_parser(pdf_path)

# For testing
if __name__ == "__main__":
    print("🧪 Testing Vodafone Header Router")
    print("=" * 50)
    
    test_files = [
        "invoices/test.vodafone.uk.pdf",
        "invoices/test.vodafone.png.pdf", 
        "invoices/test.vodafone.pdf"  # Should default to UK
    ]
    
    for test_file in test_files:
        if os.path.exists(test_file):
            print(f"\n🧪 Testing: {test_file}")
            header_df = extract_header(test_file)
            
            if not header_df.empty:
                print("✅ Header extraction successful!")
                vendor = header_df.iloc[0].get('vendor', 'Unknown')
                print(f"   Detected vendor: {vendor}")
            else:
                print("❌ Header extraction failed!")
        else:
            print(f"⚠️ Test file not found: {test_file}")
    
    print(f"\n📋 Routing Logic:")
    print(f"  1. Filename patterns: 'uk', 'britain' → UK parser")
    print(f"  2. Filename patterns: 'png', 'papua' → PNG parser") 
    print(f"  3. Content patterns: 'Your registered address:', 'GBP' → UK parser")
    print(f"  4. Content patterns: 'Papua New Guinea', 'PGK' → PNG parser")
    print(f"  5. Default fallback: UK parser")