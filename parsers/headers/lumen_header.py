# parsers/headers/lumen_header.py
"""
Lumen Header Parser
Extracts invoice metadata from first page of Lumen/Level3 invoices
"""

import fitz  # PyMuPDF
import re
import pandas as pd
from datetime import datetime
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_header(pdf_path: str) -> pd.DataFrame:
    """
    Extract header information from Lumen invoice
    
    Args:
        pdf_path: Path to Lumen PDF invoice
        
    Returns:
        DataFrame with header information in standard format
    """
    try:
        logger.info(f"🔄 Extracting Lumen header from: {os.path.basename(pdf_path)}")
        
        # Extract individual components
        invoice_id = extract_invoice_id_from_first_page(pdf_path)
        invoice_date = extract_invoice_date_from_first_page(pdf_path)
        ban = extract_ban_from_invoice(pdf_path)
        invoice_total = extract_invoice_total_from_first_page(pdf_path)
        
        # Basic validation
        if not invoice_id:
            logger.warning("Invoice ID not found - using filename fallback")
            invoice_id = os.path.splitext(os.path.basename(pdf_path))[0]
        
        if not invoice_date:
            logger.warning("Invoice date not found - using current date")
            invoice_date = datetime.now().strftime("%Y-%m-%d")
        
        if not ban:
            logger.warning("BAN not found")
            ban = "UNKNOWN"
        
        # Create header record in standard format
        header_data = {
            'invoice_id': invoice_id,
            'ban': ban,
            'billing_period': invoice_date,
            'vendor': determine_lumen_vendor(pdf_path),  # This calls the function!
            'currency': 'USD',  # Will be updated based on vendor
            'source_file': os.path.basename(pdf_path),
            'invoice_total': invoice_total or 0.0,
            'vendorno': 'LUM001',  # Will be updated based on vendor
            'documentdate': invoice_date,
            'invoiced_bu': 'LUMEN',  # Will be updated based on vendor
            'processed': 'N',
            'transtype': '0',
            'batchno': None,
            'created_at': datetime.now()
        }
        
        # Customize based on vendor variant
        vendor_name = header_data['vendor']
        if 'NL BV' in vendor_name:
            header_data['currency'] = 'USD'  # Netherlands still uses USD based on invoice
            header_data['vendorno'] = 'LUM002'  # Different vendor code
            header_data['invoiced_bu'] = 'LUMEN_NL'  # Different BU
        
        logger.info(f"✅ Lumen header extracted successfully")
        logger.info(f"   Invoice ID: {header_data['invoice_id']}")
        logger.info(f"   Vendor: {header_data['vendor']}")
        logger.info(f"   Currency: {header_data['currency']}")
        logger.info(f"   BAN: {header_data['ban']}")
        logger.info(f"   Date: {header_data['billing_period']}")
        logger.info(f"   Total: {header_data['currency']} {header_data['invoice_total']:,.2f}")
        
        return pd.DataFrame([header_data])
        
    except Exception as e:
        logger.error(f"❌ Error extracting Lumen header: {e}")
        return pd.DataFrame()

def extract_invoice_id_from_first_page(pdf_path: str) -> str:
    """Extract invoice ID with multiple pattern matching"""
    try:
        doc = fitz.open(pdf_path)
        text = doc[0].get_text()
        doc.close()
        
        # Try multiple patterns for invoice number
        patterns = [
            r"Invoice Number\s+(\d+)",
            r"Invoice #\s*(\d+)",
            r"Invoice:\s*(\d+)",
            r"Invoice\s+(\d+)",
            r"INV\s*#?\s*(\d+)",
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                invoice_id = match.group(1)
                logger.info(f"Found invoice ID: {invoice_id}")
                return invoice_id
        
        logger.warning("Invoice ID not found using standard patterns")
        return None
        
    except Exception as e:
        logger.error(f"Error extracting invoice ID: {e}")
        return None

def extract_invoice_date_from_first_page(pdf_path: str) -> str:
    """Extract invoice date and convert to YYYY-MM-DD format"""
    try:
        doc = fitz.open(pdf_path)
        text = doc[0].get_text()
        doc.close()
        
        # Pattern for both full month names and abbreviated month names
        patterns = [
            # Full month names like "April 01, 2025"
            r"Invoice Date\s+([A-Za-z]+ \d{1,2}, \d{4})",
            r"Invoice Date:\s*([A-Za-z]+ \d{1,2}, \d{4})",
            # Handle line breaks between label and date
            r"Invoice Date[^\w]*([A-Za-z]+ \d{1,2}, \d{4})",
            # Abbreviated month names like "Apr 01, 2025"  
            r"Invoice Date\s+([A-Za-z]{3} \d{1,2}, \d{4})",
            r"Invoice Date:\s*([A-Za-z]{3} \d{1,2}, \d{4})",
            r"Invoice Date[^\w]*([A-Za-z]{3} \d{1,2}, \d{4})",
            # Netherlands format: "01-Jul-2025"
            r"Invoice Date\s+(\d{2}-[A-Za-z]{3}-\d{4})",
            r"(\d{2}-[A-Za-z]{3}-\d{4})",  # Just the date pattern
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                original_date = match.group(1)
                logger.info(f"Found invoice date: {original_date}")
                
                # Convert to yyyy-mm-dd format
                try:
                    # Try full month name first, then abbreviated, then DD-MMM-YYYY format
                    try:
                        parsed_date = datetime.strptime(original_date, "%B %d, %Y")  # Full month
                    except ValueError:
                        try:
                            parsed_date = datetime.strptime(original_date, "%b %d, %Y")  # Abbreviated month
                        except ValueError:
                            parsed_date = datetime.strptime(original_date, "%d-%b-%Y")  # Netherlands format: 01-Jul-2025
                    
                    formatted_date = parsed_date.strftime("%Y-%m-%d")
                    logger.info(f"Converted invoice date to: {formatted_date}")
                    return formatted_date
                except ValueError as e:
                    logger.warning(f"Could not convert date format: {e}")
                    return original_date  # Return original if conversion fails
        
        logger.warning("Invoice date not found")
        return None
        
    except Exception as e:
        logger.error(f"Error extracting invoice date: {e}")
        return None

def extract_ban_from_invoice(pdf_path: str) -> str:
    """Extract BAN with flexible pattern matching"""
    try:
        doc = fitz.open(pdf_path)
        
        for page in doc:
            text = page.get_text()
            
            # Try multiple patterns for BAN
            patterns = [
                r'Billing Account(?:\s+Number|\s+#)?[:\s]*([A-Z0-9\-]+)',
                r'Account(?:\s+Number|\s+#)?[:\s]*([A-Z0-9\-]+)',
                r'BAN[:\s]*([A-Z0-9\-]+)',
                r'Account\s+ID[:\s]*([A-Z0-9\-]+)',
                r'Customer Account[:\s]*([A-Z0-9\-]+)',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    ban = match.group(1)
                    logger.info(f"Found BAN: {ban}")
                    doc.close()
                    return ban
        
        doc.close()
        logger.warning("BAN not found")
        return None
        
    except Exception as e:
        logger.error(f"Error extracting BAN: {e}")
        return None

def extract_invoice_total_from_first_page(pdf_path: str) -> float:
    """Extract invoice total from first page"""
    try:
        doc = fitz.open(pdf_path)
        text = doc[0].get_text()
        doc.close()
        
        # Try multiple patterns for invoice total - added Current Charges for Lumen
        patterns = [
            r"Current Charges[:\s]*\$?([\d,]+\.?\d*)",  # Lumen specific
            r"Total Due[:\s]*\$?([\d,]+\.?\d*)",
            r"Invoice Total[:\s]*\$?([\d,]+\.?\d*)",
            r"Amount Due[:\s]*\$?([\d,]+\.?\d*)",
            r"Total Amount[:\s]*\$?([\d,]+\.?\d*)",
            r"Balance Due[:\s]*\$?([\d,]+\.?\d*)",
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                total_str = match.group(1).replace(',', '')
                total = float(total_str)
                logger.info(f"Found invoice total: ${total:,.2f}")
                return total
        
        logger.warning("Invoice total not found")
        return 0.0
        
    except Exception as e:
        logger.error(f"Error extracting invoice total: {e}")
        return 0.0

def determine_lumen_vendor(pdf_path: str) -> str:
    """Determine specific Lumen vendor variant from invoice content"""
    try:
        doc = fitz.open(pdf_path)
        # Check all pages, not just first page
        full_text = ""
        for page in doc:
            full_text += page.get_text() + "\n"
        doc.close()
        
        # Check for Netherlands variant first (most specific)
        if 'Lumen Technologies NL BV' in full_text:
            logger.info("Detected Lumen Netherlands variant: Lumen Technologies NL BV")
            return 'Lumen Technologies NL BV'
        
        # Check for other specific patterns that indicate Netherlands
        if 'NL BV' in full_text or 'Netherlands' in full_text.upper() or 'NEDERLAND' in full_text.upper():
            logger.info("Detected Netherlands indicators - using Lumen Technologies NL BV")
            return 'Lumen Technologies NL BV'
        
        # Check for centurylink.smb filename pattern as additional indicator
        filename = os.path.basename(pdf_path).lower()
        if 'centurylink' in filename and 'smb' in filename:
            logger.info("Detected centurylink.smb filename - using Lumen Technologies NL BV")
            return 'Lumen Technologies NL BV'
        
        # Check for other variants
        if 'Lumen Technologies UK' in full_text:
            return 'Lumen Technologies UK Ltd'
        elif 'Lumen Technologies DE' in full_text:
            return 'Lumen Technologies DE GmbH'
        
        # Default to US variant
        logger.info("Using default Lumen Technologies (US)")
        return 'Lumen Technologies'
        
    except Exception as e:
        logger.warning(f"Error detecting Lumen vendor variant: {e}")
        return 'Lumen Technologies'  # Safe default

# For testing
if __name__ == "__main__":
    # Test with a sample file
    test_file = "invoices/sample.lumen.pdf"  # Update with actual test file
    
    if os.path.exists(test_file):
        print("🧪 Testing Lumen Header Parser")
        print("=" * 50)
        
        header_df = extract_header(test_file)
        
        if not header_df.empty:
            print("✅ Header extraction successful!")
            print("\n📋 Header Data:")
            for key, value in header_df.iloc[0].to_dict().items():
                print(f"  {key:<15}: {value}")
        else:
            print("❌ Header extraction failed!")
    else:
        print(f"❌ Test file not found: {test_file}")