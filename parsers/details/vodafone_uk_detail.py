# parsers/details/vodafone_uk_detail.py
"""
Vodafone UK Detail Parser
Extracts line items from Vodafone UK invoices from Service Details section
"""

import fitz  # PyMuPDF
import re
import pandas as pd
import logging
from typing import Dict, Any, List
import os
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_equinix_items(pdf_path: str, header_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Extract detail line items from Vodafone UK invoice
    Note: Function name kept as 'extract_equinix_items' for consistency with registry system
    
    Args:
        pdf_path: Path to Vodafone UK PDF invoice
        header_data: Header context data (contains invoice_id, ban, etc.)
        
    Returns:
        DataFrame with detail records in standard format
    """
    try:
        logger.info(f"🔄 Extracting Vodafone UK details from: {os.path.basename(pdf_path)}")
        
        # Get header context
        invoice_id = header_data.get('invoice_id')
        ban = header_data.get('ban')
        invoice_date = header_data.get('billing_period')
        
        logger.info(f"   Invoice ID: {invoice_id}")
        logger.info(f"   BAN: {ban}")
        logger.info(f"   Invoice Date: {invoice_date}")
        
        # Extract all detail records using the comprehensive logic
        all_records = extract_vodafone_uk_service_details(pdf_path, invoice_id, ban, invoice_date)
        
        if all_records.empty:
            logger.warning("No detail records extracted")
            return pd.DataFrame()
        
        # Convert to standard format
        standardized_df = standardize_vodafone_records(all_records, header_data)
        
        logger.info(f"✅ Extracted {len(standardized_df)} detail records")
        
        # Log summary by type
        charges = len([r for r in all_records.to_dict('records') if r.get('amount', 0) > 0])
        credits = len([r for r in all_records.to_dict('records') if r.get('amount', 0) < 0])
        logger.info(f"   📊 Summary: {charges} charges, {credits} credits")
        
        return standardized_df
        
    except Exception as e:
        logger.error(f"❌ Error extracting Vodafone UK details: {e}")
        return pd.DataFrame()

def extract_vodafone_uk_service_details(pdf_path: str, invoice_id: str, ban: str, invoice_date: str) -> pd.DataFrame:
    """
    Extract service details from Vodafone UK invoice using the proven logic
    """
    try:
        doc = fitz.open(pdf_path)
        
        # Find start of service details section
        start_page = find_service_details_start_page(pdf_path)
        if not start_page:
            logger.warning("Could not find 'Service Details' start page")
            doc.close()
            return pd.DataFrame()
        
        logger.info(f"   📄 Service Details section starts on page {start_page}")
        
        records = []
        current_item_number = None
        
        # Process pages from service details start to end
        for page_num in range(start_page - 1, len(doc)):
            logger.info(f"   📄 Processing page {page_num + 1}")
            
            text = doc[page_num].get_text()
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            
            i = 0
            while i < len(lines):
                # Look for Service ID pattern
                if lines[i] == "Service ID:" and i + 1 < len(lines):
                    current_item_number = lines[i + 1].strip()
                    logger.debug(f"     Found Service ID: {current_item_number}")
                    i += 2
                    continue
                
                # Look for Rental Charges section
                if lines[i] == "Rental Charges":
                    logger.debug(f"     Found Rental Charges section")
                    j = i + 1
                    
                    while j < len(lines):
                        # Check for nested Service ID within Rental Charges
                        if lines[j] == "Service ID:" and j + 1 < len(lines):
                            current_item_number = lines[j + 1].strip()
                            logger.debug(f"       Updated Service ID: {current_item_number}")
                            j += 2
                            continue
                        
                        # Look for billing period pattern (dd/mm/yy-dd/mm/yy)
                        if re.search(r'\d{2}/\d{2}/\d{2}-\d{2}/\d{2}/\d{2}', lines[j]):
                            try:
                                record = extract_rental_charge_record(lines, j, current_item_number, invoice_id, ban)
                                if record:
                                    records.append(record)
                                    logger.debug(f"       Extracted record: {record['description']} - £{record['amount']}")
                                j += 5  # Skip processed lines
                            except Exception as e:
                                logger.warning(f"       Error processing rental charge: {e}")
                                j += 1
                        else:
                            j += 1
                    
                    i = j  # Continue from where rental charges processing left off
                else:
                    i += 1
        
        doc.close()
        logger.info(f"   📊 Extracted {len(records)} rental charge records")
        
        return pd.DataFrame(records)
        
    except Exception as e:
        logger.error(f"❌ Error extracting Vodafone UK service details: {e}")
        if 'doc' in locals():
            doc.close()
        return pd.DataFrame()

def find_service_details_start_page(pdf_path: str, search_label: str = "Service Details") -> int:
    """Find the page number where Service Details section starts"""
    try:
        doc = fitz.open(pdf_path)
        
        for i in range(len(doc)):
            page_text = doc[i].get_text()
            if search_label.lower() in page_text.lower():
                doc.close()
                return i + 1  # Return 1-based page number
        
        doc.close()
        return None
        
    except Exception as e:
        logger.error(f"Error finding service details start page: {e}")
        return None

def extract_rental_charge_record(lines: List[str], j: int, current_item_number: str, 
                                invoice_id: str, ban: str) -> Dict[str, Any]:
    """
    Extract a single rental charge record from the lines array
    Based on the original proven logic
    """
    try:
        # Extract description (with USOC fix from original code)
        desc_candidate = lines[j - 1] if j - 1 >= 0 else "UNKNOWN"
        
        # Handle USOC patterns like "R123456"
        if re.match(r"^R\d{6,}$", desc_candidate):
            desc_candidate = lines[j - 2] if j - 2 >= 0 else "UNKNOWN"
        
        # Skip reference patterns in parentheses
        if re.match(r'^\(.*\)$', desc_candidate):
            return None  # Skip this record
        
        description = desc_candidate
        usoc = description
        
        # Extract billing period from date range pattern
        match = re.search(r'(\d{2}/\d{2}/\d{2})-(\d{2}/\d{2}/\d{2})', lines[j])
        if not match:
            return None
        
        billing_period_parsed = datetime.strptime(match.group(1), "%d/%m/%y").strftime("%Y-%m-%d")
        
        # Extract VAT, amount, and units from following lines
        vat_line = lines[j + 1] if j + 1 < len(lines) else ""
        amount_line = lines[j + 2] if j + 2 < len(lines) else "0"
        units_line = lines[j + 3] if j + 3 < len(lines) else "1"
        
        # Parse VAT rate
        vat_pct_match = re.search(r'(\d+\.\d+|\d+)%', vat_line)
        vat_rate = float(vat_pct_match.group(1)) / 100 if vat_pct_match else 0.0
        
        # Parse amount (skip if zero)
        amount = float(amount_line.replace(",", ""))
        if amount == 0:
            return None  # Skip zero amount records
        
        # Parse units
        units = int(units_line)
        
        # Calculate tax and total
        tax = round(amount * vat_rate, 2)
        total = round(amount + tax, 2)
        
        record = {
            "invoice_id": invoice_id,
            "item_number": current_item_number or "UNKNOWN",
            "ban": ban,
            "usoc": usoc,
            "description": description,
            "billing_period": billing_period_parsed,
            "units": units,
            "amount": amount,
            "tax": tax,
            "total": total
        }
        
        return record
        
    except Exception as e:
        logger.warning(f"Error extracting rental charge record: {e}")
        return None

def standardize_vodafone_records(records_df: pd.DataFrame, header_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Convert Vodafone UK records to standard format matching other parsers
    """
    if records_df.empty:
        return pd.DataFrame()
    
    # Create standardized DataFrame with all required columns
    standardized = records_df.copy()
    
    # Ensure all required columns exist with proper defaults
    required_columns = {
        'invoice_id': header_data.get('invoice_id', ''),
        'item_number': '',
        'ban': header_data.get('ban', ''),
        'usoc': '',
        'description': '',
        'billing_period': header_data.get('billing_period', ''),
        'units': 1,
        'amount': 0.0,
        'tax': 0.0,
        'total': 0.0,
        'currency': header_data.get('currency', 'GBP'),
        'vendor_name': header_data.get('vendor', 'Vodafone Business UK'),
        'source_file': header_data.get('source_file', ''),
        'invoiced_bu': header_data.get('invoiced_bu', ''),
        'vendorno': header_data.get('vendorno', ''),
        'extracted_at': pd.Timestamp.now(),
        'disputed': False,
        'comment': '',
        'comment_date': '1900-01-01 00:00:00'
    }
    
    # Add missing columns with defaults
    for col, default_val in required_columns.items():
        if col not in standardized.columns:
            standardized[col] = default_val
    
    # Clean up description fields (remove newlines)
    if 'description' in standardized.columns:
        standardized['description'] = standardized['description'].astype(str).str.replace('\n', ' ').str.strip()
    
    if 'usoc' in standardized.columns:
        standardized['usoc'] = standardized['usoc'].astype(str).str.replace('\n', ' ').str.strip()
    
    # Ensure proper data types
    numeric_columns = ['units', 'amount', 'tax', 'total']
    for col in numeric_columns:
        if col in standardized.columns:
            standardized[col] = pd.to_numeric(standardized[col], errors='coerce').fillna(0)
    
    # Convert units to integer
    if 'units' in standardized.columns:
        standardized['units'] = standardized['units'].astype(int)
    
    logger.info(f"✅ Standardized {len(standardized)} Vodafone UK records")
    
    return standardized

# For testing
if __name__ == "__main__":
    # Test with a sample file
    test_file = "invoices/sample.vodafone.uk.pdf"  # Update with actual test file
    
    if os.path.exists(test_file):
        print("🧪 Testing Vodafone UK Detail Parser")
        print("=" * 50)
        
        # Mock header data for testing
        mock_header = {
            'invoice_id': 'TEST123',
            'ban': 'BAN001',
            'billing_period': '2025-01-01',
            'source_file': 'test.pdf',
            'vendor': 'Vodafone Business UK',
            'currency': 'GBP'
        }
        
        detail_df = extract_equinix_items(test_file, mock_header)
        
        if not detail_df.empty:
            print(f"✅ Detail extraction successful! {len(detail_df)} records")
            print("\n📋 Sample records:")
            print(detail_df.head().to_string())
        else:
            print("❌ Detail extraction failed!")
    else:
        print(f"❌ Test file not found: {test_file}")