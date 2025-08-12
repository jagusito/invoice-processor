# parsers/details/vodafone_png_detail.py
"""
Vodafone Papua New Guinea Detail Parser
Extracts line items from Vodafone PNG invoices from Analysis Summary section
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

def extract_gst_percentage_from_first_page(pdf_path: str) -> float:
    """
    Extract GST percentage from first page of PNG invoice
    Format: "GST ( 10.00% of 15,250.00 )"
    """
    try:
        doc = fitz.open(pdf_path)
        first_page_text = doc[0].get_text()
        doc.close()
        
        logger.debug("🔍 Looking for GST percentage on first page...")
        
        # PRIMARY PATTERN: "GST ( 10.00% of 15,250.00 )"
        gst_pattern = r"GST\s*\(\s*(\d+\.?\d*)\s*%"
        match = re.search(gst_pattern, first_page_text, re.IGNORECASE)
        
        if match:
            gst_percentage = float(match.group(1)) / 100  # Convert to decimal
            logger.info(f"✅ Found GST percentage: {match.group(1)}% → {gst_percentage:.2%}")
            return gst_percentage
        
        # FALLBACK PATTERNS
        fallback_patterns = [
            r"GST[:\s]+(\d+\.?\d*)\s*%",
            r"Tax[:\s]+(\d+\.?\d*)\s*%",
            r"VAT[:\s]+(\d+\.?\d*)\s*%"
        ]
        
        for pattern in fallback_patterns:
            match = re.search(pattern, first_page_text, re.IGNORECASE)
            if match:
                gst_percentage = float(match.group(1)) / 100
                logger.info(f"✅ Found GST percentage (fallback): {match.group(1)}% → {gst_percentage:.2%}")
                return gst_percentage
        
        logger.warning("⚠️ GST percentage not found - using default 10%")
        return 0.10  # Default PNG GST rate
        
    except Exception as e:
        logger.error(f"❌ Error extracting GST percentage: {e}")
        return 0.10

def find_analysis_summary_page(pdf_path: str) -> int:
    """Find the page number containing 'analysis summary' section"""
    try:
        doc = fitz.open(pdf_path)
        
        for i in range(len(doc)):
            page_text = doc[i].get_text().lower()
            if 'analysis summary' in page_text:
                doc.close()
                return i + 1  # Return 1-based page number
        
        doc.close()
        return None
        
    except Exception as e:
        logger.error(f"Error finding analysis summary page: {e}")
        return None

def parse_analysis_summary_table(page_text: str, invoice_id: str, ban: str, invoice_date: str, gst_rate: float) -> List[Dict[str, Any]]:
    """
    Parse the analysis summary table to extract line items
    PyMuPDF breaks table into separate lines, so we need to group them:
    Line 20: '82270251'
    Line 21: 'Speedcast_DIA_30MbsUp20M'  
    Line 22: '15,000.00' (first amount = total)
    Line 23-26: other amounts (0.00, 0.00, 0.00, 15,000.00)
    """
    records = []
    
    try:
        lines = [line.strip() for line in page_text.splitlines() if line.strip()]
        
        # Find phone numbers first (they mark the start of each record)
        phone_indices = []
        for i, line in enumerate(lines):
            # Look for phone numbers (flexible pattern - any sequence of digits)
            if re.match(r'^\d+$', line) and len(line) >= 6:  # At least 6 digits, pure numeric
                phone_indices.append(i)
                logger.debug(f"     Found phone number at line {i}: {line}")
        
        logger.info(f"     Found {len(phone_indices)} phone numbers: {[lines[i] for i in phone_indices]}")
        
        # Process each phone number group
        for phone_idx in phone_indices:
            try:
                phone_number = lines[phone_idx]
                
                # Look for description in next few lines (should be next line with letters/underscores)
                description = None
                description_idx = None
                for check_idx in range(phone_idx + 1, min(phone_idx + 5, len(lines))):
                    check_line = lines[check_idx]
                    # Look for description pattern (contains letters, underscores, may have slashes)
                    if re.search(r'[A-Za-z_]', check_line) and len(check_line) > 3:
                        # Skip obvious non-descriptions
                        if not any(skip in check_line.lower() for skip in ['total', 'gst', 'page', 'see', 'details']):
                            description = check_line
                            description_idx = check_idx
                            break
                
                if not description:
                    logger.debug(f"     No description found for phone {phone_number}")
                    continue
                
                # Look for amount in next few lines after description (first non-zero amount)
                amount = None
                amount_idx = None
                for check_idx in range(description_idx + 1, min(description_idx + 8, len(lines))):
                    check_line = lines[check_idx]
                    # Check if this looks like a money amount
                    if re.match(r'^\d{1,3}(?:,\d{3})*\.\d{2}$', check_line):
                        try:
                            potential_amount = float(check_line.replace(',', ''))
                            if potential_amount > 0:  # Take first non-zero amount
                                amount = potential_amount
                                amount_idx = check_idx
                                break
                        except ValueError:
                            continue
                
                if not amount:
                    logger.debug(f"     No valid amount found for phone {phone_number}")
                    continue
                
                # Calculate tax and total
                tax = round(amount * gst_rate, 2)
                total_with_tax = round(amount + tax, 2)
                
                record = {
                    "invoice_id": invoice_id,
                    "item_number": phone_number,
                    "ban": ban,
                    "usoc": description,
                    "description": description,
                    "billing_period": invoice_date,
                    "units": 1,
                    "amount": amount,
                    "tax": tax,
                    "total": total_with_tax
                }
                
                records.append(record)
                logger.info(f"     ✅ Parsed record: {phone_number} - {description} - K{amount}")
                logger.debug(f"     Details: phone_idx={phone_idx}, desc_idx={description_idx}, amount_idx={amount_idx}")
                
            except Exception as e:
                logger.warning(f"Error processing phone number {phone_number}: {e}")
                continue
        
        return records
        
    except Exception as e:
        logger.error(f"Error parsing analysis summary table: {e}")
        return records

def extract_png_analysis_summary_records(pdf_path: str, invoice_id: str, ban: str, invoice_date: str, gst_rate: float) -> pd.DataFrame:
    """
    Extract records from "analysis summary" section of PNG invoice
    Based on table structure: Telephone Number | User Name | Standard Charges | Usage Charges | Non-Standard Charges | Credits&Adjustments | Total
    """
    try:
        doc = fitz.open(pdf_path)
        
        # Find the page with "analysis summary"
        analysis_page = find_analysis_summary_page(pdf_path)
        if not analysis_page:
            logger.warning("Could not find 'analysis summary' section")
            doc.close()
            return pd.DataFrame()
        
        logger.info(f"   📄 Found analysis summary on page {analysis_page}")
        
        page_text = doc[analysis_page - 1].get_text()  # Convert to 0-based index
        doc.close()
        
        # Debug: Log some of the page text to see what we're working with
        logger.debug(f"   📄 Page text preview (first 500 chars):")
        logger.debug(f"   {page_text[:500]}...")
        
        # Extract table records
        records = parse_analysis_summary_table(page_text, invoice_id, ban, invoice_date, gst_rate)
        
        logger.info(f"   📊 Extracted {len(records)} records from analysis summary")
        
        return pd.DataFrame(records)
        
    except Exception as e:
        logger.error(f"❌ Error extracting analysis summary records: {e}")
        if 'doc' in locals():
            doc.close()
        return pd.DataFrame()

def extract_equinix_items(pdf_path: str, header_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Extract detail line items from Vodafone PNG invoice
    Note: Function name kept as 'extract_equinix_items' for consistency with registry system
    
    Args:
        pdf_path: Path to Vodafone PNG PDF invoice
        header_data: Header context data (contains invoice_id, ban, etc.)
        
    Returns:
        DataFrame with detail records in standard format
    """
    try:
        logger.info(f"🔄 Extracting Vodafone PNG details from: {os.path.basename(pdf_path)}")
        
        # Get header context
        invoice_id = header_data.get('invoice_id')
        ban = header_data.get('ban')
        invoice_date = header_data.get('billing_period')
        
        logger.info(f"   Invoice ID: {invoice_id}")
        logger.info(f"   BAN: {ban}")
        logger.info(f"   Invoice Date: {invoice_date}")
        
        # Step 1: Extract GST percentage from first page
        gst_percentage = extract_gst_percentage_from_first_page(pdf_path)
        logger.info(f"   GST Rate: {gst_percentage:.2%}")
        
        # Step 2: Extract detail records from "analysis summary" section
        all_records = extract_png_analysis_summary_records(pdf_path, invoice_id, ban, invoice_date, gst_percentage)
        
        if all_records.empty:
            logger.warning("No PNG detail records extracted from analysis summary")
            return pd.DataFrame()
        
        # Convert to standard format
        standardized_df = standardize_vodafone_png_records(all_records, header_data)
        
        logger.info(f"✅ Extracted {len(standardized_df)} PNG detail records from analysis summary")
        
        # Log summary
        total_amount = standardized_df['amount'].sum()
        total_tax = standardized_df['tax'].sum()
        grand_total = standardized_df['total'].sum()
        logger.info(f"   📊 Financial Summary: Amount={total_amount:,.2f}, Tax={total_tax:,.2f}, Total={grand_total:,.2f}")
        
        return standardized_df
        
    except Exception as e:
        logger.error(f"❌ Error extracting Vodafone PNG details: {e}")
        return pd.DataFrame()

def standardize_vodafone_png_records(records_df: pd.DataFrame, header_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Convert Vodafone PNG records to standard format
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
        'currency': header_data.get('currency', 'PGK'),  # PNG Kina
        'vendor_name': header_data.get('vendor', 'VODAFONE PNG'),
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
    
    # Clean up text fields
    text_columns = ['description', 'usoc']
    for col in text_columns:
        if col in standardized.columns:
            standardized[col] = standardized[col].astype(str).str.replace('\n', ' ').str.strip()
    
    # Ensure proper data types
    numeric_columns = ['units', 'amount', 'tax', 'total']
    for col in numeric_columns:
        if col in standardized.columns:
            standardized[col] = pd.to_numeric(standardized[col], errors='coerce').fillna(0)
    
    # Convert units to integer
    if 'units' in standardized.columns:
        standardized['units'] = standardized['units'].astype(int)
    
    logger.info(f"✅ Standardized {len(standardized)} Vodafone PNG records")
    
    return standardized

# For testing
if __name__ == "__main__":
    #test_file = "invoices/sample.vodafone.png.pdf"
    
    print("🧪 Testing Vodafone PNG Detail Parser")
    print("=" * 50)
    
    if os.path.exists(test_file):
        # Mock header data for testing
        mock_header = {
            'invoice_id': 'PNG123',
            'ban': 'PNGBAN001',
            'billing_period': '2025-01-01',
            'source_file': 'test.png.pdf',
            'vendor': 'VODAFONE PNG',
            'currency': 'PGK'
        }
        
        detail_df = extract_equinix_items(test_file, mock_header)
        
        if not detail_df.empty:
            print(f"✅ PNG Detail extraction successful! {len(detail_df)} records")
            print("\n📋 Sample PNG records:")
            print(detail_df.head().to_string())
            
            print(f"\n💰 PNG Financial Summary:")
            total_amount = detail_df['amount'].sum()
            total_tax = detail_df['tax'].sum()
            grand_total = detail_df['total'].sum()
            print(f"  Amount: K {total_amount:,.2f}")
            print(f"  Tax:    K {total_tax:,.2f}")  
            print(f"  Total:  K {grand_total:,.2f}")
        else:
            print("❌ PNG Detail extraction failed!")
    else:
        print(f"❌ Test file not found: {test_file}")