# parsers/details/lumen_netherlands_detail.py
"""
Lumen Netherlands Detail Parser - FIXED VERSION (No Doubling)
Fixed to properly identify service IDs and avoid duplicate records
"""

import fitz  # PyMuPDF
import pandas as pd
import logging
from typing import Dict, Any, List
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_equinix_items(pdf_path: str, header_data: Dict[str, Any]) -> pd.DataFrame:
    """Extract detail line items from Netherlands Lumen invoice"""
    try:
        logger.info(f"🔄 Extracting Netherlands Lumen details from: {os.path.basename(pdf_path)}")
        
        invoice_id = header_data.get('invoice_id')
        ban = header_data.get('ban')
        invoice_date = header_data.get('billing_period')
        
        logger.info(f"   Invoice ID: {invoice_id}")
        logger.info(f"   BAN: {ban}")
        logger.info(f"   Invoice Date: {invoice_date}")
        
        service_records = extract_service_records(pdf_path, invoice_id, ban, invoice_date)
        
        if not service_records:
            logger.warning("No service records extracted")
            return pd.DataFrame()
        
        records_df = pd.DataFrame(service_records)
        standardized_df = standardize_records(records_df, header_data)
        
        logger.info(f"✅ Extracted {len(standardized_df)} Netherlands Lumen records")
        return standardized_df
        
    except Exception as e:
        logger.error(f"❌ Error extracting Netherlands Lumen details: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def extract_service_records(pdf_path: str, invoice_id: str, ban: str, invoice_date: str) -> List[Dict]:
    """Extract service records - FIXED to avoid doubling"""
    try:
        doc = fitz.open(pdf_path)
        records = []
        
        # Find SERVICE LEVEL ACTIVITY section
        for page_num in range(len(doc)):
            text = doc[page_num].get_text()
            if "SERVICE LEVEL ACTIVITY" in text:
                logger.info(f"Found SERVICE LEVEL ACTIVITY on page {page_num + 1}")
                break
        else:
            logger.warning("SERVICE LEVEL ACTIVITY section not found")
            doc.close()
            return []
        
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        processed_ids = set()
        
        logger.info(f"Processing {len(lines)} lines")
        
        for i, line in enumerate(lines):
            # FIXED: More specific service ID pattern to avoid false positives
            if is_valid_service_id(line) and line not in processed_ids:
                
                service_id = line
                processed_ids.add(service_id)
                logger.info(f"🔍 Processing service ID: {service_id}")
                
                # Collect data from next 10 lines
                usoc_parts = []
                description = ""
                billing_period = ""
                units = 1
                total = 0.0
                
                for j in range(i + 1, min(i + 11, len(lines))):
                    next_line = lines[j]
                    
                    # Billing period (contains date range)
                    if " - " in next_line and "2025" in next_line:
                        billing_period = next_line
                        logger.info(f"Found billing period: {billing_period}")
                    
                    # Location (starts with Loc)
                    elif next_line.startswith("Loc "):
                        description = next_line
                        logger.info(f"Found description: {description}")
                    
                    # Units (single number after billing period)
                    elif billing_period and next_line.isdigit():
                        units = int(next_line)
                        logger.info(f"Found units: {units}")
                    
                    # Total (decimal number after units)
                    elif "." in next_line and next_line.replace(".", "").replace(",", "").isdigit():
                        total = float(next_line)
                        logger.info(f"Found total: ${total}")
                        break
                    
                    # Service description (other descriptive text lines)
                    elif (not next_line.startswith("Total") and 
                          len(next_line) > 2 and
                          not next_line.isdigit() and
                          not next_line.startswith("PO#:")):  # FIXED: Exclude PO# lines from USOC
                        usoc_parts.append(next_line)
                        logger.info(f"Found service part: {next_line}")
                
                usoc = " ".join(usoc_parts) if usoc_parts else "Service"
                
                if billing_period and total > 0:
                    record = {
                        "invoice_id": invoice_id,
                        "item_number": service_id,
                        "ban": ban,
                        "usoc": usoc,
                        "description": description or "Location not specified",
                        "billing_period": billing_period,
                        "units": units,
                        "amount": total,
                        "tax": 0.0,
                        "total": total
                    }
                    records.append(record)
                    logger.info(f"✅ Created record for {service_id}: ${total}")
                else:
                    logger.warning(f"❌ Incomplete data for {service_id}")
        
        doc.close()
        logger.info(f"Extracted {len(records)} service records")
        return records
        
    except Exception as e:
        logger.error(f"Error extracting service records: {e}")
        import traceback
        traceback.print_exc()
        if 'doc' in locals():
            doc.close()
        return []

def is_valid_service_id(line: str) -> bool:
    """
    FIXED: More specific logic to identify real service IDs
    Avoids false positives like "Page 5 of 7" and "PO#: 2008-314"
    """
    if len(line) < 6:
        return False
    
    # Exclude common false positives
    false_positives = [
        "Page ",           # "Page 5 of 7"
        "PO#:",           # "PO#: 2008-314"
        "Total ",         # "Total AMSTERDAM"
        "Invoice ",       # "Invoice Number"
        "Billing ",       # "Billing Account Number"
        "Service ID",     # Header text
        "Description",    # Header text
        "Billing Period", # Header text
        "Units",          # Header text
        "Total",          # Header text
    ]
    
    for false_positive in false_positives:
        if line.startswith(false_positive):
            logger.debug(f"Excluding false positive: {line}")
            return False
    
    # Real service ID pattern: starts with letters, ends with numbers, no spaces, no colons
    if (line[0].isalpha() and 
        line[-1].isdigit() and 
        " " not in line and      # No spaces (excludes "Page 5 of 7")
        ":" not in line and      # No colons (excludes "PO#: 2008-314")
        len(line) >= 8):         # Reasonable minimum length for service IDs
        
        logger.debug(f"Valid service ID pattern: {line}")
        return True
    
    return False

def standardize_records(records_df: pd.DataFrame, header_data: Dict[str, Any]) -> pd.DataFrame:
    """Convert to standard format - UNCHANGED"""
    if records_df.empty:
        return pd.DataFrame()
    
    standardized = records_df.copy()
    
    # Add required columns
    required_columns = {
        'currency': 'USD',
        'vendor_name': 'Lumen Technologies NL BV',
        'source_file': header_data.get('source_file', ''),
        'extracted_at': pd.Timestamp.now(),
        'disputed': False,
        'comment': '',
        'comment_date': '1900-01-01 00:00:00'
    }
    
    for col, default_val in required_columns.items():
        if col not in standardized.columns:
            standardized[col] = default_val
    
    # Clean text fields
    for col in ['description', 'usoc']:
        if col in standardized.columns:
            standardized[col] = standardized[col].astype(str).str.replace('\n', ' ').str.strip()
    
    # Ensure proper data types
    numeric_columns = ['units', 'amount', 'tax', 'total']
    for col in numeric_columns:
        if col in standardized.columns:
            standardized[col] = pd.to_numeric(standardized[col], errors='coerce').fillna(0)
    
    if 'units' in standardized.columns:
        standardized['units'] = standardized['units'].astype(int)
    
    logger.info(f"✅ Standardized {len(standardized)} records")
    return standardized

# Test function
if __name__ == "__main__":
    print("🧪 Netherlands Lumen Detail Parser - FIXED VERSION (No Doubling)")
    print("✅ Parser loads without syntax errors")