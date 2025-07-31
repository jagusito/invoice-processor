# parsers/headers/digital_realty_uk_header.py
import camelot
import pandas as pd
import re
import fitz  # PyMuPDF
import os  # Added for os.path.basename

def extract_header(pdf_path: str) -> pd.DataFrame:
    """
    Extract header information from Digital Realty UK (Interxion) invoices
    Following the exact same pattern as Equinix header parser
    """
    try:
        # Try to extract header info using the table-based approach first
        tables = camelot.read_pdf(pdf_path, pages='1', flavor='stream')
        
        invoice_id = "UNKNOWN"
        ban = "UNKNOWN"
        billing_period = "UNKNOWN"
        invoice_total = 0.0
        
        if tables and len(tables) > 0:
            # Extract header information from the first table - DYNAMIC SEARCH
            df = tables[0].df
            
            # DYNAMIC SEARCH for invoice number - search entire table
            for i in range(len(df)):
                for j in range(len(df.columns)):
                    cell_value = str(df.iloc[i, j]).upper() if pd.notna(df.iloc[i, j]) else ''
                    if 'INVOICE NUMBER' in cell_value:
                        # Found the label, look for value in adjacent cells
                        # Try next column first
                        if j + 1 < len(df.columns) and pd.notna(df.iloc[i, j + 1]):
                            potential_id = str(df.iloc[i, j + 1]).strip()
                            if potential_id and potential_id.upper() != 'INVOICE NUMBER':
                                invoice_id = potential_id
                                break
                        # Try next row, same column
                        if i + 1 < len(df) and pd.notna(df.iloc[i + 1, j]):
                            potential_id = str(df.iloc[i + 1, j]).strip()
                            if potential_id and potential_id.upper() != 'INVOICE NUMBER':
                                invoice_id = potential_id
                                break
                if invoice_id != "UNKNOWN":
                    break
            
            # DYNAMIC SEARCH for customer number - search entire table
            for i in range(len(df)):
                for j in range(len(df.columns)):
                    cell_value = str(df.iloc[i, j]).upper() if pd.notna(df.iloc[i, j]) else ''
                    if 'CUSTOMER NUMBER' in cell_value:
                        # Found the label, look for value in adjacent cells
                        # Try next column first
                        if j + 1 < len(df.columns) and pd.notna(df.iloc[i, j + 1]):
                            potential_ban = str(df.iloc[i, j + 1]).strip()
                            if potential_ban and potential_ban.upper() != 'CUSTOMER NUMBER':
                                ban = potential_ban
                                break
                        # Try next row, same column
                        if i + 1 < len(df) and pd.notna(df.iloc[i + 1, j]):
                            potential_ban = str(df.iloc[i + 1, j]).strip()
                            if potential_ban and potential_ban.upper() != 'CUSTOMER NUMBER':
                                ban = potential_ban
                                break
                if ban != "UNKNOWN":
                    break
        
        # Extract INVOICE DATE and total from all pages - DYNAMIC PATTERNS
        doc = fitz.open(pdf_path)
        
        # Get text from first page for invoice date
        first_page_text = doc[0].get_text()
        
        # Get text from last page for total amounts
        last_page_text = doc[-1].get_text()  # Last page
        
        doc.close()
        
        # DYNAMIC search for INVOICE DATE in first page
        # Look for "INVOICE DATE" label and extract adjacent value
        for line in first_page_text.split('\n'):
            if 'INVOICE DATE' in line.upper():
                # Extract date from same line or next line
                date_match = re.search(r'(\d{2}-\w{3}-\d{4})', line)
                if date_match:
                    billing_period = date_match.group(1)
                    break
        
        # If not found in same line, look in subsequent lines
        if billing_period == "UNKNOWN":
            lines = first_page_text.split('\n')
            for i, line in enumerate(lines):
                if 'INVOICE DATE' in line.upper():
                    # Check next few lines for date
                    for j in range(i+1, min(i+3, len(lines))):
                        date_match = re.search(r'(\d{2}-\w{3}-\d{4})', lines[j])
                        if date_match:
                            billing_period = date_match.group(1)
                            break
                    if billing_period != "UNKNOWN":
                        break
        
        print(f"🔍 Sample of last page text:")
        print(f"{last_page_text[:200]}...")
        
        # DYNAMIC search for total amount in last page
        # Look for "To be paid" amount (final total including VAT)
        print(f"🔍 Searching for 'To be paid' in last page...")
        
        found_to_be_paid = False
        
        # Look for the specific "To be paid" line
        for line in last_page_text.split('\n'):
            line_clean = line.strip()
            
            if 'To be paid' in line_clean:
                print(f"   📋 Found 'To be paid' line: '{line_clean}'")
                # Extract GBP amount from same line
                amount_match = re.search(r'GBP\s*([\d,]+\.?\d*)', line_clean)
                if amount_match:
                    try:
                        invoice_total = float(amount_match.group(1).replace(',', ''))
                        print(f"   ✅ Extracted 'To be paid' total: £{invoice_total:,.2f}")
                        found_to_be_paid = True
                        break
                    except ValueError as e:
                        print(f"   ❌ Error parsing amount: {e}")
                        continue
                else:
                    print(f"   ❌ No GBP amount found in 'To be paid' line")
        
        # If "To be paid" not found, show what we have and try alternatives
        if not found_to_be_paid:
            print("   ❌ 'To be paid' not found! Showing all lines with 'GBP':")
            
            for i, line in enumerate(last_page_text.split('\n')):
                if 'GBP' in line:
                    print(f"   Line {i}: '{line.strip()}'")
            
            # Try alternative patterns
            alternative_patterns = [
                r'To be paid\s+GBP\s*([\d,]+\.?\d*)',
                r'Total\s+GBP\s*([\d,]+\.?\d*)',
                r'GBP\s*([\d,]+\.?\d*)\s*(?:Total|Due|Paid)',
            ]
            
            for pattern in alternative_patterns:
                match = re.search(pattern, last_page_text, re.IGNORECASE)
                if match:
                    try:
                        invoice_total = float(match.group(1).replace(',', ''))
                        print(f"   ⚠️ Using alternative pattern total: £{invoice_total:,.2f}")
                        break
                    except ValueError:
                        continue
        
        # Get identification data using enhanced provider detection
        from enhanced_provider_detection import identify_invoice_context
        context = identify_invoice_context(pdf_path)
        
        # Map variant to correct vendor name (same pattern as Equinix)
        vendor_variant = context['context']['vendor_variant']
        vendor_name = _get_vendor_name_from_variant(vendor_variant)
        
        # Get currency from catalog using correct vendor name
        currency = _get_vendor_currency(vendor_name)
        
        # Get Entity ID and Vendor Code from ENTITY_VENDOR_MAPPING
        entity_id, vendor_code = _get_entity_vendor_mapping(vendor_name)
        
        # Display extracted header data
        print(f"📋 Digital Realty Header Extracted:")
        print(f"   Invoice ID: {invoice_id}")
        print(f"   BAN: {ban}")
        print(f"   Billing Period: {billing_period}")
        print(f"   Invoice Total: £{invoice_total:,.2f}")
        print(f"   Vendor Variant: {vendor_variant}")
        print(f"   Vendor Name: {vendor_name}")
        print(f"   Currency: {currency}")
        print(f"   Entity ID: {entity_id}")
        print(f"   Vendor Code: {vendor_code}")
        print(f"   Source File: {os.path.basename(pdf_path)}")
        
        # Create header record following established pattern
        header_data = {
            'invoice_id': invoice_id,
            'ban': ban,
            'billing_period': billing_period,
            'vendor': vendor_name,  # Correct vendor name
            'currency': currency,   # Currency from catalog
            'source_file': os.path.basename(pdf_path),  # Just filename, no path
            'invoice_total': invoice_total,
            'created_at': pd.Timestamp.now(),
            'transtype': '0',
            'batchno': '0',
            'vendorno': vendor_code,  # From ENTITY_VENDOR_MAPPING
            'documentdate': billing_period,  # Same as billing_period
            'invoiced_bu': entity_id,  # From ENTITY_VENDOR_MAPPING
            'processed': 'N'
        }
        
        return pd.DataFrame([header_data])
        
    except Exception as e:
        print(f"Digital Realty UK header extraction error: {e}")
        return pd.DataFrame()

def extract_equinix_header(pdf_path: str) -> pd.DataFrame:
    """
    Backward compatibility function - calls extract_header
    """
    return extract_header(pdf_path)

def _get_entity_vendor_mapping(vendor_name: str) -> tuple:
    """Get Entity ID and Vendor Code from ENTITY_VENDOR_MAPPING table"""
    try:
        from config.snowflake_config import get_snowflake_session
        session = get_snowflake_session()
        
        query = f"""
            SELECT ENTITY_ID, ENTITY_VENDOR_CODE
            FROM ENTITY_VENDOR_MAPPING
            WHERE VENDOR_NAME = '{vendor_name.replace("'", "''")}' 
            AND STATUS = 'Active'
            LIMIT 1
        """
        
        result = session.sql(query).collect()
        if result and len(result) > 0:
            entity_id = result[0][0]
            vendor_code = result[0][1]
            print(f"   ✅ Found mapping: Entity ID = {entity_id}, Vendor Code = {vendor_code}")
            return entity_id, vendor_code
        else:
            print(f"   ⚠️ No mapping found for vendor '{vendor_name}' in ENTITY_VENDOR_MAPPING")
            return None, None
            
    except Exception as e:
        print(f"   ❌ Error querying ENTITY_VENDOR_MAPPING for '{vendor_name}': {e}")
        return None, None

def _get_vendor_name_from_variant(variant: str) -> str:
    """Map variant to correct vendor name that exists in catalog"""
    mapping = {
        'digital_realty': 'Digital London Ltd.'
    }
    return mapping.get(variant, 'Digital London Ltd.')

def _get_vendor_currency(vendor_name: str) -> str:
    """Get currency from vendor catalog"""
    try:
        from config.snowflake_config import get_snowflake_session
        session = get_snowflake_session()
        
        query = f"""
            SELECT CURRENCY
            FROM VENDOR_CATALOG 
            WHERE VENDOR_NAME = '{vendor_name.replace("'", "''")}' 
            AND STATUS = 'Active'
            LIMIT 1
        """
        
        result = session.sql(query).collect()
        if result and result[0][0]:
            return result[0][0]
        else:
            print(f"⚠️ Vendor '{vendor_name}' not found in catalog - using default GBP")
            return 'GBP'  # Default for Digital Realty UK
            
    except Exception as e:
        print(f"⚠️ Error looking up vendor currency for '{vendor_name}': {e}")
        return 'GBP'