# parsers/headers/equinix_header.py - SIMPLE FIX
import fitz  # PyMuPDF
import pandas as pd
import re

def extract_equinix_header(pdf_path: str) -> pd.DataFrame:
    try:
        # Get all text from page 1
        doc = fitz.open(pdf_path)
        first_page = doc[0]
        text = first_page.get_text()
        doc.close()
        
        # Replace newlines with spaces
        text = text.replace('\n', ' ')
        
        invoice_id = "UNKNOWN"
        ban = "UNKNOWN"
        billing_period = "UNKNOWN"
        invoice_total = 0.0
        
        # Extract fields (same logic as before)
        invoice_match = re.search(r'Invoice Number\s+Invoice Date\s+(\d+)\s+(\d{1,2}-\w{3}-\d{2})', text)
        if invoice_match:
            invoice_id = invoice_match.group(1)
            billing_period = invoice_match.group(2)
        else:
            invoice_match = re.search(r'Invoice #\s+Invoice Date\s+(\d+)\s+(\d{1,2}-\w{3}-\d{2})', text)
            if invoice_match:
                invoice_id = invoice_match.group(1)
                billing_period = invoice_match.group(2)
            else:
                id_match = re.search(r'(?:Invoice Number|Invoice #)\s+(\d+)', text)
                if id_match:
                    invoice_id = id_match.group(1)
                date_match = re.search(r'Invoice Date\s+(\d{1,2}-\w{3}-\d{2})', text)
                if date_match:
                    billing_period = date_match.group(1)
        
        account_match = re.search(r'Customer Account #\s+(\d+)', text)
        if account_match:
            ban = account_match.group(1)
            
        total_match = re.search(r'Invoice Total Due\s+([\d,]+\.?\d*)', text)
        if total_match:
            invoice_total = float(total_match.group(1).replace(',', ''))
        
        # Get identification data
        from enhanced_provider_detection import identify_invoice_context
        context = identify_invoice_context(pdf_path)
        
        # Map variant to correct vendor name
        vendor_variant = context['context']['vendor_variant']
        vendor_name = _get_vendor_name_from_variant(vendor_variant)
        
        # Get currency from catalog using correct vendor name
        currency = _get_vendor_currency(vendor_name)
        
        # Create header record
        header_data = {
            'invoice_id': invoice_id,
            'ban': ban,
            'billing_period': billing_period,
            'vendor': vendor_name,  # Correct vendor name
            'currency': currency,   # Currency from catalog
            'source_file': pdf_path,
            'invoice_total': invoice_total,
            'created_at': pd.Timestamp.now(),
            'transtype': '0',
            'batchno': '0',
            'vendorno': context.get('vendor_code'),
            'documentdate': billing_period,
            'invoiced_bu': context.get('entity_id'),
            'processed': 'N'
        }
        
        return pd.DataFrame([header_data])
        
    except Exception as e:
        print(f"Header extraction error: {e}")
        return pd.DataFrame()

def _get_vendor_name_from_variant(variant: str) -> str:
    """Map variant to correct vendor name that exists in catalog"""
    mapping = {
        'equinix_inc': 'Equinix, Inc',
        'equinix_germany': 'Equinix (Germany) GmbH',
        'equinix_middle_east': 'Equinix Middle East FZ-LLC',
        'equinix_japan': 'Equinix Japan KK',
        'equinix_singapore': 'Equinix Singapore Pte Ltd', 
        'equinix_australia': 'Equinix Australia Pty Ltd'
    }
    return mapping.get(variant, 'Equinix, Inc')

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
            return 'USD'
            
    except Exception as e:
        print(f"Error getting vendor currency: {e}")
        return 'USD'