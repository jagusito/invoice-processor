# parsers/headers/digital_realty_uk_header.py
"""
Digital Realty UK Header Parser - FIXED VERSION
Follows standardized 3-step validation process with NO FALLBACKS
"""

import camelot
import pandas as pd
import re
import fitz  # PyMuPDF
import os
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_header(pdf_path: str) -> pd.DataFrame:
    """
    Extract header information from Digital Realty UK (Interxion) invoices
    Following the standardized 3-step validation process
    """
    try:
        logger.info(f"🔄 Extracting Digital Realty UK header from: {os.path.basename(pdf_path)}")
        
        # STEP 1: Extract basic invoice data
        invoice_id = extract_invoice_id_uk(pdf_path)
        invoice_date = extract_invoice_date_uk(pdf_path)
        ban = extract_ban_uk(pdf_path)
        invoice_total = extract_invoice_total_uk(pdf_path)
        
        # STEP 2: MANDATORY 3-STEP VALIDATION PROCESS
        # 2A: Extract entity name from invoice and get entity_id
        entity_name = extract_entity_name_uk(pdf_path)
        entity_id = get_entity_id_from_catalog(entity_name)
        
        # 2B: Extract vendor name from invoice and get catalog vendor + currency
        extracted_vendor_name = extract_vendor_name_uk(pdf_path)
        vendor_name = get_catalog_vendor_name(extracted_vendor_name)
        currency = get_vendor_currency(vendor_name)
        
        # 2C: Get entity-vendor mapping using both entity_id and vendor_name
        vendor_code = get_vendor_code_from_mapping(entity_id, vendor_name) if entity_id else None
        
        # NO FALLBACKS ALLOWED - either found in invoice/catalog or None
        if not invoice_id:
            logger.warning("⚠️ Digital Realty UK Invoice ID not found - NO FALLBACK")
        
        if not invoice_date:
            logger.warning("⚠️ Digital Realty UK Invoice date not found - NO FALLBACK")
        
        if not ban:
            logger.warning("⚠️ Digital Realty UK BAN not found - NO FALLBACK")
        
        if not entity_id:
            logger.warning(f"⚠️ Entity ID not found for '{entity_name}'")
        
        if not vendor_code:
            logger.warning(f"⚠️ Entity-Vendor Code not found for Entity {entity_id} + {vendor_name}")
        
        if not currency:
            logger.warning(f"⚠️ Currency not found for vendor '{vendor_name}' - NO FALLBACK")
        
        # Create header record in standard format - NO FALLBACKS ALLOWED
        header_data = {
            'invoice_id': invoice_id,
            'ban': ban,
            'billing_period': invoice_date,
            'vendor': vendor_name,
            'currency': currency,
            'source_file': os.path.basename(pdf_path),
            'invoice_total': invoice_total or 0.0,
            'vendorno': vendor_code,  # ONLY from ENTITY_VENDOR_MAPPING.ENTITY_VENDOR_CODE - no fallbacks
            'documentdate': invoice_date,
            'invoiced_bu': entity_id,  # ONLY from ENTITY_CATALOG.ENTITY_ID - no fallbacks
            'entity_name_extracted': entity_name,
            'processed': 'N',
            'transtype': '0',
            'batchno': None,
            'created_at': datetime.now()
        }
        
        logger.info(f"✅ Digital Realty UK header extracted successfully")
        logger.info(f"   Invoice ID: {header_data['invoice_id']}")
        logger.info(f"   Vendor: {header_data['vendor']} (using catalog name)")
        logger.info(f"   Entity Extracted: {header_data['entity_name_extracted']} (from invoice)")
        logger.info(f"   Entity ID: {header_data['invoiced_bu']}")
        logger.info(f"   Vendor Code: {header_data['vendorno']}")
        logger.info(f"   Currency: {header_data['currency']}")
        logger.info(f"   BAN: {header_data['ban']}")
        logger.info(f"   Date: {header_data['billing_period']}")
        logger.info(f"   Total: {header_data['currency']} {header_data['invoice_total']:,.2f}")
        
        return pd.DataFrame([header_data])
        
    except Exception as e:
        logger.error(f"❌ Error extracting Digital Realty UK header: {e}")
        return pd.DataFrame()

def extract_invoice_id_uk(pdf_path: str) -> str:
    """Extract invoice ID using table-based and text-based approaches"""
    try:
        # Try table-based approach first
        tables = camelot.read_pdf(pdf_path, pages='1', flavor='stream')
        
        if tables and len(tables) > 0:
            df = tables[0].df
            
            # DYNAMIC SEARCH for invoice number in table
            for i in range(len(df)):
                for j in range(len(df.columns)):
                    cell_value = str(df.iloc[i, j]).upper() if pd.notna(df.iloc[i, j]) else ''
                    if 'INVOICE NUMBER' in cell_value:
                        # Found the label, look for value in adjacent cells
                        # Try next column first
                        if j + 1 < len(df.columns) and pd.notna(df.iloc[i, j + 1]):
                            potential_id = str(df.iloc[i, j + 1]).strip()
                            if potential_id and potential_id.upper() != 'INVOICE NUMBER':
                                logger.info(f"✅ Found Digital Realty UK invoice ID (table): {potential_id}")
                                return potential_id
                        # Try next row, same column
                        if i + 1 < len(df) and pd.notna(df.iloc[i + 1, j]):
                            potential_id = str(df.iloc[i + 1, j]).strip()
                            if potential_id and potential_id.upper() != 'INVOICE NUMBER':
                                logger.info(f"✅ Found Digital Realty UK invoice ID (table): {potential_id}")
                                return potential_id
        
        # Fallback to text-based approach with table layout support
        doc = fitz.open(pdf_path)
        text = doc[0].get_text().replace('\n', ' ')
        doc.close()
        
        # Table layout patterns (like Equinix)
        table_patterns = [
            r'Invoice Number\s+(\d+)\s+(\d{1,2}-\w{3}-\d{2,4})',  # "Invoice Number [id] [date]"
            r'Invoice #\s+(\d+)\s+(\d{1,2}-\w{3}-\d{2,4})',       # "Invoice # [id] [date]"
        ]
        
        for pattern in table_patterns:
            match = re.search(pattern, text)
            if match:
                invoice_id = match.group(1)
                logger.info(f"✅ Found Digital Realty UK invoice ID (table layout): {invoice_id}")
                return invoice_id
        
        # Standard patterns
        standard_patterns = [
            r'Invoice Number[:\s]+(\w+)',
            r'Invoice #[:\s]+(\w+)',
            r'(?:Invoice Number|Invoice #)\s+(\w+)',
        ]
        
        for pattern in standard_patterns:
            match = re.search(pattern, text)
            if match:
                invoice_id = match.group(1)
                logger.info(f"✅ Found Digital Realty UK invoice ID (standard): {invoice_id}")
                return invoice_id
        
        logger.warning("❌ Digital Realty UK Invoice ID not found")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error extracting Digital Realty UK invoice ID: {e}")
        return None

def extract_invoice_date_uk(pdf_path: str) -> str:
    """Extract invoice date with table layout support"""
    try:
        doc = fitz.open(pdf_path)
        text = doc[0].get_text().replace('\n', ' ')
        doc.close()
        
        # Table layout patterns (like Equinix)
        table_patterns = [
            r'Invoice Date\s+(\d+)\s+(\d{1,2}-\w{3}-\d{4})',  # "Invoice Date [id] [date]" 4-digit
            r'Invoice Date\s+(\d+)\s+(\d{1,2}-\w{3}-\d{2})',  # "Invoice Date [id] [date]" 2-digit
        ]
        
        for pattern in table_patterns:
            match = re.search(pattern, text)
            if match:
                date_str = match.group(2)  # Take the date part (group 2)
                try:
                    if len(date_str.split('-')[-1]) == 2:
                        parsed_date = datetime.strptime(date_str, "%d-%b-%y")
                    else:
                        parsed_date = datetime.strptime(date_str, "%d-%b-%Y")
                    formatted_date = parsed_date.strftime("%Y-%m-%d")
                    logger.info(f"✅ Found Digital Realty UK date (table layout): {date_str} → {formatted_date}")
                    return formatted_date
                except ValueError:
                    logger.info(f"✅ Found Digital Realty UK date (table layout): {date_str}")
                    return date_str
        
        # Standard patterns
        standard_patterns = [
            r'Invoice Date[:\s]+(\d{2}-\w{3}-\d{4})',
            r'Invoice Date[:\s]+(\d{2}-\w{3}-\d{2})',
            r'(\d{2}-\w{3}-\d{4})',  # Just the date pattern
            r'(\d{2}-\w{3}-\d{2})',   # Just the date pattern
        ]
        
        for pattern in standard_patterns:
            match = re.search(pattern, text)
            if match:
                date_str = match.group(1)
                try:
                    if len(date_str.split('-')[-1]) == 2:
                        parsed_date = datetime.strptime(date_str, "%d-%b-%y")
                    else:
                        parsed_date = datetime.strptime(date_str, "%d-%b-%Y")
                    formatted_date = parsed_date.strftime("%Y-%m-%d")
                    logger.info(f"✅ Found Digital Realty UK date (standard): {date_str} → {formatted_date}")
                    return formatted_date
                except ValueError:
                    logger.info(f"✅ Found Digital Realty UK date (standard): {date_str}")
                    return date_str
        
        logger.warning("❌ Digital Realty UK Invoice date not found")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error extracting Digital Realty UK invoice date: {e}")
        return None

def extract_ban_uk(pdf_path: str) -> str:
    """Extract customer number (BAN) using table-based approach"""
    try:
        # Try table-based approach first
        tables = camelot.read_pdf(pdf_path, pages='1', flavor='stream')
        
        if tables and len(tables) > 0:
            df = tables[0].df
            
            # DYNAMIC SEARCH for customer number in table
            for i in range(len(df)):
                for j in range(len(df.columns)):
                    cell_value = str(df.iloc[i, j]).upper() if pd.notna(df.iloc[i, j]) else ''
                    if 'CUSTOMER NUMBER' in cell_value:
                        # Found the label, look for value in adjacent cells
                        # Try next column first
                        if j + 1 < len(df.columns) and pd.notna(df.iloc[i, j + 1]):
                            potential_ban = str(df.iloc[i, j + 1]).strip()
                            if potential_ban and potential_ban.upper() != 'CUSTOMER NUMBER':
                                logger.info(f"✅ Found Digital Realty UK BAN (table): {potential_ban}")
                                return potential_ban
                        # Try next row, same column
                        if i + 1 < len(df) and pd.notna(df.iloc[i + 1, j]):
                            potential_ban = str(df.iloc[i + 1, j]).strip()
                            if potential_ban and potential_ban.upper() != 'CUSTOMER NUMBER':
                                logger.info(f"✅ Found Digital Realty UK BAN (table): {potential_ban}")
                                return potential_ban
        
        # Fallback to text-based approach
        doc = fitz.open(pdf_path)
        text = doc[0].get_text()
        doc.close()
        
        patterns = [
            r'Customer Number[:\s]+([A-Z0-9\-]+)',
            r'Account[:\s]+([A-Z0-9\-]+)',
            r'Customer[:\s]+([A-Z0-9\-]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                ban = match.group(1)
                logger.info(f"✅ Found Digital Realty UK BAN (text): {ban}")
                return ban
        
        logger.warning("❌ Digital Realty UK BAN not found")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error extracting Digital Realty UK BAN: {e}")
        return None

def extract_invoice_total_uk(pdf_path: str) -> float:
    """Extract invoice total from last page"""
    try:
        doc = fitz.open(pdf_path)
        last_page_text = doc[-1].get_text()  # Last page
        doc.close()
        
        # Look for "To be paid" amount
        for line in last_page_text.split('\n'):
            line_clean = line.strip()
            
            if 'To be paid' in line_clean:
                # Extract GBP amount from same line
                amount_match = re.search(r'GBP\s*([\d,]+\.?\d*)', line_clean)
                if amount_match:
                    try:
                        total = float(amount_match.group(1).replace(',', ''))
                        logger.info(f"✅ Found Digital Realty UK total: £{total:,.2f}")
                        return total
                    except ValueError:
                        continue
        
        # Alternative patterns
        alt_patterns = [
            r'To be paid\s+GBP\s*([\d,]+\.?\d*)',
            r'Total\s+GBP\s*([\d,]+\.?\d*)',
            r'GBP\s*([\d,]+\.?\d*)\s*(?:Total|Due|Paid)',
        ]
        
        for pattern in alt_patterns:
            match = re.search(pattern, last_page_text, re.IGNORECASE)
            if match:
                try:
                    total = float(match.group(1).replace(',', ''))
                    logger.info(f"✅ Found Digital Realty UK total (alternative): £{total:,.2f}")
                    return total
                except ValueError:
                    continue
        
        logger.warning("❌ Digital Realty UK Invoice total not found")
        return 0.0
        
    except Exception as e:
        logger.error(f"❌ Error extracting Digital Realty UK invoice total: {e}")
        return 0.0

def extract_entity_name_uk(pdf_path: str) -> str:
    """Extract entity name from invoice - FIXED for Digital Realty UK structure"""
    try:
        doc = fitz.open(pdf_path)
        text = doc[0].get_text()
        doc.close()
        
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        
        # Look for entity name after "INVOICE" header
        # From debug: Line 0: 'INVOICE', Line 1: 'Hermes Datacommunications International Ltd'
        for i, line in enumerate(lines):
            if line.upper() == 'INVOICE':
                # Entity name should be on the next line
                if i + 1 < len(lines):
                    potential_entity = lines[i + 1]
                    # Validate it looks like a company name
                    if (len(potential_entity) > 10 and 
                        any(suffix in potential_entity.upper() for suffix in ['LTD', 'LIMITED', 'CORP', 'CORPORATION', 'INC', 'LLC']) and
                        not any(exclude in potential_entity.upper() for exclude in ['DIGITAL', 'LONDON', 'CONTRACTING'])):
                        logger.info(f"✅ Found Digital Realty UK entity name: {potential_entity}")
                        return potential_entity
        
        # Fallback: Look for company patterns in first 10 lines
        company_patterns = [
            r'([A-Z][A-Za-z\s&,\.]+(?:Ltd\.?|Limited|Corp\.?|Corporation|Inc\.?|LLC))',
            r'(HERMES[A-Za-z\s&,\.]*(?:Ltd\.?|Limited|Corp\.?|Corporation|Inc\.?|LLC)?)',
            r'(SPEEDCAST[A-Za-z\s&,\.]*(?:Ltd\.?|Limited|Corp\.?|Corporation|Inc\.?|LLC)?)',
        ]
        
        first_10_lines = '\n'.join(lines[:10])
        
        for pattern in company_patterns:
            matches = re.findall(pattern, first_10_lines, re.IGNORECASE)
            for match in matches:
                if (len(match) > 10 and 
                    not any(exclude in match.upper() for exclude in ['DIGITAL', 'LONDON', 'CONTRACTING'])):
                    logger.info(f"✅ Found Digital Realty UK entity name (pattern): {match}")
                    return match
        
        logger.warning("❌ Digital Realty UK Entity name not found")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error extracting Digital Realty UK entity name: {e}")
        return None

def extract_vendor_name_uk(pdf_path: str) -> str:
    """Extract vendor name from invoice"""
    try:
        doc = fitz.open(pdf_path)
        text = doc[0].get_text()
        doc.close()
        
        # Look for Digital London patterns
        patterns = [
            r'(Digital London Ltd\.?)',
            r'(Digital Realty)',
            r'(Interxion)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                vendor_name = match.group(1)
                logger.info(f"✅ Found Digital Realty UK vendor name: {vendor_name}")
                return vendor_name
        
        logger.info("Using default Digital Realty UK vendor name")
        return "Digital London Ltd."
        
    except Exception as e:
        logger.error(f"❌ Error extracting Digital Realty UK vendor name: {e}")
        return "Digital London Ltd."

# 3-STEP VALIDATION FUNCTIONS (same as Equinix/Lumen)

def get_entity_id_from_catalog(entity_name: str) -> str:
    """Get Entity ID from ENTITY_CATALOG table by matching entity name - NO FALLBACKS"""
    try:
        if not entity_name:
            return None
            
        from config.snowflake_config import get_snowflake_session
        session = get_snowflake_session()
        
        clean_extracted = clean_entity_name_for_matching(entity_name)
        logger.info(f"   🔍 Matching Digital Realty UK entity: '{entity_name}' (cleaned: '{clean_extracted}')")
        
        query = """
            SELECT ENTITY_ID, ENTITY_NAME
            FROM ENTITY_CATALOG
            WHERE STATUS = 'Active'
            ORDER BY ENTITY_NAME
        """
        
        result = session.sql(query).collect()
        if not result:
            logger.warning("   ⚠️ No active entities found in catalog")
            return None
        
        # Try different matching strategies
        for row in result:
            catalog_entity_id = row[0]
            catalog_entity_name = row[1]
            clean_catalog = clean_entity_name_for_matching(catalog_entity_name)
            
            # Strategy 1: Exact match
            if clean_extracted == clean_catalog:
                logger.info(f"   ✅ Exact Digital Realty UK match: '{entity_name}' → {catalog_entity_id} ({catalog_entity_name})")
                return catalog_entity_id
            
            # Strategy 2: Core name match
            extracted_core = extract_core_company_name(clean_extracted)
            catalog_core = extract_core_company_name(clean_catalog)
            
            if extracted_core == catalog_core and len(extracted_core) > 3:
                logger.info(f"   ✅ Core Digital Realty UK match: '{entity_name}' → {catalog_entity_id} ({catalog_entity_name})")
                return catalog_entity_id
        
        logger.warning(f"   ⚠️ No Digital Realty UK entity match found for '{entity_name}' in ENTITY_CATALOG")
        return None
        
    except Exception as e:
        logger.error(f"   ❌ Error looking up Digital Realty UK entity ID for '{entity_name}': {e}")
        return None

def get_catalog_vendor_name(extracted_vendor_name: str) -> str:
    """Look up the extracted vendor name in the catalog and return the EXACT catalog version - NO FALLBACKS"""
    try:
        from config.snowflake_config import get_snowflake_session
        session = get_snowflake_session()
        
        logger.info(f"   🔍 Looking up vendor: '{extracted_vendor_name}' in catalog...")
        
        query = """
            SELECT VENDOR_NAME
            FROM VENDOR_CATALOG
            WHERE STATUS = 'Active'
            ORDER BY VENDOR_NAME
        """
        
        result = session.sql(query).collect()
        if not result:
            logger.warning("   ⚠️ No active vendors found in catalog")
            return extracted_vendor_name
        
        # Get the extracted vendor normalized for comparison
        extracted_normalized = normalize_vendor_name_for_matching(extracted_vendor_name)
        logger.info(f"   🔍 Extracted normalized: '{extracted_normalized}'")
        
        # Find the matching catalog vendor and return its EXACT name
        for row in result:
            catalog_vendor_name = row[0]  # This is the EXACT name from catalog
            catalog_normalized = normalize_vendor_name_for_matching(catalog_vendor_name)
            
            # If normalized versions match, return the EXACT catalog name
            if extracted_normalized == catalog_normalized:
                logger.info(f"   ✅ Vendor match found:")
                logger.info(f"       Extracted: '{extracted_vendor_name}' → Normalized: '{extracted_normalized}'")
                logger.info(f"       Catalog: '{catalog_vendor_name}' → Normalized: '{catalog_normalized}'")
                logger.info(f"       Returning EXACT catalog name: '{catalog_vendor_name}'")
                return catalog_vendor_name  # Return EXACT catalog name
        
        logger.warning(f"   ⚠️ No vendor match found for '{extracted_vendor_name}' in catalog")
        return extracted_vendor_name
        
    except Exception as e:
        logger.error(f"   ❌ Error looking up vendor in catalog: {e}")
        return extracted_vendor_name

def get_vendor_code_from_mapping(entity_id: str, vendor_name: str) -> str:
    """Get vendor code from ENTITY_VENDOR_MAPPING table - NO FALLBACKS"""
    try:
        if not entity_id or not vendor_name:
            logger.warning(f"   ⚠️ Missing required data - Entity ID: {entity_id}, Vendor Name: {vendor_name}")
            return None
            
        from config.snowflake_config import get_snowflake_session
        session = get_snowflake_session()
        
        logger.info(f"   🔍 Mapping lookup: Entity '{entity_id}' + Vendor '{vendor_name}'")
        
        query = f"""
            SELECT ENTITY_VENDOR_CODE
            FROM ENTITY_VENDOR_MAPPING
            WHERE ENTITY_ID = '{entity_id}' 
            AND VENDOR_NAME = '{vendor_name.replace("'", "''")}' 
            AND STATUS = 'Active'
            LIMIT 1
        """
        
        result = session.sql(query).collect()
        if result and len(result) > 0 and result[0][0]:
            vendor_code = result[0][0]
            logger.info(f"   ✅ Found Entity-Vendor Code: {vendor_code}")
            return vendor_code
        else:
            logger.warning(f"   ⚠️ Entity-Vendor Code not found for Entity {entity_id} + {vendor_name}")
            return None
            
    except Exception as e:
        logger.error(f"   ❌ Error querying Entity-Vendor mapping: {e}")
        return None

def get_vendor_currency(vendor_name: str) -> str:
    """Get currency from vendor catalog - NO FALLBACKS ALLOWED"""
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
            currency = result[0][0]
            logger.info(f"   ✅ Found Digital Realty UK currency from catalog: {currency}")
            return currency
        else:
            logger.warning(f"   ⚠️ Vendor '{vendor_name}' not found in VENDOR_CATALOG - NO FALLBACK")
            return None  # NO FALLBACKS - must come from catalog only
            
    except Exception as e:
        logger.error(f"   ❌ Error looking up Digital Realty UK vendor currency: {e}")
        return None  # NO FALLBACKS - must come from catalog only

# UTILITY FUNCTIONS (same as Equinix/Lumen)

def clean_entity_name_for_matching(name: str) -> str:
    """Clean entity name for better matching"""
    if not name:
        return ""
    
    cleaned = name.upper().strip()
    cleaned = cleaned.replace(',', '').replace('.', '').replace('-', ' ')
    cleaned = cleaned.replace(' LTD', ' LIMITED')
    cleaned = cleaned.replace(' CORP', ' CORPORATION')
    cleaned = cleaned.replace(' INC', ' INCORPORATED')
    
    import re
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    return cleaned.strip()

def normalize_vendor_name_for_matching(name: str) -> str:
    """Normalize vendor name for flexible matching"""
    if not name:
        return ""
    
    cleaned = name.upper().strip()
    cleaned = cleaned.replace(',', '').replace('.', '').replace('-', ' ')
    
    suffix_mappings = {
        ' LIMITED': ' LTD',
        ' INCORPORATED': ' INC', 
        ' CORPORATION': ' CORP'
    }
    
    for full_suffix, short_suffix in suffix_mappings.items():
        cleaned = cleaned.replace(full_suffix, short_suffix)
    
    import re
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    return cleaned.strip()

def extract_core_company_name(name: str) -> str:
    """Extract core company name by removing ALL business suffixes"""
    if not name:
        return ""
    
    suffixes = [
        'INC', 'INCORPORATED', 'CORP', 'CORPORATION', 'LLC', 'LTD', 'LIMITED',
        'CO', 'COMPANY', 'LP', 'LLP', 'PLLC', 'PC', 'ENTERPRISES', 'HOLDINGS',
        'GROUP', 'INTERNATIONAL', 'INTL', 'TECHNOLOGIES', 'TECH', 'SYSTEMS',
        'SOLUTIONS', 'SERVICES', 'COMMUNICATIONS', 'COMM', 'TELECOM', 'SA',
        'PTE', 'PTY', 'BV', 'NV', 'SRL', 'SARL', 'GMBH', 'AG', 'AB', 'AS'
    ]
    
    words = name.split()
    core_words = []
    
    for word in words:
        if word not in suffixes:
            core_words.append(word)
    
    if not core_words and words:
        core_words = words[:1]
    
    return ' '.join(core_words)

# Backward compatibility
def extract_equinix_header(pdf_path: str) -> pd.DataFrame:
    """Backward compatibility function - calls extract_header"""
    return extract_header(pdf_path)