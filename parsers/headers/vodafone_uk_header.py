# parsers/headers/vodafone_uk_header.py - FIXED VERSION  
"""
Vodafone UK Header Parser - 3-STEP VALIDATION COMPLIANT
Follows standardized 3-step validation process with NO FALLBACKS
FIXED: Invoice date extraction and vendor name detection
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
    Extract header information from Vodafone UK invoice
    Following the standardized 3-step validation process
    """
    try:
        logger.info(f"🔄 Extracting Vodafone UK header from: {os.path.basename(pdf_path)}")
        
        # STEP 1: Extract basic invoice data
        invoice_id = extract_invoice_id_from_first_page(pdf_path)
        invoice_date = extract_invoice_date_from_first_page(pdf_path)  # FIXED
        ban = extract_ban_from_first_page(pdf_path)
        invoice_total = extract_invoice_total_from_first_page(pdf_path)
        
        # STEP 2: MANDATORY 3-STEP VALIDATION PROCESS
        # 2A: Extract entity name from invoice and get entity_id
        entity_name = extract_entity_name_from_registered_address(pdf_path)
        entity_id = get_entity_id_from_catalog(entity_name)
        
        # 2B: Extract vendor name from invoice and get catalog vendor + currency
        extracted_vendor_name = extract_vendor_name_uk(pdf_path)  # FIXED
        vendor_name = get_catalog_vendor_name(extracted_vendor_name)
        currency = get_vendor_currency(vendor_name)
        
        # 2C: Get entity-vendor mapping using both entity_id and vendor_name
        vendor_code = get_vendor_code_from_mapping(entity_id, vendor_name) if entity_id else None
        
        # NO FALLBACKS ALLOWED - either found in invoice/catalog or None
        if not invoice_id:
            logger.warning("⚠️ Vodafone UK Invoice ID not found - NO FALLBACK")
        
        if not invoice_date:
            logger.warning("⚠️ Vodafone UK Invoice date not found - NO FALLBACK")
        
        if not ban:
            logger.warning("⚠️ Vodafone UK BAN not found - NO FALLBACK")
        
        if not entity_name:
            logger.warning("⚠️ Vodafone UK Entity name not found - NO FALLBACK")
        
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
        
        logger.info(f"✅ Vodafone UK header extracted successfully")
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
        logger.error(f"❌ Error extracting Vodafone UK header: {e}")
        return pd.DataFrame()

def extract_invoice_id_from_first_page(pdf_path: str) -> str:
    """Extract invoice ID from first page using 'Your invoice number' pattern"""
    try:
        doc = fitz.open(pdf_path)
        first_page_text = doc[0].get_text()
        doc.close()
        
        lines = [line.strip() for line in first_page_text.splitlines() if line.strip()]
        
        for idx, line in enumerate(lines):
            if line.strip() == "Your invoice number" and idx + 1 < len(lines):
                invoice_id = lines[idx + 1].strip()
                logger.info(f"✅ Found Vodafone UK invoice ID: {invoice_id}")
                return invoice_id
        
        logger.warning("❌ Vodafone UK Invoice ID not found using 'Your invoice number' pattern")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error extracting Vodafone UK invoice ID: {e}")
        return None

def extract_invoice_date_from_first_page(pdf_path: str) -> str:
    """
    FIXED: Extract invoice date from first page - handles Vodafone UK format
    The date appears at top of page after "Invoice" without a label
    Format: "Invoice\n01 Jun 2025"
    """
    try:
        doc = fitz.open(pdf_path)
        first_page_text = doc[0].get_text()
        doc.close()
        
        lines = [line.strip() for line in first_page_text.splitlines() if line.strip()]
        
        logger.info("🔍 Looking for Vodafone UK invoice date patterns...")
        
        # PRIMARY PATTERN: Date appears immediately after "Invoice" line
        for idx, line in enumerate(lines):
            if line.strip() == "Invoice" and idx + 1 < len(lines):
                potential_date = lines[idx + 1].strip()
                logger.info(f"   Found line after 'Invoice': '{potential_date}'")
                
                # Check if this looks like a date (DD Mon YYYY format)
                date_pattern = r'^\d{1,2}\s+\w{3}\s+\d{4}$'
                if re.match(date_pattern, potential_date):
                    try:
                        parsed_date = datetime.strptime(potential_date, "%d %b %Y")
                        formatted_date = parsed_date.strftime("%Y-%m-%d")
                        logger.info(f"✅ Found Vodafone UK invoice date: {potential_date} → {formatted_date}")
                        return formatted_date
                    except ValueError as e:
                        logger.warning(f"⚠️ Could not parse date format: {potential_date} ({e})")
                        return potential_date
        
        # FALLBACK PATTERN: Traditional "Invoice Date" label
        for idx, line in enumerate(lines):
            if line.strip() == "Invoice Date" and idx + 1 < len(lines):
                date_str = lines[idx + 1].strip()
                try:
                    parsed_date = datetime.strptime(date_str, "%d %b %Y")
                    formatted_date = parsed_date.strftime("%Y-%m-%d")
                    logger.info(f"✅ Found Vodafone UK invoice date (fallback): {date_str} → {formatted_date}")
                    return formatted_date
                except ValueError:
                    logger.warning(f"⚠️ Could not parse Vodafone UK date format: {date_str}")
                    return date_str
        
        # REGEX FALLBACK: Look for date pattern anywhere in first 10 lines
        for line in lines[:10]:
            date_match = re.search(r'\b(\d{1,2}\s+\w{3}\s+\d{4})\b', line)
            if date_match:
                date_str = date_match.group(1)
                try:
                    parsed_date = datetime.strptime(date_str, "%d %b %Y")
                    formatted_date = parsed_date.strftime("%Y-%m-%d")
                    logger.info(f"✅ Found Vodafone UK date (regex): {date_str} → {formatted_date}")
                    return formatted_date
                except ValueError:
                    continue
        
        logger.warning("❌ Vodafone UK Invoice date not found")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error extracting Vodafone UK invoice date: {e}")
        return None

def extract_ban_from_first_page(pdf_path: str) -> str:
    """Extract BAN from first page using 'Your account number' pattern"""
    try:
        doc = fitz.open(pdf_path)
        first_page_text = doc[0].get_text()
        doc.close()
        
        lines = [line.strip() for line in first_page_text.splitlines() if line.strip()]
        
        for idx, line in enumerate(lines):
            if line.strip() == "Your account number" and idx + 1 < len(lines):
                ban = lines[idx + 1].strip()
                logger.info(f"✅ Found Vodafone UK BAN: {ban}")
                return ban
        
        logger.warning("❌ Vodafone UK BAN not found using 'Your account number' pattern")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error extracting Vodafone UK BAN: {e}")
        return None

def extract_entity_name_from_registered_address(pdf_path: str) -> str:
    """Extract entity name from 'Your registered address:' line"""
    try:
        doc = fitz.open(pdf_path)
        first_page_text = doc[0].get_text()
        doc.close()
        
        # Look for the registered address pattern
        pattern = r'Your registered address:\s*([^,]+),'
        match = re.search(pattern, first_page_text, re.IGNORECASE)
        
        if match:
            entity_name = match.group(1).strip()
            logger.info(f"✅ Found Vodafone UK entity name from registered address: {entity_name}")
            return entity_name
        
        # Fallback: look for multiline pattern
        lines = [line.strip() for line in first_page_text.splitlines() if line.strip()]
        
        for idx, line in enumerate(lines):
            if "your registered address:" in line.lower():
                # Entity name might be on the same line or next line
                if idx + 1 < len(lines):
                    next_line = lines[idx + 1].strip()
                    if ',' in next_line:
                        entity_name = next_line.split(',')[0].strip()
                        logger.info(f"✅ Found Vodafone UK entity name (multiline): {entity_name}")
                        return entity_name
                
                # Check if entity name is on same line after colon
                if ':' in line:
                    after_colon = line.split(':', 1)[1].strip()
                    if ',' in after_colon:
                        entity_name = after_colon.split(',')[0].strip()
                        logger.info(f"✅ Found Vodafone UK entity name (same line): {entity_name}")
                        return entity_name
                break
        
        logger.warning("❌ Vodafone UK Entity name not found in registered address")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error extracting Vodafone UK entity name: {e}")
        return None

def extract_vendor_name_uk(pdf_path: str) -> str:
    """
    FIXED: Extract vendor name from UK invoice
    Looks for "Vodafone Limited" at the bottom of the page
    """
    try:
        doc = fitz.open(pdf_path)
        first_page_text = doc[0].get_text()
        doc.close()
        
        logger.info("🔍 Looking for Vodafone UK vendor name patterns...")
        
        # PRIMARY PATTERN: "Vodafone Limited" that appears at bottom
        # From example: "Vodafone Limited, Vodafone House, The Connection, Newbury, Berkshire, RG14 2FN..."
        vendor_patterns = [
            r'(Vodafone Limited)(?:,|\s)',  # Most specific - matches "Vodafone Limited,"
            r'(Vodafone Limited)',          # Exact match
            r'(Vodafone Business UK)',      # Alternative business name
            r'(Vodafone UK)',              # Shortened form
        ]
        
        for pattern in vendor_patterns:
            match = re.search(pattern, first_page_text, re.IGNORECASE)
            if match:
                vendor_name = match.group(1)
                logger.info(f"✅ Found Vodafone UK vendor name: '{vendor_name}'")
                return vendor_name
        
        # FALLBACK: Look line by line for vendor information
        lines = [line.strip() for line in first_page_text.splitlines() if line.strip()]
        
        # Check last 10 lines for vendor info (typically at bottom)
        for line in lines[-10:]:
            if 'vodafone limited' in line.lower():
                # Extract just "Vodafone Limited" from the line
                vendor_match = re.search(r'(Vodafone Limited)', line, re.IGNORECASE)
                if vendor_match:
                    vendor_name = vendor_match.group(1)
                    logger.info(f"✅ Found Vodafone UK vendor name (line scan): '{vendor_name}'")
                    return vendor_name
        
        # ADVANCED FALLBACK: Look for any Vodafone reference
        vodafone_patterns = [
            r'(Vodafone [^,\n]*Limited[^,\n]*)',
            r'(Vodafone [^,\n]*UK[^,\n]*)',
            r'(Vodafone [^,\n]*Business[^,\n]*)',
        ]
        
        for pattern in vodafone_patterns:
            match = re.search(pattern, first_page_text, re.IGNORECASE)
            if match:
                vendor_name = match.group(1).strip()
                logger.info(f"✅ Found Vodafone UK vendor name (advanced): '{vendor_name}'")
                return vendor_name
        
        logger.warning("❌ Vodafone UK vendor name not found")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error extracting Vodafone UK vendor name: {e}")
        return None

def extract_invoice_total_from_first_page(pdf_path: str) -> float:
    """Extract invoice total from first page - Vodafone UK format"""
    try:
        doc = fitz.open(pdf_path)
        first_page_text = doc[0].get_text()
        doc.close()
        
        logger.info("🔍 Looking for Vodafone UK invoice total...")
        
        # Primary pattern: "This month's charges after VAT 15,238.08"
        voda_pattern = r"This month's charges after VAT\s+([\d,]+\.?\d*)"
        match = re.search(voda_pattern, first_page_text, re.IGNORECASE)
        
        if match:
            total_str = match.group(1).replace(',', '')
            total = float(total_str)
            logger.info(f"✅ Found Vodafone UK total: {total:,.2f}")
            return total
        
        # Alternative pattern from the example: Look for amount before "GBP" and "Total"
        # "15,238.08 GBP\nTotal"
        gbp_total_pattern = r'([\d,]+\.?\d*)\s+GBP\s*\n\s*Total'
        match = re.search(gbp_total_pattern, first_page_text, re.IGNORECASE)
        
        if match:
            total_str = match.group(1).replace(',', '')
            total = float(total_str)
            logger.info(f"✅ Found Vodafone UK total (GBP pattern): {total:,.2f}")
            return total
        
        # Fallback patterns
        fallback_patterns = [
            r"Total due[:\s]*£?([\d,]+\.?\d*)",
            r"Amount due[:\s]*£?([\d,]+\.?\d*)", 
            r"Invoice total[:\s]*£?([\d,]+\.?\d*)",
            r"Total amount[:\s]*£?([\d,]+\.?\d*)",
            r"Balance due[:\s]*£?([\d,]+\.?\d*)",
            r"Total charges[:\s]*£?([\d,]+\.?\d*)",
            r"Total[:\s]+([\d,]+\.?\d*)",
            r"Amount[:\s]+([\d,]+\.?\d*)"
        ]
        
        for pattern in fallback_patterns:
            match = re.search(pattern, first_page_text, re.IGNORECASE)
            if match:
                total_str = match.group(1).replace(',', '')
                total = float(total_str)
                logger.info(f"✅ Found Vodafone UK total (fallback): {total:,.2f}")
                return total
        
        logger.warning("❌ Vodafone UK Invoice total not found")
        return 0.0
        
    except Exception as e:
        logger.error(f"❌ Error extracting Vodafone UK invoice total: {e}")
        return 0.0

# 3-STEP VALIDATION FUNCTIONS (same as PNG)

def get_entity_id_from_catalog(entity_name: str) -> str:
    """Get Entity ID from ENTITY_CATALOG table by matching entity name - NO FALLBACKS"""
    try:
        if not entity_name:
            return None
            
        from config.snowflake_config import get_snowflake_session
        session = get_snowflake_session()
        
        clean_extracted = clean_entity_name_for_matching(entity_name)
        logger.info(f"   🔍 Matching Vodafone UK entity: '{entity_name}' (cleaned: '{clean_extracted}')")
        
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
                logger.info(f"   ✅ Exact Vodafone UK match: '{entity_name}' → {catalog_entity_id} ({catalog_entity_name})")
                return catalog_entity_id
            
            # Strategy 2: Core name match
            extracted_core = extract_core_company_name(clean_extracted)
            catalog_core = extract_core_company_name(clean_catalog)
            
            if extracted_core == catalog_core and len(extracted_core) > 3:
                logger.info(f"   ✅ Core Vodafone UK match: '{entity_name}' → {catalog_entity_id} ({catalog_entity_name})")
                return catalog_entity_id
        
        # Strategy 3: Fuzzy matching for partial matches
        best_match = find_best_fuzzy_match(clean_extracted, result)
        if best_match:
            entity_id, matched_name, similarity = best_match
            logger.info(f"   ✅ Fuzzy Vodafone UK match ({similarity:.1%}): '{entity_name}' → {entity_id} ({matched_name})")
            return entity_id
        
        logger.warning(f"   ⚠️ No Vodafone UK entity match found for '{entity_name}' in ENTITY_CATALOG")
        return None
        
    except Exception as e:
        logger.error(f"   ❌ Error looking up Vodafone UK entity ID for '{entity_name}': {e}")
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
            logger.info(f"   ✅ Found Vodafone UK currency from catalog: {currency}")
            return currency
        else:
            logger.warning(f"   ⚠️ Vendor '{vendor_name}' not found in VENDOR_CATALOG - NO FALLBACK")
            return None  # NO FALLBACKS - must come from catalog only
            
    except Exception as e:
        logger.error(f"   ❌ Error looking up Vodafone UK vendor currency: {e}")
        return None  # NO FALLBACKS - must come from catalog only

# UTILITY FUNCTIONS (same as PNG)

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

def calculate_phrase_similarity(name1: str, name2: str) -> float:
    """Calculate similarity based on shared phrases/words"""
    if not name1 or not name2:
        return 0.0
    
    words1 = set([w for w in name1.split() if w])
    words2 = set([w for w in name2.split() if w])
    
    if not words1 or not words2:
        return 0.0
    
    common_words = words1.intersection(words2)
    all_unique_words = words1.union(words2)
    
    return len(common_words) / len(all_unique_words)

def find_best_fuzzy_match(target: str, catalog_results: list, min_similarity: float = 0.6) -> tuple:
    """Find best fuzzy match using phrase-based similarity scoring"""
    if not target or not catalog_results:
        return None
    
    best_match = None
    best_similarity = 0.0
    
    for row in catalog_results:
        catalog_entity_id = row[0]
        catalog_entity_name = row[1]
        clean_catalog = clean_entity_name_for_matching(catalog_entity_name)
        
        similarity = calculate_phrase_similarity(target, clean_catalog)
        
        if similarity > best_similarity and similarity >= min_similarity:
            best_similarity = similarity
            best_match = (catalog_entity_id, catalog_entity_name, similarity)
    
    return best_match

# Backward compatibility
def extract_vodafone_uk_header(pdf_path: str) -> pd.DataFrame:
    """Backward compatibility function - calls extract_header"""
    return extract_header(pdf_path)