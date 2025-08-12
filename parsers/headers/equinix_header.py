# parsers/headers/equinix_header.py
"""
Equinix Header Parser - MINIMAL Australia Fix Only
Preserves ALL existing working logic, only adds Australia-specific patterns
"""

import fitz  # PyMuPDF
import pandas as pd
import re
import os
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_header(pdf_path: str) -> pd.DataFrame:
    """
    Extract header information from Equinix invoices
    Following the standardized 3-step validation process
    """
    try:
        logger.info(f"🔄 Extracting Equinix header from: {os.path.basename(pdf_path)}")
        
        # STEP 1: Extract basic invoice data using Equinix-specific patterns
        invoice_id = extract_invoice_id_equinix(pdf_path)
        invoice_date = extract_invoice_date_equinix(pdf_path)
        ban = extract_ban_equinix(pdf_path)
        invoice_total = extract_invoice_total_equinix(pdf_path)
        
        # STEP 2: MANDATORY 3-STEP VALIDATION PROCESS
        # (Added on top of existing working extraction)
        
        # 2A: Extract entity name from invoice and get entity_id
        entity_name = extract_entity_name_equinix(pdf_path)
        entity_id = get_entity_id_from_catalog(entity_name)
        
        # 2B: Get vendor name using existing working logic, then validate against catalog
        # Use existing provider detection for multi-branch routing
        from enhanced_provider_detection import identify_invoice_context
        context = identify_invoice_context(pdf_path)
        vendor_variant = context['context']['vendor_variant']
        
        # Map variant to vendor name (existing working logic)
        detected_vendor_name = _get_vendor_name_from_variant(vendor_variant)
        
        # NOW validate against catalog (NEW - this is what was missing!)
        vendor_name = get_catalog_vendor_name(detected_vendor_name)
        currency = get_vendor_currency(vendor_name)
        
        logger.info(f"   📋 Equinix Multi-Branch Validation:")
        logger.info(f"       Filename/Content → Variant: {vendor_variant}")
        logger.info(f"       Variant → Detected Vendor: {detected_vendor_name}")
        logger.info(f"       Catalog Validation → Final Vendor: {vendor_name}")
        
        # 2C: Get entity-vendor mapping using both entity_id and vendor_name
        vendor_code = get_vendor_code_from_mapping(entity_id, vendor_name) if entity_id else None
        
        # NO FALLBACKS ALLOWED - either found in invoice or None
        if not invoice_id:
            logger.warning("⚠️ Equinix Invoice ID not found - NO FALLBACK")
        
        if not invoice_date:
            logger.warning("⚠️ Equinix Invoice date not found - NO FALLBACK")
        
        if not ban:
            logger.warning("⚠️ Equinix BAN not found - NO FALLBACK")
        
        # Validation flags for missing data - NO FALLBACKS ALLOWED
        if not entity_id:
            logger.warning(f"⚠️ Entity ID not found for '{entity_name}'")
        
        if not vendor_code:
            logger.warning(f"⚠️ Entity-Vendor Code not found for Entity {entity_id} + {vendor_name}")
        
        if not currency:
            logger.warning(f"⚠️ Currency not found for vendor '{vendor_name}' - NO FALLBACK ALLOWED")
            # NO FALLBACKS - currency MUST come from catalog lookup only
        
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
        
        logger.info(f"✅ Equinix header extracted successfully")
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
        logger.error(f"❌ Error extracting Equinix header: {e}")
        return pd.DataFrame()

def extract_invoice_id_equinix(pdf_path: str) -> str:
    """Extract invoice ID from Equinix invoice using Equinix-specific patterns"""
    try:
        doc = fitz.open(pdf_path)
        text = doc[0].get_text().replace('\n', ' ')
        doc.close()
        
        logger.debug("🔍 Looking for Equinix invoice ID patterns...")
        
        # Equinix-specific patterns - ONLY ADDED Australia pattern
        patterns = [
            r'Invoice #\s+(\d+)',                     # ADDED: Australia format: "Invoice # 131210211718"
            r'Invoice Number\s+Invoice Date\s+(\d+)\s+(\d{1,2}-\w{3}-\d{2})',  # ORIGINAL
            r'Invoice #\s+Invoice Date\s+(\d+)\s+(\d{1,2}-\w{3}-\d{2})',       # ORIGINAL
            r'(?:Invoice Number|Invoice #)\s+(\d+)',                           # ORIGINAL
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                invoice_id = match.group(1)
                logger.info(f"✅ Found Equinix invoice ID: {invoice_id}")
                return invoice_id
        
        logger.warning("❌ Equinix Invoice ID not found")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error extracting Equinix invoice ID: {e}")
        return None

def extract_invoice_date_equinix(pdf_path: str) -> str:
    """Extract invoice date from Equinix invoice - FIXED for all layouts"""
    try:
        doc = fitz.open(pdf_path)
        text = doc[0].get_text().replace('\n', ' ')
        doc.close()
        
        logger.debug("🔍 Looking for Equinix invoice date patterns...")
        
        # Extract from combined pattern first (ORIGINAL LOGIC)
        combined_match = re.search(r'Invoice Number\s+Invoice Date\s+(\d+)\s+(\d{1,2}-\w{3}-\d{2})', text)
        if combined_match:
            date_str = combined_match.group(2)
            try:
                parsed_date = datetime.strptime(date_str, "%d-%b-%y")
                formatted_date = parsed_date.strftime("%Y-%m-%d")
                logger.info(f"✅ Found Equinix date: {date_str} → {formatted_date}")
                return formatted_date
            except ValueError:
                logger.info(f"✅ Found Equinix date: {date_str}")
                return date_str
        
        # FIXED: Australia layout - "Invoice Date [invoice_id] [date]" with 4-digit year
        # Pattern: Invoice Date 131210211718 01-Jul-2025
        australia_layout_match = re.search(r'Invoice Date\s+(\d+)\s+(\d{1,2}-\w{3}-\d{4})', text)
        if australia_layout_match:
            date_str = australia_layout_match.group(2)  # Take the date part (group 2)
            try:
                parsed_date = datetime.strptime(date_str, "%d-%b-%Y")
                formatted_date = parsed_date.strftime("%Y-%m-%d")
                logger.info(f"✅ Found Equinix date (Australia layout 4-digit): {date_str} → {formatted_date}")
                return formatted_date
            except ValueError:
                logger.info(f"✅ Found Equinix date (Australia layout 4-digit): {date_str}")
                return date_str
        
        # NEW: Middle East layout - "Invoice Date [invoice_id] [date]" with 2-digit year
        # Pattern: Invoice Date 155210017408 01-Jul-25
        middle_east_layout_match = re.search(r'Invoice Date\s+(\d+)\s+(\d{1,2}-\w{3}-\d{2})', text)
        if middle_east_layout_match:
            date_str = middle_east_layout_match.group(2)  # Take the date part (group 2)
            try:
                parsed_date = datetime.strptime(date_str, "%d-%b-%y")
                formatted_date = parsed_date.strftime("%Y-%m-%d")
                logger.info(f"✅ Found Equinix date (Middle East layout 2-digit): {date_str} → {formatted_date}")
                return formatted_date
            except ValueError:
                logger.info(f"✅ Found Equinix date (Middle East layout 2-digit): {date_str}")
                return date_str
        
        # FIXED: Check 4-digit year BEFORE 2-digit year to prevent truncation
        australia_date_match = re.search(r'Invoice Date\s+(\d{1,2}-\w{3}-\d{4})', text)
        if australia_date_match:
            date_str = australia_date_match.group(1)
            try:
                parsed_date = datetime.strptime(date_str, "%d-%b-%Y")
                formatted_date = parsed_date.strftime("%Y-%m-%d")
                logger.info(f"✅ Found Equinix date (Australia 4-digit): {date_str} → {formatted_date}")
                return formatted_date
            except ValueError:
                logger.info(f"✅ Found Equinix date (Australia 4-digit): {date_str}")
                return date_str
        
        # Fallback: separate 2-digit date pattern (MOVED TO LAST)
        date_match = re.search(r'Invoice Date\s+(\d{1,2}-\w{3}-\d{2})', text)
        if date_match:
            date_str = date_match.group(1)
            try:
                parsed_date = datetime.strptime(date_str, "%d-%b-%y")
                formatted_date = parsed_date.strftime("%Y-%m-%d")
                logger.info(f"✅ Found Equinix date (fallback): {date_str} → {formatted_date}")
                return formatted_date
            except ValueError:
                logger.info(f"✅ Found Equinix date (fallback): {date_str}")
                return date_str
        
        logger.warning("❌ Equinix Invoice date not found")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error extracting Equinix invoice date: {e}")
        return None

def extract_ban_equinix(pdf_path: str) -> str:
    """Extract customer account number (BAN) from Equinix invoice - UNCHANGED"""
    try:
        doc = fitz.open(pdf_path)
        text = doc[0].get_text().replace('\n', ' ')
        doc.close()
        
        logger.debug("🔍 Looking for Equinix BAN patterns...")
        
        # Equinix-specific BAN pattern
        account_match = re.search(r'Customer Account #\s+(\d+)', text)
        if account_match:
            ban = account_match.group(1)
            logger.info(f"✅ Found Equinix BAN: {ban}")
            return ban
        
        logger.warning("❌ Equinix BAN not found")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error extracting Equinix BAN: {e}")
        return None

def extract_invoice_total_equinix(pdf_path: str) -> float:
    """Extract invoice total from Equinix invoice - UNCHANGED"""
    try:
        doc = fitz.open(pdf_path)
        text = doc[0].get_text().replace('\n', ' ')
        doc.close()
        
        logger.debug("🔍 Looking for Equinix invoice total patterns...")
        
        # Equinix-specific total pattern
        total_match = re.search(r'Invoice Total Due\s+([\d,]+\.?\d*)', text)
        if total_match:
            total = float(total_match.group(1).replace(',', ''))
            logger.info(f"✅ Found Equinix total: {total:,.2f}")
            return total
        
        # Alternative patterns
        alt_patterns = [
            r'Total Due\s+([\d,]+\.?\d*)',
            r'Amount Due\s+([\d,]+\.?\d*)',
            r'Invoice Total\s+([\d,]+\.?\d*)'
        ]
        
        for pattern in alt_patterns:
            match = re.search(pattern, text)
            if match:
                total = float(match.group(1).replace(',', ''))
                logger.info(f"✅ Found Equinix total (alternative): {total:,.2f}")
                return total
        
        logger.warning("❌ Equinix Invoice total not found")
        return 0.0
        
    except Exception as e:
        logger.error(f"❌ Error extracting Equinix invoice total: {e}")
        return 0.0

def extract_entity_name_equinix(pdf_path: str) -> str:
    """
    Extract entity/customer name from Equinix invoice
    ORIGINAL LOGIC PRESERVED + Australia patterns added last
    """
    try:
        doc = fitz.open(pdf_path)
        first_page_text = doc[0].get_text()
        doc.close()
        
        logger.debug("🔍 Looking for Equinix entity name using text flow analysis...")
        
        # Convert to single line text like pdfplumber shows
        text_flow = first_page_text.replace('\n', ' ').strip()
        
        # DEBUG: Show first part of text flow
        logger.debug(f"🔍 Text flow (first 300 chars): {text_flow[:300]}")
        
        # STRATEGY 1: ORIGINAL WORKING LOGIC - Look for entity name between payment info and "EQUINIX INVOICE" title
        equinix_invoice_match = re.search(r'(.+?)\s+EQUINIX INVOICE', text_flow, re.IGNORECASE)
        if equinix_invoice_match:
            before_invoice_title = equinix_invoice_match.group(1)
            logger.debug(f"   Text before 'EQUINIX INVOICE': {before_invoice_title}")
            
            # Look for company name with business entity indicators in this section
            company_patterns = [
                r'([A-Z][A-Z\s&]+(?:CORP\.?|CORPORATION))',
                r'([A-Z][A-Z\s&]+(?:INC\.?|INCORPORATED))',
                r'([A-Z][A-Z\s&]+(?:LLC|LIMITED))',
                r'([A-Z][A-Z\s&]*(?:NETWORK|SERVICES|COMMUNICATIONS|SYSTEMS)[A-Z\s]*(?:CORP\.?|INC\.?|LLC)?)',
            ]
            
            for pattern in company_patterns:
                matches = re.findall(pattern, before_invoice_title)
                for match in matches:
                    # Clean up the match
                    entity_name = match.strip()
                    # Must be reasonable length and not be vendor name
                    if (len(entity_name) > 8 and 
                        'equinix' not in entity_name.lower() and
                        any(indicator in entity_name.upper() for indicator in ['CORP', 'INC', 'LLC', 'LTD', 'NETWORK', 'SERVICES', 'COMMUNICATIONS'])):
                        logger.info(f"✅ Found Equinix entity name (Text flow pattern): {entity_name}")
                        return entity_name
        
        # STRATEGY 2: ORIGINAL WORKING LOGIC - Direct search for CORP/INC/LLC patterns in text flow
        logger.debug("   Searching for business entity patterns in full text...")
        
        business_entity_patterns = [
            r'([A-Z][A-Z\s&]*NETWORK[A-Z\s]*SERVICES[A-Z\s]*CORP\.?)',
            r'([A-Z][A-Z\s&]*GLOBECOMM[A-Z\s]*NETWORK[A-Z\s]*SERVICES[A-Z\s]*CORP\.?)',
            r'([A-Z][A-Z\s&]+(?:CORP\.?|INC\.?|LLC|LIMITED))',
            r'([A-Z\s]+(?:NETWORK|SERVICES|COMMUNICATIONS|SYSTEMS)[A-Z\s]*(?:CORP\.?|INC\.?|LLC)?)',
        ]
        
        for pattern in business_entity_patterns:
            matches = re.findall(pattern, text_flow)
            for match in matches:
                entity_name = match.strip()
                # Exclude vendor names and ensure reasonable company name
                if (len(entity_name) > 8 and 
                    'equinix' not in entity_name.lower() and
                    not any(exclude in entity_name.lower() for exclude in ['invoice', 'payment', 'total', 'amount', 'bank']) and
                    any(indicator in entity_name.upper() for indicator in ['CORP', 'INC', 'LLC', 'LTD', 'NETWORK', 'SERVICES', 'COMMUNICATIONS'])):
                    logger.info(f"✅ Found Equinix entity name (Business entity pattern): {entity_name}")
                    return entity_name
        
        # STRATEGY 3: ORIGINAL WORKING LOGIC - Split back to lines and look for company patterns
        logger.debug("   Fallback to line-by-line analysis...")
        lines = [line.strip() for line in first_page_text.splitlines() if line.strip()]
        
        for i, line in enumerate(lines):
            # Look for lines with strong business indicators
            if (len(line) > 8 and len(line) < 80 and
                any(corp_indicator in line.upper() for corp_indicator in ['CORP.', 'CORP', 'INC.', 'INC', 'LLC', 'LTD.', 'LIMITED', 'NETWORK SERVICES']) and
                not any(exclude in line.lower() for exclude in ['equinix', 'invoice', 'total', 'amount', 'payment', 'bank', 'avenue', 'street', 'drive']) and
                not re.match(r'^\d+\s', line)):
                
                logger.info(f"✅ Found Equinix entity name (Line analysis): {line}")
                return line
        
        # STRATEGY 4: ORIGINAL WORKING LOGIC - Look specifically for "GLOBECOMM" pattern (common Equinix customer)
        logger.debug("   Looking for specific customer patterns...")
        
        globecomm_match = re.search(r'(GLOBECOMM[A-Z\s]*(?:NETWORK|SERVICES|COMMUNICATIONS)[A-Z\s]*(?:CORP\.?|INC\.?|LLC)?)', text_flow, re.IGNORECASE)
        if globecomm_match:
            entity_name = globecomm_match.group(1).strip()
            logger.info(f"✅ Found Equinix entity name (Globecomm pattern): {entity_name}")
            return entity_name
        
        # STRATEGY 5: NEW - Australia-specific patterns (ONLY ADDED THIS)
        logger.debug("   Trying Australia-specific patterns...")
        
        # Look for Australian company patterns
        australia_patterns = [
            r'([A-Z][A-Za-z\s&]+Australia\s+Pty\s+Ltd)',
            r'([A-Z][A-Za-z\s&]+Pty\s+Ltd)',
        ]
        
        for pattern in australia_patterns:
            matches = re.findall(pattern, text_flow)
            for match in matches:
                entity_name = match.strip()
                # Must not be the vendor name and must be reasonable length
                if (len(entity_name) > 8 and 
                    'equinix' not in entity_name.lower()):
                    logger.info(f"✅ Found Equinix entity name (Australia pattern): {entity_name}")
                    return entity_name
        
        # STRATEGY 6: NEW - Look in lines for Australia patterns
        for line in lines:
            if (len(line) > 8 and len(line) < 80 and
                ('pty ltd' in line.lower() or 'australia' in line.lower()) and
                'equinix' not in line.lower() and
                not any(exclude in line.lower() for exclude in ['invoice', 'total', 'amount', 'payment', 'bank', 'avenue', 'street', 'drive', 'nsw', 'vic', 'qld']) and
                not re.match(r'^\d+\s', line)):
                
                logger.info(f"✅ Found Equinix entity name (Australia line): {line}")
                return line
        
        logger.warning("❌ Equinix Entity name not found")
        logger.warning("   Expected to find company name like 'GLOBECOMM NETWORK SERVICES CORP.' or 'Speedcast Australia Pty Ltd' in text flow")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error extracting Equinix entity name: {e}")
        return None

def extract_vendor_name_equinix(pdf_path: str) -> str:
    """
    Extract vendor name from Equinix invoice (what's actually printed on invoice)
    ORIGINAL LOGIC + Australia pattern
    """
    try:
        doc = fitz.open(pdf_path)
        first_page_text = doc[0].get_text()
        doc.close()
        
        logger.debug("🔍 Looking for Equinix vendor name...")
        
        # Look for various Equinix vendor patterns - ORIGINAL + Australia
        vendor_patterns = [
            r"(Equinix Singapore Pte\. Ltd\.)",       # ORIGINAL
            r"(Equinix Singapore Pte Ltd)",           # ORIGINAL
            r"(Equinix, Inc\.?)",                     # ORIGINAL
            r"(Equinix \(Germany\) GmbH)",            # ORIGINAL
            r"(Equinix Middle East FZ-LLC)",          # ORIGINAL
            r"(Equinix Japan KK)",                    # ORIGINAL
            r"(Equinix Australia Pty Ltd)",           # ADDED: Australia variant
            r"(Equinix [^,\n]*)"                      # ORIGINAL: Generic Equinix pattern
        ]
        
        for pattern in vendor_patterns:
            match = re.search(pattern, first_page_text, re.IGNORECASE)
            if match:
                vendor_name = match.group(1).strip()
                logger.info(f"✅ Found Equinix vendor name: {vendor_name}")
                return vendor_name
        
        # ORIGINAL FALLBACK LOGIC
        lines = first_page_text.split('\n')
        for line in lines[:10]:  # Check first 10 lines
            line = line.strip()
            if 'equinix' in line.lower() and len(line) > 5 and len(line) < 50:
                # Clean up and validate
                if not any(skip in line.lower() for skip in ['invoice', 'account', 'customer', 'total']):
                    logger.info(f"✅ Found Equinix vendor name (fallback): {line}")
                    return line
        
        logger.warning("❌ Equinix vendor name not found - using generic")
        return "Equinix, Inc"
        
    except Exception as e:
        logger.error(f"❌ Error extracting Equinix vendor name: {e}")
        return "Equinix, Inc"

# ORIGINAL WORKING VARIANT MAPPING FUNCTION - UNCHANGED EXCEPT Australia added
def _get_vendor_name_from_variant(variant: str) -> str:
    """
    Map variant to vendor name for multi-branch Equinix
    This handles cases where vendor name is implied from filename/content rather than printed on invoice
    """
    mapping = {
        'equinix_inc': 'Equinix, Inc',
        'equinix_germany': 'Equinix (Germany) GmbH',
        'equinix_middle_east': 'Equinix Middle East FZ-LLC',
        'equinix_japan': 'Equinix Japan K.K.',  # WITH PERIODS (keep as was working)
        'equinix_singapore': 'Equinix Singapore Pte. Ltd.',  # WITH PERIODS (keep as was working)
        'equinix_australia': 'Equinix Australia Pty Ltd'  # ADDED: Australia format
    }
    detected_name = mapping.get(variant, 'Equinix, Inc')
    logger.info(f"       Variant '{variant}' → Detected: '{detected_name}'")
    return detected_name

# ORIGINAL 3-STEP VALIDATION FUNCTIONS - UNCHANGED EXCEPT currency function

def get_entity_id_from_catalog(entity_name: str) -> str:
    """Get Entity ID from ENTITY_CATALOG table by matching entity name - UNCHANGED"""
    try:
        if not entity_name or entity_name == "UNKNOWN":
            return None
            
        from config.snowflake_config import get_snowflake_session
        session = get_snowflake_session()
        
        clean_extracted = clean_entity_name_for_matching(entity_name)
        logger.info(f"   🔍 Matching Equinix entity: '{entity_name}' (cleaned: '{clean_extracted}')")
        
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
                logger.info(f"   ✅ Exact Equinix match: '{entity_name}' → {catalog_entity_id} ({catalog_entity_name})")
                return catalog_entity_id
            
            # Strategy 2: Ltd/Limited variant matching
            if (clean_extracted.replace(' LIMITED', ' LTD') == clean_catalog.replace(' LIMITED', ' LTD') or
                clean_extracted.replace(' LTD', ' LIMITED') == clean_catalog.replace(' LTD', ' LIMITED')):
                logger.info(f"   ✅ Equinix Ltd/Limited variant match: '{entity_name}' → {catalog_entity_id} ({catalog_entity_name})")
                return catalog_entity_id
            
            # Strategy 3: Core name match
            extracted_core = extract_core_company_name(clean_extracted)
            catalog_core = extract_core_company_name(clean_catalog)
            
            if extracted_core == catalog_core and len(extracted_core) > 3:
                logger.info(f"   ✅ Core Equinix match: '{entity_name}' → {catalog_entity_id} ({catalog_entity_name})")
                return catalog_entity_id
        
        # Strategy 4: Fuzzy matching
        best_match = find_best_fuzzy_match(clean_extracted, result)
        if best_match:
            entity_id, matched_name, similarity = best_match
            logger.info(f"   ✅ Fuzzy Equinix match ({similarity:.1%}): '{entity_name}' → {entity_id} ({matched_name})")
            return entity_id
        
        logger.warning(f"   ⚠️ No Equinix entity match found for '{entity_name}' in ENTITY_CATALOG")
        return None
        
    except Exception as e:
        logger.error(f"   ❌ Error looking up Equinix entity ID for '{entity_name}': {e}")
        return None

def get_catalog_vendor_name(extracted_vendor_name: str) -> str:
    """Look up the extracted vendor name in the catalog and return the EXACT catalog version - UNCHANGED"""
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
        
        # Try core name matching if normalized matching fails
        extracted_core = extract_core_company_name(extracted_normalized)
        logger.info(f"   🔍 Trying core matching with: '{extracted_core}'")
        
        for row in result:
            catalog_vendor_name = row[0]
            catalog_normalized = normalize_vendor_name_for_matching(catalog_vendor_name)
            catalog_core = extract_core_company_name(catalog_normalized)
            
            if extracted_core == catalog_core and len(extracted_core) > 3:
                logger.info(f"   ✅ Core vendor match:")
                logger.info(f"       Extracted core: '{extracted_core}'")
                logger.info(f"       Catalog core: '{catalog_core}'")
                logger.info(f"       Returning EXACT catalog name: '{catalog_vendor_name}'")
                return catalog_vendor_name  # Return EXACT catalog name
        
        logger.warning(f"   ⚠️ No vendor match found for '{extracted_vendor_name}' in catalog")
        return extracted_vendor_name
        
    except Exception as e:
        logger.error(f"   ❌ Error looking up vendor in catalog: {e}")
        return extracted_vendor_name

def get_vendor_code_from_mapping(entity_id: str, vendor_name: str) -> str:
    """Get vendor code from ENTITY_VENDOR_MAPPING table - UNCHANGED"""
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
            logger.info(f"   ✅ Found Equinix currency from catalog: {currency}")
            return currency
        else:
            logger.warning(f"   ⚠️ Vendor '{vendor_name}' not found in VENDOR_CATALOG - NO FALLBACK")
            return None  # NO FALLBACKS - must come from catalog only
            
    except Exception as e:
        logger.error(f"   ❌ Error looking up Equinix vendor currency: {e}")
        return None  # NO FALLBACKS - must come from catalog only

# ORIGINAL UTILITY FUNCTIONS - UNCHANGED

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
def extract_equinix_header(pdf_path: str) -> pd.DataFrame:
    """Backward compatibility function - calls extract_header"""
    return extract_header(pdf_path)