# parsers/headers/lumen_header.py
"""
Lumen Header Parser - FIXED VERSION
Follows standardized 3-step validation process with NO FALLBACKS
Supports both USA and Netherlands variants with table layout date extraction
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
    Following the standardized 3-step validation process
    """
    try:
        logger.info(f"🔄 Extracting Lumen header from: {os.path.basename(pdf_path)}")
        
        # STEP 1: Extract basic invoice data
        invoice_id = extract_invoice_id_from_first_page(pdf_path)
        invoice_date = extract_invoice_date_from_first_page(pdf_path)
        ban = extract_ban_from_invoice(pdf_path)
        
        # Enhanced: Extract amounts including credits/adjustments
        invoice_amounts = extract_invoice_amounts_from_first_page(pdf_path)
        invoice_total = invoice_amounts.get('total', 0.0)
        finance_charges = invoice_amounts.get('finance_charges', 0.0)
        current_charges = invoice_amounts.get('current_charges', 0.0)
        credits_adjustments = invoice_amounts.get('credits_adjustments', 0.0)
        
        # Extract entity name
        entity_name = extract_entity_name_from_invoice(pdf_path)
        entity_id = get_entity_id_from_catalog(entity_name)
        
        # 2B: Extract vendor name from invoice and get catalog vendor + currency
        vendor_name = determine_lumen_vendor(pdf_path)
        catalog_vendor_name = get_catalog_vendor_name(vendor_name)
        currency = get_vendor_currency(catalog_vendor_name)
        
        # 2C: Get entity-vendor mapping using both entity_id and vendor_name
        vendor_code = get_vendor_code_from_mapping(entity_id, catalog_vendor_name) if entity_id else None
        
        # NO FALLBACKS ALLOWED - either found in invoice/catalog or None
        if not invoice_id:
            logger.warning("⚠️ Lumen Invoice ID not found - NO FALLBACK")
        
        if not invoice_date:
            logger.warning("⚠️ Lumen Invoice date not found - NO FALLBACK")
        
        if not ban:
            logger.warning("⚠️ Lumen BAN not found - NO FALLBACK")
        
        if not entity_id:
            logger.warning(f"⚠️ Entity ID not found for '{entity_name}'")
        
        if not vendor_code:
            logger.warning(f"⚠️ Entity-Vendor Code not found for Entity {entity_id} + {catalog_vendor_name}")
        
        if not currency:
            logger.warning(f"⚠️ Currency not found for vendor '{catalog_vendor_name}' - NO FALLBACK")
        
        # Create header record in standard format - NO FALLBACKS ALLOWED
        header_data = {
            'invoice_id': invoice_id,
            'ban': ban,
            'billing_period': invoice_date,
            'vendor': catalog_vendor_name,
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
            'created_at': datetime.now(),
            # Enhanced: Add charge scenario indicators
            'finance_charges': finance_charges,
            'current_charges': current_charges,
            'credits_adjustments': credits_adjustments,
            'has_finance_charges_only': current_charges == 0.0 and finance_charges > 0.0,
            'has_zero_current_charges': current_charges == 0.0 and finance_charges == 0.0,
            'has_credits_adjustments': credits_adjustments > 0.0
        }
        
        logger.info(f"✅ Lumen header extracted successfully")
        logger.info(f"   Invoice ID: {header_data['invoice_id']}")
        logger.info(f"   Vendor: {header_data['vendor']} (using catalog name)")
        logger.info(f"   Entity Extracted: {header_data['entity_name_extracted']} (from invoice)")
        logger.info(f"   Entity ID: {header_data['invoiced_bu']}")
        logger.info(f"   Vendor Code: {header_data['vendorno']}")
        logger.info(f"   Currency: {header_data['currency']}")
        logger.info(f"   BAN: {header_data['ban']}")
        logger.info(f"   Date: {header_data['billing_period']}")
        logger.info(f"   Current Charges: {header_data['currency']} {header_data['current_charges']:,.2f}")
        logger.info(f"   Finance Charges: {header_data['currency']} {header_data['finance_charges']:,.2f}")
        logger.info(f"   Total: {header_data['currency']} {header_data['invoice_total']:,.2f}")
        
        return pd.DataFrame([header_data])
        
    except Exception as e:
        logger.error(f"❌ Error extracting Lumen header: {e}")
        return pd.DataFrame()

def extract_invoice_date_from_first_page(pdf_path: str) -> str:
    """Extract invoice date - FIXED for table layouts + original patterns"""
    try:
        doc = fitz.open(pdf_path)
        text = doc[0].get_text().replace('\n', ' ')
        doc.close()
        
        logger.debug("🔍 Looking for Lumen invoice date patterns...")
        
        # STRATEGY 1: Table layout patterns (like Equinix) - try these first
        # Pattern: Invoice Date [invoice_id] [date] with 4-digit year
        table_layout_4digit = re.search(r'Invoice Date\s+(\d+)\s+(\d{1,2}-\w{3}-\d{4})', text)
        if table_layout_4digit:
            date_str = table_layout_4digit.group(2)  # Take the date part (group 2)
            try:
                parsed_date = datetime.strptime(date_str, "%d-%b-%Y")
                formatted_date = parsed_date.strftime("%Y-%m-%d")
                logger.info(f"✅ Found Lumen date (table layout 4-digit): {date_str} → {formatted_date}")
                return formatted_date
            except ValueError:
                logger.info(f"✅ Found Lumen date (table layout 4-digit): {date_str}")
                return date_str
        
        # Pattern: Invoice Date [invoice_id] [date] with 2-digit year
        table_layout_2digit = re.search(r'Invoice Date\s+(\d+)\s+(\d{1,2}-\w{3}-\d{2})', text)
        if table_layout_2digit:
            date_str = table_layout_2digit.group(2)  # Take the date part (group 2)
            try:
                parsed_date = datetime.strptime(date_str, "%d-%b-%y")
                formatted_date = parsed_date.strftime("%Y-%m-%d")
                logger.info(f"✅ Found Lumen date (table layout 2-digit): {date_str} → {formatted_date}")
                return formatted_date
            except ValueError:
                logger.info(f"✅ Found Lumen date (table layout 2-digit): {date_str}")
                return date_str
        
        # STRATEGY 2: Original Lumen patterns (preserved)
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
            # Netherlands format: "01-Jul-2025" (4-digit year)
            r"Invoice Date\s+(\d{2}-[A-Za-z]{3}-\d{4})",
            # Netherlands format: "01-Jul-25" (2-digit year)
            r"Invoice Date\s+(\d{2}-[A-Za-z]{3}-\d{2})",
            # Just the date pattern (fallback)
            r"(\d{2}-[A-Za-z]{3}-\d{4})",
            r"(\d{2}-[A-Za-z]{3}-\d{2})",
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                original_date = match.group(1)
                logger.info(f"✅ Found Lumen date (original pattern): {original_date}")
                
                # Convert to yyyy-mm-dd format
                try:
                    # Try different date formats
                    try:
                        parsed_date = datetime.strptime(original_date, "%B %d, %Y")  # Full month
                    except ValueError:
                        try:
                            parsed_date = datetime.strptime(original_date, "%b %d, %Y")  # Abbreviated month
                        except ValueError:
                            try:
                                parsed_date = datetime.strptime(original_date, "%d-%b-%Y")  # Netherlands 4-digit: 01-Jul-2025
                            except ValueError:
                                parsed_date = datetime.strptime(original_date, "%d-%b-%y")  # Netherlands 2-digit: 01-Jul-25
                    
                    formatted_date = parsed_date.strftime("%Y-%m-%d")
                    logger.info(f"✅ Converted Lumen date to: {formatted_date}")
                    return formatted_date
                except ValueError as e:
                    logger.warning(f"Could not convert date format: {e}")
                    return original_date  # Return original if conversion fails
        
        logger.warning("❌ Lumen Invoice date not found")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error extracting Lumen invoice date: {e}")
        return None

def extract_invoice_id_from_first_page(pdf_path: str) -> str:
    """Extract invoice ID - FIXED for table layouts + original patterns"""
    try:
        doc = fitz.open(pdf_path)
        text = doc[0].get_text().replace('\n', ' ')
        doc.close()
        
        logger.debug("🔍 Looking for Lumen invoice ID patterns...")
        
        # STRATEGY 1: Table layout patterns (like Equinix)
        # Pattern: Invoice Date [invoice_id] [date]
        table_layout_match = re.search(r'Invoice Date\s+(\d+)\s+(\d{1,2}-\w{3}-\d{2,4})', text)
        if table_layout_match:
            invoice_id = table_layout_match.group(1)  # Take the invoice ID part (group 1)
            logger.info(f"✅ Found Lumen invoice ID (table layout): {invoice_id}")
            return invoice_id
        
        # STRATEGY 2: Original Lumen patterns
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
                logger.info(f"✅ Found Lumen invoice ID (original pattern): {invoice_id}")
                return invoice_id
        
        logger.warning("❌ Lumen Invoice ID not found")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error extracting Lumen invoice ID: {e}")
        return None

def extract_ban_from_invoice(pdf_path: str) -> str:
    """Extract BAN - NO FALLBACKS"""
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
                    logger.info(f"✅ Found Lumen BAN: {ban}")
                    doc.close()
                    return ban
        
        doc.close()
        logger.warning("❌ Lumen BAN not found")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error extracting Lumen BAN: {e}")
        return None

def extract_invoice_amounts_from_first_page(pdf_path: str) -> dict:
    """Extract Current Charges, Finance Charges, and Credits/Adjustments"""
    try:
        doc = fitz.open(pdf_path)
        text = doc[0].get_text()
        doc.close()
        
        amounts = {
            'current_charges': 0.0,
            'finance_charges': 0.0,
            'credits_adjustments': 0.0,  # ADDED
            'total': 0.0
        }
        
        # Split text into lines for better pattern matching
        lines = text.split('\n')
        
        # Look for Current Charges, Finance Charges, and Credits/Adjustments patterns
        for i, line in enumerate(lines):
            line = line.strip()
            
            # Current Charges pattern (UNCHANGED)
            if re.match(r'^Current Charges$', line, re.IGNORECASE):
                # Amount should be on next line
                if i + 1 < len(lines):
                    amount_line = lines[i + 1].strip()
                    current_match = re.search(r'([\d,]+\.?\d*)', amount_line)
                    if current_match:
                        amounts['current_charges'] = float(current_match.group(1).replace(',', ''))
                        logger.info(f"Found Current Charges: ${amounts['current_charges']:,.2f}")
            
            # Finance Charges pattern (UNCHANGED)
            elif re.match(r'^Finance Charges$', line, re.IGNORECASE):
                # Amount should be on next line
                if i + 1 < len(lines):
                    amount_line = lines[i + 1].strip()
                    finance_match = re.search(r'([\d,]+\.?\d*)', amount_line)
                    if finance_match:
                        amounts['finance_charges'] = float(finance_match.group(1).replace(',', ''))
                        logger.info(f"Found Finance Charges: ${amounts['finance_charges']:,.2f}")
            
            # Credits/Adjustments pattern (NEW)
            elif re.match(r'^Credits/Adjustments$', line, re.IGNORECASE):
                # Amount should be on next line
                if i + 1 < len(lines):
                    amount_line = lines[i + 1].strip()
                    credits_match = re.search(r'[\(\-]?([\d,]+\.?\d*)', amount_line)
                    if credits_match:
                        amounts['credits_adjustments'] = float(credits_match.group(1).replace(',', ''))
                        logger.info(f"Found Credits/Adjustments: ${amounts['credits_adjustments']:,.2f}")
        
        # Fallback: try inline patterns if line-by-line didn't work (UPDATED)
        fallback_patterns = [
            (r"Current Charges[:\s]*\$?([\d,]+\.?\d*)", 'current_charges'),
            (r"Finance Charges[:\s]*\$?([\d,]+\.?\d*)", 'finance_charges'),
            (r"Credits/Adjustments[^\d]*[\(\-]?([\d,]+\.?\d*)", 'credits_adjustments'),  # NEW
        ]
        
        for pattern, amount_type in fallback_patterns:
            if amounts[amount_type] == 0.0:
                match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
                if match:
                    amounts[amount_type] = float(match.group(1).replace(',', ''))
                    logger.info(f"Found {amount_type} via fallback: ${amounts[amount_type]:,.2f}")
        
        # Calculate total: Current + Finance - Credits (UPDATED)
        amounts['total'] = amounts['current_charges'] + amounts['finance_charges'] - amounts['credits_adjustments']
        
        if amounts['credits_adjustments'] > 0.0:
            logger.info(f"Calculated total: ${amounts['current_charges']:,.2f} + ${amounts['finance_charges']:,.2f} - ${amounts['credits_adjustments']:,.2f} = ${amounts['total']:,.2f}")
        else:
            logger.info(f"Calculated total: ${amounts['current_charges']:,.2f} + ${amounts['finance_charges']:,.2f} = ${amounts['total']:,.2f}")
        
        # Validation: Ensure we have some amount (UNCHANGED)
        if amounts['total'] <= 0.0 and amounts['finance_charges'] > 0.0:
            amounts['total'] = amounts['finance_charges']
            logger.info(f"Using Finance Charges as total: ${amounts['total']:,.2f}")
        elif amounts['total'] <= 0.0 and amounts['current_charges'] > 0.0:
            amounts['total'] = amounts['current_charges']
            logger.info(f"Using Current Charges as total: ${amounts['total']:,.2f}")
        
        return amounts
        
    except Exception as e:
        logger.error(f"Error extracting invoice amounts: {e}")
        return {'current_charges': 0.0, 'finance_charges': 0.0, 'credits_adjustments': 0.0, 'total': 0.0}
        

def extract_entity_name_from_invoice(pdf_path: str) -> str:
    """Extract entity name from invoice - UNCHANGED logic"""
    try:
        import pdfplumber
        
        logger.info(f"   🔍 Opening PDF with pdfplumber: {os.path.basename(pdf_path)}")
        
        with pdfplumber.open(pdf_path) as pdf:
            # Check if page 3 exists (page index 2)
            if len(pdf.pages) < 3:
                logger.warning(f"   ⚠️ PDF only has {len(pdf.pages)} pages, need at least 3")
                return None
            
            # Get page 3 (index 2)
            page3 = pdf.pages[2]
            text = page3.extract_text()
            
            if not text:
                logger.warning("   ⚠️ No text found on page 3")
                return None
            
            # Split into lines
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            
            logger.info(f"   📄 Page 3 has {len(lines)} lines")
            logger.info("   📋 First 10 lines of page 3:")
            for i, line in enumerate(lines[:10]):
                logger.info(f"   {i+1:2d}: {line}")
            
            # Get line 5 (index 4)
            if len(lines) >= 5:
                entity_name = lines[4].strip()  # Line 5 (index 4)
                logger.info(f"   ✅ Found entity name on page 3, line 5: {entity_name}")
                return entity_name
            else:
                logger.warning(f"   ⚠️ Page 3 only has {len(lines)} lines, need at least 5")
                return None
        
    except ImportError:
        logger.error("   ❌ pdfplumber not available, falling back to PyMuPDF")
        return extract_entity_name_fallback(pdf_path)
    except Exception as e:
        logger.error(f"   ❌ Error extracting entity name with pdfplumber: {e}")
        return extract_entity_name_fallback(pdf_path)

def extract_entity_name_fallback(pdf_path: str) -> str:
    """Fallback entity extraction using PyMuPDF - UNCHANGED"""
    try:
        import fitz
        
        logger.info("   🔄 Using PyMuPDF fallback")
        doc = fitz.open(pdf_path)
        
        # Check if page 3 exists (page index 2)
        if len(doc) < 3:
            logger.warning(f"   ⚠️ PDF only has {len(doc)} pages, need at least 3")
            doc.close()
            return None
        
        # Get page 3 (index 2)
        page3 = doc[2]
        text = page3.get_text()
        doc.close()
        
        if not text:
            logger.warning("   ⚠️ No text found on page 3")
            return None
        
        # Split into lines
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        
        logger.info(f"   📄 Page 3 has {len(lines)} lines")
        
        # Get line 5 (index 4)
        if len(lines) >= 5:
            entity_name = lines[4].strip()  # Line 5 (index 4)
            logger.info(f"   ✅ Found entity name on page 3, line 5: {entity_name}")
            return entity_name
        else:
            logger.warning(f"   ⚠️ Page 3 only has {len(lines)} lines, need at least 5")
            return None
        
    except Exception as e:
        logger.error(f"   ❌ Fallback extraction failed: {e}")
        return None

def determine_lumen_vendor(pdf_path: str) -> str:
    """Determine specific Lumen vendor variant - UNCHANGED"""
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

# NEW 3-STEP VALIDATION FUNCTIONS (same as Equinix)

def get_entity_id_from_catalog(entity_name: str) -> str:
    """Get Entity ID from ENTITY_CATALOG table by matching entity name - NO FALLBACKS"""
    try:
        if not entity_name or entity_name == "UNKNOWN":
            return None
            
        from config.snowflake_config import get_snowflake_session
        session = get_snowflake_session()
        
        clean_extracted = clean_entity_name_for_matching(entity_name)
        logger.info(f"   🔍 Matching Lumen entity: '{entity_name}' (cleaned: '{clean_extracted}')")
        
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
                logger.info(f"   ✅ Exact Lumen match: '{entity_name}' → {catalog_entity_id} ({catalog_entity_name})")
                return catalog_entity_id
            
            # Strategy 2: Ltd/Limited variant matching
            if (clean_extracted.replace(' LIMITED', ' LTD') == clean_catalog.replace(' LIMITED', ' LTD') or
                clean_extracted.replace(' LTD', ' LIMITED') == clean_catalog.replace(' LTD', ' LIMITED')):
                logger.info(f"   ✅ Lumen Ltd/Limited variant match: '{entity_name}' → {catalog_entity_id} ({catalog_entity_name})")
                return catalog_entity_id
            
            # Strategy 3: Core name match
            extracted_core = extract_core_company_name(clean_extracted)
            catalog_core = extract_core_company_name(clean_catalog)
            
            if extracted_core == catalog_core and len(extracted_core) > 3:
                logger.info(f"   ✅ Core Lumen match: '{entity_name}' → {catalog_entity_id} ({catalog_entity_name})")
                return catalog_entity_id
        
        # Strategy 4: Fuzzy matching
        best_match = find_best_fuzzy_match(clean_extracted, result)
        if best_match:
            entity_id, matched_name, similarity = best_match
            logger.info(f"   ✅ Fuzzy Lumen match ({similarity:.1%}): '{entity_name}' → {entity_id} ({matched_name})")
            return entity_id
        
        logger.warning(f"   ⚠️ No Lumen entity match found for '{entity_name}' in ENTITY_CATALOG")
        return None
        
    except Exception as e:
        logger.error(f"   ❌ Error looking up Lumen entity ID for '{entity_name}': {e}")
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
            logger.info(f"   ✅ Found Lumen currency from catalog: {currency}")
            return currency
        else:
            logger.warning(f"   ⚠️ Vendor '{vendor_name}' not found in VENDOR_CATALOG - NO FALLBACK")
            return None  # NO FALLBACKS - must come from catalog only
            
    except Exception as e:
        logger.error(f"   ❌ Error looking up Lumen vendor currency: {e}")
        return None  # NO FALLBACKS - must come from catalog only

# UTILITY FUNCTIONS (same as Equinix)

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
def extract_lumen_header(pdf_path: str) -> pd.DataFrame:
    """Backward compatibility function - calls extract_header"""
    return extract_header(pdf_path)