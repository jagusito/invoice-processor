# parsers/headers/vodafone_png_header.py
"""
Vodafone Papua New Guinea Header Parser
Extracts invoice metadata from first page of Vodafone PNG invoices
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

def extract_vendor_name_png(pdf_path: str) -> str:
    """
    Extract vendor name from PNG invoice (what's actually printed on invoice)
    Based on PNG format showing "Vodafone PNG Ltd TIN: 501168358"
    """
    try:
        doc = fitz.open(pdf_path)
        first_page_text = doc[0].get_text()
        doc.close()
        
        logger.debug("🔍 Looking for PNG vendor name...")
        
        # PRIMARY PATTERN: "Vodafone PNG Ltd TIN: 501168358" format
        vendor_pattern = r"(Vodafone PNG Ltd)\s+TIN:"
        match = re.search(vendor_pattern, first_page_text, re.IGNORECASE)
        
        if match:
            vendor_name = match.group(1).strip()
            logger.info(f"✅ Found PNG vendor name: {vendor_name}")
            return vendor_name
        
        # FALLBACK PATTERNS: Other possible vendor formats
        fallback_patterns = [
            r"(Vodafone PNG [^,\n]*)",
            r"(Vodafone [^,\n]*PNG[^,\n]*)",
            r"(VODAFONE PNG[^,\n]*)"
        ]
        
        for pattern in fallback_patterns:
            match = re.search(pattern, first_page_text, re.IGNORECASE)
            if match:
                vendor_name = match.group(1).strip()
                logger.info(f"✅ Found PNG vendor name (fallback): {vendor_name}")
                return vendor_name
        
        logger.warning("❌ PNG vendor name not found - using generic")
        return "Vodafone PNG"
        
    except Exception as e:
        logger.error(f"❌ Error extracting PNG vendor name: {e}")
        return "Vodafone PNG"

def normalize_vendor_name_for_matching(name: str) -> str:
    """
    Normalize vendor name for matching
    Handles: "Vodafone PNG Ltd" → "VODAFONE PNG"
    """
    if not name:
        return ""
    
    # Convert to uppercase and clean
    cleaned = name.upper().strip()
    
    # Remove common punctuation
    cleaned = cleaned.replace(',', '').replace('.', '').replace('-', ' ')
    
    # Remove business suffixes for vendor matching
    suffixes_to_remove = [' LTD', ' LIMITED', ' INC', ' INCORPORATED', ' CORP', ' CORPORATION']
    for suffix in suffixes_to_remove:
        if cleaned.endswith(suffix):
            cleaned = cleaned.replace(suffix, '').strip()
    
    # Normalize spaces
    import re
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    return cleaned.strip()

def get_catalog_vendor_name(extracted_vendor_name: str) -> str:
    """
    Look up the extracted vendor name in the catalog and return the catalog version
    Handles: "Vodafone PNG Ltd" (invoice) → "VODAFONE PNG" (catalog)
    """
    try:
        from config.snowflake_config import get_snowflake_session
        session = get_snowflake_session()
        
        logger.info(f"   🔍 Looking up vendor: '{extracted_vendor_name}' in catalog...")
        
        # Get all active vendors from catalog
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
        
        # Try different matching strategies
        for row in result:
            catalog_vendor_name = row[0]
            
            # Strategy 1: Exact match
            if extracted_vendor_name.upper() == catalog_vendor_name.upper():
                logger.info(f"   ✅ Exact vendor match: '{extracted_vendor_name}' → '{catalog_vendor_name}'")
                return catalog_vendor_name
            
            # Strategy 2: PNG-specific matching
            # "Vodafone PNG Ltd" should match "VODAFONE PNG"
            extracted_normalized = normalize_vendor_name_for_matching(extracted_vendor_name)
            catalog_normalized = normalize_vendor_name_for_matching(catalog_vendor_name)
            
            if extracted_normalized == catalog_normalized:
                logger.info(f"   ✅ Normalized vendor match: '{extracted_vendor_name}' → '{catalog_vendor_name}'")
                logger.info(f"       Normalized: '{extracted_normalized}' = '{catalog_normalized}'")
                return catalog_vendor_name
            
            # Strategy 3: Partial matching for Vodafone variants
            if ('vodafone' in extracted_vendor_name.lower() and 'vodafone' in catalog_vendor_name.lower() and
                'png' in extracted_vendor_name.lower() and 'png' in catalog_vendor_name.lower()):
                logger.info(f"   ✅ Vodafone PNG partial match: '{extracted_vendor_name}' → '{catalog_vendor_name}'")
                return catalog_vendor_name
        
        logger.warning(f"   ⚠️ No vendor match found for '{extracted_vendor_name}' in catalog")
        return extracted_vendor_name
        
    except Exception as e:
        logger.error(f"   ❌ Error looking up vendor in catalog: {e}")
        return extracted_vendor_name

def extract_header(pdf_path: str) -> pd.DataFrame:
    """
    Extract header information from Vodafone PNG invoice
    
    Args:
        pdf_path: Path to Vodafone PNG PDF invoice
        
    Returns:
        DataFrame with header information in standard format
    """
    try:
        logger.info(f"🔄 Extracting Vodafone PNG header from: {os.path.basename(pdf_path)}")
        
        # Extract individual components from first page
        invoice_id = extract_invoice_id_png(pdf_path)
        invoice_date = extract_invoice_date_png(pdf_path)
        ban = extract_ban_png(pdf_path)
        entity_name = extract_entity_name_png(pdf_path)
        invoice_total = extract_invoice_total_png(pdf_path)
        
        # Basic validation with PNG-specific fallbacks
        if not invoice_id:
            logger.warning("PNG Invoice ID not found - using filename fallback")
            invoice_id = os.path.splitext(os.path.basename(pdf_path))[0]
        
        if not invoice_date:
            logger.warning("PNG Invoice date not found - using current date")
            invoice_date = datetime.now().strftime("%Y-%m-%d")
        
        if not ban:
            logger.warning("PNG BAN not found")
            ban = "UNKNOWN"
        
        if not entity_name:
            logger.warning("PNG Entity name not found on invoice")
            entity_name = "UNKNOWN"
        
        # Extract vendor name from invoice (what's actually on the invoice)
        extracted_vendor_name = extract_vendor_name_png(pdf_path)
        
        # Get the catalog vendor name through lookup (no hardcoding)
        vendor_name = get_catalog_vendor_name(extracted_vendor_name)
        
        logger.info(f"🏷️ Extracted vendor: '{extracted_vendor_name}' → Catalog vendor: '{vendor_name}'")
        
        # Get entity ID from catalog using extracted entity name
        entity_id = get_entity_id_from_catalog(entity_name)
        
        # Get vendor mapping and currency from catalog
        vendor_code = get_vendor_code_from_mapping(entity_id, vendor_name) if entity_id else None
        currency = get_vendor_currency(vendor_name)
        
        # IMPORTANT: Use same logic as UK parser - no hardcoded fallbacks
        if not entity_id:
            logger.warning(f"⚠️ Entity lookup failed for '{entity_name}'")
        
        if not vendor_code:
            logger.warning(f"⚠️ Vendor code lookup failed for Entity {entity_id} + {vendor_name}")
        
        if not currency:
            logger.warning(f"⚠️ Currency lookup failed for vendor '{vendor_name}' - using default PGK")
            currency = 'PGK'  # Reasonable default for PNG
        
        # Create header record in standard format
        header_data = {
            'invoice_id': invoice_id,
            'ban': ban,
            'billing_period': invoice_date,
            'vendor': vendor_name,
            'currency': currency,
            'source_file': os.path.basename(pdf_path),
            'invoice_total': invoice_total or 0.0,
            'vendorno': vendor_code or f'VOD-PNG-{entity_id}' if entity_id else 'VOD-PNG-UNKNOWN',
            'documentdate': invoice_date,
            'invoiced_bu': entity_id or 'UNKNOWN_ENTITY',
            'entity_name_extracted': entity_name,
            'processed': 'N',
            'transtype': '0',
            'batchno': None,
            'created_at': datetime.now()
        }
        
        logger.info(f"✅ Vodafone PNG header extracted successfully")
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
        logger.error(f"❌ Error extracting Vodafone PNG header: {e}")
        return pd.DataFrame()

def extract_invoice_id_png(pdf_path: str) -> str:
    """
    Extract invoice ID from Vodafone PNG invoice
    TODO: Update patterns based on actual PNG invoice format
    """
    try:
        doc = fitz.open(pdf_path)
        first_page_text = doc[0].get_text()
        doc.close()
        
        logger.debug("🔍 Looking for PNG invoice ID patterns...")
        
        # PNG-specific patterns (to be updated based on actual format)
        patterns = [
            r"Invoice Number[:\s]+([A-Z0-9\-]+)",
            r"Invoice ID[:\s]+([A-Z0-9\-]+)",
            r"Invoice No[:\s]+([A-Z0-9\-]+)",
            r"Bill Number[:\s]+([A-Z0-9\-]+)",
            r"Document Number[:\s]+([A-Z0-9\-]+)"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, first_page_text, re.IGNORECASE)
            if match:
                invoice_id = match.group(1).strip()
                logger.info(f"✅ Found PNG invoice ID: {invoice_id}")
                return invoice_id
        
        # Fallback: Look for line-by-line pattern
        lines = [line.strip() for line in first_page_text.splitlines() if line.strip()]
        
        for idx, line in enumerate(lines):
            if any(keyword in line.lower() for keyword in ['invoice number', 'invoice id', 'invoice no']):
                if idx + 1 < len(lines):
                    potential_id = lines[idx + 1].strip()
                    if re.match(r'^[A-Z0-9\-]{3,}$', potential_id):
                        logger.info(f"✅ Found PNG invoice ID (fallback): {potential_id}")
                        return potential_id
        
        logger.warning("❌ PNG Invoice ID not found")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error extracting PNG invoice ID: {e}")
        return None

def extract_invoice_date_png(pdf_path: str) -> str:
    """
    Extract invoice date from Vodafone PNG invoice
    UPDATED: Based on actual PNG format "Issue Date: 01-Jul-25"
    """
    try:
        doc = fitz.open(pdf_path)
        first_page_text = doc[0].get_text()
        doc.close()
        
        logger.debug("🔍 Looking for PNG invoice date patterns...")
        
        # PRIMARY PATTERN: PNG-specific "Issue Date: 01-Jul-25" format
        issue_date_pattern = r"Issue Date[:\s]+(\d{1,2}-\w{3}-\d{2})"
        match = re.search(issue_date_pattern, first_page_text, re.IGNORECASE)
        
        if match:
            date_str = match.group(1).strip()
            try:
                # Parse "01-Jul-25" format
                parsed_date = datetime.strptime(date_str, "%d-%b-%y")
                formatted_date = parsed_date.strftime("%Y-%m-%d")
                logger.info(f"✅ Found PNG issue date: {date_str} → {formatted_date}")
                return formatted_date
            except ValueError as e:
                logger.warning(f"⚠️ Could not parse PNG issue date format: {date_str} ({e})")
                return date_str
        
        # FALLBACK PATTERNS: Other possible date formats
        fallback_patterns = [
            r"Invoice Date[:\s]+(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})",
            r"Bill Date[:\s]+(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})",
            r"Date[:\s]+(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})",
            r"(\d{1,2}\s+\w+\s+\d{4})",  # DD Month YYYY format
        ]
        
        for pattern in fallback_patterns:
            match = re.search(pattern, first_page_text, re.IGNORECASE)
            if match:
                date_str = match.group(1).strip()
                try:
                    # Try different date formats
                    for fmt in ["%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y", "%d-%m-%y", "%d %B %Y", "%d %b %Y"]:
                        try:
                            parsed_date = datetime.strptime(date_str, fmt)
                            formatted_date = parsed_date.strftime("%Y-%m-%d")
                            logger.info(f"✅ Found PNG date (fallback): {date_str} → {formatted_date}")
                            return formatted_date
                        except ValueError:
                            continue
                    
                    # If parsing fails, return as-is
                    logger.warning(f"⚠️ Could not parse PNG date format: {date_str}")
                    return date_str
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error parsing PNG date: {e}")
                    return date_str
        
        logger.warning("❌ PNG Invoice date not found")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error extracting PNG invoice date: {e}")
        return None

def extract_ban_png(pdf_path: str) -> str:
    """
    Extract account number (BAN) from Vodafone PNG invoice
    TODO: Update patterns based on actual PNG format
    """
    try:
        doc = fitz.open(pdf_path)
        first_page_text = doc[0].get_text()
        doc.close()
        
        logger.debug("🔍 Looking for PNG account number patterns...")
        
        # PNG-specific BAN patterns (to be updated)
        patterns = [
            r"Account Number[:\s]+([A-Z0-9\-]+)",
            r"Customer Number[:\s]+([A-Z0-9\-]+)",
            r"Account ID[:\s]+([A-Z0-9\-]+)",
            r"Customer ID[:\s]+([A-Z0-9\-]+)",
            r"BAN[:\s]+([A-Z0-9\-]+)"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, first_page_text, re.IGNORECASE)
            if match:
                ban = match.group(1).strip()
                logger.info(f"✅ Found PNG BAN: {ban}")
                return ban
        
        logger.warning("❌ PNG BAN not found")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error extracting PNG BAN: {e}")
        return None

def extract_entity_name_png(pdf_path: str) -> str:
    """
    Extract entity/customer name from Vodafone PNG invoice
    UPDATED: Based on actual PNG format showing "Speedcast PNG Limited"
    """
    try:
        doc = fitz.open(pdf_path)
        first_page_text = doc[0].get_text()
        doc.close()
        
        logger.debug("🔍 Looking for PNG entity name patterns...")
        
        # PRIMARY PATTERN: Look for company names with "Limited" or similar
        # Based on screenshot showing "Speedcast PNG Limited"
        lines = [line.strip() for line in first_page_text.splitlines() if line.strip()]
        
        for i, line in enumerate(lines):
            # Look for lines with company indicators
            if any(indicator in line for indicator in ['Limited', 'Ltd', 'Inc', 'Corp', 'Communications']):
                # Skip lines that contain Vodafone (vendor info)
                if 'vodafone' not in line.lower() and len(line) > 10:
                    # Additional validation: should be a reasonable company name
                    if not any(skip in line.lower() for skip in ['invoice', 'bill', 'total', 'amount', 'page']):
                        logger.info(f"✅ Found PNG entity name: {line}")
                        return line
        
        # FALLBACK PATTERNS: Traditional bill-to patterns
        fallback_patterns = [
            r"Bill To[:\s]*\n\s*([^\n]+)",
            r"Customer Name[:\s]+([^\n]+)",
            r"Account Holder[:\s]+([^\n]+)",
            r"Company Name[:\s]+([^\n]+)"
        ]
        
        for pattern in fallback_patterns:
            match = re.search(pattern, first_page_text, re.IGNORECASE)
            if match:
                entity_name = match.group(1).strip()
                # Clean up the entity name
                entity_name = re.sub(r'\s+', ' ', entity_name)  # Normalize spaces
                if len(entity_name) > 3:  # Reasonable length check
                    logger.info(f"✅ Found PNG entity name (fallback): {entity_name}")
                    return entity_name
        
        # ADVANCED FALLBACK: Look in specific area after customer info
        # Skip first few lines (header) and look in customer details area
        for i, line in enumerate(lines[5:25], 5):  # Lines 5-25 likely contain customer info
            if (len(line) > 15 and 
                any(word in line for word in ['Speedcast', 'Limited', 'Communications', 'Systems', 'Networks']) and
                not any(skip in line.lower() for skip in ['vodafone', 'invoice', 'total', 'amount', 'tax'])):
                logger.info(f"✅ Found PNG entity name (advanced): {line}")
                return line
        
        logger.warning("❌ PNG Entity name not found")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error extracting PNG entity name: {e}")
        return None

def extract_invoice_total_png(pdf_path: str) -> float:
    """
    Extract invoice total from Vodafone PNG invoice
    UPDATED: Based on actual PNG format "Total Current Charges (K) 16,775.00"
    """
    try:
        doc = fitz.open(pdf_path)
        first_page_text = doc[0].get_text()
        doc.close()
        
        logger.debug("🔍 Looking for PNG invoice total patterns...")
        
        # PRIMARY PATTERN: PNG-specific "Total Current Charges (K)" format
        primary_pattern = r"Total Current Charges \(K\)\s+([\d,]+\.?\d*)"
        match = re.search(primary_pattern, first_page_text, re.IGNORECASE)
        
        if match:
            total_str = match.group(1).replace(',', '')
            total = float(total_str)
            logger.info(f"✅ Found PNG total: 'Total Current Charges (K) {match.group(1)}' → {total:,.2f}")
            return total
        
        # SECONDARY PATTERN: "Total Due (K)" from the highlighted box
        total_due_pattern = r"Total Due \(K\)[:\s]+([\d,]+\.?\d*)"
        match = re.search(total_due_pattern, first_page_text, re.IGNORECASE)
        
        if match:
            total_str = match.group(1).replace(',', '')
            total = float(total_str)
            logger.info(f"✅ Found PNG total: 'Total Due (K) {match.group(1)}' → {total:,.2f}")
            return total
        
        # FALLBACK PATTERNS: Other PNG total patterns with Kina currency
        png_patterns = [
            r"Total Amount[:\s]*\(K\)\s*([\d,]+\.?\d*)",           # With (K) indicator
            r"Amount Due[:\s]*\(K\)\s*([\d,]+\.?\d*)",             # With (K) indicator
            r"Invoice Total[:\s]*\(K\)\s*([\d,]+\.?\d*)",          # With (K) indicator
            r"Balance Due[:\s]*\(K\)\s*([\d,]+\.?\d*)",            # With (K) indicator
            r"Final Amount[:\s]*\(K\)\s*([\d,]+\.?\d*)",           # With (K) indicator
            # Plain patterns without currency indicator
            r"Total Amount[:\s]+([\d,]+\.?\d*)",
            r"Amount Due[:\s]+([\d,]+\.?\d*)",
            r"Total Due[:\s]+([\d,]+\.?\d*)",
            r"Invoice Total[:\s]+([\d,]+\.?\d*)"
        ]
        
        for pattern in png_patterns:
            match = re.search(pattern, first_page_text, re.IGNORECASE)
            if match:
                total_str = match.group(1).replace(',', '')
                total = float(total_str)
                logger.info(f"✅ Found PNG total (fallback): {total:,.2f}")
                return total
        
        # ADVANCED FALLBACK: Look for lines containing "total" + "(K)" + number
        lines = first_page_text.split('\n')
        for line in lines:
            line = line.strip()
            if 'total' in line.lower() and '(k)' in line.lower():
                # Look for number pattern in the line
                number_match = re.search(r'([\d,]+\.?\d*)', line)
                if number_match:
                    try:
                        total_str = number_match.group(1).replace(',', '')
                        total = float(total_str)
                        # Reasonable amount check (PNG Kina amounts)
                        if 100.00 <= total <= 1000000.00:
                            logger.info(f"✅ Found PNG total (advanced): '{line.strip()}' → {total:,.2f}")
                            return total
                    except ValueError:
                        continue
        
        # LAST RESORT: Find largest reasonable number on first page
        numbers = re.findall(r'([\d,]+\.?\d{2})', first_page_text)
        if numbers:
            candidates = []
            for num_str in numbers:
                try:
                    num = float(num_str.replace(',', ''))
                    # Reasonable PNG invoice total range (in Kina)
                    if 1000.00 <= num <= 500000.00:  # Adjusted for PNG amounts
                        candidates.append(num)
                except ValueError:
                    continue
            
            if candidates:
                total = max(candidates)
                logger.warning(f"⚠️ Using largest reasonable number as PNG total estimate: {total:,.2f}")
                return total
        
        logger.warning("❌ PNG Invoice total not found")
        return 0.0
        
    except Exception as e:
        logger.error(f"❌ Error extracting PNG invoice total: {e}")
        return 0.0

# Shared utility functions with PNG-specific modifications
# Catalog lookup functions - COPIED FROM WORKING UK PARSER
def get_entity_id_from_catalog(entity_name: str) -> str:
    """
    Get Entity ID from ENTITY_CATALOG table by matching entity name
    Enhanced to handle PNG entity variations like "Speedcast PNG Limited" → "Speedcast Communications"
    
    Args:
        entity_name: Entity name extracted from PNG invoice (e.g., "Speedcast PNG Limited")
        
    Returns:
        Entity ID from catalog or None if not found
    """
    try:
        if not entity_name or entity_name == "UNKNOWN":
            return None
            
        # Check if config module is available
        try:
            from config.snowflake_config import get_snowflake_session
        except ImportError as e:
            logger.error(f"   ❌ Cannot import Snowflake config: {e}")
            logger.error(f"   ❌ Run this parser from the project root directory where config/ exists")
            return None
        
        session = get_snowflake_session()
        
        # Clean the extracted entity name for better matching
        clean_extracted = clean_entity_name_for_matching(entity_name)
        logger.info(f"   🔍 Matching PNG entity: '{entity_name}' (cleaned: '{clean_extracted}')")
        
        # Get all active entities from catalog
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
            
            # Strategy 1: Exact match (cleaned and normalized)
            if clean_extracted == clean_catalog:
                logger.info(f"   ✅ Exact PNG match: '{entity_name}' → {catalog_entity_id} ({catalog_entity_name})")
                return catalog_entity_id
            
            # Strategy 2: PNG-specific Ltd/Limited variant matching
            # Handle "Speedcast PNG Ltd" ↔ "Speedcast PNG Limited"
            if (clean_extracted.replace(' LIMITED', ' LTD') == clean_catalog.replace(' LIMITED', ' LTD') or
                clean_extracted.replace(' LTD', ' LIMITED') == clean_catalog.replace(' LTD', ' LIMITED')):
                logger.info(f"   ✅ PNG Ltd/Limited variant match: '{entity_name}' → {catalog_entity_id} ({catalog_entity_name})")
                return catalog_entity_id
            
            # Strategy 3: Core name match (handles business suffix variations)
            extracted_core = extract_core_company_name(clean_extracted)
            catalog_core = extract_core_company_name(clean_catalog)
            
            if extracted_core == catalog_core and len(extracted_core) > 3:
                logger.info(f"   ✅ Core PNG match: '{entity_name}' → {catalog_entity_id} ({catalog_entity_name})")
                logger.info(f"       Core names: '{extracted_core}' = '{catalog_core}'")
                return catalog_entity_id
        
        # Strategy 5: Fuzzy matching for close variants (fallback)
        best_match = find_best_fuzzy_match(clean_extracted, result)
        if best_match:
            entity_id, matched_name, similarity = best_match
            logger.info(f"   ✅ Fuzzy PNG match ({similarity:.1%}): '{entity_name}' → {entity_id} ({matched_name})")
            return entity_id
        
        logger.warning(f"   ⚠️ No PNG entity match found for '{entity_name}' in ENTITY_CATALOG")
        return None
        
    except Exception as e:
        logger.error(f"   ❌ Error looking up PNG entity ID for '{entity_name}': {e}")
        return None

def clean_entity_name_for_matching(name: str) -> str:
    """Clean entity name for better matching - Enhanced for PNG variants"""
    if not name:
        return ""
    
    # Convert to uppercase and remove extra spaces
    cleaned = name.upper().strip()
    
    # Remove common punctuation
    cleaned = cleaned.replace(',', '').replace('.', '').replace('-', ' ')
    
    # PNG-specific normalization: Handle Ltd/Limited variants
    cleaned = cleaned.replace(' LTD', ' LIMITED')
    cleaned = cleaned.replace(' CORP', ' CORPORATION')
    cleaned = cleaned.replace(' INC', ' INCORPORATED')
    
    # Normalize multiple spaces
    import re
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    return cleaned.strip()

def normalize_vendor_name_for_matching(name: str) -> str:
    """
    Normalize vendor name for PNG matching
    Handles: "Vodafone PNG Ltd" ↔ "VODAFONE PNG"
    """
    if not name:
        return ""
    
    # Convert to uppercase and clean
    cleaned = name.upper().strip()
    
    # Remove common punctuation
    cleaned = cleaned.replace(',', '').replace('.', '').replace('-', ' ')
    
    # PNG-specific vendor normalization
    # Remove business suffixes for vendor matching
    suffixes_to_remove = [' LTD', ' LIMITED', ' INC', ' INCORPORATED', ' CORP', ' CORPORATION']
    for suffix in suffixes_to_remove:
        if cleaned.endswith(suffix):
            cleaned = cleaned.replace(suffix, '').strip()
    
    # Normalize spaces
    import re
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    return cleaned.strip()

def extract_core_company_name(name: str) -> str:
    """Extract core company name by removing common business suffixes"""
    if not name:
        return ""
    
    # Common business suffixes to remove for matching
    suffixes = [
        'INC', 'INCORPORATED', 'CORP', 'CORPORATION', 'LLC', 'LTD', 'LIMITED',
        'CO', 'COMPANY', 'LP', 'LLP', 'PLLC', 'PC', 'ENTERPRISES', 'HOLDINGS',
        'GROUP', 'INTERNATIONAL', 'INTL', 'TECHNOLOGIES', 'TECH', 'SYSTEMS',
        'SOLUTIONS', 'SERVICES', 'COMMUNICATIONS', 'COMM', 'TELECOM'
    ]
    
    words = name.split()
    core_words = []
    
    for word in words:
        if word not in suffixes:
            core_words.append(word)
    
    # Keep meaningful core (at least first word)
    if not core_words and words:
        core_words = words[:1]
    
    return ' '.join(core_words)

def calculate_phrase_similarity(name1: str, name2: str) -> float:
    """
    Calculate similarity based on shared phrases/words
    Perfect for matching names that share most phrases but aren't identical
    
    Examples:
    - "SPEEDCAST PNG LTD" vs "SPEEDCAST PNG LIMITED" → 66% (2/3 unique words match)
    - "VODAFONE PNG LTD" vs "VODAFONE PNG" → 100% (all words in shorter name match)
    """
    if not name1 or not name2:
        return 0.0
    
    # Split into words and remove empty strings
    words1 = set([w for w in name1.split() if w])
    words2 = set([w for w in name2.split() if w])
    
    if not words1 or not words2:
        return 0.0
    
    # Calculate intersection and union
    common_words = words1.intersection(words2)
    all_unique_words = words1.union(words2)
    
    # Jaccard similarity: common words / all unique words
    similarity = len(common_words) / len(all_unique_words)
    
    return similarity

def calculate_vendor_phrase_similarity(vendor1: str, vendor2: str) -> float:
    """
    Special vendor similarity calculation
    Handles cases like "VODAFONE PNG LTD" vs "VODAFONE PNG"
    where shorter name is subset of longer name
    """
    if not vendor1 or not vendor2:
        return 0.0
    
    # Normalize both names
    norm1 = normalize_vendor_name_for_matching(vendor1)
    norm2 = normalize_vendor_name_for_matching(vendor2)
    
    # Split into words
    words1 = set([w for w in norm1.split() if w])
    words2 = set([w for w in norm2.split() if w])
    
    if not words1 or not words2:
        return 0.0
    
    # Special handling: if one is subset of other, high similarity
    if words1.issubset(words2) or words2.issubset(words1):
        return 0.9  # Very high similarity for subset matches
    
    # Otherwise use standard Jaccard similarity
    common_words = words1.intersection(words2)
    all_unique_words = words1.union(words2)
    
    return len(common_words) / len(all_unique_words)
def find_best_fuzzy_match(target: str, catalog_results: list, min_similarity: float = 0.6) -> tuple:
    """Find best fuzzy match using phrase-based similarity scoring (lowered threshold)"""
    if not target or not catalog_results:
        return None
    
    best_match = None
    best_similarity = 0.0
    
    for row in catalog_results:
        catalog_entity_id = row[0]
        catalog_entity_name = row[1]
        clean_catalog = clean_entity_name_for_matching(catalog_entity_name)
        
        # Use phrase similarity instead of simple word intersection
        similarity = calculate_phrase_similarity(target, clean_catalog)
        
        if similarity > best_similarity and similarity >= min_similarity:
            best_similarity = similarity
            best_match = (catalog_entity_id, catalog_entity_name, similarity)
    
    return best_match

def get_vendor_code_from_mapping(entity_id: str, vendor_name: str) -> str:
    """Get vendor code from ENTITY_VENDOR_MAPPING table - PNG version"""
    try:
        if not entity_id or not vendor_name:
            return None
            
        from config.snowflake_config import get_snowflake_session
        session = get_snowflake_session()
        
        query = f"""
            SELECT ENTITY_VENDOR_CODE
            FROM ENTITY_VENDOR_MAPPING
            WHERE ENTITY_ID = '{entity_id}' 
            AND VENDOR_NAME = '{vendor_name.replace("'", "''")}' 
            AND STATUS = 'Active'
            LIMIT 1
        """
        
        result = session.sql(query).collect()
        if result and len(result) > 0:
            vendor_code = result[0][0]
            logger.info(f"   ✅ Found PNG vendor mapping: Entity {entity_id} + {vendor_name} → {vendor_code}")
            return vendor_code
        else:
            logger.warning(f"   ⚠️ No PNG vendor mapping found for Entity {entity_id} + {vendor_name}")
            return None
            
    except Exception as e:
        logger.error(f"   ❌ Error querying PNG vendor mapping: {e}")
        return None

def get_vendor_currency(vendor_name: str) -> str:
    """Get currency from vendor catalog - PNG version"""
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
            logger.info(f"   ✅ Found PNG currency: {currency}")
            return currency
        else:
            logger.warning(f"   ⚠️ Vendor '{vendor_name}' not found in catalog - using default PGK")
            return 'PGK'  # Default for Vodafone PNG (Papua New Guinea Kina)
            
    except Exception as e:
        logger.warning(f"   ⚠️ Error looking up PNG vendor currency: {e}")
        return 'PGK'

# For integration testing only - use separate test module from project root
if __name__ == "__main__":
    print("⚠️  Don't run this parser directly!")
    print("📝 Use a test module from project root instead:")
    print("   python test_png_parser.py")
    print("")
    print("🔧 Or integrate with your batch processor:")
    print("   from parsers.headers.vodafone_png_header import extract_header")
    print("   header_df = extract_header('path/to/invoice.pdf')")