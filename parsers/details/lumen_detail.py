# parsers/details/lumen_detail.py
"""
Enhanced Lumen Detail Parser
Extracts line items from Lumen/Level3 invoices including Finance Charges scenarios
"""

import fitz  # PyMuPDF
import re
import pandas as pd
import logging
from typing import Dict, Any, List
import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_equinix_items(pdf_path: str, header_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Extract detail line items from Lumen invoice
    Enhanced to handle Finance Charges scenarios
    Note: Function name kept as 'extract_equinix_items' for consistency with registry system
    
    Args:
        pdf_path: Path to Lumen PDF invoice
        header_data: Header context data (contains invoice_id, ban, finance charges info, etc.)
        
    Returns:
        DataFrame with detail records in standard format
    """
    try:
        logger.info(f"🔄 Extracting Lumen details from: {os.path.basename(pdf_path)}")
        
        # Get header context
        invoice_id = header_data.get('invoice_id')
        ban = header_data.get('ban')
        invoice_date = header_data.get('billing_period')
        has_finance_charges_only = header_data.get('has_finance_charges_only', False)
        has_zero_current_charges = header_data.get('has_zero_current_charges', False)
        finance_charges = header_data.get('finance_charges', 0.0)
        current_charges = header_data.get('current_charges', 0.0)
        
        logger.info(f"   Invoice ID: {invoice_id}")
        logger.info(f"   BAN: {ban}")
        logger.info(f"   Invoice Date: {invoice_date}")
        logger.info(f"   Current Charges: ${current_charges:,.2f}")
        logger.info(f"   Finance Charges: ${finance_charges:,.2f}")
        logger.info(f"   Finance Charges Only: {has_finance_charges_only}")
        logger.info(f"   Zero Current Charges: {has_zero_current_charges}")
        
        # Enhanced: Handle Finance Charges Only scenario
        if has_finance_charges_only:
            logger.info("📄 Processing Finance Charges Only scenario")
            finance_records = create_finance_charges_record(invoice_id, ban, invoice_date, finance_charges)
            if finance_records:
                standardized_df = standardize_lumen_records(pd.DataFrame([finance_records]), header_data)
                logger.info(f"✅ Created 1 Finance Charges record: ${finance_charges:,.2f}")
                return standardized_df
            else:
                logger.warning("Failed to create Finance Charges record")
                return pd.DataFrame()
        
        # Enhanced: Handle Zero Current Charges scenario (no finance charges, no line items)
        if has_zero_current_charges:
            logger.info("📄 Processing Zero Current Charges scenario")
            zero_charges_record = create_zero_current_charges_record(invoice_id, ban, invoice_date)
            if zero_charges_record:
                standardized_df = standardize_lumen_records(pd.DataFrame([zero_charges_record]), header_data)
                logger.info(f"✅ Created 1 Zero Current Charges record: $0.00")
                return standardized_df
            else:
                logger.warning("Failed to create Zero Current Charges record")
                return pd.DataFrame()
        
        # Original logic: Extract comprehensive details (MRC, Credits, Account Level)
        logger.info("📄 Processing standard invoice with line items")
        all_records = extract_lumen_comprehensive_details(pdf_path, invoice_id, ban, invoice_date)
        
        if all_records.empty:
            logger.warning("No detail records extracted from standard parsing")
            # Fallback: Check for finance charges or create zero charges record
            if finance_charges > 0.0:
                logger.info("📄 Fallback: Creating Finance Charges record")
                finance_records = create_finance_charges_record(invoice_id, ban, invoice_date, finance_charges)
                if finance_records:
                    standardized_df = standardize_lumen_records(pd.DataFrame([finance_records]), header_data)
                    logger.info(f"✅ Created fallback Finance Charges record: ${finance_charges:,.2f}")
                    return standardized_df
            else:
                logger.info("📄 Fallback: Creating Zero Current Charges record")
                zero_charges_record = create_zero_current_charges_record(invoice_id, ban, invoice_date)
                if zero_charges_record:
                    standardized_df = standardize_lumen_records(pd.DataFrame([zero_charges_record]), header_data)
                    logger.info(f"✅ Created fallback Zero Current Charges record: $0.00")
                    return standardized_df
            return pd.DataFrame()
        
        # Convert to standard format
        standardized_df = standardize_lumen_records(all_records, header_data)
        
        logger.info(f"✅ Extracted {len(standardized_df)} detail records")
        
        # Log summary by type
        charges = len([r for r in all_records.to_dict('records') if r.get('amount', 0) > 0])
        credits = len([r for r in all_records.to_dict('records') if r.get('amount', 0) < 0])
        logger.info(f"   📊 Summary: {charges} charges, {credits} credits")
        
        return standardized_df
        
    except Exception as e:
        logger.error(f"❌ Error extracting Lumen details: {e}")
        return pd.DataFrame()

def create_finance_charges_record(invoice_id: str, ban: str, invoice_date: str, finance_charges: float) -> Dict[str, Any]:
    """
    Create a single Finance Charges record
    
    Args:
        invoice_id: Invoice identifier
        ban: Billing account number
        invoice_date: Invoice date
        finance_charges: Finance charges amount
        
    Returns:
        Dictionary with Finance Charges record data
    """
    try:
        if finance_charges <= 0.0:
            logger.warning("Finance charges amount is zero or negative")
            return None
        
        record = {
            "invoice_id": invoice_id,
            "item_number": "Finance Charges",
            "ban": ban,
            "usoc": "Finance Charges",
            "description": "Finance Charges",
            "billing_period": invoice_date,
            "units": 1,
            "amount": finance_charges,
            "tax": 0.0,  # No tax on finance charges
            "total": finance_charges  # Total equals amount (no tax)
        }
        
        logger.info(f"Created Finance Charges record: ${finance_charges:,.2f}")
        return record
        
    except Exception as e:
        logger.error(f"Error creating Finance Charges record: {e}")
        return None

def create_zero_current_charges_record(invoice_id: str, ban: str, invoice_date: str) -> Dict[str, Any]:
    """
    Create a zero Current Charges record for invoices with no current charges and no finance charges
    
    Args:
        invoice_id: Invoice identifier
        ban: Billing account number
        invoice_date: Invoice date
        
    Returns:
        Dictionary with Zero Current Charges record data
    """
    try:
        record = {
            "invoice_id": invoice_id,
            "item_number": "Current Charges",
            "ban": ban,
            "usoc": "Current Charges",
            "description": "Current Charges",
            "billing_period": invoice_date,
            "units": 1,
            "amount": 0.0,  # Zero amount
            "tax": 0.0,     # Zero tax
            "total": 0.0    # Zero total
        }
        
        logger.info(f"Created Zero Current Charges record: $0.00")
        return record
        
    except Exception as e:
        logger.error(f"Error creating Zero Current Charges record: {e}")
        return None

def extract_lumen_comprehensive_details(pdf_path: str, invoice_id: str, ban: str, invoice_date: str) -> pd.DataFrame:
    """
    Comprehensive extraction of all Lumen detail types:
    1. MRC (Monthly Recurring Charges) blocks
    2. Current Month Credits
    3. Account Level Charges
    """
    doc = fitz.open(pdf_path)
    all_records = []
    
    # 1. Extract MRC blocks from Service Level Activity section
    mrc_records = extract_mrc_blocks(doc, invoice_id, ban, invoice_date)
    all_records.extend(mrc_records)
    logger.info(f"   📊 MRC records: {len(mrc_records)}")
    
    # 2. Extract Current Month Credits
    credit_records = extract_current_month_credits(doc, invoice_id, ban, invoice_date)
    all_records.extend(credit_records)
    logger.info(f"   💳 Credit records: {len(credit_records)}")
    
    # 3. Extract Account Level Charges
    account_records = extract_account_level_charges(doc, invoice_id, ban, invoice_date)
    all_records.extend(account_records)
    logger.info(f"   🏢 Account charge records: {len(account_records)}")
    
    doc.close()
    
    return pd.DataFrame(all_records)

def extract_mrc_blocks(doc, invoice_id: str, ban: str, invoice_date: str) -> List[Dict]:
    """Extract MRC blocks from Service Level Activity section"""
    
    # Find Service Level Activity start page
    service_start_page = find_service_level_activity_start_page(doc)
    if not service_start_page:
        logger.warning("Could not locate service section for MRC extraction")
        return []
    
    # Extract text from service section pages
    text = ""
    for i in range(service_start_page - 1, len(doc)):
        page_text = doc[i].get_text()
        text += page_text + "\n"
    
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    records = []
    i = 0
    
    def is_item_number(line, invoice_id, ban):
        """Identify valid item numbers"""
        # Must be alphanumeric, at least 6 chars, not the invoice ID or BAN
        if not re.fullmatch(r'[A-Z0-9/\-]{6,}', line):
            return False
        if line == invoice_id or line == ban:
            return False
        # Exclude phone numbers like 1-877-453-8353
        if re.fullmatch(r'1-\d{3}-\d{3}-\d{4}', line):
            return False
        return True
    
    # Extract MRC records
    while i < len(lines) - 6:
        current_line = lines[i]
        
        if is_item_number(current_line, invoice_id, ban):
            item_number = current_line
            logger.debug(f"Processing MRC item: {item_number}")
            j = i + 1
            
            while j < len(lines) - 5:
                line = lines[j]
                
                # Check for end of this item section
                if line.startswith("Total") and item_number in line:
                    break
                
                # Look for MRC billing period patterns
                billing_patterns = [
                    r'^MRC\s+([A-Za-z]{3} \d{1,2}, \d{4} - [A-Za-z]{3} \d{1,2}, \d{4})$',
                    r'^Monthly Recurring Charge\s+([A-Za-z]{3} \d{1,2}, \d{4} - [A-Za-z]{3} \d{1,2}, \d{4})$',
                    r'^RC\s+([A-Za-z]{3} \d{1,2}, \d{4} - [A-Za-z]{3} \d{1,2}, \d{4})$',
                ]
                
                billing_match = None
                for pattern in billing_patterns:
                    billing_match = re.match(pattern, line)
                    if billing_match:
                        break
                
                if billing_match:
                    try:
                        billing_period = billing_match.group(1)
                        
                        # Get surrounding lines with bounds checking
                        description = lines[j - 1] if j > 0 else ""
                        units = lines[j + 1] if j + 1 < len(lines) else ""
                        amount = lines[j + 2] if j + 2 < len(lines) else ""
                        tax = lines[j + 3] if j + 3 < len(lines) else ""
                        total = lines[j + 4] if j + 4 < len(lines) else ""
                        
                        # Validate numeric patterns
                        units_valid = re.fullmatch(r'\d+', units)
                        amount_valid = re.fullmatch(r'[\d,]+\.\d{2}', amount)
                        tax_valid = re.fullmatch(r'[\d,]+\.\d{2}', tax)
                        total_valid = re.fullmatch(r'[\d,]+\.\d{2}', total)
                        
                        if units_valid and amount_valid and tax_valid and total_valid:
                            try:
                                amount_val = float(amount.replace(",", ""))
                                tax_val = float(tax.replace(",", ""))
                                total_val = float(total.replace(",", ""))
                                units_val = int(units)
                                
                                # Include records with positive amounts OR positive tax
                                if amount_val > 0 or tax_val > 0:
                                    record = {
                                        "invoice_id": invoice_id,
                                        "item_number": item_number,
                                        "ban": ban,
                                        "usoc": description.strip(),
                                        "description": description.strip(),
                                        "billing_period": billing_period,
                                        "units": units_val,
                                        "amount": amount_val,
                                        "tax": tax_val,
                                        "total": total_val
                                    }
                                    records.append(record)
                                
                                j += 5  # Skip processed lines
                            except (ValueError, IndexError) as e:
                                logger.warning(f"Error parsing MRC numeric values: {e}")
                                j += 1
                        else:
                            j += 1
                    except Exception as e:
                        logger.error(f"Error processing MRC billing match: {e}")
                        j += 1
                else:
                    j += 1
            
            # Move to next potential item
            i = j if j > i else i + 1
        else:
            i += 1
    
    return records

def extract_current_month_credits(doc, invoice_id: str, ban: str, invoice_date: str) -> List[Dict]:
    """Extract Current Month Credits section"""
    
    # Find credits section pages
    credits_pages = find_credits_section_pages(doc)
    if not credits_pages:
        logger.info("No credits section found")
        return []
    
    all_credits = []
    
    for page_num in credits_pages:
        if page_num > len(doc):
            continue
        
        logger.debug(f"Processing credits on page {page_num}")
        
        # Get text from credits page
        text = doc[page_num - 1].get_text()
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        credits = []
        
        # Find the credits section
        in_credits_section = False
        
        for i, line in enumerate(lines):
            # Look for credits section start
            if "current month credits" in line.lower():
                in_credits_section = True
                continue
            
            # Skip header lines
            if in_credits_section and ("amount" in line.lower() and "total" in line.lower()):
                continue
            
            # Check for end of credits section
            if in_credits_section and ("service level activity" in line.lower() or 
                                     "billing details" in line.lower() or 
                                     "taxes, fees and surcharges" in line.lower() or
                                     "taxes fees and surcharges" in line.lower()):
                break
            
            # Process credit data lines
            if in_credits_section:
                # Service ID patterns for credits: 8-9 digit numbers or alphanumeric codes
                if (re.fullmatch(r'[0-9]{8,9}', line) or  # 8-9 digit service IDs
                    re.fullmatch(r'[A-Z]{2,}[0-9]{4,}', line)):  # Alphanumeric like BBSW51135
                    
                    service_id = line
                    
                    # Next line should be description
                    if i + 1 < len(lines):
                        description = lines[i + 1]
                        
                        # Next 3 lines should be amounts: amount, tax, total
                        if i + 4 < len(lines):
                            amount_line = lines[i + 2]
                            tax_line = lines[i + 3] 
                            total_line = lines[i + 4]
                            
                            # Validate all three are parenthetical amounts (credits)
                            if (re.fullmatch(r'\([\d,]+\.?\d*\)', amount_line.strip()) and
                                re.fullmatch(r'\([\d,]+\.?\d*\)', tax_line.strip()) and
                                re.fullmatch(r'\([\d,]+\.?\d*\)', total_line.strip())):
                                
                                try:
                                    def parse_credit_amount(amt_str):
                                        clean_str = amt_str.replace('(', '').replace(')', '').replace(',', '')
                                        return -float(clean_str)  # Credits are negative
                                    
                                    amount_val = parse_credit_amount(amount_line)
                                    tax_val = parse_credit_amount(tax_line)
                                    total_val = parse_credit_amount(total_line)
                                    
                                    credit_record = {
                                        "invoice_id": invoice_id,
                                        "item_number": service_id,
                                        "ban": ban,
                                        "usoc": description.strip(),
                                        "description": description.strip(),
                                        "billing_period": invoice_date,
                                        "units": 1,  # Always 1 for credits
                                        "amount": amount_val,
                                        "tax": tax_val,
                                        "total": total_val
                                    }
                                    
                                    credits.append(credit_record)
                                    
                                except (ValueError, IndexError) as e:
                                    logger.warning(f"Error parsing credit amounts: {e}")
        
        all_credits.extend(credits)
    
    return all_credits

def extract_account_level_charges(doc, invoice_id: str, ban: str, invoice_date: str) -> List[Dict]:
    """Extract Account Level Charges section"""
    
    # Find account level charges pages
    charges_pages = find_account_level_charges_pages(doc)
    if not charges_pages:
        logger.info("No account level charges section found")
        return []
    
    all_charges = []
    
    for page_num in charges_pages:
        if page_num > len(doc):
            continue
        
        text = doc[page_num - 1].get_text()
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        
        in_charges_section = False
        charges = []
        i = 0
        
        while i < len(lines):
            line = lines[i]
            
            if "account level charges" in line.lower():
                in_charges_section = True
                i += 1
                continue
            
            if in_charges_section and "total account level charges" in line.lower():
                break
            
            if in_charges_section:
                # Look for billing period (date pattern)
                billing_period_match = re.match(r'([A-Za-z]{3,9} \d{1,2}, \d{4})', line)
                if billing_period_match and i + 5 < len(lines):
                    billing_period_raw = billing_period_match.group(1)
                    
                    try:
                        # Convert to standard date format
                        try:
                            parsed_date = datetime.strptime(billing_period_raw, "%B %d, %Y")
                        except ValueError:
                            parsed_date = datetime.strptime(billing_period_raw, "%b %d, %Y")
                        billing_period = parsed_date.strftime("%Y-%m-%d")
                    except Exception:
                        billing_period = billing_period_raw  # fallback
                    
                    description = lines[i + 1].strip()
                    
                    # Optionally append extended description
                    extended_desc = ""
                    if i + 6 < len(lines):
                        extended_line = lines[i + 6].strip()
                        # Exclude headers, totals, or numeric-only lines
                        if not re.search(r'(total|units|amount|tax|billing)', extended_line.lower()) and not re.fullmatch(r'[\d,.\-]+', extended_line):
                            extended_desc = " " + extended_line
                    
                    try:
                        units = int(lines[i + 2].strip())
                        amount = float(lines[i + 3].replace(",", ""))
                        tax = float(lines[i + 4].replace(",", ""))
                        total = float(lines[i + 5].replace(",", ""))
                    except Exception as e:
                        logger.warning(f"Failed to parse account charge numeric fields: {e}")
                        i += 1
                        continue
                    
                    full_description = description + extended_desc
                    
                    record = {
                        "invoice_id": invoice_id,
                        "item_number": "Service",
                        "ban": ban,
                        "usoc": full_description,
                        "description": full_description,
                        "billing_period": billing_period,
                        "units": units,
                        "amount": amount,
                        "tax": tax,
                        "total": total
                    }
                    
                    charges.append(record)
                    i += 6 + (1 if extended_desc else 0)
                else:
                    i += 1
            else:
                i += 1
        
        all_charges.extend(charges)
    
    return all_charges

def standardize_lumen_records(records_df: pd.DataFrame, header_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Convert Lumen records to standard format matching Equinix structure
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
        'currency': header_data.get('currency', 'USD'),
        'vendor_name': header_data.get('vendor', 'Lumen Technologies'),
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
    
    logger.info(f"✅ Standardized {len(standardized)} Lumen records")
    
    return standardized

# Helper functions (copied from original fin_lumen_parser.py)
def find_service_level_activity_start_page(doc, search_labels=None, max_pages=20):
    """Find start page with multiple possible section labels"""
    if search_labels is None:
        search_labels = [
            "SERVICE LEVEL ACTIVITY",
            "Service Level Activity", 
            "Service Activity",
            "Billing Details",
            "Line Items"
        ]
    
    for i in range(min(len(doc), max_pages)):
        text = doc[i].get_text()
        for label in search_labels:
            if label.lower() in text.lower():
                logger.debug(f"Found section '{label}' on page {i + 1}")
                return i + 1  # 1-based page number
    
    logger.warning("Service level activity section not found")
    return None

def find_credits_section_pages(doc, service_start_page=None):
    """Find all pages containing CURRENT MONTH CREDITS section"""
    # If we know where service section starts, search before that
    max_page_to_search = service_start_page - 1 if service_start_page else len(doc)
    
    # Look for the actual credits section header
    search_labels = [
        "CURRENT MONTH CREDITS",
        "Current Month Credits"
    ]
    
    credits_pages = []
    
    for i in range(min(max_page_to_search, len(doc))):
        text = doc[i].get_text()
        for label in search_labels:
            if label.lower() in text.lower():
                logger.debug(f"Found credits section '{label}' on page {i + 1}")
                credits_pages.append(i + 1)  # 1-based page number
                break  # Found credits on this page, move to next page
    
    return credits_pages

def find_account_level_charges_pages(doc, service_start_page=None):
    """Find all pages containing ACCOUNT LEVEL CHARGES section"""
    search_labels = [
        "ACCOUNT LEVEL CHARGES",
        "Account Level Charges"
    ]
    
    charges_pages = []
    
    for i in range(len(doc)):
        text = doc[i].get_text()
        for label in search_labels:
            if label.lower() in text.lower():
                logger.debug(f"Found account level charges section '{label}' on page {i + 1}")
                charges_pages.append(i + 1)  # 1-based page number
                break  # Found charges on this page, move to next page
    
    return charges_pages

# For testing
if __name__ == "__main__":
    # Test with a sample file
    test_file = "invoices/sample.lumen.pdf"  # Update with actual test file
    
    if os.path.exists(test_file):
        print("🧪 Testing Enhanced Lumen Detail Parser")
        print("=" * 50)
        
        # Mock header data for testing Finance Charges scenario
        mock_header_finance = {
            'invoice_id': 'TEST123',
            'ban': 'BAN001',
            'billing_period': '2025-01-01',
            'source_file': 'test.pdf',
            'current_charges': 0.0,
            'finance_charges': 155.89,
            'has_finance_charges_only': True
        }
        
        # Mock header data for testing regular scenario
        mock_header_regular = {
            'invoice_id': 'TEST456',
            'ban': 'BAN002',
            'billing_period': '2025-01-01',
            'source_file': 'test.pdf',
            'current_charges': 1500.0,
            'finance_charges': 0.0,
            'has_finance_charges_only': False
        }
        
        print("🧪 Testing Finance Charges scenario:")
        detail_df_finance = extract_equinix_items(test_file, mock_header_finance)
        
        print("\n🧪 Testing Regular charges scenario:")
        detail_df_regular = extract_equinix_items(test_file, mock_header_regular)
        
        if not detail_df_finance.empty:
            print(f"✅ Finance Charges extraction successful! {len(detail_df_finance)} records")
            print(detail_df_finance[['item_number', 'description', 'amount', 'tax', 'total']].to_string())
        
        if not detail_df_regular.empty:
            print(f"✅ Regular extraction successful! {len(detail_df_regular)} records")
            print(detail_df_regular.head().to_string())
    else:
        print(f"❌ Test file not found: {test_file}")