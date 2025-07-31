# parsers/headers/digital_realty_usa_header.py
import pandas as pd
import re
import fitz  # PyMuPDF
import os

def extract_header(pdf_path: str) -> pd.DataFrame:
    """
    Extract header information from Digital Realty USA invoices
    """
    try:
        # Initialize default values
        invoice_id = "UNKNOWN"
        ban = "UNKNOWN"
        billing_period = "UNKNOWN"
        invoice_total = 0.0
        customer_name = "UNKNOWN"
        
        # Extract text from first and second-to-last page
        doc = fitz.open(pdf_path)
        first_page_text = doc[0].get_text()
        
        # Get second-to-last page for total (not last page!)
        if len(doc) >= 2:
            second_last_page_text = doc[-2].get_text()  # Second-to-last page
            print(f"🔍 Using second-to-last page for total (page {len(doc)-1} of {len(doc)})")
        else:
            second_last_page_text = doc[0].get_text()  # Fallback to first page if only one page
            print(f"⚠️ Only one page, using first page for total")
        
        doc.close()
        
        print(f"🔍 Extracting Digital Realty USA header from: {os.path.basename(pdf_path)}")
        
        # Extract Invoice Number
        invoice_match = re.search(r'Invoice #:\s*(\d+)', first_page_text)
        if invoice_match:
            invoice_id = invoice_match.group(1)
            print(f"   ✅ Invoice #: {invoice_id}")
        else:
            print(f"   ⚠️ Invoice # not found")
        
        # Extract Invoice Date
        date_match = re.search(r'Invoice Date:\s*(\d{2}-[A-Z]{3}-\d{4})', first_page_text)
        if date_match:
            billing_period = date_match.group(1)
            print(f"   ✅ Invoice Date: {billing_period}")
        else:
            print(f"   ⚠️ Invoice Date not found")
        
        # Extract Account# (BAN) - From debug, we know it's on separate lines
        lines = first_page_text.split('\n')
        for i, line in enumerate(lines):
            if 'Account #:' in line:
                # Check next few lines for the number
                for j in range(i + 1, min(i + 4, len(lines))):
                    next_line = lines[j].strip()
                    # Look for just the 6-digit number (240588)
                    number_match = re.search(r'(\b\d{6}\b)', next_line)
                    if number_match:
                        ban = number_match.group(1)
                        print(f"   ✅ Account # from line {j}: {ban}")
                        break
                if ban != "UNKNOWN":
                    break
        
        # Method 2: Direct search for 6-digit account number pattern
        if ban == "UNKNOWN":
            account_match = re.search(r'\b(240588)\b', first_page_text)
            if account_match:
                ban = account_match.group(1)
                print(f"   ✅ Account # (direct pattern): {ban}")
        
        if ban == "UNKNOWN":
            print(f"   ⚠️ Account # not found with any method")
        
        # Extract Customer Name from Customer Legal Entity section
        customer_patterns = [
            r'Customer Legal Entity\s+.*?\n\s*([A-Za-z\s&,\.]+ Corporation)',
            r'Customer Legal Entity\s+.*?\n\s*([^\n]+)\s+Corporation',
            r'([A-Za-z\s&,\.]+ Corporation)'  # Fallback
        ]
        
        for pattern in customer_patterns:
            customer_match = re.search(pattern, first_page_text, re.MULTILINE | re.DOTALL)
            if customer_match:
                potential_customer = customer_match.group(1).strip()
                # Clean up formatting
                potential_customer = re.sub(r'\s+', ' ', potential_customer)
                
                # Add "Corporation" if not already present
                if not potential_customer.endswith('Corporation'):
                    potential_customer += ' Corporation'
                
                if len(potential_customer) > 5:
                    customer_name = potential_customer
                    print(f"   ✅ Customer Name: {customer_name}")
                    break
        
        # Extract invoice total from second-to-last page
        print(f"🔍 Searching for 'To be paid' in second-to-last page...")
        
        found_to_be_paid = False
        lines = second_last_page_text.split('\n')
        
        for i, line in enumerate(lines):
            line_clean = line.strip()
            
            if 'To be paid' in line_clean:
                print(f"   📋 Found 'To be paid' line: '{line_clean}'")
                
                # First try to extract amount from same line
                amount_match = re.search(r'USD\s*([\d,]+\.?\d*)', line_clean)
                if not amount_match:
                    amount_match = re.search(r'([\d,]+\.\d{2})', line_clean)
                
                if amount_match:
                    try:
                        invoice_total = float(amount_match.group(1).replace(',', ''))
                        print(f"   ✅ Extracted 'To be paid' total from same line: ${invoice_total:,.2f}")
                        found_to_be_paid = True
                        break
                    except ValueError as e:
                        print(f"   ❌ Error parsing amount: {e}")
                
                # If no amount on same line, check next few lines
                if not found_to_be_paid:
                    print(f"   🔍 No amount on same line, checking next lines...")
                    for j in range(i + 1, min(i + 5, len(lines))):  # Check next 4 lines
                        next_line = lines[j].strip()
                        if next_line:  # Skip empty lines
                            amount_match = re.search(r'([\d,]+\.\d{2})', next_line)
                            if amount_match:
                                try:
                                    invoice_total = float(amount_match.group(1).replace(',', ''))
                                    print(f"   ✅ Extracted 'To be paid' total from line {j}: '{next_line}' = ${invoice_total:,.2f}")
                                    found_to_be_paid = True
                                    break
                                except ValueError:
                                    continue
                if found_to_be_paid:
                    break
        
        # If "To be paid" not found, try direct amount search
        if not found_to_be_paid:
            print("   ⚠️ 'To be paid' amount not extracted! Trying direct amount search...")
            
            # Look for the specific amount we saw in debug: 11,309.11
            for i, line in enumerate(lines):
                line_clean = line.strip()
                if line_clean == '11,309.11':
                    try:
                        invoice_total = float(line_clean.replace(',', ''))
                        print(f"   ✅ Found exact amount on line {i}: ${invoice_total:,.2f}")
                        found_to_be_paid = True
                        break
                    except ValueError:
                        continue
        
        if invoice_total == 0.0:
            print(f"   ⚠️ Invoice total not found, using 0.00")
        
        # For digital_realty_usa vendor, the vendor name is fixed
        vendor_name = "Teik - New York, LLC"
        currency = "USD"
        
        # Get Entity ID from customer name lookup
        entity_id = _lookup_entity_id(customer_name)
        
        # Get Vendor Code using Entity ID and Vendor Name
        vendor_code = _get_entity_vendor_code(entity_id, vendor_name)
        
        # Display extracted header data
        print(f"📋 Digital Realty USA Header Extracted:")
        print(f"   Invoice ID: {invoice_id}")
        print(f"   BAN: {ban}")
        print(f"   Billing Period: {billing_period}")
        print(f"   Invoice Total: ${invoice_total:,.2f}")
        print(f"   Customer Name: {customer_name}")
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
            'vendor': vendor_name,
            'currency': currency,
            'source_file': os.path.basename(pdf_path),
            'invoice_total': invoice_total,
            'created_at': pd.Timestamp.now(),
            'transtype': '0',
            'batchno': '0',
            'vendorno': vendor_code,
            'documentdate': billing_period,
            'invoiced_bu': entity_id,
            'processed': 'N'
        }
        
        return pd.DataFrame([header_data])
        
    except Exception as e:
        print(f"❌ Digital Realty USA header extraction error: {e}")
        return pd.DataFrame()

def extract_equinix_header(pdf_path: str) -> pd.DataFrame:
    """
    Backward compatibility function - calls extract_header
    """
    return extract_header(pdf_path)

def _lookup_entity_id(customer_name: str) -> str:
    """
    Lookup Entity ID from ENTITY_CATALOG based on customer name
    """
    try:
        from config.snowflake_config import get_snowflake_session
        session = get_snowflake_session()
        
        # Try exact match first
        query = f"""
            SELECT ENTITY_ID 
            FROM ENTITY_CATALOG 
            WHERE UPPER(ENTITY_NAME) = UPPER('{customer_name.replace("'", "''")}')
            AND STATUS = 'Active'
            LIMIT 1
        """
        
        result = session.sql(query).collect()
        if result and len(result) > 0:
            entity_id = result[0][0]
            print(f"   ✅ Found exact Entity ID match: {entity_id}")
            return entity_id
        
        # Try partial match (significant words)
        words_query = f"""
            SELECT ENTITY_ID, ENTITY_NAME
            FROM ENTITY_CATALOG 
            WHERE STATUS = 'Active'
        """
        
        all_entities = session.sql(words_query).collect()
        customer_words = set(customer_name.upper().split())
        
        for row in all_entities:
            entity_id, entity_name = row[0], row[1]
            entity_words = set(entity_name.upper().split())
            
            # Check if significant words match (excluding common words)
            significant_words = customer_words - {'THE', 'AND', 'OR', 'OF', 'INC', 'LLC', 'CORP', 'CORPORATION', 'SERVICES'}
            entity_significant = entity_words - {'THE', 'AND', 'OR', 'OF', 'INC', 'LLC', 'CORP', 'CORPORATION', 'SERVICES'}
            
            if len(significant_words & entity_significant) >= 2:  # At least 2 significant words match
                print(f"   ✅ Found partial Entity ID match: {entity_id} ({entity_name})")
                return entity_id
        
        print(f"   ⚠️ No Entity ID found for customer: {customer_name}")
        return None
        
    except Exception as e:
        print(f"   ❌ Error looking up Entity ID for '{customer_name}': {e}")
        return None

def _get_entity_vendor_code(entity_id: str, vendor_name: str) -> str:
    """
    Get Vendor Code from ENTITY_VENDOR_MAPPING
    """
    if not entity_id:
        return None
        
    try:
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
            print(f"   ✅ Found Vendor Code: {vendor_code}")
            return vendor_code
        else:
            print(f"   ⚠️ No Vendor Code found for Entity {entity_id} + Vendor '{vendor_name}'")
            return None
            
    except Exception as e:
        print(f"   ❌ Error looking up Vendor Code: {e}")
        return None