# parsers/details/digital_realty_uk_detail.py
import pdfplumber
import pandas as pd
import re
import fitz
from typing import Dict, Any

def extract_equinix_items(pdf_path: str, header_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Extract line items from Digital Realty UK invoices using pdfplumber
    Much cleaner than Camelot for this invoice format
    """
    
    def get_tax_rate_from_last_page():
        """Extract tax rate from last page where totals are"""
        try:
            doc = fitz.open(pdf_path)
            last_page_text = doc[-1].get_text()
            doc.close()
            
            # Look for VAT percentage in patterns like "VAT (20%) Total"
            vat_match = re.search(r'VAT\s*\((\d+)%\)', last_page_text)
            if vat_match:
                rate = float(vat_match.group(1))
                print(f"   ✅ Found VAT rate: {rate}%")
                return rate / 100  # Convert to decimal
            
            print("   ⚠️ No VAT rate found, using 0")
            return 0.0
            
        except Exception as e:
            print(f"   ❌ Error extracting tax rate: {e}")
            return 0.0
    
    try:
        # Get tax rate from last page
        tax_rate = get_tax_rate_from_last_page()
        
        all_transactions = []
        
        # Open PDF with pdfplumber
        with pdfplumber.open(pdf_path) as pdf:
            print(f"Processing {len(pdf.pages)} pages with pdfplumber...")
            
            for page_num, page in enumerate(pdf.pages, 1):
                print(f"Processing Page {page_num}...")
                
                # Extract text from page
                page_text = page.extract_text()
                
                if not page_text:
                    print(f"No text found on Page {page_num}.")
                    continue
                
                # Split into lines
                lines = page_text.split('\n')
                
                # Look for transaction lines (lines that start with a number)
                for line in lines:
                    line = line.strip()
                    
                    # Skip empty lines and headers
                    if not line or 'Customer Ref' in line or 'Agreement' in line or 'INVOICE' in line:
                        continue
                    
                    # Look for transaction lines that start with a number followed by Order:
                    transaction_match = re.match(r'^(\d+)\s+(.+)', line)
                    
                    if transaction_match:
                        line_number = transaction_match.group(1)
                        rest_of_line = transaction_match.group(2)
                        
                        print(f"   📋 Found transaction line {line_number}: {rest_of_line[:60]}...")
                        
                        # Parse the transaction data using regex patterns
                        transaction = parse_transaction_line(line_number, rest_of_line, header_data, tax_rate)
                        
                        if transaction:
                            all_transactions.append(transaction)
                            print(f"   ✅ Extracted: {transaction['item_number']} | {transaction['description'][:40]}... | £{transaction['amount']:.2f}")
                        else:
                            print(f"   ⚠️ Could not parse line {line_number}")
        
        # Create final DataFrame
        if all_transactions:
            df = pd.DataFrame(all_transactions)
            df = df.reset_index(drop=True)
            
            # Ensure required columns exist
            required_columns = ['invoice_id', 'item_number', 'ban', 'usoc', 'description', 
                              'billing_period', 'units', 'amount', 'tax', 'total',
                              'currency', 'vendor_name', 'source_file', 'extracted_at',
                              'disputed', 'comment', 'comment_date']
            
            for col in required_columns:
                if col not in df.columns:
                    if col in ['units', 'amount', 'tax', 'total']:
                        df[col] = 0.0
                    elif col == 'disputed':
                        df[col] = False
                    elif col == 'comment_date':
                        df[col] = '1900-01-01 00:00:00'
                    else:
                        df[col] = ''
            
            # Ensure numeric columns are proper type
            for col in ['units', 'amount', 'tax', 'total']:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
            
            print(f"✅ Final result: {len(df)} transactions extracted")
            return df[required_columns]
        
        else:
            print("❌ No transactions found")
            required_columns = ['invoice_id', 'item_number', 'ban', 'usoc', 'description', 
                              'billing_period', 'units', 'amount', 'tax', 'total',
                              'currency', 'vendor_name', 'source_file', 'extracted_at',
                              'disputed', 'comment', 'comment_date']
            return pd.DataFrame(columns=required_columns)
    
    except Exception as e:
        print(f"❌ Error in Digital Realty extraction: {str(e)}")
        required_columns = ['invoice_id', 'item_number', 'ban', 'usoc', 'description', 
                          'billing_period', 'units', 'amount', 'tax', 'total',
                          'currency', 'vendor_name', 'source_file', 'extracted_at',
                          'disputed', 'comment', 'comment_date']
        return pd.DataFrame(columns=required_columns)

def parse_transaction_line(line_number: str, line_text: str, header_data: Dict[str, Any], tax_rate: float) -> dict:
    """
    Parse a single transaction line from the pdfplumber text
    Example: "Order: QW1-1211 00514147 Annual rental for QW1-1211_x000D_, 01-Jul-2024 to 31-Jul-2024 Cross Connect 1.00 GBP 51.70 GBP 51.70"
    """
    try:
        # Clean the line text
        line_text = line_text.replace('\n', ' ').replace('_x000D_', '').strip()
        
        # Extract Customer Reference (Order: XXX)
        customer_ref_match = re.search(r'Order:\s*([^\s]+)', line_text)
        customer_ref = customer_ref_match.group(1) if customer_ref_match else ''
        
        # Extract Agreement Number (8-digit number)
        agreement_match = re.search(r'\b(\d{8})\b', line_text)
        item_number = agreement_match.group(1) if agreement_match else ''
        
        if not item_number:
            return None
        
        # Extract Description - FIXED VERSION
        description = ''
        
        # Pattern 1: Look for "LHR19 - Cross Connect" pattern first (most descriptive)
        lhr_pattern = r'(LHR19[^0-9]+(?:Copper|Fiber)[^1-9]*)'
        lhr_match = re.search(lhr_pattern, line_text)
        if lhr_match:
            description = lhr_match.group(1).strip()
            print(f"   ✅ Found LHR19 description: '{description[:50]}...'")
        else:
            # Pattern 2: Look for "Annual rental for" pattern
            annual_pattern = r'Annual rental for\s+([^,]+(?:,[^0-9]+)?)'
            annual_match = re.search(annual_pattern, line_text)
            if annual_match:
                description = f"Annual rental for {annual_match.group(1).strip()}"
                print(f"   ✅ Found Annual rental description: '{description[:50]}...'")
            else:
                # Pattern 3: Text between agreement number and date (but exclude "1.00")
                desc_pattern = rf'{re.escape(item_number)}\s+(.+?)\s+\d{{2}}-[A-Za-z]{{3}}-\d{{4}}'
                desc_match = re.search(desc_pattern, line_text)
                if desc_match:
                    potential_desc = desc_match.group(1).strip()
                    # Skip if it's just "1.00" or other numeric values - FIXED LINE
                    if potential_desc != "1.00" and not re.match(r'^\d+\.\d+$', potential_desc):
                        description = potential_desc
                        print(f"   ✅ Found pattern description: '{description[:50]}...'")
        
        # If still no good description, try more patterns
        if not description or description == "1.00":
            print(f"   🔍 Debug - need better description from: {line_text}")
            
            # Look for any meaningful text that's not a number, date, or GBP
            meaningful_parts = []
            words = line_text.split()
            
            for i, word in enumerate(words):
                # Skip numbers, dates, currencies, and common keywords
                if (not re.match(r'^\d+(\.\d+)?$', word) and  # Not a number
                    not re.match(r'\d{2}-[A-Za-z]{3}-\d{4}', word) and  # Not a date
                    word != 'GBP' and word != 'to' and word != 'Cross' and word != 'Connect' and
                    word != item_number and not word.startswith('Order:')):
                    meaningful_parts.append(word)
            
            if meaningful_parts:
                description = ' '.join(meaningful_parts[:6])  # Take first 6 meaningful words
                print(f"   🔧 Constructed description: '{description}'")
            else:
                description = f"Service {item_number}"
                print(f"   ⚠️ Using fallback: '{description}'")
        
        # Clean up description
        description = description.replace('_x000D_', '').replace(',', '').strip()
        
        # If description is still just "1.00", use a better fallback
        if description == "1.00":
            description = f"Cross Connect Service {item_number}"
            print(f"   🔧 Fixed 1.00 issue: '{description}'")
        
        # Extract Period (date range)
        period_match = re.search(r'(\d{2}-[A-Za-z]{3}-\d{4}\s+to\s+\d{2}-[A-Za-z]{3}-\d{4})', line_text)
        period = period_match.group(1) if period_match else ''
        
        # Extract Quantity (usually 1.00)
        qty_match = re.search(r'\b(\d+\.\d{2})\b', line_text)
        units = float(qty_match.group(1)) if qty_match else 1.0
        
        # Extract Net Value (last GBP amount)
        gbp_amounts = re.findall(r'GBP\s*([\d,.]+)', line_text)
        if gbp_amounts:
            amount = float(gbp_amounts[-1].replace(',', ''))  # Take the last (Net Value)
        else:
            return None
        
        # Calculate tax and total
        tax = round(amount * tax_rate, 2)
        total = round(amount + tax, 2)
        
        # Create transaction record
        transaction = {
            'invoice_id': header_data.get('invoice_id', 'UNKNOWN'),
            'item_number': item_number,
            'ban': header_data.get('ban', 'UNKNOWN'),
            'usoc': description,
            'description': description,
            'billing_period': header_data.get('billing_period', 'UNKNOWN'),
            'units': units,
            'amount': amount,
            'tax': tax,
            'total': total,
            'currency': header_data.get('currency', 'UNKNOWN'),
            'vendor_name': header_data.get('vendor', 'UNKNOWN'),
            'source_file': header_data.get('source_file', 'UNKNOWN'),
            'extracted_at': pd.Timestamp.now(),
            'disputed': False,
            'comment': '',
            'comment_date': '1900-01-01 00:00:00'
        }
        
        return transaction
        
    except Exception as e:
        print(f"   ❌ Error parsing transaction line: {e}")
        return None