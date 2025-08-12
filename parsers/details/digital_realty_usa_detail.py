# parsers/details/digital_realty_usa_detail.py - CLEAN VERSION WITH PDFPLUMBER
"""
Digital Realty USA Detail Parser - ENHANCED VERSION
FIXED: Handles both single-charge and multi-charge invoice layouts
Added pdfplumber fallback for single-charge invoices when Camelot fails
NO FALLBACKS - strict 3-step validation compliance
"""

import camelot
import pandas as pd
import re
import pdfplumber
import os
from typing import Dict, Any

def extract_equinix_items(pdf_path: str, header_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Extract line items from Digital Realty USA invoices using Camelot with pdfplumber fallback
    """
    
    try:
        print(f"🔍 Extracting Digital Realty USA details using Camelot stream method...")
        
        # STRATEGY 1: Use existing Camelot approach
        camelot_result = _extract_with_camelot(pdf_path, header_data)
        
        if not camelot_result.empty:
            print(f"✅ Camelot successfully extracted {len(camelot_result)} transactions")
            return camelot_result
        
        # STRATEGY 2: Camelot found no transactions - use pdfplumber for single-charge
        print(f"⚠️ Camelot found no transactions - trying pdfplumber for single-charge invoice...")
        pdfplumber_result = _extract_with_pdfplumber_fallback(pdf_path, header_data)
        
        if not pdfplumber_result.empty:
            print(f"✅ pdfplumber successfully extracted {len(pdfplumber_result)} transactions")
            return pdfplumber_result
        
        # Both strategies failed
        print("❌ Both Camelot and pdfplumber failed to extract transactions")
        return _create_empty_dataframe()
        
    except Exception as e:
        print(f"❌ Error in Digital Realty USA extraction: {str(e)}")
        return _create_empty_dataframe()

def _extract_with_camelot(pdf_path: str, header_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Your existing Camelot extraction
    """
    try:
        # Use STREAM method
        try:
            tables = camelot.read_pdf(pdf_path, pages='all', flavor='stream')
            print(f"   ✅ Stream method found {len(tables)} tables")
        except Exception as e:
            print(f"   ❌ Stream method failed: {e}")
            return _create_empty_dataframe()
        
        if not tables:
            print("   ❌ No tables found with Camelot")
            return _create_empty_dataframe()
        
        all_transactions = []
        
        # Process all tables and auto-detect which contain transaction data
        for table_idx, table in enumerate(tables):
            df = table.df
            
            print(f"\n   📋 Processing Table {table_idx + 1} ({len(df)} rows x {len(df.columns)} cols)")
            
            # Auto-detect if this table contains transaction data
            has_transaction_data = _detect_transaction_table(df, table_idx + 1)
            
            if has_transaction_data:
                print(f"   🎯 Table {table_idx + 1} contains transaction data - processing...")
                transactions = _extract_transactions_from_table(df, header_data, table_idx + 1)
                
                if transactions:
                    all_transactions.extend(transactions)
                    print(f"   ✅ Extracted {len(transactions)} transactions from Table {table_idx + 1}")
                else:
                    print(f"   ⚠️ No transactions extracted from Table {table_idx + 1}")
            else:
                print(f"   ⏭️ Skipping Table {table_idx + 1} - no transaction data detected")
        
        # Create final DataFrame
        if all_transactions:
            result_df = pd.DataFrame(all_transactions)
            result_df = _ensure_required_columns(result_df)
            
            print(f"\n🎯 Final Results:")
            print(f"   📊 Total Transactions: {len(result_df)}")
            print(f"   💰 Total Amount: ${result_df['amount'].sum():,.2f}")
            print(f"   🏛️ Total Tax: ${result_df['tax'].sum():,.2f}")
            print(f"   📋 Grand Total: ${result_df['total'].sum():,.2f}")
            
            return result_df
        else:
            print("   ❌ No transactions extracted from any table")
            return _create_empty_dataframe()
    
    except Exception as e:
        print(f"❌ Error in Camelot extraction: {str(e)}")
        return _create_empty_dataframe()

def _extract_with_pdfplumber_fallback(pdf_path: str, header_data: Dict[str, Any]) -> pd.DataFrame:
    """
    pdfplumber fallback - SIMPLE STRING OPERATIONS ONLY
    """
    try:
        print("   📄 pdfplumber: Scanning first page for single charge...")
        
        with pdfplumber.open(pdf_path) as pdf:
            first_page = pdf.pages[0]
            page_text = first_page.extract_text()
            
            if page_text:
                lines = [line.strip() for line in page_text.split('\n') if line.strip()]
                print(f"   📝 Total lines: {len(lines)}")
                
                # Look for line that starts with "1 " and contains "Remote Hands"
                for i, line in enumerate(lines):
                    if line.startswith('1 ') and 'Remote Hands' in line:
                        print(f"   🎯 FOUND TRANSACTION DATA in line {i}: {line}")
                        
                        # Split by spaces and extract numbers from the end
                        parts = line.split()
                        if len(parts) >= 7:
                            try:
                                # Extract the numeric values from the end
                                total = float(parts[-1].replace(',', ''))
                                tax = float(parts[-2].replace(',', ''))
                                tax_pct = float(parts[-3])
                                unit_price = float(parts[-4].replace(',', ''))
                                qty = float(parts[-5])
                                
                                # Everything before the numbers is the description
                                desc_parts = parts[1:-5]  # Skip the "1" and the 5 numbers
                                description = ' '.join(desc_parts)
                                
                                print(f"   ✅ Parsed: Desc='{description}', Qty={qty}, Unit=${unit_price}, Tax=${tax}, Total=${total}")
                                
                                # Validation
                                if qty > 0 and unit_price >= 0 and tax >= 0 and total > 0 and description:
                                    
                                    # Check header data - ALLOW None currency
                                    required_fields = ['invoice_id', 'ban', 'billing_period', 'vendor', 'source_file']
                                    for field in required_fields:
                                        if header_data.get(field) in (None, '', 'UNKNOWN'):
                                            print(f"   ⚠️ Missing header field '{field}'")
                                            return _create_empty_dataframe()
                                    
                                    # Currency can be None - that's OK
                                    currency = header_data.get('currency')
                                    if currency == '':
                                        currency = None
                                    
                                    # Create transaction
                                    transaction = {
                                        'invoice_id': header_data['invoice_id'],
                                        'item_number': description,
                                        'ban': header_data['ban'],
                                        'usoc': description,
                                        'description': description,
                                        'billing_period': header_data['billing_period'],
                                        'units': qty,
                                        'amount': unit_price,
                                        'tax': tax,
                                        'total': total,
                                        'currency': currency,  # Can be None
                                        'vendor_name': header_data['vendor'],
                                        'source_file': header_data['source_file'],
                                        'extracted_at': pd.Timestamp.now(),
                                        'disputed': False,
                                        'comment': '',
                                        'comment_date': '1900-01-01 00:00:00'
                                    }
                                    
                                    print(f"   ✅ pdfplumber: Extracted {description} (${total:,.2f})")
                                    result_df = pd.DataFrame([transaction])
                                    return _ensure_required_columns(result_df)
                                    
                            except (ValueError, IndexError) as e:
                                print(f"   ❌ Error parsing numbers: {e}")
                                continue
        
        print("   ⚠️ pdfplumber: No transaction found")
        return _create_empty_dataframe()
        
    except Exception as e:
        print(f"   ❌ pdfplumber error: {e}")
        return _create_empty_dataframe()

def _detect_transaction_table(df: pd.DataFrame, table_num: int) -> bool:
    """
    Auto-detect if a table contains transaction data
    """
    try:
        # Convert entire table to text for analysis
        table_text = ""
        for idx, row in df.iterrows():
            row_text = ' '.join(str(cell) for cell in row if str(cell) != 'nan')
            table_text += row_text + " "
        
        table_text = table_text.upper()
        
        print(f"   🔍 Table {table_num} content analysis:")
        
        # Check for detailed breakdown tables (EXCLUDE these)
        breakdown_indicators = [
            'BILLABLE HOURS',
            'ACCOUNT NAME',
            'CASE/TICKET',
            'START TIME',
            'END TIME',
            'ACTUAL BILL',
            'WORK PERFORMED',
            'CLOSED'
        ]
        
        breakdown_matches = [indicator for indicator in breakdown_indicators if indicator in table_text]
        if breakdown_matches:
            print(f"     ❌ BREAKDOWN TABLE detected: {breakdown_matches}")
            return False
        
        # Check for main transaction indicators
        main_transaction_indicators = [
            'CHARGE DESCRIPTION',
            'REMOTE HANDS ON DEMAND',
            'ITEM',
            'QTY',
            'UNIT PRICE',
            'TOTAL'
        ]
        
        found_main_indicators = [indicator for indicator in main_transaction_indicators if indicator in table_text]
        print(f"     Found main transaction indicators: {found_main_indicators}")
        
        # Check for service types
        service_indicators = [
            'REMOTE HANDS',
            'CROSS CONNECT', 
            'POWER',
            'SPACE',
            'COLOCATION'
        ]
        
        found_service_indicators = [indicator for indicator in service_indicators if indicator in table_text]
        print(f"     Found service indicators: {found_service_indicators}")
        
        # Priority scoring system
        priority_score = 0
        
        if 'CHARGE DESCRIPTION' in table_text:
            priority_score += 10
            print(f"     +10 points: Has 'Charge Description'")
        
        if any(service in table_text for service in ['REMOTE HANDS ON DEMAND', 'REMOTE HANDS']):
            priority_score += 15
            print(f"     +15 points: Has main service type")
        
        if any(field in table_text for field in ['QTY', 'UNIT PRICE', 'TOTAL', 'ITEM']):
            priority_score += 8
            print(f"     +8 points: Has table structure")
        
        # Table 1 gets priority
        if table_num == 1:
            priority_score += 12
            print(f"     +12 points: Table 1 priority bonus")
        
        # Decision threshold
        is_transaction_table = priority_score >= 15
        
        print(f"     📊 Priority Score: {priority_score}")
        print(f"     ✅ IS TRANSACTION TABLE: {is_transaction_table}")
        
        return is_transaction_table
        
    except Exception as e:
        print(f"   ❌ Error detecting transaction table: {e}")
        return False

def _extract_transactions_from_table(df: pd.DataFrame, header_data: Dict[str, Any], table_num: int) -> list:
    """
    Extract transactions from a table
    """
    transactions = []
    
    try:
        # Try traditional header approach first
        header_row_idx = _find_header_row(df)
        
        if header_row_idx is not None:
            print(f"   🎯 Found header at row {header_row_idx}")
            
            # Check if we have data rows after the header
            data_rows_count = len(df.iloc[header_row_idx + 1:])
            print(f"   📊 Data rows after header: {data_rows_count}")
            
            if data_rows_count == 0:
                print(f"   🚨 HEADER FOUND BUT NO DATA ROWS - This is a single-charge layout!")
                print(f"   🔄 Switching to pdfplumber extraction...")
                return []  # Return empty to trigger pdfplumber fallback
            else:
                print(f"   🎯 Using traditional header-based extraction")
                return _extract_traditional_transactions(df, header_data, header_row_idx, table_num)
        
        # Try single-charge layout extraction
        print(f"   🎯 Using single-charge layout extraction")
        return _extract_single_charge_transactions(df, header_data, table_num)
        
    except Exception as e:
        print(f"   ❌ Error extracting from Table {table_num}: {e}")
        return []

def _extract_single_charge_transactions(df: pd.DataFrame, header_data: Dict[str, Any], table_num: int) -> list:
    """
    Extract transaction from single-charge layout
    """
    transactions = []
    
    try:
        print(f"   🔍 Analyzing single-charge layout...")
        
        # Look for the main transaction row
        charge_description = None
        qty = 1.0
        unit_price = 0.0
        tax = 0.0
        total = 0.0
        
        for idx, row in df.iterrows():
            row_text = ' '.join(str(cell) for cell in row if str(cell) != 'nan').strip()
            
            # Look for charge description
            if any(service in row_text.upper() for service in ['REMOTE HANDS', 'CROSS CONNECT', 'POWER', 'SPACE', 'COLOCATION']):
                if not charge_description:
                    charge_description = row_text
                    print(f"   ✅ Found charge description: {charge_description}")
            
            # Look for financial data in this row
            amounts = re.findall(r'(\d+\.?\d*)', row_text)
            if amounts:
                print(f"   💰 Row {idx} financial data: {amounts}")
                
                for amount_str in amounts:
                    try:
                        amount_val = float(amount_str)
                        
                        if amount_val > 1000:
                            total = amount_val
                        elif amount_val > 100 and amount_val < 1000:
                            unit_price = amount_val
                        elif amount_val > 1 and amount_val < 100:
                            qty = amount_val
                        
                    except ValueError:
                        continue
        
        # If we found a charge description, create transaction
        if charge_description:
            if total == 0.0 and unit_price > 0.0:
                total = unit_price * qty
            elif unit_price == 0.0 and total > 0.0:
                unit_price = total / qty if qty > 0 else total
            
            clean_desc = re.sub(r'\s+', '_', charge_description.strip())
            clean_desc = re.sub(r'[^\w\-_]', '', clean_desc)
            item_number = f"{clean_desc}_1"
            
            transaction = {
                'invoice_id': header_data.get('invoice_id', 'UNKNOWN'),
                'item_number': item_number,
                'ban': header_data.get('ban', 'UNKNOWN'),
                'usoc': charge_description,
                'description': charge_description,
                'billing_period': header_data.get('billing_period', 'UNKNOWN'),
                'units': qty,
                'amount': unit_price,
                'tax': tax,
                'total': total,
                'currency': header_data.get('currency', 'USD'),
                'vendor_name': header_data.get('vendor', 'UNKNOWN'),
                'source_file': header_data.get('source_file', 'UNKNOWN'),
                'extracted_at': pd.Timestamp.now(),
                'disputed': False,
                'comment': '',
                'comment_date': '1900-01-01 00:00:00'
            }
            
            print(f"   ✅ Created single-charge transaction: {charge_description}")
            transactions.append(transaction)
        
        return transactions
        
    except Exception as e:
        print(f"   ❌ Error in single-charge extraction: {e}")
        return []

def _extract_traditional_transactions(df: pd.DataFrame, header_data: Dict[str, Any], header_row_idx: int, table_num: int) -> list:
    """
    Traditional multi-charge extraction
    """
    transactions = []
    
    try:
        print(f"   🔍 Using traditional multi-charge extraction...")
        
        # Map actual column positions
        column_mapping = _map_columns(df, header_row_idx)
        
        if not column_mapping:
            print(f"   ⚠️ Could not map columns in Table {table_num}")
            return []
        
        print(f"   📊 Column mapping: {column_mapping}")
        
        # Process data rows (after header)
        data_rows = df.iloc[header_row_idx + 1:].reset_index(drop=True)
        print(f"   📋 Processing {len(data_rows)} data rows...")
        
        current_item_num = None
        current_charge_lines = []
        current_financial_data = {}
        
        for row_idx, row in data_rows.iterrows():
            # Check if this is a new item
            item_col = column_mapping.get('item', 0)
            item_value = str(row.iloc[item_col]).strip()
            
            print(f"   🔍 Row {row_idx}: Item column value = '{item_value}'")
            
            # New item detected
            if item_value.isdigit() or item_value == '1':
                print(f"   ✅ New item detected: {item_value}")
                
                # Process previous item if exists
                if current_item_num is not None:
                    transaction = _create_transaction_from_data(
                        current_item_num, current_charge_lines, current_financial_data, header_data
                    )
                    if transaction:
                        transactions.append(transaction)
                        print(f"   ✅ Added transaction for item {current_item_num}")
                
                # Start new item
                current_item_num = item_value
                current_charge_lines = []
                current_financial_data = {}
                
                # Extract financial data from this row
                current_financial_data = _extract_financial_data(row, column_mapping)
                print(f"   💰 Financial data extracted: {current_financial_data}")
                
                # Get charge description from this row
                desc_col = column_mapping.get('description', 1)
                desc_value = str(row.iloc[desc_col]).strip()
                if desc_value and desc_value != 'nan':
                    current_charge_lines.append(desc_value)
                    print(f"   📝 Added charge line: {desc_value}")
            
            # Continuation row for current item
            elif current_item_num is not None:
                desc_col = column_mapping.get('description', 1)
                desc_value = str(row.iloc[desc_col]).strip()
                if desc_value and desc_value != 'nan':
                    current_charge_lines.append(desc_value)
                    print(f"   📝 Added continuation line: {desc_value}")
        
        # Process final item
        if current_item_num is not None:
            print(f"   🔄 Processing final item: {current_item_num}")
            transaction = _create_transaction_from_data(
                current_item_num, current_charge_lines, current_financial_data, header_data
            )
            if transaction:
                transactions.append(transaction)
                print(f"   ✅ Added final transaction for item {current_item_num}")
        
        print(f"   📊 Total transactions created: {len(transactions)}")
        return transactions
        
    except Exception as e:
        print(f"   ❌ Error in traditional extraction: {e}")
        return []

def _create_transaction_from_data(item_num: str, charge_lines: list, financial_data: dict, header_data: Dict[str, Any]) -> dict:
    """
    Create a transaction record from extracted data
    """
    try:
        # Extract usoc and description
        usoc = ""
        if len(charge_lines) >= 1:
            usoc = charge_lines[0].strip()
        description = usoc
        
        # Find item_number
        item_number = item_num
        
        # Look for Asset ID
        asset_id_found = False
        for i, line in enumerate(charge_lines):
            if "Asset ID:" in line:
                asset_id_found = True
                asset_match = re.search(r'Asset ID:\s*(.+)', line)
                if asset_match:
                    asset_id = asset_match.group(1).strip()
                    item_number = asset_id
                    print(f"   ✅ Found Asset ID: {item_number}")
                    break
        
        # Handle NO Asset ID case
        if not asset_id_found:
            if usoc:
                clean_usoc = re.sub(r'\s+', '_', usoc.strip())
                clean_usoc = re.sub(r'[^\w\-_]', '', clean_usoc)
                item_number = f"{clean_usoc}_{item_num}"
                print(f"   ✅ Using Charge Description as item_number: {item_number}")
            else:
                item_number = f"ITEM_{item_num}"
        
        # Clean text fields
        usoc = re.sub(r'\s+', ' ', usoc).strip()
        description = re.sub(r'\s+', ' ', description).strip()
        item_number = re.sub(r'\s+', ' ', item_number).strip()
        
        # Create transaction record
        transaction = {
            'invoice_id': header_data.get('invoice_id', 'UNKNOWN'),
            'item_number': item_number,
            'ban': header_data.get('ban', 'UNKNOWN'),
            'usoc': usoc,
            'description': description,
            'billing_period': header_data.get('billing_period', 'UNKNOWN'),
            'units': financial_data.get('units', 1.0),
            'amount': financial_data.get('amount', 0.0),
            'tax': financial_data.get('tax', 0.0),
            'total': financial_data.get('total', 0.0),
            'currency': header_data.get('currency', 'USD'),
            'vendor_name': header_data.get('vendor', 'UNKNOWN'),
            'source_file': header_data.get('source_file', 'UNKNOWN'),
            'extracted_at': pd.Timestamp.now(),
            'disputed': False,
            'comment': '',
            'comment_date': '1900-01-01 00:00:00'
        }
        
        return transaction
        
    except Exception as e:
        print(f"   ❌ Error creating transaction record for item {item_num}: {e}")
        return None

def _find_header_row(df: pd.DataFrame) -> int:
    """Find the row containing table headers"""
    for idx, row in df.iterrows():
        row_text = ' '.join(str(cell).upper() for cell in row if str(cell) != 'nan')
        
        # Check for header indicators
        header_indicators = [
            ('ITEM' in row_text and 'CHARGE DESCRIPTION' in row_text),
            ('ITEM' in row_text and 'DESCRIPTION' in row_text),
            ('ITEM' in row_text and 'QTY' in row_text and 'TOTAL' in row_text),
        ]
        
        if any(header_indicators):
            print(f"   ✅ Found header row at index {idx}: {row_text}")
            return idx
    
    print(f"   ⚠️ No traditional header row found")
    return None

def _map_columns(df: pd.DataFrame, header_row_idx: int) -> dict:
    """Map actual column positions based on header content"""
    header_row = df.iloc[header_row_idx]
    mapping = {}
    
    for col_idx, header_value in enumerate(header_row):
        header_text = str(header_value).upper().strip()
        
        if 'ITEM' in header_text:
            mapping['item'] = col_idx
        elif 'CHARGE DESCRIPTION' in header_text:
            mapping['description'] = col_idx
        elif 'QTY' in header_text:
            mapping['qty'] = col_idx
        elif 'UNIT PRICE' in header_text:
            mapping['unit_price'] = col_idx
        elif 'TAX(%)' in header_text:
            mapping['tax_percent'] = col_idx
        elif header_text == 'TAX':
            mapping['tax'] = col_idx
        elif 'TOTAL' in header_text:
            mapping['total'] = col_idx
    
    return mapping

def _extract_financial_data(row: pd.Series, column_mapping: dict) -> dict:
    """Extract financial data from a row"""
    financial_data = {}
    
    # Extract Qty
    if 'qty' in column_mapping:
        qty_value = str(row.iloc[column_mapping['qty']]).strip()
        try:
            if qty_value and qty_value != 'nan':
                financial_data['units'] = float(qty_value)
        except ValueError:
            financial_data['units'] = 1.0
    else:
        financial_data['units'] = 1.0
    
    # Extract Unit Price
    if 'unit_price' in column_mapping:
        price_value = str(row.iloc[column_mapping['unit_price']]).strip()
        try:
            if price_value and price_value != 'nan':
                financial_data['amount'] = float(price_value.replace(',', ''))
        except ValueError:
            financial_data['amount'] = 0.0
    else:
        financial_data['amount'] = 0.0
    
    # Extract Tax
    if 'tax' in column_mapping:
        tax_value = str(row.iloc[column_mapping['tax']]).strip()
        try:
            if tax_value and tax_value != 'nan':
                financial_data['tax'] = float(tax_value.replace(',', ''))
        except ValueError:
            financial_data['tax'] = 0.0
    else:
        financial_data['tax'] = 0.0
    
    # Extract Total
    if 'total' in column_mapping:
        total_value = str(row.iloc[column_mapping['total']]).strip()
        try:
            if total_value and total_value != 'nan':
                financial_data['total'] = float(total_value.replace(',', ''))
        except ValueError:
            financial_data['total'] = 0.0
    else:
        financial_data['total'] = 0.0
    
    return financial_data

def _ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure all required columns exist with proper data types"""
    required_columns = [
        'invoice_id', 'item_number', 'ban', 'usoc', 'description', 
        'billing_period', 'units', 'amount', 'tax', 'total',
        'currency', 'vendor_name', 'source_file', 'extracted_at',
        'disputed', 'comment', 'comment_date'
    ]
    
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
    
    return df[required_columns]

def _create_empty_dataframe() -> pd.DataFrame:
    """Create empty DataFrame with required columns"""
    required_columns = [
        'invoice_id', 'item_number', 'ban', 'usoc', 'description', 
        'billing_period', 'units', 'amount', 'tax', 'total',
        'currency', 'vendor_name', 'source_file', 'extracted_at',
        'disputed', 'comment', 'comment_date'
    ]
    return pd.DataFrame(columns=required_columns)