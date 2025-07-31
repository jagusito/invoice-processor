# parsers/details/digital_realty_usa_detail.py
import camelot
import pandas as pd
import re
import os
from typing import Dict, Any

def extract_equinix_items(pdf_path: str, header_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Extract line items from Digital Realty USA invoices using Camelot
    
    Field Mapping:
    - Qty → units
    - Unit Price → amount  
    - Tax → tax
    - Total → total
    - Charge Description (first row) → usoc and description (same)
    - Asset ID line + next row → item_number
    """
    
    try:
        print(f"🔍 Extracting Digital Realty USA details using Camelot stream method...")
        
        # Use STREAM method (we know lattice doesn't work for this PDF)
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
        
        # Process each table - focus on tables 2 and 3 which contain transaction data
        for table_idx, table in enumerate(tables):
            df = table.df
            
            print(f"\n   📋 Processing Table {table_idx + 1} ({len(df)} rows x {len(df.columns)} cols)")
            
            # Skip table 1 (header info), table 4 (payment info), table 5 (footer)
            if table_idx == 0:  # Table 1 - header info
                print(f"   ⏭️ Skipping header table")
                continue
            elif table_idx >= 3:  # Tables 4+ - payment/footer info
                print(f"   ⏭️ Skipping non-transaction table")
                continue
            
            # Process transaction tables (should be tables 2 and 3, indices 1 and 2)
            print(f"   🔍 Processing transaction data table...")
            
            # Analyze table structure to find data rows
            transactions = _extract_transactions_from_table(df, header_data, table_idx + 1)
            
            if transactions:
                all_transactions.extend(transactions)
                print(f"   ✅ Extracted {len(transactions)} transactions from Table {table_idx + 1}")
            else:
                print(f"   ⚠️ No transactions found in Table {table_idx + 1}")
        
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
        print(f"❌ Error in Digital Realty USA extraction: {str(e)}")
        return _create_empty_dataframe()

def _extract_transactions_from_table(df: pd.DataFrame, header_data: Dict[str, Any], table_num: int) -> list:
    """
    Extract transactions from a single table with proper field mapping
    """
    transactions = []
    
    try:
        # Find header row (contains "Item", "Charge Description", etc.)
        header_row_idx = _find_header_row(df)
        
        if header_row_idx is None:
            print(f"   ⚠️ No header row found in Table {table_num}")
            return []
        
        print(f"   🔍 Found header row at index {header_row_idx}")
        
        # Map actual column positions
        column_mapping = _map_columns(df, header_row_idx)
        
        if not column_mapping:
            print(f"   ⚠️ Could not map columns in Table {table_num}")
            return []
        
        print(f"   📊 Column mapping: {column_mapping}")
        
        # Process data rows (after header)
        data_rows = df.iloc[header_row_idx + 1:].reset_index(drop=True)
        
        current_item_num = None
        current_charge_lines = []
        current_financial_data = {}
        
        for row_idx, row in data_rows.iterrows():
            # Check if this is a new item (starts with a number)
            item_col = column_mapping.get('item', 0)
            item_value = str(row.iloc[item_col]).strip()
            
            # New item detected
            if re.match(r'^\d+$', item_value):
                # Process previous item if exists
                if current_item_num is not None:
                    transaction = _create_transaction_from_data(
                        current_item_num, current_charge_lines, current_financial_data, header_data
                    )
                    if transaction:
                        transactions.append(transaction)
                
                # Start new item
                current_item_num = item_value
                current_charge_lines = []
                current_financial_data = {}
                
                # Extract financial data from this row
                current_financial_data = _extract_financial_data(row, column_mapping)
                
                # Get charge description from this row
                desc_col = column_mapping.get('description', 1)
                desc_value = str(row.iloc[desc_col]).strip()
                if desc_value and desc_value != 'nan':
                    current_charge_lines.append(desc_value)
            
            # Continuation row for current item
            elif current_item_num is not None:
                desc_col = column_mapping.get('description', 1)
                desc_value = str(row.iloc[desc_col]).strip()
                if desc_value and desc_value != 'nan':
                    current_charge_lines.append(desc_value)
        
        # Process final item
        if current_item_num is not None:
            transaction = _create_transaction_from_data(
                current_item_num, current_charge_lines, current_financial_data, header_data
            )
            if transaction:
                transactions.append(transaction)
        
        return transactions
        
    except Exception as e:
        print(f"   ❌ Error extracting from Table {table_num}: {e}")
        return []

def _create_transaction_from_data(item_num: str, charge_lines: list, financial_data: dict, header_data: Dict[str, Any]) -> dict:
    """
    Create a transaction record from extracted data
    """
    
    try:
        # Extract usoc and description (should be the same)
        usoc = ""
        if len(charge_lines) >= 1:
            usoc = charge_lines[0].strip()
        description = usoc  # Same as usoc per requirement
        
        # Find Asset ID line and extract item_number
        item_number = item_num  # Default to item number from table
        
        for i, line in enumerate(charge_lines):
            if "Asset ID:" in line:
                # Extract everything after "Asset ID:"
                asset_match = re.search(r'Asset ID:\s*(.+)', line)
                if asset_match:
                    asset_id = asset_match.group(1).strip()
                    
                    # Check if Asset ID ends with a dash or underscore (incomplete)
                    if asset_id.endswith('-') or asset_id.endswith('_'):
                        # Definitely needs continuation - get next line
                        if i + 1 < len(charge_lines):
                            next_line = charge_lines[i + 1].strip()
                            if next_line and not next_line.startswith(('Suite #:', 'Charging Period:', 'Order #:', 'Case:')):
                                item_number = f"{asset_id} {next_line}".strip()
                            else:
                                item_number = asset_id
                        else:
                            item_number = asset_id
                    else:
                        # Asset ID seems complete, but check if next line is a short continuation
                        if i + 1 < len(charge_lines):
                            next_line = charge_lines[i + 1].strip()
                            
                            # If next line is short (like "35") and not a label, it's probably a continuation
                            if (next_line and 
                                len(next_line) <= 10 and  # Short line
                                not next_line.startswith(('Suite #:', 'Charging Period:', 'Order #:', 'Case:')) and
                                not ':' in next_line):  # Not a label
                                item_number = f"{asset_id} {next_line}".strip()
                            else:
                                item_number = asset_id
                        else:
                            item_number = asset_id
                    break
        
        # Clean text fields
        usoc = re.sub(r'\s+', ' ', usoc).replace('_x000D_', '').replace('\n', ' ').strip()
        description = re.sub(r'\s+', ' ', description).replace('_x000D_', '').replace('\n', ' ').strip()
        item_number = re.sub(r'\s+', ' ', item_number).replace('_x000D_', '').replace('\n', ' ').strip()
        
        # If no good description, use usoc
        if not description and usoc:
            description = usoc
        
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
        if 'ITEM' in row_text and 'CHARGE DESCRIPTION' in row_text:
            return idx
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
    
    # Handle shifted columns - if Qty not found where expected, search for it
    if 'qty' not in mapping:
        # Look for columns with numeric values that could be quantities
        for col_idx in range(len(header_row)):
            if col_idx not in mapping.values():
                # Check if this column contains quantity-like data in subsequent rows
                col_data = df.iloc[header_row_idx + 1:, col_idx].dropna()
                if len(col_data) > 0 and any(str(val).strip().replace('.', '').isdigit() for val in col_data):
                    mapping['qty'] = col_idx
                    print(f"   🔧 Found shifted Qty column at index {col_idx}")
                    break
    
    return mapping

def _extract_financial_data(row: pd.Series, column_mapping: dict) -> dict:
    """Extract financial data from a row"""
    financial_data = {}
    
    # Extract Qty → units
    if 'qty' in column_mapping:
        qty_value = str(row.iloc[column_mapping['qty']]).strip()
        try:
            if qty_value and qty_value != 'nan':
                financial_data['units'] = float(qty_value)
        except ValueError:
            financial_data['units'] = 1.0
    else:
        financial_data['units'] = 1.0
    
    # Extract Unit Price → amount
    if 'unit_price' in column_mapping:
        price_value = str(row.iloc[column_mapping['unit_price']]).strip()
        try:
            if price_value and price_value != 'nan':
                financial_data['amount'] = float(price_value.replace(',', ''))
        except ValueError:
            financial_data['amount'] = 0.0
    else:
        financial_data['amount'] = 0.0
    
    # Extract Tax → tax
    if 'tax' in column_mapping:
        tax_value = str(row.iloc[column_mapping['tax']]).strip()
        try:
            if tax_value and tax_value != 'nan':
                financial_data['tax'] = float(tax_value.replace(',', ''))
        except ValueError:
            financial_data['tax'] = 0.0
    else:
        financial_data['tax'] = 0.0
    
    # Extract Total → total
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