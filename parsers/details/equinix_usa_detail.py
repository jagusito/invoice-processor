# parsers/details/equinix_usa_parser.py
"""
Equinix USA Format Parser
Based on proven working logic from your extract_recurring_charges function
"""

import camelot
import pandas as pd
import numpy as np
from datetime import datetime
import re
from typing import Dict, Any

def extract_equinix_items(pdf_path: str, header_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Extract line items from Equinix USA format invoices
    
    Args:
        pdf_path: Path to PDF invoice
        header_data: Header context data from header parser
        
    Returns:
        DataFrame with line items including header context
    """
    try:
        print(f"🇺🇸 USA Parser - Extracting details from: {pdf_path}")
        print(f"📋 Header context: {header_data.get('invoice_id', 'UNKNOWN')} | {header_data.get('vendor', 'UNKNOWN')}")
        
        # Use header data instead of extracting from PDF
        header_info = {
            'invoice_id': header_data.get('invoice_id', 'UNKNOWN'),
            'billing_period': header_data.get('billing_period', 'UNKNOWN'),
            'ban': header_data.get('ban', 'UNKNOWN')
        }
        
        # Read all pages to process using Camelot
        all_tables = camelot.read_pdf(pdf_path, pages='all', flavor='stream')
        
        # Track transactions from all sections
        all_transactions = []
        
        # Process each table looking for valid sections
        for i, table in enumerate(all_tables):
            page_num = table.page
            df = table.df
            
            print(f"  📄 Processing Page {page_num}, Table {i+1}...")
            
            # Skip tables with no data
            if len(df) < 2:
                continue
            
            # Check for transaction sections
            contains_recurring = df.astype(str).apply(
                lambda row: any('Recurring Charges' in str(cell) and 'Prior Period' not in str(cell) 
                             for cell in row if pd.notna(cell)), axis=1).any()
                             
            contains_onetime = df.astype(str).apply(
                lambda row: any('One Time Charges' in str(cell) 
                             for cell in row if pd.notna(cell)), axis=1).any()
            
            if contains_recurring:
                print(f"    📊 Found Recurring Charges section")
                transactions = extract_recurring_charges(df, header_info)
                if transactions:
                    all_transactions.extend(transactions)
                    
            elif contains_onetime:
                print(f"    📊 Found One Time Charges section") 
                transactions = extract_one_time_charges(df, header_info)
                if transactions:
                    all_transactions.extend(transactions)
        
        # Clean and process transactions
        if all_transactions:
            cleaned_transactions = clean_transactions(all_transactions)
            df_result = pd.DataFrame(cleaned_transactions)
            
            # Filter out rows where both units and amount are blank
            before_count = len(df_result)
            df_result = df_result[(df_result['units'].astype(str).str.strip() != '') & 
                                (df_result['amount'].astype(str).str.strip() != '')]
            removed_count = before_count - len(df_result)
            if removed_count > 0:
                print(f"Removed {removed_count} rows with blank units or amount fields")
            
            # Add header context
            df_result = add_header_context(df_result, header_data, pdf_path)
            
            print(f"✅ USA Parser - Extracted {len(df_result)} total transactions")
            return df_result
        else:
            print("⚠️ No transactions extracted")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"❌ Error in USA parser: {e}")
        return pd.DataFrame()

def find_column_indexes(df):
    """Find the indexes of important columns by header names - USA format"""
    column_indexes = {
        'line': 0,            # Line #
        'item_number': 1,     # Billing Agreement / SOF 
        'description': 2,     # Product Description
        'reference': 3,       # Reference
        'usoc': 4,            # Product Code
        'units': 5,           # Qty
        'unit_price': 6,      # Unit Price
        'amount': 7,          # MRC
        'discount': None,     # Discounts
        'tax': None,          # Tax
        'total': None         # Total
    }
    
    # Find discount, tax and total columns by header names
    for i in range(min(5, len(df))):
        for j, cell in enumerate(df.iloc[i]):
            if pd.notna(cell):
                cell_str = str(cell).strip().upper()
                if 'DISCOUNT' in cell_str:
                    column_indexes['discount'] = j
                elif cell_str == 'TAX':
                    column_indexes['tax'] = j
                elif 'TOTAL' in cell_str:  # Handles "TOTALLL"
                    column_indexes['total'] = j
    
    # If we couldn't find tax or total by name, estimate their positions
    if column_indexes['discount'] is not None:
        if column_indexes['tax'] is None:
            column_indexes['tax'] = column_indexes['discount'] + 1
        if column_indexes['total'] is None:
            column_indexes['total'] = column_indexes['discount'] + 2
    elif column_indexes['tax'] is None:
        column_indexes['tax'] = column_indexes['amount'] + 1
    if column_indexes['total'] is None:
        column_indexes['total'] = column_indexes['tax'] + 1
        
    return column_indexes

def safe_get_column(row, idx, default=''):
    """Safely get a column value from a row, handling index out of range"""
    try:
        return str(row[idx]).strip() if pd.notna(row[idx]) else default
    except (IndexError, KeyError):
        return default

def extract_recurring_charges(df, header_info):
    """Extract recurring charges using your proven logic"""
    transactions = []
    current_transaction = None
    data_start = 0
    
    # Print column count for debugging
    print(f"DataFrame has {len(df.columns)} columns")
    
    # Find where the data table begins (after Line # row)
    for i, row in df.iterrows():
        if pd.notna(row[0]) and 'Line #' in str(row[0]):
            data_start = i + 1
            break
    
    # Find important column indexes
    columns = find_column_indexes(df)
    print(f"Column indexes found: {columns}")
    
    # Track transaction parts for multi-row items
    item_parts = []
    desc_parts = []
    
    for idx, row in df.iterrows():
        if idx < data_start:
            continue
            
        line_marker = safe_get_column(row, columns['line'])
        
        # Skip subtotal rows
        if 'Subtotal' in line_marker:
            continue
        
        # Check if this is a primary row with a line number
        is_primary_row = bool(re.match(r'^\d+(\.\d+)?$', line_marker))
        
        # Handle start of a new transaction
        if is_primary_row:
            # Save the previous transaction if it exists
            if current_transaction:
                current_transaction['item_number'] = ' '.join(item_parts)
                current_transaction['description'] = ' '.join(desc_parts) if not current_transaction['description'] else current_transaction['description']
                transactions.append(current_transaction)
                
            # Reset for new transaction
            item_parts = []
            desc_parts = []
            
            # Add item number parts
            if pd.notna(row[columns['item_number']]) and str(row[columns['item_number']]).strip():
                item_parts.append(str(row[columns['item_number']]).strip())
            
            # Get amount, discount, tax and total
            amount = safe_get_column(row, columns['amount'])
            discount = safe_get_column(row, columns['discount']) if columns['discount'] is not None else ''
            tax = safe_get_column(row, columns['tax'])
            total = safe_get_column(row, columns['total'])
            
            # Get description
            description = safe_get_column(row, columns['description'])
            if description:
                desc_parts.append(description)
            
            # Create new transaction
            current_transaction = {
                'invoice_id': header_info['invoice_id'],
                'billing_period': header_info['billing_period'],
                'ban': header_info['ban'],
                'item_number': '',
                'description': description,
                'usoc': safe_get_column(row, columns['usoc']),
                'units': safe_get_column(row, columns['units']),
                'amount': amount,
                'discount': discount,
                'tax': tax,
                'total': total,
                'section_type': 'recurring',
                'line_number': line_marker
            }
        
        # Handle continuation rows
        elif current_transaction:
            # Add to item_number if there's content
            if pd.notna(row[columns['item_number']]) and str(row[columns['item_number']]).strip():
                item_parts.append(str(row[columns['item_number']]).strip())
            
            # Add to description if there's content
            if pd.notna(row[columns['description']]) and str(row[columns['description']]).strip():
                desc = str(row[columns['description']]).strip()
                if desc:
                    if current_transaction['description']:
                        current_transaction['description'] += " " + desc
                    else:
                        current_transaction['description'] = desc
                    desc_parts.append(desc)
    
    # Add the last transaction if it exists
    if current_transaction:
        current_transaction['item_number'] = ' '.join(item_parts)
        current_transaction['description'] = ' '.join(desc_parts) if not current_transaction['description'] else current_transaction['description']
        transactions.append(current_transaction)
        
    return transactions

def extract_one_time_charges(df, header_info):
    """Extract one-time charges using same logic as recurring"""
    transactions = []
    current_transaction = None
    data_start = 0
    
    # Find where the data table begins (after Line # row)
    for i, row in df.iterrows():
        if pd.notna(row[0]) and 'Line #' in str(row[0]):
            data_start = i + 1
            break
    
    # Find important column indexes
    columns = find_column_indexes(df)
    
    # Track transaction parts for multi-row items
    item_parts = []
    desc_parts = []
    
    for idx, row in df.iterrows():
        if idx < data_start:
            continue
            
        line_marker = safe_get_column(row, columns['line'])
        
        # Skip subtotal rows
        if 'Subtotal' in line_marker:
            continue
        
        # Check if this is a primary row with a line number
        is_primary_row = bool(re.match(r'^\d+(\.\d+)?$', line_marker))
        
        # Handle start of a new transaction
        if is_primary_row:
            # Save the previous transaction if it exists
            if current_transaction:
                current_transaction['item_number'] = ' '.join(item_parts)
                current_transaction['description'] = ' '.join(desc_parts) if not current_transaction['description'] else current_transaction['description']
                transactions.append(current_transaction)
                
            # Reset for new transaction
            item_parts = []
            desc_parts = []
            
            # Add item number parts
            if pd.notna(row[columns['item_number']]) and str(row[columns['item_number']]).strip():
                item_parts.append(str(row[columns['item_number']]).strip())
            
            # Get values from appropriate columns (NRC instead of MRC for one-time)
            amount = safe_get_column(row, columns['amount'])  # This might be NRC column
            discount = safe_get_column(row, columns['discount']) if columns['discount'] is not None else ''
            tax = safe_get_column(row, columns['tax'])
            total = safe_get_column(row, columns['total'])
            
            # Get description
            description = safe_get_column(row, columns['description'])
            if description:
                desc_parts.append(description)
            
            # Create new transaction
            current_transaction = {
                'invoice_id': header_info['invoice_id'],
                'billing_period': header_info['billing_period'],
                'ban': header_info['ban'],
                'item_number': '',
                'description': description,
                'usoc': safe_get_column(row, columns['usoc']),
                'units': safe_get_column(row, columns['units']),
                'amount': amount,
                'discount': discount,
                'tax': tax,
                'total': total,
                'section_type': 'one_time',
                'line_number': line_marker
            }
        
        # Handle continuation rows
        elif current_transaction:
            # Add to item_number if there's content
            if pd.notna(row[columns['item_number']]) and str(row[columns['item_number']]).strip():
                item_parts.append(str(row[columns['item_number']]).strip())
            
            # Add to description if there's content
            if pd.notna(row[columns['description']]) and str(row[columns['description']]).strip():
                desc = str(row[columns['description']]).strip()
                if desc:
                    if current_transaction['description']:
                        current_transaction['description'] += " " + desc
                    else:
                        current_transaction['description'] = desc
                    desc_parts.append(desc)
    
    # Add the last transaction if it exists
    if current_transaction:
        current_transaction['item_number'] = ' '.join(item_parts)
        current_transaction['description'] = ' '.join(desc_parts) if not current_transaction['description'] else current_transaction['description']
        transactions.append(current_transaction)
        
    return transactions

def clean_transactions(transactions):
    """Clean transaction data"""
    for t in transactions:
        for k in t:
            if pd.isna(t[k]) or (isinstance(t[k], float) and np.isnan(t[k])):
                t[k] = ''
        
        # Clean numeric fields
        for field in ['amount', 'tax', 'total', 'units']:
            if t[field]:
                try:
                    clean = re.sub(r'[^\d.]', '', str(t[field]))
                    t[field] = float(clean) if field != 'units' else clean
                except:
                    pass
        
        # Handle discounts (subtract from amount)
        if 'discount' in t and t['discount']:
            discount_value = t['discount']
            if '(' in discount_value and ')' in discount_value:
                try:
                    discount_match = re.search(r'\(([\d,.]+)\)', discount_value)
                    if discount_match:
                        discount_amount = float(re.sub(r'[^\d.]', '', discount_match.group(1)))
                        if t['amount']:
                            try:
                                amount_value = float(re.sub(r'[^\d.]', '', str(t['amount'])))
                                t['amount'] = amount_value - discount_amount
                            except (ValueError, TypeError):
                                pass
                except (ValueError, TypeError):
                    pass
        
        # Remove tracking fields
        for field in ['discount', 'section_type', 'line_number']:
            if field in t:
                del t[field]
    
    return transactions

def add_header_context(detail_df: pd.DataFrame, header_data: Dict[str, Any], pdf_path: str) -> pd.DataFrame:
    """Add header context fields to all detail records"""
    try:
        # Add header context to each detail record
        detail_df['vendor_name'] = header_data.get('vendor', 'UNKNOWN')
        detail_df['currency'] = header_data.get('currency', 'USD')
        detail_df['source_file'] = pdf_path
        detail_df['invoiced_bu'] = header_data.get('invoiced_bu', '')
        detail_df['vendorno'] = header_data.get('vendorno', '')
        detail_df['extracted_at'] = pd.Timestamp.now()
        detail_df['regional_variant'] = 'equinix_usa'
        
        print(f"  📋 Added header context - Invoice: {header_data.get('invoice_id')}, "
              f"Vendor: {header_data.get('vendor')}, Records: {len(detail_df)}")
        
        return detail_df
        
    except Exception as e:
        print(f"  ❌ Error adding header context: {e}")
        return detail_df

# For testing
if __name__ == "__main__":
    # Test with sample file
    test_file = "invoices/1751423522.equinix.pdf"
    
    # Test with header context
    header_context = {
        'invoice_id': '100210677374',
        'ban': '350327',
        'vendor': 'Equinix, Inc',
        'currency': 'USD',
        'billing_period': '2025-07-01',
        'invoiced_bu': '1002',
        'vendorno': 'V1002-100115'
    }
    
    result = extract_equinix_items(test_file, header_context)
    print(f"\n📊 USA Parser Test Results:")
    print(f"Records extracted: {len(result)}")
    if not result.empty:
        print(f"Sample records:")
        print(result[['item_number', 'description', 'amount', 'tax', 'total']].head(3).to_string())