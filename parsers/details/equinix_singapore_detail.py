# parsers/details/equinix_singapore_detail.py
"""
Equinix Singapore Detail Parser
Follows Germany parser pattern - reads all values from source document
"""

import pdfplumber
import pandas as pd
import numpy as np
from datetime import datetime
import re
from typing import Dict, Any
import os

def extract_equinix_items(pdf_path: str, header_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Extract line items from Singapore format Equinix invoices
    Uses pdfplumber with table detection
    """
    try:
        print(f"🇸🇬 Singapore Parser - Processing: {os.path.basename(pdf_path)}")
        
        # Extract using pdfplumber
        detail_df = _extract_with_pdfplumber_singapore(pdf_path)
        
        if detail_df.empty:
            print("⚠️ No detail records extracted")
            return pd.DataFrame()
        
        # Add header context
        detail_df = _add_header_context_singapore(detail_df, header_data, pdf_path)
        
        print(f"✅ Extracted {len(detail_df)} Singapore records")
        return detail_df
        
    except Exception as e:
        print(f"❌ Error in Singapore parser: {e}")
        return pd.DataFrame()

def _extract_with_pdfplumber_singapore(pdf_path: str) -> pd.DataFrame:
    """
    Extract using pdfplumber - same logic as Germany parser
    """
    all_transactions = []
    
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            print(f"  📄 Processing Page {page_num}...")
            
            tables = page.extract_tables()
            
            for table_num, table in enumerate(tables, start=1):
                if not table or len(table) < 3:
                    continue
                
                # Find header row with "Line#"
                header_row = None
                for i, row in enumerate(table):
                    if row and row[0] and "Line" in str(row[0]):
                        header_row = i
                        break
                
                if header_row is None:
                    continue
                
                print(f"    📊 Processing table {table_num}")
                
                # Detect column mapping
                column_mapping = _detect_singapore_columns(table, header_row)
                if not column_mapping:
                    continue
                
                # Process data rows
                data_rows = table[header_row+1:]
                transactions = _process_singapore_rows(data_rows, column_mapping)
                all_transactions.extend(transactions)
    
    if all_transactions:
        return pd.DataFrame(all_transactions)
    else:
        return pd.DataFrame()

def _detect_singapore_columns(table: list, header_row: int) -> Dict[str, int]:
    """
    Detect Singapore column positions - based on actual format
    """
    header = table[header_row] if header_row < len(table) else []
    if not header:
        return None
    
    header_clean = [str(cell).strip().upper() if cell else '' for cell in header]
    print(f"    🔍 Singapore headers: {header_clean}")
    
    mapping = {}
    
    # Map columns by header text (Singapore specific)
    for i, header_text in enumerate(header_clean):
        if 'BILLING' in header_text or 'AGREEMENT' in header_text:
            mapping['billing_agreement_idx'] = 2  # Fixed: data is always in column 2
        elif 'DESCRIPTION' in header_text:
            mapping['description_idx'] = i
        elif 'PRODUCT' in header_text and 'CODE' in header_text:
            mapping['usoc_idx'] = i
        elif header_text == 'QTY':
            mapping['units_idx'] = i
        elif header_text == 'MRC':  # Singapore uses MRC not AMOUNT
            mapping['amount_idx'] = i
        elif 'TAX %' in header_text or 'TAX%' in header_text:
            mapping['tax_pct_idx'] = i
        elif header_text == 'TAX' and 'TAX %' not in header_text:
            mapping['tax_idx'] = i
        elif header_text == 'TOTAL':
            mapping['total_idx'] = i
    
    print(f"    📋 Singapore mapping: {mapping}")
    
    # Validate required fields
    required = ['billing_agreement_idx', 'description_idx', 'amount_idx', 'total_idx']
    missing = [field for field in required if field not in mapping]
    
    if missing:
        print(f"    ❌ Missing required columns: {missing}")
        return None
    else:
        print(f"    ✅ All required columns found")
        return mapping

def _process_singapore_rows(data_rows: list, column_mapping: Dict[str, int]) -> list:
    """
    Process Singapore data rows
    """
    transactions = []
    
    for row in data_rows:
        if not row or not row[0]:
            continue
        
        line_value = str(row[0]).strip()
        
        # Skip non-numeric lines and totals
        if (not re.match(r'^[0-9]+(\.[0-9]+)?$', line_value) and 
            not re.match(r'^[0-9]+$', line_value)):  # Allow both "1" and "1.1"
            if 'Subtotal' not in line_value:  # Debug subtotals
                print(f"      ⏭️ Skipping non-numeric line: '{line_value}'")
            continue
        
        # Stop at Grand Total
        row_text = ' '.join([str(cell) for cell in row if cell])
        if 'Grand Total' in row_text:
            break
        
        # Extract transaction
        transaction = _extract_singapore_transaction(row, column_mapping)
        if transaction:
            transactions.append(transaction)
            print(f"      ✅ Extracted line {line_value}: ${transaction['amount']:.2f}")
    
    return transactions

def _extract_singapore_transaction(row: list, mapping: Dict[str, int]) -> Dict[str, Any]:
    """
    Extract Singapore transaction from row - read tax rate from document
    """
    def safe_get(idx, default=''):
        if idx is not None and idx < len(row) and row[idx]:
            return str(row[idx]).strip().replace('\n', ' ').replace('  ', ' ')
        return default
    
    def safe_numeric(idx, default=0.0):
        if idx is None or idx >= len(row) or not row[idx]:
            return default
        try:
            clean = str(row[idx]).replace(',', '').replace('$', '').replace('SGD', '').strip()
            return float(clean) if clean else default
        except:
            return default
    
    # Extract data
    amount = safe_numeric(mapping['amount_idx'])
    tax = safe_numeric(mapping.get('tax_idx'))
    tax_pct = safe_numeric(mapping.get('tax_pct_idx'))  # Read tax % from document
    total = safe_numeric(mapping['total_idx'])
    
    # Skip if both amount and tax are zero
    if amount == 0 and tax == 0:
        return None
    
    # Calculate tax if missing but tax percentage is provided in document
    if tax == 0 and amount > 0 and tax_pct > 0:
        tax = amount * (tax_pct / 100)
        print(f"        💰 Calculated tax from document: {amount} × {tax_pct}% = {tax:.2f}")
    
    return {
        'item_number': safe_get(mapping['billing_agreement_idx']),
        'description': safe_get(mapping['description_idx']),
        'usoc': safe_get(mapping.get('usoc_idx')),
        'units': safe_numeric(mapping.get('units_idx'), 1.0),
        'amount': amount,
        'tax': tax,
        'total': total,
        'tax_percentage': tax_pct if tax_pct > 0 else None  # Store for reference
    }

def _add_header_context_singapore(detail_df: pd.DataFrame, header_data: Dict[str, Any], pdf_path: str) -> pd.DataFrame:
    """
    Add header context - same as Germany parser
    """
    detail_df['invoice_id'] = header_data.get('invoice_id', 'UNKNOWN')
    detail_df['ban'] = header_data.get('ban', 'UNKNOWN')
    detail_df['billing_period'] = header_data.get('billing_period', 'UNKNOWN')
    detail_df['vendor_name'] = header_data.get('vendor', 'Equinix Singapore Pte Ltd')
    detail_df['currency'] = 'SGD'
    detail_df['source_file'] = os.path.basename(pdf_path)
    detail_df['invoiced_bu'] = header_data.get('invoiced_bu', '')
    detail_df['vendorno'] = header_data.get('vendorno', '')
    detail_df['extracted_at'] = pd.Timestamp.now()
    detail_df['regional_variant'] = 'equinix_singapore'
    
    return detail_df