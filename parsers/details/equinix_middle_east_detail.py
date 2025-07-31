# parsers/details/equinix_middle_east_detail.py
"""
Equinix Middle East Detail Parser
Similar to Germany pattern but handles dual currency (USD/AED) with USD priority
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
    Extract line items from Middle East format Equinix invoices
    Uses pdfplumber with Middle East-specific column mapping
    """
    try:
        print(f"🇦🇪 Middle East Parser - Processing: {os.path.basename(pdf_path)}")
        
        # Extract using pdfplumber
        detail_df = _extract_with_pdfplumber_middle_east(pdf_path)
        
        if detail_df.empty:
            print("⚠️ No detail records extracted")
            return pd.DataFrame()
        
        # Add header context
        detail_df = _add_header_context_middle_east(detail_df, header_data, pdf_path)
        
        print(f"✅ Extracted {len(detail_df)} Middle East records")
        return detail_df
        
    except Exception as e:
        print(f"❌ Error in Middle East parser: {e}")
        return pd.DataFrame()

def _extract_with_pdfplumber_middle_east(pdf_path: str) -> pd.DataFrame:
    """
    Extract using pdfplumber - handles all three charge sections
    """
    all_transactions = []
    
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            print(f"  📄 Processing Page {page_num}...")
            
            tables = page.extract_tables()
            
            for table_num, table in enumerate(tables, start=1):
                if not table or len(table) < 3:
                    continue
                
                # Check what type of charges section this is
                table_text = ' '.join([str(cell) for row in table[:3] for cell in row if cell])
                
                section_type = None
                if 'Recurring Charges' in table_text and 'Prior Period' not in table_text:
                    section_type = 'recurring'
                elif 'Prior Period Recurring Charges' in table_text:
                    section_type = 'prior_period'
                elif 'One Time Charges' in table_text:
                    section_type = 'one_time'
                
                if not section_type:
                    continue
                
                print(f"    📊 Found {section_type} charges section in table {table_num}")
                
                # Find header row with "Line"
                header_row = None
                for i, row in enumerate(table):
                    if row and row[0] and "Line" in str(row[0]):
                        header_row = i
                        break
                
                if header_row is None:
                    continue
                
                # Detect Middle East column mapping
                column_mapping = _detect_middle_east_columns(table, header_row)
                if not column_mapping:
                    continue
                
                # Process data rows
                data_rows = table[header_row+1:]
                transactions = _process_middle_east_rows(data_rows, column_mapping, section_type)
                all_transactions.extend(transactions)
    
    if all_transactions:
        return pd.DataFrame(all_transactions)
    else:
        return pd.DataFrame()

def _detect_middle_east_columns(table: list, header_row: int) -> Dict[str, int]:
    """
    Detect Middle East column positions based on your header:
    Line # | Billing Agreement | IBX | Product | Product Description and Details | Reference | Product Code | Qty | Unit Price | MRC | Tax % | Tax
    """
    header = table[header_row] if header_row < len(table) else []
    if not header:
        return None
    
    header_clean = [str(cell).strip().upper() if cell else '' for cell in header]
    print(f"    🔍 Middle East headers: {header_clean}")
    
    mapping = {}
    
    # Map columns by header text (Middle East specific - similar to Germany)
    for i, header_text in enumerate(header_clean):
        if 'BILLING' in header_text and 'AGREEMENT' in header_text:
            mapping['billing_agreement_idx'] = i
        elif 'PRODUCT DESCRIPTION' in header_text or ('DESCRIPTION' in header_text and 'DETAILS' in header_text):
            mapping['description_idx'] = i
        elif 'PRODUCT CODE' in header_text or ('PRODUCT' in header_text and 'CODE' in header_text):
            mapping['usoc_idx'] = i
        elif header_text == 'QTY':
            mapping['units_idx'] = i
        elif header_text == 'MRC':
            mapping['mrc_idx'] = i
        elif header_text == 'NRC':
            mapping['nrc_idx'] = i
        elif 'TAX %' in header_text or 'TAX%' in header_text:
            mapping['tax_pct_idx'] = i
        elif header_text == 'TAX' and 'TAX %' not in header_text:
            mapping['tax_idx'] = i
        elif header_text == 'TOTAL':
            mapping['total_idx'] = i
        elif header_text == 'REFERENCE':
            mapping['reference_idx'] = i
    
    print(f"    📋 Middle East mapping: {mapping}")
    
    # Validate required fields
    required = ['billing_agreement_idx', 'description_idx']
    # Must have either MRC or NRC
    has_amount = 'mrc_idx' in mapping or 'nrc_idx' in mapping
    
    missing = [field for field in required if field not in mapping]
    
    if missing:
        print(f"    ❌ Missing required columns: {missing}")
        return None
    elif not has_amount:
        print(f"    ❌ No amount column found (neither MRC nor NRC)")
        return None
    else:
        print(f"    ✅ All required columns found")
        return mapping

def _process_middle_east_rows(data_rows: list, column_mapping: Dict[str, int], section_type: str) -> list:
    """
    Process Middle East data rows for any section type
    """
    transactions = []
    
    for row in data_rows:
        if not row or not row[0]:
            continue
        
        line_value = str(row[0]).strip()
        
        # Skip non-numeric lines and totals
        if not re.match(r'^[0-9]+(\.[0-9]+)?$', line_value) and not re.match(r'^[0-9]+$', line_value):
            if 'Subtotal' not in line_value:
                print(f"      ⏭️ Skipping non-numeric line: '{line_value}'")
            continue
        
        # Stop at Grand Total or Subtotal
        row_text = ' '.join([str(cell) for cell in row if cell])
        if 'Grand Total' in row_text or 'Subtotal' in row_text:
            break
        
        # Extract transaction
        transaction = _extract_middle_east_transaction(row, column_mapping, section_type)
        if transaction:
            transactions.append(transaction)
            print(f"      ✅ Extracted {section_type} line {line_value}: ${transaction['amount']:,.2f}")
    
    return transactions

def _extract_middle_east_transaction(row: list, mapping: Dict[str, int], section_type: str) -> Dict[str, Any]:
    """
    Extract Middle East transaction from row - handles USD amounts (dual currency)
    """
    def safe_get(idx, default=''):
        if idx is not None and idx < len(row) and row[idx]:
            return str(row[idx]).strip().replace('\n', ' ').replace('  ', ' ')
        return default
    
    def safe_numeric(idx, default=0.0):
        if idx is None or idx >= len(row) or not row[idx]:
            return default
        try:
            # Handle Middle East currency format (USD priority, but may show AED)
            clean = str(row[idx]).replace(',', '').replace('$', '').replace('USD', '').replace('AED', '').strip()
            return float(clean) if clean else default
        except:
            return default
    
    # Choose amount column based on section type and what's available
    amount_idx = None
    
    if section_type == 'one_time':
        # For One Time Charges, prefer NRC
        if 'nrc_idx' in mapping:
            amount_idx = mapping['nrc_idx']
            print(f"        💰 Using NRC column for one-time charges")
        elif 'mrc_idx' in mapping:
            amount_idx = mapping['mrc_idx']
            print(f"        💰 Fallback to MRC column for one-time charges")
    else:
        # For Recurring charges, prefer MRC
        if 'mrc_idx' in mapping:
            amount_idx = mapping['mrc_idx']
            print(f"        💰 Using MRC column for recurring charges")
        elif 'nrc_idx' in mapping:
            amount_idx = mapping['nrc_idx']
            print(f"        💰 Fallback to NRC column for recurring charges")
    
    if amount_idx is None:
        print(f"        ❌ No amount column available for {section_type}")
        return None
    
    # Extract data
    amount = safe_numeric(amount_idx)
    tax = safe_numeric(mapping.get('tax_idx'))
    tax_pct = safe_numeric(mapping.get('tax_pct_idx'))
    total = safe_numeric(mapping.get('total_idx'))
    
    # Skip if both amount and tax are zero
    if amount == 0 and tax == 0:
        return None
    
    # Calculate VAT if missing (Middle East typically 5% VAT)
    if tax == 0 and amount > 0 and tax_pct > 0:
        tax = amount * (tax_pct / 100)
        print(f"        💰 Calculated Middle East VAT: ${amount:,.2f} × {tax_pct}% = ${tax:.2f}")
    
    return {
        'item_number': safe_get(mapping['billing_agreement_idx']),
        'description': safe_get(mapping['description_idx']),
        'usoc': safe_get(mapping.get('usoc_idx')),
        'units': safe_numeric(mapping.get('units_idx'), 1.0),
        'amount': amount,
        'tax': tax,
        'total': total,
        'reference': safe_get(mapping.get('reference_idx')),
        'tax_percentage': tax_pct if tax_pct > 0 else None,
        'section_type': section_type
    }

def _add_header_context_middle_east(detail_df: pd.DataFrame, header_data: Dict[str, Any], pdf_path: str) -> pd.DataFrame:
    """
    Add header context for Middle East - Use USD currency (not AED)
    """
    detail_df['invoice_id'] = header_data.get('invoice_id', 'UNKNOWN')
    detail_df['ban'] = header_data.get('ban', 'UNKNOWN')
    detail_df['billing_period'] = header_data.get('billing_period', 'UNKNOWN')
    detail_df['vendor_name'] = header_data.get('vendor', 'Equinix Middle East FZ-LLC')
    detail_df['currency'] = 'USD'  # Use USD as primary currency (as specified)
    detail_df['source_file'] = os.path.basename(pdf_path)
    detail_df['invoiced_bu'] = header_data.get('invoiced_bu', '')
    detail_df['vendorno'] = header_data.get('vendorno', '')
    detail_df['extracted_at'] = pd.Timestamp.now()
    detail_df['regional_variant'] = 'equinix_middle_east'
    
    return detail_df