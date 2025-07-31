# parsers/details/equinix_detail.py
"""
Enhanced Equinix Detail Parser - Phase 2
Builds on proven Camelot-based logic from fin_new_equinix_parser.py
Handles all 5 Equinix regional variants with header context passing
"""

import pdfplumber
import pandas as pd
import numpy as np
from datetime import datetime
import re
from typing import Dict, Any

def extract_equinix_items(pdf_path: str, header_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Extract line items from Equinix invoices (all regional variants)
    
    Args:
        pdf_path: Path to PDF invoice
        header_data: Header context data from header parser
        
    Returns:
        DataFrame with enhanced line items including header context
    """
    try:
        print(f"🔍 Extracting details from: {pdf_path}")
        print(f"📋 Header context: {header_data.get('invoice_id', 'UNKNOWN')} | {header_data.get('vendor', 'UNKNOWN')}")
        
        # Use enhanced extraction logic based on proven fin_new_equinix_parser.py
        detail_df = _extract_with_enhanced_camelot(pdf_path)
        
        if detail_df.empty:
            print("⚠️ No detail records extracted")
            return pd.DataFrame()
        
        # Add header context to all detail records
        detail_df = _add_header_context(detail_df, header_data, pdf_path)
        
        print(f"✅ Extracted {len(detail_df)} detail records with header context")
        return detail_df
        
    except Exception as e:
        print(f"❌ Error extracting Equinix details: {e}")
        return pd.DataFrame()

def _extract_with_enhanced_camelot(pdf_path: str) -> pd.DataFrame:
    """
    Enhanced extraction using pdfplumber (proven approach)
    Handles all Equinix regional variants by detecting table structure
    """
    all_transactions = []
    
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            print(f"  📄 Processing Page {page_num}...")
            
            # Extract tables from the page
            tables = page.extract_tables()
            
            # Process each table
            for table_num, table in enumerate(tables, start=1):
                if not table or len(table) < 3:
                    continue
                
                # Find the header row with "Line #"
                header_row = None
                for i, row in enumerate(table):
                    if row and row[0] and "Line #" in str(row[0]):
                        header_row = i
                        break
                
                if header_row is None:
                    continue
                
                print(f"    📊 Processing table {table_num} (header at row {header_row})")
                
                # Detect table variant and get column mapping
                column_mapping = _detect_table_variant(table, header_row)
                if not column_mapping:
                    print(f"    ⚠️ Could not detect table structure for table {table_num}")
                    continue
                
                print(f"    🔧 Detected variant: {column_mapping['variant']}")
                
                # Process data rows using detected mapping
                data_rows = table[header_row+1:]
                transactions = _process_data_rows(data_rows, column_mapping)
                all_transactions.extend(transactions)
    
    # Create DataFrame
    if all_transactions:
        result_df = pd.DataFrame(all_transactions)
        print(f"  ✅ Total extracted: {len(result_df)} transactions")
        return result_df
    else:
        print("  ⚠️ No transactions extracted")
        return pd.DataFrame()

def _detect_table_variant(table: list, header_row: int) -> Dict[str, Any]:
    """
    Detect Equinix table variant based on actual column headers
    
    Standard column order (with variations):
    Line# | SOF#/Billing Agreement | IBX | Product | Description | Reference | Product Code | Qty | Unit Price | MRC/NRC | [Discounts] | [Tax%] | Tax | Total
    
    Returns:
        Dict with variant name and column indices for key fields
    """
    try:
        # Get header row
        header = table[header_row] if header_row < len(table) else []
        if not header:
            return None
        
        # Convert header to strings and clean
        header_clean = [str(cell).strip().upper() if cell else '' for cell in header]
        table_width = len(header_clean)
        
        print(f"      🔍 Analyzing header: {header_clean}")
        print(f"      📏 Table width: {table_width}")
        
        # Find key column indices by header text
        mapping = {
            'variant': f'equinix_standard_{table_width}col',
            'billing_agreement_idx': None,
            'description_idx': None,
            'usoc_idx': None,
            'units_idx': None,
            'mrc_idx': None,
            'nrc_idx': None,
            'discount_idx': None,
            'tax_pct_idx': None,
            'tax_idx': None,
            'total_idx': None
        }
        
        # Map columns based on header text patterns
        for i, header_text in enumerate(header_clean):
            if 'BILLING' in header_text or 'SOF' in header_text:
                mapping['billing_agreement_idx'] = i
            elif 'DESCRIPTION' in header_text:
                mapping['description_idx'] = i
            elif 'PRODUCT CODE' in header_text:
                mapping['usoc_idx'] = i
            elif header_text == 'QTY':
                mapping['units_idx'] = i
            elif header_text == 'MRC':
                mapping['mrc_idx'] = i
            elif header_text == 'NRC':
                mapping['nrc_idx'] = i
            elif 'DISCOUNT' in header_text:
                mapping['discount_idx'] = i
            elif 'TAX %' in header_text or 'TAX%' in header_text:
                mapping['tax_pct_idx'] = i
            elif header_text == 'TAX':
                mapping['tax_idx'] = i
            elif header_text == 'TOTAL':
                mapping['total_idx'] = i
        
        # Determine primary amount field (MRC vs NRC)
        if mapping['mrc_idx'] is not None:
            mapping['amount_idx'] = mapping['mrc_idx']
            mapping['amount_type'] = 'MRC'
        elif mapping['nrc_idx'] is not None:
            mapping['amount_idx'] = mapping['nrc_idx']
            mapping['amount_type'] = 'NRC'
        else:
            print(f"      ⚠️ Neither MRC nor NRC column found")
            return None
        
        # Validate required fields
        required_fields = ['billing_agreement_idx', 'description_idx', 'amount_idx', 'total_idx']
        missing_fields = [field for field in required_fields if mapping[field] is None]
        
        if missing_fields:
            print(f"      ⚠️ Missing required columns: {missing_fields}")
            return None
        
        print(f"      ✅ Detected variant: {mapping['variant']} (Amount: {mapping['amount_type']})")
        print(f"      📋 Key columns - Agreement:{mapping['billing_agreement_idx']}, "
              f"Amount:{mapping['amount_idx']}, Tax:{mapping['tax_idx']}, Total:{mapping['total_idx']}")
        
        return mapping
        
    except Exception as e:
        print(f"      ❌ Error detecting table variant: {e}")
        return None

def _process_data_rows(data_rows: list, column_mapping: Dict[str, Any]) -> list:
    """
    Process data rows using detected column mapping with Equinix business rules
    """
    transactions = []
    variant = column_mapping['variant']
    
    for row in data_rows:
        if not row or not row[0]:  # Skip empty rows
            continue
        
        line_value = str(row[0]).strip()
        
        # Skip non-numeric line numbers, subtotals, and grand totals
        if (not re.match(r'^[0-9]+(\.[0-9]+)?$', line_value) or 
            'Subtotal' in line_value or 
            'Grand Total' in line_value):
            continue
        
        # Stop processing if we hit "Grand Total Charges"
        row_text = ' '.join([str(cell) for cell in row if cell])
        if 'Grand Total Charges' in row_text:
            print(f"      🛑 Stopping at Grand Total Charges")
            break
        
        # Extract and validate amount fields
        amount_idx = column_mapping['amount_idx']
        tax_idx = column_mapping['tax_idx']
        
        # Get raw values
        amount_raw = str(row[amount_idx]).strip() if amount_idx < len(row) and row[amount_idx] else ''
        tax_raw = str(row[tax_idx]).strip() if tax_idx and tax_idx < len(row) and row[tax_idx] else ''
        
        # Skip rows where both MRC/NRC and TAX are blank (per requirement #2)
        if not amount_raw and not tax_raw:
            continue
        
        # Extract transaction data using mapping
        transaction = _extract_transaction_data(row, column_mapping)
        if transaction:
            transactions.append(transaction)
    
    print(f"      ✅ Processed {len(transactions)} transactions ({variant})")
    return transactions

def _extract_transaction_data(row: list, mapping: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract transaction data from row using column mapping and Equinix business rules
    """
    try:
        def safe_get(idx, default=''):
            return str(row[idx]).strip().replace('\n', ' ') if idx is not None and idx < len(row) and row[idx] else default
        
        def safe_get_numeric(idx, default=0.0):
            if idx is None or idx >= len(row) or not row[idx]:
                return default
            try:
                clean_value = str(row[idx]).replace(',', '').replace('$', '').replace('€', '').replace('¥', '').strip()
                return float(clean_value) if clean_value else default
            except ValueError:
                return default
        
        # Extract basic fields
        transaction = {
            'item_number': safe_get(mapping['billing_agreement_idx']),
            'description': safe_get(mapping['description_idx']),
            'usoc': safe_get(mapping['usoc_idx']),
            'units': safe_get_numeric(mapping['units_idx'], 1.0)
        }
        
        # Extract financial fields with business logic
        raw_amount = safe_get_numeric(mapping['amount_idx'])  # MRC or NRC
        discount = safe_get_numeric(mapping['discount_idx'], 0.0)  # Discount if present
        tax_percentage = safe_get_numeric(mapping['tax_pct_idx'], 0.0)  # Tax % if present
        tax_amount = safe_get_numeric(mapping['tax_idx'])  # Tax amount
        total_amount = safe_get_numeric(mapping['total_idx'])  # Total
        
        # Calculate final amount: MRC/NRC minus discount (requirement #4)
        final_amount = raw_amount - discount
        
        # Calculate tax if we have percentage and amount but no tax amount (requirement #1)
        if tax_percentage > 0 and final_amount > 0 and tax_amount == 0:
            calculated_tax = final_amount * (tax_percentage / 100)
            tax_amount = calculated_tax
            print(f"        💰 Calculated tax: {final_amount} × {tax_percentage}% = {calculated_tax:.2f}")
        
        # Store calculated values
        transaction.update({
            'amount': final_amount,
            'tax': tax_amount,
            'total': total_amount,
            'raw_amount': raw_amount,  # For debugging
            'discount': discount if discount > 0 else None,  # Only store if discount exists
            'tax_percentage': tax_percentage if tax_percentage > 0 else None
        })
        
        # Validation: Skip if both amount and tax are zero/empty (requirement #2)
        if transaction['amount'] == 0 and transaction['tax'] == 0:
            return None
        
        # Debug logging for complex calculations
        if discount > 0 or tax_percentage > 0:
            print(f"        🧮 Complex calculation - Raw: {raw_amount}, Discount: {discount}, "
                  f"Final: {final_amount}, Tax%: {tax_percentage}, Tax: {tax_amount}")
        
        return transaction
        
    except Exception as e:
        print(f"        ⚠️ Error extracting transaction data: {e}")
        return None

def _add_header_context(detail_df: pd.DataFrame, header_data: Dict[str, Any], pdf_path: str) -> pd.DataFrame:
    """
    Add header context fields to all detail records
    Required fields: invoice_id, ban, vendor, source_file
    """
    try:
        # Add header context to each detail record
        detail_df['invoice_id'] = header_data.get('invoice_id', 'UNKNOWN')
        detail_df['ban'] = header_data.get('ban', 'UNKNOWN')
        detail_df['billing_period'] = header_data.get('billing_period', 'UNKNOWN')
        detail_df['vendor_name'] = header_data.get('vendor', 'UNKNOWN')  # Provider field
        detail_df['currency'] = header_data.get('currency', 'USD')
        detail_df['source_file'] = pdf_path
        detail_df['invoiced_bu'] = header_data.get('invoiced_bu', '')
        detail_df['vendorno'] = header_data.get('vendorno', '')
        detail_df['extracted_at'] = pd.Timestamp.now()
        
        # Add processing metadata
        detail_df['regional_variant'] = _detect_regional_variant(header_data.get('vendor', ''))
        
        print(f"  📋 Added header context - Invoice: {header_data.get('invoice_id')}, "
              f"Vendor: {header_data.get('vendor')}, Records: {len(detail_df)}")
        
        return detail_df
        
    except Exception as e:
        print(f"  ❌ Error adding header context: {e}")
        return detail_df

def _detect_regional_variant(vendor_name: str) -> str:
    """
    Detect regional variant from vendor name for metadata
    """
    if not vendor_name:
        return 'unknown'
    
    vendor_lower = vendor_name.lower()
    if 'germany' in vendor_lower:
        return 'equinix_germany'
    elif 'middle east' in vendor_lower:
        return 'equinix_middle_east'
    elif 'japan' in vendor_lower:
        return 'equinix_japan'
    elif 'singapore' in vendor_lower:
        return 'equinix_singapore'
    elif 'australia' in vendor_lower:
        return 'equinix_australia'
    else:
        return 'equinix_inc'

# For backward compatibility and testing
def new_extract_equinix(pdf_path: str) -> pd.DataFrame:
    """
    Backward compatibility function that mimics original new_extract_equinix
    Used when header context is not available
    """
    print("⚠️ Using backward compatibility mode (no header context)")
    
    # Extract without header context
    detail_df = _extract_with_enhanced_camelot(pdf_path)
    
    if not detail_df.empty:
        # Add minimal required fields for backward compatibility
        detail_df['invoice_id'] = 'UNKNOWN'
        detail_df['ban'] = 'UNKNOWN'
        detail_df['billing_period'] = 'UNKNOWN'
        detail_df['vendor_name'] = 'Equinix'
        detail_df['currency'] = 'USD'
        detail_df['source_file'] = pdf_path
        detail_df['extracted_at'] = pd.Timestamp.now()
        detail_df['regional_variant'] = 'unknown'
    
    return detail_df