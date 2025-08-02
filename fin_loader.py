# fin_loader.py - Updated for Phase 2 with correct table names
"""
Updated loader functions for Phase 2 separated architecture
Uses INVOICE_HEADER_DUP and INVOICE_LINE_ITEMS_DETAILED_DUP tables
"""

import pandas as pd
from snowflake.snowpark import Session
from typing import Dict, Any

def load_to_snowflake_header(session: Session, df_header: pd.DataFrame) -> bool:
    """
    Load header data to INVOICE_HEADER_DUP table
    
    Args:
        session: Snowflake session
        df_header: Header DataFrame with invoice metadata
        
    Returns:
        bool: Success status
    """
    try:
        if df_header.empty:
            print("Warning: Empty header DataFrame, skipping load")
            return False
            
        ## Next two lines inserted to delete record
        invoice_id = df_header.iloc[0]['invoice_id']
        session.sql(f"DELETE FROM INVOICE_HEADER_DUP WHERE INVOICE_ID = '{invoice_id}'").collect()
                
        # Convert DataFrame to Snowpark DataFrame
        snowpark_df = session.create_dataframe(df_header)
        
        # Write to INVOICE_HEADER_DUP table
        snowpark_df.write.mode("append").save_as_table("INVOICE_HEADER_DUP")
        
        print(f"✅ Successfully loaded {len(df_header)} header record(s) to INVOICE_HEADER_DUP")
        return True
        
    except Exception as e:
        print(f"❌ Error loading header data to Snowflake: {e}")
        return False

def load_to_snowflake_detailed(session: Session, df_details: pd.DataFrame) -> bool:
    """
    Load detail line items to INVOICE_LINE_ITEMS_DETAILED_DUP table
    
    Args:
        session: Snowflake session
        df_details: Detail DataFrame with line items
        
    Returns:
        bool: Success status
    """
    try:
        if df_details.empty:
            print("Warning: Empty details DataFrame, skipping load")
            return False

        ## Next two lines inserted to delete record
        invoice_id = df_details.iloc[0]['invoice_id']
        session.sql(f"DELETE FROM INVOICE_LINE_ITEMS_DETAILED_DUP WHERE INVOICE_ID = '{invoice_id}'").collect()
        
        # Convert DataFrame to Snowpark DataFrame
        snowpark_df = session.create_dataframe(df_details)
        
        # Write to INVOICE_LINE_ITEMS_DETAILED_DUP table
        snowpark_df.write.mode("append").save_as_table("INVOICE_LINE_ITEMS_DETAILED_DUP")
        
        print(f"✅ Successfully loaded {len(df_details)} detail record(s) to INVOICE_LINE_ITEMS_DETAILED_DUP")
        return True
        
    except Exception as e:
        print(f"❌ Error loading detail data to Snowflake: {e}")
        return False

def create_invoice_header_from_detail(df_details: pd.DataFrame, source_file: str = None) -> pd.DataFrame:
    """
    Create header record from detail records (legacy function for backward compatibility)
    Used when processing with legacy combined parsers
    
    Args:
        df_details: Detail DataFrame
        source_file: Source PDF file path
        
    Returns:
        DataFrame with single header record matching new schema
    """
    try:
        if df_details.empty:
            return pd.DataFrame()
        
        # Extract header info from first detail record
        first_record = df_details.iloc[0]
        
        # Calculate INVOICE_TOTAL: Sum(all amounts) + Sum(all taxes)
        total_amounts = df_details['amount'].sum() if 'amount' in df_details.columns else 0.0
        total_taxes = df_details['tax'].sum() if 'tax' in df_details.columns else 0.0
        invoice_total = total_amounts + total_taxes
        
        # Create header record with correct schema
        header_data = {
            'invoice_id': first_record.get('invoice_id', 'UNKNOWN'),
            'ban': first_record.get('ban', 'UNKNOWN'),
            'billing_period': first_record.get('billing_period', 'UNKNOWN'),
            'vendor': first_record.get('vendor_name', ''),  # PROVIDER renamed to VENDOR
            'source_file': source_file,
            'invoice_total': invoice_total,  # Calculated from detail records
            'created_at': pd.Timestamp.now(),
            'transtype': None,  # No value at this point
            'batchno': None,  # No value at this point
            'vendorno': None,  # Will be populated by identification
            'documentdate': first_record.get('billing_period', 'UNKNOWN'),  # Same as billing_period
            'invoiced_bu': None,  # Will be populated by identification
            'processed': 'N'  # Default to 'N' when first loaded
        }
        
        return pd.DataFrame([header_data])
        
    except Exception as e:
        print(f"Error creating header from detail: {e}")
        return pd.DataFrame()

# Table schema documentation for reference
def get_table_schemas() -> Dict[str, Dict]:
    """
    Return expected table schemas for documentation
    """
    return {
        'INVOICE_HEADER_DUP': {
            'invoice_id': 'VARCHAR(16777216) NOT NULL',
            'ban': 'VARCHAR(16777216)',
            'billing_period': 'VARCHAR(16777216)',
            'vendor': 'VARCHAR(16777216)',  # PROVIDER renamed to VENDOR
            'source_file': 'VARCHAR(16777216)',
            'invoice_total': 'FLOAT',  # Calculated from Sum(Amount + Tax)
            'created_at': 'TIMESTAMP_NTZ(9)',
            'transtype': 'VARCHAR(1)',  # No value at this point
            'batchno': 'VARCHAR(16777216)',  # No value at this point
            'vendorno': 'VARCHAR(16777216)',  # vendor_code from entity-vendor mapping
            'documentdate': 'TIMESTAMP_NTZ(9)',  # Same as billing_period
            'invoiced_bu': 'VARCHAR(16777216)',  # entity_id from identification
            'processed': 'VARCHAR(1)'  # Default 'N'
        },
        'INVOICE_LINE_ITEMS_DETAILED_DUP': {
            'invoice_id': 'VARCHAR',
            'item_number': 'VARCHAR',
            'ban': 'VARCHAR',
            'usoc': 'VARCHAR',
            'description': 'TEXT',
            'billing_period': 'DATE',
            'units': 'DECIMAL(10,2)',
            'unit_price': 'DECIMAL(10,2)',
            'amount': 'DECIMAL(10,2)',
            'tax_rate': 'DECIMAL(5,4)',
            'tax': 'DECIMAL(10,2)',
            'total': 'DECIMAL(10,2)',
            'currency': 'VARCHAR(3)',
            'regional_variant': 'VARCHAR',
            'vendor_name': 'VARCHAR',
            'source_file': 'VARCHAR',
            'extracted_at': 'TIMESTAMP'
        }
    }