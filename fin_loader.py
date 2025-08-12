# fin_loader.py - IMPROVED VERSION with better error handling
"""
Updated loader functions for Phase 2 with correct table names and column filtering
Uses INVOICE_HEADER_DUP and INVOICE_LINE_ITEMS_DETAILED_DUP tables
"""

import pandas as pd
from snowflake.snowpark import Session
from typing import Dict, Any
import logging

# Set up logging
logger = logging.getLogger(__name__)

def load_to_snowflake_header(session: Session, df_header: pd.DataFrame) -> bool:
    """
    Load header data to INVOICE_HEADER_DUP table with column filtering
    
    Args:
        session: Snowflake session
        df_header: Header DataFrame with invoice metadata
        
    Returns:
        bool: Success status
    """
    try:
        if df_header.empty:
            logger.warning("Empty header DataFrame, skipping load")
            return False
        
        # FIXED: Filter to only expected columns for INVOICE_HEADER_DUP (14 columns)
        expected_columns = [
            'invoice_id', 'ban', 'billing_period', 'vendor', 'source_file',
            'invoice_total', 'created_at', 'transtype', 'batchno', 'vendorno',
            'documentdate', 'invoiced_bu', 'processed', 'currency'
        ]
        
        # Create filtered DataFrame with only expected columns
        filtered_df = pd.DataFrame()
        for col in expected_columns:
            if col in df_header.columns:
                filtered_df[col] = df_header[col]
            else:
                # Add missing columns with default values
                if col == 'created_at':
                    filtered_df[col] = pd.Timestamp.now()
                elif col == 'processed':
                    filtered_df[col] = 'N'
                elif col == 'invoice_total':
                    filtered_df[col] = 0.0
                else:
                    filtered_df[col] = None
        
        logger.info(f"📊 Header columns before filtering: {len(df_header.columns)}")
        logger.info(f"📊 Header columns after filtering: {len(filtered_df.columns)}")
        
        # Delete existing record
        invoice_id = filtered_df.iloc[0]['invoice_id']
        try:
            session.sql(f"DELETE FROM INVOICE_HEADER_DUP WHERE INVOICE_ID = '{invoice_id}'").collect()
        except Exception as delete_error:
            logger.warning(f"⚠️ Could not delete existing header record: {delete_error}")
                
        # Convert DataFrame to Snowpark DataFrame with error handling
        try:
            snowpark_df = session.create_dataframe(filtered_df)
        except Exception as df_error:
            logger.error(f"❌ Error creating Snowpark DataFrame for header: {df_error}")
            logger.error(f"❌ DataFrame dtypes: {filtered_df.dtypes}")
            logger.error(f"❌ DataFrame sample: {filtered_df.head(1).to_dict()}")
            return False
        
        # Write to INVOICE_HEADER_DUP table with error handling
        try:
            snowpark_df.write.mode("append").save_as_table("INVOICE_HEADER_DUP")
            logger.info(f"✅ Successfully loaded {len(filtered_df)} header record(s) to INVOICE_HEADER_DUP")
            return True
        except Exception as write_error:
            logger.error(f"❌ Error writing header to INVOICE_HEADER_DUP: {write_error}")
            return False
        
    except Exception as e:
        logger.error(f"❌ Error loading header data to Snowflake: {e}")
        logger.error(f"❌ DataFrame columns: {list(df_header.columns) if not df_header.empty else 'Empty DataFrame'}")
        return False

def load_to_snowflake_detailed(session: Session, df_details: pd.DataFrame) -> bool:
    """
    Load detail line items to INVOICE_LINE_ITEMS_DETAILED_DUP table with column filtering
    
    Args:
        session: Snowflake session
        df_details: Detail DataFrame with line items
        
    Returns:
        bool: Success status
    """
    try:
        if df_details.empty:
            logger.warning("Empty details DataFrame, skipping load")
            return False

        # FIXED: Filter to only expected columns for INVOICE_LINE_ITEMS_DETAILED_DUP (13 columns)
        expected_columns = [
            'invoice_id', 'item_number', 'ban', 'usoc', 'description',
            'billing_period', 'units', 'amount', 'tax', 'total',
            'disputed', 'comment', 'comment_date'
        ]
        
        # Create filtered DataFrame with only expected columns
        filtered_df = pd.DataFrame()
        for col in expected_columns:
            if col in df_details.columns:
                filtered_df[col] = df_details[col]
            else:
                # Add missing columns with default values
                if col == 'extracted_at':
                    filtered_df[col] = pd.Timestamp.now()
                elif col in ['units', 'unit_price', 'amount', 'tax_rate', 'tax', 'total']:
                    filtered_df[col] = 0.0
                elif col == 'item_number':
                    filtered_df[col] = range(1, len(df_details) + 1)  # Auto-generate item numbers
                else:
                    filtered_df[col] = None

        logger.info(f"📊 Detail columns before filtering: {len(df_details.columns)}")
        logger.info(f"📊 Detail columns after filtering: {len(filtered_df.columns)}")

        # Delete existing records
        invoice_id = filtered_df.iloc[0]['invoice_id']
        try:
            session.sql(f"DELETE FROM INVOICE_LINE_ITEMS_DETAILED_DUP WHERE INVOICE_ID = '{invoice_id}'").collect()
        except Exception as delete_error:
            logger.warning(f"⚠️ Could not delete existing detail records: {delete_error}")
        
        # Convert DataFrame to Snowpark DataFrame with error handling
        try:
            snowpark_df = session.create_dataframe(filtered_df)
        except Exception as df_error:
            logger.error(f"❌ Error creating Snowpark DataFrame for details: {df_error}")
            logger.error(f"❌ DataFrame dtypes: {filtered_df.dtypes}")
            logger.error(f"❌ DataFrame sample: {filtered_df.head(1).to_dict()}")
            return False
        
        # Write to INVOICE_LINE_ITEMS_DETAILED_DUP table with error handling
        try:
            snowpark_df.write.mode("append").save_as_table("INVOICE_LINE_ITEMS_DETAILED_DUP")
            logger.info(f"✅ Successfully loaded {len(filtered_df)} detail record(s) to INVOICE_LINE_ITEMS_DETAILED_DUP")
            return True
        except Exception as write_error:
            logger.error(f"❌ Error writing details to INVOICE_LINE_ITEMS_DETAILED_DUP: {write_error}")
            return False
        
    except Exception as e:
        logger.error(f"❌ Error loading detail data to Snowflake: {e}")
        logger.error(f"❌ DataFrame columns: {list(df_details.columns) if not df_details.empty else 'Empty DataFrame'}")
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
            'processed': 'N',  # Default to 'N' when first loaded
            'currency': first_record.get('currency', '')
        }
        
        return pd.DataFrame([header_data])
        
    except Exception as e:
        logger.error(f"Error creating header from detail: {e}")
        return pd.DataFrame()

# Table schema documentation for reference
def get_table_schemas() -> Dict[str, Dict]:
    """
    Return ACTUAL table schemas based on working Snowflake tables
    """
    return {
        'INVOICE_HEADER_DUP': {
            'invoice_id': 'VARCHAR(16777216)',
            'ban': 'VARCHAR(16777216)',
            'billing_period': 'VARCHAR(16777216)',
            'vendor': 'VARCHAR(16777216)',
            'source_file': 'VARCHAR(16777216)',
            'invoice_total': 'FLOAT',
            'created_at': 'TIMESTAMP_NTZ(9)',
            'transtype': 'VARCHAR(1)',
            'batchno': 'VARCHAR(16777216)',
            'vendorno': 'VARCHAR(16777216)',
            'documentdate': 'TIMESTAMP_NTZ(9)',
            'invoiced_bu': 'VARCHAR(16777216)',
            'processed': 'VARCHAR(1)',
            'currency': 'VARCHAR(3)'  # 14th column
        },
        'INVOICE_LINE_ITEMS_DETAILED_DUP': {
            'invoice_id': 'VARCHAR(16777216)',
            'item_number': 'VARCHAR(16777216)', 
            'ban': 'VARCHAR(16777216)',
            'usoc': 'VARCHAR(16777216)',
            'description': 'VARCHAR(16777216)',
            'billing_period': 'VARCHAR(16777216)',
            'units': 'NUMBER(38,0)',
            'amount': 'FLOAT',
            'tax': 'FLOAT',
            'total': 'FLOAT',
            'disputed': 'BOOLEAN',
            'comment': 'VARCHAR(16777216)',
            'comment_date': 'TIMESTAMP_NTZ(9)'  # 13 columns total
        }
    }

def test_snowflake_connection(session: Session) -> bool:
    """
    Test Snowflake connection and table access
    """
    try:
        # Test connection
        result = session.sql("SELECT 1 as test").collect()
        logger.info("✅ Snowflake connection test successful")
        
        # Test table access
        header_count = session.sql("SELECT COUNT(*) FROM INVOICE_HEADER_DUP").collect()[0][0]
        detail_count = session.sql("SELECT COUNT(*) FROM INVOICE_LINE_ITEMS_DETAILED_DUP").collect()[0][0]
        
        logger.info(f"✅ Table access test: Header={header_count}, Detail={detail_count}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Snowflake connection/table test failed: {e}")
        return False