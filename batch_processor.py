# batch_processor.py - CLEAN VERSION with minimal changes
import os
import json
import logging
import uuid
import pandas as pd
from datetime import datetime
from typing import Dict, Any, Optional
from config.snowflake_config import get_snowflake_session
from fin_loader import load_to_snowflake_detailed, load_to_snowflake_header

# Import the registry and identification modules
from parsers.parser_registry import registry
from enhanced_provider_detection import identify_invoice_context
from header_enrichment import validate_invoice_for_processing, enhance_header_with_identification

class BatchProcessor:
    def __init__(self, config_file: str = "config/processing_config.json"):
        self.setup_logging()
        self.session = None
        self.load_config(config_file)
        self.registry = registry
        
    def setup_logging(self):
        """Setup logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger("batch_processor")
        
    def load_config(self, config_file: str):
        """Load processing configuration"""
        try:
            with open(config_file, 'r') as f:
                self.config = json.load(f)
        except FileNotFoundError:
            self.config = {
                "invoice_folder": "invoices",
                "processed_folder": "processed", 
                "failed_folder": "failed",
                "batch_size": 50,
                "enable_identification": True,
                "validation_mode": "strict"
            }
    
    def process_single_invoice(self, filepath: str) -> bool:
        """Process a single invoice with proper session management and schema preparation"""
        filename = os.path.basename(filepath)
        start_time = datetime.now()
        vendor = "unknown"
        entity_id = None
        vendor_code = None
        invoice_total = 0.0
        currency = None
        invoice_id = None
        
        try:
            self.logger.info(f"🔄 Processing invoice: {filename}")
            
            # STEP 1: Entity/Vendor Identification (if enabled)
            if self.config.get("enable_identification", True):
                self.logger.info(f"🔍 Validating entity/vendor identification...")
                validation = validate_invoice_for_processing(filepath)
                
                if not validation['is_valid'] and self.config.get("validation_mode") == "strict":
                    error_msg = f"Identification failed: {', '.join(validation['issues'])}"
                    self.logger.warning(f"⚠️ {error_msg}")
                    self._log_processing_result_to_snowflake(
                        filename=filename,
                        vendor=vendor,
                        status="FAILED",
                        error_message=error_msg,
                        records_processed=0,
                        processing_time_seconds=(datetime.now() - start_time).total_seconds()
                    )
                    return False
                elif validation['is_valid']:
                    entity_id = validation.get('entity_id')
                    vendor_code = validation.get('vendor_code')
                    self.logger.info(f"✅ Identified - Entity: {entity_id}, Vendor Code: {vendor_code}")
            
            # STEP 2: Detect vendor using registry
            vendor = self.registry.detect_vendor(filepath)
            if not vendor:
                error_msg = f"Unknown vendor - cannot determine parser"
                self.logger.warning(f"⚠️ {error_msg}")
                self._log_processing_result_to_snowflake(
                    filename=filename,
                    vendor="unknown",
                    status="FAILED",
                    error_message=error_msg,
                    records_processed=0,
                    processing_time_seconds=(datetime.now() - start_time).total_seconds()
                )
                return False
            
            self.logger.info(f"🎯 Detected vendor: {vendor}")
            
            # STEP 3: Extract header using registry
            self.logger.info(f"📋 Extracting header data...")
            header_df = self.registry.extract_header(filepath, vendor)
            
            if header_df.empty:
                error_msg = f"Header extraction failed for vendor: {vendor}"
                self.logger.warning(f"⚠️ {error_msg}")
                self._log_processing_result_to_snowflake(
                    filename=filename,
                    vendor=vendor,
                    status="FAILED",
                    error_message=error_msg,
                    records_processed=0,
                    processing_time_seconds=(datetime.now() - start_time).total_seconds()
                )
                return False
            
            # STEP 4: Enhance header with identification data
            if self.config.get("enable_identification", True):
                self.logger.info(f"📋 Enhancing header with identification...")
                
                # DEBUG: Check invoiced_bu BEFORE enhancement
                before_invoiced_bu = header_df.iloc[0].get('invoiced_bu')
                self.logger.info(f"🔍 DEBUG invoiced_bu BEFORE enhancement: '{before_invoiced_bu}'")
                
                header_df = enhance_header_with_identification(header_df, filepath)
                
                # DEBUG: Check invoiced_bu AFTER enhancement
                after_invoiced_bu = header_df.iloc[0].get('invoiced_bu')
                self.logger.info(f"🔍 DEBUG invoiced_bu AFTER enhancement: '{after_invoiced_bu}'")
                
                # CRITICAL FIX: If enhancement cleared invoiced_bu, restore it
                if before_invoiced_bu and not after_invoiced_bu:
                    self.logger.info(f"🔧 FIXING: Restoring invoiced_bu from '{after_invoiced_bu}' to '{before_invoiced_bu}'")
                    header_df.iloc[0, header_df.columns.get_loc('invoiced_bu')] = before_invoiced_bu
            
            # Add catalog enrichment
            header_df = self._enhance_header_with_catalog_data(header_df, vendor)
            
            self.logger.info(f"✅ Header extracted: {len(header_df)} record(s)")
            
            # Extract data for logging from header
            header_data = header_df.iloc[0].to_dict()
            
            # DEBUG: Check invoiced_bu specifically
            self.logger.info(f"🔍 DEBUG invoiced_bu in header_data: '{header_data.get('invoiced_bu')}' (type: {type(header_data.get('invoiced_bu'))})")
            
            # Get ALL values from header_data consistently
            invoice_id = header_data.get('invoice_id')
            entity_id = header_data.get('invoiced_bu')  # Get directly from header
            vendor_code = header_data.get('vendorno')
            invoice_total = header_data.get('invoice_total', 0.0)
            currency = header_data.get('currency')
            
            # DEBUG: Show final entity_id value
            self.logger.info(f"🔍 DEBUG final entity_id for logging: '{entity_id}' (type: {type(entity_id)})")
            
            # STEP 5: Extract details using registry
            self.logger.info(f"📄 Extracting detail data...")
            detail_df = self.registry.extract_details(filepath, header_data)
            
            if detail_df.empty:
                error_msg = f"Detail extraction failed for vendor: {vendor}"
                self.logger.warning(f"⚠️ {error_msg}")
                self._log_processing_result_to_snowflake(
                    filename=filename,
                    vendor=vendor,
                    status="FAILED",
                    error_message=error_msg,
                    records_processed=0,
                    processing_time_seconds=(datetime.now() - start_time).total_seconds(),
                    entity_id=entity_id,
                    vendor_code=vendor_code,
                    invoice_total=invoice_total,
                    currency=currency,
                    invoice_id=invoice_id
                )
                return False
            
            self.logger.info(f"✅ Details extracted: {len(detail_df)} record(s)")
            
            # STEP 6: Prepare data for Snowflake schema
            self.logger.info(f"🔧 Preparing data for Snowflake schema...")
            header_prepared = self._prepare_header_for_snowflake(header_df)
            detail_prepared = self._prepare_detail_for_snowflake(detail_df, header_df)
            
            # STEP 7: Load to Snowflake using prepared data
            self.logger.info(f"💾 Loading to Snowflake...")
            temp_session = get_snowflake_session()
            try:
                load_to_snowflake_header(temp_session, header_prepared)
                load_to_snowflake_detailed(temp_session, detail_prepared)
            except Exception as snowflake_error:
                self.logger.error(f"❌ Snowflake loading error: {snowflake_error}")
                raise snowflake_error
            finally:
                try:
                    temp_session.close()
                except Exception as close_error:
                    self.logger.warning(f"⚠️ Session close warning: {close_error}")
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # Log SUCCESS to Snowflake
            self._log_processing_result_to_snowflake(
                filename=filename,
                vendor=vendor,
                status="SUCCESS",
                records_processed=len(detail_prepared),
                processing_time_seconds=processing_time,
                entity_id=entity_id,
                vendor_code=vendor_code,
                invoice_total=invoice_total,
                currency=currency,
                invoice_id=invoice_id
            )
            
            self.logger.info(f"✅ Successfully processed: {filename} ({len(detail_prepared)} records in {processing_time:.2f}s)")
            
            # Log financial summary
            if 'amount' in detail_prepared.columns:
                total_amount = detail_prepared['amount'].sum()
                self.logger.info(f"💰 Financial total: {total_amount:,.2f}")
            
            return True
            
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            error_msg = f"Processing error: {str(e)}"
            self.logger.error(f"❌ Error processing {filename}: {e}")
            
            # Log FAILURE to Snowflake
            self._log_processing_result_to_snowflake(
                filename=filename,
                vendor=vendor,
                status="FAILED",
                error_message=error_msg,
                records_processed=0,
                processing_time_seconds=processing_time,
                entity_id=entity_id,
                vendor_code=vendor_code,
                invoice_total=invoice_total,
                currency=currency,
                invoice_id=invoice_id
            )
            
            return False

    def _prepare_header_for_snowflake(self, header_df: pd.DataFrame) -> pd.DataFrame:
        """Prepare header data to match exact Snowflake schema - FIXED to preserve documentdate"""
        prepared = header_df.copy()
        
        # CRITICAL: Match exact Snowflake column names and order (14 columns)
        required_columns = [
            'invoice_id', 'ban', 'billing_period', 'vendor', 'source_file', 
            'invoice_total', 'created_at', 'transtype', 'batchno', 'vendorno', 
            'documentdate', 'invoiced_bu', 'processed', 'currency'
        ]
        
        # Ensure all required columns exist with correct data types
        for col in required_columns:
            if col not in prepared.columns:
                if col == 'transtype':
                    prepared[col] = '1'  # CHAR(1) - just use '1'
                elif col == 'batchno':
                    prepared[col] = None
                elif col == 'processed':
                    prepared[col] = 'N'
                elif col == 'created_at':
                    prepared[col] = pd.Timestamp.now()
                elif col == 'documentdate':
                    prepared[col] = prepared.get('billing_period', pd.Timestamp.now())
                elif col == 'vendor':
                    prepared[col] = 'Unknown'  # Fallback if vendor missing
                else:
                    prepared[col] = ''
        
        # Fix data type issues
        
        # 1. Ensure timestamps are proper datetime objects - FIXED VERSION
        timestamp_columns = ['created_at', 'documentdate']
        for col in timestamp_columns:
            if col in prepared.columns:
                # Force conversion to proper timestamp format
                try:
                    if col == 'created_at':
                        # Use current timestamp for created_at
                        prepared[col] = pd.Timestamp.now()
                    elif col == 'documentdate':
                        # FIXED: Only process if documentdate is missing/invalid, otherwise keep extracted value
                        current_value = prepared[col].iloc[0] if not prepared[col].empty else None
                        
                        # Check if current documentdate value is valid
                        if pd.isna(current_value) or current_value == '' or current_value is None:
                            # documentdate is missing, try to derive from billing_period
                            if 'billing_period' in prepared.columns:
                                billing_period = prepared['billing_period'].iloc[0]
                                if isinstance(billing_period, str):
                                    try:
                                        # Handle date formats like "01-Jul-25"
                                        parsed_date = pd.to_datetime(billing_period, format='%d-%b-%y')
                                        prepared[col] = parsed_date
                                    except:
                                        try:
                                            # Try other common date formats
                                            parsed_date = pd.to_datetime(billing_period)
                                            prepared[col] = parsed_date
                                        except:
                                            prepared[col] = pd.Timestamp.now()
                                else:
                                    prepared[col] = pd.Timestamp.now()
                            else:
                                prepared[col] = pd.Timestamp.now()
                        else:
                            # documentdate has a valid value, keep it but ensure it's datetime type
                            try:
                                prepared[col] = pd.to_datetime(current_value)
                            except:
                                # If conversion fails, use current time as fallback
                                prepared[col] = pd.Timestamp.now()
                    
                    # Ensure it's definitely a datetime64 type
                    prepared[col] = pd.to_datetime(prepared[col])
                    
                except Exception as e:
                    self.logger.warning(f"Could not convert {col} to timestamp, using current time: {e}")
                    prepared[col] = pd.Timestamp.now()
        
        # 2. Ensure numeric fields are proper numeric types
        numeric_columns = ['invoice_total']
        for col in numeric_columns:
            if col in prepared.columns:
                prepared[col] = pd.to_numeric(prepared[col], errors='coerce').fillna(0.0)
        
        # 3. CRITICAL FIX: Ensure string fields are strings INCLUDING invoiced_bu
        string_columns = ['invoice_id', 'ban', 'billing_period', 'vendor', 'currency', 
                         'source_file', 'transtype', 'batchno', 'vendorno', 'invoiced_bu', 'processed']
        for col in string_columns:
            if col in prepared.columns:
                prepared[col] = prepared[col].astype(str).fillna('')
        
        # 4. Ensure source_file is just filename (no path)
        if 'source_file' in prepared.columns:
            prepared['source_file'] = prepared['source_file'].apply(lambda x: os.path.basename(str(x)) if pd.notna(x) else '')
        
        # CRITICAL: Select columns in EXACT Snowflake order
        final_columns = [col for col in required_columns if col in prepared.columns]
        prepared = prepared[final_columns]
        
        # DEBUG: Show the invoiced_bu value specifically
        if 'invoiced_bu' in prepared.columns:
            invoiced_bu_value = prepared['invoiced_bu'].iloc[0]
            self.logger.info(f"🔍 invoiced_bu value in prepared header: '{invoiced_bu_value}' (type: {type(invoiced_bu_value)})")
        
        # DEBUG: Show the documentdate value specifically
        if 'documentdate' in prepared.columns:
            documentdate_value = prepared['documentdate'].iloc[0]
            self.logger.info(f"🔍 documentdate value in prepared header: '{documentdate_value}' (type: {type(documentdate_value)})")
        
        return prepared

    def _prepare_detail_for_snowflake(self, detail_df: pd.DataFrame, header_df: pd.DataFrame) -> pd.DataFrame:
        """Prepare detail data to match exact Snowflake schema"""
        prepared = detail_df.copy().reset_index(drop=True)
        
        # Required columns for INVOICE_LINE_ITEMS_DETAILED_DUP (13 columns)
        required_columns = [
            'invoice_id', 'item_number', 'ban', 'usoc', 'description',
            'billing_period', 'units', 'amount', 'tax', 'total',
            'disputed', 'comment', 'comment_date'
        ]
        
        # Add missing columns with defaults
        if 'disputed' not in prepared.columns:
            prepared['disputed'] = False
        if 'comment' not in prepared.columns:
            prepared['comment'] = ''
        if 'comment_date' not in prepared.columns:
            prepared['comment_date'] = "1900-01-01 00:00:00"
        
        # Ensure all required columns exist
        for col in required_columns:
            if col not in prepared.columns:
                if col in ['invoice_id', 'ban', 'billing_period']:
                    header_data = header_df.iloc[0]
                    prepared[col] = header_data.get(col, '')
                elif col in ['units']:
                    prepared[col] = 1.0
                elif col in ['amount', 'tax', 'total']:
                    prepared[col] = 0.0
                else:
                    prepared[col] = ''
        
        # Convert data types
        if 'units' in prepared.columns:
            prepared['units'] = pd.to_numeric(prepared['units'], errors='coerce').fillna(0).astype(int)
        
        for col in ['amount', 'tax', 'total']:
            if col in prepared.columns:
                prepared[col] = pd.to_numeric(prepared[col], errors='coerce').fillna(0.0)
        
        # Select only required columns in order
        prepared = prepared[required_columns]
        
        return prepared
    
    def process_folder(self, folder_path: str = None) -> Dict[str, Any]:
        """Process all invoices in folder"""
        folder_path = folder_path or self.config["invoice_folder"]
        
        results = {
            "total_files": 0,
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "processing_time": 0,
            "errors": [],
            "vendor_breakdown": {},
            "file_details": []
        }
        
        start_time = datetime.now()
        self.logger.info(f"🚀 Starting batch processing in: {folder_path}")
        
        if not os.path.exists(folder_path):
            error_msg = f"Folder does not exist: {folder_path}"
            self.logger.error(error_msg)
            results["errors"].append(error_msg)
            return results
        
        # Get all PDF files
        pdf_files = [f for f in os.listdir(folder_path) if f.lower().endswith(".pdf")]
        
        if not pdf_files:
            self.logger.warning(f"No PDF files found in: {folder_path}")
            return results
        
        self.logger.info(f"📁 Found {len(pdf_files)} PDF files to process")
        
        # Process each file
        for file in pdf_files:
            results["total_files"] += 1
            filepath = os.path.join(folder_path, file)
            
            try:
                vendor = self.registry.detect_vendor(filepath)
                vendor_key = vendor or "unknown"
                
                if vendor_key not in results["vendor_breakdown"]:
                    results["vendor_breakdown"][vendor_key] = {"success": 0, "failed": 0}
                
                success = self.process_single_invoice(filepath)
                
                file_result = {
                    "filename": file,
                    "vendor": vendor_key,
                    "status": "SUCCESS" if success else "FAILED",
                    "processed_at": datetime.now().isoformat()
                }
                
                if success:
                    results["successful"] += 1
                    results["vendor_breakdown"][vendor_key]["success"] += 1
                    self._move_file(filepath, self.config["processed_folder"])
                    file_result["destination"] = "processed"
                else:
                    results["failed"] += 1
                    results["vendor_breakdown"][vendor_key]["failed"] += 1
                    self._move_file(filepath, self.config["failed_folder"])
                    file_result["destination"] = "failed"
                
                results["file_details"].append(file_result)
                    
            except Exception as e:
                results["failed"] += 1
                error_msg = f"{file}: {str(e)}"
                results["errors"].append(error_msg)
                self.logger.error(f"Error processing {file}: {e}")
                self._move_file(filepath, self.config["failed_folder"])
                
                self._log_processing_result_to_snowflake(
                    filename=file,
                    vendor="error",
                    status="ERROR",
                    error_message=str(e),
                    records_processed=0,
                    processing_time_seconds=0
                )
                
                results["file_details"].append({
                    "filename": file,
                    "vendor": "error",
                    "status": "ERROR",
                    "error": str(e),
                    "destination": "failed",
                    "processed_at": datetime.now().isoformat()
                })
        
        results["processing_time"] = (datetime.now() - start_time).total_seconds()
        
        # Log summary
        self.logger.info(f"🎯 Batch processing completed:")
        self.logger.info(f"   Total files: {results['total_files']}")
        self.logger.info(f"   Successful: {results['successful']}")
        self.logger.info(f"   Failed: {results['failed']}")
        self.logger.info(f"   Processing time: {results['processing_time']:.2f}s")
        
        return results
    
    def get_registry_status(self) -> Dict[str, Any]:
        """Get status of parser registry"""
        return self.registry.get_registry_status()
    
    def _enhance_header_with_catalog_data(self, df_header, vendor):
        """Enhance header with data from Entity/Vendor catalogs"""
        if df_header.empty:
            return df_header
            
        try:
            vendor_data = self._get_vendor_from_catalog(vendor)
            entity_data = self._get_entity_from_catalog(df_header.iloc[0].get('ban', ''))
            
            if vendor_data:
                df_header['vendor_name'] = vendor_data.get('vendor_name', '')
                df_header['vendor_type'] = vendor_data.get('vendor_type', '')
                df_header['vendor_contact'] = vendor_data.get('contact_person', '')
                df_header['vendor_email'] = vendor_data.get('email', '')
            
            if entity_data:
                df_header['entity_name'] = entity_data.get('entity_name', '')
                df_header['entity_type'] = entity_data.get('entity_type', '')
                df_header['entity_currency'] = entity_data.get('currency', '')
                df_header['entity_contact'] = entity_data.get('contact_person', '')
                
            return df_header
            
        except Exception as e:
            self.logger.warning(f"Could not enhance header with catalog data: {e}")
            return df_header
    
    def _get_vendor_from_catalog(self, vendor):
        """Get vendor data from Snowflake catalog"""
        try:
            vendor_mapping = {
                'equinix': 'Equinix, Inc',
                'lumen': 'Lumen Technologies', 
                'vodafone': 'Vodafone Limited',
                'att': 'AT&T',
                'digital_realty': 'Digital London Ltd.'
            }
            
            vendor_name = vendor_mapping.get(vendor.lower())
            if not vendor_name:
                return None
            
            temp_session = get_snowflake_session()
            try:
                result = temp_session.sql(f"""
                    SELECT VENDOR_NAME, VENDOR_TYPE, CONTACT_PERSON, EMAIL, CURRENCY
                    FROM VENDOR_CATALOG 
                    WHERE VENDOR_NAME = '{vendor_name}' AND STATUS = 'Active'
                    LIMIT 1
                """).collect()
                
                if result:
                    row = result[0]
                    return {
                        'vendor_name': row[0], 
                        'vendor_type': row[1],
                        'contact_person': row[2],
                        'email': row[3],
                        'currency': row[4]
                    }
                return None
            finally:
                temp_session.close()
                
        except Exception as e:
            self.logger.warning(f"Could not fetch vendor data: {e}")
            return None
    
    def _get_entity_from_catalog(self, ban_or_entity_id):
        """Get entity data from Snowflake catalog"""
        try:
            if not ban_or_entity_id:
                return None
            
            temp_session = get_snowflake_session()
            try:
                result = temp_session.sql(f"""
                    SELECT ENTITY_ID, ENTITY_NAME, ENTITY_TYPE, CONTACT_PERSON
                    FROM ENTITY_CATALOG 
                    WHERE ENTITY_ID = '{ban_or_entity_id}' AND STATUS = 'Active'
                    LIMIT 1
                """).collect()
                
                if result:
                    row = result[0]
                    return {
                        'entity_id': row[0],
                        'entity_name': row[1],
                        'entity_type': row[2], 
                        'contact_person': row[3]
                    }
                return None
            finally:
                temp_session.close()
                
        except Exception as e:
            self.logger.warning(f"Could not fetch entity data: {e}")
            return None
    
    def _move_file(self, source: str, dest_folder: str):
        """Move processed file to appropriate folder"""
        try:
            os.makedirs(dest_folder, exist_ok=True)
            filename = os.path.basename(source)
            dest_path = os.path.join(dest_folder, filename)
            os.rename(source, dest_path)
            self.logger.debug(f"Moved {filename} to {dest_folder}")
        except Exception as e:
            self.logger.error(f"Failed to move file {source}: {e}")
    
    def _log_processing_result_to_snowflake(self, filename: str, vendor: str, status: str, 
                                          error_message: str = None, records_processed: int = 0, 
                                          processing_time_seconds: float = 0.0, entity_id: str = None,
                                          vendor_code: str = None, invoice_total: float = None,
                                          currency: str = None, invoice_id: str = None):
        """Log processing result to Snowflake with updated schema"""
        try:
            log_id = f"LOG_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
            
            def clean_sql_string(value):
                if value is None:
                    return ''
                return str(value).replace("'", "''")
            
            query = f"""
                INSERT INTO PROCESSING_LOGS (
                    LOG_ID, FILENAME, VENDOR, STATUS, ERROR_MESSAGE,
                    RECORDS_PROCESSED, PROCESSING_TIME_SECONDS, INVOICE_ID,
                    ENTITY_ID, VENDOR_CODE, INVOICE_TOTAL, CURRENCY
                ) VALUES (
                    '{log_id}',
                    '{clean_sql_string(filename)}',
                    '{clean_sql_string(vendor)}',
                    '{clean_sql_string(status)}',
                    '{clean_sql_string(error_message or '')}',
                    {records_processed or 0},
                    {processing_time_seconds or 0.0},
                    '{clean_sql_string(invoice_id or '')}',
                    '{clean_sql_string(entity_id or '')}',
                    '{clean_sql_string(vendor_code or '')}',
                    {invoice_total or 0.0},
                    '{clean_sql_string(currency or '')}'
                )
            """
            
            log_session = get_snowflake_session()
            try:
                log_session.sql(query).collect()
                self.logger.debug(f"✅ Logged to Snowflake: {filename} - {status}")
            finally:
                log_session.close()
            
        except Exception as e:
            self.logger.error(f"❌ Error logging to Snowflake: {e}")
            self.logger.info(f"📝 FALLBACK LOG: {filename} | {vendor} | {status} | Invoice: {invoice_id} | Entity: {entity_id}")

# Convenience functions for API endpoints
def process_single_file_endpoint(filepath: str) -> Dict[str, Any]:
    """API endpoint for processing single file"""
    processor = BatchProcessor()
    start_time = datetime.now()
    filename = os.path.basename(filepath)
    
    result = {
        "filename": filename,
        "filepath": filepath,
        "status": "PENDING",
        "started_at": start_time.isoformat(),
    }
    
    try:
        success = processor.process_single_invoice(filepath)
        processing_time = (datetime.now() - start_time).total_seconds()
        
        result.update({
            "status": "SUCCESS" if success else "FAILED",
            "processing_time": processing_time,
            "completed_at": datetime.now().isoformat()
        })
        
    except Exception as e:
        result.update({
            "status": "ERROR",
            "error": str(e),
            "completed_at": datetime.now().isoformat()
        })
    
    return result

def process_folder_endpoint(folder_path: str = None) -> Dict[str, Any]:
    """API endpoint for processing folder"""
    processor = BatchProcessor()
    return processor.process_folder(folder_path)

def get_parser_status_endpoint() -> Dict[str, Any]:
    """API endpoint for getting parser registry status"""
    processor = BatchProcessor()
    return processor.get_registry_status()

if __name__ == "__main__":
    import sys
    
    processor = BatchProcessor()
    
    if len(sys.argv) > 1:
        target = sys.argv[1]
        
        if os.path.isfile(target):
            print(f"🔄 Processing single file: {target}")
            result = process_single_file_endpoint(target)
            print(f"📊 Result: {json.dumps(result, indent=2)}")
        elif os.path.isdir(target):
            print(f"🔄 Processing folder: {target}")
            results = processor.process_folder(target)
            print(f"📊 Results: {json.dumps(results, indent=2)}")
        else:
            print(f"❌ Path does not exist: {target}")
    else:
        print("🔄 Processing default invoice folder...")
        results = processor.process_folder()
        print(f"📊 Results: {json.dumps(results, indent=2)}")