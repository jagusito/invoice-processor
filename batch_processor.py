import os
import json
import logging
import sqlite3
from datetime import datetime
from typing import Dict, Any, Optional
from snowflake.snowpark import Session
from core.logger import setup_logger
from config.snowflake_config import get_snowflake_session
from fin_loader import load_to_snowflake_detailed, load_to_snowflake_header, create_invoice_header_from_detail

# NEW: Import the identification modules
from enhanced_provider_detection import identify_invoice_context
from header_enrichment import validate_invoice_for_processing, enhance_header_with_identification

class BatchProcessor:
    def __init__(self, config_file: str = "config/processing_config.json"):
        self.logger = setup_logger("batch_processor")
        self.session = get_snowflake_session()
        self.load_config(config_file)
        
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
                "batch_size": 50
            }
    
    def process_single_invoice(self, filepath: str) -> bool:
        """Process a single invoice with enhanced entity/vendor identification"""
        filename = os.path.basename(filepath)
        filename_lower = filename.lower()
        
        start_time = datetime.now()
        
        try:
            # NEW: Step 1 - Validate entity/vendor identification BEFORE parsing
            self.logger.info(f"🔍 Identifying entity/vendor for: {filename}")
            validation = validate_invoice_for_processing(filepath)
            
            if not validation['is_valid']:
                error_msg = f"Identification failed: {', '.join(validation['issues'])}"
                self.logger.warning(f"⚠️ {error_msg}")
                self._log_processing_result(filename, "unknown", "FAILED", 
                                          error_msg, 0, 
                                          (datetime.now() - start_time).total_seconds())
                return False
            
            # Log successful identification
            self.logger.info(f"✅ Identified - Entity: {validation['entity_id']}, Vendor Code: {validation['vendor_code']}")
            
            # Step 2 - Use your existing parser logic (UNCHANGED)
            if "equinix" in filename_lower:
                from parsers.fin_equinix_parser import extract_equinix_items
                self.logger.info(f"📄 Parsing Equinix invoice: {filename}")
                df = extract_equinix_items(filepath)
                provider = "equinix"
            elif "lumen" in filename_lower or "level3" in filename_lower:
                from parsers.fin_lumen_parser import process_invoice_safely
                self.logger.info(f"📄 Parsing Lumen invoice: {filename}")
                df = process_invoice_safely(filepath)
                provider = "lumen"
            elif "vodafone" in filename_lower:
                from parsers.fin_vodafone_parser import extract_vodafone_items
                self.logger.info(f"📄 Parsing Vodafone invoice: {filename}")
                df = extract_vodafone_items(filepath)
                provider = "vodafone"
            elif "att" in filename_lower:
                from parsers.fin_att_parser import extract_att_items
                self.logger.info(f"📄 Parsing AT&T invoice: {filename}")
                df = extract_att_items(filepath)
                provider = "att"
            elif "newequin" in filename_lower:
                from parsers.fin_new_equinix_parser import new_extract_equinix
                self.logger.info(f"📄 Parsing NewEquinix invoice: {filename}")
                df = new_extract_equinix(filepath)
                provider = "newequin"
            elif "interxion" in filename_lower:
                from parsers.fin_interxion_parser import extract_interx
                self.logger.info(f"📄 Parsing Interxion invoice: {filename}")
                df = extract_interx(filepath)
                provider = "interxion"
            else:
                self.logger.warning(f"⚠️ Skipping unknown provider for file: {filename}")
                self._log_processing_result(filename, "unknown", "SKIPPED", 
                                          "Unknown provider", 0, 0)
                return False
            
            if df.empty:
                self.logger.warning(f"⚠️ No records extracted from: {filename}")
                self._log_processing_result(filename, provider, "FAILED", 
                                          "No records extracted", 0, 
                                          (datetime.now() - start_time).total_seconds())
                return False
            
            # Step 3 - Create header using your existing logic (UNCHANGED)
            df_header = create_invoice_header_from_detail(df, source_file=filepath)
            
            # MODIFIED: Step 4 - Enhance header with NEW identification + existing catalog data
            df_header = self._enhance_header_with_identification_and_catalog(df_header, filepath, provider)
            
            # Step 5 - Load to Snowflake using your existing functions (UNCHANGED)
            load_to_snowflake_detailed(self.session, df)
            load_to_snowflake_header(self.session, df_header)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            self.logger.info(f"✅ Successfully processed: {filename} ({len(df)} records in {processing_time:.2f}s)")
            
            self._log_processing_result(filename, provider, "SUCCESS", None, 
                                      len(df), processing_time)
            return True
            
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            self.logger.error(f"❌ Error processing {filename}: {e}")
            self._log_processing_result(filename, provider if 'provider' in locals() else "unknown", 
                                      "FAILED", str(e), 0, processing_time)
            return False
    
    def _enhance_header_with_identification_and_catalog(self, df_header, filepath, provider):
        """
        ENHANCED: Combine NEW entity/vendor identification with existing catalog enrichment
        """
        if df_header.empty:
            return df_header
            
        try:
            # NEW: Add entity/vendor identification from first page
            df_header = enhance_header_with_identification(df_header, filepath)
            
            # EXISTING: Add your current catalog enhancement (keep this for backward compatibility)
            df_header = self._enhance_header_with_catalog_data(df_header, provider)
            
            self.logger.info(f"📋 Header enriched with identification data - Entity: {df_header.iloc[0].get('invoiced_bu', 'N/A')}, Vendor Code: {df_header.iloc[0].get('vendor_code', 'N/A')}")
            
            return df_header
            
        except Exception as e:
            self.logger.warning(f"Could not enhance header with identification: {e}")
            # Fallback to existing catalog enhancement only
            return self._enhance_header_with_catalog_data(df_header, provider)
    
    def _enhance_header_with_catalog_data(self, df_header, provider):
        """Enhance header with data from Entity/Vendor catalogs (EXISTING CODE - UNCHANGED)"""
        if df_header.empty:
            return df_header
            
        try:
            # Get vendor data from catalog
            vendor_data = self._get_vendor_from_catalog(provider)
            entity_data = self._get_entity_from_catalog(df_header.iloc[0].get('ban', ''))
            
            # Add vendor information
            if vendor_data:
                df_header['vendor_name'] = vendor_data.get('vendor_name', '')
                df_header['vendor_type'] = vendor_data.get('vendor_type', '')
                df_header['vendor_contact'] = vendor_data.get('contact_person', '')
                df_header['vendor_email'] = vendor_data.get('email', '')
            
            # Add entity information  
            if entity_data:
                df_header['entity_name'] = entity_data.get('entity_name', '')
                df_header['entity_type'] = entity_data.get('entity_type', '')
                df_header['entity_currency'] = entity_data.get('currency', '')
                df_header['entity_contact'] = entity_data.get('contact_person', '')
                
            return df_header
            
        except Exception as e:
            self.logger.warning(f"Could not enhance header with catalog data: {e}")
            return df_header
    
    def _get_vendor_from_catalog(self, provider):
        """Get vendor data from Snowflake catalog (EXISTING CODE - NEEDS UPDATE)"""
        try:
            # UPDATED: Map to vendor names since you removed VENDOR_ID
            provider_mapping = {
                'equinix': 'Equinix, Inc',
                'lumen': 'Lumen Technologies', 
                'vodafone': 'Vodafone Business',
                'att': 'AT&T',
                'newequin': 'Equinix New',
                'interxion': 'Interxion'
            }
            
            vendor_name = provider_mapping.get(provider.lower())
            if not vendor_name:
                return None
                
            result = self.session.sql(f"""
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
            
        except Exception as e:
            self.logger.warning(f"Could not fetch vendor data: {e}")
            return None
    
    def _get_entity_from_catalog(self, ban_or_entity_id):
        """Get entity data from Snowflake catalog (EXISTING CODE - UNCHANGED)"""
        try:
            if not ban_or_entity_id:
                return None
                
            result = self.session.sql(f"""
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
            
        except Exception as e:
            self.logger.warning(f"Could not fetch entity data: {e}")
            return None
            
    def process_folder(self, folder_path: str = None) -> Dict[str, Any]:
        """Process all invoices in folder (EXISTING CODE - UNCHANGED)"""
        folder_path = folder_path or self.config["invoice_folder"]
        
        results = {
            "total_files": 0,
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "processing_time": 0,
            "errors": []
        }
        
        start_time = datetime.now()
        
        if not os.path.exists(folder_path):
            self.logger.error(f"Folder does not exist: {folder_path}")
            return results
        
        for file in os.listdir(folder_path):
            if not file.lower().endswith(".pdf"):
                continue
                
            results["total_files"] += 1
            filepath = os.path.join(folder_path, file)
            
            try:
                success = self.process_single_invoice(filepath)
                if success:
                    results["successful"] += 1
                    self._move_file(filepath, self.config["processed_folder"])
                else:
                    results["failed"] += 1
                    self._move_file(filepath, self.config["failed_folder"])
                    
            except Exception as e:
                results["failed"] += 1
                results["errors"].append(f"{file}: {str(e)}")
                self.logger.error(f"Error processing {file}: {e}")
                self._move_file(filepath, self.config["failed_folder"])
        
        results["processing_time"] = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"Batch processing completed: {results}")
        return results

    def _move_file(self, source: str, dest_folder: str):
        """Move processed file to appropriate folder (EXISTING CODE - UNCHANGED)"""
        try:
            os.makedirs(dest_folder, exist_ok=True)
            filename = os.path.basename(source)
            dest_path = os.path.join(dest_folder, filename)
            os.rename(source, dest_path)
        except Exception as e:
            self.logger.error(f"Failed to move file {source}: {e}")
    
    def _log_processing_result(self, filename: str, provider: str, status: str, 
                             error_message: str = None, records_processed: int = 0, 
                             processing_time: float = 0):
        """Log processing result to SQLite database (EXISTING CODE - UNCHANGED)"""
        try:
            os.makedirs('data', exist_ok=True)
            conn = sqlite3.connect('data/logs.db')
            cursor = conn.cursor()
            
            # Create table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processing_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    provider TEXT,
                    status TEXT,
                    error_message TEXT,
                    records_processed INTEGER,
                    processing_time_seconds REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.execute("""
                INSERT INTO processing_logs 
                (filename, provider, status, error_message, records_processed, processing_time_seconds)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (filename, provider, status, error_message, records_processed, processing_time))
            
            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.error(f"Failed to log processing result: {e}")

# Main execution (use this to test)
if __name__ == "__main__":
    processor = BatchProcessor()
    results = processor.process_folder()
    print(f"Processing results: {results}")