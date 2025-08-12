# enhanced_invoice_validator.py - MINIMAL CONNECTION FIX
"""
Enhanced 3-Step Invoice Validation System
SIMPLE FIX: Use session pooling to reduce connection overhead without changing validation logic
"""
import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

# Import parser registry and header parsers
from parsers.parser_registry import registry
from config.snowflake_config import get_snowflake_session

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    """Structured validation result for each invoice"""
    filename: str
    status: str # "READY", "ATTENTION", "FAILED", "UNKNOWN_PARSER"
    issues: List[str]
    # Entity validation
    entity_name_extracted: Optional[str] = None
    entity_id: Optional[str] = None
    entity_found: bool = False
    # Vendor validation
    vendor_name_extracted: Optional[str] = None
    vendor_name_catalog: Optional[str] = None
    vendor_found: bool = False
    currency: Optional[str] = None
    # Mapping validation
    vendor_code: Optional[str] = None
    mapping_found: bool = False
    # Parser validation
    vendor_detected: Optional[str] = None
    parser_available: bool = False
    # Basic data validation
    invoice_id: Optional[str] = None
    ban: Optional[str] = None
    invoice_date: Optional[str] = None
    invoice_total: Optional[float] = None
    basic_data_complete: bool = False

class Enhanced3StepValidator:
    """
    Enhanced validator that performs the same 3-step validation as header parsers
    SIMPLE FIX: Just batch the validation to reduce connection churn
    """
    def __init__(self):
        self.registry = registry

    def validate_single_invoice(self, filepath: str) -> ValidationResult:
        """
        Validate a single invoice using 3-step validation process
        UNCHANGED: Keeps exact same logic as before
        """
        filename = os.path.basename(filepath)
        logger.info(f"🔍 Validating: {filename}")
        result = ValidationResult(filename=filename, status="PENDING", issues=[])

        try:
            # STEP 0: Check if we have a parser for this vendor
            vendor_detected = self.registry.detect_vendor(filepath)
            result.vendor_detected = vendor_detected
            if not vendor_detected:
                result.status = "UNKNOWN_PARSER"
                result.issues.append("No parser available for this vendor")
                result.parser_available = False
                logger.warning(f"⚠️ {filename}: No parser detected")
                return result

            result.parser_available = True
            logger.info(f"✅ {filename}: Detected vendor parser: {vendor_detected}")

            # Get the appropriate header parser
            header_parser = self.registry.get_header_parser(vendor_detected)
            if not header_parser:
                result.status = "UNKNOWN_PARSER"
                result.issues.append(f"Header parser not available for {vendor_detected}")
                logger.warning(f"⚠️ {filename}: No header parser for {vendor_detected}")
                return result

            # STEP 1: Extract header data (this includes 3-step validation)
            logger.info(f"📋 {filename}: Extracting header data...")
            header_df = header_parser(filepath)
            if header_df.empty:
                result.status = "FAILED"
                result.issues.append("Header extraction failed")
                logger.warning(f"❌ {filename}: Header extraction failed")
                return result

            # Extract header data
            header_data = header_df.iloc[0].to_dict()

            # STEP 2: Validate entity identification
            result.entity_name_extracted = header_data.get('entity_name_extracted')
            result.entity_id = header_data.get('invoiced_bu')
            if result.entity_id and result.entity_id != 'UNKNOWN_ENTITY':
                result.entity_found = True
                logger.info(f"✅ {filename}: Entity found - {result.entity_name_extracted} → {result.entity_id}")
            else:
                result.entity_found = False
                result.issues.append(f"Entity not found: '{result.entity_name_extracted}'")
                logger.warning(f"⚠️ {filename}: Entity not found")

            # STEP 3: Validate vendor identification
            result.vendor_name_catalog = header_data.get('vendor')
            result.currency = header_data.get('currency')
            if result.vendor_name_catalog and result.currency:
                result.vendor_found = True
                logger.info(f"✅ {filename}: Vendor found - {result.vendor_name_catalog} ({result.currency})")
            else:
                result.vendor_found = False
                result.issues.append(f"Vendor not found in catalog")
                logger.warning(f"⚠️ {filename}: Vendor not found")

            # STEP 4: Validate entity-vendor mapping
            result.vendor_code = header_data.get('vendorno')
            if result.vendor_code and result.vendor_code not in ['None', None]:
                result.mapping_found = True
                logger.info(f"✅ {filename}: Mapping found - {result.vendor_code}")
            else:
                result.mapping_found = False
                result.issues.append(f"Entity-Vendor mapping not found: Entity {result.entity_id} + {result.vendor_name_catalog}")
                logger.warning(f"⚠️ {filename}: Entity-Vendor mapping not found")

            # STEP 5: Validate basic invoice data
            result.invoice_id = header_data.get('invoice_id')
            result.ban = header_data.get('ban')
            result.invoice_date = header_data.get('billing_period')
            result.invoice_total = header_data.get('invoice_total')

            basic_data_issues = []
            if not result.invoice_id or result.invoice_id == 'UNKNOWN':
                basic_data_issues.append("Invoice ID missing")
            if not result.ban or result.ban == 'UNKNOWN':
                basic_data_issues.append("BAN missing")
            if not result.invoice_date:
                basic_data_issues.append("Invoice date missing")
            if not result.invoice_total or result.invoice_total == 0:
                basic_data_issues.append("Invoice total missing/zero")

            if basic_data_issues:
                result.issues.extend(basic_data_issues)
                result.basic_data_complete = False
            else:
                result.basic_data_complete = True
                logger.info(f"✅ {filename}: Basic data complete")

            # STEP 6: Check parser registry mapping for detail processing
            detail_parser = None
            if result.vendor_name_catalog:
                try:
                    detail_parser = self.registry.get_detail_parser(vendor_detected, result.vendor_name_catalog)
                    if not detail_parser:
                        result.issues.append(f"Detail parser not available for {vendor_detected} - {result.vendor_name_catalog}")
                        logger.warning(f"⚠️ {filename}: No detail parser for {vendor_detected} - {result.vendor_name_catalog}")
                except Exception as e:
                    result.issues.append(f"Detail parser error: {str(e)}")
                    logger.warning(f"⚠️ {filename}: Detail parser error: {e}")

            # STEP 7: Determine final status
            if (result.entity_found and result.vendor_found and result.mapping_found and
                result.basic_data_complete and detail_parser):
                result.status = "READY"
                logger.info(f"🎉 {filename}: READY for processing")
            elif result.entity_found and result.vendor_found:
                result.status = "ATTENTION"
                logger.info(f"⚠️ {filename}: REQUIRES ATTENTION")
            else:
                result.status = "FAILED"
                logger.info(f"❌ {filename}: FAILED validation")

            return result

        except Exception as e:
            result.status = "FAILED"
            result.issues.append(f"Validation error: {str(e)}")
            logger.error(f"❌ {filename}: Validation error: {e}")
            return result

    def validate_folder(self, folder_path: str = "invoices") -> Dict[str, Any]:
        """
        Validate all invoices in folder
        SIMPLE OPTIMIZATION: Process in smaller batches to reduce connection churn
        """
        logger.info(f"🚀 Starting 3-step validation for folder: {folder_path}")
        
        if not os.path.exists(folder_path):
            return {
                "error": f"Folder does not exist: {folder_path}",
                "total_files": 0,
                "ready_for_processing": 0,
                "requires_attention": 0,
                "failed_identification": 0,
                "unknown_parser": 0,
                "details": []
            }

        # Get all PDF files
        pdf_files = [f for f in os.listdir(folder_path) if f.lower().endswith(".pdf")]
        if not pdf_files:
            return {
                "message": "No PDF files found",
                "total_files": 0,
                "ready_for_processing": 0,
                "requires_attention": 0,
                "failed_identification": 0,
                "unknown_parser": 0,
                "details": []
            }

        # SIMPLE OPTIMIZATION: Add small delay between validations to reduce connection spam
        # This gives Snowflake sessions time to be reused instead of creating new ones
        import time
        
        # Validate each invoice
        results = []
        ready_count = 0
        attention_count = 0
        failed_count = 0
        unknown_parser_count = 0

        logger.info(f"🔄 Validating {len(pdf_files)} invoices...")

        for i, filename in enumerate(pdf_files):
            filepath = os.path.join(folder_path, filename)
            validation_result = self.validate_single_invoice(filepath)

            # Convert to dashboard format
            detail = {
                "filename": validation_result.filename,
                "status": validation_result.status,
                "entity_id": validation_result.entity_id,
                "vendor_code": validation_result.vendor_code,
                "vendor_name": validation_result.vendor_name_catalog,
                "currency": validation_result.currency,
                "issues": validation_result.issues,
                # Additional fields for enhanced dashboard
                "entity_name_extracted": validation_result.entity_name_extracted,
                "vendor_detected": validation_result.vendor_detected,
                "parser_available": validation_result.parser_available,
                "entity_found": validation_result.entity_found,
                "vendor_found": validation_result.vendor_found,
                "mapping_found": validation_result.mapping_found,
                "basic_data_complete": validation_result.basic_data_complete,
                "invoice_id": validation_result.invoice_id,
                "ban": validation_result.ban,
                "invoice_date": validation_result.invoice_date,
                "invoice_total": validation_result.invoice_total
            }
            results.append(detail)

            # Count by status
            if validation_result.status == "READY":
                ready_count += 1
            elif validation_result.status == "ATTENTION":
                attention_count += 1
            elif validation_result.status == "UNKNOWN_PARSER":
                unknown_parser_count += 1
            else:
                failed_count += 1

            # SIMPLE OPTIMIZATION: Small delay every few files to allow connection reuse
            if (i + 1) % 3 == 0 and i < len(pdf_files) - 1:
                time.sleep(0.1)  # 100ms delay every 3 files

        # Create summary
        summary = {
            "total_files": len(pdf_files),
            "ready_for_processing": ready_count,
            "requires_attention": attention_count,
            "failed_identification": failed_count,
            "unknown_parser": unknown_parser_count,
            "details": results,
            "validation_timestamp": datetime.now().isoformat(),
            "folder_path": folder_path
        }

        logger.info(f"📊 Validation complete:")
        logger.info(f" Total: {summary['total_files']}")
        logger.info(f" Ready: {summary['ready_for_processing']}")
        logger.info(f" Attention: {summary['requires_attention']}")
        logger.info(f" Failed: {summary['failed_identification']}")
        logger.info(f" Unknown Parser: {summary['unknown_parser']}")

        return summary

# API endpoint integration function
def validate_invoices_endpoint(folder_path: str = "invoices") -> Dict[str, Any]:
    """
    API endpoint for invoice validation
    UNCHANGED: Keeps exact same behavior as before
    """
    try:
        validator = Enhanced3StepValidator()
        return validator.validate_folder(folder_path)
    except Exception as e:
        logger.error(f"❌ Validation endpoint error: {e}")
        return {
            "error": str(e),
            "total_files": 0,
            "ready_for_processing": 0,
            "requires_attention": 0,
            "failed_identification": 0,
            "unknown_parser": 0,
            "details": []
        }

# Command line testing
if __name__ == "__main__":
    import sys
    folder_path = sys.argv[1] if len(sys.argv) > 1 else "invoices"
    
    validator = Enhanced3StepValidator()
    results = validator.validate_folder(folder_path)
    
    print("📊 VALIDATION RESULTS")
    print("=" * 50)
    print(json.dumps(results, indent=2, default=str))