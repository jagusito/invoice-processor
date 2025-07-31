# header_enrichment.py
"""
Header Enrichment Module for Invoice Processing
Integrates entity/vendor identification with batch processing
"""

from enhanced_provider_detection import EnhancedProviderDetection, identify_invoice_context
import pandas as pd
from typing import Dict, Optional

class HeaderEnrichmentService:
    """
    Service to enrich invoice headers with entity and vendor information
    """
    
    def __init__(self):
        self.detector = EnhancedProviderDetection()
    
    def enrich_header_dataframe(self, header_df: pd.DataFrame, pdf_path: str) -> pd.DataFrame:
        """
        Enrich header DataFrame with entity/vendor identification
        
        Args:
            header_df: Original header DataFrame from parser
            pdf_path: Path to the PDF file for identification
            
        Returns:
            Enhanced DataFrame with additional columns
        """
        if header_df.empty:
            return header_df
        
        # Get identification context
        context = identify_invoice_context(pdf_path)
        
        # Create enriched copy
        enriched_df = header_df.copy()
        
        # Add enrichment columns
        enriched_df['invoiced_bu'] = context['entity_id']
        enriched_df['vendor_code'] = context['vendor_code']
        enriched_df['identified_vendor_name'] = context['vendor_name']
        enriched_df['identified_currency'] = context['currency']
        enriched_df['identification_success'] = context['identification_success']
        
        # Add processing metadata
        enriched_df['source_file'] = pdf_path
        enriched_df['processed_at'] = pd.Timestamp.now()
        
        return enriched_df
    
    def validate_identification(self, pdf_path: str) -> Dict:
        """
        Validate that entity/vendor can be identified before processing
        
        Returns:
            Dict with validation results and any issues found
        """
        context = identify_invoice_context(pdf_path)
        
        issues = []
        if not context['entity_id']:
            issues.append("Entity not identified or not found in database")
        if not context['vendor_code']:
            issues.append("Vendor code not found in entity-vendor mapping")
        if not context['vendor_name']:
            issues.append("Vendor not identified or not found in database")
        
        return {
            'is_valid': len(issues) == 0,
            'issues': issues,
            'context': context['context'],
            'entity_id': context['entity_id'],
            'vendor_code': context['vendor_code']
        }
    
    def get_processing_context(self, pdf_path: str) -> Dict:
        """
        Get full processing context for logging and debugging
        """
        return self.detector.detect_full_context_with_database(pdf_path)

# Integration functions for existing batch processor
def enhance_header_with_identification(header_df: pd.DataFrame, pdf_path: str) -> pd.DataFrame:
    """
    Main function to be called from batch_processor.py
    Enhances header DataFrame with entity/vendor identification
    """
    service = HeaderEnrichmentService()
    return service.enrich_header_dataframe(header_df, pdf_path)

def validate_invoice_for_processing(pdf_path: str) -> Dict:
    """
    Validate invoice before processing
    Call this before running parsers to ensure identification works
    """
    service = HeaderEnrichmentService()
    return service.validate_identification(pdf_path)

# Usage example for batch processor integration:
"""
# In your batch_processor.py, you would modify process_single_invoice like this:

def process_single_invoice(self, filepath: str) -> bool:
    try:
        # Step 1: Validate identification
        validation = validate_invoice_for_processing(filepath)
        if not validation['is_valid']:
            self.log_error(filepath, f"Identification failed: {', '.join(validation['issues'])}")
            return False
        
        # Step 2: Get provider and run parsers
        provider = self._detect_provider(filename)
        parser_func = self.parser_registry.get_parser(provider)
        
        # Extract header and detail data
        raw_header_df, detail_df = parser_func(filepath)
        
        # Step 3: Enrich header with identification
        enriched_header_df = enhance_header_with_identification(raw_header_df, filepath)
        
        # Step 4: Validate both extractions
        if enriched_header_df.empty or detail_df.empty:
            return False
            
        # Step 5: Load to Snowflake
        load_to_snowflake_header(self.session, enriched_header_df)
        load_to_snowflake_detailed(self.session, detail_df)
        
        return True
        
    except Exception as e:
        self.log_error(filepath, str(e))
        return False
"""