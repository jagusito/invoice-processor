# pre_processing_validator.py
"""
Pre-Processing Invoice Validation System
Validates all invoices in folder before processing and generates reports
"""

import os
import pandas as pd
from datetime import datetime
from enhanced_provider_detection import EnhancedProviderDetection
from typing import List, Dict
import json

class PreProcessingValidator:
    
    def __init__(self, invoice_folder: str = "invoices"):
        self.invoice_folder = invoice_folder
        self.detector = EnhancedProviderDetection()
        self.validation_results = []
    
    def validate_all_invoices(self) -> Dict:
        """
        Validate all PDFs in the invoice folder
        Returns comprehensive validation report
        """
        print("🔍 PRE-PROCESSING INVOICE VALIDATION")
        print("=" * 80)
        
        if not os.path.exists(self.invoice_folder):
            return {"error": f"Invoice folder '{self.invoice_folder}' not found"}
        
        pdf_files = [f for f in os.listdir(self.invoice_folder) if f.lower().endswith('.pdf')]
        
        if not pdf_files:
            return {"error": f"No PDF files found in '{self.invoice_folder}'"}
        
        print(f"📁 Found {len(pdf_files)} PDF files to validate")
        print("-" * 80)
        
        summary = {
            "total_files": len(pdf_files),
            "ready_for_processing": 0,
            "requires_attention": 0,
            "failed_identification": 0,
            "validation_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "details": []
        }
        
        for filename in sorted(pdf_files):
            filepath = os.path.join(self.invoice_folder, filename)
            result = self._validate_single_invoice(filepath)
            summary["details"].append(result)
            
            # Update counters
            if result["status"] == "✅ READY":
                summary["ready_for_processing"] += 1
            elif result["status"] == "⚠️ ATTENTION":
                summary["requires_attention"] += 1
            else:
                summary["failed_identification"] += 1
        
        # Generate reports
        self._print_summary_report(summary)
        self._generate_detailed_report(summary)
        self._generate_issues_report(summary)
        
        return summary
    
    def _validate_single_invoice(self, filepath: str) -> Dict:
        """Validate a single invoice and return detailed results"""
        filename = os.path.basename(filepath)
        
        try:
            # Get identification context
            context = self.detector.detect_full_context_with_database(filepath)
            
            # Determine status
            status = "✅ READY"
            issues = []
            
            if not context['entity_info']:
                issues.append("Entity not identified or not found in database")
                status = "❌ FAILED"
            
            if not context['vendor_info']:
                issues.append("Vendor not identified or not found in database")
                status = "❌ FAILED"
            
            if not context['entity_vendor_code']:
                issues.append("Entity-vendor mapping not found")
                status = "❌ FAILED"
            
            # Check for partial matches (might need attention)
            if context['entity_info'] and context['entity_info'].get('match_type') == 'partial':
                issues.append(f"Entity matched partially: '{context['detected_entity']['entity_name']}' → '{context['entity_info']['entity_name']}'")
                if status == "✅ READY":
                    status = "⚠️ ATTENTION"
            
            if context['vendor_info'] and context['vendor_info'].get('match_type') == 'partial':
                issues.append(f"Vendor matched partially")
                if status == "✅ READY":
                    status = "⚠️ ATTENTION"
            
            return {
                "filename": filename,
                "status": status,
                "entity_id": context['header_enrichment']['invoiced_bu'],
                "vendor_code": context['header_enrichment']['vendor_code'],
                "vendor_name": context['header_enrichment']['vendor_name'],
                "currency": context['header_enrichment']['currency'],
                "detected_entity": context['detected_entity']['entity_name'] if context['detected_entity'] else None,
                "detected_vendor": context['vendor_variant'],
                "issues": issues,
                "context": context
            }
            
        except Exception as e:
            return {
                "filename": filename,
                "status": "❌ ERROR",
                "entity_id": None,
                "vendor_code": None,
                "vendor_name": None,
                "currency": None,
                "detected_entity": None,
                "detected_vendor": None,
                "issues": [f"Validation error: {str(e)}"],
                "context": None
            }
    
    def _print_summary_report(self, summary: Dict):
        """Print summary report to console"""
        print("\n📊 VALIDATION SUMMARY")
        print("=" * 80)
        print(f"Total Files: {summary['total_files']}")
        print(f"✅ Ready for Processing: {summary['ready_for_processing']}")
        print(f"⚠️ Requires Attention: {summary['requires_attention']}")
        print(f"❌ Failed Identification: {summary['failed_identification']}")
        
        if summary['ready_for_processing'] == summary['total_files']:
            print("\n🎉 ALL INVOICES READY FOR PROCESSING!")
        else:
            print(f"\n⚠️ {summary['total_files'] - summary['ready_for_processing']} invoices need attention before processing")
        
        print("\n📋 DETAILED RESULTS:")
        print("-" * 80)
        
        for detail in summary['details']:
            status_icon = detail['status']
            entity_info = f"Entity: {detail['entity_id']}" if detail['entity_id'] else "Entity: ❌"
            vendor_info = f"Vendor: {detail['vendor_code']}" if detail['vendor_code'] else "Vendor: ❌"
            
            print(f"{status_icon} {detail['filename']}")
            print(f"    {entity_info} | {vendor_info}")
            
            if detail['issues']:
                for issue in detail['issues']:
                    print(f"    ⚠️ {issue}")
            print()
    
    def _generate_detailed_report(self, summary: Dict):
        """Generate detailed Excel report"""
        try:
            # Prepare data for Excel
            report_data = []
            for detail in summary['details']:
                report_data.append({
                    'Filename': detail['filename'],
                    'Status': detail['status'],
                    'Entity_ID': detail['entity_id'],
                    'Vendor_Code': detail['vendor_code'],
                    'Vendor_Name': detail['vendor_name'],
                    'Currency': detail['currency'],
                    'Detected_Entity': detail['detected_entity'],
                    'Detected_Vendor': detail['detected_vendor'],
                    'Issues': '; '.join(detail['issues']) if detail['issues'] else 'None'
                })
            
            # Create DataFrame and save
            df = pd.DataFrame(report_data)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"reports/invoice_validation_{timestamp}.xlsx"
            
            os.makedirs('reports', exist_ok=True)
            df.to_excel(filename, index=False, sheet_name='Validation Results')
            
            print(f"📄 Detailed report saved: {filename}")
            
        except Exception as e:
            print(f"⚠️ Could not generate Excel report: {e}")
    
    def _generate_issues_report(self, summary: Dict):
        """Generate issues-only report for fixing"""
        issues_found = [detail for detail in summary['details'] if detail['status'] != "✅ READY"]
        
        if not issues_found:
            print("✅ No issues found - all invoices ready for processing!")
            return
        
        print(f"\n🚨 ISSUES REQUIRING ATTENTION ({len(issues_found)} files)")
        print("=" * 80)
        
        # Group by issue type
        missing_entities = []
        missing_vendors = []
        missing_mappings = []
        partial_matches = []
        errors = []
        
        for issue_detail in issues_found:
            filename = issue_detail['filename']
            for issue in issue_detail['issues']:
                if "Entity not identified" in issue:
                    missing_entities.append({
                        'file': filename,
                        'detected': issue_detail['detected_entity']
                    })
                elif "Vendor not identified" in issue:
                    missing_vendors.append({
                        'file': filename,
                        'detected': issue_detail['detected_vendor']
                    })
                elif "mapping not found" in issue:
                    missing_mappings.append({
                        'file': filename,
                        'entity': issue_detail['entity_id'],
                        'vendor': issue_detail['vendor_name']
                    })
                elif "matched partially" in issue:
                    partial_matches.append({
                        'file': filename,
                        'issue': issue
                    })
                else:
                    errors.append({
                        'file': filename,
                        'error': issue
                    })
        
        # Print categorized issues
        if missing_entities:
            print("❌ MISSING ENTITIES (Add to Entity Catalog):")
            for item in missing_entities:
                print(f"   📄 {item['file']} → Detected: '{item['detected']}'")
            print()
        
        if missing_vendors:
            print("❌ MISSING VENDORS (Add to Vendor Catalog):")
            for item in missing_vendors:
                print(f"   📄 {item['file']} → Detected: '{item['detected']}'")
            print()
        
        if missing_mappings:
            print("❌ MISSING ENTITY-VENDOR MAPPINGS:")
            for item in missing_mappings:
                print(f"   📄 {item['file']} → Map Entity '{item['entity']}' to Vendor '{item['vendor']}'")
            print()
        
        if partial_matches:
            print("⚠️ PARTIAL MATCHES (Review for accuracy):")
            for item in partial_matches:
                print(f"   📄 {item['file']} → {item['issue']}")
            print()
        
        if errors:
            print("🚨 PROCESSING ERRORS:")
            for item in errors:
                print(f"   📄 {item['file']} → {item['error']}")
            print()
    
    def get_processing_readiness(self) -> Dict:
        """
        Quick check for processing readiness
        Returns simple ready/not ready status
        """
        summary = self.validate_all_invoices()
        
        return {
            "ready_for_processing": summary['ready_for_processing'] == summary['total_files'],
            "ready_count": summary['ready_for_processing'],
            "total_count": summary['total_files'],
            "issues_count": summary['requires_attention'] + summary['failed_identification']
        }

def validate_invoices_cli():
    """Command line interface for validation"""
    validator = PreProcessingValidator()
    summary = validator.validate_all_invoices()
    
    # Return exit code for scripting
    if summary['ready_for_processing'] == summary['total_files']:
        return 0  # Success - all ready
    else:
        return 1  # Issues found

if __name__ == "__main__":
    import sys
    exit_code = validate_invoices_cli()
    sys.exit(exit_code)