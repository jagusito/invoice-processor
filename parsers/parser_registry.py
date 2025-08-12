# parsers/parser_registry.py - UPDATED with Vodafone UK support
"""
Enhanced Multi-Vendor Parser Registry
Handles both header and detail parsers for multiple vendors and regional variants
"""

import importlib
import os
import re
from typing import Dict, Any, Optional, Callable, Tuple
import pandas as pd

class MultiVendorParserRegistry:
    """Registry to manage and route to different vendor and regional parsers"""
    
    def __init__(self):
        # Vendor detection patterns (from filename or content)
        self.vendor_detection_patterns = {
            'equinix': {
                'filename_patterns': [r'\.equinix\.', r'equinix'],
                'content_patterns': ['Equinix', 'IBX'],
                'header_parser': 'parsers.headers.equinix_header'
            },
            'lumen': {
                'filename_patterns': [
                    r'\.lumen\.', r'lumen', r'level3',
                    r'\.centurylink\.', r'centurylink', r'\.smb'
                ],
                'content_patterns': ['Lumen', 'Level 3', 'Lumen Technologies'],
                'header_parser': 'parsers.headers.lumen_header'
            },
            'digital_realty': {
                'filename_patterns': [r'\.digitalrealty\.', r'digitalrealty', r'\.interxion\.', r'interxion'],
                'content_patterns': ['Digital London Ltd.', 'Teik - New York, LLC', 'Digital Realty', 'Interxion'],
                'header_parser': 'parsers.headers.digital_realty_header'
            },
            
            # UPDATED: Vodafone with multi-branch support
            'vodafone': {
                'filename_patterns': [
                    r'\.vodafone\.', r'vodafone', r'\.voda\.', r'voda'
                ],
                'content_patterns': [
                    'Vodafone', 'Vodafone Business', 'Your registered address:', 
                    'Your invoice number'
                ],
                'header_parser': 'parsers.headers.vodafone_header'  # Will create router
            },
            
            'att': {
                'filename_patterns': [r'\.att\.', r'att'],
                'content_patterns': ['AT&T', 'ATT'],
                'header_parser': 'parsers.headers.att_header'
            }
        }
        
        # Detail parser mapping: vendor + regional variant → parser module
        self.detail_parser_mapping = {
            # Equinix regional variants
            ('equinix', 'Equinix, Inc'): 'parsers.details.equinix_usa_detail',
            ('equinix', 'Equinix (Germany) GmbH'): 'parsers.details.equinix_germany_detail',
            ('equinix', 'Equinix Singapore Pte. Ltd.'): 'parsers.details.equinix_singapore_detail',
            ('equinix', 'Equinix Japan K.K.'): 'parsers.details.equinix_japan_detail',
            ('equinix', 'Equinix Australia Pty Ltd'): 'parsers.details.equinix_australia_detail',
            ('equinix', 'Equinix Middle East FZ-LLC'): 'parsers.details.equinix_middle_east_detail',
            ('equinix', 'Equinix, Inc'): 'parsers.details.equinix_usglobe_detail',  # Override for Globe format
            
            # Lumen variants - same detail parser, different vendor names
            ('lumen', 'Lumen Technologies'): 'parsers.details.lumen_detail',
            ('lumen', 'Level 3 Communications'): 'parsers.details.lumen_detail',
            ('lumen', 'Lumen Technologies NL BV'): 'parsers.details.lumen_netherlands_detail',  # Netherlands variant            
            
            # Digital Realty variants
            ('digital_realty', 'Digital London Ltd.'): 'parsers.details.digital_realty_uk_detail',
            ('digital_realty', 'Telx - New York, LLC'): 'parsers.details.digital_realty_usa_detail',
            
            # UPDATED: Vodafone variants with branch-specific parsers - FIXED VENDOR NAMES
            ('vodafone', 'Vodafone Limited'): 'parsers.details.vodafone_uk_detail',
            ('vodafone', 'Vodafone PNG Ltd'): 'parsers.details.vodafone_png_detail',  # FIXED: Match catalog name
            
            # AT&T (may have regional variants)
            ('att', 'AT&T'): 'parsers.details.att_detail',
        }
        
        # Cache loaded modules to avoid repeated imports
        self._loaded_header_parsers = {}
        self._loaded_detail_parsers = {}
    
    def detect_vendor(self, pdf_path: str, content_sample: str = None) -> Optional[str]:
        """
        Detect vendor from filename and/or content
        
        Args:
            pdf_path: Path to PDF file
            content_sample: Sample text from PDF (optional)
            
        Returns:
            Vendor key or None if not detected
        """
        filename = os.path.basename(pdf_path).lower()
        
        for vendor, patterns in self.vendor_detection_patterns.items():
            # Check filename patterns
            for pattern in patterns['filename_patterns']:
                if re.search(pattern, filename):
                    print(f"🔍 Detected vendor '{vendor}' from filename pattern: {pattern}")
                    return vendor
            
            # Check content patterns if available
            if content_sample:
                for pattern in patterns['content_patterns']:
                    if pattern.lower() in content_sample.lower():
                        print(f"🔍 Detected vendor '{vendor}' from content pattern: {pattern}")
                        return vendor
        
        print(f"⚠️ Could not detect vendor from: {filename}")
        return None
    
    def get_header_parser(self, vendor: str) -> Optional[Callable]:
        """
        Get header parser for a vendor
        
        Args:
            vendor: Vendor key (e.g., 'equinix', 'lumen', 'digital_realty', 'vodafone')
            
        Returns:
            Header parser function or None
        """
        try:
            vendor_config = self.vendor_detection_patterns.get(vendor)
            if not vendor_config:
                print(f"⚠️ No configuration found for vendor: {vendor}")
                return None
            
            module_name = vendor_config['header_parser']
            
            # Check if already loaded
            if module_name in self._loaded_header_parsers:
                return self._loaded_header_parsers[module_name]
            
            # Dynamically import the header parser
            print(f"📦 Loading header parser: {module_name}")
            module = importlib.import_module(module_name)
            
            # Get the extract function (standardized name)
            if hasattr(module, 'extract_header'):
                parser_func = module.extract_header
            elif hasattr(module, 'extract_equinix_header'):  # Backward compatibility
                parser_func = module.extract_equinix_header
            else:
                print(f"❌ Header parser module {module_name} missing standard extract function")
                return None
            
            self._loaded_header_parsers[module_name] = parser_func
            print(f"✅ Loaded header parser for: {vendor}")
            return parser_func
            
        except ImportError as e:
            print(f"❌ Failed to import header parser for {vendor}: {e}")
            return None
        except Exception as e:
            print(f"❌ Error loading header parser for {vendor}: {e}")
            return None


    def get_detail_parser(self, vendor: str, vendor_name: str) -> Optional[Callable]:
        """
        Get detail parser for a vendor and regional variant
        """
        try:
            # Look for exact match first
            parser_key = (vendor, vendor_name)
            module_name = self.detail_parser_mapping.get(parser_key)
            
            print(f"🔍 Looking for parser_key: {parser_key}")
            print(f"🔍 Found module_name: {module_name}")
            
            if not module_name:
                print(f"⚠️ No detail parser found for vendor: {vendor}, variant: {vendor_name}")
                print(f"🔍 Available mappings:")
                for key in self.detail_parser_mapping.keys():
                    if key[0] == vendor:
                        print(f"    {key}")
                return None
            
            # Check if already loaded
            if module_name in self._loaded_detail_parsers:
                print(f"✅ Using cached parser: {module_name}")
                return self._loaded_detail_parsers[module_name]
            
            # Dynamically import the detail parser
            print(f"📦 Loading detail parser: {module_name}")
            module = importlib.import_module(module_name)
            print(f"✅ Module imported successfully: {module}")
            
            # Get the extract function (standardized name)
            if hasattr(module, 'extract_equinix_items'):
                parser_func = module.extract_equinix_items
                print(f"✅ Found extract_equinix_items function: {parser_func}")
                self._loaded_detail_parsers[module_name] = parser_func
                print(f"✅ Loaded detail parser for: {vendor} - {vendor_name}")
                return parser_func
            else:
                print(f"❌ Detail parser module {module_name} missing extract_equinix_items function")
                print(f"🔍 Available functions: {[attr for attr in dir(module) if not attr.startswith('_')]}")
                return None
                
        except ImportError as e:
            print(f"❌ Failed to import detail parser for {vendor} - {vendor_name}: {e}")
            return None
        except Exception as e:
            print(f"❌ Error loading detail parser for {vendor} - {vendor_name}: {e}")
            return None

   
    def extract_header(self, pdf_path: str, vendor: str = None) -> pd.DataFrame:
        """
        Extract header using appropriate vendor parser
        
        Args:
            pdf_path: Path to PDF invoice
            vendor: Vendor key (if known), otherwise auto-detect
            
        Returns:
            DataFrame with header data
        """
        # Auto-detect vendor if not provided
        if not vendor:
            vendor = self.detect_vendor(pdf_path)
            if not vendor:
                print(f"❌ Cannot determine vendor for: {pdf_path}")
                return pd.DataFrame()
        
        # Get and use header parser
        header_parser = self.get_header_parser(vendor)
        if header_parser:
            print(f"🎯 Using header parser for vendor: {vendor}")
            return header_parser(pdf_path)
        else:
            print(f"❌ No header parser available for vendor: {vendor}")
            return pd.DataFrame()
    
    def extract_details(self, pdf_path: str, header_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Extract details using appropriate vendor and regional parser
        
        Args:
            pdf_path: Path to PDF invoice
            header_data: Header context data with vendor information
            
        Returns:
            DataFrame with detail line items
        """
        # Determine vendor from header data or filename
        vendor_name = header_data.get('vendor', 'UNKNOWN')
        
        # Map vendor name back to vendor key
        vendor = None
        for v, config in self.vendor_detection_patterns.items():
            for pattern in config['content_patterns']:
                if pattern.lower() in vendor_name.lower():
                    vendor = v
                    break
            if vendor:
                break
        
        # Fallback to filename detection
        if not vendor:
            vendor = self.detect_vendor(pdf_path)
        
        if not vendor:
            print(f"❌ Cannot determine vendor for detail extraction: {vendor_name}")
            return pd.DataFrame()
        
        # Get and use detail parser
        detail_parser = self.get_detail_parser(vendor, vendor_name)
        if detail_parser:
            print(f"🎯 Using detail parser for vendor: {vendor} - {vendor_name}")
            return detail_parser(pdf_path, header_data)
        else:
            print(f"❌ No detail parser available for vendor: {vendor} - {vendor_name}")
            return pd.DataFrame()
    
    def process_complete_invoice(self, pdf_path: str, vendor: str = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Complete invoice processing: header + details
        
        Args:
            pdf_path: Path to PDF invoice
            vendor: Vendor key (optional, will auto-detect)
            
        Returns:
            Tuple of (header_df, detail_df)
        """
        print(f"🔄 Processing complete invoice: {os.path.basename(pdf_path)}")
        
        # Extract header
        header_df = self.extract_header(pdf_path, vendor)
        if header_df.empty:
            print("❌ Header extraction failed")
            return pd.DataFrame(), pd.DataFrame()
        
        # Extract details
        header_data = header_df.iloc[0].to_dict()
        detail_df = self.extract_details(pdf_path, header_data)
        
        return header_df, detail_df
    
    def get_registry_status(self) -> Dict[str, Any]:
        """Get comprehensive status of the registry"""
        status = {
            'supported_vendors': list(self.vendor_detection_patterns.keys()),
            'header_parsers': {},
            'detail_parsers': {},
            'loaded_modules': {
                'headers': len(self._loaded_header_parsers),
                'details': len(self._loaded_detail_parsers)
            }
        }
        
        # Check header parser status
        for vendor, config in self.vendor_detection_patterns.items():
            module_name = config['header_parser']
            try:
                importlib.import_module(module_name)
                status['header_parsers'][vendor] = "AVAILABLE"
            except ImportError:
                status['header_parsers'][vendor] = "MISSING"
        
        # Check detail parser status  
        for (vendor, variant), module_name in self.detail_parser_mapping.items():
            key = f"{vendor}_{variant.replace(' ', '_').replace('.', '').replace(',', '')}"
            try:
                importlib.import_module(module_name)
                status['detail_parsers'][key] = "AVAILABLE"
            except ImportError:
                status['detail_parsers'][key] = "MISSING"
        
        return status

# Global registry instance
registry = MultiVendorParserRegistry()

# Convenience functions for direct use
def extract_header(pdf_path: str, vendor: str = None) -> pd.DataFrame:
    """Extract header using appropriate vendor parser"""
    return registry.extract_header(pdf_path, vendor)

def extract_details(pdf_path: str, header_data: Dict[str, Any]) -> pd.DataFrame:
    """Extract details using appropriate vendor and regional parser"""
    return registry.extract_details(pdf_path, header_data)

def process_complete_invoice(pdf_path: str, vendor: str = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Process complete invoice: header + details"""
    return registry.process_complete_invoice(pdf_path, vendor)

def get_supported_vendors() -> list:
    """Get list of supported vendors"""
    return list(registry.vendor_detection_patterns.keys())

def get_registry_status() -> Dict[str, Any]:
    """Get comprehensive registry status"""
    return registry.get_registry_status()

# For testing and debugging
if __name__ == "__main__":
    print("🧪 Testing Enhanced Multi-Vendor Parser Registry")
    print("=" * 60)
    
    # Get registry status
    status = get_registry_status()
    
    print(f"📋 Supported Vendors: {', '.join(status['supported_vendors'])}")
    
    print(f"\n📊 Header Parser Status:")
    for vendor, state in status['header_parsers'].items():
        icon = "✅" if state == "AVAILABLE" else "❌"
        print(f"  {icon} {vendor}: {state}")
    
    print(f"\n📊 Detail Parser Status:")
    for parser, state in status['detail_parsers'].items():
        icon = "✅" if state == "AVAILABLE" else "❌"
        print(f"  {icon} {parser}: {state}")
    
    print(f"\n📦 Loaded Modules: {status['loaded_modules']}")
    
    # Test vendor detection
    print(f"\n🔍 Testing Vendor Detection:")
    test_files = [
        "invoices/1751423522.equinix.pdf",
        "invoices/sample.lumen.pdf", 
        "invoices/1752029216.centurylink.smb.pdf",  # Netherlands Lumen test
        "invoices/1728531828.interxion.pdf",        # Digital Realty test
        "invoices/test.vodafone.uk.pdf",            # UPDATED: Vodafone UK test
        "invoices/test.vodafone.png.pdf"            # UPDATED: Vodafone PNG test
    ]
    
    for test_file in test_files:
        vendor = registry.detect_vendor(test_file)
        print(f"  {test_file} → {vendor}")