# enhanced_provider_detection.py - UPDATED with Vodafone UK support
import fitz  # PyMuPDF
import re
import os
from typing import Dict, Optional
from config.snowflake_config import get_snowflake_session

class EnhancedProviderDetection:
    
    def __init__(self):
        """Initialize detection patterns with correct vendor names"""
        self.vendor_patterns = {
            'equinix_inc': {
                'header_indicators': ['Equinix, INC', 'Equinix, Inc', '77-0487526'],
                'currency': 'USD',
                'vendor_name': 'Equinix, Inc'
            },
            'equinix_germany': {
                'header_indicators': ['Equinix (Germany) GmbH', 'Ust-ID DE813255814'],
                'currency': 'EUR',
                'vendor_name': 'Equinix (Germany) GmbH'
            },
            'equinix_middle_east': {
                'header_indicators': ['Equinix Middle East FZ-LLC', 'TRN 100349080000003'],
                'currency': 'USD',
                'vendor_name': 'Equinix Middle East FZ-LLC'
            },
            'equinix_japan': {
                'header_indicators': ['Equinix Japan', '株式会社'],
                'currency': 'JPY',
                'vendor_name': 'Equinix Japan KK'
            },
            'equinix_singapore': {
                'header_indicators': ['Equinix Singapore', 'GST REG'],  # More specific GST indicator
                'currency': 'SGD',
                'vendor_name': 'Equinix Singapore Pte Ltd'
            },
            'equinix_australia': {
                'header_indicators': ['Equinix Australia', 'ABN'],
                'currency': 'AUD',
                'vendor_name': 'Equinix Australia Pty Ltd'
            },
            'lumen': {
                'header_indicators': ['Lumen Technologies', 'Lumen'],
                'currency': 'USD',
                'vendor_name': 'Lumen Technologies'
            },
            
            # UPDATED: Enhanced Vodafone detection with UK branch support
            'vodafone_uk': {
                'header_indicators': [
                    'Vodafone Business UK', 'Your registered address:', 
                    'Vodafone', 'GBP', 'United Kingdom'
                ],
                'currency': 'GBP',
                'vendor_name': 'Vodafone Business UK'
            },
            # UPDATED: PNG branch detection with filename support
            'vodafone_png': {
                'header_indicators': [
                    'Vodafone PNG Ltd', 'PNG', 'Papua New Guinea', 'Kina', 'PGK'
                ],
                'currency': 'PGK',  # Papua New Guinea Kina
                'vendor_name': 'VODAFONE PNG'  # FIXED: Match catalog exactly
            },
            
            'digital_realty_usa': {
                'header_indicators': ['Teik - New York, LLC', 'Teik', 'New York', 'digitalrealty'],
                'currency': 'USD',
                'vendor_name': 'Teik - New York, LLC'
            },
            'digital_realty_uk': {
                'header_indicators': ['Digital London Ltd', 'Digital London', 'interxion', 'London'],
                'currency': 'GBP',
                'vendor_name': 'Digital London Ltd'
            },
                                    
            'att': {
                'header_indicators': ['AT&T', 'ATT'],
                'currency': 'USD',
                'vendor_name': 'AT&T'
            }
        }
        
        # Entity name patterns - companies that receive invoices
        self.entity_patterns = [
            'Speedcast Communications, Inc',
            'Speedcast Communications Inc',
            'Globecomm Network Services Corp',
            'Globecomm Systems Inc.',
            'Speedcast',
            'Globecomm',
            'Hermes Datacommunications International Ltd',
            'Hermes Datacommunications'

        ]
    
    def detect_provider_from_filename(self, filename: str) -> Dict:
        """Initial provider detection from filename"""
        filename = filename.lower()
        
        if ".att." in filename:
            return {'base_provider': 'att', 'needs_header_detection': False}
        
        if ".techflow." in filename:
            return {'base_provider': 'techflow', 'needs_header_detection': False}
        
        if ".lumen." in filename:
            return {'base_provider': 'lumen', 'needs_header_detection': False}
            
        # UPDATED: Vodafone detection with branch support
        if ".vodafone." in filename:
            return {'base_provider': 'vodafone', 'needs_header_detection': True}
            
        if ".interxion." in filename:
            return {'base_provider': 'digital_realty', 'needs_header_detection': True}
            
        
        if ".equinix." in filename:
            return {
                'base_provider': 'equinix',
                'needs_header_detection': True
            }
        
        return {'base_provider': 'unknown', 'needs_header_detection': True}
    
    def extract_header_text(self, pdf_path: str, max_chars: int = 3000) -> str:
        """Extract first page text for detection"""
        try:
            doc = fitz.open(pdf_path)
            first_page = doc[0]
            text = first_page.get_text()[:max_chars]
            doc.close()
            return text
        except Exception as e:
            print(f"Error extracting header text: {e}")
            return ""
    
    def detect_vendor_variant(self, header_text: str, base_provider: str) -> Optional[str]:
        """
        UPDATED: Detect specific vendor variant using scoring system
        Added Vodafone UK/PNG branch detection
        """
        
        if base_provider == 'equinix':
            # Check each Equinix variant and score by specificity
            variant_scores = {}
            
            for variant, config in self.vendor_patterns.items():
                if variant.startswith('equinix_'):
                    indicators = config['header_indicators']
                    found_indicators = [ind for ind in indicators if ind in header_text]
                    
                    if found_indicators:
                        # Score based on specificity and number of matches
                        score = 0
                        for indicator in found_indicators:
                            if 'Equinix' in indicator and any(country in indicator for country in ['Australia', 'Singapore', 'Germany', 'Japan', 'Middle East']):
                                score += 10  # High score for specific country/company names
                            elif indicator in ['ABN', 'GST REG', 'Ust-ID', 'TRN', '77-0487526', '株式会社']:
                                score += 5   # Medium score for specific tax/registration IDs
                            elif indicator in ['GST']:
                                score += 1   # Low score for generic indicators
                            else:
                                score += 3   # Default score
                        
                        variant_scores[variant] = {
                            'score': score,
                            'found_indicators': found_indicators
                        }
                        
                        print(f"   {variant}: {found_indicators} → Score: {score}")
            
            # Return the variant with the highest score
            if variant_scores:
                best_variant = max(variant_scores.keys(), key=lambda v: variant_scores[v]['score'])
                best_score = variant_scores[best_variant]['score']
                print(f"   🎯 Best match: {best_variant} (score: {best_score})")
                return best_variant
            
            # Default fallback to INC if no specific match
            print(f"   🔄 No specific match found, defaulting to equinix_inc")
            return 'equinix_inc'
        
        # UPDATED: Vodafone branch detection with PNG filename support
        if base_provider == 'vodafone':
            # Method 1: Filename-based routing (fastest) - ENHANCED for PNG detection
            if hasattr(self, '_current_filename'):
                filename = self._current_filename.lower()
            else:
                filename = ''
            
            # Check for specific branch indicators in filename
            if 'uk' in filename or 'britain' in filename:
                print(f"   🇬🇧 Filename indicates UK branch")
                return 'vodafone_uk'
            elif 'png' in filename or 'papua' in filename:  # FIXED: Will match .vodafone.png.pdf
                print(f"   🇵🇬 Filename indicates Papua New Guinea branch")
                return 'vodafone_png'
            
            # Method 2: Content-based analysis with scoring
            variant_scores = {}
            
            for variant, config in self.vendor_patterns.items():
                if variant.startswith('vodafone_'):
                    indicators = config['header_indicators']
                    found_indicators = [ind for ind in indicators if ind in header_text]
                    
                    if found_indicators:
                        score = 0
                        for indicator in found_indicators:
                            # High score for exact company names and unique patterns
                            if indicator in ['Vodafone Business UK', 'Your registered address:']:
                                score += 10
                            elif indicator in ['Vodafone PNG Ltd']:  # UPDATED for PNG
                                score += 10
                            # Medium score for country/currency indicators
                            elif indicator in ['United Kingdom', 'Papua New Guinea', 'GBP', 'PGK', 'Kina']:
                                score += 5
                            # Low score for generic indicators
                            elif indicator in ['Vodafone', 'PNG']:
                                score += 2
                            else:
                                score += 3
                        
                        variant_scores[variant] = {
                            'score': score,
                            'found_indicators': found_indicators
                        }
                        print(f"   {variant}: {found_indicators} → Score: {score}")
            
            # Return highest scoring variant
            if variant_scores:
                best_variant = max(variant_scores.keys(), key=lambda v: variant_scores[v]['score'])
                print(f"   🎯 Best match: {best_variant}")
                return best_variant
            
            # Default fallback to UK (most common)
            print(f"   🔄 No specific match found, defaulting to vodafone_uk")
            return 'vodafone_uk'
        
        if base_provider == 'digital_realty':
            # Check filename patterns first
            if hasattr(self, '_current_filename'):
                filename = self._current_filename.lower()
            else:
                filename = ''
            
            if 'interxion' in filename:
                print(f"   🇬🇧 Filename indicates UK branch (Interxion)")
                return 'digital_realty_uk'
            elif 'digitalrealty' in filename:
                print(f"   🇺🇸 Filename indicates USA branch (Digital Realty)")
                return 'digital_realty_usa'
            
            # Fallback to content analysis with scoring
            variant_scores = {}
            
            for variant, config in self.vendor_patterns.items():
                if variant.startswith('digital_realty_'):
                    indicators = config['header_indicators']
                    found_indicators = [ind for ind in indicators if ind in header_text]
                    
                    if found_indicators:
                        score = len(found_indicators) * 5  # Base score
                        # Boost score for exact company name matches
                        if 'Digital London Ltd' in header_text:
                            score += 10
                        elif 'Teik - New York, LLC' in header_text:
                            score += 10
                        
                        variant_scores[variant] = {
                            'score': score,
                            'found_indicators': found_indicators
                        }
                        print(f"   {variant}: {found_indicators} → Score: {score}")
            
            # Return best match
            if variant_scores:
                best_variant = max(variant_scores.keys(), key=lambda v: variant_scores[v]['score'])
                return best_variant
            
            # Default to USA
            return 'digital_realty_usa'
        
        # For non-multi-branch providers, return as-is
        return base_provider
    
    def detect_entity_from_header(self, header_text: str) -> Optional[Dict]:
        """Detect entity (customer) from header text"""
        lines = header_text.split('\n')
        
        for i, line in enumerate(lines[15:35], 15):
            line = line.strip()
            
            for entity_pattern in self.entity_patterns:
                if entity_pattern in line:
                    return {
                        'entity_name': entity_pattern,
                        'entity_code': self._generate_entity_code(entity_pattern),
                        'found_at_line': i,
                        'detected_line': line
                    }
            
            if any(indicator in line.lower() for indicator in ['inc', 'corp', 'llc', 'ltd', 'gmbh']):
                if len(line) > 10 and not any(skip in line.lower() 
                    for skip in ['equinix', 'invoice', 'payment', 'bank', 'account']):
                    return {
                        'entity_name': line,
                        'entity_code': self._generate_entity_code(line),
                        'found_at_line': i,
                        'detected_line': line
                    }
        
        return None
    
    def _generate_entity_code(self, entity_name: str) -> str:
        """Generate entity code from entity name"""
        entity_mapping = {
            'Speedcast Communications, Inc': 'SPEEDCAST_INC',
            'Speedcast Communications Inc': 'SPEEDCAST_INC',
            'Globecomm Network Services Corp': 'GLOBECOMM_NET',
            'Globecomm Systems Inc.': 'GLOBECOMM_SYS',
            'Speedcast': 'SPEEDCAST_INC',
            'Globecomm': 'GLOBECOMM_SYS'
        }
        
        return entity_mapping.get(entity_name, entity_name.upper().replace(' ', '_')[:20])
    
    def get_vendor_info(self, variant: str) -> Dict:
        """Get vendor configuration information"""
        return self.vendor_patterns.get(variant, {})
    
    def lookup_entity_in_database(self, detected_entity: Dict) -> Optional[Dict]:
        """Lookup detected entity in ENTITY_CATALOG table"""
        if not detected_entity:
            return None
            
        try:
            session = get_snowflake_session()
            
            entity_name = detected_entity['entity_name']
            query = f"""
                SELECT ENTITY_ID, ENTITY_NAME, ENTITY_TYPE, STATUS
                FROM ENTITY_CATALOG 
                WHERE UPPER(ENTITY_NAME) = UPPER('{entity_name.replace("'", "''")}')
                AND STATUS = 'Active'
            """
            
            result = session.sql(query).collect()
            if result:
                return {
                    'entity_id': result[0][0],
                    'entity_name': result[0][1],
                    'entity_type': result[0][2],
                    'status': result[0][3],
                    'match_type': 'exact'
                }
            
            # Try partial match for common variants
            partial_queries = []
            if 'Speedcast' in entity_name:
                partial_queries.append("ENTITY_NAME LIKE '%Speedcast%'")
            if 'Globecomm' in entity_name:
                partial_queries.append("ENTITY_NAME LIKE '%Globecomm%'")
            
            for partial_condition in partial_queries:
                query = f"""
                    SELECT ENTITY_ID, ENTITY_NAME, ENTITY_TYPE, STATUS
                    FROM ENTITY_CATALOG 
                    WHERE {partial_condition}
                    AND STATUS = 'Active'
                    LIMIT 1
                """
                
                result = session.sql(query).collect()
                if result:
                    return {
                        'entity_id': result[0][0],
                        'entity_name': result[0][1],
                        'entity_type': result[0][2],
                        'status': result[0][3],
                        'match_type': 'partial'
                    }
            
            return None
            
        except Exception as e:
            print(f"Error looking up entity in database: {e}")
            return None
    
    def lookup_vendor_in_database(self, vendor_variant: str) -> Optional[Dict]:
        """Lookup detected vendor in VENDOR_CATALOG table"""
        try:
            session = get_snowflake_session()
            
            vendor_config = self.get_vendor_info(vendor_variant)
            if not vendor_config or 'vendor_name' not in vendor_config:
                print(f"No vendor config found for variant: {vendor_variant}")
                return None
                
            vendor_name = vendor_config['vendor_name']
            
            query = f"""
                SELECT VENDOR_NAME, VENDOR_TYPE, CURRENCY, STATUS
                FROM VENDOR_CATALOG 
                WHERE UPPER(VENDOR_NAME) = UPPER('{vendor_name.replace("'", "''")}')
                AND STATUS = 'Active'
            """
            
            result = session.sql(query).collect()
            if result:
                return {
                    'vendor_name': result[0][0],
                    'vendor_type': result[0][1],
                    'currency': result[0][2],
                    'status': result[0][3],
                    'match_type': 'exact'
                }
            
            print(f"Vendor '{vendor_name}' not found in catalog for variant '{vendor_variant}'")
            return None
            
        except Exception as e:
            print(f"Error looking up vendor in database: {e}")
            return None
    
    def lookup_entity_vendor_code(self, entity_id: str, vendor_name: str) -> Optional[str]:
        """Lookup entity-specific vendor code from ENTITY_VENDOR_MAPPING table"""
        try:
            if not entity_id or not vendor_name:
                return None
                
            session = get_snowflake_session()
            
            query = f"""
                SELECT ENTITY_VENDOR_CODE
                FROM ENTITY_VENDOR_MAPPING 
                WHERE ENTITY_ID = '{entity_id}' 
                AND VENDOR_NAME = '{vendor_name.replace("'", "''")}'
                AND STATUS = 'Active'
            """
            
            result = session.sql(query).collect()
            if result:
                return result[0][0]
            
            return None
            
        except Exception as e:
            print(f"Error looking up entity vendor code: {e}")
            return None
    
    def detect_full_context_with_database(self, filepath: str) -> Dict:
        """Complete detection with database lookups"""
        filename = os.path.basename(filepath)
        
        # Store filename for use in vendor variant detection
        self._current_filename = filename
        
        provider_info = self.detect_provider_from_filename(filename)
        header_text = self.extract_header_text(filepath)
        
        if provider_info['needs_header_detection']:
            vendor_variant = self.detect_vendor_variant(header_text, provider_info['base_provider'])
        else:
            vendor_variant = provider_info['base_provider']
        
        detected_entity = self.detect_entity_from_header(header_text)
        entity_info = self.lookup_entity_in_database(detected_entity)
        vendor_info = self.lookup_vendor_in_database(vendor_variant)
        
        entity_vendor_code = None
        if entity_info and vendor_info:
            entity_vendor_code = self.lookup_entity_vendor_code(
                entity_info['entity_id'], 
                vendor_info['vendor_name']
            )
        
        return {
            'filename': filename,
            'vendor_variant': vendor_variant,
            'detected_entity': detected_entity,
            'entity_info': entity_info,
            'vendor_info': vendor_info,
            'entity_vendor_code': entity_vendor_code,
            'header_enrichment': {
                'invoiced_bu': entity_info['entity_id'] if entity_info else None,
                'vendor_code': entity_vendor_code,
                'currency': vendor_info['currency'] if vendor_info else None,
                'vendor_name': vendor_info['vendor_name'] if vendor_info else None
            },
            'identification_status': {
                'entity_found': entity_info is not None,
                'vendor_found': vendor_info is not None,
                'mapping_found': entity_vendor_code is not None,
                'ready_for_processing': all([entity_info, vendor_info, entity_vendor_code])
            },
            'header_text_preview': header_text[:500] + "..." if len(header_text) > 500 else header_text
        }

def identify_invoice_context(pdf_path: str) -> Dict:
    """Main function to be called by batch processor"""
    detector = EnhancedProviderDetection()
    context = detector.detect_full_context_with_database(pdf_path)
    
    return {
        'entity_id': context['header_enrichment']['invoiced_bu'],
        'vendor_code': context['header_enrichment']['vendor_code'],
        'vendor_name': context['header_enrichment']['vendor_name'],
        'currency': context['header_enrichment']['currency'],
        'identification_success': context['identification_status']['ready_for_processing'],
        'context': context
    }

if __name__ == "__main__":
    # Test with Vodafone UK file
    test_file = "invoices/test.vodafone.uk.pdf"
    result = identify_invoice_context(test_file)
    print(f"🎯 VODAFONE UK TEST RESULT:")
    print(f"   Variant: {result['context']['vendor_variant']}")
    print(f"   Vendor: {result['vendor_name']}")
    print(f"   Currency: {result['currency']}")