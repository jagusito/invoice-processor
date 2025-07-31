# parsers/headers/digital_realty_header.py
"""
Digital Realty Header Router
Routes to appropriate USA (Teik) or UK (Digital London) header parser
"""

import os
from parsers.headers import digital_realty_usa_header, digital_realty_uk_header

def extract_header(pdf_path: str):
    """
    Route to appropriate Digital Realty header parser based on filename
    """
    filename = os.path.basename(pdf_path).lower()
    
    if 'interxion' in filename:
        print("🇬🇧 Routing to Digital London Ltd (UK) header parser")
        return digital_realty_uk_header.extract_header(pdf_path)
    elif 'digitalrealty' in filename:
        print("🇺🇸 Routing to Teik - New York, LLC (USA) header parser")
        return digital_realty_usa_header.extract_header(pdf_path)
    else:
        # Fallback - check content if filename unclear
        print("🔄 Filename unclear, defaulting to USA parser")
        return digital_realty_usa_header.extract_header(pdf_path)