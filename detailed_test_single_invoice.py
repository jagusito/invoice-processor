# detailed_test_single_invoice.py
"""
Test the enhanced multi-vendor parser registry
Automatically detects vendor and routes to appropriate header and detail parsers
"""

import os
import sys
import pandas as pd
sys.path.append('.')

from parsers.parser_registry import process_complete_invoice, get_registry_status

def test_single_invoice_detailed(filepath: str):
    """Test single invoice with automatic vendor detection and parser selection"""
    
    filename = os.path.basename(filepath)
    print(f"🔍 DETAILED TEST: {filename}")
    print("=" * 80)
    
    # STAGE 1: Complete Invoice Processing (Header + Details)
    print("\n🔄 STAGE 1: COMPLETE INVOICE PROCESSING")
    print("-" * 50)
    
    header_df, detail_df = process_complete_invoice(filepath)
    
    if header_df.empty:
        print("❌ Header extraction failed!")
        return False
    
    if detail_df.empty:
        print("❌ Detail extraction failed!")
        return False
    
    header_data = header_df.iloc[0].to_dict()
    vendor_name = header_data.get('vendor', 'UNKNOWN')
    
    print("✅ Header Data:")
    for key, value in header_data.items():
        if key not in ['created_at']:  # Skip timestamp for cleaner output
            print(f"  {key:<15}: {value}")
    
    print(f"\n✅ Extracted {len(detail_df)} detail records")
    
    # STAGE 2: COMPREHENSIVE DATA OUTPUT
    print(f"\n📝 STAGE 2: COMPLETE RECORD LIST")
    print("=" * 80)
    
    # Output 1: Full detailed table (all columns)
    print("\n🔍 FULL DETAIL TABLE (All Columns):")
    print("-" * 120)
    
    # Select key columns for display
    display_columns = [
        'item_number', 'description', 'usoc', 'units', 
        'amount', 'tax', 'total'
    ]
    
    # Only show columns that exist
    available_columns = [col for col in display_columns if col in detail_df.columns]
    
    # Create display DataFrame with formatting
    display_df = detail_df[available_columns].copy()
    
    # Format numeric columns for better readability
    for col in ['amount', 'tax', 'total']:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(lambda x: f"${x:,.2f}" if pd.notnull(x) and x != 0 else "")
    
    # Print with row numbers for easy reference
    print(display_df.to_string(index=True, max_colwidth=50))
    
    # Output 2: Summary calculations
    print(f"\n📊 FINANCIAL SUMMARY:")
    print("-" * 40)
    
    total_amount = detail_df['amount'].sum()
    total_tax = detail_df['tax'].sum()
    total_grand = detail_df['total'].sum()
    header_total = header_data.get('invoice_total', 0)
    
    print(f"💰 Sum of Amounts:     ${total_amount:,.2f}")
    print(f"🏛️  Sum of Taxes:       ${total_tax:,.2f}")
    print(f"📊 Sum of Totals:      ${total_grand:,.2f}")
    print(f"📋 Header Total:       ${header_total:,.2f}")
    print(f"🔍 Calculated Total:   ${total_amount + total_tax:,.2f}")
    
    # Check for discrepancies
    calc_total = total_amount + total_tax
    if abs(calc_total - header_total) < 0.01:
        print("✅ Totals match header!")
    else:
        diff = calc_total - header_total
        print(f"⚠️  Difference: ${diff:,.2f}")
    
    # Output 3: CSV export for detailed analysis
    csv_filename = f"test_results_{filename.replace('.pdf', '')}_details.csv"
    detail_df.to_csv(csv_filename, index=False)
    print(f"\n💾 Full data exported to: {csv_filename}")
    
    # Output 4: Issue detection
    print(f"\n🔍 DATA QUALITY CHECKS:")
    print("-" * 40)
    
    # Check for missing descriptions
    missing_desc = detail_df['description'].isna().sum()
    print(f"📝 Missing descriptions: {missing_desc}")
    
    # Check for zero amounts
    zero_amounts = (detail_df['amount'] == 0).sum()
    print(f"💰 Zero amounts: {zero_amounts}")
    
    # Check vendor-specific fields
    if 'regional_variant' in detail_df.columns:
        variants = detail_df['regional_variant'].unique()
        print(f"🌍 Regional variants: {', '.join(variants)}")
    
    # Output 5: Quick comparison format (for manual checking)
    print(f"\n📋 QUICK COMPARISON FORMAT:")
    print("-" * 60)
    print("Line | Description (first 30 chars) | Amount | Tax | Total")
    print("-" * 60)
    
    for idx, row in detail_df.iterrows():
        line_num = idx + 1
        desc = str(row['description'])[:30] + "..." if len(str(row['description'])) > 30 else str(row['description'])
        amount = row['amount']
        tax = row['tax']
        total = row['total']
        print(f"{line_num:4d} | {desc:<33} | ${amount:8.2f} | ${tax:6.2f} | ${total:8.2f}")
    
    print("\n" + "=" * 80)
    print("🎯 TEST COMPLETE")
    print(f"📄 Invoice: {filename}")
    print(f"🏢 Vendor: {vendor_name}")
    print(f"📊 Records: {len(detail_df)}")
    print(f"💾 CSV Export: {csv_filename}")
    print("=" * 80)
    
    return True

def test_registry_status():
    """Test the enhanced registry functionality"""
    
    print("🧪 ENHANCED PARSER REGISTRY TEST")
    print("=" * 60)
    
    # Get comprehensive status
    status = get_registry_status()
    
    print(f"📋 Supported Vendors: {', '.join(status['supported_vendors'])}")
    
    print(f"\n📊 Header Parser Status:")
    for vendor, state in status['header_parsers'].items():
        icon = "✅" if state == "AVAILABLE" else "❌"
        print(f"  {icon} {vendor}: {state}")
    
    print(f"\n📊 Detail Parser Status:")
    for parser, state in status['detail_parsers'].items():
        icon = "✅" if state == "AVAILABLE" else "❌"
        # Format the parser name nicely
        vendor, variant = parser.split('_', 1)
        print(f"  {icon} {vendor} → {variant.replace('_', ' ')}: {state}")
    
    print(f"\n📦 Currently Loaded Modules:")
    print(f"  Headers: {status['loaded_modules']['headers']}")
    print(f"  Details: {status['loaded_modules']['details']}")

def test_vendor_detection():
    """Test vendor detection from filenames"""
    
    print(f"\n🔍 VENDOR DETECTION TEST:")
    print("-" * 30)
    
    from parsers.parser_registry import registry
    
    test_files = [
        "invoices/1751423522.equinix.pdf",
        "invoices/sample.lumen.pdf", 
        "invoices/test.vodafone.pdf",
        "invoices/bill.att.pdf",
        "invoices/unknown_vendor.pdf"
    ]
    
    for test_file in test_files:
        vendor = registry.detect_vendor(test_file)
        filename = os.path.basename(test_file)
        status = "✅" if vendor else "❌"
        print(f"  {status} {filename:<30} → {vendor or 'Unknown'}")

def test_multiple_invoices():
    """Test multiple invoices to verify consistency"""
    
    test_files = [
        #"invoices/1751423522.equinix.pdf",
        #"invoices/1751534362.equinix.pdf",  # German variant
        #"invoices/1751514405.equinix.singapore.pdf",
        # Add more test files as available
    ]
    
    print("\n🔄 Testing Multiple Invoices")
    print("=" * 60)
    
    results = []
    
    for filepath in test_files:
        if os.path.exists(filepath):
            print(f"\n🧪 Testing: {os.path.basename(filepath)}")
            print("-" * 40)
            success = test_single_invoice_detailed(filepath)
            results.append({
                'file': os.path.basename(filepath),
                'success': success
            })
        else:
            print(f"⚠️ File not found: {filepath}")
            results.append({
                'file': os.path.basename(filepath),
                'success': False
            })
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 BATCH TEST SUMMARY")
    print("-" * 30)
    
    successful = sum(1 for r in results if r['success'])
    total = len(results)
    
    for result in results:
        status = "✅ PASS" if result['success'] else "❌ FAIL"
        print(f"  {result['file']:<30} {status}")
    
    print(f"\n🎯 Overall: {successful}/{total} files processed successfully")
    
    return successful == total

if __name__ == "__main__":
    # Test registry status first
    test_registry_status()
    
    # Test vendor detection
    test_vendor_detection()
    
    print("\n" + "🚀 Starting Invoice Test")
    print("=" * 50)
    
    # Test single invoice - will auto-detect vendor and route to appropriate parsers
    #test_file = "invoices/1751423522.equinix.pdf"  # Change this to your test file
    #test_file = "invoices/1751514405.equinix.singapore.pdf"
    #test_file = "invoices/1751465799.equinix.japan.pdf"
    #test_file = "invoices/1751534361.equinix.australia.pdf"    
    #test_file = "invoices/1751491848.equinix.pdf"  #middle east
    #test_file = "invoices/1751451386.equinix.pdf"  #equinix to globecom
    #test_file = "invoices/1752161536.level3.pdf"  #lumen
    #test_file = "invoices/1751980594.level3.pdf"  #lumen  multiple sections
    #test_file = "invoices/1752029216.centurylink.smb.pdf" #lumen netherlands looks ok
    #test_file = "invoices/1749458730.centurylink.smb.pdf" #lumen netherlands doubled amount
    #test_file = "invoices/1751879930.level3.pdf"  #lumen  multiple sections
    #test_file = "invoices/1728531828.interxion.pdf"   # digital realty - interxion
    test_file = "invoices/1751574806.digitalrealty.pdf" # digital realty - usa
    
    
    
    if os.path.exists(test_file):
        test_single_invoice_detailed(test_file)
    else:
        print(f"❌ Test file not found: {test_file}")
        print("\n📁 Available test files:")
        if os.path.exists("invoices"):
            for file in os.listdir("invoices"):
                if file.endswith(".pdf"):
                    print(f"  - {file}")
        else:
            print("  - No invoices folder found")
    
    # Test multiple invoices if available
    print("\n" + "🚀 Starting Batch Test")
    print("=" * 50)
    test_multiple_invoices()