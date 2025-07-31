import pdfplumber
import pandas as pd
import numpy as np
from datetime import datetime
import re

def extract_equinix_eur(pdf_path):
    """
    Extract data from Equinix EUR format invoices using direct column access
    """
    # Extract header info
    with pdfplumber.open(pdf_path) as pdf:
        first_page = pdf.pages[0]
        text = first_page.extract_text()
        
        # Extract header information with regex
        invoice_match = re.search(r"Invoice\s+#\s+(\d+)", text)
        invoice_id = invoice_match.group(1) if invoice_match else ''
            
        date_match = re.search(r"Invoice\s+Date\s+(\d{2}-[A-Za-z]{3}-\d{2})", text)
        billing_period = date_match.group(1) if date_match else ''
        
        # Format the billing period
        if billing_period:
            try:
                billing_period = billing_period.replace("–", "-").replace("‑", "-").strip()
                billing_period = datetime.strptime(billing_period, "%d-%b-%y").strftime("%Y-%m-%d")
            except:
                pass
            
        ban_match = re.search(r"Customer\s+Account\s+#\s+(\d+)", text)
        ban = ban_match.group(1) if ban_match else ''
    
    header_info = {
        'invoice_id': invoice_id.strip(),
        'billing_period': billing_period,
        'ban': ban.strip()
    }
    
    print(f"Extracted header info: {header_info}")
    
    # Process tables
    all_transactions = []
    
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            print(f"Processing Page {page_num}...")
            
            # Extract tables from the page
            tables = page.extract_tables()
            
            # Process each table
            for table_num, table in enumerate(tables, start=1):
                if not table or len(table) < 3:
                    continue
                
                # Find the header row with "Line #"
                header_row = None
                for i, row in enumerate(table):
                    if row and row[0] and "Line #" in str(row[0]):
                        header_row = i
                        break
                
                if header_row is None:
                    continue
                
                # Process data rows (after header row)
                data_rows = table[header_row+1:]
                
                # Determine column indices based on table structure
                # For recurring and prior period charges
                item_number_idx = 1   # Billing Agreement
                description_idx = 4   # Product Description
                usoc_idx = 6          # Product Code
                units_idx = 7         # Qty (hardcoded based on table structure)
                amount_idx = 11       # MRC
                tax_idx = 13          # Tax
                total_idx = 14        # Total
                
                # Check if this is a one-time charges table (has more columns)
                is_one_time = False
                for row in table[:header_row+1]:
                    if any(cell and 'One Time Charges' in str(cell) for cell in row if cell):
                        is_one_time = True
                        # Adjust indices for one-time charges
                        description_idx = 5
                        usoc_idx = 8
                        units_idx = 10
                        amount_idx = 13  # NRC
                        tax_idx = 15
                        total_idx = 16
                        break
                
                # Process each data row
                for row in data_rows:
                    # Skip rows without a line number
                    if not row[0]:
                        continue
                    
                    line_value = str(row[0]).strip() 
                    if not re.match(r'^[0-9]+(\.[0-9]+)?$', line_value) or 'Subtotal' in line_value:
                        continue
                    
                    # Only include rows with both MRC/NRC and a line number
                    if amount_idx >= len(row) or not row[amount_idx]:
                        continue
                        
                    # Skip the empty row with line 2
                    if line_value == '2' and (not row[amount_idx] or str(row[amount_idx]).strip() == ''):
                        continue
                    
                    # Create transaction
                    transaction = {
                        'invoice_id': header_info['invoice_id'],
                        'billing_period': header_info['billing_period'],
                        'ban': header_info['ban'],
                        'item_number': str(row[item_number_idx]).strip().replace('\n', ' ') if item_number_idx < len(row) and row[item_number_idx] else '',
                        'description': str(row[description_idx]).strip().replace('\n', ' ') if description_idx < len(row) and row[description_idx] else '',
                        'usoc': str(row[usoc_idx]).strip() if usoc_idx < len(row) and row[usoc_idx] else '',
                        'units': str(row[units_idx]).strip() if units_idx < len(row) and row[units_idx] else '1.00',
                        'amount': str(row[amount_idx]).strip() if amount_idx < len(row) and row[amount_idx] else '',
                        'tax': str(row[tax_idx]).strip() if tax_idx < len(row) and row[tax_idx] else '',
                        'total': str(row[total_idx]).strip() if total_idx < len(row) and row[total_idx] else ''
                    }
                    
                    # Clean numeric fields
                    for field in ['amount', 'tax', 'total']:
                        if transaction[field]:
                            try:
                                clean_value = transaction[field].replace(',', '')
                                transaction[field] = float(clean_value)
                            except:
                                pass
                    
                    # Add transaction
                    all_transactions.append(transaction)
    
    # Create DataFrame
    if all_transactions:
        result_df = pd.DataFrame(all_transactions)
        print(f"Total extracted: {len(result_df)} transactions")
        return result_df
    else:
        print("No transactions extracted")
        return pd.DataFrame()
            
                    
df = new_extract_equinix("C:\\BDA\\JasonInv\\1743678991.equinix.pdf")   #USD
df = extract_equinix_eur("C:\\BDA\\JasonInv\\1738755443.equinix.pdf")   #EUR
df = new_extract_equinix("C:\\BDA\\JasonInv\\1743704855.equinix.singapore.pdf")   #SGD


df.to_csv("C:\\BDA\\JasonInv\\xtrct\\equinix_new_eur.csv", index=False)


# Print information about the dataframe
print(f"Shape of tables[0].df: {tables[0].df.shape}")

# Print the actual content
print("\nTable content:")
for i, row in tables[0].df.iterrows():
    # Create a string representation of the row
    row_str = " | ".join([f"{j}: {str(val).strip()}" for j, val in enumerate(row)])
    print(f"Row {i}: {row_str}")
    
