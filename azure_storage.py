# azure_storage.py
import os
from azure.storage.blob import BlobServiceClient
import tempfile

class AzureInvoiceStorage:
    def __init__(self):
        self.connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        self.container_name = "invoices"
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
    
    def upload_invoice(self, file_stream, filename):
        blob_client = self.blob_service_client.get_blob_client(
            container=self.container_name, 
            blob=f"uploads/{filename}"
        )
        blob_client.upload_blob(file_stream, overwrite=True)
        return f"uploads/{filename}"
    
    def download_invoice_to_temp(self, blob_name):
        blob_client = self.blob_service_client.get_blob_client(
            container=self.container_name, 
            blob=blob_name
        )
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
        with open(temp_file.name, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        return temp_file.name