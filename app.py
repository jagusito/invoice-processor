# Complete app.py with Snowflake logging integration
from flask import Flask, render_template, request, jsonify, send_from_directory
import json
import os
import uuid
from datetime import datetime, timedelta
from batch_processor import BatchProcessor
from enhanced_invoice_validator import validate_invoices_endpoint
from catalog.catalog_api import catalog_bp
from config.snowflake_config import get_snowflake_session

app = Flask(__name__)
app.register_blueprint(catalog_bp, url_prefix='/catalog')

# Initialize processor
processor = BatchProcessor()

# Serve your existing catalog manager
@app.route('/catalog-manager')
def catalog_manager():
    """Serve the existing catalog management interface"""
    return send_from_directory('catalog', 'catalog_manager.html')

# Serve validation dashboard
@app.route('/validation-dashboard')
def validation_dashboard():
    """Serve the validation dashboard interface"""
    return send_from_directory('.', 'validation_dashboard.html')

###############  Validation #########################

# FIXED Flask endpoint - handles both JSON and non-JSON requests
@app.route('/api/validate-invoices', methods=['POST'])
def api_validate_invoices():
    """Enhanced 3-step validation endpoint - FIXED VERSION"""
    try:
        # Handle both JSON and non-JSON requests
        folder_path = 'invoices'  # Default folder
        
        # Try to get folder path from JSON, but don't require it
        try:
            if request.is_json and request.json and request.json.get('folder_path'):
                folder_path = request.json.get('folder_path')
        except:
            # If JSON parsing fails, just use default
            pass
        
        # Run the enhanced validation
        results = validate_invoices_endpoint(folder_path)
        return jsonify(results)
        
    except Exception as e:
        app.logger.error(f"❌ Validation endpoint error: {e}")
        return jsonify({
            "error": str(e),
            "total_files": 0,
            "ready_for_processing": 0,
            "requires_attention": 0,
            "failed_identification": 0,
            "unknown_parser": 0,
            "details": []
        }), 500

# Alternative: Make it a GET request instead (even simpler)
@app.route('/api/validate-invoices-get', methods=['GET'])
def api_validate_invoices_get():
    """Enhanced 3-step validation endpoint - GET version"""
    try:
        folder_path = request.args.get('folder_path', 'invoices')
        results = validate_invoices_endpoint(folder_path)
        return jsonify(results)
        
    except Exception as e:
        app.logger.error(f"❌ Validation endpoint error: {e}")
        return jsonify({
            "error": str(e),
            "total_files": 0,
            "ready_for_processing": 0,
            "requires_attention": 0,
            "failed_identification": 0,
            "unknown_parser": 0,
            "details": []
        }), 500

######################################

# REPLACE your existing dashboard route with this enhanced version:
@app.route('/')
def dashboard():
    """Main processing dashboard with validation link"""
    stats = get_processing_stats()
    recent_jobs = get_recent_jobs()
    vendor_stats = get_vendor_performance()
    
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Invoice Processing Dashboard</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; background: #f8fafc; }}
            .container {{ max-width: 1200px; margin: 0 auto; }}
            .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                      color: white; padding: 2rem; margin-bottom: 2rem; border-radius: 12px; text-align: center; }}
            .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
                     gap: 20px; margin-bottom: 30px; }}
            .stat-card {{ background: white; padding: 20px; border-radius: 8px; text-align: center; 
                         box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .stat-number {{ font-size: 2em; font-weight: bold; color: #333; }}
            .stat-label {{ color: #666; margin-top: 5px; }}
            .process-btn {{ background: #007bff; color: white; padding: 12px 24px; border: none; 
                           border-radius: 6px; cursor: pointer; text-decoration: none; 
                           display: inline-block; margin: 5px; }}
            .process-btn:hover {{ background: #0056b3; }}
            .validate-btn {{ background: #28a745; color: white; padding: 12px 24px; border: none; 
                           border-radius: 6px; cursor: pointer; text-decoration: none; 
                           display: inline-block; margin: 5px; }}
            .validate-btn:hover {{ background: #218838; }}
            .recent-jobs {{ margin-top: 30px; background: white; padding: 20px; border-radius: 8px; 
                           box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
            th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #f8f9fa; font-weight: 600; }}
            .success {{ color: #28a745; font-weight: bold; }}
            .failed {{ color: #dc3545; font-weight: bold; }}
            .skipped {{ color: #ffc107; font-weight: bold; }}
            tr:hover {{ background: #f8f9fa; }}
            .nav-buttons {{ margin-bottom: 20px; }}
            .validation-notice {{ background: #e7f3ff; border: 1px solid #b3d9ff; border-radius: 6px; 
                                  padding: 15px; margin-bottom: 20px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Invoice Processing Dashboard</h1>
                <p>Monitor and manage invoice processing operations</p>
            </div>
            
            <div class="validation-notice">
                <strong>🔍 Pre-Processing Validation:</strong> Always validate invoices before processing to ensure proper entity/vendor identification.
                <a href="/validation-dashboard" class="validate-btn" style="margin-left: 10px;">📋 Validate Invoices</a>
            </div>
            
            <div class="stats">
                <div class="stat-card">
                    <div class="stat-number">{stats['total_processed_today']}</div>
                    <div class="stat-label">Processed Today</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">{stats['successful_today']}</div>
                    <div class="stat-label">Successful</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">{stats['failed_today']}</div>
                    <div class="stat-label">Failed</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">{stats['vendors_configured']}</div>
                    <div class="stat-label">Vendors Configured</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">{stats['average_processing_time']}</div>
                    <div class="stat-label">Avg Processing Time</div>
                </div>
            </div>
            
            <div class="nav-buttons">
                <button class="process-btn" onclick="processFolder()">Process Invoice Folder</button>
                <a href="/catalog-manager" class="process-btn">Manage Catalogs</a>
                <a href="/vendor-performance" class="process-btn">Vendor Performance</a>
                <a href="/processing-logs" class="process-btn">View All Logs</a>
                <a href="/invoices" class="process-btn">📁 View Invoice Files</a>
            </div>
            
            <div class="recent-jobs">
                <h2>Recent Processing Jobs</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Filename</th>
                            <th>Vendor</th>
                            <th>Status</th>
                            <th>Records</th>
                            <th>Time</th>
                            <th>Entity ID</th>
                            <th>Vendor Code</th>
                            <th>Amount</th>
                            <th>Processed At</th>
                        </tr>
                    </thead>
                    <tbody>
                        {"".join([f'''
                        <tr>
                            <td>{job["filename"]}</td>
                            <td>{job["vendor"]}</td>
                            <td class="{job["status"].lower()}">{job["status"]}</td>
                            <td>{job["records_processed"]}</td>
                            <td>{job["processing_time"]:.2f}s</td>
                            <td>{job.get("entity_id", "") or "—"}</td>
                            <td>{job.get("vendor_code", "") or "—"}</td>
                            <td>{job.get("currency", "")} {job.get("invoice_total", 0):,.2f}</td>
                            <td>{job["created_at"]}</td>
                        </tr>
                        ''' for job in recent_jobs[:10]])}
                    </tbody>
                </table>
            </div>
        </div>
        
        <script>
            function processFolder() {{
                if(confirm('Process all invoices in the invoices folder?')) {{
                    fetch('/api/process-folder', {{
                        method: 'POST',
                        headers: {{'Content-Type': 'application/json'}},
                        body: JSON.stringify({{}})
                    }})
                    .then(response => response.json())
                    .then(data => {{
                        alert(`Processing completed: ${{data.successful}} successful, ${{data.failed}} failed`);
                        location.reload();
                    }})
                    .catch(error => {{
                        alert('Error: ' + error);
                    }});
                }}
            }}
        </script>
    </body>
    </html>
    """
    
################
#  List invoices
################    
# Add these routes to your app.py

@app.route('/invoices')
def list_invoices():
    """Beautifully styled invoice file listing"""
    try:
        invoice_folder = 'invoices'
        if not os.path.exists(invoice_folder):
            return f"<h1>Invoice folder not found: {invoice_folder}</h1>"
        
        pdf_files = []
        for file in os.listdir(invoice_folder):
            if file.lower().endswith('.pdf'):
                file_path = os.path.join(invoice_folder, file)
                file_stat = os.stat(file_path)
                file_size = file_stat.st_size
                modified_time = datetime.fromtimestamp(file_stat.st_mtime)
                
                pdf_files.append({
                    'name': file,
                    'size': file_size,
                    'size_mb': file_size / (1024 * 1024),
                    'modified': modified_time.strftime('%Y-%m-%d %H:%M'),
                    'vendor': detect_vendor_from_filename(file)
                })
        
        # Sort by modified date (newest first)
        pdf_files.sort(key=lambda x: x['modified'], reverse=True)
        
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Invoice Files</title>
            <style>
                body {{ 
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
                    margin: 0; 
                    padding: 20px; 
                    background: #f8fafc; 
                    color: #334155;
                }}
                .container {{ max-width: 1200px; margin: 0 auto; }}
                .header {{ 
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                    color: white; 
                    padding: 2rem; 
                    margin-bottom: 2rem; 
                    border-radius: 12px; 
                    text-align: center;
                    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                }}
                .header h1 {{ margin: 0; font-size: 2.5rem; font-weight: 700; }}
                .header p {{ margin: 0.5rem 0 0 0; opacity: 0.9; font-size: 1.1rem; }}
                
                .controls {{ 
                    background: white; 
                    padding: 20px; 
                    border-radius: 12px; 
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1); 
                    margin-bottom: 20px;
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    flex-wrap: wrap;
                    gap: 15px;
                }}
                
                .search-box {{ 
                    flex: 1; 
                    min-width: 300px;
                    position: relative;
                }}
                .search-box input {{ 
                    width: 100%; 
                    padding: 12px 45px 12px 15px; 
                    border: 2px solid #e2e8f0; 
                    border-radius: 8px; 
                    font-size: 1rem;
                    transition: border-color 0.3s ease;
                }}
                .search-box input:focus {{ 
                    outline: none; 
                    border-color: #667eea; 
                    box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
                }}
                .search-icon {{ 
                    position: absolute; 
                    right: 15px; 
                    top: 50%; 
                    transform: translateY(-50%); 
                    color: #64748b;
                }}
                
                .filter-buttons {{ display: flex; gap: 10px; flex-wrap: wrap; }}
                .filter-btn {{ 
                    padding: 8px 16px; 
                    border: 2px solid #e2e8f0; 
                    background: white; 
                    border-radius: 6px; 
                    cursor: pointer; 
                    transition: all 0.3s ease;
                    font-size: 0.9rem;
                    font-weight: 500;
                }}
                .filter-btn:hover {{ background: #f1f5f9; border-color: #cbd5e1; }}
                .filter-btn.active {{ background: #667eea; color: white; border-color: #667eea; }}
                
                .back-btn {{ 
                    background: #6c757d; 
                    color: white; 
                    padding: 12px 24px; 
                    text-decoration: none; 
                    border-radius: 8px; 
                    font-weight: 500;
                    transition: background 0.3s ease;
                }}
                .back-btn:hover {{ background: #5a6268; }}
                
                .stats {{ 
                    display: grid; 
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
                    gap: 15px; 
                    margin-bottom: 20px;
                }}
                .stat-card {{ 
                    background: white; 
                    padding: 20px; 
                    border-radius: 8px; 
                    text-align: center; 
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                .stat-number {{ font-size: 1.8rem; font-weight: bold; color: #1e293b; }}
                .stat-label {{ color: #64748b; margin-top: 5px; font-size: 0.9rem; }}
                
                .files-container {{ 
                    background: white; 
                    border-radius: 12px; 
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1); 
                    overflow: hidden;
                }}
                
                .files-header {{ 
                    background: #f8fafc; 
                    padding: 20px; 
                    border-bottom: 1px solid #e2e8f0;
                    display: grid;
                    grid-template-columns: 2fr 120px 100px 140px 80px;
                    gap: 15px;
                    font-weight: 600;
                    color: #374151;
                    font-size: 0.9rem;
                    text-transform: uppercase;
                    letter-spacing: 0.05em;
                }}
                
                .file-row {{ 
                    display: grid;
                    grid-template-columns: 2fr 120px 100px 140px 80px;
                    gap: 15px;
                    padding: 20px;
                    border-bottom: 1px solid #f1f5f9;
                    align-items: center;
                    transition: background 0.2s ease;
                    position: relative;
                }}
                .file-row:hover {{ background: #f8fafc; }}
                .file-row:last-child {{ border-bottom: none; }}
                
                .file-name {{ 
                    display: flex; 
                    align-items: center; 
                    gap: 12px;
                }}
                .file-icon {{ 
                    width: 40px; 
                    height: 40px; 
                    background: #ef4444; 
                    border-radius: 8px; 
                    display: flex; 
                    align-items: center; 
                    justify-content: center; 
                    color: white; 
                    font-weight: bold; 
                    font-size: 0.8rem;
                    flex-shrink: 0;
                }}
                .file-details {{ flex: 1; min-width: 0; }}
                .file-title {{ 
                    font-weight: 600; 
                    color: #1e293b; 
                    margin-bottom: 4px;
                    word-break: break-all;
                }}
                .file-title a {{ 
                    color: #667eea; 
                    text-decoration: none; 
                    transition: color 0.3s ease;
                }}
                .file-title a:hover {{ color: #4f46e5; text-decoration: underline; }}
                
                .vendor-badge {{ 
                    display: inline-block; 
                    padding: 4px 8px; 
                    border-radius: 4px; 
                    font-size: 0.75rem; 
                    font-weight: 500; 
                    text-transform: uppercase; 
                    letter-spacing: 0.05em;
                }}
                .vendor-equinix {{ background: #dbeafe; color: #1e40af; }}
                .vendor-lumen {{ background: #dcfce7; color: #166534; }}
                .vendor-vodafone {{ background: #fef3c7; color: #92400e; }}
                .vendor-digital {{ background: #e0e7ff; color: #3730a3; }}
                .vendor-unknown {{ background: #f1f5f9; color: #64748b; }}
                
                .file-size {{ color: #64748b; font-size: 0.9rem; text-align: right; }}
                .file-date {{ color: #64748b; font-size: 0.9rem; }}
                
                .download-btn {{ 
                    background: #10b981; 
                    color: white; 
                    padding: 8px 12px; 
                    border-radius: 6px; 
                    text-decoration: none; 
                    font-size: 0.8rem; 
                    font-weight: 500;
                    transition: background 0.3s ease;
                    text-align: center;
                }}
                .download-btn:hover {{ background: #059669; }}
                
                .no-files {{ 
                    text-align: center; 
                    padding: 60px 20px; 
                    color: #64748b;
                }}
                .no-files-icon {{ font-size: 3rem; margin-bottom: 1rem; }}
                
                @media (max-width: 768px) {{
                    .files-header, .file-row {{ 
                        grid-template-columns: 1fr; 
                        gap: 10px;
                    }}
                    .file-name {{ justify-content: space-between; }}
                    .controls {{ flex-direction: column; align-items: stretch; }}
                    .search-box {{ min-width: auto; }}
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>📁 Invoice Files</h1>
                    <p>Browse and download invoice PDFs</p>
                </div>
                
                <div class="controls">
                    <div class="search-box">
                        <input type="text" id="searchInput" placeholder="Search invoices by filename or vendor..." onkeyup="filterFiles()">
                        <span class="search-icon">🔍</span>
                    </div>
                    <div class="filter-buttons">
                        <button class="filter-btn active" onclick="filterByVendor('all')">All</button>
                        <button class="filter-btn" onclick="filterByVendor('equinix')">Equinix</button>
                        <button class="filter-btn" onclick="filterByVendor('lumen')">Lumen</button>
                        <button class="filter-btn" onclick="filterByVendor('vodafone')">Vodafone</button>
                        <button class="filter-btn" onclick="filterByVendor('digital')">Digital Realty</button>
                    </div>
                    <a href="/" class="back-btn">← Dashboard</a>
                </div>
                
                <div class="stats">
                    <div class="stat-card">
                        <div class="stat-number">{len(pdf_files)}</div>
                        <div class="stat-label">Total Files</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number">{sum(f['size'] for f in pdf_files) / (1024*1024*1024):.1f} GB</div>
                        <div class="stat-label">Total Size</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number">{len(set(f['vendor'] for f in pdf_files))}</div>
                        <div class="stat-label">Vendors</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-number">{pdf_files[0]['modified'] if pdf_files else 'N/A'}</div>
                        <div class="stat-label">Latest File</div>
                    </div>
                </div>
                
                <div class="files-container">
                    <div class="files-header">
                        <div>Filename</div>
                        <div>Vendor</div>
                        <div>Size</div>
                        <div>Modified</div>
                        <div>Action</div>
                    </div>
                    
                    {"".join([f'''
                    <div class="file-row" data-vendor="{file['vendor']}">
                        <div class="file-name">
                            <div class="file-icon">PDF</div>
                            <div class="file-details">
                                <div class="file-title">
                                    <a href="/invoices/{file['name']}" target="_blank">{file['name']}</a>
                                </div>
                            </div>
                        </div>
                        <div>
                            <span class="vendor-badge vendor-{file['vendor']}">{file['vendor'].title()}</span>
                        </div>
                        <div class="file-size">{file['size_mb']:.1f} MB</div>
                        <div class="file-date">{file['modified']}</div>
                        <div>
                            <a href="/invoices/{file['name']}" class="download-btn" download>Download</a>
                        </div>
                    </div>
                    ''' for file in pdf_files]) if pdf_files else '''
                    <div class="no-files">
                        <div class="no-files-icon">📄</div>
                        <h3>No invoice files found</h3>
                        <p>Upload some PDF invoices to get started</p>
                    </div>
                    '''}
                </div>
            </div>
            
            <script>
                function filterFiles() {{
                    const searchTerm = document.getElementById('searchInput').value.toLowerCase();
                    const rows = document.querySelectorAll('.file-row');
                    
                    rows.forEach(row => {{
                        const filename = row.querySelector('.file-title a').textContent.toLowerCase();
                        const vendor = row.dataset.vendor.toLowerCase();
                        const visible = filename.includes(searchTerm) || vendor.includes(searchTerm);
                        row.style.display = visible ? 'grid' : 'none';
                    }});
                }}
                
                function filterByVendor(vendor) {{
                    // Update active button
                    document.querySelectorAll('.filter-btn').forEach(btn => btn.classList.remove('active'));
                    event.target.classList.add('active');
                    
                    const rows = document.querySelectorAll('.file-row');
                    rows.forEach(row => {{
                        const rowVendor = row.dataset.vendor;
                        const visible = vendor === 'all' || rowVendor === vendor;
                        row.style.display = visible ? 'grid' : 'none';
                    }});
                    
                    // Clear search when filtering
                    document.getElementById('searchInput').value = '';
                }}
                
                // Auto-refresh every 30 seconds
                setInterval(() => {{
                    location.reload();
                }}, 30000);
            </script>
        </body>
        </html>
        """
        
    except Exception as e:
        return f"<h1>Error: {e}</h1>"

@app.route('/invoices/<filename>')
def serve_invoice(filename):
    """Serve individual PDF files"""
    try:
        # Security: Only allow PDF files
        if not filename.lower().endswith('.pdf'):
            return "Only PDF files allowed", 403
        
        return send_from_directory('invoices', filename)
    except Exception as e:
        return f"File not found: {filename}", 404

def detect_vendor_from_filename(filename):
    """Detect vendor from filename patterns"""
    filename_lower = filename.lower()
    
    if 'equinix' in filename_lower:
        return 'equinix'
    elif 'lumen' in filename_lower or 'level3' in filename_lower or 'centurylink' in filename_lower:
        return 'lumen'
    elif 'vodafone' in filename_lower:
        return 'vodafone'
    elif 'digital' in filename_lower or 'interxion' in filename_lower:
        return 'digital'
    else:
        return 'unknown'
        
################
#
################

@app.route('/vendor-performance')
def vendor_performance_page():
    """Vendor performance page"""
    vendors = get_vendor_performance()
    
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Vendor Performance</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; background: #f8fafc; }}
            .container {{ max-width: 1200px; margin: 0 auto; }}
            .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                      color: white; padding: 2rem; margin-bottom: 2rem; border-radius: 12px; text-align: center; }}
            .vendor-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }}
            .vendor-card {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .vendor-name {{ font-size: 1.2em; font-weight: bold; margin-bottom: 10px; }}
            .metric {{ display: flex; justify-content: space-between; margin: 5px 0; }}
            .success-rate {{ font-weight: bold; color: #28a745; }}
            .back-btn {{ background: #6c757d; color: white; padding: 12px 24px; border: none; 
                        border-radius: 6px; cursor: pointer; text-decoration: none; margin-bottom: 20px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Vendor Performance</h1>
                <p>Success rates and processing statistics by vendor</p>
            </div>
            
            <a href="/" class="back-btn">← Back to Dashboard</a>
            
            <div class="vendor-grid">
                {"".join([f'''
                <div class="vendor-card">
                    <div class="vendor-name">{vendor["name"]}</div>
                    <div class="metric">
                        <span>Success Rate:</span>
                        <span class="success-rate">{vendor["success_rate"]:.1%}</span>
                    </div>
                    <div class="metric">
                        <span>Total Attempts:</span>
                        <span>{vendor["total_attempts"]}</span>
                    </div>
                    <div class="metric">
                        <span>Successful:</span>
                        <span>{vendor["successful"]}</span>
                    </div>
                    <div class="metric">
                        <span>Avg Time:</span>
                        <span>{vendor["avg_processing_time"]:.2f}s</span>
                    </div>
                    <div class="metric">
                        <span>Last Processed:</span>
                        <span>{vendor["last_processed"] or "Never"}</span>
                    </div>
                </div>
                ''' for vendor in vendors])}
            </div>
        </div>
    </body>
    </html>
    """

@app.route('/processing-logs')
def processing_logs_page():
    """Processing logs page"""
    logs = get_processing_logs(50)
    
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Processing Logs</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; background: #f8fafc; }}
            .container {{ max-width: 1400px; margin: 0 auto; }}
            .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                      color: white; padding: 2rem; margin-bottom: 2rem; border-radius: 12px; text-align: center; }}
            .logs-table {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            table {{ width: 100%; border-collapse: collapse; }}
            th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #f8f9fa; font-weight: 600; }}
            .success {{ color: #28a745; font-weight: bold; }}
            .failed {{ color: #dc3545; font-weight: bold; }}
            .error {{ color: #dc3545; font-weight: bold; }}
            .skipped {{ color: #ffc107; font-weight: bold; }}
            tr:hover {{ background: #f8f9fa; }}
            .back-btn {{ background: #6c757d; color: white; padding: 12px 24px; border: none; 
                        border-radius: 6px; cursor: pointer; text-decoration: none; margin-bottom: 20px; }}
            .error-message {{ max-width: 200px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Processing Logs</h1>
                <p>Detailed processing history and error messages from Snowflake</p>
            </div>
            
            <a href="/" class="back-btn">← Back to Dashboard</a>
            
            <div class="logs-table">
                <table>
                    <thead>
                        <tr>
                            <th>Filename</th>
                            <th>Vendor</th>
                            <th>Status</th>
                            <th>Records</th>
                            <th>Time</th>
                            <th>Entity ID</th>
                            <th>Vendor Code</th>
                            <th>Amount</th>
                            <th>Error Message</th>
                            <th>Processed At</th>
                        </tr>
                    </thead>
                    <tbody>
                        {"".join([f'''
                        <tr>
                            <td>{log["filename"]}</td>
                            <td>{log["vendor"]}</td>
                            <td class="{log["status"].lower()}">{log["status"]}</td>
                            <td>{log["records_processed"]}</td>
                            <td>{log["processing_time"]:.2f}s</td>
                            <td>{log.get("entity_id", "") or "—"}</td>
                            <td>{log.get("vendor_code", "") or "—"}</td>
                            <td>{log.get("currency", "")} {log.get("invoice_total", 0):,.2f}</td>
                            <td class="error-message" title="{log["error_message"] or ""}">{log["error_message"] or "—"}</td>
                            <td>{log["created_at"]}</td>
                        </tr>
                        ''' for log in logs])}
                    </tbody>
                </table>
            </div>
        </div>
    </body>
    </html>
    """

# API Endpoints
@app.route('/api/process-folder', methods=['POST'])
def process_folder_api():
    """API endpoint to trigger folder processing"""
    try:
        # Get folder path from request, default to 'invoices'
        data = request.get_json() if request.is_json else {}
        folder_path = data.get('folder_path', 'invoices')
        
        # Process the folder
        results = processor.process_folder(folder_path)
        
        # Return JSON response
        return jsonify(results)
        
    except Exception as e:
        print(f"Error in process_folder_api: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/process-single', methods=['POST'])
def process_single():
    """API endpoint to process single file"""
    try:
        file_path = request.json.get('file_path')
        if not file_path or not os.path.exists(file_path):
            return jsonify({"error": "File not found"}), 400
        
        success = processor.process_single_invoice(file_path)
        return jsonify({"success": success})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/processing-stats')
def processing_stats():
    """Get overall processing statistics"""
    return jsonify(get_processing_stats())

@app.route('/api/recent-jobs')
def recent_jobs():
    """Get recent processing jobs"""
    return jsonify(get_recent_jobs())

@app.route('/api/vendor-performance')
def vendor_performance_api():
    """Get vendor processing performance stats"""
    return jsonify(get_vendor_performance())

# =====================================================
# SNOWFLAKE PROCESSING LOGS FUNCTIONS
# =====================================================

def init_snowflake_processing_logs():
    """Initialize Snowflake processing logs table"""
    try:
        session = get_snowflake_session()
        
        # Create PROCESSING_LOGS table in Snowflake
        session.sql("""
            CREATE TABLE IF NOT EXISTS PROCESSING_LOGS (
                LOG_ID VARCHAR(50) PRIMARY KEY,
                FILENAME VARCHAR(255) NOT NULL,
                VENDOR VARCHAR(50),
                STATUS VARCHAR(20),
                ERROR_MESSAGE TEXT,
                RECORDS_PROCESSED INTEGER DEFAULT 0,
                PROCESSING_TIME_SECONDS FLOAT DEFAULT 0.0,
                ENTITY_ID VARCHAR(50),
                VENDOR_CODE VARCHAR(50),
                INVOICE_TOTAL FLOAT,
                CURRENCY VARCHAR(3),
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """).collect()
        
        session.close()
        print("✅ Snowflake PROCESSING_LOGS table initialized successfully")
        
    except Exception as e:
        print(f"❌ Error initializing Snowflake processing logs table: {e}")

def log_processing_result_to_snowflake(filename, vendor, status, **kwargs):
    """
    Log processing result to Snowflake PROCESSING_LOGS table
    
    Args:
        filename: Invoice filename
        vendor: Vendor provider (equinix, vodafone, etc.)
        status: Processing status (SUCCESS, FAILED, SKIPPED)
        **kwargs: Additional fields (error_message, records_processed, processing_time_seconds, etc.)
    """
    try:
        session = get_snowflake_session()
        
        # Generate unique log ID
        log_id = f"LOG_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
        
        # Extract optional fields with defaults
        error_message = kwargs.get('error_message', '')
        records_processed = kwargs.get('records_processed', 0)
        processing_time = kwargs.get('processing_time_seconds', 0.0)
        entity_id = kwargs.get('entity_id', '')
        vendor_code = kwargs.get('vendor_code', '')
        invoice_total = kwargs.get('invoice_total', 0.0)
        currency = kwargs.get('currency', '')
        
        # Clean and escape strings for SQL
        def clean_sql_string(value):
            if value is None:
                return ''
            return str(value).replace("'", "''")
        
        # Insert log record
        query = f"""
            INSERT INTO PROCESSING_LOGS (
                LOG_ID, FILENAME, VENDOR, STATUS, ERROR_MESSAGE,
                RECORDS_PROCESSED, PROCESSING_TIME_SECONDS, ENTITY_ID,
                VENDOR_CODE, INVOICE_TOTAL, CURRENCY
            ) VALUES (
                '{log_id}',
                '{clean_sql_string(filename)}',
                '{clean_sql_string(vendor)}',
                '{clean_sql_string(status)}',
                '{clean_sql_string(error_message)}',
                {records_processed},
                {processing_time},
                '{clean_sql_string(entity_id)}',
                '{clean_sql_string(vendor_code)}',
                {invoice_total if invoice_total else 0.0},
                '{clean_sql_string(currency)}'
            )
        """
        
        session.sql(query).collect()
        session.close()
        
        print(f"✅ Logged to Snowflake: {filename} - {status}")
        
    except Exception as e:
        print(f"❌ Error logging to Snowflake: {e}")
        # Fallback logging to console so we don't lose the information
        print(f"📝 FALLBACK LOG: {filename} | {vendor} | {status} | {kwargs}")

def get_processing_stats():
    """Get processing statistics from Snowflake PROCESSING_LOGS"""
    try:
        session = get_snowflake_session()
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Get today's stats
        query = f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN STATUS = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
                SUM(CASE WHEN STATUS = 'FAILED' THEN 1 ELSE 0 END) as failed,
                AVG(PROCESSING_TIME_SECONDS) as avg_time
            FROM PROCESSING_LOGS 
            WHERE DATE(CREATED_AT) = '{today}'
        """
        
        result = session.sql(query).collect()
        session.close()
        
        if result and len(result) > 0:
            row = result[0]
            return {
                "total_processed_today": row[0] or 0,
                "successful_today": row[1] or 0,
                "failed_today": row[2] or 0,
                "vendors_configured": len(get_configured_vendors()),
                "average_processing_time": f"{row[3]:.1f}s" if row[3] else "0.0s"
            }
        else:
            return {
                "total_processed_today": 0,
                "successful_today": 0,
                "failed_today": 0,
                "vendors_configured": len(get_configured_vendors()),
                "average_processing_time": "0.0s"
            }
            
    except Exception as e:
        print(f"Error getting stats from Snowflake: {e}")
        return {
            "total_processed_today": 0,
            "successful_today": 0,
            "failed_today": 0,
            "vendors_configured": len(get_configured_vendors()),
            "average_processing_time": "0.0s"
        }

def get_recent_jobs(limit=10):
    """Get recent processing jobs from Snowflake PROCESSING_LOGS"""
    try:
        session = get_snowflake_session()
        
        query = f"""
            SELECT 
                FILENAME, VENDOR, STATUS, ERROR_MESSAGE, RECORDS_PROCESSED, 
                PROCESSING_TIME_SECONDS, ENTITY_ID, VENDOR_CODE, INVOICE_TOTAL,
                CURRENCY, CREATED_AT, INVOICE_ID
            FROM PROCESSING_LOGS 
            ORDER BY CREATED_AT DESC 
            LIMIT {limit}
        """
        
        result = session.sql(query).collect()
        session.close()
        
        jobs = []
        for row in result:
            jobs.append({
                "filename": row[0],
                "vendor": row[1],  # Still use 'provider' key for compatibility with template
                "status": row[2],
                "error_message": row[3],
                "records_processed": row[4] or 0,
                "processing_time": row[5] or 0.0,
                "entity_id": row[6],
                "vendor_code": row[7],
                "invoice_total": row[8] or 0.0,
                "currency": row[9],
                "created_at": str(row[10]) if row[10] else "",
                "invoice_id": row[11]
            })
        
        return jobs
        
    except Exception as e:
        print(f"Error getting recent jobs from Snowflake: {e}")
        return []

def get_vendor_performance():
    """Get vendor performance statistics from Snowflake PROCESSING_LOGS"""
    try:
        session = get_snowflake_session()
        
        # Get performance stats for last 30 days
        query = """
            SELECT 
                VENDOR,
                COUNT(*) as total_attempts,
                SUM(CASE WHEN STATUS = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
                AVG(PROCESSING_TIME_SECONDS) as avg_time,
                MAX(CREATED_AT) as last_processed
            FROM PROCESSING_LOGS 
            WHERE CREATED_AT >= DATEADD(day, -30, CURRENT_DATE())
            AND VENDOR IS NOT NULL
            GROUP BY VENDOR
            ORDER BY VENDOR
        """
        
        result = session.sql(query).collect()
        session.close()
        
        vendors = []
        for row in result:
            success_rate = (row[2] / row[1]) if row[1] > 0 else 0
            vendors.append({
                "name": row[0].title() if row[0] else "Unknown",
                "total_attempts": row[1] or 0,
                "successful": row[2] or 0,
                "success_rate": success_rate,
                "avg_processing_time": round(row[3], 2) if row[3] else 0.0,
                "last_processed": str(row[4]) if row[4] else None
            })
        
        return vendors
        
    except Exception as e:
        print(f"Error getting vendor performance from Snowflake: {e}")
        return []

def get_processing_logs(limit=50):
    """Get detailed processing logs from Snowflake PROCESSING_LOGS"""
    try:
        session = get_snowflake_session()
        
        query = f"""
            SELECT 
                FILENAME, VENDOR, STATUS, ERROR_MESSAGE, RECORDS_PROCESSED,
                PROCESSING_TIME_SECONDS, ENTITY_ID, VENDOR_CODE, INVOICE_TOTAL,
                CURRENCY, CREATED_AT
            FROM PROCESSING_LOGS 
            ORDER BY CREATED_AT DESC 
            LIMIT {limit}
        """
        
        result = session.sql(query).collect()
        session.close()
        
        logs = []
        for row in result:
            logs.append({
                "filename": row[0],
                "vendor": row[1],
                "status": row[2],
                "error_message": row[3],
                "records_processed": row[4] or 0,
                "processing_time": row[5] or 0.0,
                "entity_id": row[6],
                "vendor_code": row[7],
                "invoice_total": row[8] or 0.0,
                "currency": row[9],
                "created_at": str(row[10]) if row[10] else ""
            })
        
        return logs
        
    except Exception as e:
        print(f"Error getting processing logs from Snowflake: {e}")
        return []

def get_configured_vendors():
    """Get list of configured vendors from catalog (FIXED VERSION)"""
    try:
        from catalog.catalog_api import get_vendors_from_snowflake
        vendors = get_vendors_from_snowflake()
        # Filter only active vendors and return just the names
        active_vendors = [vendor['name'] for vendor in vendors if vendor.get('status') == 'Active']
        return active_vendors
    except Exception as e:
        print(f"Error getting configured vendors: {e}")
        return []

if __name__ == '__main__':
    # Initialize Snowflake processing logs table on startup
    init_snowflake_processing_logs()
    
    # Initialize Snowflake catalog tables
    from catalog.catalog_api import init_snowflake_tables
    init_snowflake_tables()
    
    print("🚀 Starting Invoice Processing System...")
    print("📊 Dashboard: http://localhost:5000")
    print("📋 Catalog Manager: http://localhost:5000/catalog-manager")
    print("📋 Validation Dashboard: http://localhost:5000/validation-dashboard")
    print("📈 Vendor Performance: http://localhost:5000/vendor-performance")
    print("💾 Using Snowflake for all data storage")
    
    app.run(debug=True, host='0.0.0.0', port=5000)