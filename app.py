# Add these imports at the top of your app.py
from pre_processing_validator import PreProcessingValidator
from flask import Flask, render_template, request, jsonify, send_from_directory
import json
import os
import sqlite3
from datetime import datetime, timedelta
from batch_processor import BatchProcessor
from catalog.catalog_api import catalog_bp

app = Flask(__name__)
app.register_blueprint(catalog_bp, url_prefix='/catalog')

# Initialize processor
processor = BatchProcessor()

# Serve your existing catalog manager
@app.route('/catalog-manager')
def catalog_manager():
    """Serve the existing catalog management interface"""
    return send_from_directory('catalog', 'catalog_manager.html')


###############



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
            </div>
            
            <div class="recent-jobs">
                <h2>Recent Processing Jobs</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Filename</th>
                            <th>Provider</th>
                            <th>Status</th>
                            <th>Records</th>
                            <th>Time</th>
                            <th>Processed At</th>
                        </tr>
                    </thead>
                    <tbody>
                        {"".join([f'''
                        <tr>
                            <td>{job["filename"]}</td>
                            <td>{job["provider"]}</td>
                            <td class="{job["status"].lower()}">{job["status"]}</td>
                            <td>{job["records_processed"]}</td>
                            <td>{job["processing_time"]:.2f}s</td>
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
#################

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
            .container {{ max-width: 1200px; margin: 0 auto; }}
            .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                      color: white; padding: 2rem; margin-bottom: 2rem; border-radius: 12px; text-align: center; }}
            .logs-table {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            table {{ width: 100%; border-collapse: collapse; }}
            th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #f8f9fa; font-weight: 600; }}
            .success {{ color: #28a745; font-weight: bold; }}
            .failed {{ color: #dc3545; font-weight: bold; }}
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
                <p>Detailed processing history and error messages</p>
            </div>
            
            <a href="/" class="back-btn">← Back to Dashboard</a>
            
            <div class="logs-table">
                <table>
                    <thead>
                        <tr>
                            <th>Filename</th>
                            <th>Provider</th>
                            <th>Status</th>
                            <th>Records</th>
                            <th>Time</th>
                            <th>Error Message</th>
                            <th>Processed At</th>
                        </tr>
                    </thead>
                    <tbody>
                        {"".join([f'''
                        <tr>
                            <td>{log["filename"]}</td>
                            <td>{log["provider"]}</td>
                            <td class="{log["status"].lower()}">{log["status"]}</td>
                            <td>{log["records_processed"]}</td>
                            <td>{log["processing_time"]:.2f}s</td>
                            <td class="error-message" title="{log["error_message"] or ""}">{log["error_message"] or ""}</td>
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
    
    
# Add these routes to your app.py file (after your existing routes)

@app.route('/validation-dashboard')
def validation_dashboard():
    """Serve the validation dashboard interface"""
    return send_from_directory('.', 'validation_dashboard.html')

@app.route('/api/validate-invoices', methods=['POST'])
def validate_invoices_api():
    """API endpoint to validate all invoices before processing"""
    try:
        validator = PreProcessingValidator()
        results = validator.validate_all_invoices()
        
        return jsonify(results)
        
    except Exception as e:
        print(f"Error in validate_invoices_api: {e}")
        return jsonify({"error": str(e)}), 500
    
    

# Helper functions
def get_processing_stats():
    """Get processing statistics from SQLite logs"""
    try:
        init_logs_db()
        conn = sqlite3.connect('data/logs.db')
        cursor = conn.cursor()
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Get today's stats
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
                AVG(processing_time_seconds) as avg_time
            FROM processing_logs 
            WHERE DATE(created_at) = ?
        """, (today,))
        
        result = cursor.fetchone()
        conn.close()
        
        return {
            "total_processed_today": result[0] or 0,
            "successful_today": result[1] or 0,
            "failed_today": result[2] or 0,
            "vendors_configured": len(get_configured_vendors()),
            "average_processing_time": f"{result[3]:.1f}s" if result[3] else "0.0s"
        }
    except Exception as e:
        print(f"Error getting stats: {e}")
        return {
            "total_processed_today": 0,
            "successful_today": 0,
            "failed_today": 0,
            "vendors_configured": 6,
            "average_processing_time": "0.0s"
        }

def get_recent_jobs(limit=10):
    """Get recent processing jobs"""
    try:
        init_logs_db()
        conn = sqlite3.connect('data/logs.db')
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT filename, provider, status, error_message, records_processed, 
                   processing_time_seconds, created_at
            FROM processing_logs 
            ORDER BY created_at DESC 
            LIMIT ?
        """, (limit,))
        
        jobs = []
        for row in cursor.fetchall():
            jobs.append({
                "filename": row[0],
                "provider": row[1],
                "status": row[2],
                "error_message": row[3],
                "records_processed": row[4],
                "processing_time": row[5] or 0,
                "created_at": row[6]
            })
        
        conn.close()
        return jobs
    except Exception as e:
        print(f"Error getting recent jobs: {e}")
        return []

def get_vendor_performance():
    """Get vendor performance statistics"""
    try:
        init_logs_db()
        conn = sqlite3.connect('data/logs.db')
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                provider,
                COUNT(*) as total_attempts,
                SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
                AVG(processing_time_seconds) as avg_time,
                MAX(created_at) as last_processed
            FROM processing_logs 
            WHERE created_at >= date('now', '-30 days')
            GROUP BY provider
            ORDER BY provider
        """)
        
        vendors = []
        for row in cursor.fetchall():
            success_rate = (row[2] / row[1]) if row[1] > 0 else 0
            vendors.append({
                "name": row[0].title() if row[0] else "Unknown",
                "total_attempts": row[1],
                "successful": row[2],
                "success_rate": success_rate,
                "avg_processing_time": round(row[3], 2) if row[3] else 0,
                "last_processed": row[4]
            })
        
        conn.close()
        return vendors
    except Exception as e:
        print(f"Error getting vendor performance: {e}")
        return []

def get_configured_vendors():
    """Get list of configured vendors from catalog"""
    try:
        from catalog.catalog_api import get_vendors
        vendors = get_vendors()
        return vendors
    except Exception as e:
        print(f"Error getting configured vendors: {e}")
        return []

def get_processing_logs(limit=50):
    """Get processing logs"""
    try:
        init_logs_db()
        conn = sqlite3.connect('data/logs.db')
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT filename, provider, status, error_message, records_processed,
                   processing_time_seconds, created_at
            FROM processing_logs 
            ORDER BY created_at DESC 
            LIMIT ?
        """, (limit,))
        
        logs = []
        for row in cursor.fetchall():
            logs.append({
                "filename": row[0],
                "provider": row[1],
                "status": row[2],
                "error_message": row[3],
                "records_processed": row[4],
                "processing_time": row[5] or 0,
                "created_at": row[6]
            })
        
        conn.close()
        return logs
    except Exception as e:
        print(f"Error getting processing logs: {e}")
        return []

def init_logs_db():
    """Initialize logs database"""
    os.makedirs('data', exist_ok=True)
    conn = sqlite3.connect('data/logs.db')
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processing_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            provider TEXT,
            status TEXT,
            error_message TEXT,
            records_processed INTEGER,
            processing_time_seconds REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    conn.close()

if __name__ == '__main__':
    # Initialize databases on startup
    init_logs_db()
    
    # Initialize Snowflake catalog tables
    from catalog.catalog_api import init_snowflake_tables
    init_snowflake_tables()
    
    print("🚀 Starting Invoice Processing System...")
    print("📊 Dashboard: http://localhost:5000")
    print("📋 Catalog Manager: http://localhost:5000/catalog-manager")
    print("📈 Vendor Performance: http://localhost:5000/vendor-performance")
    
    app.run(debug=True, host='0.0.0.0', port=5000)    





