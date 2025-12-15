"""
CORD-19 File Opener Service
Opens actual JSON files from pdf_json and pmc_json directories
Run this alongside your search engine on a different port (8081)
"""

from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from pathlib import Path
import json
import os

app = Flask(__name__)
CORS(app)

# Base path configuration - UPDATE THIS TO YOUR PATH
BASE_PATH = r"D:\Hamza\cord-19_2020-05-01\2020-05-01"

# ============================================================================
# FILE OPENER ENDPOINTS
# ============================================================================

@app.route('/open-file', methods=['POST', 'OPTIONS'])
def open_file():
    """
    Open a file in the system's default application
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    try:
        data = request.get_json()
        doc_id = data.get('doc_id', '').strip()
        source_type = data.get('source_type', 'PDF').strip()
        
        if not doc_id:
            return jsonify({'error': 'doc_id is required'}), 400
        
        # Determine the directory based on source type
        if source_type.upper() == 'PDF':
            source_dir = 'pdf_json'
        else:
            source_dir = 'pmc_json'
        
        # Construct file path
        file_path = Path(BASE_PATH) / source_dir / f"{doc_id}.json"
        
        if not file_path.exists():
            return jsonify({
                'error': 'File not found',
                'path': str(file_path)
            }), 404
        
        # Open file in default application
        import subprocess
        import platform
        
        system = platform.system()
        
        try:
            if system == 'Windows':
                os.startfile(str(file_path))
            elif system == 'Darwin':  # macOS
                subprocess.run(['open', str(file_path)])
            else:  # Linux
                subprocess.run(['xdg-open', str(file_path)])
            
            print(f"✓ Opened file: {file_path}")
            
            return jsonify({
                'success': True,
                'message': 'File opened successfully',
                'path': str(file_path)
            }), 200
            
        except Exception as e:
            return jsonify({
                'error': f'Failed to open file: {str(e)}',
                'path': str(file_path)
            }), 500
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/get-file-content', methods=['POST', 'OPTIONS'])
def get_file_content():
    """
    Get the content of a file (alternative to opening)
    Returns the JSON content for display in browser
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    try:
        data = request.get_json()
        doc_id = data.get('doc_id', '').strip()
        source_type = data.get('source_type', 'PDF').strip()
        
        if not doc_id:
            return jsonify({'error': 'doc_id is required'}), 400
        
        # Determine the directory based on source type
        if source_type.upper() == 'PDF':
            source_dir = 'pdf_json'
        else:
            source_dir = 'pmc_json'
        
        # Construct file path
        file_path = Path(BASE_PATH) / source_dir / f"{doc_id}.json"
        
        if not file_path.exists():
            return jsonify({
                'error': 'File not found',
                'path': str(file_path)
            }), 404
        
        # Read and return file content
        with open(file_path, 'r', encoding='utf-8') as f:
            content = json.load(f)
        
        print(f"✓ Loaded content: {file_path}")
        
        return jsonify({
            'success': True,
            'doc_id': doc_id,
            'source_type': source_type,
            'path': str(file_path),
            'content': content
        }), 200
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/view-file', methods=['GET'])
def view_file():
    """
    View a file in the browser with nice formatting
    """
    try:
        doc_id = request.args.get('doc_id', '').strip()
        source_type = request.args.get('source_type', 'PDF').strip()
        
        if not doc_id:
            return jsonify({'error': 'doc_id is required'}), 400
        
        # Determine the directory based on source type
        if source_type.upper() == 'PDF':
            source_dir = 'pdf_json'
        else:
            source_dir = 'pmc_json'
        
        # Construct file path
        file_path = Path(BASE_PATH) / source_dir / f"{doc_id}.json"
        
        if not file_path.exists():
            return f"""
            <html>
            <head>
                <title>File Not Found</title>
                <style>
                    body {{ font-family: Arial; padding: 40px; background: #f5f5f5; }}
                    .error {{ background: white; padding: 30px; border-radius: 8px; max-width: 600px; margin: 0 auto; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
                    h1 {{ color: #d32f2f; }}
                    code {{ background: #f5f5f5; padding: 2px 8px; border-radius: 4px; }}
                </style>
            </head>
            <body>
                <div class="error">
                    <h1>❌ File Not Found</h1>
                    <p><strong>Document ID:</strong> {doc_id}</p>
                    <p><strong>Source Type:</strong> {source_type}</p>
                    <p><strong>Expected Path:</strong></p>
                    <code>{file_path}</code>
                </div>
            </body>
            </html>
            """, 404
        
        # Read the JSON file
        with open(file_path, 'r', encoding='utf-8') as f:
            content = json.load(f)
        
        # Extract metadata for display
        title = "Untitled Document"
        authors = []
        abstract = ""
        
        if 'metadata' in content:
            title = content['metadata'].get('title', title)
            if 'authors' in content['metadata']:
                authors = [f"{a.get('first', '')} {a.get('last', '')}".strip() 
                          for a in content['metadata'].get('authors', [])[:5]]
        
        if 'abstract' in content:
            if isinstance(content['abstract'], list):
                abstract_parts = []
                for item in content['abstract']:
                    if isinstance(item, dict) and 'text' in item:
                        abstract_parts.append(item['text'])
                abstract = ' '.join(abstract_parts)
            elif isinstance(content['abstract'], str):
                abstract = content['abstract']
        
        # Create a nice HTML view
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>{title}</title>
            <style>
                * {{ margin: 0; padding: 0; box-sizing: border-box; }}
                body {{ 
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
                    background: #f8f9fa;
                    padding: 20px;
                    line-height: 1.6;
                }}
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    background: white;
                    border-radius: 8px;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.1);
                    overflow: hidden;
                }}
                .header {{
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 30px;
                }}
                .header h1 {{
                    font-size: 28px;
                    margin-bottom: 15px;
                }}
                .metadata {{
                    display: flex;
                    gap: 20px;
                    flex-wrap: wrap;
                    font-size: 14px;
                    opacity: 0.9;
                }}
                .metadata-item {{
                    display: flex;
                    align-items: center;
                    gap: 5px;
                }}
                .content {{
                    padding: 30px;
                }}
                .section {{
                    margin-bottom: 30px;
                }}
                .section-title {{
                    font-size: 20px;
                    font-weight: 600;
                    color: #333;
                    margin-bottom: 15px;
                    padding-bottom: 10px;
                    border-bottom: 2px solid #e9ecef;
                }}
                .abstract {{
                    font-size: 15px;
                    color: #495057;
                    text-align: justify;
                    background: #f8f9fa;
                    padding: 20px;
                    border-radius: 6px;
                    border-left: 4px solid #667eea;
                }}
                .json-viewer {{
                    background: #1e1e1e;
                    color: #d4d4d4;
                    padding: 20px;
                    border-radius: 6px;
                    overflow-x: auto;
                    font-family: 'Consolas', 'Monaco', monospace;
                    font-size: 13px;
                    line-height: 1.5;
                    max-height: 600px;
                    overflow-y: auto;
                }}
                .json-viewer pre {{
                    margin: 0;
                    white-space: pre-wrap;
                    word-wrap: break-word;
                }}
                .tabs {{
                    display: flex;
                    gap: 10px;
                    padding: 0 30px;
                    background: #f8f9fa;
                    border-bottom: 1px solid #dee2e6;
                }}
                .tab {{
                    padding: 15px 25px;
                    cursor: pointer;
                    border: none;
                    background: none;
                    font-size: 15px;
                    font-weight: 500;
                    color: #6c757d;
                    border-bottom: 3px solid transparent;
                    transition: all 0.2s;
                }}
                .tab:hover {{
                    color: #495057;
                }}
                .tab.active {{
                    color: #667eea;
                    border-bottom-color: #667eea;
                }}
                .tab-content {{
                    display: none;
                }}
                .tab-content.active {{
                    display: block;
                }}
                .authors-list {{
                    display: flex;
                    flex-wrap: wrap;
                    gap: 10px;
                }}
                .author-badge {{
                    background: #e7f3ff;
                    color: #0056b3;
                    padding: 5px 12px;
                    border-radius: 15px;
                    font-size: 13px;
                }}
                .download-btn {{
                    display: inline-block;
                    background: #28a745;
                    color: white;
                    padding: 10px 20px;
                    border-radius: 5px;
                    text-decoration: none;
                    font-size: 14px;
                    margin-top: 20px;
                }}
                .download-btn:hover {{
                    background: #218838;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>{title}</h1>
                    <div class="metadata">
                        <div class="metadata-item">
                            <span>📄</span>
                            <span>Document ID: {doc_id}</span>
                        </div>
                        <div class="metadata-item">
                            <span>📁</span>
                            <span>Source: {source_type}</span>
                        </div>
                    </div>
                </div>
                
                <div class="tabs">
                    <button class="tab active" onclick="switchTab('overview')">📋 Overview</button>
                    <button class="tab" onclick="switchTab('json')">💻 Raw JSON</button>
                </div>
                
                <div class="content">
                    <div id="overview-tab" class="tab-content active">
                        {"<div class='section'><div class='section-title'>👥 Authors</div><div class='authors-list'>" + "".join([f"<span class='author-badge'>{author}</span>" for author in authors]) + "</div></div>" if authors else ""}
                        
                        {"<div class='section'><div class='section-title'>📝 Abstract</div><div class='abstract'>" + abstract + "</div></div>" if abstract else "<div class='section'><div class='abstract'>No abstract available.</div></div>"}
                        
                        <a href="/download-file?doc_id={doc_id}&source_type={source_type}" class="download-btn">⬇️ Download JSON</a>
                    </div>
                    
                    <div id="json-tab" class="tab-content">
                        <div class="json-viewer">
                            <pre>{json.dumps(content, indent=2, ensure_ascii=False)}</pre>
                        </div>
                    </div>
                </div>
            </div>
            
            <script>
                function switchTab(tabName) {{
                    // Hide all tabs
                    document.querySelectorAll('.tab-content').forEach(tab => {{
                        tab.classList.remove('active');
                    }});
                    document.querySelectorAll('.tab').forEach(tab => {{
                        tab.classList.remove('active');
                    }});
                    
                    // Show selected tab
                    document.getElementById(tabName + '-tab').classList.add('active');
                    event.target.classList.add('active');
                }}
            </script>
        </body>
        </html>
        """
        
        print(f"✓ Serving file in browser: {file_path}")
        return html
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return f"""
        <html>
        <head><title>Error</title></head>
        <body>
            <h1>Error loading file</h1>
            <p>{str(e)}</p>
        </body>
        </html>
        """, 500


@app.route('/download-file', methods=['GET'])
def download_file():
    """
    Download a file directly
    """
    try:
        doc_id = request.args.get('doc_id', '').strip()
        source_type = request.args.get('source_type', 'PDF').strip()
        
        if not doc_id:
            return jsonify({'error': 'doc_id is required'}), 400
        
        # Determine the directory based on source type
        if source_type.upper() == 'PDF':
            source_dir = 'pdf_json'
        else:
            source_dir = 'pmc_json'
        
        # Construct file path
        file_path = Path(BASE_PATH) / source_dir / f"{doc_id}.json"
        
        if not file_path.exists():
            return jsonify({
                'error': 'File not found',
                'path': str(file_path)
            }), 404
        
        return send_file(
            str(file_path),
            as_attachment=True,
            download_name=f"{doc_id}.json",
            mimetype='application/json'
        )
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/check-file', methods=['POST'])
def check_file():
    """
    Check if a file exists
    """
    try:
        data = request.get_json()
        doc_id = data.get('doc_id', '').strip()
        source_type = data.get('source_type', 'PDF').strip()
        
        if not doc_id:
            return jsonify({'error': 'doc_id is required'}), 400
        
        # Determine the directory based on source type
        if source_type.upper() == 'PDF':
            source_dir = 'pdf_json'
        else:
            source_dir = 'pmc_json'
        
        # Construct file path
        file_path = Path(BASE_PATH) / source_dir / f"{doc_id}.json"
        
        exists = file_path.exists()
        
        return jsonify({
            'exists': exists,
            'path': str(file_path),
            'doc_id': doc_id,
            'source_type': source_type
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/health', methods=['GET'])
def health():
    """Health check"""
    return jsonify({
        'status': 'healthy',
        'service': 'CORD-19 File Opener Service',
        'version': '1.0.0',
        'base_path': BASE_PATH
    }), 200


# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    print("\n" + "="*60)
    print("📂 CORD-19 File Opener Service")
    print("="*60)
    print(f"📁 Base Path: {BASE_PATH}")
    print("🌐 Service: http://localhost:8081")
    print("="*60 + "\n")
    
    # Check if directories exist
    pdf_dir = Path(BASE_PATH) / 'pdf_json'
    pmc_dir = Path(BASE_PATH) / 'pmc_json'
    
    if pdf_dir.exists():
        print(f"✓ PDF directory found: {pdf_dir}")
    else:
        print(f"⚠ PDF directory NOT found: {pdf_dir}")
    
    if pmc_dir.exists():
        print(f"✓ PMC directory found: {pmc_dir}")
    else:
        print(f"⚠ PMC directory NOT found: {pmc_dir}")
    
    print("\n" + "="*60 + "\n")
    
    try:
        from waitress import serve
        print("✓ Using Waitress (production server)\n")
        serve(app, host='0.0.0.0', port=8081, threads=2)
    except ImportError:
        print("⚠ Using Flask dev server\n")
        app.run(host='0.0.0.0', port=8081, debug=False, threaded=True)