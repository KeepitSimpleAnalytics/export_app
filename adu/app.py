from flask import Flask, jsonify, render_template, request, session
from flask_seasurf import SeaSurf
from flask_talisman import Talisman
import uuid
import time
import json
from cryptography.fernet import Fernet
from database import get_db_connection

import os

app = Flask(__name__)

# Configuration from environment variables
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
app.config['DEBUG'] = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'

# Security settings
csrf = SeaSurf(app)
talisman_config = {
    'force_https': os.environ.get('FORCE_HTTPS', 'False').lower() == 'true',
    'strict_transport_security': True,
    'strict_transport_security_max_age': 31536000,  # 1 year
    'content_security_policy': {
        'default-src': "'self'",
        'script-src': "'self' 'unsafe-inline'",
        'style-src': "'self' 'unsafe-inline'",
    }
}
talisman = Talisman(app, **talisman_config)

# Encryption key management
fernet_key = os.environ.get('FERNET_KEY')
if not fernet_key:
    # Generate a new key if not provided (development only)
    if app.config['DEBUG']:
        key = Fernet.generate_key()
        fernet = Fernet(key)
        os.environ['FERNET_KEY'] = key.decode()
        app.logger.warning("Generated new FERNET_KEY for development. Set FERNET_KEY environment variable in production!")
    else:
        raise ValueError("FERNET_KEY environment variable must be set in production")
else:
    fernet = Fernet(fernet_key.encode())

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/history')
def history():
    return render_template('history.html')

@app.route('/job/<job_id>')
def job_details(job_id):
    return render_template('job_details.html', job_id=job_id)

@app.route('/api/history')
def get_history():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM jobs ORDER BY start_time DESC')
    jobs = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(jobs)

@app.route('/api/job/<job_id>')
def get_job(job_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM jobs WHERE job_id = ?', (job_id,))
    job = dict(cursor.fetchone())
    conn.close()
    return jsonify(job)

@app.route('/api/job/<job_id>/errors')
def get_job_errors(job_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM errors WHERE job_id = ? ORDER BY timestamp DESC', (job_id,))
    errors = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(errors)

@app.route('/api/job/<job_id>/config')
def get_job_config(job_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT config FROM job_configs WHERE job_id = ?', (job_id,))
    result = cursor.fetchone()
    conn.close()
    if result:
        config = json.loads(result['config'])
        # Remove sensitive information
        config_safe = config.copy()
        config_safe['db_password'] = '***ENCRYPTED***'
        return jsonify(config_safe)
    return jsonify({}), 404

@app.route('/api/job/<job_id>/tables')
def get_job_tables(job_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM table_exports WHERE job_id = ? ORDER BY table_name', (job_id,))
    tables = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(tables)

@app.route('/api/logs/worker')
def get_worker_logs():
    """Get worker log file contents with optional filtering"""
    lines = request.args.get('lines', 100, type=int)
    job_id = request.args.get('job_id', None)
    
    try:
        log_file_path = '/tmp/worker.log'
        if not os.path.exists(log_file_path):
            return jsonify({'error': 'Log file not found'}), 404
        
        with open(log_file_path, 'r') as f:
            all_lines = f.readlines()
        
        # Filter by job_id if provided
        if job_id:
            filtered_lines = [line for line in all_lines if job_id in line]
            log_lines = filtered_lines[-lines:] if len(filtered_lines) > lines else filtered_lines
        else:
            log_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines
        
        return jsonify({
            'lines': log_lines,
            'total_lines': len(all_lines),
            'filtered': bool(job_id),
            'showing': len(log_lines)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/logs')
def logs_page():
    return render_template('logs.html')

@app.route('/api/jobs', methods=['POST'])
@csrf.exempt
def create_job():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400
        
        # Validate required fields
        required_fields = ['db_type', 'db_host', 'db_port', 'db_username', 'db_password']
        missing_fields = [field for field in required_fields if field not in data or not data[field]]
        if missing_fields:
            return jsonify({'error': f'Missing required fields: {", ".join(missing_fields)}'}), 400
        
        job_id = str(uuid.uuid4())
        
        # Encrypt the password
        data['db_password'] = fernet.encrypt(data['db_password'].encode()).decode()

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO jobs (job_id, db_username, overall_status, start_time) VALUES (?, ?, ?, ?)",
                       (job_id, data['db_username'], 'queued', time.strftime('%Y-%m-%d %H:%M:%S')))
        cursor.execute("INSERT INTO job_configs (job_id, config) VALUES (?, ?)",
                       (job_id, json.dumps(data)))
        conn.commit()
        conn.close()

        return jsonify({
            'job_id': job_id, 
            'message': 'Job created successfully',
            'status': 'queued'
        })
        
    except Exception as e:
        app.logger.error(f"Error creating job: {str(e)}")
        return jsonify({'error': f'Failed to create job: {str(e)}'}), 500

@app.route('/api/discover-schema', methods=['POST'])
@csrf.exempt
def discover_schema():
    """Discover database schema without creating a job"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400
        
        # Validate required fields for connection
        required_fields = ['db_type', 'db_host', 'db_port', 'db_username', 'db_password']
        missing_fields = [field for field in required_fields if field not in data or not data[field]]
        if missing_fields:
            return jsonify({'error': f'Missing required fields: {", ".join(missing_fields)}'}), 400
        
        # Import database functions
        from worker import get_database_connection, discover_tables
        
        # Connect to database
        db_conn = get_database_connection(
            data['db_type'], 
            data['db_host'], 
            int(data['db_port']), 
            data['db_username'], 
            data['db_password'],
            data.get('db_name')
        )
        
        # Discover tables
        tables = discover_tables(db_conn, data['db_type'])
        db_conn.close()
        
        return jsonify({
            'tables': tables,
            'count': len(tables),
            'database_type': data['db_type'],
            'host': data['db_host']
        })
        
    except Exception as e:
        app.logger.error(f"Error discovering schema: {str(e)}")
        return jsonify({'error': f'Failed to discover schema: {str(e)}'}), 500

@app.route('/api/table-info', methods=['POST'])
@csrf.exempt
def get_table_info():
    """Get detailed information about a specific table"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400
        
        # Validate required fields
        required_fields = ['db_type', 'db_host', 'db_port', 'db_username', 'db_password', 'table_name']
        missing_fields = [field for field in required_fields if field not in data or not data[field]]
        if missing_fields:
            return jsonify({'error': f'Missing required fields: {", ".join(missing_fields)}'}), 400
        
        # Import database functions
        from worker import get_database_connection
        
        # Connect to database
        db_conn = get_database_connection(
            data['db_type'], 
            data['db_host'], 
            int(data['db_port']), 
            data['db_username'], 
            data['db_password'],
            data.get('db_name')
        )
        
        cursor = db_conn.cursor()
        table_name = data['table_name']
        
        # Get table info based on database type
        if data['db_type'].lower() == 'postgresql':
            # Get column information
            cursor.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_name = %s AND table_schema = 'public'
                ORDER BY ordinal_position
            """, (table_name,))
            columns = [{'name': row[0], 'type': row[1], 'nullable': row[2], 'default': row[3]} for row in cursor.fetchall()]
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
        elif data['db_type'].lower() == 'vertica':
            # Get column information
            cursor.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM v_catalog.columns 
                WHERE table_name = %s AND schema_name = 'public'
                ORDER BY ordinal_position
            """, (table_name,))
            columns = [{'name': row[0], 'type': row[1], 'nullable': row[2], 'default': row[3]} for row in cursor.fetchall()]
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
        
        db_conn.close()
        
        return jsonify({
            'table_name': table_name,
            'columns': columns,
            'row_count': row_count,
            'column_count': len(columns)
        })
        
    except Exception as e:
        app.logger.error(f"Error getting table info: {str(e)}")
        return jsonify({'error': f'Failed to get table info: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
