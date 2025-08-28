from flask import Flask, jsonify, render_template, request, session
import uuid
import time
import json
import logging
from adu.database import get_db_connection

import os

app = Flask(__name__)

# Configuration from environment variables  
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'simple-airgapped-secret')
app.config['DEBUG'] = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'

# Disable HTTP request logging unless DEBUG is enabled
if not app.config['DEBUG']:
    # Disable Werkzeug HTTP request logging
    logging.getLogger('werkzeug').setLevel(logging.WARNING)
    
    # Only show errors and warnings for application logs
    logging.basicConfig(level=logging.WARNING)

# No encryption needed for airgapped environment

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
    job_row = cursor.fetchone()
    conn.close()
    if job_row:
        return jsonify(dict(job_row))
    return jsonify({'error': 'Job not found'}), 404

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

@app.route('/api/job/<job_id>/export-details')
def get_job_export_details(job_id):
    """Get detailed information about exported files including partitioning info"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get job configuration to find output path
        cursor.execute('SELECT config FROM job_configs WHERE job_id = ?', (job_id,))
        config_result = cursor.fetchone()
        if not config_result:
            return jsonify({'error': 'Job configuration not found'}), 404
        
        config = json.loads(config_result['config'])
        output_path = config.get('output_path', '/app/exports')
        
        # Get table export records
        cursor.execute('SELECT * FROM table_exports WHERE job_id = ? ORDER BY table_name', (job_id,))
        tables = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        # Enhanced table details with file information
        enhanced_tables = []
        for table in tables:
            table_info = table.copy()
            
            if table['status'] == 'completed' and table['file_path']:
                try:
                    # Check if the path points to a directory (new format) or file (old format)
                    import os
                    from pathlib import Path
                    
                    table_path = Path(table['file_path'])
                    if table_path.is_dir():
                        # New partitioned format
                        metadata_file = table_path / "_export_metadata.json"
                        if metadata_file.exists():
                            with open(metadata_file, 'r') as f:
                                metadata = json.load(f)
                            
                            table_info['partitioned'] = metadata.get('partitioned', False)
                            table_info['chunk_count'] = metadata.get('chunk_count', 1)
                            table_info['chunk_size'] = metadata.get('chunk_size')
                            table_info['files'] = metadata.get('files', [])
                            table_info['export_timestamp'] = metadata.get('export_timestamp')
                            
                            # Calculate total file size
                            total_size = 0
                            for filename in table_info['files']:
                                file_path = table_path / filename
                                if file_path.exists():
                                    total_size += file_path.stat().st_size
                            table_info['total_size_bytes'] = total_size
                            table_info['total_size_mb'] = round(total_size / (1024 * 1024), 2)
                        else:
                            table_info['partitioned'] = False
                            table_info['error'] = 'Metadata file not found'
                    else:
                        # Old single file format or file doesn't exist
                        if table_path.exists():
                            table_info['partitioned'] = False
                            table_info['chunk_count'] = 1
                            table_info['files'] = [table_path.name]
                            file_size = table_path.stat().st_size
                            table_info['total_size_bytes'] = file_size
                            table_info['total_size_mb'] = round(file_size / (1024 * 1024), 2)
                        else:
                            table_info['error'] = 'Export file not found'
                            
                except Exception as e:
                    table_info['error'] = f'Error reading export details: {str(e)}'
            
            enhanced_tables.append(table_info)
        
        return jsonify({
            'job_id': job_id,
            'tables': enhanced_tables,
            'total_tables': len(enhanced_tables),
            'output_path': output_path
        })
        
    except Exception as e:
        app.logger.error(f"Error getting export details: {str(e)}")
        return jsonify({'error': f'Failed to get export details: {str(e)}'}), 500

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

@app.route('/api/job/<job_id>/chunks')
def get_job_chunks(job_id):
    """Get detailed chunk processing information for a specific job"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get tables for this job with their processing status
        cursor.execute('''
            SELECT table_name, status, start_time, end_time, 
                   row_count, file_path, error_message
            FROM table_exports 
            WHERE job_id = ? 
            ORDER BY table_name
        ''', (job_id,))
        
        tables = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        # Enhance with chunk information for each table
        chunks_info = []
        for table in tables:
            table_info = table.copy()
            
            if table['status'] in ['processing', 'completed'] and table['file_path']:
                try:
                    from pathlib import Path
                    import json
                    
                    table_path = Path(table['file_path'])
                    if table_path.is_dir():
                        # Check for metadata file
                        metadata_file = table_path / "_export_metadata.json"
                        if metadata_file.exists():
                            with open(metadata_file, 'r') as f:
                                metadata = json.load(f)
                            
                            table_info['chunk_count'] = metadata.get('chunk_count', 1)
                            table_info['chunk_size'] = metadata.get('chunk_size')
                            table_info['partitioned'] = metadata.get('partitioned', False)
                            table_info['files'] = metadata.get('files', [])
                            
                            # For processing tables, estimate current chunk
                            if table['status'] == 'processing':
                                completed_files = [f for f in table_info['files'] 
                                                 if (table_path / f).exists()]
                                table_info['chunks_completed'] = len(completed_files)
                                table_info['chunks_remaining'] = table_info['chunk_count'] - len(completed_files)
                                table_info['processing_chunk'] = len(completed_files) + 1 if len(completed_files) < table_info['chunk_count'] else table_info['chunk_count']
                            else:
                                table_info['chunks_completed'] = table_info['chunk_count']
                                table_info['chunks_remaining'] = 0
                        else:
                            # Single file or no metadata
                            table_info['chunk_count'] = 1
                            table_info['partitioned'] = False
                            if table['status'] == 'processing':
                                table_info['chunks_completed'] = 0
                                table_info['chunks_remaining'] = 1
                                table_info['processing_chunk'] = 1
                            else:
                                table_info['chunks_completed'] = 1
                                table_info['chunks_remaining'] = 0
                    else:
                        # Single file
                        table_info['chunk_count'] = 1
                        table_info['partitioned'] = False
                        if table['status'] == 'processing':
                            table_info['chunks_completed'] = 0
                            table_info['chunks_remaining'] = 1
                            table_info['processing_chunk'] = 1
                        else:
                            table_info['chunks_completed'] = 1
                            table_info['chunks_remaining'] = 0
                            
                except Exception as e:
                    table_info['error'] = f'Error reading chunk details: {str(e)}'
                    table_info['chunk_count'] = 1
                    table_info['partitioned'] = False
            else:
                # Queued or failed tables
                table_info['chunk_count'] = 1
                table_info['partitioned'] = False
                table_info['chunks_completed'] = 0
                table_info['chunks_remaining'] = 1 if table['status'] in ['queued', 'processing'] else 0
            
            chunks_info.append(table_info)
        
        return jsonify({
            'job_id': job_id,
            'tables': chunks_info,
            'summary': {
                'total_tables': len(chunks_info),
                'processing_tables': len([t for t in chunks_info if t['status'] == 'processing']),
                'completed_tables': len([t for t in chunks_info if t['status'] == 'completed']),
                'failed_tables': len([t for t in chunks_info if t['status'] == 'failed']),
                'total_chunks': sum(t.get('chunk_count', 1) for t in chunks_info),
                'completed_chunks': sum(t.get('chunks_completed', 0) for t in chunks_info),
                'processing_chunks': sum(1 for t in chunks_info if t['status'] == 'processing' and t.get('chunks_remaining', 0) > 0)
            }
        })
        
    except Exception as e:
        app.logger.error(f"Error getting chunk details: {str(e)}")
        return jsonify({'error': f'Failed to get chunk details: {str(e)}'}), 500

@app.route('/chunks')
def chunks_page():
    """Chunk processing monitoring page"""
    return render_template('chunks.html')

@app.route('/api/jobs', methods=['POST'])
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
        
        # No encryption needed for airgapped environment
        # data['db_password'] remains as-is

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

@app.route('/api/job/<job_id>/cancel', methods=['POST'])
def cancel_job(job_id):
    """Cancel a running or queued job"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check current job status
        cursor.execute('SELECT overall_status FROM jobs WHERE job_id = ?', (job_id,))
        result = cursor.fetchone()
        
        if not result:
            conn.close()
            return jsonify({'error': 'Job not found'}), 404
        
        current_status = result[0]
        
        # Only allow cancellation of queued or running jobs
        if current_status not in ['queued', 'running']:
            conn.close()
            return jsonify({'error': f'Cannot cancel job with status: {current_status}'}), 400
        
        # Update job status to cancelled
        cursor.execute(
            "UPDATE jobs SET overall_status = ?, end_time = ? WHERE job_id = ?",
            ('cancelled', time.strftime('%Y-%m-%d %H:%M:%S'), job_id)
        )
        
        # Log the cancellation
        cursor.execute(
            "INSERT INTO errors (job_id, timestamp, error_message, context) VALUES (?, ?, ?, ?)",
            (job_id, time.strftime('%Y-%m-%d %H:%M:%S'), 
             'Job cancelled by user request', 
             json.dumps({'action': 'user_cancellation', 'previous_status': current_status}))
        )
        
        conn.commit()
        conn.close()
        
        app.logger.info(f"Job {job_id} cancelled by user request")
        
        return jsonify({
            'message': f'Job {job_id} has been cancelled',
            'previous_status': current_status,
            'new_status': 'cancelled'
        })
        
    except Exception as e:
        app.logger.error(f"Error cancelling job {job_id}: {str(e)}")
        return jsonify({'error': f'Failed to cancel job: {str(e)}'}), 500

@app.route('/api/discover-schema', methods=['POST'])
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
        from adu.worker import get_database_connection, discover_tables, discover_schemas, discover_tables_by_schema
        
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
        from adu.worker import get_database_connection
        
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
        if data['db_type'].lower() in ['postgresql', 'greenplum']:
            # Parse table name to handle schema.table format
            if '.' in table_name:
                schema_name, actual_table_name = table_name.split('.', 1)
            else:
                schema_name = 'public'
                actual_table_name = table_name
                
            # Get column information - try multiple approaches for better Greenplum compatibility
            try:
                cursor.execute("""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns 
                    WHERE table_name = %s AND table_schema = %s
                    ORDER BY ordinal_position
                """, (actual_table_name, schema_name))
                columns = [{'name': row[0], 'type': row[1], 'nullable': row[2], 'default': row[3]} for row in cursor.fetchall()]
                
                # If no columns found and not public schema, try with public schema as fallback
                if not columns and schema_name != 'public':
                    cursor.execute("""
                        SELECT column_name, data_type, is_nullable, column_default
                        FROM information_schema.columns 
                        WHERE table_name = %s AND table_schema = 'public'
                        ORDER BY ordinal_position
                    """, (actual_table_name,))
                    columns = [{'name': row[0], 'type': row[1], 'nullable': row[2], 'default': row[3]} for row in cursor.fetchall()]
                    
                # If still no columns, try without schema restriction (for Greenplum compatibility)
                if not columns:
                    cursor.execute("""
                        SELECT column_name, data_type, is_nullable, column_default
                        FROM information_schema.columns 
                        WHERE table_name = %s
                        ORDER BY ordinal_position
                    """, (actual_table_name,))
                    columns = [{'name': row[0], 'type': row[1], 'nullable': row[2], 'default': row[3]} for row in cursor.fetchall()]
                    
            except Exception as e:
                app.logger.warning(f"Failed to get column info from information_schema: {e}")
                # Fallback: try to get columns directly from the table
                try:
                    cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
                    columns = [{'name': desc[0], 'type': 'unknown', 'nullable': 'YES', 'default': None} 
                              for desc in cursor.description]
                except Exception as e2:
                    app.logger.error(f"Failed to get column info via direct query: {e2}")
                    columns = []
            
            # Get row count
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cursor.fetchone()[0]
            except Exception as e:
                app.logger.warning(f"Failed to get row count: {e}")
                row_count = 0
            
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
        
        # Calculate partitioning information
        chunk_size = data.get('chunk_size', 1000000)
        will_be_partitioned = row_count > chunk_size
        estimated_chunks = max(1, (row_count + chunk_size - 1) // chunk_size) if will_be_partitioned else 1
        
        # Estimate file size (rough calculation)
        estimated_size_mb = max(1, row_count * len(columns) * 10 / (1024 * 1024))  # Rough estimate
        
        return jsonify({
            'table_name': table_name,
            'columns': columns,
            'row_count': row_count,
            'column_count': len(columns),
            'partitioning_info': {
                'will_be_partitioned': will_be_partitioned,
                'estimated_chunks': estimated_chunks,
                'chunk_size': chunk_size,
                'estimated_size_mb': round(estimated_size_mb, 2)
            }
        })
        
    except Exception as e:
        app.logger.error(f"Error getting table info: {str(e)}")
        return jsonify({'error': f'Failed to get table info: {str(e)}'}), 500

@app.route('/api/discover-schemas', methods=['POST'])
def discover_database_schemas():
    """Discover all schemas in the database"""
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
        from adu.worker import get_database_connection, discover_schemas
        
        # Connect to database
        db_conn = get_database_connection(
            data['db_type'], 
            data['db_host'], 
            int(data['db_port']), 
            data['db_username'], 
            data['db_password'],
            data.get('db_name')
        )
        
        # Discover schemas
        schemas = discover_schemas(db_conn, data['db_type'])
        db_conn.close()
        
        return jsonify({
            'schemas': schemas,
            'count': len(schemas),
            'database_type': data['db_type'],
            'host': data['db_host']
        })
        
    except Exception as e:
        app.logger.error(f"Error discovering schemas: {str(e)}")
        return jsonify({'error': f'Failed to discover schemas: {str(e)}'}), 500

@app.route('/api/discover-tables-by-schema', methods=['POST'])
def discover_schema_tables():
    """Discover tables in a specific schema or all schemas"""
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
        from adu.worker import get_database_connection, discover_tables_by_schema
        
        # Connect to database
        db_conn = get_database_connection(
            data['db_type'], 
            data['db_host'], 
            int(data['db_port']), 
            data['db_username'], 
            data['db_password'],
            data.get('db_name')
        )
        
        # Discover tables by schema
        schema_name = data.get('schema_name')  # Optional - if not provided, returns all tables
        tables = discover_tables_by_schema(db_conn, data['db_type'], schema_name)
        db_conn.close()
        
        # Group tables by schema for better organization
        schemas_dict = {}
        for table in tables:
            schema = table['schema']
            if schema not in schemas_dict:
                schemas_dict[schema] = []
            schemas_dict[schema].append({
                'table_name': table['table'],
                'full_name': table['full_name']
            })
        
        return jsonify({
            'tables_by_schema': schemas_dict,
            'tables': tables,  # Keep flat list for backward compatibility
            'total_tables': len(tables),
            'schema_count': len(schemas_dict),
            'requested_schema': schema_name,
            'database_type': data['db_type'],
            'host': data['db_host']
        })
        
    except Exception as e:
        app.logger.error(f"Error discovering tables by schema: {str(e)}")
        return jsonify({'error': f'Failed to discover tables by schema: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
