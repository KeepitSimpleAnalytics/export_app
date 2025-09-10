from flask import Flask, jsonify, render_template, request, session
import uuid
import time
import json
import os
from adu.database import get_db_connection  # Keep for backwards compatibility
from adu.enhanced_logger import logger
from adu.sqlite_writer import get_sqlite_writer
from adu.greenplum_pool import get_pool_stats, pool_health_check, get_database_connection as get_pooled_connection, initialize_connection_pool
from adu.websocket_manager import websocket_manager
from adu.tasks import execute_export_job

app = Flask(__name__)

# Initialize WebSocket support
websocket_manager.init_app(app)

# Configuration from environment variables  
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'simple-airgapped-secret')
app.config['DEBUG'] = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'

# No encryption needed for airgapped environment

@app.route('/api/health')
def health_check():
    """Health check endpoint with connection pool and SQLite queue status"""
    try:
        health_info = {
            'status': 'healthy',
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'components': {}
        }
        
        # Check connection pool health
        try:
            pool_health = pool_health_check()
            pool_stats = get_pool_stats()
            health_info['components']['connection_pool'] = {
                'status': pool_health.get('status', 'unknown'),
                'active_connections': pool_stats.get('current_active', 0),
                'max_connections': pool_stats.get('max_connections', 6),
                'circuit_breaker': pool_stats.get('circuit_breaker_state', 'unknown')
            }
        except:
            health_info['components']['connection_pool'] = {'status': 'not_initialized'}
        
        # Check SQLite writer queue
        try:
            sqlite_writer = get_sqlite_writer()
            queue_stats = sqlite_writer.get_stats()
            health_info['components']['sqlite_queue'] = {
                'status': 'healthy' if queue_stats.get('worker_active', False) else 'unhealthy',
                'queue_size': queue_stats.get('queue_size', 0),
                'operations_processed': queue_stats.get('operations_processed', 0)
            }
        except Exception as e:
            health_info['components']['sqlite_queue'] = {'status': 'error', 'error': str(e)}
        
        # Overall health based on components
        if any(comp.get('status') in ['unhealthy', 'error'] 
               for comp in health_info['components'].values()):
            health_info['status'] = 'degraded'
        
        return jsonify(health_info)
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({'status': 'error', 'error': str(e)}), 500

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
    """Get job history using SQLite writer queue"""
    try:
        sqlite_writer = get_sqlite_writer()
        query = """
            SELECT job_id, db_username, status, overall_status, celery_task_id,
                   start_time, end_time, error_message, progress_percent,
                   tables_total, tables_completed, tables_failed,
                   created_at
            FROM jobs ORDER BY start_time DESC
        """
        jobs = sqlite_writer.query(query, (), fetchone=False, timeout=10.0)
        
        # Convert to list of dicts for JSON serialization
        jobs_list = [dict(row) for row in jobs] if jobs else []
        return jsonify(jobs_list)
        
    except Exception as e:
        logger.error(f"Failed to get job history: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/job/<job_id>')
def get_job(job_id):
    """Get specific job details using SQLite writer queue"""
    try:
        sqlite_writer = get_sqlite_writer()
        query = "SELECT * FROM jobs WHERE job_id = ?"
        job_row = sqlite_writer.query(query, (job_id,), fetchone=True, timeout=5.0)
        
        if job_row:
            return jsonify(dict(job_row))
        return jsonify({'error': 'Job not found'}), 404
            
    except Exception as e:
        logger.error(f"Failed to get job {job_id}: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/job/<job_id>/errors')
def get_job_errors(job_id):
    """Get job errors using SQLite writer queue"""
    try:
        sqlite_writer = get_sqlite_writer()
        query = "SELECT * FROM errors WHERE job_id = ? ORDER BY timestamp DESC"
        errors = sqlite_writer.query(query, (job_id,), fetchone=False, timeout=5.0)
        
        errors_list = [dict(row) for row in errors] if errors else []
        return jsonify(errors_list)
        
    except Exception as e:
        logger.error(f"Failed to get errors for job {job_id}: {e}")
        return jsonify({'error': str(e)}), 500

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

@app.route('/api/job/<job_id>/chunks')
def get_job_chunks(job_id):
    """Get chunk processing information for a specific job"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get table export details for chunking information
    cursor.execute('SELECT * FROM table_exports WHERE job_id = ? ORDER BY table_name', (job_id,))
    tables = []
    
    total_chunks = 0
    completed_chunks = 0
    processing_chunks = 0
    
    for row in cursor.fetchall():
        table = dict(row)
        
        # Estimate chunk information based on file paths and status
        chunk_count = 1  # Default to single file
        chunks_completed = 0
        chunks_remaining = 1
        
        # Check if this table has chunk files (partitioned export)
        if table.get('file_path'):
            try:
                # Count actual parquet files for this table
                from pathlib import Path
                import os
                
                # Extract table directory from file path
                file_path = Path(table['file_path'])
                if file_path.exists():
                    table_dir = file_path.parent
                    # Count part_*.parquet files
                    chunk_files = list(table_dir.glob('part_*.parquet'))
                    if len(chunk_files) > 1:
                        chunk_count = len(chunk_files)
                        chunks_completed = len(chunk_files) if table['status'] == 'completed' else 0
                        chunks_remaining = chunk_count - chunks_completed
            except Exception:
                pass  # Use defaults if file system check fails
        
        # Update based on table status
        if table['status'] == 'completed':
            chunks_completed = chunk_count
            chunks_remaining = 0
        elif table['status'] == 'processing':
            processing_chunks += chunks_remaining
        
        table['chunk_count'] = chunk_count
        table['chunks_completed'] = chunks_completed
        table['chunks_remaining'] = chunks_remaining
        
        total_chunks += chunk_count
        completed_chunks += chunks_completed
        
        tables.append(table)
    
    summary = {
        'total_chunks': total_chunks,
        'completed_chunks': completed_chunks, 
        'processing_chunks': processing_chunks
    }
    
    conn.close()
    return jsonify({
        'tables': tables,
        'summary': summary
    })

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
                            # Metadata file not found, try to infer from directory contents
                            table_info['partitioned'] = False
                            
                            # Look for parquet files in the directory
                            parquet_files = list(table_path.glob("*.parquet"))
                            if parquet_files:
                                # Check if it's partitioned (multiple part_*.parquet files)
                                part_files = [f for f in parquet_files if f.name.startswith('part_')]
                                
                                if len(part_files) > 1:
                                    # Partitioned export
                                    table_info['partitioned'] = True
                                    table_info['chunk_count'] = len(part_files)
                                    table_info['files'] = [f.name for f in part_files]
                                else:
                                    # Single file export
                                    table_info['partitioned'] = False
                                    table_info['chunk_count'] = 1
                                    table_info['files'] = [parquet_files[0].name]
                                
                                # Calculate total size
                                total_size = sum(f.stat().st_size for f in parquet_files)
                                table_info['total_size_bytes'] = total_size
                                table_info['total_size_mb'] = round(total_size / (1024 * 1024), 2)
                                
                                # Add a note about inferred data
                                table_info['metadata_source'] = 'inferred_from_files'
                            else:
                                table_info['error'] = 'No parquet files found in export directory'
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
        
        # If log file doesn't exist, create it or return empty logs with helpful message
        if not os.path.exists(log_file_path):
            # Try to create the log file directory if needed
            os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
            
            # Return empty logs with informative message instead of error
            return jsonify({
                'lines': ['[INFO] Worker log file not found - no worker processes have started yet.'],
                'total_lines': 1,
                'filtered': False,
                'showing': 1,
                'status': 'log_file_not_found'
            })
        
        # Check if file is readable
        if not os.access(log_file_path, os.R_OK):
            return jsonify({
                'lines': ['[ERROR] Log file exists but is not readable - check permissions.'],
                'total_lines': 1,
                'filtered': False,
                'showing': 1,
                'status': 'permission_error'
            })
        
        with open(log_file_path, 'r') as f:
            all_lines = f.readlines()
        
        # Handle empty log file
        if not all_lines:
            return jsonify({
                'lines': ['[INFO] Worker log file is empty - worker processes may not have logged anything yet.'],
                'total_lines': 0,
                'filtered': False,
                'showing': 1,
                'status': 'empty_log'
            })
        
        # Filter by job_id if provided
        if job_id:
            filtered_lines = [line for line in all_lines if job_id in line]
            log_lines = filtered_lines[-lines:] if len(filtered_lines) > lines else filtered_lines
            
            # If no matching lines found for job_id
            if not log_lines:
                log_lines = [f'[INFO] No log entries found for job ID: {job_id}']
                
        else:
            log_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines
        
        return jsonify({
            'lines': log_lines,
            'total_lines': len(all_lines),
            'filtered': bool(job_id),
            'showing': len(log_lines),
            'status': 'success'
        })
        
    except PermissionError:
        return jsonify({
            'lines': ['[ERROR] Permission denied accessing log file - check file permissions.'],
            'total_lines': 1,
            'filtered': False,
            'showing': 1,
            'status': 'permission_error'
        })
    except Exception as e:
        return jsonify({
            'lines': [f'[ERROR] Failed to read log file: {str(e)}'],
            'total_lines': 1,
            'filtered': False,
            'showing': 1,
            'status': 'error'
        })

@app.route('/api/logs/test')
def test_logs_api():
    """Test endpoint to verify API connectivity"""
    return jsonify({
        'status': 'success',
        'message': 'Logs API is accessible',
        'server_time': time.strftime('%Y-%m-%d %H:%M:%S'),
        'log_file_path': '/tmp/worker.log',
        'log_file_exists': os.path.exists('/tmp/worker.log'),
        'log_file_readable': os.path.exists('/tmp/worker.log') and os.access('/tmp/worker.log', os.R_OK)
    })

@app.route('/logs')
def logs_page():
    return render_template('logs.html')

@app.route('/chunks')
def chunks_page():
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
        cursor.execute("INSERT INTO jobs (job_id, db_username, status, start_time) VALUES (?, ?, ?, ?)",
                       (job_id, data['db_username'], 'queued', time.strftime('%Y-%m-%d %H:%M:%S')))
        cursor.execute("INSERT INTO job_configs (job_id, config) VALUES (?, ?)",
                       (job_id, json.dumps(data)))
        conn.commit()
        conn.close()

        # Start the job using Celery
        execute_export_job.delay(job_id, data)

        return jsonify({
            'job_id': job_id, 
            'message': 'Job created and queued successfully',
            'status': 'queued'
        })
        
    except Exception as e:
        app.logger.error(f"Error creating job: {str(e)}")
        return jsonify({'error': f'Failed to create job: {str(e)}'}), 500

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
        from .worker import discover_tables, discover_schemas, discover_tables_by_schema
        
        # Initialize connection pool for this request
        initialize_connection_pool(
            data['db_type'], 
            data['db_host'], 
            int(data['db_port']), 
            data['db_username'], 
            data['db_password'],
            data.get('db_name') or ('postgres' if data['db_type'].lower() in ['postgresql', 'greenplum'] else 'defaultdb')
        )
        
        # Use connection pool
        with get_pooled_connection() as db_conn:
            tables = discover_tables(db_conn, data['db_type'])
        
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
        from .worker import get_database_connection
        
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
        from .worker import discover_schemas
        
        # Initialize connection pool for this request
        initialize_connection_pool(
            data['db_type'], 
            data['db_host'], 
            int(data['db_port']), 
            data['db_username'], 
            data['db_password'],
            data.get('db_name') or ('postgres' if data['db_type'].lower() in ['postgresql', 'greenplum'] else 'defaultdb')
        )
        
        # Use connection pool
        with get_pooled_connection() as db_conn:
            schemas = discover_schemas(db_conn, data['db_type'])
        
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
        from .worker import discover_tables_by_schema
        
        # Initialize connection pool for this request
        initialize_connection_pool(
            data['db_type'], 
            data['db_host'], 
            int(data['db_port']), 
            data['db_username'], 
            data['db_password'],
            data.get('db_name') or ('postgres' if data['db_type'].lower() in ['postgresql', 'greenplum'] else 'defaultdb')
        )
        
        # Use connection pool
        schema_name = data.get('schema_name')  # Optional - if not provided, returns all tables
        with get_pooled_connection() as db_conn:
            tables = discover_tables_by_schema(db_conn, data['db_type'], schema_name)
        
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
