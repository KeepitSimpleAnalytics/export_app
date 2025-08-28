import sqlite3
import os

DB_FILE = os.environ.get('ADU_DB_PATH', '/tmp/adu.db')

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Create jobs table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            db_username TEXT,
            overall_status TEXT DEFAULT 'queued',
            celery_task_id TEXT,
            start_time DATETIME,
            end_time DATETIME,
            error_message TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Create errors table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT,
            timestamp DATETIME,
            error_message TEXT,
            traceback TEXT,
            context TEXT,
            FOREIGN KEY (job_id) REFERENCES jobs (job_id)
        )
    ''')

    # Create job_configs table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS job_configs (
            job_id TEXT PRIMARY KEY,
            config TEXT,
            FOREIGN KEY (job_id) REFERENCES jobs (job_id)
        )
    ''')

    # Create table_exports table to track individual table processing
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS table_exports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT,
            table_name TEXT,
            status TEXT,
            row_count INTEGER,
            file_path TEXT,
            start_time DATETIME,
            end_time DATETIME,
            error_message TEXT,
            FOREIGN KEY (job_id) REFERENCES jobs (job_id)
        )
    ''')

    conn.commit()
    conn.close()