import unittest
import tempfile
import os
import sys
from unittest.mock import patch

# Add the adu directory to the path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'adu'))

from database import init_db, get_db_connection


class TestDatabase(unittest.TestCase):
    """Test cases for database functionality"""
    
    def setUp(self):
        """Set up test environment"""
        # Create a temporary database for testing
        self.test_db = tempfile.NamedTemporaryFile(delete=False)
        self.test_db.close()
        
        # Patch the database file path
        self.db_patcher = patch('adu.database.DB_FILE', self.test_db.name)
        self.db_patcher.start()
    
    def tearDown(self):
        """Clean up test environment"""
        self.db_patcher.stop()
        try:
            os.unlink(self.test_db.name)
        except FileNotFoundError:
            pass
    
    def test_init_db(self):
        """Test database initialization"""
        # Initialize the database
        init_db()
        
        # Check that the database file was created
        self.assertTrue(os.path.exists(self.test_db.name))
        
        # Check that tables were created
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check for jobs table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='jobs'")
        self.assertIsNotNone(cursor.fetchone())
        
        # Check for errors table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='errors'")
        self.assertIsNotNone(cursor.fetchone())
        
        # Check for job_configs table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='job_configs'")
        self.assertIsNotNone(cursor.fetchone())
        
        # Check for table_exports table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='table_exports'")
        self.assertIsNotNone(cursor.fetchone())
        
        conn.close()
    
    def test_get_db_connection(self):
        """Test database connection"""
        # Initialize the database first
        init_db()
        
        # Get a connection
        conn = get_db_connection()
        self.assertIsNotNone(conn)
        
        # Test that we can execute a query
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        self.assertEqual(result[0], 1)
        
        conn.close()
    
    def test_jobs_table_structure(self):
        """Test jobs table structure"""
        init_db()
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get table info
        cursor.execute("PRAGMA table_info(jobs)")
        columns = cursor.fetchall()
        
        # Expected columns
        expected_columns = ['job_id', 'db_username', 'overall_status', 'start_time', 'end_time']
        actual_columns = [col[1] for col in columns]
        
        for expected_col in expected_columns:
            self.assertIn(expected_col, actual_columns)
        
        conn.close()
    
    def test_table_exports_table_structure(self):
        """Test table_exports table structure"""
        init_db()
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get table info
        cursor.execute("PRAGMA table_info(table_exports)")
        columns = cursor.fetchall()
        
        # Expected columns
        expected_columns = ['id', 'job_id', 'table_name', 'status', 'row_count', 
                          'file_path', 'start_time', 'end_time', 'error_message']
        actual_columns = [col[1] for col in columns]
        
        for expected_col in expected_columns:
            self.assertIn(expected_col, actual_columns)
        
        conn.close()
    
    def test_database_operations(self):
        """Test basic database operations"""
        init_db()
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Insert a test job
        cursor.execute(
            "INSERT INTO jobs (job_id, db_username, overall_status, start_time) VALUES (?, ?, ?, ?)",
            ('test-job-1', 'testuser', 'queued', '2024-01-01 10:00:00')
        )
        
        # Insert a test job config
        cursor.execute(
            "INSERT INTO job_configs (job_id, config) VALUES (?, ?)",
            ('test-job-1', '{"db_type": "postgresql", "db_host": "localhost"}')
        )
        
        # Insert a test table export
        cursor.execute(
            "INSERT INTO table_exports (job_id, table_name, status, start_time) VALUES (?, ?, ?, ?)",
            ('test-job-1', 'users', 'processing', '2024-01-01 10:01:00')
        )
        
        conn.commit()
        
        # Test retrieval
        cursor.execute("SELECT * FROM jobs WHERE job_id = ?", ('test-job-1',))
        job = cursor.fetchone()
        self.assertIsNotNone(job)
        self.assertEqual(job['job_id'], 'test-job-1')
        self.assertEqual(job['db_username'], 'testuser')
        
        cursor.execute("SELECT * FROM job_configs WHERE job_id = ?", ('test-job-1',))
        config = cursor.fetchone()
        self.assertIsNotNone(config)
        self.assertIn('postgresql', config['config'])
        
        cursor.execute("SELECT * FROM table_exports WHERE job_id = ?", ('test-job-1',))
        table_export = cursor.fetchone()
        self.assertIsNotNone(table_export)
        self.assertEqual(table_export['table_name'], 'users')
        self.assertEqual(table_export['status'], 'processing')
        
        conn.close()


if __name__ == '__main__':
    unittest.main()