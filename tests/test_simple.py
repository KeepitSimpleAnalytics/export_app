import unittest
import tempfile
import os
import sys
import json

# Add the adu directory to the path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'adu'))

# Simple tests that don't require external dependencies
from database import init_db, get_db_connection


class TestBasicFunctionality(unittest.TestCase):
    """Basic tests that don't require external dependencies"""
    
    def setUp(self):
        """Set up test environment"""
        # Create a temporary database for testing
        self.test_db = tempfile.NamedTemporaryFile(delete=False)
        self.test_db.close()
        
        # Store original DB_FILE value
        import database
        self.original_db_file = database.DB_FILE
        database.DB_FILE = self.test_db.name
    
    def tearDown(self):
        """Clean up test environment"""
        # Restore original DB_FILE value
        import database
        database.DB_FILE = self.original_db_file
        
        try:
            os.unlink(self.test_db.name)
        except FileNotFoundError:
            pass
    
    def test_database_initialization(self):
        """Test that database can be initialized"""
        init_db()
        self.assertTrue(os.path.exists(self.test_db.name))
        
        # Test connection
        conn = get_db_connection()
        self.assertIsNotNone(conn)
        
        # Test basic query
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        # Should have our expected tables
        table_names = [table[0] for table in tables]
        expected_tables = ['jobs', 'errors', 'job_configs', 'table_exports']
        
        for expected_table in expected_tables:
            self.assertIn(expected_table, table_names)
        
        conn.close()
    
    def test_sensitive_data_redaction_basic(self):
        """Test basic sensitive data redaction without importing worker"""
        # Import here to avoid dependency issues
        try:
            from worker import redact_sensitive_data
            
            # Test basic password redaction
            test_text = 'password: "secret123"'
            result = redact_sensitive_data(test_text)
            self.assertNotIn('secret123', result)
            self.assertIn('***REDACTED***', result)
            
            # Test non-sensitive text
            normal_text = "This is just normal log text"
            result = redact_sensitive_data(normal_text)
            self.assertEqual(result, normal_text)
            
        except ImportError:
            self.skipTest("Skipping worker tests due to missing dependencies")
    
    def test_flask_app_import(self):
        """Test that Flask app can be imported"""
        try:
            from app import app
            self.assertIsNotNone(app)
            self.assertEqual(app.name, 'app')
        except ImportError as e:
            self.skipTest(f"Skipping Flask test due to missing dependencies: {e}")
    
    def test_job_creation_logic(self):
        """Test basic job creation logic"""
        init_db()
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Test inserting a job
        job_id = 'test-job-123'
        cursor.execute(
            "INSERT INTO jobs (job_id, db_username, overall_status, start_time) VALUES (?, ?, ?, ?)",
            (job_id, 'testuser', 'queued', '2024-01-01 10:00:00')
        )
        conn.commit()
        
        # Test retrieving the job
        cursor.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,))
        job = cursor.fetchone()
        
        self.assertIsNotNone(job)
        self.assertEqual(job['job_id'], job_id)
        self.assertEqual(job['db_username'], 'testuser')
        self.assertEqual(job['overall_status'], 'queued')
        
        conn.close()
    
    def test_table_exports_functionality(self):
        """Test table exports database functionality"""
        init_db()
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Insert a job first
        job_id = 'test-job-456'
        cursor.execute(
            "INSERT INTO jobs (job_id, db_username, overall_status, start_time) VALUES (?, ?, ?, ?)",
            (job_id, 'testuser', 'running', '2024-01-01 10:00:00')
        )
        
        # Insert table export
        cursor.execute(
            "INSERT INTO table_exports (job_id, table_name, status, start_time) VALUES (?, ?, ?, ?)",
            (job_id, 'users', 'processing', '2024-01-01 10:01:00')
        )
        conn.commit()
        
        # Test retrieving table export
        cursor.execute("SELECT * FROM table_exports WHERE job_id = ?", (job_id,))
        table_export = cursor.fetchone()
        
        self.assertIsNotNone(table_export)
        self.assertEqual(table_export['job_id'], job_id)
        self.assertEqual(table_export['table_name'], 'users')
        self.assertEqual(table_export['status'], 'processing')
        
        # Test updating table export
        cursor.execute(
            "UPDATE table_exports SET status = ?, row_count = ?, end_time = ? WHERE job_id = ? AND table_name = ?",
            ('completed', 1000, '2024-01-01 10:05:00', job_id, 'users')
        )
        conn.commit()
        
        # Verify update
        cursor.execute("SELECT * FROM table_exports WHERE job_id = ? AND table_name = ?", (job_id, 'users'))
        updated_export = cursor.fetchone()
        
        self.assertEqual(updated_export['status'], 'completed')
        self.assertEqual(updated_export['row_count'], 1000)
        
        conn.close()


if __name__ == '__main__':
    unittest.main()