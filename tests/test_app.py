import unittest
import json
import tempfile
import os
import sys
from unittest.mock import patch, MagicMock

# Add the adu directory to the path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'adu'))

from app import app
from database import init_db, get_db_connection


class TestFlaskApp(unittest.TestCase):
    """Test cases for Flask application"""
    
    def setUp(self):
        """Set up test environment"""
        # Create a temporary database for testing
        self.test_db = tempfile.NamedTemporaryFile(delete=False)
        self.test_db.close()
        
        # Patch the database file path
        self.db_patcher = patch('adu.database.DB_FILE', self.test_db.name)
        self.db_patcher.start()
        
        # Initialize test database
        init_db()
        
        # Configure Flask app for testing
        app.config['TESTING'] = True
        app.config['SECRET_KEY'] = 'test-secret-key'
        self.client = app.test_client()
        
        # Disable CSRF for testing
        app.config['WTF_CSRF_ENABLED'] = False
    
    def tearDown(self):
        """Clean up test environment"""
        # Clean up the database
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM jobs")
        cursor.execute("DELETE FROM job_configs")
        cursor.execute("DELETE FROM table_exports")
        cursor.execute("DELETE FROM errors")
        conn.commit()
        conn.close()

        self.db_patcher.stop()
        try:
            os.unlink(self.test_db.name)
        except FileNotFoundError:
            pass
    
    def test_index_page(self):
        """Test main index page loads"""
        response = self.client.get('/')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Air-gapped Data Utility', response.data)
    
    def test_history_page(self):
        """Test history page loads"""
        response = self.client.get('/history')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Job History', response.data)
    
    def test_job_details_page(self):
        """Test job details page loads"""
        response = self.client.get('/job/test-job-id')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Job Details', response.data)
    
    def test_logs_page(self):
        """Test logs page loads"""
        response = self.client.get('/logs')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Worker Logs', response.data)
    
    def test_api_history_empty(self):
        """Test API history endpoint with no jobs"""
        response = self.client.get('/api/history')
        self.assertEqual(response.status_code, 200)
        
        data = json.loads(response.data)
        self.assertEqual(data, [])
    
    def test_api_job_not_found(self):
        """Test API job endpoint with non-existent job"""
        response = self.client.get('/api/job/nonexistent')
        self.assertEqual(response.status_code, 404)
    
    def test_api_job_config_not_found(self):
        """Test API job config endpoint with non-existent job"""
        response = self.client.get('/api/job/nonexistent/config')
        self.assertEqual(response.status_code, 404)
    
    def test_api_job_tables_empty(self):
        """Test API job tables endpoint with non-existent job"""
        response = self.client.get('/api/job/nonexistent/tables')
        self.assertEqual(response.status_code, 200)
        
        data = json.loads(response.data)
        self.assertEqual(data, [])
    
    def test_api_job_errors_empty(self):
        """Test API job errors endpoint with non-existent job"""
        response = self.client.get('/api/job/nonexistent/errors')
        self.assertEqual(response.status_code, 200)
        
        data = json.loads(response.data)
        self.assertEqual(data, [])
    
    @patch('os.path.exists')
    def test_api_logs_worker_not_found(self, mock_exists):
        """Test API worker logs endpoint when log file doesn't exist"""
        mock_exists.return_value = False
        
        response = self.client.get('/api/logs/worker')
        self.assertEqual(response.status_code, 404)
        
        data = json.loads(response.data)
        self.assertIn('error', data)
        self.assertIn('Log file not found', data['error'])
    
    def test_create_job_missing_data(self):
        """Test creating job with missing data"""
        response = self.client.post('/api/jobs', 
                                  json={},
                                  content_type='application/json')
        # Should return 400 or 500 due to missing required fields
        self.assertIn(response.status_code, [400, 500])
    
    def test_create_job_valid_data(self):
        """Test creating job with valid data"""
        job_data = {
            'db_type': 'postgresql',
            'db_host': 'localhost',
            'db_port': '5432',
            'db_name': 'testdb',
            'db_username': 'testuser',
            'db_password': 'testpass',
            'tables': ['users', 'orders'],
            'output_path': '/tmp/test_exports'
        }
        
        response = self.client.post('/api/jobs',
                                  json=job_data,
                                  content_type='application/json')
        self.assertEqual(response.status_code, 200)
        
        data = json.loads(response.data)
        self.assertIn('job_id', data)
        self.assertIn('message', data)


class TestApiIntegration(unittest.TestCase):
    """Integration tests for API endpoints"""
    
    def setUp(self):
        """Set up test environment"""
        # Create a temporary database for testing
        self.test_db = tempfile.NamedTemporaryFile(delete=False)
        self.test_db.close()
        
        # Patch the database file path
        self.db_patcher = patch('adu.database.DB_FILE', self.test_db.name)
        self.db_patcher.start()
        
        # Initialize test database
        init_db()
        
        # Configure Flask app for testing
        app.config['TESTING'] = True
        app.config['SECRET_KEY'] = 'test-secret-key'
        self.client = app.test_client()
        
        # Disable CSRF for testing
        app.config['WTF_CSRF_ENABLED'] = False
    
    def tearDown(self):
        """Clean up test environment"""
        # Clean up the database
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM jobs")
        cursor.execute("DELETE FROM job_configs")
        cursor.execute("DELETE FROM table_exports")
        cursor.execute("DELETE FROM errors")
        conn.commit()
        conn.close()

        self.db_patcher.stop()
        try:
            os.unlink(self.test_db.name)
        except FileNotFoundError:
            pass
    
    def test_create_and_retrieve_job(self):
        """Test creating a job and then retrieving it"""
        # Create a job
        job_data = {
            'db_type': 'postgresql',
            'db_host': 'localhost',
            'db_port': '5432',
            'db_name': 'testdb',
            'db_username': 'testuser',
            'db_password': 'testpass',
            'tables': ['users'],
            'output_path': '/tmp/test_exports'
        }
        
        create_response = self.client.post('/api/jobs',
                                         json=job_data,
                                         content_type='application/json')
        self.assertEqual(create_response.status_code, 200)
        
        create_data = json.loads(create_response.data)
        job_id = create_data['job_id']
        
        # Retrieve the job from history
        history_response = self.client.get('/api/history')
        self.assertEqual(history_response.status_code, 200)
        
        history_data = json.loads(history_response.data)
        self.assertEqual(len(history_data), 1)
        self.assertEqual(history_data[0]['job_id'], job_id)
        
        # Retrieve job configuration
        config_response = self.client.get(f'/api/job/{job_id}/config')
        self.assertEqual(config_response.status_code, 200)
        
        config_data = json.loads(config_response.data)
        self.assertEqual(config_data['db_type'], 'postgresql')
        self.assertEqual(config_data['db_host'], 'localhost')
        self.assertEqual(config_data['db_password'], '***ENCRYPTED***')  # Should be redacted


if __name__ == '__main__':
    unittest.main()