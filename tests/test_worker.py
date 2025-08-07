import unittest
import tempfile
import os
import sys
import json
from unittest.mock import patch, MagicMock

# Add the adu directory to the path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'adu'))

from worker import redact_sensitive_data, create_basic_schema, validate_data
import polars as pl


class TestWorker(unittest.TestCase):
    """Test cases for worker functionality"""
    
    def test_redact_sensitive_data(self):
        """Test sensitive data redaction"""
        test_cases = [
            ('password: "secret123"', 'password: "***REDACTED***"'),
            ('{"password":"mysecret"}', '{"password":"***REDACTED***"}'),
            ("The password=secret", "The password=***REDACTED***"),
            ('postgresql://user:password@host', 'postgresql://user:***REDACTED***@host'),
            ('vertica://admin:secret@host', 'vertica://admin:***REDACTED***@host'),
            ('api_key: "abc123def"', 'api_key: "***REDACTED***"'),
        ]

        for original, expected in test_cases:
            with self.subTest(original=original):
                result = redact_sensitive_data(original)
                self.assertEqual(result, expected)
    
    def test_redact_encrypted_password(self):
        """Test redaction of encrypted passwords"""
        encrypted_value = "gAAAAABhZ1234567890ABCDEFGHIJKLMNOPqrstuvwxyz"
        text = f"Encrypted password: {encrypted_value}"
        
        result = redact_sensitive_data(text)
        self.assertIn('***ENCRYPTED_PASSWORD***', result)
        self.assertNotIn(encrypted_value, result)
    
    def test_create_basic_schema(self):
        """Test basic schema creation for data validation"""
        # Create a test DataFrame
        df = pl.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35],
            'salary': [50000.0, 60000.0, 70000.0],
            'is_active': [True, False, True]
        })
        
        schema = create_basic_schema(df)
        self.assertIsNotNone(schema)
        
        # Test that the schema has the expected columns
        self.assertIn('id', schema.columns)
        self.assertIn('name', schema.columns)
        self.assertIn('age', schema.columns)
        self.assertIn('salary', schema.columns)
        self.assertIn('is_active', schema.columns)
    
    def test_validate_data_success(self):
        """Test successful data validation"""
        # Create a valid DataFrame
        df = pl.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })
        
        success, message = validate_data(df, 'test_table')
        self.assertTrue(success)
        self.assertIn('Validation passed', message)
    
    def test_validate_data_with_nulls(self):
        """Test data validation with null values"""
        # Create DataFrame with null values
        df = pl.DataFrame({
            'id': [1, None, 3],
            'name': ['Alice', None, 'Charlie'],
            'age': [25, 30, None]
        })
        
        success, message = validate_data(df, 'test_table_nulls')
        # Should pass since our schema allows nulls
        self.assertTrue(success)
    
    def test_redact_sensitive_data_non_string(self):
        """Test redaction with non-string input"""
        result = redact_sensitive_data(12345)
        self.assertEqual(result, '12345')
        
        result = redact_sensitive_data(None)
        self.assertEqual(result, 'None')


class TestDataValidation(unittest.TestCase):
    """Test cases for data validation functionality"""
    
    def test_empty_dataframe(self):
        """Test validation with empty DataFrame"""
        df = pl.DataFrame({})
        success, message = validate_data(df, 'empty_table')
        # Should handle empty DataFrames gracefully
        self.assertIsInstance(success, bool)
        self.assertIsInstance(message, str)
    
    def test_large_dataframe(self):
        """Test validation with large DataFrame"""
        # Create a larger DataFrame to test performance
        import random
        
        data = {
            'id': list(range(1000)),
            'name': [f'User_{i}' for i in range(1000)],
            'value': [random.random() for _ in range(1000)]
        }
        df = pl.DataFrame(data)
        
        success, message = validate_data(df, 'large_table')
        self.assertTrue(success)
        self.assertIn('1000 rows validated', message)


if __name__ == '__main__':
    unittest.main()