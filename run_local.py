#!/usr/bin/env python3
"""
Local development server for ADU
Run this script to test the application without Docker
"""

import os
import sys
import subprocess
from pathlib import Path

def check_requirements():
    """Check if required packages are installed"""
    try:
        import flask
        import polars
        import psycopg2
        import pandera
        import cryptography
        print("✅ All required packages are installed")
        return True
    except ImportError as e:
        print(f"❌ Missing package: {e}")
        return False

def setup_environment():
    """Setup environment variables for local development"""
    if not os.getenv('FERNET_KEY'):
        # Generate a temporary key for development
        from cryptography.fernet import Fernet
        key = Fernet.generate_key().decode()
        os.environ['FERNET_KEY'] = key
        print(f"🔑 Generated temporary Fernet key: {key}")
        print("   Save this key if you want to persist encrypted data")
    
    # Set development environment
    os.environ['FLASK_ENV'] = 'development'
    os.environ['FLASK_DEBUG'] = '1'
    
    # Create required directories
    for dir_name in ['data', 'logs', 'exports']:
        dir_path = Path('adu') / dir_name
        dir_path.mkdir(exist_ok=True)
        print(f"📁 Created directory: {dir_path}")

def initialize_database():
    """Initialize the SQLite database"""
    try:
        print("🗄️  Initializing database...")
        result = subprocess.run([sys.executable, 'init_database.py'], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Database initialized successfully")
        else:
            print(f"❌ Database initialization failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Error initializing database: {e}")
        return False
    return True

def start_server():
    """Start the Flask development server"""
    try:
        print("🚀 Starting ADU development server...")
        print("📍 Server will be available at: http://localhost:5000")
        print("🛑 Press Ctrl+C to stop the server")
        print("-" * 50)
        
        # Start the Flask app
        subprocess.run([sys.executable, 'adu/app.py'])
        
    except KeyboardInterrupt:
        print("\n🛑 Server stopped by user")
    except Exception as e:
        print(f"❌ Error starting server: {e}")

def main():
    """Main function to setup and start the development environment"""
    print("🔧 ADU Local Development Setup")
    print("=" * 40)
    
    # Check if we're in the right directory
    if not Path('adu/app.py').exists():
        print("❌ Error: adu/app.py not found.")
        print("   Please run this script from the export_app directory.")
        return 1
    
    # Check requirements
    if not check_requirements():
        print("\n📦 Installing requirements...")
        try:
            subprocess.run([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'], 
                         check=True)
            print("✅ Requirements installed successfully")
        except subprocess.CalledProcessError:
            print("❌ Failed to install requirements")
            return 1
    
    # Setup environment
    setup_environment()
    
    # Initialize database
    if not initialize_database():
        return 1
    
    # Start server
    start_server()
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
