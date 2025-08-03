import os
from adu.database import init_db

# This script is meant to be run once to initialize the database.
if __name__ == "__main__":
    print("Initializing database...")
    # I'll remove the existing database file to ensure a clean start
    db_file = '/tmp/adu.db'
    if os.path.exists(db_file):
        os.remove(db_file)
    init_db()
    print("Database initialized successfully.")
