"""
Virtual Storage Migration Script
=================================
This script migrates existing JSON files to MongoDB for use with the
virtual filesystem. Run this ONCE before activating virtual storage.

It reads all JSON files from data/ directory and imports them into MongoDB.
"""

import json
import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment
load_dotenv()

# File to collection mapping
FILE_TO_COLLECTION = {
    'data/users.json': 'users',
    'data/admins.json': 'admins',
    'data/time_entries.json': 'time_entries',
    'data/message.json': 'message',
    'data/admin_settings.json': 'admin_settings',
}

def migrate_json_to_mongodb():
    """
    Migrate all JSON files to MongoDB collections.
    """
    print("\n" + "="*70)
    print("JSON TO MONGODB MIGRATION (for Virtual Filesystem)")
    print("="*70 + "\n")
    
    # Connect to MongoDB
    uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
    db_name = os.getenv('MONGODB_DB_NAME', 'ems_database')
    
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client[db_name]
        print(f"✓ Connected to MongoDB: {db_name}\n")
    except Exception as e:
        print(f"✗ Failed to connect to MongoDB: {e}")
        return False
    
    # Check if collections already have data
    has_data = False
    for collection_name in FILE_TO_COLLECTION.values():
        if db[collection_name].count_documents({}) > 0:
            has_data = True
            print(f"⚠ Collection '{collection_name}' already has data")
    
    if has_data:
        response = input("\n⚠ Some collections have data. Overwrite? (yes/no): ")
        if response.lower() != 'yes':
            print("✗ Migration cancelled")
            return False
        print("")
    
    # Migrate each file
    success_count = 0
    total_files = len(FILE_TO_COLLECTION)
    
    for file_path, collection_name in FILE_TO_COLLECTION.items():
        print(f"Migrating: {file_path} → {collection_name}")
        
        # Check if file exists
        if not os.path.exists(file_path):
            print(f"  ⚠ File not found, skipping\n")
            continue
        
        try:
            # Read JSON file
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Clear existing data in collection
            db[collection_name].delete_many({})
            
            # Insert data into MongoDB
            db[collection_name].insert_one(data)
            
            # Count records
            record_count = 0
            if isinstance(data, dict):
                for value in data.values():
                    if isinstance(value, list):
                        record_count += len(value)
            
            print(f"  ✓ Success! Imported {record_count} records\n")
            success_count += 1
            
        except Exception as e:
            print(f"  ✗ Error: {e}\n")
    
    # Summary
    print("="*70)
    print(f"Migration complete: {success_count}/{total_files} files migrated")
    print("="*70)
    
    if success_count == total_files:
        print("\n✅ All files migrated successfully!")
        print("\nNext steps:")
        print("1. Update app.py to activate virtual storage (see instructions)")
        print("2. Start your application: python app.py")
        print("3. Application will now use MongoDB via virtual filesystem")
        print("\nJSON files in data/ folder can be deleted (backup first if needed)")
        return True
    else:
        print(f"\n⚠ {total_files - success_count} files failed to migrate")
        return False

if __name__ == '__main__':
    try:
        success = migrate_json_to_mongodb()
        exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n✗ Migration cancelled by user")
        exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        exit(1)
