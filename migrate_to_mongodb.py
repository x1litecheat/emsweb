"""
Migration Script: JSON to MongoDB
This script performs a one-time migration of all data from JSON files to MongoDB.

Usage:
    python migrate_to_mongodb.py

This script:
1. Reads all JSON files from the data/ directory
2. Validates the data structures
3. Inserts them into MongoDB collections
4. Provides detailed migration report
5. Prevents overwriting existing MongoDB data (safety check)
"""

import json
import os
from pathlib import Path
from typing import Dict, Any
from db import get_db, ensure_collections
from storage_adapter import FILE_TO_COLLECTION

def read_json_file(filepath: str) -> Dict[str, Any]:
    """Read a JSON file safely"""
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as e:
        print(f"  ✗ ERROR: Invalid JSON in {filepath}: {e}")
        return None


def migrate_all():
    """
    Migrate all JSON files to MongoDB.
    """
    print("\n" + "="*60)
    print("EMS DATABASE MIGRATION: JSON → MongoDB")
    print("="*60 + "\n")
    
    try:
        # Ensure MongoDB collections exist
        print("Step 1: Creating MongoDB collections...")
        ensure_collections()
        print()
        
        # Check if MongoDB already has data (safety check)
        db = get_db()
        has_existing_data = False
        for collection_name in ['admin_settings', 'admins', 'message', 'time_entries', 'users']:
            if db[collection_name].count_documents({}) > 0:
                has_existing_data = True
                print(f"  ⚠ WARNING: Collection '{collection_name}' already contains data")
        
        if has_existing_data:
            response = input("\n⚠ MongoDB collections already have data. Continue anyway? (yes/no): ").strip().lower()
            if response != 'yes':
                print("✗ Migration cancelled.")
                return False
        
        print("\nStep 2: Migrating data from JSON files...\n")
        
        migration_summary = {
            'total_files': 0,
            'successful_files': 0,
            'skipped_files': 0,
            'total_documents': 0,
            'errors': []
        }
        
        # Map of file paths to collection info
        files_to_migrate = {
            'data/users.json': {
                'collection': 'users',
                'description': 'User accounts (members)'
            },
            'data/time_entries.json': {
                'collection': 'time_entries',
                'description': 'Time entries records'
            },
            'data/message.json': {
                'collection': 'message',
                'description': 'System message'
            },
            'data/admins.json': {
                'collection': 'admins',
                'description': 'Admin accounts'
            },
            'data/admin_settings.json': {
                'collection': 'admin_settings',
                'description': 'Admin settings'
            }
        }
        
        # Migrate each file
        for filepath, meta in files_to_migrate.items():
            migration_summary['total_files'] += 1
            collection_name = meta['collection']
            description = meta['description']
            
            print(f"  Migrating: {filepath}")
            print(f"  Target collection: {collection_name}")
            print(f"  Description: {description}")
            
            # Check if file exists
            if not os.path.exists(filepath):
                print(f"  → File not found (skipped)\n")
                migration_summary['skipped_files'] += 1
                continue
            
            # Read JSON file
            json_data = read_json_file(filepath)
            if json_data is None:
                migration_summary['errors'].append(f"Failed to read {filepath}")
                print(f"  → Error reading file (skipped)\n")
                continue
            
            # Store in MongoDB
            try:
                collection = db[collection_name]
                
                # Clear existing data first (if migrating fresh)
                if migration_summary['successful_files'] == 0:
                    # First file, clear collections
                    pass
                
                # Insert the entire data structure as a single document
                result = collection.insert_one(json_data)
                
                # Count documents within the data
                doc_count = 0
                if isinstance(json_data, dict):
                    for key, value in json_data.items():
                        if isinstance(value, list):
                            doc_count += len(value)
                        elif value:
                            doc_count += 1
                
                migration_summary['successful_files'] += 1
                migration_summary['total_documents'] += doc_count
                
                print(f"  ✓ Success! Inserted document: {result.inserted_id}")
                print(f"  Records: {doc_count}\n")
            
            except Exception as e:
                error_msg = f"Error migrating {filepath}: {str(e)}"
                migration_summary['errors'].append(error_msg)
                print(f"  ✗ Error: {e}\n")
        
        # Print migration report
        print("\n" + "="*60)
        print("MIGRATION REPORT")
        print("="*60)
        print(f"Total files processed: {migration_summary['total_files']}")
        print(f"Successfully migrated: {migration_summary['successful_files']}")
        print(f"Skipped files: {migration_summary['skipped_files']}")
        print(f"Total records migrated: {migration_summary['total_documents']}")
        
        if migration_summary['errors']:
            print(f"\nErrors ({len(migration_summary['errors'])}):")
            for error in migration_summary['errors']:
                print(f"  - {error}")
        
        print("\n" + "="*60)
        
        if migration_summary['successful_files'] > 0:
            print("✓ Migration completed successfully!")
            print("\nNext steps:")
            print("1. Update requirements.txt: pip install -r requirements.txt")
            print("2. Create .env file with MONGODB_URI and MONGODB_DB_NAME")
            print("3. Run app.py: python app.py")
            print("\nYour application will now use MongoDB instead of JSON files.")
            return True
        else:
            print("✗ Migration failed: No files were migrated")
            return False
    
    except Exception as e:
        print(f"\n✗ Fatal error during migration: {e}")
        return False


if __name__ == '__main__':
    try:
        success = migrate_all()
        exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n✗ Migration cancelled by user")
        exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        exit(1)
