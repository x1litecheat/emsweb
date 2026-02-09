"""
Storage Adapter Layer - JSON-like Interface for MongoDB
This module provides a drop-in replacement for JSON file operations.
It maintains the exact same behavior as read_json() and write_json() 
but uses MongoDB as the backend storage.

The adapter ensures:
1. All data structures remain identical
2. All collections are automatically created
3. Seamless integration without changing existing code
4. Automatic data structure normalization
"""

from db import get_db, find_many, update_many, delete_many, ensure_collections
from typing import Dict, Any, List
from pymongo.errors import PyMongoError

# Mapping of file paths to MongoDB collections and their expected structures
FILE_TO_COLLECTION = {
    'data/users.json': ('users', 'users'),
    'data/time_entries.json': ('time_entries', 'entries'),
    'data/message.json': ('message', 'message_data'),
    'data/admins.json': ('admins', 'admins'),
    'data/admin_settings.json': ('admin_settings', 'settings')
}


def _get_collection_and_key(filename: str) -> tuple:
    """
    Map a filename to its MongoDB collection and data key.
    Returns: (collection_name, data_key_in_document)
    """
    if filename in FILE_TO_COLLECTION:
        return FILE_TO_COLLECTION[filename]
    raise ValueError(f"Unknown file: {filename}")


def _ensure_structure(collection_name: str, data_key: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure the data structure matches the expected format.
    Returns the normalized data structure for storage.
    """
    # Define expected structures for each collection
    structures = {
        'users': lambda d: {
            'users': d.get('users', []) if isinstance(d.get('users'), list) else []
        },
        'time_entries': lambda d: {
            'entries': d.get('entries', []) if isinstance(d.get('entries'), list) else []
        },
        'message': lambda d: {
            'message_data': d.get('message', '') if isinstance(d.get('message'), str) else ''
        },
        'admins': lambda d: {
            'admins': d.get('admins', []) if isinstance(d.get('admins'), list) else []
        },
        'admin_settings': lambda d: {
            'settings': d  # Store entire object as-is
        }
    }
    
    if collection_name in structures:
        return structures[collection_name](data)
    return data


def read_json(filename: str) -> Dict[str, Any]:
    """
    Read data from MongoDB and return it as if it came from a JSON file.
    
    This function:
    1. Looks up the MongoDB collection for the given filename
    2. Retrieves the document(s) from MongoDB
    3. Returns data in the exact same format as the original JSON files
    4. Returns empty dict if collection is empty (matching JSON file behavior)
    
    Args:
        filename: Path to the JSON file (e.g., 'data/users.json')
    
    Returns:
        Dictionary with the same structure as the original JSON file
    """
    try:
        collection_name, data_key = _get_collection_and_key(filename)
        db = get_db()
        collection = db[collection_name]
        
        # Try to find existing document
        document = collection.find_one({})
        
        if document is None:
            # Collection is empty, return empty structure
            if collection_name == 'users':
                return {'users': []}
            elif collection_name == 'time_entries':
                return {'entries': []}
            elif collection_name == 'message':
                return {'message': ''}
            elif collection_name == 'admins':
                return {'admins': []}
            elif collection_name == 'admin_settings':
                return {}
        
        # Remove MongoDB's internal _id field if present
        if '_id' in document:
            del document['_id']
        
        return document
    
    except Exception as e:
        print(f"Error reading from MongoDB collection: {e}")
        # Return empty structure on error (fail gracefully like missing JSON file)
        if 'users' in filename:
            return {'users': []}
        elif 'entries' in filename:
            return {'entries': []}
        elif 'message' in filename:
            return {'message': ''}
        elif 'admins' in filename:
            return {'admins': []}
        return {}


def write_json(filename: str, data: Dict[str, Any]) -> None:
    """
    Write data to MongoDB instead of a JSON file.
    
    This function:
    1. Normalizes the data structure
    2. Updates or inserts the document in MongoDB
    3. Preserves the exact data format
    4. Handles any data type that the JSON files would contain
    
    Args:
        filename: Path to the JSON file (e.g., 'data/users.json')
        data: Dictionary with the same structure as the JSON file
    
    Raises:
        ValueError: If the filename is not recognized
        Exception: If MongoDB operation fails
    """
    try:
        collection_name, data_key = _get_collection_and_key(filename)
        db = get_db()
        collection = db[collection_name]
        
        # Normalize the data structure
        normalized_data = _ensure_structure(collection_name, data_key, data)
        
        # Find and update, or insert if doesn't exist
        # Use upsert to handle both create and update in one operation
        result = collection.update_one(
            {},  # Empty filter matches the first document
            {'$set': normalized_data},
            upsert=True  # Create if doesn't exist
        )
        
        if result.matched_count > 0:
            print(f"✓ Updated {collection_name} collection")
        else:
            print(f"✓ Inserted new document in {collection_name} collection")
    
    except Exception as e:
        print(f"Error writing to MongoDB collection: {e}")
        raise RuntimeError(f"Failed to write to {filename}: {e}")


def ensure_file_exists(filename: str) -> None:
    """
    Ensure a 'file' (collection) exists with proper structure.
    This is equivalent to creating an empty JSON file.
    
    Args:
        filename: Path to the JSON file
    """
    try:
        collection_name, data_key = _get_collection_and_key(filename)
        db = get_db()
        
        # Check if collection has data
        if db[collection_name].count_documents({}) == 0:
            # Create with empty structure
            if collection_name == 'users':
                write_json(filename, {'users': []})
            elif collection_name == 'time_entries':
                write_json(filename, {'entries': []})
            elif collection_name == 'message':
                write_json(filename, {'message': ''})
            elif collection_name == 'admins':
                write_json(filename, {'admins': []})
            elif collection_name == 'admin_settings':
                write_json(filename, {})
    except Exception as e:
        print(f"Error ensuring file exists: {e}")


def file_exists(filename: str) -> bool:
    """
    Check if a 'file' (collection) has data.
    
    Args:
        filename: Path to the JSON file
    
    Returns:
        True if collection has documents, False otherwise
    """
    try:
        collection_name, _ = _get_collection_and_key(filename)
        db = get_db()
        return db[collection_name].count_documents({}) > 0
    except:
        return False


def initialize_storage():
    """
    Initialize all collections and ensure they exist.
    This should be called once at application startup.
    """
    try:
        ensure_collections()
        
        # Ensure all collections have proper structure
        for filename in FILE_TO_COLLECTION.keys():
            ensure_file_exists(filename)
        
        print("✓ Storage adapter initialized successfully")
    except Exception as e:
        print(f"✗ Error initializing storage: {e}")
        raise RuntimeError(f"Failed to initialize storage: {e}")
