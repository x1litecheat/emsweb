"""
MongoDB Database Connection and Management Layer
This module handles all MongoDB connections and operations.
It provides a simple interface for the storage adapter to use.
"""

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import os
from dotenv import load_dotenv
from typing import Dict, Any, Optional, List

# Load environment variables from .env file
load_dotenv()

class MongoDBConnection:
    """Singleton class to manage MongoDB connection"""
    _instance = None
    _client = None
    _db = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoDBConnection, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize MongoDB connection"""
        if self._client is None:
            mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
            db_name = os.getenv('MONGODB_DB_NAME', 'ems_database')
            
            try:
                self._client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000)
                # Verify connection
                self._client.admin.command('ping')
                self._db = self._client[db_name]
                print(f"✓ Connected to MongoDB: {db_name}")
            except (ConnectionFailure, ServerSelectionTimeoutError) as e:
                print(f"✗ MongoDB connection failed: {e}")
                raise RuntimeError(f"Cannot connect to MongoDB at {mongodb_uri}. Make sure MongoDB is running.")

    def get_db(self):
        """Get database instance"""
        if self._db is None:
            raise RuntimeError("Database not initialized")
        return self._db

    def close(self):
        """Close the MongoDB connection"""
        if self._client:
            self._client.close()
            self._client = None
            self._db = None


def get_db():
    """Get database instance (helper function)"""
    return MongoDBConnection().get_db()


def ensure_collections():
    """Ensure all required collections exist"""
    db = get_db()
    required_collections = [
        'admin_settings',
        'admins',
        'message',
        'time_entries',
        'users'
    ]
    
    for collection_name in required_collections:
        if collection_name not in db.list_collection_names():
            db.create_collection(collection_name)
            print(f"✓ Created collection: {collection_name}")
        else:
            print(f"✓ Collection already exists: {collection_name}")


def insert_one(collection_name: str, document: Dict[str, Any]) -> Any:
    """Insert a single document"""
    db = get_db()
    result = db[collection_name].insert_one(document)
    return result.inserted_id


def insert_many(collection_name: str, documents: List[Dict[str, Any]]) -> List[Any]:
    """Insert multiple documents"""
    db = get_db()
    result = db[collection_name].insert_many(documents)
    return result.inserted_ids


def find_one(collection_name: str, query: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Find a single document"""
    db = get_db()
    return db[collection_name].find_one(query)


def find_many(collection_name: str, query: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """Find multiple documents"""
    db = get_db()
    if query is None:
        query = {}
    return list(db[collection_name].find(query))


def update_one(collection_name: str, query: Dict[str, Any], update: Dict[str, Any]) -> int:
    """Update a single document"""
    db = get_db()
    result = db[collection_name].update_one(query, {'$set': update})
    return result.modified_count


def update_many(collection_name: str, query: Dict[str, Any], update: Dict[str, Any]) -> int:
    """Update multiple documents"""
    db = get_db()
    result = db[collection_name].update_many(query, {'$set': update})
    return result.modified_count


def delete_one(collection_name: str, query: Dict[str, Any]) -> int:
    """Delete a single document"""
    db = get_db()
    result = db[collection_name].delete_one(query)
    return result.deleted_count


def delete_many(collection_name: str, query: Dict[str, Any]) -> int:
    """Delete multiple documents"""
    db = get_db()
    result = db[collection_name].delete_many(query)
    return result.deleted_count


def count_documents(collection_name: str, query: Dict[str, Any] = None) -> int:
    """Count documents matching query"""
    db = get_db()
    if query is None:
        query = {}
    return db[collection_name].count_documents(query)


def clear_collection(collection_name: str) -> int:
    """Clear all documents from a collection"""
    db = get_db()
    result = db[collection_name].delete_many({})
    return result.deleted_count


def drop_collection(collection_name: str) -> None:
    """Drop an entire collection"""
    db = get_db()
    db[collection_name].drop()
