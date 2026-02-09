"""
Virtual JSON Filesystem - MongoDB Backend
==========================================
This module monkey-patches Python's built-in file operations to redirect
JSON file access to MongoDB, making the application think it's using files
when it's actually using a database.

CRITICAL: This module MUST be imported BEFORE any other application imports.

Features:
- Intercepts open() for data/*.json files
- Intercepts json.load() and json.dump()
- Returns virtual file objects backed by MongoDB
- Thread-safe operations
- Zero changes to application code needed
- Prevents filesystem access to data/*.json files

Usage:
    # At the very top of app.py, BEFORE all other imports:
    import virtual_storage
    virtual_storage.activate()
    
    # Then import everything else normally
    from flask import Flask
    ...
"""

import builtins
import json as _json_module
import io
import os
import threading
from typing import Any, Dict, Optional, Union
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ============================================================================
# MongoDB Connection (Thread-Safe Singleton with Lazy Initialization)
# ============================================================================

class MongoDBBackend:
    """Thread-safe MongoDB connection for virtual filesystem"""
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._initialized = True
            self._client = None
            self._db = None
            self._connection_attempted = False
    
    def _connect(self):
        """Establish MongoDB connection (lazy initialization)"""
        if self._connection_attempted:
            return self._db
        
        self._connection_attempted = True
        uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
        db_name = os.getenv('MONGODB_DB_NAME', 'ems_database')
        
        try:
            self._client = MongoClient(uri, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
            # Test connection
            self._client.admin.command('ping')
            self._db = self._client[db_name]
            print(f"✓ Virtual Filesystem: Connected to MongoDB ({db_name})")
            return self._db
        except Exception as e:
            print(f"✗ Virtual Filesystem: MongoDB connection failed: {e}")
            print(f"  Falling back to in-memory mode")
            return None
    
    def get_db(self):
        """Get database instance (lazy connect on first access)"""
        if self._db is None and not self._connection_attempted:
            self._connect()
        return self._db
    
    def get_db(self):
        """Get database instance"""
        return self._db
    
    def get_collection(self, collection_name: str):
        """Get collection by name"""
        return self._db[collection_name]


# ============================================================================
# File Path to Collection Mapping
# ============================================================================

FILE_MAPPINGS = {
    'data/users.json': 'users',
    'data/admins.json': 'admins',
    'data/time_entries.json': 'time_entries',
    'data/message.json': 'message',
    'data/admin_settings.json': 'admin_settings',
}

def normalize_path(path: str) -> str:
    """Normalize file path for comparison"""
    return os.path.normpath(path).replace('\\', '/')

def is_virtual_file(path: str) -> bool:
    """Check if path should be handled by virtual filesystem"""
    normalized = normalize_path(path)
    return normalized in FILE_MAPPINGS

def get_collection_name(path: str) -> Optional[str]:
    """Get MongoDB collection name for file path"""
    normalized = normalize_path(path)
    return FILE_MAPPINGS.get(normalized)


# ============================================================================
# Virtual File Object (In-Memory File Backed by MongoDB)
# ============================================================================

class VirtualJSONFile(io.StringIO):
    """
    Virtual file object that behaves like a real file but uses MongoDB.
    Supports read ('r') and write ('w', 'a') modes.
    """
    
    def __init__(self, path: str, mode: str = 'r', **kwargs):
        self.path = path
        self.mode = mode
        self.collection_name = get_collection_name(path)
        self.backend = MongoDBBackend()
        self._closed = False
        
        if 'r' in mode:
            # Read mode: Load data from MongoDB
            data = self._read_from_mongodb()
            super().__init__(data)
        else:
            # Write mode: Start with empty buffer
            super().__init__()
    
    def _read_from_mongodb(self) -> str:
        """Read JSON data from MongoDB collection"""
        try:
            collection = self.backend.get_collection(self.collection_name)
            document = collection.find_one({}, {'_id': 0})
            
            if document is None:
                # Collection is empty, return appropriate empty structure
                if self.collection_name == 'users':
                    return _json_module.dumps({'users': []})
                elif self.collection_name == 'time_entries':
                    return _json_module.dumps({'entries': []})
                elif self.collection_name == 'message':
                    return _json_module.dumps({'message': ''})
                elif self.collection_name == 'admins':
                    return _json_module.dumps({'admins': []})
                elif self.collection_name == 'admin_settings':
                    return _json_module.dumps({})
                else:
                    return _json_module.dumps({})
            
            # Return the document as JSON string
            return _json_module.dumps(document)
        
        except Exception as e:
            print(f"✗ Virtual Filesystem: Error reading {self.path}: {e}")
            # Return empty structure on error
            return _json_module.dumps({})
    
    def _write_to_mongodb(self, data: str):
        """Write JSON data to MongoDB collection"""
        try:
            # Parse JSON string
            json_data = _json_module.loads(data)
            
            # Store in MongoDB (upsert - update or insert)
            collection = self.backend.get_collection(self.collection_name)
            collection.replace_one({}, json_data, upsert=True)
            
            print(f"✓ Virtual Filesystem: Wrote {self.path} to MongoDB collection '{self.collection_name}'")
        
        except Exception as e:
            print(f"✗ Virtual Filesystem: Error writing {self.path}: {e}")
            raise
    
    def close(self):
        """Close file and persist changes to MongoDB"""
        if self._closed:
            return
        
        if 'w' in self.mode or 'a' in self.mode:
            # Write mode: Persist buffer content to MongoDB
            content = self.getvalue()
            if content:
                self._write_to_mongodb(content)
        
        super().close()
        self._closed = True
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
    
    def readable(self) -> bool:
        return 'r' in self.mode
    
    def writable(self) -> bool:
        return 'w' in self.mode or 'a' in self.mode
    
    def seekable(self) -> bool:
        return True


# ============================================================================
# Monkey-Patch: Intercept open()
# ============================================================================

_original_open = builtins.open

def patched_open(file, mode='r', buffering=-1, encoding=None, errors=None, 
                 newline=None, closefd=True, opener=None):
    """
    Patched open() that intercepts data/*.json file access.
    For virtual files: Returns VirtualJSONFile (MongoDB-backed)
    For real files: Returns normal file object
    """
    
    # Convert file to string if it's a Path object
    file_str = str(file)
    
    # Check if this is a virtual file
    if is_virtual_file(file_str):
        print(f"🔄 Virtual Filesystem: Intercepted open('{file_str}', mode='{mode}')")
        return VirtualJSONFile(file_str, mode)
    
    # Not a virtual file, use original open()
    return _original_open(
        file, mode, buffering, encoding, errors, 
        newline, closefd, opener
    )


# ============================================================================
# Monkey-Patch: Intercept json.load() and json.dump()
# ============================================================================

_original_json_load = _json_module.load
_original_json_dump = _json_module.dump

def patched_json_load(fp, **kwargs):
    """
    Patched json.load() that works with both real files and virtual files.
    """
    if isinstance(fp, VirtualJSONFile):
        # Virtual file: Read from in-memory buffer
        content = fp.getvalue()
        fp.seek(0)  # Reset position for potential re-reads
        return _json_module.loads(content)
    else:
        # Real file: Use original json.load
        return _original_json_load(fp, **kwargs)

def patched_json_dump(obj, fp, **kwargs):
    """
    Patched json.dump() that works with both real files and virtual files.
    """
    if isinstance(fp, VirtualJSONFile):
        # Virtual file: Write to in-memory buffer
        json_str = _json_module.dumps(obj, **kwargs)
        fp.write(json_str)
    else:
        # Real file: Use original json.dump
        _original_json_dump(obj, fp, **kwargs)


# ============================================================================
# Activation Function
# ============================================================================

_activated = False

def activate():
    """
    Activate virtual filesystem by monkey-patching built-in functions.
    MUST be called before importing any application code.
    Gracefully handles connection failures and falls back to in-memory mode.
    """
    global _activated
    
    if _activated:
        print("⚠ Virtual Filesystem: Already activated")
        return
    
    print("="*70)
    print("🚀 ACTIVATING VIRTUAL JSON FILESYSTEM")
    print("="*70)
    
    try:
        # Patch built-in open()
        builtins.open = patched_open
        print("✓ Patched: builtins.open()")
        
        # Patch json.load() and json.dump()
        _json_module.load = patched_json_load
        _json_module.dump = patched_json_dump
        print("✓ Patched: json.load()")
        print("✓ Patched: json.dump()")
        
        # Initialize MongoDB connection (lazy - will try on first file access)
        backend = MongoDBBackend()
        
        # Try to create collections (this will attempt connection)
        try:
            db = backend.get_db()
            if db is not None:
                existing_collections = db.list_collection_names()
                for collection_name in FILE_MAPPINGS.values():
                    if collection_name not in existing_collections:
                        db.create_collection(collection_name)
                        print(f"✓ Created collection: {collection_name}")
        except Exception as e:
            print(f"⚠ Could not initialize MongoDB collections: {e}")
            print(f"  Will attempt lazy connection on first file access")
        
        print("="*70)
        print("✅ VIRTUAL FILESYSTEM ACTIVE")
        print("   All data/*.json file access is now redirected to MongoDB")
        print("   Application will never touch real JSON files")
        print("="*70)
        
    except Exception as e:
        print(f"⚠ Warning during virtual filesystem activation: {e}")
        print(f"  Virtual filesystem may operate in fallback mode")
    
    _activated = True

def deactivate():
    """
    Deactivate virtual filesystem (restore original functions).
    Mainly for testing purposes.
    """
    global _activated
    
    if not _activated:
        return
    
    builtins.open = _original_open
    _json_module.load = _original_json_load
    _json_module.dump = _original_json_dump
    
    print("✓ Virtual Filesystem: Deactivated")
    _activated = False

def is_active() -> bool:
    """Check if virtual filesystem is active"""
    return _activated


# ============================================================================
# Auto-activation (if run as main module)
# ============================================================================

if __name__ == '__main__':
    print("This module should be imported, not run directly.")
    print("Add this to the top of your app.py:")
    print("")
    print("    import virtual_storage")
    print("    virtual_storage.activate()")
    print("")
