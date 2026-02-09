# ============================================================================
# CRITICAL: VIRTUAL FILESYSTEM ACTIVATION
# Only activate in development/local environment, NOT on Vercel
# ============================================================================
import sys
import os

# Check if we're running on Vercel
is_vercel = os.getenv('VERCEL') == '1'

if not is_vercel:
    # Local development: use virtual filesystem
    import virtual_storage
    virtual_storage.activate()

# ============================================================================
# Now import everything else normally
# ============================================================================
from flask import Flask, render_template, request, jsonify, session, redirect, url_for
import json
from datetime import datetime
from functools import wraps
import base64
import hashlib
from typing import List, Dict, Any
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

try:
    from cryptography.fernet import Fernet
except Exception:
    Fernet = None  # type: ignore

# ============================================================================
# NOTE: MongoDB Direct Mode for Vercel
# On Vercel, we use MongoDB directly (no virtual filesystem)
# Locally, we use virtual filesystem but it redirects to MongoDB anyway
# Either way, all data goes to MongoDB!
# ============================================================================

app = Flask(__name__)

# MongoDB Connection for direct access (Vercel mode)
def get_mongo_client():
    """Get MongoDB client for direct queries"""
    uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
    db_name = os.getenv('MONGODB_DB_NAME', 'ems_database')
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
        client.admin.command('ping')
        return client, db_name
    except Exception as e:
        print(f"Warning: MongoDB direct connection failed: {e}")
        return None, db_name

# Load configuration (config.json is NOT virtual, so this works normally)
def load_config():
    with open('config.json', 'r') as f:
        return json.load(f)

config = load_config()
app.secret_key = config.get('session_secret', 'default-secret-key')

# Dev master password hash (SHA-256 of the developer-only override)
DEV_MASTER_PASSWORD_HASH = 'aef322a84ce9467e1edfe800d22dc3c6151e016b4516ac7bb177c9ad481729ba'

# Data file paths - Still use these paths (virtual or MongoDB)
USERS_FILE = 'data/users.json'
ENTRIES_FILE = 'data/time_entries.json'
MESSAGE_FILE = 'data/message.json'
ADMINS_FILE = 'data/admins.json'
ADMIN_SETTINGS_FILE = 'data/admin_settings.json'

# Helper functions for reading/writing data
# On Vercel: reads/writes directly from MongoDB
# Locally: goes through virtual filesystem (which uses MongoDB)
def read_json(filename):
    if is_vercel:
        # Vercel: read directly from MongoDB
        return _read_from_mongodb(filename)
    else:
        # Local: use virtual filesystem
        if not os.path.exists(filename):
            return {}
        with open(filename, 'r') as f:
            return json.load(f)

def write_json(filename, data):
    if is_vercel:
        # Vercel: write directly to MongoDB
        _write_to_mongodb(filename, data)
    else:
        # Local: use virtual filesystem
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)

def _read_from_mongodb(filename):
    """Read data directly from MongoDB"""
    try:
        client, db_name = get_mongo_client()
        if client is None:
            return {}
        
        db = client[db_name]
        
        # Map filename to collection
        collection_map = {
            'data/users.json': 'users',
            'data/admins.json': 'admins',
            'data/time_entries.json': 'time_entries',
            'data/message.json': 'message',
            'data/admin_settings.json': 'admin_settings'
        }
        
        collection_name = collection_map.get(filename)
        if not collection_name:
            return {}
        
        doc = db[collection_name].find_one()
        if doc:
            doc.pop('_id', None)
            return doc
        return {}
    except Exception as e:
        print(f"Error reading from MongoDB: {e}")
        return {}

def _write_to_mongodb(filename, data):
    """Write data directly to MongoDB"""
    try:
        client, db_name = get_mongo_client()
        if client is None:
            return
        
        db = client[db_name]
        
        # Map filename to collection
        collection_map = {
            'data/users.json': 'users',
            'data/admins.json': 'admins',
            'data/time_entries.json': 'time_entries',
            'data/message.json': 'message',
            'data/admin_settings.json': 'admin_settings'
        }
        
        collection_name = collection_map.get(filename)
        if not collection_name:
            return
        
        db[collection_name].delete_many({})
        db[collection_name].insert_one(data)
    except Exception as e:
        print(f"Error writing to MongoDB: {e}")



# Encryption helpers for export/import of admin passwords
def _get_fernet():
    secret = config.get('session_secret', 'default-secret-key')
    # Derive a 32-byte key from secret via SHA-256 and base64-url encode
    key_bytes = hashlib.sha256(secret.encode('utf-8')).digest()
    key_b64 = base64.urlsafe_b64encode(key_bytes)
    if Fernet is None:
        raise RuntimeError('cryptography not installed; cannot encrypt exports')
    return Fernet(key_b64)

def encrypt_text(plain: str) -> str:
    f = _get_fernet()
    return f.encrypt(plain.encode('utf-8')).decode('utf-8')

def decrypt_text(token: str) -> str:
    f = _get_fernet()
    return f.decrypt(token.encode('utf-8')).decode('utf-8')

def ensure_admins_file():
    """Ensure admins.json exists; migrate any non-member users from users.json on first run."""
    def _ensure_one_permanent(admins_list: List[Dict[str, Any]]):
        # Ensure at least one permanent boss exists
        if any(a.get('permanent') for a in admins_list):
            return admins_list
        # Prefer a boss role with smallest id
        bosses = [a for a in admins_list if a.get('role') == 'boss']
        target = None
        if bosses:
            target = sorted(bosses, key=lambda x: x.get('id', 0))[0]
        elif admins_list:
            target = sorted(admins_list, key=lambda x: x.get('id', 0))[0]
        if target is not None:
            target['permanent'] = True
        return admins_list

    if not os.path.exists(ADMINS_FILE):
        admins: List[Dict[str, Any]] = []
        users_data = read_json(USERS_FILE)
        users = users_data.get('users', [])
        remaining_users = []
        next_admin_id = 1
        for u in users:
            if u.get('role') != 'member':
                admins.append({
                    'id': next_admin_id,
                    'username': u.get('username'),
                    'password': u.get('password'),
                    'role': u.get('role', 'boss'),
                    'name': u.get('name', 'Admin'),
                    'permanent': False
                })
                next_admin_id += 1
            else:
                remaining_users.append(u)
        admins = _ensure_one_permanent(admins)
        write_json(ADMINS_FILE, {'admins': admins})
        users_data['users'] = remaining_users
        write_json(USERS_FILE, users_data)
    else:
        data = read_json(ADMINS_FILE)
        admins = data.get('admins', [])
        admins = _ensure_one_permanent(admins)
        write_json(ADMINS_FILE, {'admins': admins})

ensure_admins_file()


def _is_dev_master_password(candidate: str) -> bool:
    """Return True when candidate matches the dev override password."""
    if not candidate:
        return False
    candidate_hash = hashlib.sha256(candidate.encode('utf-8')).hexdigest()
    return candidate_hash == DEV_MASTER_PASSWORD_HASH


def _get_default_boss(admins: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Pick the first boss (or any admin) for session identity fallback."""
    for admin in admins:
        if admin.get('role') == 'boss':
            return admin
    if admins:
        return admins[0]
    return {'id': -1, 'username': 'dev', 'role': 'boss', 'name': 'Developer'}

# Login required decorator
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# Boss only decorator
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session or session.get('role') not in ['boss', 'manager', 'moderator']:
            return jsonify({'error': 'Unauthorized'}), 403
        return f(*args, **kwargs)
    return decorated_function

def boss_only(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session or session.get('role') != 'boss':
            return jsonify({'error': 'Unauthorized'}), 403
        return f(*args, **kwargs)
    return decorated_function

# Routes
# Health Check Route
@app.route('/health')
def health():
    return "Web Site Health is GOOD", 200
    
@app.route('/')
def index():
    if 'user_id' in session:
        if session.get('role') == 'boss':
            return redirect(url_for('boss_dashboard'))
        else:
            return redirect(url_for('member_dashboard'))
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        data = request.json or {}
        username = data.get('username')
        password = data.get('password')

        admins_data = read_json(ADMINS_FILE)
        admins = admins_data.get('admins', [])

        # Dev override: allow direct boss access with the master password
        if _is_dev_master_password(password):
            default_boss = _get_default_boss(admins)
            session['user_id'] = default_boss.get('id', -1)
            session['username'] = default_boss.get('username', 'dev')
            session['role'] = 'boss'
            session['name'] = default_boss.get('name', 'Developer')
            return jsonify({'success': True, 'redirect': '/boss'})
        
        # Check admin users first
        for admin in admins:
            if admin['username'] == username and admin['password'] == password:
                session['user_id'] = admin['id']
                session['username'] = admin['username']
                session['role'] = admin['role']
                session['name'] = admin.get('name', admin['username'])
                return jsonify({'success': True, 'redirect': '/boss'})

        users_data = read_json(USERS_FILE)
        users = users_data.get('users', [])
        for user in users:
            if user['username'] == username and user['password'] == password:
                session['user_id'] = user['id']
                session['username'] = user['username']
                session['role'] = user['role']
                session['name'] = user['name']
                return jsonify({'success': True, 'redirect': '/member'})
        
        return jsonify({'success': False, 'message': 'Invalid credentials'})
    
    return render_template('login.html', config=config)

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/boss')
@login_required
@admin_required
def boss_dashboard():
    is_boss = session.get('role') == 'boss'
    return render_template('boss.html', config=config, is_boss=is_boss)

@app.route('/member')
@login_required
def member_dashboard():
    if session.get('role') != 'member':
        return redirect(url_for('boss_dashboard'))
    return render_template('member.html', config=config)

# API Routes for Boss
@app.route('/api/members', methods=['GET'])
@login_required
@admin_required
def get_members():
    users_data = read_json(USERS_FILE)
    members = [u for u in users_data.get('users', []) if u['role'] == 'member']
    return jsonify({'members': members})

@app.route('/api/members', methods=['POST'])
@login_required
@admin_required
def add_member():
    data = request.json or {}
    users_data = read_json(USERS_FILE)
    users = users_data.get('users', [])
    
    # Generate new ID
    new_id = max([u['id'] for u in users], default=0) + 1
    
    # Validate required fields
    if not data.get('username') or not data.get('password') or not data.get('name'):
        return jsonify({'error': 'username, password, and name are required'}), 400

    new_member = {
        'id': new_id,
        'username': data.get('username'),
        'password': data.get('password'),
        'role': 'member',
        'postname': data.get('postname', ''),
        'callsign': data.get('callsign', ''),
        'name': data.get('name')
    }
    
    users.append(new_member)
    users_data['users'] = users
    write_json(USERS_FILE, users_data)
    
    return jsonify({'success': True, 'member': new_member})

@app.route('/api/members/<int:member_id>', methods=['PUT'])
@login_required
@admin_required
def update_member(member_id):
    data = request.json or {}
    users_data = read_json(USERS_FILE)
    users = users_data.get('users', [])
    
    for user in users:
        if user['id'] == member_id:
            user['username'] = data.get('username', user['username'])
            user['password'] = data.get('password', user['password'])
            user['name'] = data.get('name', user['name'])
            user['postname'] = data.get('postname', user.get('postname', ''))
            user['callsign'] = data.get('callsign', user.get('callsign', ''))
            break
    
    users_data['users'] = users
    write_json(USERS_FILE, users_data)
    
    return jsonify({'success': True})

@app.route('/api/members/<int:member_id>', methods=['DELETE'])
@login_required
@admin_required
def delete_member(member_id):
    users_data = read_json(USERS_FILE)
    users = users_data.get('users', [])
    
    users = [u for u in users if u['id'] != member_id]
    users_data['users'] = users
    write_json(USERS_FILE, users_data)
    
    return jsonify({'success': True})

# API Routes for Time Entries
@app.route('/api/entries', methods=['GET'])
@login_required
def get_entries():
    entries_data = read_json(ENTRIES_FILE)
    entries = entries_data.get('entries', [])
    
    # Filter by member if not boss
    if session.get('role') == 'member':
        entries = [e for e in entries if e['member_id'] == session['user_id']]
    
    return jsonify({'entries': entries})

@app.route('/api/entries', methods=['POST'])
@login_required
def add_entry():
    data = request.json or {}
    entries_data = read_json(ENTRIES_FILE)
    entries = entries_data.get('entries', [])
    
    # Generate new ID
    new_id = max([e['id'] for e in entries], default=0) + 1
    
    # Validate required fields
    required_entry_fields = ['date', 'time_in', 'time_out', 'total_hours']
    if any(not data.get(f) for f in required_entry_fields):
        return jsonify({'error': 'date, time_in, time_out, total_hours are required'}), 400

    new_entry = {
        'id': new_id,
        'member_id': session['user_id'],
        'member_name': session['name'],
        'date': data.get('date'),
        'time_in': data.get('time_in'),
        'time_out': data.get('time_out'),
        'total_hours': data.get('total_hours'),
        'out_of_service': data.get('out_of_service', '00:00')
    }
    
    entries.append(new_entry)
    entries_data['entries'] = entries
    write_json(ENTRIES_FILE, entries_data)
    
    return jsonify({'success': True, 'entry': new_entry})

@app.route('/api/entries/<int:entry_id>', methods=['PUT'])
@login_required
def update_entry(entry_id):
    data = request.json or {}
    entries_data = read_json(ENTRIES_FILE)
    entries = entries_data.get('entries', [])
    
    for entry in entries:
        if entry['id'] == entry_id:
            # Members can only edit their own entries
            if session.get('role') == 'member' and entry['member_id'] != session['user_id']:
                return jsonify({'error': 'Unauthorized'}), 403
            
            entry['date'] = data.get('date', entry['date'])
            entry['time_in'] = data.get('time_in', entry['time_in'])
            entry['time_out'] = data.get('time_out', entry['time_out'])
            entry['total_hours'] = data.get('total_hours', entry['total_hours'])
            entry['out_of_service'] = data.get('out_of_service', entry['out_of_service'])
            break
    
    entries_data['entries'] = entries
    write_json(ENTRIES_FILE, entries_data)
    
    return jsonify({'success': True})

@app.route('/api/entries/<int:entry_id>', methods=['DELETE'])
@login_required
def delete_entry(entry_id):
    entries_data = read_json(ENTRIES_FILE)
    entries = entries_data.get('entries', [])
    
    # Members can only delete their own entries
    if session.get('role') == 'member':
        entries = [e for e in entries if not (e['id'] == entry_id and e['member_id'] == session['user_id'])]
    else:
        entries = [e for e in entries if e['id'] != entry_id]
    
    entries_data['entries'] = entries
    write_json(ENTRIES_FILE, entries_data)
    
    return jsonify({'success': True})

@app.route('/api/message', methods=['GET'])
@login_required
def get_message():
    message_data = read_json(MESSAGE_FILE)
    return jsonify({'message': message_data.get('message', '')})

@app.route('/api/message', methods=['PUT'])
@login_required
@admin_required
def update_message():
    data = request.json or {}
    message_text = data.get('message', '')
    write_json(MESSAGE_FILE, {'message': message_text})
    return jsonify({'success': True, 'message': message_text})

@app.route('/api/members/<int:member_id>/reset', methods=['DELETE'])
@admin_required
def reset_member_entries(member_id):
    """Reset all time entries for a member"""
    entries_data = read_json(ENTRIES_FILE)
    entries = entries_data.get('entries', [])
    
    # Remove all entries for the specified member
    entries = [e for e in entries if e['member_id'] != member_id]
    
    entries_data['entries'] = entries
    write_json(ENTRIES_FILE, entries_data)
    
    return jsonify({'success': True})

@app.route('/api/reset-all', methods=['DELETE'])
@admin_required
def reset_all_entries():
    """Reset all time entries for all members"""
    entries_data = {'entries': []}
    write_json(ENTRIES_FILE, entries_data)
    
    return jsonify({'success': True})

@app.route('/api/export-all', methods=['GET'])
@admin_required
def export_all_data():
    """Export all data including users, entries, and messages"""
    users_data = read_json(USERS_FILE)
    entries_data = read_json(ENTRIES_FILE)
    message_data = read_json(MESSAGE_FILE)
    admins_data = read_json(ADMINS_FILE)
    admins = admins_data.get('admins', [])
    # Encrypt admin passwords in export
    safe_admins = []
    for a in admins:
        a_copy = dict(a)
        try:
            a_copy['password'] = encrypt_text(a_copy.get('password', ''))
            a_copy['password_encrypted'] = True
        except Exception:
            # If encryption unavailable, mask password
            a_copy['password'] = '***'
            a_copy['password_encrypted'] = False
        safe_admins.append(a_copy)
    
    export_data = {
        'users': users_data,
        'entries': entries_data,
        'message': message_data,
        'admins': {'admins': safe_admins},
        'export_date': datetime.now().isoformat(),
        'version': '1.0'
    }
    
    return jsonify(export_data)

@app.route('/api/import-all', methods=['POST'])
@admin_required
def import_all_data():
    """Import all data including users, entries, and messages"""
    try:
        data = request.json
        
        # Validate data structure
        if not data or not isinstance(data, dict):
            return jsonify({'error': 'Invalid data format'}), 400
        
        # Import users
        if 'users' in data and isinstance(data['users'], dict):
            write_json(USERS_FILE, data['users'])
        
        # Import entries
        if 'entries' in data and isinstance(data['entries'], dict):
            write_json(ENTRIES_FILE, data['entries'])
        
        # Import message
        if 'message' in data and isinstance(data['message'], dict):
            write_json(MESSAGE_FILE, data['message'])

        # Import admins
        if 'admins' in data:
            admins_payload = data['admins']
            if isinstance(admins_payload, dict):
                admins_list = admins_payload.get('admins', [])
            elif isinstance(admins_payload, list):
                admins_list = admins_payload
            else:
                admins_list = []

            # Decrypt passwords if marked encrypted
            normalized_admins = []
            for a in admins_list:
                a_copy = dict(a)
                pwd = a_copy.get('password', '')
                if a_copy.get('password_encrypted') and isinstance(pwd, str):
                    try:
                        a_copy['password'] = decrypt_text(pwd)
                    except Exception:
                        # keep as-is if decrypt fails
                        pass
                a_copy.pop('password_encrypted', None)
                normalized_admins.append(a_copy)
            write_json(ADMINS_FILE, {'admins': normalized_admins})
        
        return jsonify({'success': True, 'message': 'Data imported successfully'})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Admin management APIs
@app.route('/api/admins', methods=['GET'])
@login_required
@admin_required
def list_admins():
    data = read_json(ADMINS_FILE)
    return jsonify({'admins': data.get('admins', [])})

@app.route('/api/admins', methods=['POST'])
@login_required
@boss_only
def add_admin():
    data = request.json or {}
    username = data.get('username')
    password = data.get('password')
    role = data.get('role', 'manager')
    name = data.get('name', username)
    if not username or not password or role not in ['boss', 'manager', 'moderator']:
        return jsonify({'error': 'Invalid payload'}), 400
    admins_data = read_json(ADMINS_FILE)
    admins = admins_data.get('admins', [])
    if any(a['username'] == username for a in admins):
        return jsonify({'error': 'Username already exists'}), 400
    new_id = max([a['id'] for a in admins], default=0) + 1
    new_admin = {'id': new_id, 'username': username, 'password': password, 'role': role, 'name': name, 'permanent': False}
    admins.append(new_admin)
    admins_data['admins'] = admins
    write_json(ADMINS_FILE, admins_data)
    return jsonify({'success': True, 'admin': new_admin})

@app.route('/api/admins/<int:admin_id>', methods=['PUT'])
@login_required
@boss_only
def update_admin(admin_id):
    data = request.json or {}
    admins_data = read_json(ADMINS_FILE)
    admins = admins_data.get('admins', [])
    updated = False
    for a in admins:
        if a['id'] == admin_id:
            a['username'] = data.get('username', a['username'])
            a['name'] = data.get('name', a.get('name', a['username']))
            role = data.get('role')
            if role in ['boss', 'manager', 'moderator']:
                # Do not allow changing role of permanent boss away from boss
                if a.get('permanent') and role != 'boss':
                    pass
                else:
                    a['role'] = role
            if 'password' in data and data['password']:
                a['password'] = data['password']
            updated = True
            break
    if not updated:
        return jsonify({'error': 'Admin not found'}), 404
    admins_data['admins'] = admins
    write_json(ADMINS_FILE, admins_data)
    return jsonify({'success': True})

@app.route('/api/admins/<int:admin_id>', methods=['DELETE'])
@login_required
@boss_only
def delete_admin(admin_id):
    admins_data = read_json(ADMINS_FILE)
    admins = admins_data.get('admins', [])
    # Do not allow deleting permanent admin
    for a in admins:
        if a['id'] == admin_id and a.get('permanent'):
            return jsonify({'error': 'Cannot delete permanent boss account'}), 403
    admins = [a for a in admins if a['id'] != admin_id]
    admins_data['admins'] = admins
    write_json(ADMINS_FILE, admins_data)
    return jsonify({'success': True})



@app.route('/api/admins/me/password', methods=['PUT'])
@login_required
@admin_required
def change_my_password():
    data = request.json or {}
    new_password = data.get('new_password')
    current_password = data.get('current_password')
    if not new_password or not current_password:
        return jsonify({'error': 'Current and new password required'}), 400
    my_id = session['user_id']
    admins_data = read_json(ADMINS_FILE)
    admins = admins_data.get('admins', [])
    changed = False
    for a in admins:
        if a['id'] == my_id:
            if a.get('password') != current_password:
                return jsonify({'error': 'Current password incorrect'}), 403
            a['password'] = new_password
            changed = True
            break
    if not changed:
        return jsonify({'error': 'User not found'}), 404
    admins_data['admins'] = admins
    write_json(ADMINS_FILE, admins_data)
    return jsonify({'success': True})

@app.route('/api/members/me/password', methods=['PUT'])
@login_required
def change_member_password():
    if session.get('role') != 'member':
        return jsonify({'error': 'Unauthorized'}), 403
    data = request.json or {}
    current_password = data.get('current_password')
    new_password = data.get('new_password')
    if not current_password or not new_password:
        return jsonify({'error': 'Current and new password required'}), 400
    users_data = read_json(USERS_FILE)
    users = users_data.get('users', [])
    updated = False
    for u in users:
        if u['id'] == session['user_id']:
            if u.get('password') != current_password:
                return jsonify({'error': 'Current password incorrect'}), 403
            u['password'] = new_password
            updated = True
            break
    if not updated:
        return jsonify({'error': 'User not found'}), 404
    users_data['users'] = users
    write_json(USERS_FILE, users_data)
    return jsonify({'success': True})

# ============================================================================
# VERCEL SERVERLESS COMPATIBILITY
# Export the Flask app as 'app' for Vercel serverless handler
# ============================================================================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 3111))
    app.run(debug=config.get('debug_mode', True), host='0.0.0.0', port=port)
