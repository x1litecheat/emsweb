# ============================================================================
# VERCEL SERVERLESS ENTRY POINT
# This file is required for Vercel to properly handle Flask requests
# Vercel looks for this specific path and file name
# ============================================================================

import sys
import os

# Add parent directory to path so we can import app
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    # Import the Flask app from app.py
    from app import app
    
    # Ensure Flask knows where to find static and template files
    app.static_folder = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'static')
    app.template_folder = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'templates')
    
    print("✓ Flask app imported successfully")
    
except Exception as e:
    print(f"✗ Error importing Flask app: {e}")
    import traceback
    traceback.print_exc()
    raise

# Export as 'app' for Vercel serverless handler
__all__ = ['app']
