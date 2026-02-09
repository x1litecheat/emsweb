# ============================================================================
# VERCEL SERVERLESS ENTRY POINT
# This file is required for Vercel to properly handle Flask requests
# Vercel looks for this specific path and file name
# ============================================================================

import sys
import os

# Add parent directory to path so we can import app
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the Flask app from app.py
from app import app

# Ensure Flask knows where to find static and template files
app.static_folder = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'static')
app.template_folder = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'templates')

# Export as 'app' for Vercel serverless handler
__all__ = ['app']
