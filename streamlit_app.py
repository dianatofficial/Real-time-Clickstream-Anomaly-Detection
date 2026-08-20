"""
Root Streamlit Entrypoint Proxy for Streamlit Community Cloud.
Redirects execution to src/dashboard/app.py with proper sys.path resolution.
"""

import os
import sys
from pathlib import Path

# Add project root directory to sys.path
ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# Execute main dashboard application
dashboard_app_path = ROOT_DIR / "src" / "dashboard" / "app.py"
with open(dashboard_app_path, "r", encoding="utf-8") as f:
    code = f.read()

exec(code, globals())
