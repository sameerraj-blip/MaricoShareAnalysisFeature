"""Vercel entrypoint for the Python Data Ops FastAPI service."""
import os
import sys

# Ensure the standalone python-service package is importable when bundled
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
PYTHON_SERVICE_DIR = os.path.join(ROOT_DIR, "python-service")
if PYTHON_SERVICE_DIR not in sys.path:
    sys.path.insert(0, PYTHON_SERVICE_DIR)

# Import the pre-built FastAPI app (keeps logic in one place for local + Vercel)
from main import app as fastapi_app  # type: ignore  # pragma: no cover

# Vercel's Python runtime automatically detects ASGI apps exposed as `app`
app = fastapi_app

