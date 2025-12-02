"""Configuration for Python Data Ops Service"""
import os
from typing import Optional

class Config:
    """Service configuration"""
    # Server configuration
    HOST: str = os.getenv("PYTHON_SERVICE_HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PYTHON_SERVICE_PORT", "8001"))
    
    # CORS configuration
    CORS_ORIGINS: list[str] = os.getenv(
        "CORS_ORIGINS", 
        "http://localhost:3000,http://localhost:5173"
    ).split(",")
    
    # Timeout configuration
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "300"))  # 5 minutes
    
    # Data processing limits
    MAX_ROWS: int = int(os.getenv("MAX_ROWS", "1000000"))  # 1M rows max
    MAX_PREVIEW_ROWS: int = int(os.getenv("MAX_PREVIEW_ROWS", "10000"))  # 10K for preview

config = Config()

