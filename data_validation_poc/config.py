"""
Configuration settings for the application
"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Groq API key
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

# LLM Model to use
LLM_MODEL = "llama3-70b-8192"

# Database configuration
DB_PATH = "data/temp_database.db"

# Temporary directory for uploaded files
TEMP_DIR = "data/temp_uploads"

# Create directories if they don't exist
os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)