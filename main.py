from dotenv import load_dotenv
import os

load_dotenv("Credentials/TMDB_key_credentials.env")

TMDB_ACCESS_TOKEN = os.getenv("TMDB_ACCESS_TOKEN")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")