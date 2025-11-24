from dotenv import load_dotenv
import os

def get_tmdb_credentials():
    load_dotenv("Credentials/TMDB_key_credentials.env")
    return os.getenv("TMDB_ACCESS_TOKEN"), os.getenv("TMDB_API_KEY")

TMDB_ACCESS_TOKEN, TMDB_API_KEY = get_tmdb_credentials()