from dotenv import load_dotenv
import csv, requests, sqlite3, os
from pathlib import Path
from tqdm import tqdm

def get_tmdb_credentials():
    load_dotenv("Credentials/TMDB_key_credentials.env")
    return os.getenv("TMDB_ACCESS_TOKEN"), os.getenv("TMDB_API_KEY")

TMDB_ACCESS_TOKEN, TMDB_API_KEY = get_tmdb_credentials()

CSV_PATH = Path("data/letterboxd/ratings.csv")
DB_PATH = Path("data/movies.db")

# ====== DB SETUP ======
def create_tables(conn):
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS movie (
            id          INTEGER PRIMARY KEY,
            tmdb_id     INTEGER UNIQUE,
            title       TEXT NOT NULL,
            director    TEXT,
            year        INTEGER,
            length_min  INTEGER,
            rating      REAL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS person (
            id    INTEGER PRIMARY KEY,
            name  TEXT NOT NULL UNIQUE
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS movie_cast (
            movie_id      INTEGER NOT NULL,
            person_id     INTEGER NOT NULL,
            billing_order INTEGER,
            PRIMARY KEY (movie_id, person_id),
            FOREIGN KEY (movie_id) REFERENCES movie(id),
            FOREIGN KEY (person_id) REFERENCES person(id)
        );
    """)

    conn.commit()


# ====== TMDB HELPERS ======
def tmdb_search_movie(title):
    """
    Search TMDB by title and return the best match (or None).
    """
    url = "https://api.themoviedb.org/3/search/movie"
    params = {
        "api_key": TMDB_API_KEY,
        "query": title,
        "include_adult": "false",
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()

    results = data.get("results", [])
    if not results:
        return None

    # Take the first result as the best match
    return results[0]


def tmdb_get_movie_details_and_credits(tmdb_id):
    """
    Get details (runtime, release date, title) + credits (cast/crew)
    in ONE request using append_to_response.
    """
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
    params = {
        "api_key": TMDB_API_KEY,
        "append_to_response": "credits",
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    return resp.json()


# ====== DB INSERT HELPERS ======
def get_or_create_person(conn, name):
    cur = conn.cursor()

    # Try to find existing person
    cur.execute("SELECT id FROM person WHERE name = ?", (name,))
    row = cur.fetchone()
    if row:
        return row[0]

    # Insert new person
    cur.execute("INSERT INTO person (name) VALUES (?)", (name,))
    return cur.lastrowid


def insert_or_update_movie(conn, tmdb_id, title, director, year, length_min, rating):
    cur = conn.cursor()

    # UPSERT based on tmdb_id
    cur.execute("""
        INSERT INTO movie (tmdb_id, title, director, year, length_min, rating)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(tmdb_id) DO UPDATE SET
            title      = excluded.title,
            director   = excluded.director,
            year       = excluded.year,
            length_min = excluded.length_min,
            rating     = excluded.rating;
    """, (tmdb_id, title, director, year, length_min, rating))

    # Get internal movie id
    cur.execute("SELECT id FROM movie WHERE tmdb_id = ?", (tmdb_id,))
    row = cur.fetchone()
    return row[0]


def replace_movie_cast(conn, movie_id, cast_names_in_order):
    """
    Replace the cast for this movie with the given top N names.
    """
    cur = conn.cursor()

    # Delete previous cast entries for this movie (if any)
    cur.execute("DELETE FROM movie_cast WHERE movie_id = ?", (movie_id,))

    # Insert up to 10 actors in order
    for order, name in enumerate(cast_names_in_order[:10], start=1):
        person_id = get_or_create_person(conn, name)
        cur.execute("""
            INSERT INTO movie_cast (movie_id, person_id, billing_order)
            VALUES (?, ?, ?);
        """, (movie_id, person_id, order))


# ====== MAIN IMPORT LOGIC ======
def process_csv(conn):
    """
    Read ratings.csv and import all movies into the database.
    - 2nd column = movie title (index 1)
    - 5th column = rating (index 4)
    - First row is header
    """

    with CSV_PATH.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)

        # Skip header row
        header = next(reader, None)

        rows = list(reader)       # so we know total length exactly
        for row_index, row in enumerate(tqdm(rows, desc="Importing movies"), start=2):

            # Basic sanity check
            if len(row) < 5:
                # Skip rows that don't have enough columns
                continue

            title = row[1].strip()       # 2nd column
            rating_str = row[4].strip()  # 5th column

            if not title:
                continue

            try:
                rating = float(rating_str)
            except ValueError:
                # If rating can't be parsed, skip
                continue

            # --- TMDB: search movie ---
            try:
                search_result = tmdb_search_movie(title)
            except Exception:
                # If TMDB search fails, skip this row
                continue

            if not search_result:
                # No result found
                continue

            tmdb_id = search_result["id"]

            # --- TMDB: details + credits in one call ---
            try:
                details = tmdb_get_movie_details_and_credits(tmdb_id)
            except Exception:
                continue

            # Title (prefer TMDB title)
            tmdb_title = title  # always use CSV title

            # Year from release_date
            release_date = details.get("release_date") or ""
            year = None
            if len(release_date) >= 4:
                try:
                    year = int(release_date[:4])
                except ValueError:
                    year = None

            # Runtime
            length_min = details.get("runtime")

            # Credits: director and top cast
            credits = details.get("credits", {}) or {}

            director_name = None
            for crew_member in credits.get("crew", []):
                if crew_member.get("job") == "Director":
                    director_name = crew_member.get("name")
                    break

            cast_list = credits.get("cast", []) or []
            top_cast_names = [c.get("name") for c in cast_list if c.get("name")]

            # --- Insert into DB (movie + cast) ---
            movie_id = insert_or_update_movie(
                conn,
                tmdb_id=tmdb_id,
                title=tmdb_title,
                director=director_name,
                year=year,
                length_min=length_min,
                rating=rating,
            )

            replace_movie_cast(conn, movie_id, top_cast_names)


def main():
    # Remove old database files if they exist
    for ext in ("", "-wal", "-shm"):
        path = f"movies.db{ext}"
        if os.path.exists(path):
            os.remove(path)
            
    conn = sqlite3.connect(DB_PATH)

    # Better performance: turn on WAL journal mode (optional but nice)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    create_tables(conn)

    # HUGE speedup: one big transaction
    try:
        conn.execute("BEGIN")
        process_csv(conn)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
