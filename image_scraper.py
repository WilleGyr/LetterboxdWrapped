import re
import requests
import sqlite3
from pathlib import Path
from io import BytesIO
import time
import os
from dotenv import load_dotenv

from top5_analysis import analyze  # your analyze() from the code you pasted

DB_PATH = Path("data/movies.db")

def get_tmdb_credentials():
    load_dotenv("Credentials/TMDB_key_credentials.env")
    return os.getenv("TMDB_ACCESS_TOKEN"), os.getenv("TMDB_API_KEY")

TMDB_ACCESS_TOKEN, TMDB_API_KEY = get_tmdb_credentials()

# Put your real TMDB API key here
TMDB_API_BASE = "https://api.themoviedb.org/3"
TMDB_IMG_BASE = "https://image.tmdb.org/t/p"


# ---------- Small helpers ----------

def slugify(name: str) -> str:
    """Simple slug: 'Christopher Nolan' -> 'Christopher_Nolan'."""
    name = name.strip().replace(" ", "_")
    return re.sub(r"[^A-Za-z0-9_]+", "", name)


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def tmdb_search_movie(title: str, year: int | None = None) -> int | None:
    """
    Search TMDB for a movie title, return TMDB movie ID or None.
    (Fallback if tmdb_id in DB is NULL)
    """
    params = {
        "api_key": TMDB_API_KEY,
        "query": title,
        "include_adult": "false",
    }
    if year is not None:
        params["year"] = year

    resp = requests.get(f"{TMDB_API_BASE}/search/movie", params=params)
    resp.raise_for_status()
    data = resp.json()
    results = data.get("results") or []
    if not results:
        return None
    return results[0]["id"]


def tmdb_get_movie_images(tmdb_id: int) -> tuple[list[str], list[str]]:
    """
    Return (poster_paths, backdrop_paths) for a TMDB movie ID.
    These are relative paths like '/abcd123.jpg'.
    """
    params = {"api_key": TMDB_API_KEY}
    resp = requests.get(f"{TMDB_API_BASE}/movie/{tmdb_id}/images", params=params)
    resp.raise_for_status()
    data = resp.json()
    posters = [p["file_path"] for p in data.get("posters", []) if p.get("file_path")]
    backdrops = [b["file_path"] for b in data.get("backdrops", []) if b.get("file_path")]
    return posters, backdrops


def download_image(url: str, dest_path: Path):
    """Download an image from URL to dest_path, skipping if already exists."""
    if dest_path.exists():
        return
    resp = requests.get(url, stream=True)
    if resp.status_code == 200:
        with dest_path.open("wb") as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)


def get_movie_tmdb_id(conn, title: str) -> tuple[int | None, int | None]:
    """
    Look up tmdb_id and year for a movie by title from the movie table.
    Returns (tmdb_id, year).
    """
    cur = conn.cursor()
    cur.execute("SELECT tmdb_id, year FROM movie WHERE title = ? LIMIT 1;", (title,))
    row = cur.fetchone()
    if row:
        return row[0], row[1]
    return None, None


def download_movie_images_for_target(
    title: str,
    tmdb_id: int | None,
    year: int | None,
    target_root: Path,
    max_posters: int = 5,
    max_backdrops: int = 5,
):
    """
    Download posters & backdrops for a movie into:

        target_root/<movie_slug>/posters/*.jpg
        target_root/<movie_slug>/backdrops/*.jpg
    where target_root may be e.g. images/movie or images/director/Christopher_Nolan.
    """
    movie_slug = slugify(title)
    posters_dir = target_root / movie_slug / "posters"
    backdrops_dir = target_root / movie_slug / "backdrops"
    ensure_dir(posters_dir)
    ensure_dir(backdrops_dir)

    # Fallback to TMDB search if no tmdb_id
    if tmdb_id is None:
        try:
            tmdb_id = tmdb_search_movie(title, year=year)
        except Exception as e:
            print(f"[movie:{title}] TMDB search failed: {e}")
            return

    if tmdb_id is None:
        print(f"[movie:{title}] No TMDB result found.")
        return

    try:
        posters, backdrops = tmdb_get_movie_images(tmdb_id)
    except Exception as e:
        print(f"[movie:{title}] TMDB images failed: {e}")
        return

    # Download posters
    for i, path in enumerate(posters[:max_posters], start=1):
        url = f"{TMDB_IMG_BASE}/w780{path}"  # good poster size
        dest = posters_dir / f"poster_{i}.jpg"
        download_image(url, dest)

    # Download backdrops
    for i, path in enumerate(backdrops[:max_backdrops], start=1):
        url = f"{TMDB_IMG_BASE}/w1280{path}"  # decent backdrop size
        dest = backdrops_dir / f"backdrop_{i}.jpg"
        download_image(url, dest)


# ---------- DB-based helpers for director/actor movies ----------

def get_director_movies(conn, director_name: str, limit=5):
    """
    Get up to 'limit' movies for a given director, ordered by rating desc.
    Returns list of (title, tmdb_id, year).
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT m.title, m.tmdb_id, m.year
        FROM movie m
        WHERE m.director = ?
        ORDER BY m.rating DESC NULLS LAST, m.year DESC
        LIMIT ?;
    """, (director_name, limit))
    return cur.fetchall()


def get_actor_movies(conn, actor_name: str, limit=5):
    """
    Get up to 'limit' movies for a given actor, ordered by rating desc.
    Returns list of (title, tmdb_id, year).
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT m.title, m.tmdb_id, m.year
        FROM movie m
        JOIN movie_cast mc ON mc.movie_id = m.id
        JOIN person p ON p.id = mc.person_id
        WHERE p.name = ?
        ORDER BY m.rating DESC NULLS LAST, m.year DESC
        LIMIT ?;
    """, (actor_name, limit))
    return cur.fetchall()


# ---------- Main scraping logic ----------

def scrape_images():
    conn = sqlite3.connect(DB_PATH)

    # 1. Use your existing analyze() to get top 5 directors/actors/movies
    stats = analyze()

    # Top movies (from your analyze() structure)
    top_movies = stats["movies"]["top_watched"]  # list of dicts with "title", ...

    # Top directors & actors from "most_watched"
    top_directors = [d["director"] for d in stats["directors"]["most_watched"]]
    top_actors = [a["actor"] for a in stats["actors"]["most_watched"]]

    # ---- Movies ----
    movie_root = Path("images/movie")
    for m in top_movies:
        title = m["title"]
        tmdb_id, year = get_movie_tmdb_id(conn, title)
        print(f"[movie] {title} (tmdb_id={tmdb_id})")
        download_movie_images_for_target(
            title=title,
            tmdb_id=tmdb_id,
            year=year,
            target_root=movie_root,
            max_posters=5,
            max_backdrops=5,
        )

    # ---- Directors ----
    director_root = Path("images/director")
    for name in top_directors:
        director_slug = slugify(name)
        person_root = director_root / director_slug
        print(f"[director] {name}")
        movies = get_director_movies(conn, name, limit=5)
        for title, tmdb_id, year in movies:
            download_movie_images_for_target(
                title=title,
                tmdb_id=tmdb_id,
                year=year,
                target_root=person_root,
                max_posters=5,
                max_backdrops=5,
            )

    # ---- Actors ----
    actor_root = Path("images/actor")
    for name in top_actors:
        actor_slug = slugify(name)
        person_root = actor_root / actor_slug
        print(f"[actor] {name}")
        movies = get_actor_movies(conn, name, limit=5)
        for title, tmdb_id, year in movies:
            download_movie_images_for_target(
                title=title,
                tmdb_id=tmdb_id,
                year=year,
                target_root=person_root,
                max_posters=5,
                max_backdrops=5,
            )

    conn.close()


if __name__ == "__main__":
    scrape_images()
