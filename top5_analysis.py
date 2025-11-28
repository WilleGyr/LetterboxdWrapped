import sqlite3
from pathlib import Path

DB_PATH = Path("data/movies.db")

def get_top_directors_most_watched(conn, limit=5):
    """
    Top directors by number of diary entries (rewatches included).
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT
            m.director AS director,
            COUNT(*) AS watch_count,
            COUNT(DISTINCT d.movie_id) AS movie_count,
            AVG(m.rating) AS avg_rating
        FROM diary d
        JOIN movie m ON d.movie_id = m.id
        WHERE m.director IS NOT NULL
        GROUP BY m.director
        ORDER BY watch_count DESC
        LIMIT ?;
    """, (limit,))
    rows = cur.fetchall()

    result = []
    for director, watch_count, movie_count, avg_rating in rows:
        result.append({
            "director": director,
            "watch_count": watch_count,
            "movie_count": movie_count,
            "avg_rating": avg_rating,
        })
    return result


def get_top_directors_highest_rated(conn, limit=5):
    """
    Top directors by average movie rating, but only if the user has
    watched more than 1 different movie by them (COUNT(DISTINCT movie_id) > 1).
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT
            m.director AS director,
            COUNT(DISTINCT d.movie_id) AS movie_count,
            COUNT(*) AS watch_count,
            AVG(m.rating) AS avg_rating
        FROM diary d
        JOIN movie m ON d.movie_id = m.id
        WHERE m.director IS NOT NULL
          AND m.rating IS NOT NULL
        GROUP BY m.director
        HAVING COUNT(DISTINCT d.movie_id) > 1
        ORDER BY avg_rating DESC
        LIMIT ?;
    """, (limit,))
    rows = cur.fetchall()

    result = []
    for director, movie_count, watch_count, avg_rating in rows:
        result.append({
            "director": director,
            "movie_count": movie_count,
            "watch_count": watch_count,
            "avg_rating": avg_rating,
        })
    return result


def get_top_actors_most_watched(conn, limit=5):
    """
    Top actors/actresses by number of diary entries for movies they appear in.
    Rewatches count as additional watches.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT
            p.name AS actor,
            COUNT(*) AS watch_count,
            COUNT(DISTINCT d.movie_id) AS movie_count,
            AVG(m.rating) AS avg_rating
        FROM diary d
        JOIN movie m      ON d.movie_id = m.id
        JOIN movie_cast mc ON mc.movie_id = m.id
        JOIN person p     ON p.id = mc.person_id
        GROUP BY p.id
        ORDER BY watch_count DESC
        LIMIT ?;
    """, (limit,))
    rows = cur.fetchall()

    result = []
    for actor, watch_count, movie_count, avg_rating in rows:
        result.append({
            "actor": actor,
            "watch_count": watch_count,
            "movie_count": movie_count,
            "avg_rating": avg_rating,
        })
    return result


def get_top_actors_highest_rated(conn, limit=5):
    """
    Top actors/actresses by average movie rating, but only if the user has
    watched more than 1 different movie with them.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT
            p.name AS actor,
            COUNT(DISTINCT d.movie_id) AS movie_count,
            COUNT(*) AS watch_count,
            AVG(m.rating) AS avg_rating
        FROM diary d
        JOIN movie m      ON d.movie_id = m.id
        JOIN movie_cast mc ON mc.movie_id = m.id
        JOIN person p     ON p.id = mc.person_id
        WHERE m.rating IS NOT NULL
        GROUP BY p.id
        HAVING COUNT(DISTINCT d.movie_id) > 1
        ORDER BY avg_rating DESC
        LIMIT ?;
    """, (limit,))
    rows = cur.fetchall()

    result = []
    for actor, movie_count, watch_count, avg_rating in rows:
        result.append({
            "actor": actor,
            "movie_count": movie_count,
            "watch_count": watch_count,
            "avg_rating": avg_rating,
        })
    return result

# Function that gets the top 5 movies by first rating and watch count
def get_top_movies(conn, limit=5):
    cur = conn.cursor()
    cur.execute("""
        SELECT
            m.title AS title,
            COUNT(*) AS watch_count,
            AVG(m.rating) AS avg_rating
        FROM diary d
        JOIN movie m ON d.movie_id = m.id
        WHERE m.rating IS NOT NULL
        AND EXISTS (
                SELECT 1
                FROM diary d2
                WHERE d2.movie_id = d.movie_id
                AND d2.rewatch = 0
            )
        GROUP BY m.id
        ORDER BY watch_count DESC, avg_rating DESC
        LIMIT ?;
    """, (limit,))
    rows = cur.fetchall()

    result = []
    for title, watch_count, avg_rating in rows:
        result.append({
            "title": title,
            "watch_count": watch_count,
            "avg_rating": avg_rating,
        })
    return result

def analyze(db_path: Path = DB_PATH) -> dict:
    """
    Analyze the database and return a dict with:
      - top 5 most watched directors
      - top 5 highest rated directors (with >1 movie)
      - top 5 most watched actors
      - top 5 highest rated actors (with >1 movie)
    """
    conn = sqlite3.connect(db_path)

    try:
        data = {
            "directors": {
                "most_watched": get_top_directors_most_watched(conn, limit=5),
                "highest_rated": get_top_directors_highest_rated(conn, limit=5),
            },
            "actors": {
                "most_watched": get_top_actors_most_watched(conn, limit=5),
                "highest_rated": get_top_actors_highest_rated(conn, limit=5),
            },
            "movies": {
                "top_watched": get_top_movies(conn, limit=5),
            },
        }
    finally:
        conn.close()

    return data

def print_analysis(data: dict):
    print("\n=== Top Directors ===")
    print("\nMost Watched:")
    for d in data["directors"]["most_watched"]:
        print(f"- {d['director']} | {d['watch_count']} watches | {d['avg_rating']:.1f} avg")

    print("\nHighest Rated (min 2 movies):")
    for d in data["directors"]["highest_rated"]:
        print(f"- {d['director']} | {d['movie_count']} movies | {d['avg_rating']:.1f} avg")

    print("\n=== Top Actors ===")
    print("\nMost Watched:")
    for a in data["actors"]["most_watched"]:
        print(f"- {a['actor']} | {a['watch_count']} watches | {a['avg_rating']:.1f} avg")

    print("\nHighest Rated (min 2 movies):")
    for a in data["actors"]["highest_rated"]:
        print(f"- {a['actor']} | {a['movie_count']} movies | {a['avg_rating']:.1f} avg")

    print("\n=== Top New Movies ===")
    for m in data["movies"]["top_watched"]:
        print(f"- {m['title']} | {m['watch_count']} watches | {m['avg_rating']:.1f} avg")

if __name__ == "__main__":
    stats = analyze()
    print_analysis(stats)

