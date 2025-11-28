import sqlite3
from pathlib import Path

DB_PATH = Path("data/movies.db")

def get_movie_count(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM movie;")
    (count,) = cur.fetchone()
    return count

def get_director_count(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(DISTINCT director)
        FROM movie
        WHERE director IS NOT NULL;
    """)
    (count,) = cur.fetchone()
    return count

def get_total_durations(conn):
    """
    Returns:
      {
        "watched": { "minutes": ..., "hours": ... },  # from diary (incl. rewatches)
        "library": { "minutes": ..., "hours": ... },  # all movies once
      }
    """
    cur = conn.cursor()

    # ---- total time actually watched (from diary) ----
    cur.execute("""
        SELECT
            SUM(m.length_min * d.watch_count) AS total_minutes
        FROM movie m
        JOIN (
            SELECT movie_id, COUNT(*) AS watch_count
            FROM diary
            GROUP BY movie_id
        ) d ON m.id = d.movie_id;
    """)
    (watched_minutes,) = cur.fetchone()
    if watched_minutes is None:
        watched_minutes = 0

    # ---- total time of all movies in the library (each once) ----
    cur.execute("""
        SELECT SUM(length_min) AS total_minutes
        FROM movie;
    """)
    (library_minutes,) = cur.fetchone()
    if library_minutes is None:
        library_minutes = 0

    return {
        "watched": {
            "minutes": watched_minutes,
            "hours": watched_minutes / 60.0,
        },
        "library": {
            "minutes": library_minutes,
            "hours": library_minutes / 60.0,
        },
    }

# function that gets the number of each rating from 0,5 to 5,0 in the diary
def get_diary_ratings(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT
            m.rating AS rating,
            COUNT(*) AS rating_count
        FROM diary d
        JOIN movie m ON d.movie_id = m.id
        WHERE m.rating IS NOT NULL
        GROUP BY m.rating
        ORDER BY m.rating ASC;
    """)
    rows = cur.fetchall()

    result = {}
    for rating, rating_count in rows:
        result[rating] = rating_count
    return result

from datetime import datetime

def get_monthly_stats(conn):
    """
    Returns a dict mapping 'January 2024' -> stats for that month:
      - watches: diary entries (incl. rewatches)
      - rewatches: number of rewatches
      - minutes: total minutes watched
      - hours: total hours watched
      - avg_rating: average rating for that month
    """

    cur = conn.cursor()

    cur.execute("""
        SELECT
            SUBSTR(d.watched_date, 1, 7) AS month,   -- 'YYYY-MM'
            COUNT(*) AS watches,
            SUM(d.rewatch) AS rewatches,
            SUM(m.length_min) AS total_minutes,
            AVG(m.rating) AS avg_rating
        FROM diary d
        JOIN movie m ON m.id = d.movie_id
        GROUP BY month
        ORDER BY month;
    """)

    rows = cur.fetchall()

    stats = {}

    for month_str, watches, rewatches, minutes, avg_rating in rows:
        minutes = minutes or 0
        rewatches = rewatches or 0

        # Convert "YYYY-MM" to "MonthName YYYY"
        month_obj = datetime.strptime(month_str, "%Y-%m")
        pretty_month = month_obj.strftime("%b")

        stats[pretty_month] = {
            "watches": watches,
            "rewatches": rewatches,
            "minutes": minutes,
            "hours": minutes / 60.0,
            "avg_rating": avg_rating,
        }

    return stats

if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)

    print("\nGeneral analysis of the movie database:")
    print(f"Total movies in database: {get_movie_count(conn)}")
    print(f"Total unique directors in database: {get_director_count(conn)}")

    watch_duration = get_total_durations(conn)
    print(f"Total library duration (in hours): {watch_duration['library']['hours']:.1f}")
    print(f"Total diary duration (in hours): {watch_duration['watched']['hours']:.1f}")

    diary_ratings = get_diary_ratings(conn)
    print("\nDiary ratings distribution:")
    for rating in sorted(diary_ratings.keys()):
        print(f"  Rating {rating}: {diary_ratings[rating]} entries")

    monthly_stats = get_monthly_stats(conn)
    print("\nMonthly stats:")
    for month in sorted(monthly_stats.keys()):
        stats = monthly_stats[month]
        print(f"  {month}: {stats['watches']} watches, {stats['rewatches']} rewatches, "
              f"{stats['hours']:.1f} hours, avg rating {stats['avg_rating']:.2f}")

    conn.close()