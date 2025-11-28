from PIL import Image, ImageDraw, ImageFont
import sqlite3
from pathlib import Path
from io import BytesIO
import matplotlib.pyplot as plt

from general_analysis import (
    get_movie_count,
    get_director_count,
    get_total_durations,
    get_diary_ratings,
    get_monthly_stats,
)

DB_PATH = Path("data/movies.db")


def measure_text(draw, text, font):
    bbox = draw.textbbox((0, 0), text, font=font)
    width = bbox[2] - bbox[0]
    height = bbox[3] - bbox[1]
    return width, height


def create_wrapped_image(conn, output_path: str = "movie_wrapped.png"):
    """
    Create a Spotify Wrapped-style image summarizing your movie stats,
    including an embedded ratings distribution graph.
    """

    # ---- 1. Gather stats ----
    total_movies = get_movie_count(conn)
    total_directors = get_director_count(conn)

    durations = get_total_durations(conn)
    library_hours = durations["library"]["hours"]
    watched_hours = durations["watched"]["hours"]

    diary_ratings = get_diary_ratings(conn)
    monthly_stats = get_monthly_stats(conn)

    # Best month by number of watches
    best_month, best_month_data = None, None
    if monthly_stats:
        best_month, best_month_data = max(
            monthly_stats.items(),
            key=lambda kv: kv[1]["watches"],
        )

    # Most common rating
    top_rating, top_rating_count = None, 0
    if diary_ratings:
        top_rating, top_rating_count = max(
            diary_ratings.items(),
            key=lambda kv: kv[1],
        )

    # ---- 2. Create base image ----
    width, height = 1080, 1920
    img = Image.new("RGB", (width, height), color=(18, 18, 40))  # dark bluish bg
    draw = ImageDraw.Draw(img)

    # Fonts
    try:
        title_font = ImageFont.truetype("Arial.ttf", 80)
        big_font = ImageFont.truetype("Arial.ttf", 60)
        normal_font = ImageFont.truetype("Arial.ttf", 40)
    except OSError:
        title_font = ImageFont.load_default()
        big_font = ImageFont.load_default()
        normal_font = ImageFont.load_default()

    # Convenience
    def center_text(text, y, font, fill=(255, 255, 255)):
        w, h = measure_text(draw, text, font=font)
        x = (width - w) // 2
        draw.text((x, y), text, font=font, fill=fill)

    # ---- 3. Layout content ----

    # Title
    center_text("Your Movie Wrapped", 80, title_font, fill=(255, 255, 255))

    y = 220
    line_spacing = 70

    # Section: General stats
    center_text("General", y, big_font, fill=(255, 200, 0))
    y += line_spacing + 20

    center_text(f"{total_movies} movies in library", y, normal_font)
    y += line_spacing
    center_text(f"{total_directors} unique directors", y, normal_font)
    y += line_spacing
    center_text(f"{watched_hours:.1f} hours watched", y, normal_font)
    y += line_spacing
    center_text(
        f"{library_hours:.1f} hours to watch everything once",
        y,
        normal_font,
    )
    y += line_spacing + 40

    # Section: Monthly stats
    center_text("Monthly", y, big_font, fill=(0, 200, 255))
    y += line_spacing + 20

    if best_month:
        center_text(f"Most active: {best_month}", y, normal_font)
        y += line_spacing
        center_text(
            f"{best_month_data['watches']} watches, {best_month_data['hours']:.1f} hours",
            y,
            normal_font,
        )
        y += line_spacing + 40
    else:
        center_text("No diary entries yet", y, normal_font)
        y += line_spacing + 40

    # Section: Ratings
    center_text("Ratings", y, big_font, fill=(150, 255, 150))
    y += line_spacing + 20

    if top_rating is not None:
        center_text(f"Most common rating: {top_rating}", y, normal_font)
        y += line_spacing
        center_text(f"Given {top_rating_count} times", y, normal_font)
        y += line_spacing
    else:
        center_text("No ratings found", y, normal_font)
        y += line_spacing

        # ---- 4. Ratings graph embedded ----
    if diary_ratings:
        # Prepare data
        ratings = sorted(diary_ratings.keys())
        counts = [diary_ratings[r] for r in ratings]

        # Create matplotlib figure in memory
        fig, ax = plt.subplots(figsize=(6, 3), dpi=150)
        
        # Bar style — bright turquoise fits Wrapped
        ax.bar(ratings, counts, color="#4DF6FF")

        # White text everywhere
        ax.set_title("Rating distribution", color="white", fontsize=14)
        ax.set_xlabel("Rating", color="white")
        ax.set_ylabel("Entries", color="white")

        # Ticks
        ax.set_xticks(ratings)
        ax.set_xticklabels([str(r) for r in ratings], color="white")
        ax.set_yticklabels(ax.get_yticks(), color="white")

        # Spines (borders around plot)
        for spine in ax.spines.values():
            spine.set_color("white")

        # Grid disabled (cleaner Spotify feel)
        ax.grid(False)

        # Backgrounds
        ax.set_facecolor("#111128")   # dark background for inner chart
        fig.patch.set_facecolor("#111128")

        fig.tight_layout()

        buf = BytesIO()
        fig.savefig(buf, format="PNG", transparent=True)
        plt.close(fig)
        buf.seek(0)

        chart_img = Image.open(buf).convert("RGBA")

        # Resize chart to fit nicely
        max_chart_width = width - 200
        max_chart_height = 400
        scale = min(
            max_chart_width / chart_img.width,
            max_chart_height / chart_img.height,
        )
        new_size = (int(chart_img.width * scale), int(chart_img.height * scale))
        chart_img = chart_img.resize(new_size, Image.LANCZOS)

        chart_w, chart_h = chart_img.size
        chart_x = (width - chart_w) // 2
        chart_y = y + 20

        img.paste(chart_img, (chart_x, chart_y), chart_img)
        y = chart_y + chart_h + 40


    # ---- 5. Footer ----
    footer_text = "Generated with Python · movies.db"
    w, h = measure_text(draw, footer_text, font=normal_font)
    draw.text(
        (width - w - 40, height - h - 40),
        footer_text,
        font=normal_font,
        fill=(180, 180, 180),
    )

    # ---- 6. Save image ----
    img.save(output_path, "PNG")
    print(f"Wrapped image saved to {output_path}")


if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)
    create_wrapped_image(conn, output_path="movie_wrapped.png")
    conn.close()
