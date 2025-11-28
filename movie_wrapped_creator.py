import random
import re
import sqlite3
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from top5_analysis import analyze  # your analyze() from the code you pasted

DB_PATH = Path("data/movies.db")
IMAGE_ROOT = Path("images/movie")   # where image_scraper.py stored movie images


# ---------- helpers ----------

def slugify(name: str) -> str:
    """Simple slug: 'The Dark Knight' -> 'The_Dark_Knight'."""
    name = name.strip().replace(" ", "_")
    return re.sub(r"[^A-Za-z0-9_]+", "", name)


def measure_text(draw, text, font):
    bbox = draw.textbbox((0, 0), text, font=font)
    width = bbox[2] - bbox[0]
    height = bbox[3] - bbox[1]
    return width, height


def pick_random_image(directory: Path) -> Path | None:
    if not directory.exists():
        return None
    files = list(directory.glob("*.jpg")) + list(directory.glob("*.jpeg")) + list(directory.glob("*.png"))
    if not files:
        return None
    return random.choice(files)


def paste_cover_background(base_img: Image.Image, backdrop_path: Path | None, dim_alpha: int = 140):
    """
    Paste backdrop covering the entire base_img.
    If no backdrop, keep the base_img background as-is.
    Dim with a semi-transparent overlay to make text readable.
    """
    width, height = base_img.size
    if backdrop_path is not None and backdrop_path.exists():
        bg = Image.open(backdrop_path).convert("RGB")

        # Resize with cover behavior (fill and crop)
        bw, bh = bg.size
        scale = max(width / bw, height / bh)
        new_size = (int(bw * scale), int(bh * scale))
        bg = bg.resize(new_size, Image.LANCZOS)

        # Center-crop to canvas size
        x0 = (bg.width - width) // 2
        y0 = (bg.height - height) // 2
        bg = bg.crop((x0, y0, x0 + width, y0 + height))

        base_img.paste(bg, (0, 0))

    # Dark overlay
    overlay = Image.new("RGBA", (width, height), (0, 0, 0, dim_alpha))
    base_img.alpha_composite(overlay)


# ---------- main wrapped image ----------

def create_movie_wrapped_image(stats: dict, output_path: str = "movie_wrapped_movies.png"):
    """
    Create a Wrapped-style image focusing on movies:
      - Top movie big and centered with backdrop
      - Other top movies smaller in a list with posters
    """

    top_movies = stats["movies"]["top_watched"]
    if not top_movies:
        print("No top movies found in stats.")
        return

    # We’ll use the first as the main movie, others as secondary
    main_movie = top_movies[0]
    other_movies = top_movies[1:]  # if you want 1 + 5 total, increase limit in analyze()

    main_title = main_movie["title"]

    # Prepare canvas (RGBA so we can alpha-composite)
    width, height = 1080, 1920
    img = Image.new("RGBA", (width, height), (18, 18, 40, 255))
    draw = ImageDraw.Draw(img)

    # Fonts
    try:
        title_font = ImageFont.truetype("Arial.ttf", 80)
        big_font = ImageFont.truetype("Arial.ttf", 60)
        normal_font = ImageFont.truetype("Arial.ttf", 40)
        small_font = ImageFont.truetype("Arial.ttf", 32)
    except OSError:
        title_font = ImageFont.load_default()
        big_font = ImageFont.load_default()
        normal_font = ImageFont.load_default()
        small_font = ImageFont.load_default()

    def center_text(text, y, font, fill=(255, 255, 255)):
        w, h = measure_text(draw, text, font=font)
        x = (width - w) // 2
        draw.text((x, y), text, font=font, fill=fill)

    # ---------- pick random poster/backdrop for main movie ----------

    main_slug = slugify(main_title)
    main_posters_dir = IMAGE_ROOT / main_slug / "posters"
    main_backdrops_dir = IMAGE_ROOT / main_slug / "backdrops"

    main_backdrop_path = pick_random_image(main_backdrops_dir)
    main_poster_path = pick_random_image(main_posters_dir)

    # Background with backdrop + dark overlay
    paste_cover_background(img, main_backdrop_path, dim_alpha=150)

    # Title banner at top
    center_text("Your Top Movies", 60, title_font, fill=(255, 255, 255))

    # ---------- main movie poster ----------

    y_top_area = 180

    if main_poster_path and main_poster_path.exists():
        poster = Image.open(main_poster_path).convert("RGBA")
        pw, ph = poster.size

        # Scale poster to a max width/height
        max_poster_width = 450
        max_poster_height = 650
        scale = min(max_poster_width / pw, max_poster_height / ph)
        new_size = (int(pw * scale), int(ph * scale))
        poster = poster.resize(new_size, Image.LANCZOS)

        # Paste centered
        pw, ph = poster.size
        px = (width - pw) // 2
        py = y_top_area
        img.paste(poster, (px, py), poster)

        text_y = py + ph + 20
    else:
        # No poster found, just use text
        text_y = y_top_area + 50

    # Main movie title & stats
    center_text(main_title, text_y, big_font, fill=(255, 255, 0))
    text_y += 60
    center_text(
        f"{main_movie['watch_count']} watches · {main_movie['avg_rating']:.1f}★",
        text_y,
        normal_font,
        fill=(255, 255, 255),
    )

        # ---------- other movies list ----------
    list_start_y = text_y + 120

    draw.text(
        (80, list_start_y - 60),
        "Your other top movies",
        font=normal_font,
        fill=(255, 255, 255),
    )

    current_y = list_start_y
    row_margin = 40  # space between rows

    for idx, movie in enumerate(other_movies, start=2):
        title = movie["title"]
        slug = slugify(title)
        posters_dir = IMAGE_ROOT / slug / "posters"
        backdrops_dir = IMAGE_ROOT / slug / "backdrops"

        poster_path = pick_random_image(posters_dir)
        backdrop_path = pick_random_image(backdrops_dir)

        # --- Prepare poster on the left ---
        poster_width = 180
        poster_height = 260  # target height for row

        if poster_path and poster_path.exists():
            poster = Image.open(poster_path).convert("RGBA")
            pw, ph = poster.size
            scale = min(poster_width / pw, poster_height / ph)
            new_size = (int(pw * scale), int(ph * scale))
            poster = poster.resize(new_size, Image.LANCZOS)
            pw, ph = poster.size
        else:
            # No poster -> fake box size
            pw, ph = poster_width, poster_height
            poster = None

        row_height = ph  # row is as tall as poster

        px = 80
        py = current_y

        if poster is not None:
            img.paste(poster, (px, py), poster)

        # --- Text area on the right ---
        text_x = px + pw + 30
        text_y_top = py

        title_text = f"{idx}. {title}"
        title_w, title_h = measure_text(draw, title_text, font=normal_font)
        draw.text((text_x, text_y_top), title_text, font=normal_font, fill=(255, 255, 255))

        stats_text = f"{movie['watch_count']} watches · {movie['avg_rating']:.1f}★"
        stats_w, stats_h = measure_text(draw, stats_text, font=small_font)
        stats_y = text_y_top + title_h + 8
        draw.text((text_x, stats_y), stats_text, font=small_font, fill=(220, 220, 220))

        # --- Backdrop under the text, aligned with poster bottom ---
        # Top of backdrop = below stats text; bottom of backdrop = bottom of poster
        backdrop_top = stats_y + stats_h + 8
        backdrop_bottom = py + row_height
        available_height = backdrop_bottom - backdrop_top
        available_width = (width - 80) - text_x  # right side up to 80 px margin

        if backdrop_path and backdrop_path.exists() and available_height > 40:
            backdrop = Image.open(backdrop_path).convert("RGBA")
            bw, bh = backdrop.size

            # Scale backdrop to cover the right-side area
            scale = max(available_width / bw, available_height / bh)
            new_bw = int(bw * scale)
            new_bh = int(bh * scale)
            backdrop = backdrop.resize((new_bw, new_bh), Image.LANCZOS)

            # Center-crop to exactly the available area
            x0 = (new_bw - available_width) // 2
            y0 = (new_bh - available_height) // 2
            backdrop = backdrop.crop(
                (x0, y0, x0 + available_width, y0 + available_height)
            )

            # Paste so that bottom aligns with poster bottom
            by = backdrop_top
            bx = text_x
            img.paste(backdrop, (bx, by), backdrop)

        # Move down for next row
        current_y += row_height + row_margin

    # Footer
    footer_text = "Movie Wrapped · generated with Python"
    fw, fh = measure_text(draw, footer_text, font=small_font)
    draw.text(
        (width - fw - 40, height - fh - 40),
        footer_text,
        font=small_font,
        fill=(200, 200, 200),
    )

    # Save
    img = img.convert("RGB")  # strip alpha for final PNG/JPEG
    img.save(output_path, "PNG")
    print(f"Movie wrapped image saved to {output_path}")


if __name__ == "__main__":
    # Use your existing analyze() to get stats
    stats = analyze()
    create_movie_wrapped_image(stats, output_path="movie_wrapped_movies.png")
