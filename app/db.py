"""
CS 457 Final Project
db.py
Database connection and service layer for Steam Explorer
"""

import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from models import Game, Review, PlatformSupport

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "steam_explorer",
    "user":     "postgres",
    "password": "postgres",
}

@contextmanager
def get_connection():
    conn = psycopg2.connect(**DB_CONFIG, cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield conn
    finally:
        conn.close()

class GameService:
    
    def get_all_genres(self) -> list[str]:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM genre ORDER BY name")
                return [row["name"] for row in cur.fetchall()]

    def get_release_year_range(self) -> tuple[int, int]:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT MIN(EXTRACT(YEAR FROM release_date)),
                           MAX(EXTRACT(YEAR FROM release_date))
                    FROM game
                    WHERE release_date IS NOT NULL
                """)
                row = cur.fetchone()
                return int(row["min"]), int(row["max"])

    def get_price_range(self) -> tuple[float, float]:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MIN(price), MAX(price) FROM game WHERE price IS NOT NULL")
                row = cur.fetchone()
                return float(row["min"] or 0), float(row["max"] or 0)

    def search_games(
        self,
        genres: list[str] | None = None,
        year_min: int | None = None,
        year_max: int | None = None,
        price_min: float | None = None,
        price_max: float | None = None,
        name_query: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        conditions = []
        params: list = []

        if name_query:
            conditions.append("g.name ILIKE %s")
            params.append(f"%{name_query}%")

        if year_min is not None:
            conditions.append("EXTRACT(YEAR FROM g.release_date) >= %s")
            params.append(year_min)

        if year_max is not None:
            conditions.append("EXTRACT(YEAR FROM g.release_date) <= %s")
            params.append(year_max)

        if price_min is not None:
            conditions.append("g.price >= %s")
            params.append(price_min)

        if price_max is not None:
            conditions.append("g.price <= %s")
            params.append(price_max)

        if genres:
            conditions.append("""
                g.appid IN (
                    SELECT gg.appid FROM game_genre gg
                    JOIN genre gn ON gg.genre_id = gn.genre_id
                    WHERE gn.name = ANY(%s)
                )
            """)
            params.append(genres)

        where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        query = f"""
            SELECT
                g.appid,
                g.name,
                d.name          AS developer,
                p.name          AS publisher,
                g.release_date,
                g.price,
                g.owners,
                r.positive_ratings,
                r.negative_ratings,
                CASE
                    WHEN (r.positive_ratings + r.negative_ratings) = 0 THEN NULL
                    ELSE ROUND(
                        r.positive_ratings::numeric /
                        (r.positive_ratings + r.negative_ratings) * 100, 1
                    )
                END             AS review_score,
                STRING_AGG(DISTINCT gn.name, ', ' ORDER BY gn.name) AS genres
            FROM game g
            LEFT JOIN developer d  ON g.developer_id  = d.developer_id
            LEFT JOIN publisher p  ON g.publisher_id  = p.publisher_id
            LEFT JOIN review r     ON g.appid          = r.appid
            LEFT JOIN game_genre gg ON g.appid         = gg.appid
            LEFT JOIN genre gn     ON gg.genre_id      = gn.genre_id
            {where_clause}
            GROUP BY g.appid, d.name, p.name, r.positive_ratings, r.negative_ratings
            ORDER BY g.name
            LIMIT %s OFFSET %s
        """
        params += [limit, offset]

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return [dict(row) for row in cur.fetchall()]

    def count_games(
        self,
        genres: list[str] | None = None,
        year_min: int | None = None,
        year_max: int | None = None,
        price_min: float | None = None,
        price_max: float | None = None,
        name_query: str | None = None,
    ) -> int:
        conditions = []
        params: list = []

        if name_query:
            conditions.append("g.name ILIKE %s")
            params.append(f"%{name_query}%")
        if year_min is not None:
            conditions.append("EXTRACT(YEAR FROM g.release_date) >= %s")
            params.append(year_min)
        if year_max is not None:
            conditions.append("EXTRACT(YEAR FROM g.release_date) <= %s")
            params.append(year_max)
        if price_min is not None:
            conditions.append("g.price >= %s")
            params.append(price_min)
        if price_max is not None:
            conditions.append("g.price <= %s")
            params.append(price_max)
        if genres:
            conditions.append("""
                g.appid IN (
                    SELECT gg.appid FROM game_genre gg
                    JOIN genre gn ON gg.genre_id = gn.genre_id
                    WHERE gn.name = ANY(%s)
                )
            """)
            params.append(genres)

        where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        query = f"SELECT COUNT(DISTINCT g.appid) FROM game g {where_clause}"

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchone()["count"]

    def games_per_year(self) -> list[dict]:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXTRACT(YEAR FROM release_date)::int AS year,
                           COUNT(*) AS count
                    FROM game
                    WHERE release_date IS NOT NULL
                    GROUP BY year
                    ORDER BY year
                """)
                return [dict(row) for row in cur.fetchall()]

    def top_genres(self, limit: int = 15) -> list[dict]:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT gn.name AS genre, COUNT(*) AS count
                    FROM game_genre gg
                    JOIN genre gn ON gg.genre_id = gn.genre_id
                    GROUP BY gn.name
                    ORDER BY count DESC
                    LIMIT %s
                """, (limit,))
                return [dict(row) for row in cur.fetchall()]

    def avg_price_by_genre(self) -> list[dict]:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT gn.name AS genre,
                           ROUND(AVG(g.price)::numeric, 2) AS avg_price
                    FROM game g
                    JOIN game_genre gg ON g.appid = gg.appid
                    JOIN genre gn      ON gg.genre_id = gn.genre_id
                    WHERE g.price IS NOT NULL AND g.price > 0
                    GROUP BY gn.name
                    HAVING COUNT(*) > 10
                    ORDER BY avg_price DESC
                    LIMIT 15
                """)
                return [dict(row) for row in cur.fetchall()]

    def top_rated_games(self, min_reviews: int = 500, limit: int = 20) -> list[dict]:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT g.name,
                           r.positive_ratings,
                           r.negative_ratings,
                           ROUND(
                               r.positive_ratings::numeric /
                               (r.positive_ratings + r.negative_ratings) * 100, 1
                           ) AS score
                    FROM game g
                    JOIN review r ON g.appid = r.appid
                    WHERE (r.positive_ratings + r.negative_ratings) >= %s
                    ORDER BY score DESC
                    LIMIT %s
                """, (min_reviews, limit))
                return [dict(row) for row in cur.fetchall()]

    def platform_breakdown(self) -> dict:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        SUM(CASE WHEN windows THEN 1 ELSE 0 END) AS windows,
                        SUM(CASE WHEN mac     THEN 1 ELSE 0 END) AS mac,
                        SUM(CASE WHEN linux   THEN 1 ELSE 0 END) AS linux
                    FROM platform_support
                """)
                return dict(cur.fetchone())