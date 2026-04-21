"""
CS 457 Final Project
elt.py
Reads the dataset, normalizes, and load it into PostgresSQL
"""

import argparse
import sys
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "steam_explorer",
    "user": "postgres",
    "password": "",
}

DEFAULT_CSV = "steam.csv"

# Helpers

def split_semis(val) -> list[str]:
    if pd.isna(val) or str(val).strip() == "":
        return []
    return [v.strip() for v in str(val).split(";") if v.strip()]

def parse_date(val):
    if pd.isna(val) or str(val).strip() == "":
        return None
    for fmt in ("%b %d, %Y", "%Y-%m-%d", "%b %Y"):
        try:
            return datetime.strptime(str(val).strip(), fmt).date()
        except ValueError:
            pass
    return None

def has_platform(platforms_val, platform: str) -> bool:
    return platform.lower() in str(platforms_val).lower()

def upsert_lookup(cur, table: str, id_col: str, name_col: str, names: list[str]) -> dict[str, int]:
    if not names:
        return {}
    execute_values(
        cur,
        f"INSERT INTO {table} ({name_col}) VALUES %s ON CONFLICT ({name_col}) DO NOTHING",
        [(n,) for n in names],
    )
    cur.execute(f"SELECT {name_col}, {id_col} FROM {table} WHERE {name_col} = ANY(%s)", (names,))
    return {row[0]: row[1] for row in cur.fetchall()}

# Main

def run(csv_path: str):
    print(f"[ETL Reading {csv_path}]")
    df = pd.read_csv(csv_path)
    print(f"[ETL] Loaded {len(df):,} rows, columns: {list(df.columns)}")

    # Clean Data

    df = df.dropna(subset=["appid", "name"])
    df["appid"] = df["appid"].astype(int)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["positive_ratings"] = pd.to_numeric(df.get("positive_ratings", 0), errors="coerce").fillna(0).astype(int)
    df["negative_ratings"] = pd.to_numeric(df.get("negative_ratings", 0), errors="coerce").fillna(0).astype(int)
    df["average_playtime"] = pd.to_numeric(df.get("average_playtime", 0), errors="coerce").fillna(0).astype(int)

    df["developer_clean"] = df["developer"].apply(lambda v: split_semis(v)[0] if split_semis(v) else None)
    df["publisher_clean"] = df["publisher"].apply(lambda v: split_semis(v)[0] if split_semis(v) else None)

    print("[ETL] Connecting to database ...")
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        print("[ETL] Loading publishers ...")
        all_publishers = df["publisher_clean"].dropna().unique().tolist()
        pub_map = upsert_lookup(cur, "publisher", "publisher_id", "name", all_publishers)

        print("[ETL] Loading developers ...")
        all_developers = df["developer_clean"].dropna().unique().tolist()
        dev_map = upsert_lookup(cur, "developer", "developer_id", "name", all_developers)

        print("[ETL] Loading genres ...")
        all_genres = list({g for val in df["genres"].dropna() for g in split_semis(val)})
        genre_map = upsert_lookup(cur, "genre", "genre_id", "name", all_genres)

        print("[ETL] Loading categories ...")
        all_cats = list({c for val in df["categories"].dropna() for c in split_semis(val)})
        cat_map = upsert_lookup(cur, "category", "category_id", "name", all_cats)

        print("[ETL] Loading games ...")
        game_rows = []
        for _, row in df.iterrows():
            game_rows.append((
                int(row["appid"]),
                str(row["name"]),
                dev_map.get(row["developer_clean"]),
                pub_map.get(row["publisher_clean"]),
                parse_date(row.get("release_date")),
                row["price"] if pd.notna(row["price"]) else None,
                int(row["average_playtime"]),
                str(row["owners"]) if pd.notna(row.get("owners")) else None,
            ))
        execute_values(
            cur,
            """INSERT INTO game (appid, name, developer_id, publisher_id, release_date, price, average_playtime, owners)
               VALUES %s ON CONFLICT (appid) DO NOTHING""",
            game_rows,
        )

        print("[ETL] Loading reviews ...")
        review_rows = [
            (int(row["appid"]), int(row["positive_ratings"]), int(row["negative_ratings"]))
            for _, row in df.iterrows()
        ]
        execute_values(
            cur,
            "INSERT INTO review (appid, positive_ratings, negative_ratings) VALUES %s ON CONFLICT (appid) DO NOTHING",
            review_rows,
        )

        print("[ETL] Loading platform support ...")
        platform_rows = [
            (
                int(row["appid"]),
                has_platform(row.get("platforms", ""), "windows"),
                has_platform(row.get("platforms", ""), "mac"),
                has_platform(row.get("platforms", ""), "linux"),
            )
            for _, row in df.iterrows()
        ]
        execute_values(
            cur,
            "INSERT INTO platform_support (appid, windows, mac, linux) VALUES %s ON CONFLICT (appid) DO NOTHING",
            platform_rows,
        )

        print("[ETL] Loading game-genre links ...")
        game_genre_rows = []
        for _, row in df.iterrows():
            appid = int(row["appid"])
            for g in split_semis(row.get("genres", "")):
                if g in genre_map:
                    game_genre_rows.append((appid, genre_map[g]))
        execute_values(
            cur,
            "INSERT INTO game_genre (appid, genre_id) VALUES %s ON CONFLICT DO NOTHING",
            game_genre_rows,
        )

        print("[ETL] Loading game-category links ...")
        game_cat_rows = []
        for _, row in df.iterrows():
            appid = int(row["appid"])
            for c in split_semis(row.get("categories", "")):
                if c in cat_map:
                    game_cat_rows.append((appid, cat_map[c]))
        execute_values(
            cur,
            "INSERT INTO game_category (appid, category_id) VALUES %s ON CONFLICT DO NOTHING",
            game_cat_rows,
        )

        conn.commit()
        print("[ETL] Finished! Database populated succesfully")

    except Exception as e:
        conn.rollback()
        print(f"[ETL] Error: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Steam Explorer ETL")
    parser.add_argument("--csv", default=DEFAULT_CSV, help="Path to steam.csv")
    args = parser.parse_args()
    run(args.csv)