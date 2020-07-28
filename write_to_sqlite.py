import sqlite3
from pathlib import Path
from hashlib import sha1
from typing import Dict, Any

import pandas as pd

TARGET_FILE = Path("output/movies.sqlite")
SOURCE_DATA = Path("output/box_office.csv")


def create_tables(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute('''CREATE TABLE movies
        (
            id CHAR(40) PRIMARY KEY, 
            name VARCHAR(256),
            release_date DATE, 
            country VARCHAR(20),
            publisher VARCHAR(128),
            agent VARCHAR(128)
        );
    ''')
    cur.execute('''CREATE TABLE weekly_box_office
        (
            id CHAR(40) PRIMARY KEY,
            movie_id VARCHAR(40), 
            date DATE, 
            revenue INTEGER,
            tickets INTEGER,
            total_revenue INTEGER,
            total_tickets INTEGER,
            FOREIGN KEY(movie_id) REFERENCES movies(id)
        );
    ''')
    conn.commit()


def sha1_hex(target: str):
    return sha1(target.encode("utf8")).hexdigest()


def write_movies(df: pd.DataFrame, conn: sqlite3.Connection):
    df_movies = df[
        ~df.country.isnull()  # only one entry
    ][["country", "name", "release_date", "publisher", "agent"]].drop_duplicates(
        ["name", "release_date", "agent"], keep="last"
    )
    buffer = []
    seen: Dict[str, Any] = {}
    for _, row in df_movies.iterrows():
        key = sha1_hex(row["name"] + row["release_date"] + row["agent"])
        if key in seen:
            print(seen[key], row)
        seen[key] = row
        buffer.append((
            key,
            row["name"],
            row["release_date"],
            row["country"],
            row["publisher"],
            row["agent"]
        ))
    cur = conn.cursor()
    cur.executemany(
        'INSERT INTO movies VALUES (?, ?, ?, ?, ?, ?)',
        buffer
    )
    conn.commit()
    return len(buffer)


def write_box_office(df: pd.DataFrame, conn: sqlite3.Connection):
    buffer = []
    for _, row in df.iterrows():
        buffer.append((
            sha1_hex(row["name"] + row["release_date"] + row["agent"] + row["week"]),
            sha1_hex(row["name"] + row["release_date"] + row["agent"]),
            row["week"],
            row["revenue"],
            row["tickets"],
            row["total_revenue"],
            row["total_tickets"]
        ))
    cur = conn.cursor()
    cur.executemany(
        'INSERT INTO weekly_box_office VALUES (?, ?, ?, ?, ?, ?, ?)',
        buffer
    )
    conn.commit()
    return len(buffer)


def main():
    df = pd.read_csv(SOURCE_DATA)
    df["agent"].fillna("", inplace=True)
    if TARGET_FILE.exists():
        TARGET_FILE.rename("cache/db.bak")
    conn = sqlite3.connect(TARGET_FILE)
    create_tables(conn)
    write_movies(df, conn)
    write_box_office(df, conn)
    conn.close()


if __name__ == "__main__":
    main()
