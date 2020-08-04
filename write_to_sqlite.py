import sqlite3
from pathlib import Path
from hashlib import sha1
from typing import Dict, Any
from datetime import timedelta

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
            theaters INTEGER,
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
        ["name", "publisher"], keep="last"
    )
    buffer = []
    seen: Dict[str, Any] = {}
    for _, row in df_movies.iterrows():
        key = sha1_hex(row["name"] + row["publisher"])
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
            sha1_hex(row["name"] + row["publisher"] + row["week"].strftime("%Y/%m/%d")),
            sha1_hex(row["name"] + row["publisher"]),
            row["week"].strftime("%Y/%m/%d"),
            row["theaters"],
            row["revenue"],
            row["tickets"],
            row["total_revenue"],
            row["total_tickets"]
        ))
    cur = conn.cursor()
    cur.executemany(
        'INSERT INTO weekly_box_office VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        buffer
    )
    conn.commit()
    return len(buffer)


def recover_opening_week_data(df):
    """The opening week data can be recovered for movies that were released after Nov. 2017"""
    df_tmp = df[df.release_date >= "2017-11-01"].sort_values(
        "week").groupby(["name", "publisher"], as_index=False).first()
    # No movie was released on Monday
    assert df_tmp[df_tmp.week == df_tmp.release_date].shape[0] == 0
    tmp = []
    for _, row in df_tmp.iterrows():
        row = row.copy()
        row["total_revenue"] = row["total_revenue"] - row["revenue"]
        row["total_tickets"] = row["total_tickets"] - row["tickets"]
        row["revenue"] = row["total_revenue"]
        row["tickets"] = row["total_tickets"]
        assert (row["revenue"] >= 0) and (row["tickets"] >= 0)
        row["week"] = row["week"] - timedelta(days=7)
        tmp.append(row)
    df_recovered = pd.DataFrame(tmp)
    df_recovered.head(3)
    df = pd.concat(
        [df, df_recovered], axis=0, ignore_index=True
    ).sort_values(["week", "revenue"], ascending=[True, False]).reset_index(drop=True)
    print("Problematic entries:")
    cnt = df.groupby(["name", "publisher", "week"])["week"].count()
    print(cnt[cnt > 1])
    # throw away a random row for now
    print(df.shape[0])
    df = df.drop_duplicates(["name", "publisher", "week"])
    print(df.shape[0])
    return df


def main():
    df = pd.read_csv(SOURCE_DATA, parse_dates=["week"])
    df = recover_opening_week_data(df)
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
