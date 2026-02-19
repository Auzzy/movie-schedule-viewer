import os
import sqlite3
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import RealDictCursor


def _connect():
    global _DATETIME, _PH
    database_url = os.getenv('DATABASE_URL')
    if database_url:
        _PH = "%s"
        _DATETIME = "::timestamptz"
        return psycopg2.connect(database_url, cursor_factory=RealDictCursor)
    else:
        _PH = "?"
        _DATETIME = ""
        db = sqlite3.connect("metadata.db")
        db.row_factory = sqlite3.Row
        return db


def load_visibility():
    db = _connect()
    cur = db.cursor()
    cur.execute("SELECT title, hidden FROM moviemetadata")
    result = cur.fetchall()
    db.close()

    return {row["title"]: row["hidden"] == 0 for row in result}
    

def hide_movie(title):
    db = _connect()
    cur = db.cursor()
    cur.execute(
        f"INSERT INTO moviemetadata(title, hidden) VALUES({_PH}, 1) ON CONFLICT(title) DO UPDATE SET hidden = 1",
        (title,)
    )

    db.commit()
    db.close()


def show_movie(title):
    db = _connect()
    cur = db.cursor()
    cur.execute(f"UPDATE moviemetadata SET hidden = 0 WHERE title = {_PH}", (title,))
    db.commit()
    db.close()


def load_schedule(first_time, last_time):
    db = _connect()
    cur = db.cursor()

    query_params = (first_time, last_time)

    cur.execute(f"""
        SELECT *
        FROM schedule s
        WHERE s.start_time{_DATETIME} >= {_PH} AND s.start_time{_DATETIME} <= {_PH}
        ORDER BY s.start_time""",
        query_params
    )

    rows = []
    for row in cur.fetchall():
        row_dict = dict(row)
        row_dict["is_open_caption"] = row["is_open_caption"] == 1
        row_dict["no_alist"] = row["no_alist"] == 1
        rows.append(row_dict)
    return rows

def add_to_schedule(showtime):
    db = _connect()
    cur = db.cursor()

    create_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    field_names = ("theater", "title", "format", "is_open_caption", "no_alist", "start_time", "end_time", "create_time")
    field_names_str = ", ".join(field_names)
    field_values = (
        showtime["theater"],
        showtime["title"],
        showtime["format"],
        int(showtime["is_open_caption"]),
        int(showtime["no_alist"]),
        showtime["start_time"],
        showtime["end_time"],
        create_time
    )
    
    cur.execute(f"""
        INSERT INTO schedule({field_names_str})
        VALUES ({', '.join([_PH] * len(field_names))})
        ON CONFLICT(theater, title, format, is_open_caption, no_alist, start_time) DO NOTHING""",
        field_values
    )
            
    db.commit()
    db.close()

def remove_from_schedule(showtime):
    db = _connect()
    cur = db.cursor()

    delete_field_names = ("theater", "title", "format", "is_open_caption", "no_alist", "start_time")
    delete_field_where_str = " and ".join([f"{field} = {_PH}" for field in delete_field_names])
    delete_field_raw_values = tuple([showtime[field] for field in delete_field_names])
    delete_field_values = tuple([int(value) if isinstance(value, bool) else value for value in delete_field_raw_values])
    cur.execute(f"DELETE FROM schedule WHERE {delete_field_where_str}", delete_field_values)
    
    db.commit()
    db.close()


def clear_schedule(first_time, last_time):
    db = _connect()
    cur = db.cursor()

    query_params = (first_time, last_time)

    cur.execute(f"""
        DELETE FROM schedule
        WHERE start_time{_DATETIME} >= {_PH} AND start_time{_DATETIME} <= {_PH}""",
        query_params
    )

    db.commit()
    db.close()


def _init_db():
    db = _connect()
    cur = db.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS moviemetadata (
        title TEXT PRIMARY KEY,
        hidden INTEGER DEFAULT 0
    )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS schedule (
        theater TEXT NOT NULL,
        title TEXT NOT NULL,
        format TEXT,
        is_open_caption INT NOT NULL,
        no_alist INT NOT NULL,
        start_time TEXT NOT NULL,
        end_time TEXT NOT NULL,
        create_time TEXT NOT NULL,
        PRIMARY KEY(theater, title, format, is_open_caption, no_alist, start_time)
    )""")

    db.commit()
    db.close()


_init_db()
