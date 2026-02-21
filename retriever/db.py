import os
from datetime import datetime, timezone
import sqlite3

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
        db = sqlite3.connect("showtimes.db")
        db.row_factory = sqlite3.Row
        return db

def load_showtimes(theater, first_time, last_time, title=None, *, clean=True):
    db = _connect()
    cur = db.cursor()

    where_title = ""
    query_params = (theater, first_time, last_time)
    if title:
        where_title = f" AND s.title = {_PH}"
        query_params += (title, )

    cur.execute(f"""
        SELECT *
        FROM showtimes s
        WHERE s.theater = {_PH} AND s.start_time{_DATETIME} >= {_PH} AND s.start_time{_DATETIME} <= {_PH}{where_title}
        ORDER BY s.title""",
        query_params
    )

    rows = []
    for row in cur.fetchall():
        row_dict = dict(row)
        row_dict["is_open_caption"] = row["is_open_caption"] == 1
        row_dict["no_alist"] = row["no_alist"] == 1
        if clean:
            del row_dict["create_time"]
        rows.append(row_dict)
    return rows

def store_showtimes(theater, schedule, *, clean=True):
    db = _connect()
    cur = db.cursor()

    create_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    inserted = []
    for movie in schedule.movies:
        for showing in movie.showings:
            field_names = ("theater", "title", "format", "is_open_caption", "no_alist", "start_time", "end_time", "create_time")
            field_names_str = ", ".join(field_names)
            field_values = (
                theater,
                movie.name,
                showing.fmt,
                int(showing.is_open_caption),
                int(showing.no_alist),
                showing.start.isoformat(),
                showing.end.isoformat(),
                create_time
            )
            
            cur.execute(f"""
                INSERT INTO showtimes({field_names_str})
                VALUES ({', '.join([_PH] * len(field_names))})
                ON CONFLICT(theater, title, format, is_open_caption, no_alist, start_time) DO NOTHING""",
                field_values
            )
            
            inserted_dict = dict(zip(field_names, field_values))
            inserted_dict["is_open_caption"] = inserted_dict["is_open_caption"] == 1
            inserted_dict["no_alist"] = inserted_dict["no_alist"] == 1
            if clean:
                del inserted_dict["create_time"]
            inserted.append(inserted_dict)

    db.commit()
    db.close()

    return inserted

def delete_showtimes(showtimes_dicts):
    db = _connect()
    cur = db.cursor()

    delete_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    for showtime in showtimes_dicts:
        delete_field_names = ("theater", "title", "format", "is_open_caption", "no_alist", "start_time")
        delete_field_where_str = " and ".join([f"{field} = {_PH}" for field in delete_field_names])
        delete_field_raw_values = tuple([showtime[field] for field in delete_field_names])
        delete_field_values = tuple([int(value) if isinstance(value, bool) else value for value in delete_field_raw_values])
        cur.execute(f"DELETE FROM showtimes WHERE {delete_field_where_str}", delete_field_values)

        new_insert_field_names = ("end_time", "delete_time")
        insert_field_names = delete_field_names + new_insert_field_names
        insert_field_names_str = ", ".join(insert_field_names)
        insert_field_values = delete_field_values + tuple([showtime[field] for field in new_insert_field_names[:-1]])
        cur.execute(f"""
            INSERT INTO deleted_showtimes({insert_field_names_str})
            VALUES ({', '.join([_PH] * len(insert_field_names))})""",
            tuple(insert_field_values) + (delete_time,)
        )

    db.commit()
    db.close()

def load_deleted_showtimes(first_delete_time, last_delete_time, *, clean=True):
    db = _connect()
    cur = db.cursor()

    cur.execute(f"""
        SELECT *
        FROM deleted_showtimes s
        WHERE s.delete_time{_DATETIME} >= {_PH} AND s.delete_time{_DATETIME} <= {_PH}
        ORDER BY s.title""",
        (first_delete_time, last_delete_time)
    )

    rows = []
    for row in cur.fetchall():
        row_dict = dict(row)
        row_dict["is_open_caption"] = row["is_open_caption"] == 1
        row_dict["no_alist"] = row["no_alist"] == 1
        if clean:
            del row_dict["delete_time"]
            del row_dict["id"]
        rows.append(row_dict)
    return rows


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


def theaters_last_update():
    db = _connect()
    cur = db.cursor()
    
    cur.execute("""
        SELECT theater, MAX(create_time) as last_update_time
        FROM showtimes
        GROUP BY theater"""
    )

    return {row["theater"]: row["last_update_time"] for row in cur.fetchall()}


def _init_db():
    db = _connect()
    cur = db.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS showtimes (
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

    # I could do this as a soft delete from showtimes. But this allows
    # capturing any instance of them re-adding the exact same showtime.
    cur.execute("""CREATE TABLE IF NOT EXISTS deleted_showtimes (
        id BIGSERIAL PRIMARY KEY,
        theater TEXT NOT NULL,
        title TEXT NOT NULL,
        format TEXT,
        is_open_caption INT NOT NULL,
        no_alist INT NOT NULL,
        start_time TEXT NOT NULL,
        end_time TEXT NOT NULL,
        delete_time TEXT NOT NULL
    )""")

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
