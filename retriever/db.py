import json
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

def _cast_value(value):
    if isinstance(value, bool):
        return int(value)
    elif isinstance(value, list):
        return json.dumps(value)
    else:
        return value

def serialize_showing(theater, title, showing):
    return {
        "theater": theater,
        "title": title,
        "format": showing.fmt,
        "is_open_caption": bool(showing.is_open_caption),
        "no_alist": bool(showing.no_alist),
        "language": showing.language,
        "programs": sorted(showing.programs),
        "start_time": showing.start.isoformat(),
        "end_time": showing.end.isoformat()
    }

def serialize_schedule(schedule):
    return [serialize_showing(schedule.theater, movie.name, showing) for movie in schedule.movies for showing in movie.showings]

def _read_showtimes_query(raw_rows, *, clean=True):
    rows = []
    for row in raw_rows:
        row_dict = dict(row)
        row_dict["is_open_caption"] = row["is_open_caption"] == 1
        row_dict["no_alist"] = row["no_alist"] == 1
        row_dict["programs"] = json.loads(row["programs"] or "[]")
        if clean:
            del row_dict["create_time"]
        rows.append(row_dict)
    return rows

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

    return _read_showtimes_query(cur.fetchall())

def store_showtimes(schedule, *, clean=True):
    db = _connect()
    cur = db.cursor()

    create_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    for movie in schedule.movies:
        for showing in movie.showings:
            field_names = ("theater", "title", "format", "is_open_caption", "no_alist", "language", "programs", "start_time", "end_time", "create_time")
            field_names_str = ", ".join(field_names)
            field_values = (
                schedule.theater,
                movie.name,
                showing.fmt,
                int(showing.is_open_caption),
                int(bool(showing.no_alist)),
                showing.language,
                json.dumps(sorted(showing.programs)),
                showing.start.isoformat(),
                showing.end.isoformat(),
                create_time
            )

            cur.execute(f"""
                INSERT INTO showtimes({field_names_str})
                VALUES ({', '.join([_PH] * len(field_names))})
                ON CONFLICT(theater, title, format, is_open_caption, no_alist, language, start_time) DO NOTHING""",
                field_values
            )

    db.commit()

    cur.execute(f"""SELECT * FROM showtimes s WHERE s.create_time >= {_PH} ORDER BY s.title""", (create_time, ))

    return _read_showtimes_query(cur.fetchall())

def delete_showtimes(showtimes_dicts):
    db = _connect()
    cur = db.cursor()

    delete_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    for showtime in showtimes_dicts:
        delete_field_names = ("theater", "title", "format", "is_open_caption", "no_alist", "language", "programs", "start_time")
        delete_field_where_str = " and ".join([f"{field} = {_PH}" for field in delete_field_names])
        delete_field_raw_values = tuple([showtime[field] for field in delete_field_names])
        delete_field_values = tuple([_cast_value(value) for value in delete_field_raw_values])
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
        row_dict["programs"] = json.loads(row["programs"] or "[]")
        if clean:
            del row_dict["delete_time"]
            del row_dict["id"]
        rows.append(row_dict)
    return rows


def load_visibility(*, client_id):
    db = _connect()
    cur = db.cursor()
    cur.execute(f"SELECT title, hidden FROM moviemetadata where client = {_PH}", (client_id, ))
    result = cur.fetchall()
    db.close()

    return {row["title"]: row["hidden"] == 0 for row in result}
    

def hide_movie(title, *, client_id):
    db = _connect()
    cur = db.cursor()
    cur.execute(
        f"INSERT INTO moviemetadata(title, hidden, client) VALUES({_PH}, 1, {_PH}) ON CONFLICT(title, client) DO UPDATE SET hidden = 1",
        (title, client_id)
    )

    db.commit()
    db.close()


def show_movie(title, *, client_id):
    db = _connect()
    cur = db.cursor()
    cur.execute(f"UPDATE moviemetadata SET hidden = 0 WHERE title = {_PH} AND client = {_PH}", (title, client_id))
    db.commit()
    db.close()


def load_schedule(first_time, last_time, *, client_id):
    db = _connect()
    cur = db.cursor()

    query_params = (client_id, first_time, last_time)

    cur.execute(f"""
        SELECT *
        FROM schedule s
        WHERE s.client = {_PH} AND s.start_time{_DATETIME} >= {_PH} AND s.start_time{_DATETIME} <= {_PH}
        ORDER BY s.start_time""",
        query_params
    )

    rows = []
    for row in cur.fetchall():
        row_dict = dict(row)
        row_dict["is_open_caption"] = row["is_open_caption"] == 1
        row_dict["no_alist"] = row["no_alist"] == 1
        row_dict["programs"] = json.loads(row["programs"] or "[]")
        rows.append(row_dict)
    return rows

def add_to_schedule(showtime, *, client_id):
    db = _connect()
    cur = db.cursor()

    create_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    field_names = ("theater", "title", "format", "is_open_caption", "no_alist", "language", "programs", "start_time", "end_time", "create_time", "client")
    field_names_str = ", ".join(field_names)
    field_values = (
        showtime["theater"],
        showtime["title"],
        showtime["format"],
        int(showtime["is_open_caption"]),
        int(bool(showtime["no_alist"])),
        showtime["language"],
        json.dumps(sorted(showtime["programs"])),
        showtime["start_time"],
        showtime["end_time"],
        create_time,
        client_id
    )
    
    cur.execute(f"""
        INSERT INTO schedule({field_names_str})
        VALUES ({', '.join([_PH] * len(field_names))})
        ON CONFLICT(theater, title, format, is_open_caption, no_alist, language, start_time, client) DO NOTHING""",
        field_values
    )
            
    db.commit()
    db.close()

def remove_from_schedule(showtime, *, client_id):
    db = _connect()
    cur = db.cursor()

    delete_field_names = ("theater", "title", "format", "is_open_caption", "no_alist", "language", "start_time")
    delete_field_where_str = " and ".join([f"{field} = {_PH}" for field in delete_field_names])
    delete_field_raw_values = tuple([showtime[field] for field in delete_field_names])
    delete_field_values = tuple([_cast_value(value) for value in delete_field_raw_values])
    cur.execute(f"DELETE FROM schedule WHERE {delete_field_where_str} and client = {_PH}", delete_field_values + (client_id,))
    
    db.commit()
    db.close()


def clear_schedule(first_time, last_time, *, client_id):
    db = _connect()
    cur = db.cursor()

    query_params = (client_id, first_time, last_time)

    cur.execute(f"""
        DELETE FROM schedule
        WHERE client = {_PH} AND start_time{_DATETIME} >= {_PH} AND start_time{_DATETIME} <= {_PH}""",
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
        GROUP BY theater
    """)

    return {row["theater"]: row["last_update_time"] for row in cur.fetchall()}


def add_theater(name, fullname, code, tzname, is_open, rank, parser, query):
    db = _connect()
    cur = db.cursor()

    code = code.lower() if code is not None else None

    cur.execute(f"""
        INSERT INTO theater(name, fullname, code, tzname, isopen, rank, parser, query)
        VALUES ({_PH}, {_PH}, {_PH}, {_PH}, {_PH}, {_PH}, {_PH}, {_PH})
        ON CONFLICT(name) DO NOTHING""",
        (name, fullname, code, tzname, int(bool(is_open)), rank, parser, query)
    )

    db.commit()
    db.close()


def get_theaters(*, is_open=None, clean=True):
    db = _connect()
    cur = db.cursor()

    where_clause = ""
    if is_open is not None:
        where_clause = f"WHERE isopen = {int(bool(is_open))}"

    cur.execute(f"""SELECT * FROM theater {where_clause} ORDER BY rank""")

    rows = []
    for row in cur.fetchall():
        row_dict = dict(row)
        row_dict["is_open"] = row["isopen"] == 1
        if clean:
            del row_dict["parser"]
            
        rows.append(row_dict)
    return rows


def get_theater(name):
    db = _connect()
    cur = db.cursor()

    cur.execute(f"""SELECT * FROM theater WHERE name = {_PH} AND isopen = 1""", (name, ))

    row_dict = dict(cur.fetchone() or {})
    if row_dict:
        row_dict["is_open"] = row_dict["isopen"] == 1
        return row_dict
    else:
        return {}


def load_watchlist(client_id):
    db = _connect()
    cur = db.cursor()

    query_params = (client_id, )

    cur.execute(f"""
        SELECT *
        FROM watchlist w
        WHERE w.client = {_PH}
        ORDER BY w.title""",
        query_params
    )

    return [dict(row) for row in cur.fetchall()]


def load_all_watchlists():
    db = _connect()
    cur = db.cursor()

    cur.execute(f"""
        SELECT *
        FROM watchlist w
        ORDER BY w.title
    """)

    return [dict(row) for row in cur.fetchall()]


def watchlist_mark_sent(theater_title_pairs):
    theater_title_pairs = [(theater, title.lower()) for theater, title in theater_title_pairs]
    sent_pairs = [(row["theater"], row["title"]) for row in load_all_watchlists() if (row["theater"], row["title"].lower()) in theater_title_pairs]
    
    db = _connect()
    cur = db.cursor()

    sent_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    sent_pairs_str = ", ".join(repr(p) for p in sent_pairs)

    cur.execute(f"""
        UPDATE watchlist
        SET sent_time = {_PH}
        WHERE (theater, title) IN ({sent_pairs_str})""",
        (sent_time, ))

    db.commit()
    db.close()


def add_to_watchlist(title, theater , *, client_id):
    db = _connect()
    cur = db.cursor()

    cur.execute(f"""
        INSERT INTO watchlist(title, theater, client)
        VALUES ({_PH}, {_PH}, {_PH})
        ON CONFLICT(title, theater, client) DO NOTHING""",
        (title, theater, client_id)
    )

    db.commit()
    db.close()


def remove_from_watchlist(title, theater , *, client_id):
    db = _connect()
    cur = db.cursor()

    cur.execute(f"""
        DELETE FROM watchlist
        WHERE title = {_PH} and theater = {_PH} and client = {_PH}""",
        (title, theater, client_id)
    )

    db.commit()
    db.close()


def _init_db():
    db = _connect()
    cur = db.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS showtimes (
        theater TEXT NOT NULL,
        title TEXT NOT NULL,
        format TEXT,
        is_open_caption INT NOT NULL,
        no_alist INT NOT NULL,
        language TEXT NOT NULL,
        programs TEXT,
        start_time TEXT NOT NULL,
        end_time TEXT NOT NULL,
        create_time TEXT NOT NULL,
        PRIMARY KEY(theater, title, format, is_open_caption, no_alist, language, start_time)
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
        language TEXT NOT NULL,
        programs TEXT,
        start_time TEXT NOT NULL,
        end_time TEXT NOT NULL,
        delete_time TEXT NOT NULL
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS moviemetadata (
        title TEXT NOT NULL,
        hidden INTEGER DEFAULT 0,
        client TEXT NOT NULL,
        PRIMARY KEY(title, client)
    )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS schedule (
        theater TEXT NOT NULL,
        title TEXT NOT NULL,
        format TEXT,
        is_open_caption INT NOT NULL,
        no_alist INT NOT NULL,
        language TEXT NOT NULL,
        programs TEXT,
        start_time TEXT NOT NULL,
        end_time TEXT NOT NULL,
        create_time TEXT NOT NULL,
        client TEXT NOT NULL,
        PRIMARY KEY(theater, title, format, is_open_caption, no_alist, language, start_time, client)
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS theater (
        name TEXT PRIMARY KEY,
        fullname TEXT NOT NULL,
        code TEXT,
        tzname TEXT NOT NULL,
        isopen INTEGER NOT NULL,
        rank INTEGER,
        parser TEXT NOT NULL,
        query TEXT
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS watchlist (
        title TEXT NOT NULL,
        theater TEXT NOT NULL,
        sent_time TEXT,
        client TEXT NOT NULL,
        PRIMARY KEY(title, theater, client)
    )""")

    db.commit()
    db.close()


_init_db()
