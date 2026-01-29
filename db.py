import os
import sqlite3

import psycopg2
from psycopg2.extras import RealDictCursor


def _connect():
    global _PH
    database_url = os.getenv('DATABASE_URL')
    if database_url:
        _PH = "%s"
        return psycopg2.connect(database_url, cursor_factory=RealDictCursor)
    else:
        _PH = "?"
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

def _init_db():
    db = _connect()
    cur = db.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS moviemetadata (
        title TEXT PRIMARY KEY,
        hidden INTEGER DEFAULT 0
    )""")

    db.commit()
    db.close()


_init_db()
