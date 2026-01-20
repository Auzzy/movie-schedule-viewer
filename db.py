from datetime import datetime
import sqlite3

_DB_FILENAME = "metadata.db"

def load_visibility():
    db = sqlite3.connect(_DB_FILENAME)
    db.row_factory = sqlite3.Row
    cur = db.cursor()
    cur.execute("SELECT title, hidden FROM moviemetadata")

    return {row["title"]: row["hidden"] == 0 for row in cur.fetchall()}
    

def hide_movie(title):
    db = sqlite3.connect(_DB_FILENAME)
    cur = db.cursor()
    cur.execute(
        "INSERT INTO moviemetadata(title, hidden) VALUES(:title, TRUE) ON CONFLICT(title) DO UPDATE SET hidden = TRUE",
        {"title": title}
    )

    db.commit()

def show_movie(title):
    db = sqlite3.connect(_DB_FILENAME)
    cur = db.cursor()
    cur.execute("UPDATE moviemetadata SET hidden = FALSE WHERE title = :title", {"title": title})
    db.commit()

def _init_db():
    db = sqlite3.connect(_DB_FILENAME)
    cur = db.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS moviemetadata (
        title TEXT PRIMARY KEY,
        hidden INTEGER DEFAULT FALSE
    )""")


_init_db()
