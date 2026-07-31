import json
from datetime import datetime, timedelta, timezone
from enum import StrEnum

from retriever import orm


class Task(StrEnum):
    UPDATE_SHOWTIMES = "update-showtimes"
    DELETION_REPORT = "deletion-report"
    WATCHLIST_NOTIFICATIONS = "watchlist-notifications"
    GATHER_FANDANGO_SCREENS = "gather-fandango-screens"



def _read_showtimes_query(raw_rows, *, clean=True):
    rows = []
    for row_dict in raw_rows:
        row_dict["programs"] = set(json.loads(row_dict["programs"] or "[]"))
        row_dict["extra_properties"] = json.loads(row_dict["extra_properties"] or "{}")
        if clean:
            if "create_time" in row_dict:
                del row_dict["create_time"]
            if "delete_time" in row_dict:
                del row_dict["delete_time"]
        rows.append(row_dict)
    return rows


def load_showtimes(first_time, last_time, theater=None, title=None, *, clean=True):
    where = {"theater": theater, "title": title, "start_time": [(">=", first_time), ("<=", last_time)]}
    with orm.connection() as conn:
        raw_result = conn.select("showtimes", where=where, order_by="title")
    return _read_showtimes_query(raw_result, clean=clean)


def load_showtimes_by_create_time(first_create_time, last_create_time=None, *, order_by=None, clean=True):
    create_time_filter = [(">=", first_create_time)] + ([("<=", last_create_time)] if last_create_time else [])
    where = {"create_time": create_time_filter}
    with orm.connection() as conn:
        raw_result = conn.select("showtimes", where=where, order_by=order_by)
    return _read_showtimes_query(raw_result, clean=clean)


def load_deleted_showtimes_by_delete_time(first_delete_time, last_delete_time=None, *, order_by=None, clean=True):
    delete_time_filter = [(">=", first_delete_time)] + ([("<=", last_delete_time)] if last_delete_time else [])
    where = {"delete_time": delete_time_filter}
    with orm.connection() as conn:
        raw_result = conn.select("deleted_showtimes", where=where, order_by=order_by)
    return _read_showtimes_query(raw_result, clean=clean)


def store_showtimes(schedule, *, clean=True):
    new_showtimes = []
    for movie in schedule.movies:
        for showing in movie.showings:
            if not showing.id:
                continue

            new_showtimes.append({
                "id": showing.id,
                "theater": schedule.theater,
                "title": movie.name,
                "format": showing.fmt,
                "language": showing.language,
                "start_time": showing.start,
                "end_time": showing.end,
                "programs": showing.programs,
                "screen": showing.screen,
                "extra_properties": showing.extra_properties
            })

    if not new_showtimes:
        # Maybe sub in the hash? But I'd need to solve the duplication that would occur if the ID disappears after being entered in the DB...
        print("The list of new showtimes was empty. This is likely due to the showtimes found lacking IDs.")
        return [], []

    with orm.connection() as conn:
        where = {"theater": schedule.theater, "start_time": [(">=", schedule.start), ("<=", schedule.end + timedelta(days=1))]}
        current_showtimes = _read_showtimes_query(conn.select("showtimes", where=where))
        current_showtimes_by_key = {(s["id"], s["theater"]): s for s in current_showtimes}

        now = datetime.now(timezone.utc).replace(microsecond=0)
        to_insert, to_delete = [], []
        for new_showtime in new_showtimes:
            current_showtime = current_showtimes_by_key.get((new_showtime["id"], new_showtime["theater"]))

            # TODO: Need to do something to handle screens, but I don't have time right now.
            current_showtime_screen = current_showtime.pop("screen", None) if current_showtime else None
            new_showtime_screen = new_showtime.pop("screen", None)
            if new_showtime != current_showtime:
                to_insert.append(new_showtime | {"create_time": now})
                if current_showtime:
                    to_delete.append(current_showtime | {"delete_time": now})

        if to_delete:
            conn.insert("deleted_showtimes", to_delete)
            conn.delete("showtimes", {"theater": schedule.theater, "id": [("in", [s["id"] for s in to_delete])]})

        if to_insert:
            conn.insert("showtimes", to_insert, conflict={("id", "theater"): None})

    showtimes = load_showtimes_by_create_time(now, order_by="title", clean=clean)
    deleted_showtimes = load_deleted_showtimes_by_delete_time(now, order_by="title", clean=clean)
    return showtimes, deleted_showtimes


def update_showtime_screens(hash_to_auditorium):
    with orm.connection() as conn:
        for hash_code, auditorium in hash_to_auditorium.items():
            conn.update("showtimes", {"screen": auditorium}, {"extra_properties": [("like", f"%{hash_code}%")]})


def load_visibility(*, client_id):
    where = {"client": client_id}
    with orm.connection() as conn:
        raw_result = conn.select("moviemetadata", columns=["title", "hidden"], where=where)
    return {row["title"]: row["hidden"] == 0 for row in raw_result}
    

def hide_movie(title, *, client_id):
    with orm.connection() as conn:
        conn.insert("moviemetadata", {"title": title, "hidden": 1, "client": client_id}, conflict={("title", "client"): {"hidden": 1}})


def show_movie(title, *, client_id):
    with orm.connection() as conn:
        conn.update("moviemetadata", {"hidden": 0}, {"title": title, "client": client_id})


def load_schedule(first_time, last_time, *, client_id):
    where = {"client": client_id, "start_time": [(">=", first_time), ("<=", last_time)]}
    with orm.connection() as conn:
        raw_result = conn.select("schedule", where=where, order_by="start_time")
    return _read_showtimes_query(raw_result)


def load_whole_schedule(*, client_id):
    where = {"client": client_id}
    with orm.connection() as conn:
        raw_result = conn.select("schedule", where=where, order_by="start_time")
    return _read_showtimes_query(raw_result, clean=False)


def add_to_schedule(showtime, *, client_id):
    entry = {
        "id": showtime["id"],
        "theater": showtime["theater"],
        "title": showtime["title"],
        "format": showtime["format"],
        "screen": showtime["screen"],
        "language": showtime["language"],
        "programs": showtime["programs"],
        "start_time": showtime["start_time"],
        "end_time": showtime["end_time"],
        "extra_properties": showtime["extra_properties"],
        "create_time": datetime.now(timezone.utc).replace(microsecond=0),
        "client": client_id
    }

    with orm.connection() as conn:
        conn.insert("schedule", entry, conflict={("id", "theater", "client"): None})


def remove_from_schedule(showtime, *, client_id):
    with orm.connection() as conn:
        conn.delete("schedule", where={"id": showtime["id"], "theater": showtime["theater"], "client": client_id})


def clear_schedule(first_time, last_time, *, client_id):
    where = {"client": client_id, "start_time": [(">=", first_time), ("<=", last_time)]}
    with orm.connection() as conn:
        conn.delete("schedule", where)


def theaters_last_update():
    columns = [
        "theater",
        "MAX(create_time) last_update_time"
    ]
    with orm.connection() as conn:
        raw_result = conn.select("showtimes", columns, group_by="theater")
    return {row["theater"]: row["last_update_time"] for row in raw_result}


def add_theater(name, fullname, code, tzname, is_open, rank, parser, query):
    code = code.lower() if code is not None else None

    info = {
        "name": name,
        "fullname": fullname,
        "code": code,
        "tzname": tzname,
        "isopen": int(bool(is_open)),
        "rank": rank,
        "parser": parser,
        "query": query
    }
    with orm.connection() as conn:
        conn.insert("theater", info)


def get_theaters(*, is_open=None, clean=True):
    where = {"isopen": int(bool(is_open))} if is_open is not None else {}
    rows = []
    with orm.connection() as conn:
        raw_result = conn.select("theater", where=where, order_by="rank")

    for row_dict in raw_result:
        row_dict["is_open"] = row_dict["isopen"] == 1
        if clean:
            del row_dict["parser"]
            
        rows.append(row_dict)
    return rows


def get_theater(name):
    with orm.connection() as conn:
        row_dict = conn.selectone("theater", where={"name": name, "isopen": 1})

    return {**row_dict, "is_open": row_dict["isopen"] == 1} if row_dict else {}


def load_watchlist(client_id):
    where = {"client": client_id}
    with orm.connection() as conn:
        return conn.select("watchlist", where=where, order_by="title")


def load_all_watchlists():
    with orm.connection() as conn:
        return conn.select("watchlist", order_by="title")


def add_to_watchlist(title, *, client_id):
    entry = {"title": title, "client": client_id}
    with orm.connection() as conn:
        conn.insert("watchlist", entry, conflict={tuple(entry.keys()): None})


def remove_from_watchlist(title, *, client_id):
    with orm.connection() as conn:
        conn.delete("watchlist", {"title": title, "client": client_id})


def log_task(name, start_time, end_time, success):
    if name not in list(Task):
        raise ValueError(f"\"name\" must be one of: {list(Task)}")

    info = {
        "name":name,
        "start_time": start_time,
        "end_time": end_time,
        "success": int(bool(success))
    }
    with orm.connection() as conn:
        conn.insert("task_log", info)


def last_successful_task_run(name):
    with orm.connection() as conn:
        raw_result = conn.selectone("task_log", ["max(start_time) last_run"], {"name": name, "success": 1})

    last_run_str = raw_result.get("last_run")
    return datetime.fromisoformat(last_run_str) if last_run_str else None


def _init_db():
    with orm.connection() as conn:
        cur = conn.db.cursor()

        cur.execute("""CREATE TABLE IF NOT EXISTS showtimes (
            id TEXT,
            theater TEXT NOT NULL,
            title TEXT NOT NULL,
            format TEXT NOT NULL,
            screen TEXT,
            language TEXT NOT NULL,
            programs TEXT,
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL,
            extra_properties TEXT,
            create_time TEXT NOT NULL,
            PRIMARY KEY(id, theater)
        )""")

        # I could do this as a soft delete from showtimes. But this allows
        # capturing any instance of them re-adding the exact same showtime.
        cur.execute("""CREATE TABLE IF NOT EXISTS deleted_showtimes (
            id TEXT,
            autoid BIGSERIAL PRIMARY KEY,
            theater TEXT NOT NULL,
            title TEXT NOT NULL,
            format TEXT NOT NULL,
            screen TEXT,
            language TEXT NOT NULL,
            programs TEXT,
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL,
            extra_properties TEXT,
            delete_time TEXT NOT NULL
        )""")

        cur.execute("""CREATE TABLE IF NOT EXISTS moviemetadata (
            title TEXT NOT NULL,
            hidden INTEGER DEFAULT 0,
            client TEXT NOT NULL,
            PRIMARY KEY(title, client)
        )""")

        cur.execute("""CREATE TABLE IF NOT EXISTS schedule (
            id TEXT,
            theater TEXT NOT NULL,
            title TEXT NOT NULL,
            format TEXT NOT NULL,
            screen TEXT,
            language TEXT NOT NULL,
            programs TEXT,
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL,
            create_time TEXT NOT NULL,
            extra_properties TEXT,
            client TEXT NOT NULL,
            PRIMARY KEY(id, theater, client)
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
            client TEXT NOT NULL,
            PRIMARY KEY(title, client)
        )""")

        cur.execute("""CREATE TABLE IF NOT EXISTS task_log (
            name TEXT NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL,
            success INT NOT NULL,
            PRIMARY KEY(name, start_time)
        )""")


_init_db()
