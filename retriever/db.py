import json
from datetime import datetime, timezone
from enum import StrEnum

from retriever import orm


class Task(StrEnum):
    UPDATE_SHOWTIMES = "update-showtimes"
    DELETION_REPORT = "deletion-report"
    WATCHLIST_NOTIFICATIONS = "watchlist-notifications"
    GATHER_FANDANGO_SCREENS = "gather-fandango-screens"



def _cast_value(value):
    if isinstance(value, bool):
        return int(value)
    elif isinstance(value, list):
        return json.dumps(value)
    elif isinstance(value, set):
        return json.dumps(sorted(value))
    elif isinstance(value, dict):
        return json.dumps(value)
    else:
        return value

def showtime_key(theater, title, showing):
    return {
        "theater": theater,
        "title": title,
        "format": showing.fmt,
        "language": showing.language,
        "start_time": showing.start.isoformat(),
    }

def schedule_keys(schedule):
    return [showtime_key(schedule.theater, movie.name, showing) for movie in schedule.movies for showing in movie.showings]

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


def load_showtimes_by_create_time(first_create_time, last_create_time):
    where = {"create_time": [(">=", first_create_time), ("<=", last_create_time)]}
    with orm.connection() as conn:
        return _read_showtimes_query(conn.select("showtimes", where=where))


def store_showtimes(schedule, *, clean=True):
    db = orm.connect()
    cur = db.cursor()

    key_field_names = ("theater", "title", "format", "language", "start_time")
    key_field_names_str = ", ".join(key_field_names)
    field_names = key_field_names + ("end_time", "programs", "screen", "create_time", "id", "extra_properties")
    field_names_str = ", ".join(field_names)

    recheck = []
    create_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    for movie in schedule.movies:
        for showing in movie.showings:
            field_values = (
                schedule.theater,
                movie.name,
                showing.fmt,
                showing.language,
                showing.start.isoformat(),
                showing.end.isoformat(),
                json.dumps(sorted(showing.programs)),
                showing.screen,
                create_time,
                showing.id,
                json.dumps(showing.extra_properties)
            )

            cur.execute(f"""
                INSERT INTO showtimes({field_names_str})
                VALUES ({', '.join([_PH] * len(field_names))})
                ON CONFLICT({key_field_names_str}) DO NOTHING
                RETURNING *""",
                field_values
            )

            if not cur.fetchone():
                showing_dict = dict(zip(field_names, field_values))

                update_field_where_str = " and ".join([f"{field} = {_PH}" for field in key_field_names])
                update_field_set_str = f"extra_properties = {_PH}" + (f", id = {_PH}" if showing.id else "")
                update_field_base_values = (json.dumps(showing.extra_properties), ) + ((showing.id, ) if showing.id else ())
                update_field_values = update_field_base_values + tuple([_cast_value(showing_dict[field]) for field in key_field_names])

                cur.execute(f"""
                    UPDATE showtimes
                    SET {update_field_set_str}
                    WHERE {update_field_where_str}""",
                    update_field_values
                )

                recheck.append(showing_dict)

    db.commit()

    ### Update showtimes with missing runtimes ###
    # Movies entered into the DB with no runtime have their end time set to their start time. When
    # inserting new showtimes into the DB, end time isn't considered for identification, so even
    # when it's present, it's considered a conflict, and thus omitted.
    # To update it, we capture the inserts that don't do anything, check if they identify rows
    # without a runtime, then update their end_time as appropriate.
    # As this does not change create_time, they're omitted from the returned set of stored rows.
    cur.execute(f"""
        SELECT {key_field_names_str}
        FROM showtimes s
        WHERE s.create_time < {_PH} and s.start_time = s.end_time""",
        (create_time, )
    )

    showtimes_without_end = [dict(row) for row in cur.fetchall()]
    if showtimes_without_end:
        for showtime_dict in recheck:
            key_showtime_dict = {key: value for key, value in showtime_dict.items() if key in key_field_names}
            if key_showtime_dict in showtimes_without_end:
                update_field_where_str = " and ".join([f"{field} = {_PH}" for field in key_field_names])
                update_field_values = (showtime_dict["end_time"], ) + tuple([showtime_dict[field] for field in key_field_names])

                cur.execute(f"""
                    UPDATE showtimes
                    SET end_time = {_PH}
                    WHERE {update_field_where_str}""",
                    update_field_values)

        db.commit()

    cur.execute(f"""SELECT * FROM showtimes s WHERE s.create_time >= {_PH} ORDER BY s.title""", (create_time, ))

    return _read_showtimes_query([dict(row) for row in cur.fetchall()], clean=clean)


def delete_showtimes(showtimes_dicts):
    delete_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    with orm.connection() as conn:
        for showtime in showtimes_dicts:
            delete_field_names = ("id", "theater", "title", "format", "language", "programs", "start_time")
            insert_field_names = ("end_time", "extra_properties", "screen")

            where = {field: showtime[field] for field in delete_field_names if field in showtime}
            assign = where | {field: showtime[field] for field in insert_field_names if field in showtime} | {"delete_time": delete_time}
            
            conn.delete("showtimes", where)
            conn.insert("deleted_showtimes", assign)


def update_showtime_screens(hash_to_auditorium):
    with orm.connection() as conn:
        for hash_code, auditorium in hash_to_auditorium.items():
            conn.update("showtimes", {"screen": auditorium}, {"extra_properties": [("like", f"%{hash_code}%")]})


def load_deleted_showtimes_by_deletion_time(first_delete_time, last_delete_time, *, clean=True):
    where = {"delete_time": [(">=", first_delete_time), ("<=", last_delete_time)]}
    with orm.connection() as conn:
        raw_result = conn.select("deleted_showtimes", where=where, order_by="title")
    return _read_showtimes_query(raw_result, clean=clean)


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
        "programs": json.dumps(sorted(showtime["programs"])),
        "start_time": showtime["start_time"],
        "end_time": showtime["end_time"],
        "extra_properties": json.dumps(showtime["extra_properties"]),
        "create_time": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "client": client_id
    }
    
    with orm.connection() as conn:
        conn.insert("schedule", entry, conflict={("theater", "title", "format", "language", "start_time", "client"): None})


def remove_from_schedule(showtime, *, client_id):
    delete_field_names = ("theater", "title", "format", "language", "start_time")
    where = {field: showtime[field] for field in delete_field_names} | {"client": client_id}
    
    with orm.connection() as conn:
        conn.delete("schedule", where)


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
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(), 
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
            PRIMARY KEY(theater, title, format, language, start_time)
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
            PRIMARY KEY(theater, title, format, language, start_time, client)
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
