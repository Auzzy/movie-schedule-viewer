import itertools
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Annotated, Any
from zoneinfo import ZoneInfo

from fastapi import Body, FastAPI, Cookie, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from ical.calendar import Calendar
from ical.calendar_stream import IcsCalendarStream
from ical.event import Event
from pydantic import BaseModel

from retriever import db
from retriever.movie_times_lib import collect_schedule, db_showtime_updates, \
        send_error_email, send_deletion_report, send_watchlist_notification
from retriever.schedule import Filter, FullSchedule
from retriever.utils import offset_timezone


app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost",
        "http://localhost:8080",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Type"]
)

templates = Jinja2Templates(directory=".", trim_blocks=True, lstrip_blocks=True)


def _check_write_permission(client_id):
    if not client_id:
        raise ValueError("No client ID included in request. Please request one and add it to your browser's local storage.")

    clients = os.environ.get("MOVIE_VIEWER_CLIENTS", "").split(',')
    if client_id not in clients:
        raise RuntimeError("Unauthorized client.")


def _showtimes_to_ics(showtimes):
    calendar = Calendar()
    for showtime in showtimes:
        description = showtime["format"]
        if showtime["is_open_caption"]:
            description += ", Open Caption"
        if showtime["no_alist"]:
            description += ", No A-List"
        calendar.events.append(
            Event(
                summary=showtime["title"],
                description=description,
                location=showtime["theater"],
                start=datetime.fromisoformat(showtime["start_time"]),
                end=datetime.fromisoformat(showtime["end_time"]),
            )
        )

    return IcsCalendarStream.calendar_to_ics(calendar)


def _load_visibility(theater, first_time, last_time, *, client_id):
    showtimes = _load_showtimes(theater, first_time, last_time)
    visibility = db.load_visibility(client_id=client_id) if client_id else {}

    titles = {s["title"] for s in showtimes}
    return {title: visibility.get(title, True) for title in titles}


def _load_showtimes(theater, first_time, last_time, title=None):
    last_time = last_time or first_time
    return db.load_showtimes(theater, first_time, last_time, title)


@app.get("/", response_class=HTMLResponse)
def read_root(request: Request, client_id: Annotated[str | None, Cookie()] = None):
    if client_id is None:
        allow_editing = None
    else:
        try:
            _check_write_permission(client_id)
            allow_editing = True
        except (ValueError, RuntimeError):
            allow_editing = False

    context = {"allow_editing": allow_editing}
    return templates.TemplateResponse(request=request, name="index.html", context=context)


@app.get("/showtimes/{theater}/{first_time}/{last_time}")
def request_showtimes(theater: str, first_time: datetime, last_time: datetime):
    showtimes = _load_showtimes(theater, first_time, last_time)
    return {"showtimes": showtimes}

@app.get("/showtimes/{theater}/{first_time}/{last_time}/visibility")
def request_visibility(theater: str, first_time: datetime, last_time: datetime, client_id: Annotated[str | None, Cookie()] = None):
    visibility = _load_visibility(theater, first_time, last_time, client_id=client_id)
    return {"visibility": visibility}

@app.put("/movies/{title}/hide")
def request_hide_movie(title, client_id: Annotated[str | None, Cookie()] = None):
    _check_write_permission(client_id)

    db.hide_movie(title, client_id=client_id)
    return {}

@app.put("/movies/{title}/show")
def request_show_movie(title, client_id: Annotated[str | None, Cookie()] = None):
    _check_write_permission(client_id)

    db.show_movie(title, client_id=client_id)
    return {}

@app.post("/export-ics")
def request_export_ics(payload: dict[str, Any], client_id: Annotated[str | None, Cookie()] = None):
    _check_write_permission(client_id)

    ics_stream = _showtimes_to_ics(payload["showtimes"])
    return Response(content=ics_stream, media_type="text/calendar")

@app.get("/schedule/{first_time}/{last_time}")
def load_schedule(first_time: datetime, last_time: datetime, client_id: Annotated[str | None, Cookie()] = None):
    _check_write_permission(client_id)

    schedule = db.load_schedule(first_time, last_time, client_id=client_id)
    return {
        "schedule": schedule
    }

@app.post("/schedule/{first_time}/{last_time}/clear")
def clear_schedule(first_time: datetime, last_time: datetime, client_id: Annotated[str | None, Cookie()] = None):
    _check_write_permission(client_id)

    schedule = db.clear_schedule(first_time, last_time, client_id=client_id)
    return {}

@app.post("/schedule/new-showtime")
def add_showtime_to_schedule(showtime: dict[str, Any], client_id: Annotated[str | None, Cookie()] = None):
    _check_write_permission(client_id)

    db.add_to_schedule(showtime, client_id=client_id)
    return {}

@app.post("/schedule/remove-showtime")
def remove_showtime_from_schedule(showtime: dict[str, Any], client_id: Annotated[str | None, Cookie()] = None):
    _check_write_permission(client_id)

    db.remove_from_schedule(showtime, client_id=client_id)
    return {}

@app.get("/theaters")
def request_theaters():
    theaters = db.get_theaters(is_open=True)
    return {"names": [info["name"] for info in theaters]}

@app.get("/theaters/last-updated")
def request_theaters_last_updated():
    theaters_last_update = db.theaters_last_update()
    updates_in_local_tz = {}
    for theater, last_update_utc_str in theaters_last_update.items():
        last_update_utc = datetime.fromisoformat(last_update_utc_str)
        theater_info = db.get_theater(theater)
        if not theater_info:
            print(f"[ERROR] There should not be showtimes in the DB for theaters that are not also in the DB.")
            continue

        last_update_tz = last_update_utc.astimezone(offset_timezone(theater_info["tzname"]))
        updates_in_local_tz[theater] = last_update_tz.isoformat()

    return {"updates": updates_in_local_tz}

@app.get("/watchlist")
def request_watchlist(client_id: Annotated[str | None, Cookie()] = None):
    _check_write_permission(client_id)

    watchlist = defaultdict(list)
    for watchlist_entry in db.load_watchlist(client_id):
        watchlist[watchlist_entry["title"]].append({
            "theater": watchlist_entry["theater"],
            "sent": watchlist_entry["sent_time"]
        })

    return dict(watchlist)


@app.post("/watchlist/new")
def add_new_watchlist_entry(title: Annotated[str, Body()], theaters: Annotated[list[str], Body()], client_id: Annotated[str | None, Cookie()] = None):
    _check_write_permission(client_id)

    theaters_response = []
    for theater_name in theaters:
        db.add_to_watchlist(title, theater_name, client_id=client_id)
        theaters_response.append({"theater": theater_name, "sent": None})
    return {title: theaters_response}


@app.post("/watchlist/add")
def add_to_watchlist(title: Annotated[str, Body()], theater: Annotated[str, Body()], client_id: Annotated[str | None, Cookie()] = None):
    _check_write_permission(client_id)

    db.add_to_watchlist(title, theater, client_id=client_id)
    return {}


@app.post("/watchlist/remove")
def remove_from_watchlist(title: Annotated[str, Body()], theater: Annotated[str, Body()], client_id: Annotated[str | None, Cookie()] = None):
    _check_write_permission(client_id)

    db.remove_from_watchlist(title, theater, client_id=client_id)
    return {}


@app.get("/update-showtimes")
def scan():
    try:
        print(f"Update starting at {datetime.now(timezone.utc)} UTC")

        theaters_to_scan = os.environ.get("MOVIE_VIEWER_THEATERS", "").split(",")
        schedules = []
        for theater in theaters_to_scan:
            tz = offset_timezone(db.get_theater(theater)["tzname"])
            today = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
            date_range = (today, today + timedelta(weeks=4))

            print(f"Updating the showtimes for {theater} between {date_range[0].isoformat()} and {date_range[1].isoformat()}...")
            schedule = collect_schedule(theater, None, date_range, Filter.empty(), True)
            if schedule:
                schedules.append(schedule)
                stored = db.store_showtimes(schedule)
                db_showtime_updates(theater, date_range, schedule)

        send_watchlist_notification(schedules)

        return {"success": True}
    except Exception as exc:
        send_error_email(exc)

@app.get("/send-deletion-report")
def scan_deletions():
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)

    send_deletion_report(yesterday)
