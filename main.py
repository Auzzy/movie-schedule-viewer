import itertools
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import Body, FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from ical.calendar import Calendar
from ical.calendar_stream import IcsCalendarStream
from ical.event import Event
from pydantic import BaseModel

sys.path.append("scheduleretriever")
from retriever import db as retrieverdb, theaters
from retriever.fandango_json import load_schedules_by_day
from retriever.movie_times_lib import collect_schedule, db_showtime_updates, send_error_email, send_deletion_report
from retriever.schedule import Filter, FullSchedule

import db as viewerdb


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


def _load_visibility(theater, first_time, last_time):
    showtimes = _load_showtimes(theater, first_time, last_time)
    visibility = viewerdb.load_visibility()

    titles = {s["title"] for s in showtimes}
    return {title: visibility.get(title, True) for title in titles}


def _load_showtimes(theater, first_time, last_time, title=None):
    last_time = last_time or first_time
    return retrieverdb.load_showtimes(theater, first_time, last_time, title)


@app.get("/", response_class=HTMLResponse)
def read_root():
    with open("index.html") as homepage_html:
        return homepage_html.read()

@app.get("/showtimes/{theater}/{first_time}/{last_time}")
def request_showtimes(theater: str, first_time: datetime, last_time: datetime):
    showtimes = _load_showtimes(theater, first_time, last_time)
    return {"showtimes": showtimes}

@app.get("/showtimes/{theater}/{first_time}/{last_time}/visibility")
def request_visibility(theater: str, first_time: datetime, last_time: datetime):
    visibility = _load_visibility(theater, first_time, last_time)
    return {"visibility": visibility}

@app.put("/movies/{title}/hide")
def request_hide_movie(title):
    viewerdb.hide_movie(title)
    return {}

@app.put("/movies/{title}/show")
def request_show_movie(title):
    viewerdb.show_movie(title)
    return {}

@app.post("/export-ics")
def request_export_ics(payload: dict[str, Any]):
    ics_stream = _showtimes_to_ics(payload["showtimes"])
    return Response(content=ics_stream, media_type="text/calendar")

@app.get("/schedule/{first_time}/{last_time}")
def load_schedule(first_time: datetime, last_time: datetime):
    schedule = viewerdb.load_schedule(first_time, last_time)
    return {
        "schedule": schedule
    }

@app.post("/schedule/{first_time}/{last_time}/clear")
def clear_schedule(first_time: datetime, last_time: datetime):
    schedule = viewerdb.clear_schedule(first_time, last_time)
    return {}

@app.post("/schedule/new-showtime")
def add_showtime_to_schedule(showtime: dict[str, Any]):
    viewerdb.add_to_schedule(showtime)
    return {}

@app.post("/schedule/remove-showtime")
def remove_showtime_from_schedule(showtime: dict[str, Any]):
    viewerdb.remove_from_schedule(showtime)
    return {}

@app.get("/update-showtimes")
def scan():
    try:
        print(f"Update starting at {datetime.now(timezone.utc)} UTC")

        for theater in theaters.THEATER_NAMES:
            tz = theaters.timezone(theater)
            today = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
            date_range = (today, today + timedelta(weeks=4))

            print(f"Updating the showtimes for {theater} between {date_range[0].isoformat()} and {date_range[1].isoformat()}...")
            showtimes = collect_schedule(theater, None, date_range, Filter.empty(), True)
            if showtimes:
                stored_showtimes = retrieverdb.store_showtimes(theater, showtimes)
                db_showtime_updates(theater, date_range, stored_showtimes)

        return {"success": True}
    except Exception as exc:
        send_error_email(exc)

@app.get("/send-deletion-report")
def scan_deletions():
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)

    send_deletion_report(yesterday)
