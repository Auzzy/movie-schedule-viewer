import itertools
import sys
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

sys.path.append("scheduleretriever")
from retriever import db as retrieverdb, theaters
from retriever.fandango_json import load_schedules_by_day
from retriever.movie_times_lib import collect_schedule, db_showtime_updates, send_error_email, send_deletion_report
from retriever.schedule import Filter, FullSchedule

import db as viewerdb


# (text, background)
MOVIE_COLORS = [
    ("white", "blue"),
    ("white", "green"),
    ("black", "lime"),
    ("white", "slateblue"),
    ("white", "slategray"),
    ("white", "maroon"),
    ("black", "skyblue"),
    ("white", "crimson"),
    ("white", "magenta"),
    ("black", "orange"),
    ("black", "gold"),
    ("black", "yellow"),
    ("black", "pink"),
    ("black", "coral"),
    ("black", "violet"),
    ("black", "palegreen"),
    ("white", "saddlebrown"),
    ("black", "aquamarine"),
    ("black", "burlywood"),
    ("white", "olive"),
    ("black", "greenyellow"),
    ("black", "rosybrown"),
    ("black", "thistle"),
    ("black", "sandybrown"),
    ("black", "salmon"),
    ("black", "lightgray"),
    ("white", "brown"),
    ("white", "purple")
]


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
)


def _load_movie_display_info(showtimes):
    visibility = viewerdb.load_visibility()

    titles = {s["title"] for s in showtimes}
    color_iter = itertools.cycle(iter(MOVIE_COLORS))
    display_info = {}
    for title in titles:
        text_color, bg_color = next(color_iter)
        display_info[title] = {
            "visible": visibility.get(title, True),
            "background_color": bg_color,
            "text_color": text_color
        }
    return display_info


def _load_showtimes(theater, first_time, last_time, title=None):
    last_time = last_time or first_time
    return retrieverdb.load_showtimes(theater, first_time, last_time, title)


@app.get("/", response_class=HTMLResponse)
def read_root():
    with open("index.html") as homepage_html:
        return homepage_html.read()

@app.get("/showtimes/{theater}/{first_time}/{last_time}")
def request_schedule_date_range(theater: str, first_time: datetime, last_time: datetime):
    showtimes = _load_showtimes(theater, first_time, last_time)
    display_info = _load_movie_display_info(showtimes)
    return {"showtimes": showtimes, "display": display_info}

@app.put("/movies/{title}/hide")
def request_hide_movie(title):
    viewerdb.hide_movie(title)
    return {}

@app.put("/movies/{title}/show")
def request_show_movie(title):
    viewerdb.show_movie(title)
    return {}


@app.get("/update-schedule")
def scan():
    try:
        print(f"Update starting at {datetime.now(timezone.utc)} UTC")

        for theater in theaters.THEATER_NAMES:
            tz = theaters.timezone(theater)
            today = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
            date_range = (today, today + timedelta(weeks=4))

            print(f"Updating the schedule for {theater} between {date_range[0].isoformat()} and {date_range[1].isoformat()}...")
            schedule = collect_schedule(theater, None, date_range, Filter.empty(), True)
            if schedule:
                showtimes = retrieverdb.store_showtimes(theater, schedule)
                db_showtime_updates(theater, date_range, showtimes)

        return {"success": True}
    except Exception as exc:
        send_error_email(exc)


@app.get("/send-deletion-report")
def scan_deletions():
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    deleted_showtimes = retrieverdb.load_deleted_showtimes(yesterday, today)

    send_deletion_report(deleted_showtimes)
