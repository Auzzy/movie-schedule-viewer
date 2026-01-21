import itertools
import sys
from datetime import date, timedelta

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

sys.path.append("scheduleretriever")
from retriever import db as retrieverdb
from retriever.fandango_json import load_schedules_by_day
from retriever.schedule import THEATER_SLUG_DICT, Filter, FullSchedule
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
    filtered_showtimes = [s for s in showtimes if display_info[s["title"]]["visible"]]
    return display_info, filtered_showtimes


def _load_showtimes(theater, first_date, last_date, title=None):
    last_date = last_date or first_date
    showtimes = retrieverdb.load_showtimes(theater, first_date, last_date)
    return [s for s in showtimes if s["title"] == title] if title else showtimes

@app.get("/", response_class=HTMLResponse)
def read_root():
    with open("index.html") as homepage_html:
        return homepage_html.read()

@app.get("/showtimes/{theater}/{first_date}/{last_date}")
def request_schedule_date_range(theater: str, first_date: date, last_date: date):
    showtimes = _load_showtimes(theater, first_date, last_date)
    display_info, filtered_showtimes = _load_movie_display_info(showtimes)
    return {"showtimes": filtered_showtimes, "display": display_info}

@app.get("/showtimes/{theater}/{title}/{first_date}/{last_date}")
def request_movie_schedule_date_range(theater: str, title: str, first_date: date, last_date: date):
    return {"showtimes": _load_showtimes(theater, first_date, last_date, title)}

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
    # Should these be configurable via env vars?
    theater = "AMC Methuen"  # Will likely expand to THEATER_SLUG_DICT.keys() once I implement theater switching into the app.
    date_range = (date.today(), date.today() + timedelta(weeks=4))
    
    print(f"Updating the schedule for {theater} between {date_range[0].isoformat()} and {date_range[1].isoformat()}...")

    schedules_by_day = load_schedules_by_day(theater, None, date_range, Filter.empty(), quiet=True)
    if not schedules_by_day:
        print("Could not find any data for the requested date(s).")
        return

    schedule_range = FullSchedule.create(schedules_by_day)
    retrieverdb.store_showtimes(theater, schedule_range)
    
    return {"success": True}
