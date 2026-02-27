from datetime import date, datetime
from xml.etree import ElementTree

import requests
from bs4 import BeautifulSoup

from retriever.schedule import DaySchedule

THEATER_NAME = "Somerville Theater"
SHOWTIMES_URL = "https://www.somervilletheatre.com/wp-admin/admin-ajax.php?action=tapos_feed"
SHOWTIMES_HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36'}


def _retrieve_page():
    response_text = requests.get(SHOWTIMES_URL, headers=SHOWTIMES_HEADERS).text
    return response_text if "?xml" in response_text.strip().splitlines()[0] else None

def _child(root, name, *, parse_none=True):
    tag = root.find(name)
    return tag.text if tag is not None and (not parse_none or tag != "None") else None

def _load_schedules(schedule_xml, tzname):
    films = {}
    film_section = schedule_xml.find("Films")
    for film in film_section.findall("Film"):
        film_info = {
            "title": _child(film, "FilmTitle", parse_none=False),
            "runtime": _child(film, "RunningTime"),
            "is_reperatory": _child(film, "Genre").lower() == "reperatory"
        }
        films[_child(film, "Code")] = film_info

    schedules = {}
    performance_section = schedule_xml.find("Performances")
    for perf in performance_section.findall("Performance"):
        showdate = date.fromisoformat(_child(perf, "PerformDate"))

        schedule = schedules[showdate] = schedules.get(showdate, DaySchedule(THEATER_NAME, showdate))

        id_ = _child(perf, "Code")
        film_code = _child(perf, "FilmCode")
        movie_info = films.get(film_code)
        if not movie_info:
            raise ValueError("Found performance that referenced non-existing film code: {file_code}.")

        name = movie_info["title"]
        movie = next((m for m in schedule.movies if m.name == name), None)
        if not movie:
            runtime = movie_info["runtime"]
            movie = schedule.add_raw_movie(name, runtime)

        start_dt = datetime.strptime(_child(perf, "StartTime"), "%H:%M:%S")
        fmt = _child(perf, "PerfFlags") or "Standard"
        screen = _child(perf, "ScreenCode")

        programs = {"Reperatory"} if movie_info["is_reperatory"] else set()
        perf_cat = _child(perf, "PerfCat")
        if perf_cat and perf_cat != "Standard":
            programs.add(perf_cat)

        movie.add_raw_showing(id_, start_dt, showdate, tzname, fmt, screen, programs=programs)

    return sorted(schedules.values(), key=lambda s: s.day)

def load_schedules_by_day(theater_info, date_range, quiet=False):
    schedules_by_day = []
    showtimes_xml_text = _retrieve_page()
    if not showtimes_xml_text:
        return []

    try:
        showtimes_xml = ElementTree.fromstring(showtimes_xml_text)
    except ElementTree.ParseError as exc:
        exc.msg += f"\nRAW TEXT: {showtimes_xml_text}"
        raise exc

    schedules_by_day = _load_schedules(showtimes_xml, theater_info["tzname"])
    return [s for s in schedules_by_day if date_range[0] <= s.day <= date_range[1]]
