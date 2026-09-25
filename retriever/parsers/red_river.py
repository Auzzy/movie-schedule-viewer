import requests
from datetime import datetime

from retriever.schedule import DaySchedule
from retriever.utils import offset_timezone


THEATER_NAME = "Red River"
SHOWTIMES_URL = "https://redrivertheatres.org/graphql"
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "client-type": "consumer",
    "content-type": "application/json",
    "site-id": "361"
}
GRAPHQL_QUERY = {
    "variables": {},
    "extensions": {"clientLibrary": {"name": "@apollo/client", "version": "4.0.9"}},
    "query": """query ($movieId: ID) {
  showingsForDate(movieId: $movieId) {
    data {
      id
      time
      screenId
      movie {
        id
        name
        showingStatus
        duration
        originalLanguage
      }
      showingBadgeIds
    }
    count
    resultVersion
  }
}"""
}


def _get_screen(showing_json):
    match showing_json["screenId"]:
        case "2220": return "Lincoln"
        case "2221": return "Stonyfield"
        case "2626": return "Simchik"
        case other: return other

def _get_language(showing_json):
    match showing_json["movie"].get("originalLanguage"):
        case "en": return "English"
        case other: return other

def _get_programs(showing_json):
    programs = set()
    for badge_id in showing_json["showingBadgeIds"]:
        match badge_id:
            case "3330": programs.add("Open Caption")
            case "3333": programs.add("Popcorn & Pacifiers")
            case "3351": programs.add("Special Event")
            case "2679" | "3332" | "3353":
                # accessible, advanced screening (i.e. Thursday), and limited engagement, respectively. None of which are useful.
                pass
            case other: programs.add(other)

    return programs

def _load_schedules(schedule_json, tzname):
    tz = offset_timezone(tzname)

    films = {}
    schedules = {}
    for showing_json in schedule_json["data"]["showingsForDate"]["data"]:
        start_dt = datetime.fromisoformat(showing_json["time"]).astimezone(tz)

        showdate = start_dt.date()
        schedule = schedules[showdate] = schedules.get(showdate, DaySchedule(THEATER_NAME, showdate))

        id_ = showing_json["id"]
        movie_info = showing_json["movie"]
        if not movie_info:
            raise ValueError("Found performance that was missing movie info: {id_}.")

        name = movie_info["name"]
        movie = next((m for m in schedule.movies if m.name == name), None)
        if not movie:
            runtime = movie_info["duration"]
            movie = schedule.add_raw_movie(name, runtime)

        fmt = "Standard"
        language = _get_language(showing_json)
        screen = _get_screen(showing_json)
        programs = _get_programs(showing_json)

        movie.add_raw_showing(id_, start_dt, showdate, tzname, fmt, screen, language, programs=programs)

    return sorted(schedules.values(), key=lambda s: s.day)


def _retrieve_page():
    return requests.post(SHOWTIMES_URL, json=GRAPHQL_QUERY, headers=REQUEST_HEADERS).json()

def load_schedules_by_day(theater_info, date_range, quiet=False):
    schedules_by_day = []
    showtimes_json = _retrieve_page()
    schedules_by_day = _load_schedules(showtimes_json, theater_info["tzname"])
    return [s for s in schedules_by_day if date_range[0] <= s.day <= date_range[1]]
