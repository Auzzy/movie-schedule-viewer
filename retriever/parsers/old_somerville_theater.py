from datetime import date, datetime, timedelta

import requests

from retriever.schedule import DaySchedule, FullSchedule

THEATER_NAME = "Somerville Theater"
SHOWTIMES_URL = "https://api.us.veezi.com/v1/websession"
SHOWTIMES_HEADERS = {"veeziaccesstoken": "qvn2v2bvnf11k126ehz14zcxwr"}

def _retrieve_page():
    return requests.get(SHOWTIMES_URL, headers=SHOWTIMES_HEADERS).json()

def _load_schedules(schedule_json, tzname):
    schedules = {}
    for showing in schedule_json:
        name = showing["Title"]
        start_dt = datetime.fromisoformat(showing["FeatureStartTime"])

        showdate = start_dt.date()
        schedule = schedules[showdate] = schedules.get(showdate, DaySchedule(THEATER_NAME, showdate))

        movie = next((m for m in schedule.movies if m.name == name), None)
        if not movie:
            runtime = int((datetime.fromisoformat(showing["FeatureEndTime"]) - start_dt).seconds / 60)
            movie = schedule.add_raw_movie(name, runtime)

        raw_formats = showing.get("Attributes")
        if raw_formats:
            match raw_formats:
                case ["0000000013"]: fmt = "35mm"
                case ["0000000015"]: fmt = "4k"
                case _: fmt = raw_formats[0]
        else:
            fmt = "Standard"

        programs = [showing["PriceCardName"]]
        if programs[0] in ("Repertory Evening", "Matinee-SMV", "Evening-SMV", "$7 Tuesdays"):
            # You'd think "Reperatory Evening" would mean just that. But it's sometimes applied to new releases.
            programs = []

        is_open_caption = False

        movie.add_raw_showings([start_dt], showdate, tzname, fmt, is_open_caption, programs=programs)

    return sorted(schedules.values(), key=lambda s: s.day)

def load_schedules_by_day(theater_info, date_range, quiet=False):
    schedules_by_day = []
    showtimes_json = _retrieve_page()
    schedules_by_day = _load_schedules(showtimes_json, theater_info["tzname"])
    return [s for s in schedules_by_day if date_range[0] <= s.day <= date_range[1]]
