from datetime import date, datetime, timedelta

import requests

from retriever.schedule import DaySchedule, FullSchedule

THEATER_NAME = "Somerville Theater"
SHOWTIMES_URL = "https://api.us.veezi.com/v1/websession"
SHOWTIMES_HEADERS = {"veeziaccesstoken": "qvn2v2bvnf11k126ehz14zcxwr"}

def _retrieve_page():
    return requests.get(SHOWTIMES_URL, headers=SHOWTIMES_HEADERS).json()

def _load_schedules(schedule_json):
    schedules = {}
    for showing in schedule_json:
        name = showing["Title"]
        start_dt = datetime.fromisoformat(showing["FeatureStartTime"])

        showdate = start_dt.date()
        schedule = schedules[showdate] = schedules.get(showdate, DaySchedule(showdate))

        movie = next((m for m in schedule.movies if m.name == name), None)
        if not movie:
            runtime = int((datetime.fromisoformat(showing["FeatureEndTime"]) - start_dt).seconds / 60)
            movie = schedule.add_raw_movie(name, runtime)

        raw_formats = showing.get("Attributes")
        if raw_formats:
            match raw_formats:
                case ["0000000013"]: formats = ["35mm"]
                case ["0000000015"]: formats = ["4k"]
                case _: formats = raw_formats
        else:
            formats = ["Standard"]

        programs = [showing["PriceCardName"]]
        if programs[0] in ("Repertory Evening", "Matinee-SMV", "Evening-SMV", "$7 Tuesdays"):
            # You'd think "Reperatory Evening" would mean just that. But it's sometimes applied to new releases.
            programs = []

        movie.add_raw_showings(formats, [start_dt], showdate, THEATER_NAME, programs)

    return sorted(schedules.values(), key=lambda s: s.day)

def load_schedules_by_day(theater, date_range, quiet=False):
    schedules_by_day = []
    showtimes_json = _retrieve_page()
    schedules_by_day = _load_schedules(showtimes_json)
    return [s for s in schedules_by_day if date_range[0] <= s.day <= date_range[1]]
