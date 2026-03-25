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

        fmt = showing["FilmFormat"]
        if fmt == "2D Film":
            attrs = showing["Attributes"] or ["Film"]
            attributes = ["35mm"] if attrs == ["0000000013"] else attrs
        else:
            attributes = [fmt]

        movie.add_raw_showings(attributes, [start_dt], showdate, THEATER_NAME)

    return sorted(schedules.values(), key=lambda s: s.day)

def load_schedules_by_day(date_range, quiet=False):
    schedules_by_day = []
    showtimes_json = _retrieve_page()
    schedules_by_day = _load_schedules(showtimes_json)
    return [s for s in schedules_by_day if date_range[0] <= s.day <= date_range[1]]

if __name__ == "__main__":
    schedules_by_day = load_schedules_by_day((date(2026, 3, 22), date(2026, 3, 27)))
    print(FullSchedule.create(schedules_by_day).output(False, False))
