import calendar
import itertools
import json
import requests
from datetime import date, timedelta

from retriever.schedule import DaySchedule
from retriever.theaters import THEATERS


def _load_schedule(showtimes_json, theater):
    day = date.fromisoformat(showtimes_json["viewModel"]["date"])
    schedule = DaySchedule(day)
    for movie_info in showtimes_json["viewModel"]["movies"]:
        if " " in movie_info["title"]:
            name, year_str = movie_info["title"].rsplit(maxsplit=1)
            if year_str[0] != "(" or year_str[-1] != ")" or not all(c.isdigit() for c in year_str[1:-1]):
                name += f" {year_str}"
        else:
            name = movie_info["title"]

        runtime = movie_info["runtime"]

        movie = schedule.add_raw_movie(name, runtime)

        # showtimes_sections = itertools.chain([(fmt["format"], ag) for fmt in movie_info["variants"] for ag in fmt["amenityGroups"]])
        showtimes_sections = itertools.chain([(fmt["filmFormatHeader"], ag) for fmt in movie_info["variants"] for ag in fmt["amenityGroups"]])
        for heading, showtimes_listing in showtimes_sections:
            raw_amenities = showtimes_listing.get("amenities", [])
            attributes = [heading]
            if raw_amenities:
                attributes += [attr["name"] for attr in showtimes_listing["amenities"]]
            elif showtimes_listing.get("isDolby", False):
                attributes += ["dolby cinema @ amc"]

            raw_showtimes = [showtime["date"] for showtime in showtimes_listing["showtimes"]]
            movie.add_raw_showings(attributes, raw_showtimes, day, theater)

    return schedule


def _retrieve_json(theater, showdate):
    url = f"https://www.fandango.com/napi/theaterMovieShowtimes/{THEATERS[theater]['code']}?startDate={showdate.date().isoformat()}"
    headers = {"referer": f"https://www.fandango.com/{THEATERS[theater]['slug']}/theater-page?format=all&date={showdate.date().isoformat()}"}
    return requests.get(url, headers=headers).json()


def _showtimes_iter(theater, filepath, date_range):
    if filepath:
        with open(filepath) as showtimes_file:
            yield json.read(showtimes_file)
    elif date_range:
        current_date, end_date = date_range
        while current_date <= end_date:
            yield _retrieve_json(theater, current_date)
            current_date += timedelta(days=1)


def load_schedules_by_day(theater, filepath, date_range, filter_params, quiet=False):
    schedules_by_day = []
    if not quiet:
        print(".", end="", flush=True)
    for showtimes_json in _showtimes_iter(theater, filepath, date_range):
        if "viewModel" in showtimes_json:
            schedule = _load_schedule(showtimes_json, theater)
            filtered_schedule = schedule.filter(filter_params)
            schedules_by_day.append(filtered_schedule)

        if not quiet:
            print(".", end="", flush=True)

    return schedules_by_day
