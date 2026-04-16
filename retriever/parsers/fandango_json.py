import calendar
import itertools
import json
import requests
from datetime import date, timedelta

from retriever.schedule import DaySchedule
from retriever.theaters import THEATERS


def _parse_language(attributes, theater):
    for attr in attributes:
        if "dubbed" in attr or "subtitles" in attr or "language" in attr:
            return " ".join(s if s in ("with", "dubbed", "subtitles", "language") else s.title() for s in attr.split())

    # AMC and Apple are pretty good about labeling their films' language. At
    # least when it comes to Japanese, Spanish, or an Indian language.
    return "English" if "AMC" in theater or "Apple" in theater else None

def _parse_format(attributes):
    if "dolby cinema @ amc" in attributes:
        return "Dolby"
    elif "imax" in attributes:
        return "IMAX"
    elif "reald 3d" in attributes:
        return "3D"
    elif "xl at amc" in attributes:
        return "XL at AMC"
    elif "d-box" in attributes:
        return "D-Box"
    elif "screenx" in attributes:
        return "ScreenX"
    elif "laser at amc" in attributes or "standard format" in attributes:
        return "Standard"
    else:
        return None

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
            if raw_amenities:
                attributes = [attr["name"].lower() for attr in showtimes_listing["amenities"]]

                fmt = _parse_format([heading] + attributes) or heading
                language = _parse_language(attributes, theater)
                is_open_caption = "open caption" in attributes
                no_alist = "alternative content" in attributes or "no passes" in attributes
            elif showtimes_listing.get("isDolby", False):
                fmt = "Dolby"

            raw_showtimes = [showtime["date"] for showtime in showtimes_listing["showtimes"]]
            movie.add_raw_showings(raw_showtimes, day, theater, fmt, is_open_caption, no_alist, language)

    return schedule


def _retrieve_json(theater, showdate):
    url = f"https://www.fandango.com/napi/theaterMovieShowtimes/{THEATERS[theater]['code']}?startDate={showdate.isoformat()}"
    headers = {"referer": f"https://www.fandango.com/{THEATERS[theater]['slug']}/theater-page?format=all&date={showdate.isoformat()}"}
    return requests.get(url, headers=headers).json()


def _showtimes_iter(theater, date_range):
    current_date, end_date = date_range
    while current_date <= end_date:
        yield _retrieve_json(theater, current_date)
        current_date += timedelta(days=1)


def load_schedules_by_day(theater, date_range, quiet=False):
    schedules_by_day = []
    if not quiet:
        print(".", end="", flush=True)
    for showtimes_json in _showtimes_iter(theater, date_range):
        if "viewModel" in showtimes_json:
            schedules_by_day.append(_load_schedule(showtimes_json, theater))

        if not quiet:
            print(".", end="", flush=True)

    return schedules_by_day
