import calendar
import itertools
import json
import os
import requests
import time
from datetime import date, timedelta
from urllib.parse import urlencode

from retriever.schedule import DaySchedule


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

def _load_schedule(showtimes_json, theater_info):
    tzname = theater_info["tzname"]
    theater_name = theater_info["name"]

    day = date.fromisoformat(showtimes_json["viewModel"]["date"])
    schedule = DaySchedule(theater_name, day)
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
                language = _parse_language(attributes, theater_name)
                is_open_caption = "open caption" in attributes
                no_alist = "alternative content" in attributes or "no passes" in attributes
            elif showtimes_listing.get("isDolby", False):
                fmt = "Dolby"

            raw_showtimes = [showtime["date"] for showtime in showtimes_listing["showtimes"]]
            movie.add_raw_showings(raw_showtimes, day, tzname, fmt, is_open_caption, no_alist, language)

    return schedule


def _request(url, headers=None):
    return requests.get(url, headers=headers)

def _request_fandango(url):
    headers = {"referer": "https://www.fandango.com"}
    return requests.get(url, headers=headers)

def _retrieve_showtimes(theater_code, showdate):
    url = f"https://www.fandango.com/napi/theaterMovieShowtimes/{theater_code}?startDate={showdate.isoformat()}"
    return _request_fandango(url).json()

def _search_theaters(name):
    search_param = urlencode({"search": name})
    search_url = f"https://www.fandango.com/napi/home/autocompleteDesktopSearch?{search_param}"
    search_response = _request_fandango(search_url).json()
    return search_response["resultsByType"]["theaters"]["items"]

def _get_timezone(latitude, longitude):
    tzdb_url = f"http://api.timezonedb.com/v2.1/get-time-zone?key={os.environ['TZDB_KEY']}&format=json&by=position&lat={latitude}&lng={longitude}"
    tzdb_response = _request(tzdb_url)
    if tzdb_response.status_code == 429:
        time.sleep(1)
        return _get_timezone(latitude, longitude)

    return tzdb_response.json()["zoneName"]

def _showtimes_iter(theater_code, date_range):
    current_date, end_date = date_range
    while current_date <= end_date:
        yield _retrieve_showtimes(theater_code, current_date)
        current_date += timedelta(days=1)


def load_schedules_by_day(theater_info, date_range, quiet=False):
    schedules_by_day = []
    if not quiet:
        print(".", end="", flush=True)
    for showtimes_json in _showtimes_iter(theater_info["code"], date_range):
        if "viewModel" in showtimes_json:
            schedules_by_day.append(_load_schedule(showtimes_json, theater_info))

        if not quiet:
            print(".", end="", flush=True)

    return schedules_by_day

def search(query):
    search_results = _search_theaters(query)
    if not search_results:
        return []
        
    results = []
    for result in search_results:
        link = result["link"]
        theater_code = link.strip("/").split("/", 1)[0].rsplit("-", 1)[1]

        results.append({
            "query": query,
            "fullname": result["name"],
            "code": theater_code,
            "is_open": True,
            "parser": "fandango_json"
        })
    return results

def get_tzname(theater_code):
    showtime_response = _retrieve_showtimes(theater_code, date.today())
    theater_info = showtime_response["viewModel"]["theater"]["details"]
    return _get_timezone(**theater_info["geo"])
