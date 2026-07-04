import json
import os.path
import re
from datetime import date, datetime, timedelta
from urllib.parse import urlsplit

import requests
from bs4 import BeautifulSoup, Tag

from retriever.schedule import DaySchedule, FullSchedule

THEATER_NAME = "Red River"
MAIN_URL = "https://redrivertheatres.org/"
SHOWTIMES_URL = "https://ticketing.useast.veezi.com/sessions/?siteToken=rh66des21wzqpsqgg0jkjqcr88"
REQUEST_HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36'}
RUNTIME_RE = re.compile(r"\((?P<runtime>\d{1,3}) min.*\) \d{4}")
SHOWTIME_INFO_RE = re.compile(r"(?P<showtime>\d\d?:\d\d (?:am|pm)) Screen (?P<screen>\d)")

def _retrieve_page(url):
    return requests.get(url, headers=REQUEST_HEADERS).text

def _get_programs(movie_info):
    programs = set()
    for attr_el in movie_info.find_all(class_="screen-attribute"):
        match attr_el.get_text(strip=True):
            case "CP": programs.add("Community Program")
            case "REP": programs.add("Repertory Screening")
            case "MM": programs.add("Member Monday")
            case "SE": programs.add("Special Event")
            case "OCAP": programs.add("Open Caption")
            case "P&P": programs.add("Popcorn & Pacifiers")
            case "ADV": pass
            case other: programs.add(other)
    
    return programs

def _clean_name(name):
    parts = name.rsplit("-", 1)
    if len(parts) ==2 and parts[1].lower().strip() in ("open captions", "popcorn & pacifiers", "popcorn and pacifiers"):
        name = parts[0].strip()
    return name

# When a showtime sells out, the link to purchase tickets is removed, so its ID
# becomes unavailable. However, there's a JSON blob embedded at the bottom of
# the page that still contains it.
def _load_ids(page, extra_info_dict):
    for blob in page.find_all(type="application/ld+json"):
        showtime_json = json.loads(blob.get_text(strip=True))
        if isinstance(showtime_json, list):
            for showtime_entry in showtime_json:
                if showtime_entry["@type"] != "VisualArtsEvent":
                    continue

                id_ = os.path.split(urlsplit(showtime_entry["url"]).path.strip("/"))[-1]
                name = showtime_entry["name"]
                showtime = datetime.fromisoformat(showtime_entry["startDate"])
                key = (showtime.date(), showtime.time())
                if name in extra_info_dict:
                    extra_info_dict[name]["showtimes"].setdefault(key, {})["id"] = id_
                else:
                    extra_info_dict[name] = {"showtimes": {key: {"id": id_}}}

def _load_schedules(page, extra_info_dict, tzname):
    _load_ids(page, extra_info_dict)

    schedules = {}
    for movie_info in page.find(id="sessionsByDateConent").find_all(class_="film"):
        name = _clean_name(movie_info.find(class_="title").get_text(strip=True))
        extra_info = extra_info_dict.get(name, {})

        showdate_str = movie_info.find(class_="date").get_text(strip=True)
        showdate = datetime.strptime(showdate_str, "%A %d, %B").replace(year=date.today().year).date()
        if date.today() > showdate:
            showdate = showdate.replace(year=showdate.year + 1)

        for screening_info in movie_info.find(class_="session-times").children:
            if not isinstance(screening_info, Tag):
                continue

            raw_start_time = screening_info.find("time").get_text(strip=True)

            schedule = schedules[showdate] = schedules.get(showdate, DaySchedule(THEATER_NAME, showdate))
            
            movie = next((m for m in schedule.movies if m.name == name), None)
            if not movie:
                runtime_str = extra_info.get("runtime", "0")
                movie = schedule.add_raw_movie(name, runtime_str)

            key = (showdate, datetime.strptime(raw_start_time, "%I:%M %p").time())
            extra_showtime_info = extra_info["showtimes"].get(key, {})
            screen = extra_showtime_info.get("screen") or None
            id_ = extra_showtime_info.get("id") or None

            programs = _get_programs(movie_info)

            movie.add_raw_showing(id_, raw_start_time, showdate, tzname, "Standard", screen, programs=programs)

    return sorted(schedules.values(), key=lambda s: s.day)

# Red River's main page is an unreliable source of showtimes. It only displays
# ten showtimes per entry before requiring the user to click a link for the
# rest, and it's not uncommon for it to omit movies altogether in error.
# However, the main ticketing page doesn't include runtime or screen info.
# Thus, we attempt to load them from the main page, so they can be looked up
# while parsing the ticketing page.
def _load_extra_info_by_movies():
    main_html = BeautifulSoup(_retrieve_page(MAIN_URL), 'html.parser')
    info_dict = {}
    for movie_info in main_html.find_all(class_="podsfilm"):
        name = _clean_name(movie_info.find(class_="podsfilmtitlelink").get_text(strip=True))

        details_str = movie_info.find(class_="showinfodiv").get_text(strip=True)
        runtime_match = RUNTIME_RE.match(details_str)

        showtime_el = movie_info.find(class_="datediv")
        if not showtime_el:
            continue

        curdate = None
        showtime_info = {}
        while showtime_el:
            if "datediv" in showtime_el.get("class"):
                curdate = datetime.strptime(showtime_el.get_text(strip=True), "%A, %b %d").replace(year=date.today().year).date()
            elif "arthousebutton" in showtime_el.get("class"):
                info = SHOWTIME_INFO_RE.match(showtime_el.get_text(strip=True)).groupdict()
                showtime = datetime.strptime(info["showtime"], "%I:%M %p").time()
                showtime_info[(curdate, showtime)] = {
                    "screen": info["screen"]
                }

            showtime_el = showtime_el.next_sibling

        info_dict[name] = {
            "runtime": runtime_match.group("runtime") if runtime_match else 0,
            "showtimes": showtime_info
        }

    return info_dict


def load_schedules_by_day(theater_info, date_range, quiet=False):
    schedules_by_day = []
    showtimes_html = BeautifulSoup(_retrieve_page(SHOWTIMES_URL), 'html.parser')
    extra_info_dict = _load_extra_info_by_movies()
    schedules_by_day = _load_schedules(showtimes_html, extra_info_dict, theater_info["tzname"])
    return [s for s in schedules_by_day if date_range[0] <= s.day <= date_range[1]]
