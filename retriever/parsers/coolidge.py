import re
from calendar import day_abbr
from datetime import date, datetime, timedelta

import requests
from bs4 import BeautifulSoup

from retriever.schedule import DaySchedule, FullSchedule, Showing


THEATER_NAME = "Coolidge Corner"
COOLIDGE_URL = "https://coolidge.org/"
SIGNATURE_PROGRAMS_URL = COOLIDGE_URL
SHOWTIMES_URL_FMT = f"{COOLIDGE_URL}showtimes?date={{date}}"
OPEN_CAPTIONS_URL = f"{COOLIDGE_URL}films-events/open-captions"

projection_specifics_cache = {}

WEEKDAY_REGEX = "Mon|Tue|Wed|Thu|Fri|Sat|Sun"
RANGE_RE = re.compile(f"(?:\d\d?(?::\d\d)?(?:am|pm)).*?(?:{WEEKDAY_REGEX})(?:-(?:{WEEKDAY_REGEX}))?")
TIME_RE = re.compile("(?:\d\d?(?::\d\d)?(?:am|pm))")
DAYS_RE = re.compile(f"({WEEKDAY_REGEX})")


def _retrieve_page(url):
    return BeautifulSoup(requests.get(url).text, 'html.parser')

def _retrieve_movie_detail_page(movie_detail_path):
    return _retrieve_page(f"{COOLIDGE_URL}{movie_detail_path}")

def _retrieve_showtimes_page(showdate):
    return _retrieve_page(SHOWTIMES_URL_FMT.format(date=showdate))

def _retrieve_open_captions_page():
    return _retrieve_page(OPEN_CAPTIONS_URL)

def _retrieve_signature_programs_page():
    return _retrieve_page(SIGNATURE_PROGRAMS_URL)

def _dict_find_by_value(adict, target_value):
    for key, value in adict.items():
        if value == target_value:
            return key
    return None

def _apply_projection_specifics(name, raw_showtime, day, attributes):
    showtime = Showing._parse_showtime(raw_showtime, THEATER_NAME)
    projection_specifics = projection_specifics_cache.get(name)
    if projection_specifics:
        fmt = projection_specifics["format"]
        dates_to_times = projection_specifics["showtimes"]
        if dates_to_times is None:
            attributes.append(fmt + "*")
        elif showtime in dates_to_times.get(day, []):
            attributes.append(fmt)
        else:
            attributes.append("Standard")

# The movies projected on film are often digital for some showtimes. This is only noted on the
# movie's details page, in a text block. So we attempt to parse it and tag only the noted showtimes.
# In the case of failure, all showtime formats will be the format plus an asterisk (e.g. 35mm*).
#
# Example text (from The Drama, April 3 through April 9): "Screening in 35mm in Moviehouse 2 (MH2)
# at 4:30pm, 7pm, & 9:30pm Fri-Sun and 4:30pm, 7:15pm, & 9:30pm on Thurs. Screening digitally in
# all other houses and on Mon-Wed."
def _load_projection_specifics(movie_detail_path, fmt):
    try:
        detail_page = _retrieve_movie_detail_page(movie_detail_path)
        notes_block = detail_page.find(class_="cite").get_text(strip=True)
        line = notes_block.split(".", 1)[0]

        date_to_weekday = {}
        for date_el in detail_page.find_all(class_="datepicker__date"):
            showdate = datetime.strptime(date_el.get_text(strip=True), "%m/%d").date().replace(year=date.today().year)
            weekday_abbr = day_abbr[showdate.weekday()]
            date_to_weekday[showdate] = weekday_abbr

        day_to_times = {}
        for range_match in RANGE_RE.findall(line):
            time_matches = TIME_RE.findall(range_match)
            day_matches = DAYS_RE.findall(range_match)
            end_date = _dict_find_by_value(date_to_weekday, day_matches[-1])
            if not end_date:
                # The showtimes in question have passed.
                continue

            day = end_date
            while True:
                day_to_times[day] = [Showing._parse_showtime(time_str, THEATER_NAME) for time_str in time_matches]
                if day.weekday() == list(day_abbr).index(day_matches[0]):
                    break

                day -= timedelta(days=1)
    except Exception as exc:
        day_to_times = None

    return {
        "format": fmt,
        "showtimes": day_to_times
    }

def _update_projection_specifics_cache(attributes, movie_info, name):
    if "35mm" in attributes:
        attributes.remove("35mm")
        if name not in projection_specifics_cache:
            movie_detail_path = movie_info.find(class_="film-card__link")["href"]
            projection_specifics_cache[name] = _load_projection_specifics(movie_detail_path, "35mm")

# Makes Coolidge's tagging work better for me by recatagorizing some.
def _program_adjustments(attributes, programs):
    def _move(src, dest, srcval, destval=None):
        if srcval in src:
            src.remove(srcval)
            dest.append(destval or srcval)

    _move(programs, attributes, "Cinema in 70mm", "70mm")
    _move(attributes, programs, "Digital Restoration")
    _move(attributes, programs, "Spotlight on Women")
    _move(attributes, programs, "Speaker")
    _move(attributes, programs, "Special Screenings")
    _move(attributes, programs, "New Release")

    if "Digital Restoration" in programs and "New Release" in programs:
        programs.remove("New Release")

    if not attributes:
        attributes.append("Standard")


def _load_schedule(page, day, open_captions_dict, signature_programs_dict):
    schedule = DaySchedule(day)
    for movie_info in page.find_all(class_="film-card"):
        details = movie_info.find(class_="film-card__detail")
        name = details.find(class_="film-card__link").get_text(strip=True)
        runtime_el = details.find(class_="film-card__runtime")
        raw_runtime_str = runtime_el.get_text(strip=True) if runtime_el else "0mins"
        runtime_str = ' '.join([s.strip() for s in raw_runtime_str.splitlines()])

        movie = schedule.add_raw_movie(name, runtime_str)

        attrib_chip_parent = movie_info.find(class_="view-film-event-type-link")
        base_attributes = [a.get_text(strip=True) for a in attrib_chip_parent.find_all(class_="film-program__title")] if attrib_chip_parent else []

        programs = []
        program_chip_parent = movie_info.find(class_="view-program-taxonomy-link")
        if program_chip_parent:
            for anchor in program_chip_parent.find_all(class_="film-program__link"):
                path = anchor.get("href", "").replace("/programs", "")
                chip_label = anchor.find(class_="film-program__title").get_text(strip=True)
                programs.append(signature_programs_dict.get(path, chip_label))

        _program_adjustments(base_attributes, programs)
        _update_projection_specifics_cache(base_attributes, movie_info, name)

        for showtime_el in movie_info.find_all(class_="showtime-ticket__time"):
            raw_showtime = showtime_el.get_text(strip=True)
            attributes = base_attributes + (["Open Caption"] if raw_showtime in open_captions_dict.get(name, {}).get(day, []) else [])

            _apply_projection_specifics(name, raw_showtime, day, attributes)

            movie.add_raw_showings(attributes, [raw_showtime], day, "Coolidge Corner", programs)

    return schedule

def _showtimes_text_iter(date_range):
    current_date, end_date = date_range
    
    while current_date <= end_date:
        page = _retrieve_showtimes_page(current_date)
        yield (page, current_date)
        current_date += timedelta(days=1)

def _load_signature_programs():
    page = _retrieve_signature_programs_page()
    signature_programs_menu_el = page.find(lambda el: "menu-item" in el.get("class", []) and el.find(string="Signature Programs"))
    return {item.a["href"].replace("/programs", ""): item.get_text(strip=True) for item in signature_programs_menu_el.find_all(class_="menu-item")}

def _load_open_captions_showtimes():
    open_captions = {}
    page = _retrieve_open_captions_page()
    for movie_info in page.find_all(class_="showtimes"):
        name = movie_info.find(class_="film-card__title").get_text(strip=True)
        open_captions[name] = {}

        for day_showtimes in movie_info.find_all(class_="film-showtime-list"):
            day_str = day_showtimes.find(class_="datepicker__date").get_text(strip=True)  # e.g. 3/26
            day = date(date.today().year, *[int(s) for s in day_str.split("/")])
            if day < date.today():
                day = day.replace(year=day.year + 1)
            
            open_captions[name][day] = [el.get_text(strip=True) for el in day_showtimes.find_all(class_="showtime-ticket__time")]
    return open_captions


def load_schedules_by_day(theater, date_range, quiet=False):
    schedules_by_day = []
    if not quiet:
        print(".", end="", flush=True)

    signature_programs_dict = _load_signature_programs()
    open_captions_dict = _load_open_captions_showtimes()
    for showtimes_html, day in _showtimes_text_iter(date_range):
        schedules_by_day.append(_load_schedule(showtimes_html, day, open_captions_dict, signature_programs_dict))

        if not quiet:
            print(".", end="", flush=True)

    return schedules_by_day
