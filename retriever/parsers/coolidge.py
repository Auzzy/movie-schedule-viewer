from datetime import date, timedelta

import requests
from bs4 import BeautifulSoup

from retriever.schedule import DaySchedule, FullSchedule


SIGNATURE_PROGRAMS_URL = "https://coolidge.org/"
SHOWTIMES_URL_FMT = "https://coolidge.org/showtimes?date={date}"
OPEN_CAPTIONS_URL = "https://coolidge.org/films-events/open-captions"

def _retrieve_page(url):
    return BeautifulSoup(requests.get(url).text, 'html.parser')

def _retrieve_showtimes_page(showdate):
    return _retrieve_page(SHOWTIMES_URL_FMT.format(date=showdate))

def _retrieve_open_captions_page():
    return _retrieve_page(OPEN_CAPTIONS_URL)

def _retrieve_signature_programs_page():
    return _retrieve_page(SIGNATURE_PROGRAMS_URL)

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

        program_chip_parent = movie_info.find(class_="view-program-taxonomy-link")
        program_paths = [a["href"].replace("/programs", "") for a in program_chip_parent.find_all(class_="film-program__link")] if program_chip_parent else []
        programs = [signature_programs_dict[path] for path in program_paths]

        _program_adjustments(base_attributes, programs)

        for showtime_el in movie_info.find_all(class_="showtime-ticket__time"):
            raw_showtime = showtime_el.get_text(strip=True)
            attributes = base_attributes + (["Open Caption"] if raw_showtime in open_captions_dict.get(name, {}).get(day, []) else [])

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
