from datetime import date, datetime, timedelta

import requests
from bs4 import BeautifulSoup

from retriever.schedule import DaySchedule, FullSchedule

THEATER_NAME = "Brattle Theater"
SHOWTIMES_URL = "https://brattlefilm.org/coming-soon/"
SHOWTIMES_HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36'}

def _retrieve_page():
    return requests.get(SHOWTIMES_URL, headers=SHOWTIMES_HEADERS).text

def _parse_language(movie_info):
    language_el = movie_info.find(class_="show-spec-label", string="Language:")
    return language_el.next_sibling.strip() if language_el else None

def _parse_format(movie_info):
    format_el = movie_info.find(class_="show-spec-label", string="Format:")
    if format_el:
        fmt = format_el.next_sibling.strip()
        if fmt.lower() in ("dcp", "4k dcp"):
            return "Standard"
        elif fmt.lower() in ("35mm film", ):
            return "35mm"
        else:
            return fmt
    else:
        return "Standard"

def _load_schedules(page):
    schedules = {}
    for movie_info in page.find_all(class_="show-details"):
        name = movie_info.find(class_="show-title").get_text(strip=True)
        if name.lower() == "closed for private event":
            continue

        showtimes_section = movie_info.find(class_="showtimes")
        if not showtimes_section:
            continue

        raw_programs = {el.get_text(strip=True) for el in movie_info.find(class_="pill-container").find_all(class_="pill")}
        programs = [prog for prog in raw_programs if prog not in ("35mm Screenings", "Closed Captions")]
        
        for screening_info in showtimes_section.find_all(lambda tag: tag.has_attr("data-date")):
            start_time_el = screening_info.find(class_="showtime")
            for child in start_time_el.children:
                if not isinstance(child, str):
                    child.clear()
            raw_start_time = start_time_el.get_text(strip=True)

            showdate = datetime.fromtimestamp(int(screening_info["data-date"])).date()
            schedule = schedules[showdate] = schedules.get(showdate, DaySchedule(showdate))
            
            movie = next((m for m in schedule.movies if m.name == name), None)
            if not movie:
                runtime_str = movie_info.find(class_="show-spec-label", string="Run Time:").next_sibling.strip()
                movie = schedule.add_raw_movie(name, runtime_str)
            
            fmt = _parse_format(movie_info)
            language = _parse_language(movie_info)
            # Brattle almost certainly offers open captions, but none right now, so I can't ensure I capture it correctly.
            is_open_caption = False

            movie.add_raw_showings([raw_start_time], showdate, THEATER_NAME, fmt, is_open_caption, language=language, programs=programs)

    return sorted(schedules.values(), key=lambda s: s.day)

def load_schedules_by_day(theater, date_range, quiet=False):
    schedules_by_day = []
    showtimes_html = BeautifulSoup(_retrieve_page(), 'html.parser')
    schedules_by_day = _load_schedules(showtimes_html)
    return [s for s in schedules_by_day if date_range[0] <= s.day <= date_range[1]]
