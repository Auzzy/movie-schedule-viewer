from datetime import date, timedelta

import requests
from bs4 import BeautifulSoup

from retriever.schedule import DaySchedule, FullSchedule


SHOWTIMES_URL_FMT = "https://coolidge.org/views/ajax?date={date}&view_name=now_playing&view_display_id=page_1&ajax_page_state%5Blibraries%5D=eJxtkFFywyAMRC9kzJEyMqhYtUAMUuL49sWdZGw3_QH0xOyuFICxRGg-vB6jzZhxCCJMMaGHEKRFknJCiRidUVjQqKSj8dZwJinxSeSLOLsALR4osUzATkOjavrJbeOLdpZpdy3woAR2yfMfqxAWSKiuqywHVoQW5o98OstqlPv39winXr_wqG9F1sqw9XA-gmHd19AGfNru5GO7V-DxVQ5JpBvdoABvfWHq_4JBNzXMfgLF4UG4qv89R_iG5wVkiXfGH6_kp3A'"

def _retrieve_page(showdate):
    response = requests.get(SHOWTIMES_URL_FMT.format(date=showdate))
    for section in response.json():
        if section["command"] == "insert" and section["method"] == "replaceWith":
            return section["data"]

def _load_schedule(page, day):
    schedule = DaySchedule(day)
    for movie_info in page.find_all(class_="film-card"):
        details = movie_info.find(class_="film-card__detail")
        name = details.find(class_="film-card__title").get_text(strip=True)
        raw_runtime_str = details.find(class_="film-card__runtime").get_text(strip=True)
        runtime_str = ' '.join([s.strip() for s in raw_runtime_str.splitlines()])

        movie = schedule.add_raw_movie(name, runtime_str)

        attributes = [a.get_text(strip=True) for a in movie_info.find_all(class_="film-program__title")]
        raw_showtimes = [s.get_text(strip=True) for s in movie_info.find_all(class_="showtime-ticket__time")]

        movie.add_raw_showings(attributes, raw_showtimes, day, "Coolidge Corner")

    return schedule

# Should also get the open caption showtimes from https://coolidge.org/films-events/open-captions
def _showtimes_text_iter(date_range):
    current_date, end_date = date_range
    while current_date <= end_date:
        page_text = _retrieve_page(current_date)
        yield (BeautifulSoup(page_text, 'html.parser'), current_date)
        current_date += timedelta(days=1)

def load_schedules_by_day(theater, date_range, quiet=False):
    schedules_by_day = []
    if not quiet:
        print(".", end="", flush=True)
    for showtimes_html, day in _showtimes_text_iter(date_range):
        schedules_by_day.append(_load_schedule(showtimes_html, day))

        if not quiet:
            print(".", end="", flush=True)

    return schedules_by_day
