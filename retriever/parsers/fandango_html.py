import calendar
import re
from bs4 import BeautifulSoup
from datetime import date, timedelta
from playwright.sync_api import sync_playwright

from retriever.schedule import DaySchedule, THEATER_SLUG_DICT


RUNTIME_RE = re.compile(r"(?:(?P<hr>\d) hr)? ?(?:(?P<min>\d\d?) min)?")
LANGUAGE_RE = re.compile("([a-z]+) spoken with ([a-z]+) subtitles")


def _get_date(page):
    # TODO: How does it handle when the calendar rolls over? Does it also display the year?
    active_date_button = page.find("button", class_="date-picker__date--selected")
    month_text = active_date_button.find("span", class_="date-picker__date-month").get_text(strip=True)
    day_text = active_date_button.find("span", class_="date-picker__date-day").get_text(strip=True)
    return date(date.today().year, list(calendar.month_abbr).index(month_text), int(day_text))

    date_section = page.find("label", attrs={"aria-label": "Date Filter"})
    date_str = date_section("div")[1].get_text(strip=True)
    return date.fromisoformat(date_section.find("option", string=date_str)["value"])


def _load_schedule(page):
    day = _get_date(page)
    schedule = DaySchedule(day)
    for movie_info in page.find("ul", class_="thtr-mv-list").find_all("li", recursive=False):
        header = movie_info.find("h2", class_="thtr-mv-list__detail-title").get_text(strip=True)
        name = header.rsplit(maxsplit=1)[0]

        rating_and_runtime_str = movie_info.find("li", class_="thtr-mv-list__info-bloc-item").get_text(strip=True)
        runtime_str = rating_and_runtime_str.split(", ")[-1] if "," in rating_and_runtime_str else "0 hr 0 min"

        movie = schedule.add_raw_movie(name, runtime_str)

        showtimes_section = movie_info.find("div", class_="thtr-mv-list__amenity-group-wrap")
        for showtimes_listing in showtimes_section("div", class_="thtr-mv-list__amenity-group"):
            attributes = [attr.get_text(strip=True) for attr in showtimes_listing.find("ul", class_="fd-list-inline").find_all("li")]
            raw_showtimes = [next(showtime.stripped_strings) for showtime in showtimes_listing.find("ol", class_="showtimes-btn-list").find_all("li")]
            movie.add_raw_showings(attributes, raw_showtimes, day)

    return schedule

def _retrieve_page(theater, showdate):
    slug = THEATER_SLUG_DICT[theater]
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(f"https://www.fandango.com/{slug}/theater-page?format=all&date={showdate.isoformat()}")
        content = page.content()
        browser.close()
    return content


def _showtimes_text_iter(theater, filepath, showdate, date_range):
    if filepath:
        with open(filepath) as showtimes_file:
            yield showtimes_file.read()
    elif showdate:
        yield _retrieve_page(theater, showdate)
    elif date_range:
        current_date, end_date = date_range
        while current_date <= end_date:
            yield _retrieve_page(theater, current_date)
            current_date += timedelta(days=1)


def load_schedules_by_day(theater, filepath, showdate, date_range, filter_params):
    schedules_by_day = []
    print(".", end="", flush=True)
    for showtimes_text in _showtimes_text_iter(theater, filepath, showdate, date_range):
        page = BeautifulSoup(showtimes_text, 'html.parser')
        schedule = _load_schedule(page)
        filtered_schedule = schedule.filter(filter_params)
        schedules_by_day.append(filtered_schedule)
        print(".", end="", flush=True)
    print(end="\n\n")

    return schedules_by_day
