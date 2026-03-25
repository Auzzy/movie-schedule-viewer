import re
from datetime import date, datetime, timedelta

import requests
from bs4 import BeautifulSoup, Tag

from retriever.schedule import DaySchedule, FullSchedule

THEATER_NAME = "Red River"
MAIN_URL = "https://redrivertheatres.org/"
SHOWTIMES_URL = "https://ticketing.useast.veezi.com/sessions/?siteToken=rh66des21wzqpsqgg0jkjqcr88"
REQUEST_HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36'}
RUNTIME_RE = re.compile("\((?P<runtime>\d{1,3}) min.*\) \d{4}")

def _retrieve_page(url):
    return requests.get(url, headers=REQUEST_HEADERS).text

def _get_attributes(movie_info):
    attributes = ["Standard"]
    attribute_el = movie_info.find(class_="screen-attribute")
    if attribute_el and attribute_el.get_text(strip=True) == "OCAP":
        return attributes + ["Open Caption"]
    else:
        return attributes

def _clean_name(name):
    if name.endswith("Open Captions"):
        name = name.rsplit("-", 1)[0]
    return name.strip()

def _load_schedules(page, runtime_dict):
    schedules = {}
    for movie_info in page.find_all(class_="film"):
        name = _clean_name(movie_info.find(class_="title").get_text(strip=True))

        showdate_str = movie_info.find(class_="date").get_text(strip=True)
        showdate = datetime.strptime(showdate_str, "%A %d, %B").replace(year=date.today().year).date()
        if date.today() > showdate:
            showdate = showdate.replace(year=showdate.year + 1)

        for screening_info in movie_info.find(class_="session-times").children:
            if not isinstance(screening_info, Tag):
                continue

            raw_start_time = screening_info.find("time").get_text(strip=True)

            schedule = schedules[showdate] = schedules.get(showdate, DaySchedule(showdate))
            
            movie = next((m for m in schedule.movies if m.name == name), None)
            if not movie:
                runtime_str = runtime_dict.get(name, "0")
                movie = schedule.add_raw_movie(name, runtime_str)
            
            attributes = _get_attributes(movie_info)
            movie.add_raw_showings(attributes, [raw_start_time], showdate, THEATER_NAME)

    return sorted(schedules.values(), key=lambda s: s.day)

# Red River's main page is an unreliable source of showtimes. It only displays
# ten showtimes per entry before requiring the user to click a link for the
# rest, and it's not uncommon for it to omit movies altogether in error.
# However, the main ticketing page doesn't include runtime info.
# Thus, we attempt to load the runtimes from the main page, so they can be
# looked up while parsing the ticketing page.
def _load_runtimes_by_movies():
    main_html = BeautifulSoup(_retrieve_page(MAIN_URL), 'html.parser')
    runtime_dict = {}
    for movie_info in main_html.find_all(class_="podsfilm"):
        name = _clean_name(movie_info.find(class_="podsfilmtitlelink").get_text(strip=True))

        details_str = movie_info.find(class_="showinfodiv").get_text(strip=True)
        runtime_match = RUNTIME_RE.match(details_str)
        
        runtime_dict[name] = runtime_match.group("runtime") if runtime_match else 0
    return runtime_dict


def load_schedules_by_day(date_range, quiet=False):
    schedules_by_day = []
    showtimes_html = BeautifulSoup(_retrieve_page(SHOWTIMES_URL), 'html.parser')
    runtime_dict = _load_runtimes_by_movies()
    schedules_by_day = _load_schedules(showtimes_html, runtime_dict)
    return [s for s in schedules_by_day if date_range[0] <= s.day <= date_range[1]]

if __name__ == "__main__":
    schedules_by_day = load_schedules_by_day((date(2026, 3, 25), date(2026, 4, 30)))
    print(FullSchedule.create(schedules_by_day).output(False, False))
