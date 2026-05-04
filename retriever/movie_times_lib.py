import base64
import importlib
import json
import os
import traceback
from collections import defaultdict
from datetime import datetime, timedelta

from ical.calendar import Calendar
from ical.calendar_stream import IcsCalendarStream
from ical.event import Event
from mailtrap import Address, Attachment, Mail, MailtrapClient

from retriever import db
from retriever.parsers import brattle, coolidge, fandango_json, red_river, somerville_theater
from retriever.schedule import Filter, FullSchedule, ParseError
from retriever.utils import date_ranges, date_range_to_str, group_dict_by, group_obj_by, offset_timezone


def _build_attachment(content, filename, *, encoding="utf-8"):
    return Attachment(
        content=base64.b64encode(content.encode(encoding)),
        filename=filename
    )

def _ics_attachments(schedules):
    attachments = []
    for schedule in schedules:
        calendar = Calendar()
        for movie in schedule.movies:
            for showing in movie.showings:
                start = showing.start
                end = showing.end or (start + timedelta(minutes=5))
                calendar.events.append(
                    Event(summary=movie.name, start=start, end=end),
                )

        calendar_ics = IcsCalendarStream.calendar_to_ics(calendar)
        attachments.append(_build_attachment(calendar_ics, f"{schedule.theater}.ics"))

    return attachments

def _plaintext_attachments(schedules):
    attachments = []
    for schedule in schedules:
        schedule_text = schedule.output(name_only=False, date_only=True)
        attachments.append(_build_attachment(schedule_text, f"{schedule.theater}.txt"))

    return attachments

def _send_email(subject, text, sender=None, sender_name=None, receiver=None, attachments=[]):
    sender = sender or os.environ.get("MAILTRAP_SENDER")
    sender_name = sender_name or os.environ.get("MAILTRAP_SENDER_NAME")
    receiver = receiver or os.environ.get("MAILTRAP_RECEIVER")

    mail = Mail(
        sender=Address(email=sender, name=sender_name),
        to=[Address(email=receiver)],
        subject=subject,
        text=text,
        attachments=attachments
    )

    client = MailtrapClient(token=os.environ["MAILTRAP_API_TOKEN"])
    client.send(mail)


def email_theater_schedules(schedules, dates, sender, sender_name, receiver):
    attachments = _plaintext_attachments(schedules) + _ics_attachments(schedules)

    subject = f"Movie Schedules {dates[0].isoformat()}"
    if dates[0] != dates[1]:
        subject += f" to {dates[1].isoformat()}"

    _send_email(subject, "Schedules attached", sender, sender_name, receiver, attachments)


def collect_schedule(theater, filepath, date_range, filter_params, quiet):
    date_range = [d.date() for d in date_range]

    theater_info = db.get_theater(theater)
    if not theater_info:
        print(f"[ERROR] No theater found with the name {theater}. Has it been added?")
        return

    parser = importlib.import_module(f"retriever.parsers.{theater_info['parser']}")
    raw_schedules = parser.load_schedules_by_day(theater_info, date_range, quiet)

    filtered_schedules = [schedule.filter(filter_params) for schedule in raw_schedules]

    if not filtered_schedules:
        date_range_str = ' - '.join(d.isoformat() for d in date_range)
        print(f"[WARN] Could not find any data for the requested date(s): {date_range_str}")
        return

    return FullSchedule.create(filtered_schedules)


def db_showtime_updates(date_range, schedule):
    tz = offset_timezone(db.get_theater(schedule.theater)["tzname"])
    now = datetime.now(tz).replace(microsecond=0).isoformat()

    # The date_range is inclusive of the end time, but load_showtimes is not.
    aware_date_range = (date_range[0].astimezone(tz), date_range[1].astimezone(tz) + timedelta(days=1))

    current_showtimes = db.serialize_schedule(schedule)

    deleted_showtimes = []
    for showtime in db.load_showtimes(schedule.theater, *aware_date_range):
        showtime_dict = dict(showtime)

        if now < showtime_dict['start_time'] and showtime_dict not in current_showtimes:
            deleted_showtimes.append(showtime_dict)

    db.delete_showtimes(deleted_showtimes)

    return deleted_showtimes


def send_watchlist_notification(schedules):
    watchlist = db.load_all_watchlists()
    watchlist_by_theater = group_dict_by(watchlist, "theater")

    mark_sent = []
    watchlist_hits = defaultdict(lambda: defaultdict(list))
    for schedule in schedules:
        theater_watchlist = {entry["title"].lower(): entry["sent_time"] is None for entry in watchlist_by_theater.get(schedule.theater, {})}
        for movie in schedule.movies:
            should_send = theater_watchlist.get(movie.name.lower())
            if not should_send:
                continue

            watchlist_hits[movie.name][schedule.theater].extend(movie.showings)
            mark_sent.append((schedule.theater, movie.name))

    if watchlist_hits:
        lines = []
        for title, showings_dict in watchlist_hits.items():
            lines.append(f"Showings added for {title}:")
            for theater, showings in showings_dict.items():
                showing_ranges = date_ranges(sorted({showing.start.date() for showing in showings}))
                showing_dates_str = ", ".join(date_range_to_str(dr) for dr in showing_ranges)
                lines.append(f"- {theater}: {showing_dates_str}")

        msg = "\n".join(lines)
        _send_email("Watchlist notification", msg, receiver=None)

        db.watchlist_mark_sent(mark_sent)


def _true_deletion_filter(deleted_showtimes, current_showtimes):
    def _drop_key(adict, key):
        return {k: v for k, v, in adict.items() if k != key}

    current_without_end = [_drop_key(showtime, "end_time") for showtime in current_showtimes]

    filtered_deleted_showtimes = []
    for showtime_dict in deleted_showtimes:
        if showtime_dict["start_time"] == showtime_dict["end_time"] and _drop_key(showtime_dict, "end_time") in current_without_end:
            print(f"SKIPPING {showtime_dict}")
            continue

        filtered_deleted_showtimes.append(showtime_dict)

    return filtered_deleted_showtimes

def send_deletion_report(day):
    def _start_range(showtimes):
        time_strs = sorted([s["start_time"] for s in showtimes])
        start = datetime.fromisoformat(time_strs[0]).replace(hour=0, minute=0, second=0, microsecond=0)
        end = datetime.fromisoformat(time_strs[-1]).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        return start, end

    day = day.replace(hour=0, minute=0, second=0, microsecond=0)
    eod = day + timedelta(days=1)

    deleted_showtimes_by_theater = group_dict_by(db.load_deleted_showtimes(day, eod), "theater")
    filtered_deleted_showtimes = []
    for theater, deleted_showtimes in deleted_showtimes_by_theater.items():
        theater_showtimes = db.load_showtimes(theater, *_start_range(deleted_showtimes))
        filtered_deleted_showtimes.extend(_true_deletion_filter(deleted_showtimes, theater_showtimes))

    deleted_showtimes_json = "[\n" + ",\n".join([f"  {json.dumps(s, sort_keys=True)}" for s in filtered_deleted_showtimes]) + "\n]"
    deleted_attachment = _build_attachment(deleted_showtimes_json, "deleted.json")

    _send_email("Schedule Updater Deletion Report", "Deletion report attached",  attachments=[deleted_attachment])


def send_error_email(exc):
    error_str = "".join(traceback.format_exception(exc))
    _send_email("Schedule Updater encountered an error", error_str)


def add_theater(name, *, tzname, parser, is_open=True, rank=None, fullname=None):
    fullname = fullname or name
    db.add_theater(name=name, fullname=fullname, tzname=tzname, is_open=is_open, rank=rank, parser=parser, code=None, query=None)


def add_theater_from_search(query, *, name=None, rank=None):
    name = name or query
    search_result = fandango_json.search(query)
    if len(search_result) == 1:
        result = search_result[0]
        tzname = fandango_json.get_tzname(result["code"])
        db.add_theater(**result, name=name, rank=rank, tzname=tzname)
    elif len(search_result) < 1:
        print(f"[ERROR] No results found for \"{query}\".") 
    else:
        print(f"[ERROR] Found mutiple theaters for \"{query}\". Please narrow the search term.")
        for result in search_result:
            print(f"- {result['fullname']}")
