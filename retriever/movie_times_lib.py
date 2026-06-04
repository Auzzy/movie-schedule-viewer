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
from retriever.utils import date_ranges, date_range_to_str, get_days_to_scan, group_dict_by, group_obj_by, offset_timezone


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
    theater = db.get_theater(schedule.theater)
    tz = offset_timezone(theater["tzname"])
    now = datetime.now(tz).replace(microsecond=0).isoformat()

    # The date_range is inclusive of the end time, but load_showtimes is not.
    aware_date_range = (date_range[0].astimezone(tz), date_range[1].astimezone(tz) + timedelta(days=1))

    current_showtimes = db.serialize_schedule(schedule)
    if theater["parser"] == "fandango_json":
        current_showtimes = [{k: v for k, v in s.items() if k != "screen"} for s in current_showtimes]

    deleted_showtimes = []
    for showtime in db.load_showtimes(*aware_date_range, theater=schedule.theater):
        showtime_dict = dict(showtime)
        if theater["parser"] == "fandango_json":
            # Screens for these showtimes aren't populated until later, so they're deleted separately.
            showtime_dict.pop("screen", None)
        showtime_dict.pop("extra_properties", None)

        if now < showtime_dict['start_time'] and showtime_dict not in current_showtimes:
            deleted_showtimes.append(showtime_dict)

    db.delete_showtimes(deleted_showtimes)

    return deleted_showtimes


def send_watchlist_notification():
    last_time = datetime.now()

    try:
        first_time = db.last_successful_task_run(db.Task.WATCHLIST_NOTIFICATIONS) or (last_time - timedelta(days=365))

        stored_showings = db.load_showtimes_by_create_time(first_time, last_time)

        showdates_by_title = defaultdict(lambda: defaultdict(set))
        for showing in stored_showings:
            showdate = datetime.fromisoformat(showing["start_time"]).date()
            showdates_by_title[showing["title"]][showing["theater"]].add(showdate)

        watched = db.load_all_watchlists()
        for client_id, entries in group_dict_by(watched, "client").items():
            lines = []
            for entry in entries:
                title = entry["title"]
                showdates_by_theater = showdates_by_title.get(title, {})
                if showdates_by_theater:
                    lines.append(f"Showings added for {title}:")
                    for theater, showdates in showdates_by_theater.items():
                        showdate_ranges = date_ranges(showdates)
                        showdates_str = ", ".join(date_range_to_str(dr) for dr in showdate_ranges)
                        lines.append(f"- {theater}: {showdates_str}")

            if lines:
                msg = "\n".join(lines)
                _send_email("Watchlist notification", msg, receiver=None)
        return True
    except Exception as exc:
        send_error_email(exc)
        return False

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

def send_deletion_report():
    def _start_range(showtimes):
        time_strs = sorted([s["start_time"] for s in showtimes])
        start = datetime.fromisoformat(time_strs[0]).replace(hour=0, minute=0, second=0, microsecond=0)
        end = datetime.fromisoformat(time_strs[-1]).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        return start, end

    try:
        last_time = datetime.now()
        first_time = db.last_successful_task_run(db.Task.DELETION_REPORT) or (last_time - timedelta(days=365))

        deleted_showtimes_by_theater = group_dict_by(db.load_deleted_showtimes_by_deletion_time(first_time, last_time), "theater")
        filtered_deleted_showtimes = []
        for theater, deleted_showtimes in deleted_showtimes_by_theater.items():
            deleted_showtimes = [{**s, "programs": list(s.get("programs", set()))} for s in deleted_showtimes]
            theater_showtimes = db.load_showtimes(*_start_range(deleted_showtimes), theater=theater)
            filtered_deleted_showtimes.extend(_true_deletion_filter(deleted_showtimes, theater_showtimes))

        if not filtered_deleted_showtimes:
            return True

        deleted_showtimes_json = "[\n" + ",\n".join([f"  {json.dumps(s, sort_keys=True)}" for s in filtered_deleted_showtimes]) + "\n]"
        deleted_attachment = _build_attachment(deleted_showtimes_json, "deleted.json")

        _send_email("Schedule Updater Deletion Report", "Deletion report attached",  attachments=[deleted_attachment])
        return True
    except Exception as exc:
        send_error_email(exc)
        return False


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


def gather_fandango_screens(theater):
    try:
        first_time = datetime.now().replace(microsecond=0)
        last_time = first_time + timedelta(days=get_days_to_scan())

        fandango_theaters = [theater["name"] for theater in db.get_theaters(clean=False) if theater["parser"] == "fandango_json"]
        if theater not in fandango_theaters:
            raise ValueError(f"{theater} is not one of: {fandango_theaters.join(', ')}.")

        showtimes = db.load_showtimes(first_time, last_time, theater)
        hash_to_auditorium = fandango_json.gather_seat_info(showtimes)
        db.update_showtime_screens(hash_to_auditorium)
        return True
    except Exception as exc:
        send_error_email(exc)
        return False
