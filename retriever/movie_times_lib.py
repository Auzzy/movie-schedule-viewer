import base64
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
from retriever.fandango_json import load_schedules_by_day
from retriever.schedule import Filter, FullSchedule, ParseError
from retriever.theaters import timezone


def _build_attachment(content, filename, *, encoding="utf-8"):
    return Attachment(
        content=base64.b64encode(content.encode(encoding)),
        filename=filename
    )

def _ics_attachments(theaters_to_schedule):
    attachments = []
    for theater, schedule in theaters_to_schedule.items():
        calendar = Calendar()
        for movie in schedule.movies:
            for showing in movie.showings:
                start = showing.start
                end = showing.end or (start + timedelta(minutes=5))
                calendar.events.append(
                    Event(summary=movie.name, start=start, end=end),
                )

        calendar_ics = IcsCalendarStream.calendar_to_ics(calendar)
        attachments.append(_build_attachment(calendar_ics, f"{theater}.ics"))

    return attachments

def _plaintext_attachments(theaters_to_schedule):
    attachments = []
    for theater, schedule in theaters_to_schedule.items():
        schedule_text = schedule.output(name_only=False, date_only=True)
        attachments.append(_build_attachment(schedule_text, f"{theater}.txt"))

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


def email_theater_schedules(theaters_to_schedule, dates, sender, sender_name, receiver):
    attachments = _plaintext_attachments(theaters_to_schedule) + _ics_attachments(theaters_to_schedule)

    subject = f"Movie Schedules {dates[0].isoformat()}"
    if dates[0] != dates[1]:
        subject += f" to {dates[1].isoformat()}"

    _send_email(subject, "Schedules attached", sender, sender_name, receiver, attachments)


def collect_schedule(theater, filepath, date_range, filter_params, quiet):
    schedules_by_day = load_schedules_by_day(theater, filepath, date_range, filter_params, quiet)

    if not schedules_by_day:
        print("[WARN] Could not find any data for the requested date(s).")
        return

    return FullSchedule.create(schedules_by_day)


def db_showtime_updates(theater, date_range, detected_showtimes):
    tz = timezone(theater)
    now = datetime.now(tz).replace(microsecond=0).isoformat()

    # The date_range is inclusive of the end time, but load_showtimes is not.
    aware_date_range = (date_range[0].astimezone(tz), date_range[1].astimezone(tz) + timedelta(days=1))

    deleted_showtimes = []
    for showtime in db.load_showtimes(theater, *aware_date_range):
        showtime_dict = dict(showtime)

        if now < showtime_dict['start_time'] and showtime_dict not in detected_showtimes:
            deleted_showtimes.append(showtime_dict)

    db.delete_showtimes(deleted_showtimes)

    return deleted_showtimes


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
    def _group_by_theater(showtimes):
        showtimes_by_theater = defaultdict(list)
        for showtime in showtimes:
            showtimes_by_theater[showtime["theater"]].append(showtime)
        return dict(showtimes_by_theater)

    def _start_range(showtimes):
        time_strs = sorted([s["start_time"] for s in showtimes])
        start = datetime.fromisoformat(time_strs[0]).replace(hour=0, minute=0, second=0, microsecond=0)
        end = datetime.fromisoformat(time_strs[-1]).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        return start, end

    day = day.replace(hour=0, minute=0, second=0, microsecond=0)
    eod = day + timedelta(days=1)

    deleted_showtimes_by_theater = _group_by_theater(db.load_deleted_showtimes(day, eod))
    filtered_deleted_showtimes = []
    for theater, deleted_showtimes in deleted_showtimes_by_theater.items():
        theater_showtimes = db.load_showtimes(theater, *_start_range(deleted_showtimes))
        filtered_deleted_showtimes.extend(_true_deletion_filter(deleted_showtimes, theater_showtimes))

    deleted_showtimes_json = "[\n" + ",\n".join([f"  {json.dumps(s, sort_keys=True)}" for s in filtered_deleted_showtimes]) + "\n]"
    deleted_attachment = _build_attachment(deleted_showtimes_json, "deleted.json")

    _send_email("Schedule Updater Deletion Report", "Deletion report attached",  attachments=[deleted_attachment])


def send_error_email(exc):
    error_str = traceback.format_exception(exc)
    _send_email("Schedule Updater encountered an error", error_str)
