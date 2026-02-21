import argparse
import base64
import os

from ical.calendar import Calendar
from ical.calendar_stream import IcsCalendarStream
from ical.event import Event
from mailtrap import Address, Attachment, Mail, MailtrapClient

from retriever import db
from retriever.fandango_json import load_schedules_by_day
from retriever.schedule import Filter, FullSchedule, ParseError, \
        date_range_str_parser as _raw_date_parser, time_str_parser as _raw_time_parser
from retriever.movie_times_lib import collect_schedule, db_showtime_updates, \
        email_theater_schedules, send_deletion_report
from retriever.theaters import THEATER_NAMES


def _wrap_parser(parser):
    def parse(value):
        try:
            return parser(value)
        except ParseError as exc:
            raise argparse.ArgumentTypeError(str(exc))
    return parse

date_range_str_parser = _wrap_parser(_raw_date_parser)
time_str_parser = _wrap_parser(_raw_time_parser)


def db_main(theater, date_range, deletion_report=True):
    schedule_range = collect_schedule(theater, None, date_range, Filter.empty(), False)
    showtimes = db.store_showtimes(theater, schedule_range)
    deleted_showtimes = db_showtime_updates(theater, date_range, showtimes)
    if deletion_report and deleted_showtimes:
        send_deletion_report(deleted_showtimes)

def email_main(dates, theaters, sender, sender_name, receiver):
    theaters = theaters or THEATER_NAMES

    theaters_to_schedule = {theater: collect_schedule(theater, None, dates, Filter.empty(), True) for theater in theaters}
    email_theater_schedules(theaters_to_schedule, dates, sender, sender_name, receiver)

def cli_main(theater, filepath, date_range, name_only, date_only, filter_params):
    schedule_range = collect_schedule(theater, filepath, date_range, filter_params, False)
    
    print(end="\n\n")
    print(schedule_range.output(name_only, date_only))
    print(f"\n- {len(schedule_range)} showtimes")

def main(args):
    if args.output == "cli":
        filter_params = Filter(args.earliest, args.latest, args.movie, args.not_movie, args.format, args.not_format)
        cli_main(args.theater, args.filepath, args.date_range, args.name_only, args.date_only, filter_params)
    elif args.output == "email":
        email_main(args.date_range, args.theaters, args.frm, args.from_name, args.to)
    elif args.output == "db":
        db_main(args.theater, args.date_range, args.deletion_report)


def parse_args():
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(title="output modes")

    cli_parser = subparsers.add_parser("plaintext", help="Output in plaintext to stdout")
    cli_parser.set_defaults(output="cli")
    cli_parser.add_argument("--theater", default="AMC Methuen", choices=sorted(THEATER_NAMES))
    input_group = cli_parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--filepath")
    input_group.add_argument("--date", type=date_range_str_parser, dest="date_range")
    cli_parser.add_argument("--name-only", action="store_true")
    cli_parser.add_argument("--date-only", action="store_true")
    cli_parser.add_argument("--earliest", "-e", type=time_str_parser)
    cli_parser.add_argument("--latest", "-l", type=time_str_parser)
    cli_parser.add_argument("--movie", "-m", action="append")
    cli_parser.add_argument("--not-movie", action="append")
    cli_parser.add_argument("--format", "-f", action="append")
    cli_parser.add_argument("--not-format", action="append")
    
    email_parser = subparsers.add_parser("email", help="Email the result.")
    email_parser.set_defaults(output="email")
    email_parser.add_argument("--date", type=date_range_str_parser, dest="date_range", default="next movie week")
    email_parser.add_argument("--theater", action="append", choices=sorted(THEATER_NAMES), dest="theaters")
    email_parser.add_argument("--from", dest="frm")
    email_parser.add_argument("--from-name", default="Test Movie Sender")
    email_parser.add_argument("--to")

    db_parser = subparsers.add_parser("db", help="Output the result to a database.")
    db_parser.set_defaults(output="db")
    db_parser.add_argument("--theater", default="AMC Methuen", choices=sorted(THEATER_NAMES))
    db_parser.add_argument("--date", type=date_range_str_parser, dest="date_range", default="next movie week")
    db_parser.add_argument("--deletion-report", action="store_true")

    return parser.parse_args()

if __name__ == "__main__":
    main(parse_args())
