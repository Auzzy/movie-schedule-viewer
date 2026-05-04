import calendar
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

def offset_timezone(tzname):
    """Crafts the offset version of a timezone.

    When applying certain timezones (e.g. America/New_York) to a time object,
    they are considered naive due to the omission of offset data (for some
    reason). This loads up the named time zone as a datetime, extracts its
    offset info, and crafts a fresh (named) timezone object from it.
    """
    now = datetime.now(ZoneInfo(tzname))
    return timezone(now.tzinfo.utcoffset(now), tzname)

def group_by(items, key):
    grouped_items = defaultdict(list)
    for item in items:
        grouped_items[key(item)].append(item)
    return dict(grouped_items)

def group_dict_by(items, key):
    return group_by(items, lambda item: item[key])

def group_obj_by(items, attr):
    return group_by(items, lambda item: getattr(item, attr))

def date_ranges(date_list):
    ranges = []
    start, end = date_list[0], None
    for nxt in date_list[1:]:
        if (end and nxt == end + timedelta(days=1)) or nxt == start + timedelta(days=1):
            end = nxt
        else:
            ranges.append((start, end))
            start, end = nxt, None

    return ranges + [(start, end)]

def date_range_to_str(date_range):
    if not date_range[1]:
        return date_range[0].strftime("%b %d, %Y")

    start, end = date_range
    if start.year == end.year:
        if start.month == end.month:
            return f"{calendar.month_abbr[start.month]} {start.day} - {end.day}, {start.year}"
        else:
            return " - ".join(d.strftime('%b %d') for d in date_range) + f", {start.year}"
    else:
        return " - ".join(d.strftime('%b %d, %Y') for d in date_range)
