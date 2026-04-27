from collections import defaultdict
from datetime import datetime, timezone
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
