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
