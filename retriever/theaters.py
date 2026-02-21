from retriever.utils import offset_timezone

THEATERS = {
    "AMC Methuen": {"code": "aaoze", "slug": "amc-methuen-20-aaoze", "tz": "US/Eastern"},
    "AMC Tyngsboro": {"code": "aadxs", "slug": "amc-tyngsboro-12-aadxs", "tz": "US/Eastern"},
    "AMC Boston Common": {"code": "aapnv", "slug": "amc-boston-common-19-aapnv", "tz": "US/Eastern"},
    "AMC Causeway": {"code": "aayqs", "slug": "amc-causeway-13-aayqs", "tz": "US/Eastern"},
    "Apple Hooksett": {"code": "aauoc", "slug": "apple-cinemas-hooksett-imax-aauoc", "tz": "US/Eastern"},
    "Apple Merrimack": {"code": "aatgl", "slug": "apple-cinemas-merrimack-aatgl", "tz": "US/Eastern"},
    "Showcase Randolph": {"code": "aaeea", "slug": "showcase-cinemas-de-lux-randolph-aaeea", "tz": "US/Eastern"},
    "O'Neil Epping": {"code": "aawvb", "slug": "oneil-cinemas-at-brickyard-square-aawvb", "tz": "US/Eastern"},
    "O'Neil Londonderry": {"code": "aakgu", "slug": "oneil-cinemas-londonderry-aakgu", "tz": "US/Eastern"}
}

THEATER_NAMES = tuple(THEATERS.keys())

def timezone(theater_name):
    return offset_timezone(THEATERS[theater_name]["tz"])
