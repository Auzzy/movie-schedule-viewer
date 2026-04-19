from retriever.movie_times_lib import add_theater, add_theater_from_search

add_theater_from_search("AMC Methuen", rank=1)
add_theater_from_search("AMC Boston Common", rank=2)
add_theater("Coolidge Corner", rank=3, fullname="Coolidge Corner", tzname="America/New_York", is_open=True, parser="coolidge")
add_theater("Brattle Theater", fullname="Brattle Theater", rank=4, tzname="America/New_York", is_open=True, parser="brattle")
add_theater("Red River", fullname="Red River Theatres", rank=5, tzname="America/New_York", is_open=True, parser="red_river")
add_theater("Somerville Theater", fullname="Somerville Theater", rank=6, tzname="America/New_York", is_open=True, parser="somerville_theater")
add_theater_from_search("AMC Tyngsboro", rank=7)
add_theater_from_search("AMC Causeway", rank=8)
add_theater_from_search("Apple Cinemas Hooksett", name="Apple Hooksett", rank=9)
add_theater_from_search("Apple Cinemas Merrimack", name="Apple Merrimack", rank=10)
add_theater_from_search("O'Neil Cinemas Londonderry", name="O'Neil Londonderry", rank=11)
