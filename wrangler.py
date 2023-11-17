import awswrangler as wr
import pandas as pd
from datetime import datetime

df = wr.athena.read_sql_query(
    sql="SELECT * FROM webbeds_bookings.bookings WHERE year=:x; AND Month=:y;",
    params={"x": "2023", "y": "11"},
    database="webbeds_bookings"
) 