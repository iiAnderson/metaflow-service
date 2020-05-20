import json
import sys
import traceback
from datetime import datetime, timedelta

async def read_body(request_content):
    byte_array = bytearray()
    while not request_content.at_eof():
        data = await request_content.read(4)
        byte_array.extend(data)

    return json.loads(byte_array.decode("utf-8"))


def get_traceback_str():
    """Get the traceback as a string."""

    exc_info = sys.exc_info()
    stack = traceback.extract_stack()
    _tb = traceback.extract_tb(exc_info[2])
    full_tb = stack[:-1] + _tb
    exc_line = traceback.format_exception_only(*exc_info[:2])
    return "\n".join(
        [
            "Traceback (most recent call last):",
            "".join(traceback.format_list(full_tb)),
            "".join(exc_line),
        ]
    )

def get_week_times():
    now = datetime.now()

    days = {}

    for i in [0, 1, 2, 3, 4, 5, 6]:
        n_days_ago = now - timedelta(days=i)
        days[get_formatted_time(n_days_ago)] = 0
    
    return days

def get_formatted_time(datetime_obj):
    return f"{datetime_obj.month}/{datetime_obj.day}"