import pytz
from dateutil.parser import parse
import datetime as dt


class DateTimeUtil:

    def __init__(self):
        pass

    @staticmethod
    def string_to_date(str_date, str_timezone):
        date_obj = parse(str_date)
        timezone = pytz.timezone(str_timezone)
        return date_obj.replace(tzinfo=timezone)

    @staticmethod
    def UTC_to_local(date_obj, str_local_timezone):
        local_timezone = pytz.timezone(str_local_timezone)
        return date_obj.astimezone(local_timezone)

    @staticmethod
    def get_day_of_year(date_obj):
        new_year_day = dt.datetime(year=date_obj.year, month=1, day=1, tzinfo=date_obj.tzinfo)
        return (date_obj - new_year_day).days + 1
