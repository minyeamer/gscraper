from .cast import cast_int, cast_datetime, cast_date, get_timezone
from .map import re_get
from .types import TypeHint, DateNumeric, DateFormat, Timedelta, Timezone
from .types import is_type, is_str_type, is_timestamp_type, INTEGER_TYPES

from typing import List, Optional, Union
from dateutil.parser import parse as dateparse
from pandas.tseries.offsets import BDay
from pytz import timezone
import datetime as dt
import numpy as np
import pandas as pd
import re

KST = "Asia/Seoul"
NOW = dt.datetime.now(timezone(KST)).replace(microsecond=0, tzinfo=None)
TODAY = NOW.replace(hour=0, minute=0, second=0, microsecond=0)

DATEPART = ["SECOND", "MINUTE", "HOUR", "DAY", "MONTH", "YEAR"]
WEEKDAY = ["월", "화", "수", "목", "금", "토", "일"]

DATETIME_FORMAT = {
    "date": "%Y-%m-%d",
    "datetime": "%Y-%m-%d %H:%M:%S",
    "datetime_ms": "%Y-%m-%d %H:%M:%S.%f",
    "timezone": "%Y-%m-%d %H:%M:%S%z",
    "timezone_ms": "%Y-%m-%d %H:%M:%S.%f%z",
    "js": "%Y-%m-%dT%H:%M:%S.%f%z",
}

DATETIME_PATTERN = {
    "date": r"^\d{4}-\d{2}-\d{2}$",
    "datetime": r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
    "datetime_ms": r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+$",
    "timezone": r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+\d{2}:\d{2}$",
    "timezone_ms": r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+\+\d{2}:\d{2}$",
    "js": r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2}$",
}

COMMON_DATE_PATTERN = r"\d{4}-\d{2}-\d{2}"
COMMON_DATETIME_PATTERN = r"^(?:\d{4}-\d{2}-\d{2}(?: \d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:[+-]\d{4})?)?|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:[+-]\d{4})?)$"

PANDAS_FREQUENCY = {
    "B": "business day frequency",
    "C": "custom business day frequency",
    "D": "calendar day frequency",
    "W": "weekly frequency",
    "M": "month end frequency",
    "SM": "semi-month end frequency (15th and end of month)",
    "BM": "business month end frequency",
    "CBM": "custom business month end frequency",
    "MS": "month start frequency",
    "SMS": "semi-month start frequency (1st and 15th)",
    "BMS": "business month start frequency",
    "CBMS": "custom business month start frequency",
    "Q": "quarter end frequency",
    "BQ": "business quarter end frequency",
    "QS": "quarter start frequency",
    "BQS": "business quarter start frequency",
    "A": "year end frequency",
    "Y": "year end frequency",
    "BA": "business year end frequency",
    "BY": "business year end frequency",
    "AS": "year start frequency",
    "YS": "year start frequency",
    "BAS": "business year start frequency",
    "BYS": "business year start frequency",
    "BH": "business hour frequency",
    "H": "hourly frequency",
    "T": "minutely frequency",
    "min": "minutely frequency",
    "S": "secondly frequency",
    "L": "milliseconds",
    "ms": "milliseconds",
    "U": "microseconds",
    "us": "microseconds",
    "N": "nanoseconds"
}


strptimekr = lambda date_string: cast_datetime(date_string, tzinfo=KST, droptz=True)
strpdatekr = lambda date_string: cast_date(strptimekr(date_string))


def trunc_datetime(__datetime: dt.datetime, part=str(), **kwargs) -> dt.datetime:
    if part not in DATEPART: return __datetime
    index = DATEPART.index(part)
    if index >= 0: __datetime = __datetime.replace(microsecond=0)
    if index >= 1: __datetime = __datetime.replace(second=0)
    if index >= 2: __datetime = __datetime.replace(minute=0)
    if index >= 3: __datetime = __datetime.replace(hour=0)
    if index >= 4: __datetime = __datetime.replace(day=1)
    if index >= 5: __datetime = __datetime.replace(month=1)
    return __datetime


def now(__format=str(), days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0,
        hours=0, weeks=0, tzinfo=KST, droptz=True, datetimePart="SECOND", **kwargs) -> Union[dt.datetime,str]:
    delta = dt.timedelta(days, seconds, microseconds, milliseconds, minutes, hours, weeks)
    __datetime = dt.datetime.now(get_timezone(tzinfo)) - delta
    if droptz: __datetime = __datetime.replace(tzinfo=None)
    if datetimePart: __datetime = trunc_datetime(__datetime, datetimePart)
    return __datetime.strftime(__format) if __format else __datetime


def today(__format=str(), days=0, weeks=0, tzinfo=KST, **kwargs) -> Union[dt.date,str]:
    if __format: return now(__format, days=days, weeks=weeks, tzinfo=tzinfo, datetimePart="DAY")
    else: return now(days=days, weeks=weeks, tzinfo=tzinfo, datetimePart="DAY").date()


def get_datetime(__object: Optional[DateFormat]=None, days=0, seconds=0, microseconds=0, milliseconds=0,
                minutes=0, hours=0, weeks=0, tzinfo=None, astimezone=None, droptz=False, datetimePart="SECOND",
                **kwargs) -> dt.datetime:
    context = dict(tzinfo=tzinfo, astimezone=astimezone, droptz=droptz)
    __datetime = cast_datetime(__object, **context)
    __datetime = __datetime if isinstance(__datetime, dt.datetime) else now(days=cast_int(__object), **context)
    if isinstance(__datetime, dt.datetime):
        __datetime = __datetime - dt.timedelta(days, seconds, microseconds, milliseconds, minutes, hours, weeks)
        return trunc_datetime(__datetime, datetimePart) if datetimePart else __datetime


def get_time(__object: Optional[DateFormat]=None, days=0, seconds=0, microseconds=0, milliseconds=0,
            minutes=0, hours=0, weeks=0, tzinfo=None, astimezone=None, datetimePart="SECOND",
            **kwargs) -> dt.time:
    __datetime = get_datetime(**locals())
    if isinstance(__datetime, dt.datetime):
        return __datetime.time()


def get_timestamp(__object: Optional[DateFormat]=None, days=0, seconds=0, microseconds=0, milliseconds=0,
                minutes=0, hours=0, weeks=0, tzinfo=None, astimezone=None, droptz=False, datetimePart="SECOND",
                tsUnit="ms", **kwargs) -> int:
    __datetime = get_datetime(**locals())
    if isinstance(__datetime, dt.datetime):
        return int(__datetime.timestamp()*(1000 if tsUnit == "ms" else 1))


def get_date(__object: Optional[DateFormat]=None, days=0, weeks=0, tzinfo=None, **kwargs) -> dt.date:
    __date = cast_date(__object, tzinfo=tzinfo)
    __date = __date if isinstance(__date, dt.date) else today(days=cast_int(__object), tzinfo=tzinfo)
    if isinstance(__date, dt.date):
        return __date - dt.timedelta(days=days, weeks=weeks)


def get_busdate(__object: Optional[DateFormat]=None, days=0, weeks=0, tzinfo=None, **kwargs) -> dt.date:
    __date = get_date(**locals())
    if isinstance(__date, dt.date):
        return __date if np.is_busday(__date) else (__date-BDay(1)).date()


def is_datetime_format(__object: DateFormat, strict=False, **kwargs) -> bool:
    if isinstance(__object, str) and re.fullmatch(COMMON_DATETIME_PATTERN, __object):
        try: return bool(dateparse(__object)) if strict else True
        except: return False
    else: return False


def get_datetime_format(__object: DateFormat, strict=False, **kwargs) -> str:
    if is_datetime_format(__object, strict=strict):
        return "date" if re.fullmatch(COMMON_DATE_PATTERN, __object) else "datetime"


def cast_datetime_format(__object: DateFormat, default=None, strict=False, **kwargs) -> DateNumeric:
    format = get_datetime_format(__object, strict=strict)
    if format == "date": return cast_date(__object, default=default)
    elif format == "datetime": return cast_datetime(__object, default=default)
    else: return default


def is_pandas_frequency(__object: Timedelta, **kwargs) -> bool:
    if isinstance(__object, dt.timedelta): return True
    elif isinstance(__object, str): return any(map(lambda freq: __object.endswith(freq), PANDAS_FREQUENCY.keys()))
    else: return False


def get_date_range(startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                    periods: Optional[int]=None, interval: Timedelta="D",
                    tzinfo: Optional[Timezone]=None, **kwargs) -> List[dt.date]:
    if sum(map(bool, (startDate, endDate, periods, interval))) < 3: return list()
    if isinstance(interval, int):
        interval = dt.timedelta(days=interval)
    if not is_pandas_frequency(interval): return list()
    else: return [date.date() for date in pd.date_range(startDate, endDate, periods, interval, tzinfo)]


def set_datetime(__datetime: dt.datetime, __type: TypeHint=str,
                __format="%Y-%m-%d %H:%M:%S", tsUnit="ms", **kwargs) -> Union[str,int]:
    if __format: return __datetime.strftime(DATETIME_FORMAT.get(__format,__format))
    elif is_str_type(__type): return str(__datetime)
    elif is_timestamp_type(__type): return get_timestamp(__datetime, tsUnit=tsUnit)
    else: return


def set_date(__date: dt.date, __type: TypeHint=str,
            __format="%Y-%m-%d", **kwargs) -> Union[str,int]:
    if __format: return __date.strftime(DATETIME_FORMAT.get(__format,__format))
    elif is_str_type(__type): return str(__date)
    elif is_type(__type, INTEGER_TYPES+["ordinal"]): return __date.toordinal()
    else: return


def relative_strptime(__date_string: str, **kwargs) -> dt.datetime:
    str2int = lambda pattern, string: cast_int(re_get(pattern, string))
    if re.search("(\d+)초 전", __date_string):
        return now(seconds=str2int("(\d+)초 전", __date_string))
    elif re.search("(\d+)분 전", __date_string):
        return now(minutes=str2int("(\d+)분 전", __date_string))
    elif re.search("(\d+)시간 전", __date_string):
        return now(hours=str2int("(\d+)시간 전", __date_string))
    elif re.search("(\d+)일 전", __date_string):
        return today(days=str2int("(\d+)일 전", __date_string))
    elif "어제" in __date_string:
        return today(days=1)
    else: return cast_datetime(__date_string)


def strfweek(__date: dt.date, **kwargs) -> str:
    cur_year, weeknum, _ = __date.isocalendar()
    year, first_week, first_day = __date.replace(day=1).isocalendar()
    if cur_year != year:
        return f"{cur_year}년{__date.month}월{weeknum}주차"
    monthly_week = weeknum - first_week + int(first_day < 4)
    return f"{year}년{__date.month}월{monthly_week}주차"
