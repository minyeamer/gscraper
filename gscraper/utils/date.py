from gscraper.base.types import TypeHint, DateFormat, Timedelta, Timezone, CastError
from gscraper.base.types import is_type, is_str_type, is_timestamp_type, INTEGER_TYPES

from gscraper.utils.cast import cast_datetime, cast_date, get_timezone
from gscraper.utils.map import drop_dict

from typing import List, Literal, Optional, Union
from pandas.tseries.offsets import BDay
import datetime as dt
import numpy as np
import pandas as pd
import re

UTC = "UTC"
EST = "US/Eastern"
KST = "Asia/Seoul"

DATE_UNIT = ["second", "minute", "hour", "day", "month", "year"]

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
    "N": "nanoseconds",
    "W-SUN":'', "W-MON":'', "W-TUE":'', "W-WED":'', "W-THU":'', "W-FRI":'', "W-SAT":'',
}

DATE_RANGE_MSG = "Of the four parameters: start, end, periods, and freq, exactly three must be specified."
INVALID_INTERVAL_MSG = lambda interval: f"'{interval}' is not valid date interval for pandas date range."


def trunc_datetime(__datetime: dt.datetime,
                    unit: Literal["second","minute","hour","day","month","year"]=str()) -> dt.datetime:
    if unit not in DATE_UNIT: return __datetime
    index = DATE_UNIT.index(unit.lower())
    if index >= 0: __datetime = __datetime.replace(microsecond=0)
    if index >= 1: __datetime = __datetime.replace(second=0)
    if index >= 2: __datetime = __datetime.replace(minute=0)
    if index >= 3: __datetime = __datetime.replace(hour=0)
    if index >= 4: __datetime = __datetime.replace(day=1)
    if index >= 5: __datetime = __datetime.replace(month=1)
    return __datetime


def now(__format=str(), days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0,
        hours=0, weeks=0, tzinfo=None, droptz=False, droptime=False, 
        unit: Literal["second","minute","hour","day","month","year"]="second") -> Union[dt.datetime,dt.date,str]:
    try: delta = dt.timedelta(days, seconds, microseconds, milliseconds, minutes, hours, weeks)
    except CastError: return

    __datetime = dt.datetime.now(get_timezone(tzinfo)) - delta
    if droptz: __datetime = __datetime.replace(tzinfo=None)
    if unit: __datetime = trunc_datetime(__datetime, unit=unit)
    if droptime: __datetime = __datetime.date()
    return __datetime.strftime(__format) if __format else __datetime


def today(__format=str(), days=0, weeks=0, tzinfo=None) -> Union[dt.date,str]:
    if __format: return now(__format, days=days, weeks=weeks, tzinfo=tzinfo, droptime=True, unit="day")
    else: return now(days=days, weeks=weeks, tzinfo=tzinfo, droptime=True, unit="day")


def get_datetime(__object: Optional[DateFormat]=None, if_null: Optional[Union[int,str]]=0,
                days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0,
                tzinfo=None, astimezone=None, droptz=False,
                unit: Literal["second","minute","hour","day","month","year"]="second") -> dt.datetime:
    context = dict(tzinfo=tzinfo, astimezone=astimezone, droptz=droptz)
    __datetime = cast_datetime(__object, **context)
    if not isinstance(__datetime, dt.datetime):
        if isinstance(__object, int): __datetime = now(hours=__object, **context)
        elif isinstance(if_null, int): __datetime = now(hours=if_null, **context)
        elif isinstance(if_null, str): __datetime = cast_datetime(__object, **context)
    if isinstance(__datetime, dt.datetime):
        __datetime = __datetime - dt.timedelta(days, seconds, microseconds, milliseconds, minutes, hours, weeks)
        return trunc_datetime(__datetime, unit=unit) if unit else __datetime


def get_time(__object: Optional[DateFormat]=None, if_null: Optional[Union[int,str]]=0,
            seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, tzinfo=None, astimezone=None,
            unit: Literal["second","minute","hour","day","month","year"]="second") -> dt.time:
    __datetime = get_datetime(**locals())
    if isinstance(__datetime, dt.datetime):
        return __datetime.time()


def get_timestamp(__object: Optional[DateFormat]=None, if_null: Optional[Union[int,str]]=0,
                days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0,
                tzinfo=None, astimezone=None, droptz=False, tsUnit: Literal["ms","s"]="ms",
                unit: Literal["second","minute","hour","day","month","year"]="second") -> int:
    __datetime = get_datetime(**drop_dict(locals(), "tsUnit", inplace=False))
    if isinstance(__datetime, dt.datetime):
        return int(__datetime.timestamp()*(1000 if tsUnit == "ms" else 1))


def get_date(__object: Optional[DateFormat]=None, if_null: Optional[Union[int,str]]=0,
            days=0, weeks=0, tzinfo=None) -> dt.date:
    __date = cast_date(__object)
    if not isinstance(__date, dt.date):
        if isinstance(__object, int): __date = today(days=__object, tzinfo=tzinfo)
        elif isinstance(if_null, int): __date = today(days=if_null, tzinfo=tzinfo)
        elif isinstance(if_null, str): __date = cast_date(if_null)
    if isinstance(__date, dt.date):
        return __date - dt.timedelta(days=days, weeks=weeks)


def get_busdate(__object: Optional[DateFormat]=None, if_null: Optional[Union[int,str]]=0,
                days=0, weeks=0, tzinfo=None) -> dt.date:
    __date = get_date(__object, if_null=if_null, tzinfo=tzinfo)
    if isinstance(__date, dt.date):
        if not np.is_busday(__date): __date = (__date-BDay(1)).date()
        if days or weeks: return get_busdate(__date - dt.timedelta(days=days, weeks=weeks))
        return __date


def is_pandas_frequency(interval: Timedelta) -> bool:
    if isinstance(interval, dt.timedelta): return True
    elif isinstance(interval, str):
        return any(map(lambda freq: interval.upper().endswith(freq.upper()), PANDAS_FREQUENCY.keys()))
    else: return False


def is_daily_frequency(interval: Timedelta) -> bool:
    if isinstance(interval, dt.timedelta): return (interval.days == 1) and (interval.seconds == 0)
    elif isinstance(interval, str): return bool(re.match(r"^(?=.*[Dd])(?!.*[02-9]).*$", interval))
    else: return False


def validate_pandas_frequency(interval: Timedelta) -> Timedelta:
    if is_pandas_frequency(interval): return interval
    elif isinstance(interval, int): return dt.timedelta(days=interval)
    else: raise INVALID_INTERVAL_MSG(interval)


def get_date_range(startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                    periods: Optional[int]=None, interval: Timedelta="D", tzinfo: Optional[Timezone]=None) -> List[dt.date]:
    if sum(map(bool, (startDate, endDate, periods, interval))) < 3: raise ValueError(DATE_RANGE_MSG)
    interval = validate_pandas_frequency(interval)
    return [date.date() for date in pd.date_range(startDate, endDate, periods, interval, tzinfo)]


def set_datetime(__datetime: dt.datetime, __type: TypeHint=str,
                __format="%Y-%m-%d %H:%M:%S", tsUnit: Literal["ms","s"]="ms") -> Union[str,int]:
    if __format: return __datetime.strftime(DATETIME_FORMAT.get(__format,__format))
    elif is_str_type(__type): return str(__datetime)
    elif is_timestamp_type(__type): return get_timestamp(__datetime, tsUnit=tsUnit)
    else: return


def set_date(__date: dt.date, __type: TypeHint=str, __format="%Y-%m-%d") -> Union[str,int]:
    if __format: return __date.strftime(DATETIME_FORMAT.get(__format,__format))
    elif is_str_type(__type): return str(__date)
    elif is_type(__type, INTEGER_TYPES+["ordinal"]): return __date.toordinal()
    else: return
