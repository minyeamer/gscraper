from gscraper.base.types import TypeHint, Logic, Unit, DateFormat, Timedelta, Timezone, CastError
from gscraper.base.types import is_type, is_str_type, is_timestamp_type, INTEGER_TYPES

from gscraper.utils.cast import cast_datetime, cast_date, get_timezone
from gscraper.utils.map import isin, drop_dict, get_scala

from typing import Dict, List, Literal, Optional, Sequence, Tuple, Union
from pandas.tseries.frequencies import to_offset
import datetime as dt
import pandas as pd
import random
import re

from holidays.countries import *
from holidays import country_holidays

from workalendar.core import Calendar
from workalendar.europe import *
from workalendar.usa import *
from workalendar.america import *
from workalendar.africa import *
from workalendar.asia import *
from workalendar.oceania import *
from workalendar.registry import registry


UTC = "UTC"
EST = "US/Eastern"
KST = "Asia/Seoul"

US = "US"
KR = "KR"

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

DATE_RANGE_MSG = "Of the four parameters: start, end, periods, and freq, exactly three must be specified."
INVALID_INTERVAL_MSG = lambda interval: f"'{interval}' is not valid date interval for pandas date range."


###################################################################
######################### Current Datetime ########################
###################################################################

def trunc_datetime(__datetime: dt.datetime, unit: Literal["second","minute","hour","day","month","year"]=str()) -> dt.datetime:
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


def today(__format=str(), days=0, weeks=0, tzinfo=None, droptime=True) -> Union[dt.date,dt.datetime,str]:
    return now(__format, days=days, weeks=weeks, tzinfo=tzinfo, droptime=droptime, unit="day")


###################################################################
########################### Get Datetime ##########################
###################################################################

def get_datetime(__object: Optional[DateFormat]=None, if_null: Optional[Union[int,str]]=0,
                days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0,
                tzinfo=None, astimezone=None, droptz=False,
                unit: Literal["second","minute","hour","day","month","year"]="second") -> dt.datetime:
    __datetime = cast_datetime(__object, tzinfo=tzinfo, astimezone=astimezone, droptz=droptz)
    if not isinstance(__datetime, dt.datetime):
        if isinstance(__object, int): __datetime = now(hours=__object, tzinfo=tzinfo, droptz=droptz)
        elif isinstance(if_null, int): __datetime = now(hours=if_null, tzinfo=tzinfo, droptz=droptz)
        elif isinstance(if_null, str): __datetime = cast_datetime(__object, tzinfo=tzinfo, astimezone=astimezone, droptz=droptz)
    if isinstance(__datetime, dt.datetime):
        __datetime = __datetime - dt.timedelta(days, seconds, microseconds, milliseconds, minutes, hours, weeks)
        return trunc_datetime(__datetime, unit=unit) if unit else __datetime


def get_datetime_pair(startTime: Optional[DateFormat]=None, endTime: Optional[DateFormat]=None,
                    if_null: Optional[Unit]=None, tzinfo=None, astimezone=None, droptz=False,
                    unit: Literal["second","minute","hour","day","month","year"]="second") -> Tuple[dt.datetime,dt.datetime]:
    context = dict(tzinfo=tzinfo, astimezone=astimezone, droptz=droptz, unit=unit)
    startTime = get_datetime(startTime, if_null=get_scala(if_null, 0), **context)
    endTime = get_datetime(endTime, if_null=get_scala(if_null, 1), **context)
    __min = min(startTime, endTime) if startTime and endTime else startTime
    __max = max(startTime, endTime) if startTime and endTime else endTime
    return __min, __max


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


def get_random_seconds(start: Union[float,int], stop: Optional[Union[float,int]]=None, step=1) -> Union[float,int]:
    has_millis = isinstance(start, float) or isinstance(stop, float)
    if has_millis:
        start = int(start * 1000)
        stop = int(stop * 1000) if isinstance(stop, (float,int)) else None
    secs = random.randrange(start, stop, step)
    return secs/1000 if has_millis else secs


def get_date(__object: Optional[DateFormat]=None, if_null: Optional[Union[int,str]]=0,
            days=0, weeks=0, tzinfo=None, busdate=False, country_code=str()) -> dt.date:
    __date = cast_date(__object)
    if not isinstance(__date, dt.date):
        if isinstance(__object, int): __date = today(days=__object, tzinfo=tzinfo)
        elif isinstance(if_null, int): __date = today(days=if_null, tzinfo=tzinfo)
        elif isinstance(if_null, str): __date = cast_date(if_null)
    if isinstance(__date, dt.date):
        __date = __date - dt.timedelta(days=days, weeks=weeks)
        return get_last_working_day(__date, country_code) if busdate and country_code else __date


def get_date_pair(startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                if_null: Optional[Unit]=None, tzinfo=None, busdate: Optional[Logic]=False, country_code=str()) -> Tuple[dt.date,dt.date]:
    context = dict(tzinfo=tzinfo, country_code=country_code)
    startDate = get_date(startDate, if_null=get_scala(if_null, 0), busdate=get_scala(busdate, 0), **context)
    endDate = get_date(endDate, if_null=get_scala(if_null, 1), busdate=get_scala(busdate, 1), **context)
    __min = min(startDate, endDate) if startDate and endDate else startDate
    __max = max(startDate, endDate) if startDate and endDate else endDate
    return __min, __max


def get_weekday(__date: Union[dt.date,dt.datetime], format: Literal["short","long"]="short", tzinfo=None) -> str:
    if tzinfo == KST:
        weekday = ["월", "화", "수", "목", "금", "토", "일"][__date.weekday()]
        return weekday + ("요일" if format == "long" else str())
    else: return __date.strftime("%A" if format == "long" else "%a")


def set_datetime(__datetime: dt.datetime, __format="%Y-%m-%d %H:%M:%S",
                __type: TypeHint=str, tsUnit: Literal["ms","s"]="ms") -> Union[str,int]:
    if not isinstance(__datetime, dt.datetime): return str()
    if __format: return __datetime.strftime(DATETIME_FORMAT.get(__format,__format))
    elif is_str_type(__type): return str(__datetime)
    elif is_timestamp_type(__type): return get_timestamp(__datetime, tsUnit=tsUnit)
    else: return


def set_date(__date: dt.date, __format="%Y-%m-%d", __type: TypeHint=str) -> Union[str,int]:
    if not isinstance(__date, dt.date): return str()
    if __format: return __date.strftime(DATETIME_FORMAT.get(__format,__format))
    elif is_str_type(__type): return str(__date)
    elif is_type(__type, INTEGER_TYPES+["ordinal"]): return __date.toordinal()
    else: return


###################################################################
############################# Holidays ############################
###################################################################

def get_calendar(country_code: str) -> Calendar:
    return registry.get_calendars()[country_code]()


def get_holidays(country_code: str) -> Dict:
    return country_holidays(country_code)


def is_holiday(__date: dt.date, country_code: str) -> bool:
    return (__date in get_holidays(country_code)) or (not get_calendar(country_code).is_working_day(__date))


def is_working_day(__date: dt.date, country_code: str) -> bool:
    return (__date not in get_holidays(country_code)) and get_calendar(country_code).is_working_day(__date)


def get_last_working_day(__date: dt.date, country_code: str, how: Literal["backward","forward"]="backward") -> dt.date:
    calendar, holidays = get_calendar(country_code), get_holidays(country_code)
    while (__date in holidays) or (not calendar.is_working_day(__date)):
        __date = __date + (dt.timedelta(days=1) * (-1 if how == "backward" else 1))
    return __date


###################################################################
######################### Pandas Frequency ########################
###################################################################

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
}

PANDAS_ANCHORED_FREQUENCY = {
    "W-SUN": "weekly frequency (Sundays). Same as 'W'",
    "W-MON": "weekly frequency (Mondays)",
    "W-TUE": "weekly frequency (Tuesdays)",
    "W-WED": "weekly frequency (Wednesdays)",
    "W-THU": "weekly frequency (Thursdays)",
    "W-FRI": "weekly frequency (Fridays)",
    "W-SAT": "weekly frequency (Saturdays)",
    "(B)Q(S)-DEC": "quarterly frequency, year ends in December. Same as 'Q'",
    "(B)Q(S)-JAN": "quarterly frequency, year ends in January",
    "(B)Q(S)-FEB": "quarterly frequency, year ends in February",
    "(B)Q(S)-MAR": "quarterly frequency, year ends in March",
    "(B)Q(S)-APR": "quarterly frequency, year ends in April",
    "(B)Q(S)-MAY": "quarterly frequency, year ends in May",
    "(B)Q(S)-JUN": "quarterly frequency, year ends in June",
    "(B)Q(S)-JUL": "quarterly frequency, year ends in July",
    "(B)Q(S)-AUG": "quarterly frequency, year ends in August",
    "(B)Q(S)-SEP": "quarterly frequency, year ends in September",
    "(B)Q(S)-OCT": "quarterly frequency, year ends in October",
    "(B)Q(S)-NOV": "quarterly frequency, year ends in November",
    "(B)A(S)-DEC": "annual frequency, anchored end of December. Same as 'A'",
    "(B)A(S)-JAN": "annual frequency, anchored end of January",
    "(B)A(S)-FEB": "annual frequency, anchored end of February",
    "(B)A(S)-MAR": "annual frequency, anchored end of March",
    "(B)A(S)-APR": "annual frequency, anchored end of April",
    "(B)A(S)-MAY": "annual frequency, anchored end of May",
    "(B)A(S)-JUN": "annual frequency, anchored end of June",
    "(B)A(S)-JUL": "annual frequency, anchored end of July",
    "(B)A(S)-AUG": "annual frequency, anchored end of August",
    "(B)A(S)-SEP": "annual frequency, anchored end of September",
    "(B)A(S)-OCT": "annual frequency, anchored end of October",
    "(B)A(S)-NOV": "annual frequency, anchored end of November",
}

_map_freq_pattern = lambda freq: re.sub(r"\((\w+)\)", r"[\1]{,1}", freq)


def flip_pandas_frequency(freq: str, sep='') -> str:
    if re.search(r"(M|Q|Y|A)S"+sep, freq):
        return freq
    elif re.search(r"(M|Q|Y|A)E"+sep, freq):
        return re.sub(r"(M|Q|Y|A)E"+sep, r'\1'+sep, freq)
    else: return re.sub(r"(M|Q|Y|A)"+sep, r'\1S'+sep, freq)


def map_pandas_frequency(freq: str, repl: Optional[str]=None, upper=False, flip=False) -> str:
    if isinstance(repl, str): freq = re.sub(r"\d+", repl, freq).strip()
    if upper and not isin(freq, include=("min","ms","us"), how="any"): freq = freq.upper()
    if flip: freq = flip_pandas_frequency(freq, sep=('-' if '-' in freq else ''))
    return freq


def _is_pandas_str_frequency(freq: str, upper=False, flip=False) -> bool:
    context = dict(upper=upper, flip=flip)
    return (
        (map_pandas_frequency(freq, repl='', **context) in PANDAS_FREQUENCY) or
        (freq.upper() in PANDAS_ANCHORED_FREQUENCY) or
        all([__i in PANDAS_FREQUENCY for __i in map_pandas_frequency(freq, repl=' ', **context).split(' ')]) or
        (freq.upper() in map(_map_freq_pattern, PANDAS_ANCHORED_FREQUENCY.keys())))


def is_pandas_frequency(freq: Timedelta, upper=False, flip=False) -> bool:
    if isinstance(freq, dt.timedelta): return True
    elif freq and isinstance(freq, str):
        return _is_pandas_str_frequency(freq, upper, flip)
    else: return False


def is_daily_frequency(freq: Timedelta) -> bool:
    if isinstance(freq, dt.timedelta): return (freq.days == 1) and (freq.seconds == 0)
    elif isinstance(freq, str): return to_offset(freq).freqstr == "D"
    else: return False


def _validate_pandas_str_frequency(freq: str, upper=True, flip=True) -> str:
    freq = map_pandas_frequency(freq, upper=upper, flip=flip)
    if is_pandas_frequency(freq): return freq
    else: raise ValueError(INVALID_INTERVAL_MSG(freq))


def validate_pandas_frequency(freq: Timedelta, upper=True, flip=True) -> Timedelta:
    if isinstance(freq, str):
        return _validate_pandas_str_frequency(freq, upper=upper, flip=flip)
    elif isinstance(freq, dt.timedelta): return freq
    elif isinstance(freq, int): return dt.timedelta(days=freq)
    else: raise ValueError(INVALID_INTERVAL_MSG(freq))


###################################################################
######################## Pandas Date Range ########################
###################################################################

def trunc_date(__date: dt.date, interval: Timedelta="D", how: Literal["backward","forward"]="backward") -> dt.date:
    if is_daily_frequency(interval) or not isinstance(__date, dt.date): return __date
    offset = to_offset(interval)
    if how == "backward": return offset.rollback(__date).date()
    delta = offset.rollforward(__date).date()
    return offset.rollforward(__date+dt.timedelta(days=1)).date() if delta == __date else delta


def _pair_date_range(date_range: Sequence[dt.date], interval: Timedelta="D") -> Sequence[Union[dt.date,dt.date]]:
    if is_daily_frequency(interval) or not date_range:
        return [(date, date) for date in date_range]
    return [(start, (end-dt.timedelta(days=1))) for start, end in zip(date_range[:-1],date_range[1:])]


def get_date_range(startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                    periods: Optional[int]=None, interval: Timedelta="D", upper=True, flip=True,
                    tzinfo: Optional[Timezone]=None, paired=False) -> List[Union[dt.date,Tuple[dt.date,dt.date]]]:
    if sum(map((lambda x: x is not None), (startDate, endDate, periods, interval))) < 3:
        raise ValueError(DATE_RANGE_MSG)
    interval = validate_pandas_frequency(interval, upper=upper, flip=flip)
    startDate, endDate = trunc_date(get_date(startDate), interval, how="backward"), get_date(endDate, if_null=None)
    date_range = [date.date() for date in pd.date_range(startDate, endDate, periods, interval, tzinfo)]
    if (not isinstance(endDate, dt.date)) and isinstance(periods, int):
        endDate = pd.date_range(startDate, endDate, periods+1, interval, tzinfo)[-1].date() - dt.timedelta(days=1)
    if date_range and (not is_daily_frequency(interval)):
        date_range.append(endDate + dt.timedelta(days=int(paired)))
    return _pair_date_range(date_range, interval) if paired else date_range
