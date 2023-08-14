from .cast import cast_int, cast_datetime, cast_date
from .map import re_get

from typing import Optional, Union
from pytz import timezone
import datetime as dt
import pandas as pd
import re

KST = "Asia/Seoul"
NOW = dt.datetime.now(timezone(KST)).replace(microsecond=0, tzinfo=None)
TODAY = NOW.replace(hour=0, minute=0, second=0, microsecond=0)
DATETYPE = {"datetime":dt.datetime, "date":dt.date, "str":str, "timestamp":int, "ordinal":int}
DATEPART = ["SECOND", "MINUTE", "HOUR", "DAY", "MONTH", "YEAR"]
WEEKDAY = ["월", "화", "수", "목", "금", "토", "일"]

DAET_FORMAT = "%Y-%m-%d"
DAETTIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TIMEZONE_FORMAT = "%Y-%m-%d %H:%M:%S.%f%z"
JS_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

strptimekr = lambda date_string, default=None: cast_datetime(date_string, default, tzinfo=KST)
strpdatekr = lambda date_string, default=None: get_date(strptimekr(date_string), default)
dateptime = lambda __date: dt.datetime(*__date.timetuple()[:6]) if isinstance(__date, dt.date) else None

date_range = lambda startDate, endDate: [str(date.date()) for date in pd.date_range(startDate, endDate)]


def now(format=str(), days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0,
        hours=0, weeks=0, datetimePart=str(), tzinfo=KST, droptz=True, dropms=True,
        **kwargs) -> Union[dt.datetime,str]:
    delta = dt.timedelta(days, seconds, microseconds, milliseconds, minutes, hours, weeks)
    __datetime = dt.datetime.now(timezone(tzinfo)) - delta
    if droptz: __datetime = __datetime.replace(tzinfo=None)
    if dropms: __datetime = __datetime.replace(microsecond=0)
    if datetimePart: __datetime = trunc_datetime(__datetime, datetimePart)
    return __datetime.strftime(format) if format else __datetime


def today(format=str(), days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0,
            hours=0, weeks=0, tzinfo=KST, droptz=True, dropms=True, **kwargs) -> Union[dt.datetime,str]:
    __date = now(str(), days, seconds, microseconds, milliseconds, minutes, hours, weeks, tzinfo, droptz, dropms)
    __date = __date.replace(hour=0, minute=0, second=0)
    return __date.strftime(format) if format else __date


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


def get_datetime(__datetime: Union[dt.datetime,dt.date,str,int], default=0, time=True, **kwargs) -> dt.datetime:
    if isinstance(__datetime, dt.datetime): pass
    elif isinstance(__datetime, dt.date): __datetime = dt.datetime(*__datetime.timetuple()[:6])
    elif cast_datetime(__datetime): __datetime = cast_datetime(__datetime)
    elif str(__datetime).isdigit(): __datetime = now(days=int(__datetime))
    elif type(default) in [dt.datetime,dt.date,str,int]: return get_datetime(default, default=None, time=time)
    else: return default
    return __datetime if time else __datetime.replace(hour=0, minute=0, second=0, microsecond=0)


def get_timestamp(__datetime: Union[dt.datetime,dt.date,str,float,int], default=0, time=True, tsUnit="ms",
                    **kwargs) -> Union[float,int]:
    __datetime = get_datetime(__datetime, default=default, time=time)
    __timestamp = __datetime.timestamp() if __datetime else None
    return int(__timestamp*1000) if __timestamp and tsUnit == "ms" else __timestamp


def get_date(__date: Union[dt.datetime,dt.date,str,int], default=0, **kwargs) -> dt.date:
    if isinstance(__date, dt.datetime): return __date.date()
    elif isinstance(__date, dt.date): return __date
    elif cast_date(__date): return cast_date(__date)
    elif str(__date).replace('-','').isdigit(): return now(days=int(__date)).date()
    elif type(default) in [dt.datetime,dt.date,str,int]: return get_date(default, default=None)
    else: return default


def set_datetime(__datetime: dt.datetime, __type: Optional[Union[type,str]]=str,
                __format="%Y-%m-%d %H:%M:%S", **kwargs) -> Union[str,int]:
    __type = __type if isinstance(__type, type) else DATETYPE.get(__type)
    if not isinstance(__type, type): return None
    elif isinstance(__datetime, __type): return __datetime
    elif __type == str and __format: return __datetime.strftime(__format)
    elif __type == float: return int(__datetime.timestamp()*1000)
    elif __type == int: return __datetime.toordinal()
    else: return __type(**kwargs)


def set_date(__date: dt.date, __type: Optional[Union[type,str]]=str,
            __format="%Y-%m-%d", **kwargs) -> Union[str,int]:
    __type = __type if isinstance(__type, type) else DATETYPE.get(__type)
    if not isinstance(__type, type): return None
    elif isinstance(__date, __type): return __date
    elif __type == str: return __date.strftime(__format)
    elif __type == int: return __date.toordinal()
    else: return __type(**kwargs)


def relative_strptime(date_string: str, **kwargs) -> dt.datetime:
    str2int = lambda pattern, string: cast_int(re_get(pattern, string))
    if re.search("(\d+)초 전", date_string):
        return now(seconds=str2int("(\d+)초 전", date_string))
    elif re.search("(\d+)분 전", date_string):
        return now(minutes=str2int("(\d+)분 전", date_string))
    elif re.search("(\d+)시간 전", date_string):
        return now(hours=str2int("(\d+)시간 전", date_string))
    elif re.search("(\d+)일 전", date_string):
        return today(days=str2int("(\d+)일 전", date_string))
    elif "어제" in date_string:
        return today(days=1)
    else: return cast_datetime(date_string)


def strptime_weeknum(std_date: dt.date, **kwargs) -> str:
    cur_year, weeknum, _ = std_date.isocalendar()
    year, first_week, first_day = std_date.replace(day=1).isocalendar()
    if cur_year != year:
        return f"{cur_year}년{std_date.month}월{weeknum}주차"
    monthly_week = weeknum - first_week + int(first_day < 4)
    return f"{year}년{std_date.month}월{monthly_week}주차"
