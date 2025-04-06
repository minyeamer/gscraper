from gscraper.base.types import _TYPE, TypeHint, Timezone, CastError
from gscraper.base.types import get_type, is_bool_type, is_float_type, is_int_type
from gscraper.base.types import is_datetime_type, is_time_type, is_timestamp_type, is_date_type
from gscraper.base.types import is_str_type, is_list_type, is_tuple_type, is_set_type
from gscraper.base.types import RegexFormat, MatchFunction
from gscraper.base.types import DateFormat, DateNumeric, Timestamp

from gscraper.utils import notna, exists

from typing import Any, Callable, List, Literal, Optional, Set, Tuple, Union
from dateutil.parser import parse as dateparse
from pytz import timezone, BaseTzInfo, UnknownTimeZoneError
import datetime as dt
import re


###################################################################
########################### Numeric Type ##########################
###################################################################

def isfloat(__object) -> bool:
    try: return isinstance(float(__object), float)
    except CastError: return False


def cast_float(__object, default=0., clean=True, trunc: Optional[int]=None) -> float:
    try:
        __object = float(re.sub(r"[^\d.-]",'',str(__object))) if clean else float(__object)
        return round(__object, trunc) if isinstance(trunc, int) else __object
    except CastError: return default


def cast_float2(__object, default=0., clean=True, trunc=2) -> float:
    return cast_float(__object, default, clean, trunc)


def cast_int(__object, default=0, clean=True) -> int:
    try: return int(float(re.sub(r"[^\d.-]",'',str(__object)))) if clean else int(float(__object))
    except CastError: return default


def cast_int1(__object, default=1, clean=True) -> int:
    return cast_int(__object, default, clean)


def cast_numeric(__object, __type: TypeHint="auto", default=0, clean=True,
                trunc: Optional[int]=None, **kwargs) -> Union[float,int]:
    if __type == "auto":
        __object = cast_float(__object, default=default, clean=clean, trunc=trunc)
        if isinstance(__object, float): return __object if __object % 1. else int(__object)
        else: return __object
    elif is_int_type(__type): return cast_int(__object, default=default, clean=clean)
    elif is_float_type(__type): return cast_float(__object, default=default, clean=clean, trunc=trunc)
    else: return default


###################################################################
########################## Datetime Type ##########################
###################################################################

TS_SECONDS = 10
TS_MILLISECONDS = 13


def get_timezone(tzinfo: Optional[Timezone]=None) -> BaseTzInfo:
    if not tzinfo: return None
    try: return timezone(tzinfo)
    except UnknownTimeZoneError: return


def set_timezone(__datetime: dt.datetime, tzinfo: Optional[Timezone]=None,
                astimezone: Optional[Timezone]=None, droptz=False) -> dt.datetime:
    tzinfo = get_timezone(tzinfo)
    if tzinfo:
        __datetime = __datetime.astimezone(tzinfo) if __datetime.tzinfo else tzinfo.localize(__datetime)
    if astimezone:
        __datetime = __datetime.astimezone(get_timezone(astimezone))
    return __datetime.replace(tzinfo=None) if droptz else __datetime


def get_ts_unit(__timestamp: Timestamp) -> str:
    __timestamp = cast_int(__timestamp, clean=False)
    if len(str(__timestamp)) == TS_SECONDS: return "s"
    elif len(str(__timestamp)) == TS_MILLISECONDS: return "ms"


def from_timestamp(__timestamp: Union[Timestamp,str], default=None,
                    tzinfo: Optional[Timezone]=None, astimezone: Optional[Timezone]=None) -> dt.datetime:
    try:
        if isinstance(__timestamp, str): __timestamp = cast_numeric(__timestamp, default=None, clean=False)
        __timestamp = __timestamp/1000 if get_ts_unit(__timestamp) == "ms" else __timestamp
        return set_timezone(dt.datetime.fromtimestamp(__timestamp), tzinfo, astimezone)
    except CastError: return default


def cast_datetime(__object: DateFormat, default=None, tzinfo: Optional[Timezone]=None,
                    astimezone: Optional[Timezone]=None, droptz=False) -> dt.datetime:
    try:
        if isinstance(__object, dt.datetime): __datetime = __object
        elif isinstance(__object, dt.date): __datetime = dt.datetime(*__object.timetuple()[:6])
        elif get_ts_unit(__object): return from_timestamp(__object, default, tzinfo, astimezone)
        elif isinstance(__object, str): __datetime = dateparse(__object, yearfirst=True)
        else: return default
        return set_timezone(__datetime, tzinfo, astimezone, droptz)
    except CastError: return default


def cast_time(__object: DateFormat, default=None, tzinfo: Optional[Timezone]=None,
                astimezone: Optional[Timezone]=None) -> dt.time:
    __datetime = cast_datetime(__object, default, tzinfo, astimezone, droptz=False)
    if isinstance(__datetime, dt.datetime): return __datetime.time()


def cast_timestamp(__object: DateFormat, default=None, tzinfo: Optional[Timezone]=None,
                    astimezone: Optional[Timezone]=None, tsUnit: Literal["ms","s"]="ms") -> int:
    __datetime = cast_datetime(__object, default, tzinfo, astimezone, droptz=False)
    if isinstance(__datetime, dt.datetime):
        return int(__datetime.timestamp()*(1000 if tsUnit == "ms" else 1))


def cast_date(__object: DateFormat, default=None, from_ordinal=False) -> dt.date:
    try:
        if from_ordinal: return dt.date.fromordinal(cast_int(__object, clean=False))
        elif isinstance(__object, dt.datetime): return __object.date()
        elif isinstance(__object, dt.date): return __object
        elif isinstance(__object, str): return dateparse(__object, yearfirst=True).date()
        else: return default
    except CastError: return default


def to_datetime(__object: DateFormat, __type: TypeHint="auto", default=None,
                tzinfo: Optional[Timezone]=None, astimezone: Optional[Timezone]=None,
                droptz=False, tsUnit: Literal["ms","s"]="ms", from_ordinal=False, **kwargs) -> DateNumeric:
    if __type == "auto": pass
    elif is_datetime_type(__type): return cast_datetime(__object, tzinfo=tzinfo, astimezone=astimezone, droptz=droptz)
    elif is_time_type(__type): return cast_time(__object, tzinfo=tzinfo, astimezone=astimezone)
    elif is_timestamp_type(__type): return cast_timestamp(__object, tzinfo=tzinfo, astimezone=astimezone, tsUnit=tsUnit)
    elif is_date_type(__type): return cast_date(__object, from_ordinal=from_ordinal)
    else: return default
    __datetime = cast_datetime(__object, tzinfo=tzinfo, astimezone=astimezone)
    if isinstance(__datetime, dt.datetime):
        return __datetime.date() if __datetime.time() == dt.time(0,0) else __datetime


###################################################################
########################## Datetime Forat #########################
###################################################################

COMMON_DATE_PATTERN = r"\d{4}-\d{2}-\d{2}"
COMMON_DATETIME_PATTERN = r"^(?:\d{4}-\d{2}-\d{2}(?: \d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:[+-]\d{4})?)?|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:[+-]\d{4})?)$"


def is_datetime_format(__object: DateFormat, parse=False) -> bool:
    if isinstance(__object, str) and re.fullmatch(COMMON_DATETIME_PATTERN, __object):
        try: return bool(dateparse(__object)) if parse else True
        except: return False
    else: return False


def get_datetime_format(__object: DateFormat, parse=False) -> str:
    if is_datetime_format(__object, parse=parse):
        return "date" if re.fullmatch(COMMON_DATE_PATTERN, __object) else "datetime"


def cast_datetime_format(__object: DateFormat, default=None, parse=False) -> DateNumeric:
    format = get_datetime_format(__object, parse=parse)
    if format == "date": return cast_date(__object, default=default)
    elif format == "datetime": return cast_datetime(__object, default=default)
    else: return default


###################################################################
############################ Array Type ###########################
###################################################################

def cast_str(__object, default=str(), drop_empty=False, strip=False,
            match: Optional[Union[RegexFormat,MatchFunction]]=None) -> str:
    if match:
        if isinstance(match, Callable) and match(__object): pass
        elif isinstance(match, re.Pattern) and match.search(str(__object)): pass
        elif isinstance(match, str) and re.search(match, str(__object)): pass
        else: return default
    elif (exists(__object) if drop_empty else notna(__object)):
        return str(__object).strip() if strip else str(__object)
    else: return default


def cast_id(__object, default=None) -> str:
    return cast_str(cast_int(__object, default=None), default)


def cast_list(__object, drop_empty=False, iter_type: _TYPE=(List,Set,Tuple)) -> List:
    if isinstance(__object, List): return [__e for __e in __object if exists(__e)] if drop_empty else __object
    elif isinstance(__object, iter_type): return [__e for __e in __object if exists(__e)] if drop_empty else list(__object)
    elif (exists(__object) if drop_empty else notna(__object)): return [__object]
    else: return list()


def cast_tuple(__object, drop_empty=False, iter_type: _TYPE=(List,Set,Tuple)) -> Tuple:
    if isinstance(__object, Tuple): return tuple(__e for __e in __object if exists(__e)) if drop_empty else __object
    elif isinstance(__object, iter_type): return tuple(__e for __e in __object if exists(__e)) if drop_empty else tuple(__object)
    elif (exists(__object) if drop_empty else notna(__object)): return (__object,)
    else: return tuple()


def cast_set(__object, drop_empty=False, iter_type: _TYPE=(List,Set,Tuple)) -> Set:
    if isinstance(__object, Set): return {__e for __e in __object if exists(__e)} if drop_empty else __object
    elif isinstance(__object, iter_type): return {__e for __e in __object if exists(__e)} if drop_empty else set(__object)
    elif (exists(__object) if drop_empty else notna(__object)): return {__object}
    else: return set()


###################################################################
############################ Multitype ############################
###################################################################

def cast_object(__object, __type: TypeHint, default=None, clean=True, drop_empty=False, strip=False,
                match: Optional[Union[RegexFormat,MatchFunction]]=None, trunc: Optional[int]=None,
                tzinfo: Optional[Timezone]=None, astimezone: Optional[Timezone]=None,
                droptz=False, tsUnit: Literal["ms","s"]="ms", from_ordinal=False,
                iter_type: _TYPE=(List,Set,Tuple), **kwargs) -> Any:
    if is_str_type(__type): return cast_str(__object, default=default, drop_empty=drop_empty, strip=strip, match=match)
    elif is_int_type(__type): return cast_int(__object, default=default, clean=clean)
    elif is_float_type(__type): return cast_float(__object, default=default, clean=clean, trunc=trunc)
    elif is_bool_type(__type): return exists(__object) if drop_empty else notna(__object)
    elif is_datetime_type(__type): return cast_datetime(__object, default=default, tzinfo=tzinfo, astimezone=astimezone, droptz=droptz)
    elif is_date_type(__type): return cast_date(__object, default=default, from_ordinal=from_ordinal)
    elif is_list_type(__type): return cast_list(__object, drop_empty=drop_empty, iter_type=iter_type)
    elif is_tuple_type(__type): return cast_tuple(__object, drop_empty=drop_empty, iter_type=iter_type)
    elif is_set_type(__type): return cast_set(__object, drop_empty=drop_empty, iter_type=iter_type)
    elif is_time_type(__type): return cast_time(__object, default=default, tzinfo=tzinfo, astimezone=astimezone)
    elif is_timestamp_type(__type): return cast_timestamp(__object, default=default, tzinfo=tzinfo, astimezone=astimezone, tsUnit=tsUnit)
    elif isinstance(__object, get_type(__type)): return __object
    else: return get_type(__type)(default)
