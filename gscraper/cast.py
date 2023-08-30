from .types import _type, TypeHint, CastError
from .types import not_na, is_float_type, is_int_type
from .types import is_datetime_type, is_time_type, is_timestamp_type, is_date_type
from .types import is_str_type, is_list_type, is_tuple_type
from .types import RegexFormat, MatchFunction
from .types import DateFormat, DateNumeric, Timestamp

from typing import Any, Callable, List, Literal, Optional, Set, Tuple, Union
from dateutil.parser import parse as dateparse
from pytz import timezone, BaseTzInfo
import datetime as dt
import re


###################################################################
########################### Numeric Type ##########################
###################################################################

def isfloat(__object) -> bool:
    try: return isinstance(float(__object), float)
    except CastError: return False


def cast_float(__object, default=0., strict=False, trunc=None) -> float:
    try:
        __object = float(__object) if strict else float(re.sub("[^\d.-]",'',str(__object)))
        return round(__object, trunc) if isinstance(trunc, int) else __object
    except CastError: return default


def cast_float2(__object, default=0., strict=False) -> float:
    return cast_float(__object, default, strict, trunc=2)


def cast_int(__object, default=0, strict=False) -> int:
    try:
        return int(float(__object)) if strict else int(cast_float(__object, default=None))
    except CastError: return default


def cast_numeric(__object, __type: TypeHint="auto", default=0, strict=False, trunc=None) -> Union[float,int]:
    if __type == "auto": pass
    elif is_int_type(__type): return cast_int(__object, default=default, strict=strict)
    elif is_float_type(__type): return cast_float(__object, default=default, strict=strict, trunc=trunc)
    else: return default

    __object = cast_float(__object, default=default, strict=strict, trunc=trunc)
    if isinstance(__object, float):
        return __object if __object % 1. else int(__object)


###################################################################
########################## Datetime Type ##########################
###################################################################

TS_SECONDS = 10
TS_MILLISECONDS = 13


def get_timezone(tzinfo=None) -> BaseTzInfo:
    if not tzinfo: return None
    try: return timezone(tzinfo)
    except: return


def set_timezone(__datetime: dt.datetime, tzinfo=None, astimezone=None, droptz=False) -> dt.datetime:
    tzinfo = get_timezone(tzinfo)
    if tzinfo:
        __datetime = __datetime.astimezone(tzinfo) if __datetime.tzinfo else tzinfo.localize(__datetime)
    if astimezone:
        __datetime = __datetime.astimezone(get_timezone(astimezone))
    return __datetime.replace(tzinfo=None) if droptz else __datetime


def get_ts_unit(__timestamp: Timestamp) -> str:
    __timestamp = cast_int(__timestamp, strict=True)
    if len(str(__timestamp)) == TS_SECONDS: return "s"
    elif len(str(__timestamp)) == TS_MILLISECONDS: return "ms"


def from_timestamp(__timestamp: Union[Timestamp,str], default=None, tzinfo=None, astimezone=None) -> dt.datetime:
    try:
        if isinstance(__timestamp, str): __timestamp = cast_numeric(__timestamp, default=None, strict=True)
        __timestamp = __timestamp/1000 if get_ts_unit(__timestamp) == "ms" else __timestamp
        return set_timezone(dt.datetime.fromtimestamp(__timestamp), tzinfo, astimezone)
    except CastError: return default


def cast_datetime(__object: DateFormat, default=None, tzinfo=None, astimezone=None, droptz=False) -> dt.datetime:
    try:
        if isinstance(__object, dt.datetime): __datetime = __object
        elif isinstance(__object, dt.date): __datetime = dt.datetime(*__object.timetuple()[:6])
        elif get_ts_unit(__object): return from_timestamp(**locals())
        else: __datetime = dateparse(__object, yearfirst=True)
        return set_timezone(__datetime, tzinfo, astimezone, droptz)
    except CastError: return default


def cast_time(__object: DateFormat, default=None, tzinfo=None, astimezone=None) -> dt.time:
    __datetime = cast_datetime(**locals())
    if isinstance(__datetime, dt.datetime): return __datetime.time()


def cast_timestamp(__object: DateFormat, default=None, tzinfo=None, astimezone=None, tsUnit: Literal["ms","s"]="ms") -> int:
    __datetime = cast_datetime(**locals())
    if isinstance(__datetime, dt.datetime):
        return int(__datetime.timestamp()*(1000 if tsUnit == "ms" else 1))


def cast_date(__object: DateFormat, default=None, from_ordinal=False) -> dt.date:
    try:
        if from_ordinal: return dt.date.fromordinal(cast_int(__object, strict=True))
        elif isinstance(__object, dt.datetime): return __object.date()
        elif isinstance(__object, dt.date): return __object
        else: return dateparse(__object, yearfirst=True).date()
    except CastError: return default


def to_datetime(__object: DateFormat, __type: TypeHint="auto", default=None, tzinfo=None, astimezone=None) -> DateNumeric:
    if __type == "auto": pass
    elif is_datetime_type(__type): return cast_datetime(__object, tzinfo=tzinfo, astimezone=astimezone)
    elif is_time_type(__type): return cast_time(__object, tzinfo=tzinfo, astimezone=astimezone)
    elif is_timestamp_type(__type): return cast_timestamp(__object, tzinfo=tzinfo, astimezone=astimezone)
    elif is_date_type(__type): return cast_date(__object)
    else: return default
    __datetime = cast_datetime(__object, tzinfo=tzinfo, astimezone=astimezone)
    if isinstance(__datetime, dt.datetime):
        return __datetime.date() if __datetime.time() == dt.time(0,0) else __datetime


###################################################################
########################## Datetime Forat #########################
###################################################################

COMMON_DATE_PATTERN = r"\d{4}-\d{2}-\d{2}"
COMMON_DATETIME_PATTERN = r"^(?:\d{4}-\d{2}-\d{2}(?: \d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:[+-]\d{4})?)?|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:[+-]\d{4})?)$"


def is_datetime_format(__object: DateFormat, strict=False) -> bool:
    if isinstance(__object, str) and re.fullmatch(COMMON_DATETIME_PATTERN, __object):
        try: return bool(dateparse(__object)) if strict else True
        except: return False
    else: return False


def get_datetime_format(__object: DateFormat, strict=False) -> str:
    if is_datetime_format(__object, strict=strict):
        return "date" if re.fullmatch(COMMON_DATE_PATTERN, __object) else "datetime"


def cast_datetime_format(__object: DateFormat, default=None, strict=False) -> DateNumeric:
    format = get_datetime_format(__object, strict=strict)
    if format == "date": return cast_date(__object, default=default)
    elif format == "datetime": return cast_datetime(__object, default=default)
    else: return default


###################################################################
############################ Array Type ###########################
###################################################################

def cast_str(__object, default=str(), strict=True, match: Optional[Union[RegexFormat,MatchFunction]]=None) -> str:
    if match:
        is_pattern_type = lambda __object: re.search(str(match), str(__object))
        if not (match(__object) if isinstance(match, Callable) else is_pattern_type(__object)): return default
    return str(__object) if not_na(__object, strict=strict) else default


def cast_id(__object, default=str()) -> str:
    return cast_str(cast_int(__object, default=__object), default, match="^((?!nan).)*$")


def cast_list(__object, strict=True, iter_type: _type=(List,Set,Tuple)) -> Union[List,List[str]]:
    if isinstance(__object, List): return __object
    elif isinstance(__object, iter_type): return list(__object)
    elif not_na(__object, strict=strict): return [__object]
    else: return list()


def cast_tuple(__object, strict=True, iter_type: _type=(List,Set,Tuple)) -> Union[Tuple,Tuple[str]]:
    if isinstance(__object, Tuple): return __object
    elif isinstance(__object, iter_type): return tuple(__object)
    elif not_na(__object, strict=strict): return (__object,)
    else: return tuple()


###################################################################
############################ Multitype ############################
###################################################################

def cast_object(__object, __type: TypeHint, default=None) -> Any:
    if is_list_type(__type): return cast_list(__object)
    elif is_tuple_type(__type): return cast_tuple(__object)
    elif is_str_type(__type): return cast_str(__object, default=default)
    elif is_int_type(__type): return cast_int(__object, default=default)
    elif is_float_type(__type): return cast_float(__object, default=default)
    elif is_datetime_type(__type): return cast_datetime(__object, default=default)
    elif is_time_type(__type): return cast_time(__object, default=default)
    elif is_timestamp_type(__type): return cast_timestamp(__object, default=default)
    elif is_date_type(__type): return cast_date(__object, default=default)
    else: return default
