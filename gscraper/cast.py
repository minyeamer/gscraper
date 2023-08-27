from .types import _type, TypeHint, CastError
from .types import is_float_type, is_int_type
from .types import is_datetime_type, is_time_type, is_timestamp_type, is_date_type
from .types import is_str_type, is_list_type, is_tuple_type
from .types import PatternStr, MatchFunction
from .types import DateFormat, DateNumeric, Timestamp

from typing import Any, Callable, List, Optional, Set, Tuple, Union
from dateutil.parser import parse as dateparse
from pytz import timezone, BaseTzInfo
import datetime as dt
import re


###################################################################
########################### Numeric Type ##########################
###################################################################

def isfloat(__object, **kwargs) -> bool:
    try: return isinstance(float(__object), float)
    except CastError: return False


def cast_float(__object, default=0., strict=False, trunc=None, **kwargs) -> float:
    try:
        __object = float(__object) if strict else float(re.sub("[^\d.-]",'',str(__object)))
        return round(__object, trunc) if isinstance(trunc, int) else __object
    except CastError: return default


def cast_float2(__object, default=0., strict=False, **kwargs) -> float:
    return cast_float(__object, default, strict, trunc=2, **kwargs)


def cast_int(__object, default=0, strict=False, **kwargs) -> int:
    try:
        return int(float(__object)) if strict else int(cast_float(__object, default=None))
    except CastError: return default


def cast_numeric(__object, __type: TypeHint="auto",
                default=0, strict=False, trunc=None, **kwargs) -> Union[float,int]:
    if __type == "auto": pass
    elif is_int_type(__type): return cast_int(__object, default=default, strict=strict)
    elif is_float_type(__type): return cast_float(__object, default=default, strict=strict, trunc=trunc)
    else: return
    __object = cast_float(__object, default=default, strict=strict, trunc=trunc)
    if isinstance(__object, float):
        return __object if __object % 1. else int(__object)


###################################################################
########################## Datetime Type ##########################
###################################################################

TS_SECONDS = 10
TS_MILLISECONDS = 13


def get_timezone(tzinfo=None, **kwargs) -> BaseTzInfo:
    if not tzinfo: return None
    try: return timezone(tzinfo)
    except: return


def set_timezone(__datetime: dt.datetime, tzinfo=None, astimezone=None, droptz=False, **kwargs) -> dt.datetime:
    tzinfo = get_timezone(tzinfo)
    if tzinfo:
        __datetime = __datetime.astimezone(tzinfo) if __datetime.tzinfo else tzinfo.localize(__datetime)
    if astimezone:
        __datetime = __datetime.astimezone(get_timezone(astimezone))
    return __datetime.replace(tzinfo=None) if droptz else __datetime


def get_ts_unit(__timestamp: Timestamp, **kwargs) -> str:
    __timestamp = cast_int(__timestamp, strict=True)
    if len(str(__timestamp)) == TS_SECONDS: return "s"
    elif len(str(__timestamp)) == TS_MILLISECONDS: return "ms"


def from_timestamp(__timestamp: Union[Timestamp,str], default=None, tzinfo=None, astimezone=None, **kwargs) -> dt.datetime:
    try:
        if isinstance(__timestamp, str): __timestamp = cast_numeric(__timestamp, default=None, strict=True)
        __timestamp = __timestamp/1000 if get_ts_unit(__timestamp) == "ms" else __timestamp
        return set_timezone(dt.datetime.fromtimestamp(__timestamp), tzinfo, astimezone)
    except CastError: return default


def cast_datetime(__object: DateFormat, default=None, tzinfo=None, astimezone=None, droptz=False, **kwargs) -> dt.datetime:
    try:
        if isinstance(__object, dt.datetime): __datetime = __object
        elif isinstance(__object, dt.date): __datetime = dt.datetime(*__object.timetuple()[:6])
        elif get_ts_unit(__object): return from_timestamp(**locals())
        else: __datetime = dateparse(__object, yearfirst=True)
        return set_timezone(__datetime, tzinfo, astimezone, droptz)
    except CastError: return default


def cast_time(__object: DateFormat, default=None, tzinfo=None, astimezone=None, **kwargs) -> dt.time:
    __datetime = cast_datetime(**locals())
    if isinstance(__datetime, dt.datetime): return __datetime.time()


def cast_timestamp(__object: DateFormat, default=None, tzinfo=None, astimezone=None, tsUnit="ms", **kwargs) -> int:
    __datetime = cast_datetime(**locals())
    if isinstance(__datetime, dt.datetime):
        return int(__datetime.timestamp()*(1000 if tsUnit == "ms" else 1))


def cast_date(__object: DateFormat, default=None, from_ordinal=False, **kwargs) -> dt.date:
    try:
        if from_ordinal: return dt.date.fromordinal(cast_int(__object, strict=True))
        elif isinstance(__object, dt.datetime): return __object.date()
        elif isinstance(__object, dt.date): return __object
        else: return dateparse(__object, yearfirst=True).date()
    except CastError: return default


def to_datetime(__object: DateFormat, __type: TypeHint="auto", default=None,
                tzinfo=None, astimezone=None, **kwargs) -> DateNumeric:
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
############################ Array Type ###########################
###################################################################

def cast_str(__object, default=str(), strict=False,
            match: Optional[Union[PatternStr,MatchFunction]]=None, **kwargs) -> str:
    if match:
        is_pattern_type = lambda __object: re.search(str(match), str(__object))
        if not (match(__object) if isinstance(match, Callable) else is_pattern_type(__object)): return default
    return default if ((__object != None) if strict else (not __object)) else default


def cast_id(__object, default=str(), **kwargs) -> str:
    return cast_str(cast_int(__object, default=__object, strict=True), default, match="^((?!nan).)*$")


def cast_list(__object, empty=True, iter_type: _type=(List,Set,Tuple), **kwargs) -> Union[List,List[str]]:
    if isinstance(__object, List): return __object
    elif isinstance(__object, iter_type): return list(__object)
    elif (__object != None if empty else bool(__object)): return [__object]
    else: return list()


def cast_tuple(__object, empty=True, iter_type: _type=(List,Set,Tuple), **kawrgs) -> Union[Tuple,Tuple[str]]:
    if isinstance(__object, Tuple): return __object
    elif isinstance(__object, iter_type): return tuple(__object)
    elif (__object != None if empty else bool(__object)): return (__object,)
    else: return tuple()


###################################################################
############################ Multitype ############################
###################################################################

def cast_object(__object, __type: TypeHint, default=None, **kwargs) -> Any:
    if is_list_type(__type): return cast_list(__object, **kwargs)
    elif is_tuple_type(__type): return cast_tuple(__object, **kwargs)
    elif is_str_type(__type): return cast_str(__object, default=default, **kwargs)
    elif is_int_type(__type): return cast_int(__object, default=default, **kwargs)
    elif is_float_type(__type): return cast_float(__object, default=default, **kwargs)
    elif is_datetime_type(__type): return cast_datetime(__object, default=default, **kwargs)
    elif is_time_type(__type): return cast_time(__object, default=default, **kwargs)
    elif is_timestamp_type(__type): return cast_timestamp(__object, default=default, **kwargs)
    elif is_date_type(__type): return cast_date(__object, default=default, **kwargs)
    else: return default
