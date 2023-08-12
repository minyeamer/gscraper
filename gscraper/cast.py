from typing import Any, Iterable, Optional, Type, Union
from dateutil.parser import parse as dateparse
from pytz import timezone
import datetime as dt
import re


def cast_object(__object, type: Optional[Type], default=None, **kwargs) -> Any:
    if isinstance(__object, dict):
        return {key: cast_object(values, type) for key, values in __object.items()}
    elif isinstance(__object, list):
        return [cast_object(value, type) for value in __object]
    elif type == int:
        return cast_int(__object, default=default)
    elif type == float:
        return cast_float(__object, default=default)
    else:
        return type(__object) if __object else default


def cast_list(__object: Iterable, iterable=[list,set,tuple]) -> Iterable:
    return list(__object) if type(__object) in iterable else [__object]


def cast_str(__object, default: Optional[str]=str(), match=str(), **kwargs) -> str:
    if not __object or (match and not re.search(match, str(__object))):
        return default
    else:
        return str(__object)


def cast_float(__object, default: Optional[float]=0., strict=False, **kwargs) -> float:
    try:
        return float(__object) if strict else float(re.sub("[^\d.-]",'',str(__object)))
    except (ValueError, TypeError):
        return default


def cast_float2(__object, default: Optional[float]=0., strict=False, **kwargs) -> float:
    return round(cast_float(__object, default, strict, **kwargs), 2)


def cast_int(__object, default: Optional[int]=0, strict=False, **kwargs) -> int:
    try:
        return int(float(__object)) if strict else int(cast_float(__object, None))
    except (ValueError, TypeError):
        return default


def cast_id(__object, default: Optional[str]=str(), **kwargs) -> str:
    return cast_str(cast_int(__object, default=__object, strict=True), default, match="^((?!nan).)*$")


def isfloat(__object, **kwargs) -> bool:
    try:
        float(__object)
        return True
    except (ValueError, TypeError):
        return False


def cast_datetime(__date_string: Union[str,int], default: Optional[dt.datetime]=None, tzinfo=None, timestamp=False, **kwargs) -> dt.datetime:
    try:
        if not __date_string: return default
        elif not timestamp: __datetime = dateparse(__date_string, yearfirst=True)
        elif str(__date_string).isdigit(): __datetime = dt.datetime.fromtimestamp(int(__date_string)/1000)
        else: __datetime = dt.datetime.fromtimestamp(cast_float(__date_string))
        return __datetime.astimezone(timezone(tzinfo)).replace(tzinfo=None) if tzinfo else __datetime
    except (ValueError, TypeError):
        return default


def cast_timestamp(__date_string: Union[str,int], default: Optional[dt.datetime]=None, tzinfo=None, timestamp=True, **kwargs) -> dt.datetime:
    return cast_datetime(__date_string, default, tzinfo, timestamp=True, **kwargs)


def cast_date(__date_string: str, default: Optional[dt.date]=None, ordinal=False, **kwargs) -> dt.date:
    try:
        if not __date_string: return default
        elif ordinal: return dt.date.fromordinal(cast_int(__date_string))
        else: return dateparse(__date_string, yearfirst=True).date()
    except (ValueError, TypeError):
        return default
