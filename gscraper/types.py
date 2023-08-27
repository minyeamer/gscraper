from typing import Callable, Sequence, Tuple, Type, Union, TypeVar, Hashable
from typing import Any, Dict, List, Set, get_type_hints, get_origin, get_args

from datetime import datetime, date, time, timedelta
from pandas import DataFrame
from pytz import BaseTzInfo


_KT = TypeVar("_KT", Hashable, Sequence[Hashable])
_VT = TypeVar("_VT", Any, Sequence[Any])
_bool = TypeVar("_bool", bool, Sequence[bool])
_type = TypeVar("_type", Type, Sequence[Type])

Comparable = TypeVar("Comparable")
ClassInstance = TypeVar("ClassInstance")

Arugments = Tuple[Any]
ArgsMapper = Callable[[Arugments],Arugments]

Context = Dict[Any,Any]
ContextMapper = Callable[[Context],Context]

ObjectSequence = Union[List, Tuple]
OBJECT_SEQUENCE = (List, Tuple)

IndexedSequence = Union[List, Tuple]
INDEXED_SEQUENCE = (List, Tuple)

SequenceSet = Union[ObjectSequence, Set]
SEQUENCE_SET = (List, Set, Tuple)

NestedSequence = Sequence[Sequence]
NestedDict = Dict[_KT,Dict]
RenameDict = Dict[str,str]
Records = List[Dict]

NestedData = Union[NestedSequence, NestedDict]
TabularData = Union[Records, DataFrame]
MappingData = Union[Records, DataFrame, Dict]
Data = Union[Records, DataFrame, Dict, List, NestedSequence, NestedDict]

JsonData = Union[Dict, List]
RedirectData = Union[Records, Dict[str,Records], JsonData]

LogMessage = Dict[str,str]

Index = Union[int, Sequence[int]]
IndexLabel = Union[Hashable, Sequence[Hashable]]

Column = Union[str, Sequence[str]]
Keyword = Union[str, Sequence[str]]
Shape = Union[int, Sequence[int]]
Unit = Union[int, Sequence[int]]

Timestamp = Union[float, int]
DateNumeric = Union[datetime, date, time, float, int]
DateFormat = Union[datetime, date, time, float, int, str]

DateQuery = Dict[str,datetime]
Timedelta = Union[timedelta, str, int]
Timezone= Union[BaseTzInfo, str]

PatternStr = str
ApplyFunction = Union[Callable[[Any],Any], Sequence[Callable[[Any],Any]]]
MatchFunction = Union[Callable[[Any],bool], Sequence[Callable[[Any],bool]]]
BetweenRange = Union[Sequence[Tuple], Sequence[Dict]]

CastError = (ValueError, TypeError)


def init_origin(__object, default=None, **kawrgs) -> Any:
    try:
        __type = get_origin(__object)
        if __type == Union: return init_origin(get_args(__object)[0])
        elif __type: return __type()
        elif isinstance(__object, Type): return __object()
        elif __object is None: return default
        return init_origin(get_type_hints(__object).get("return") if isinstance(__object, Callable) else type(__object))
    except: return default


###################################################################
############################ Type Check ###########################
###################################################################

TypeHint = Union[Type, str]
TypeList = Sequence[TypeHint]

FLOAT_TYPES = [float, "float"]
INTEGER_TYPES = [int, "int", "integer"]

DATETIME_TYPES = [datetime, "datetime"]
TIME_TYPES = [time, "time"]
TIMESTAMP_TYPES = ["timestamp"]
DATE_TYPES = [date, "date"]

STRING_TYPES = [str, "str", "string"]
LIST_TYPES = [list, List, "list"]
TUPLE_TYPES = [tuple, Tuple, "tuple"]

DICT_TYPES = [Dict, dict, "dict"]
RECORDS_TYPES = [Records, "records"]
DATAFRAME_TYPES = [DataFrame, "dataframe", "df", "pandas", "pd"]

TYPE_LIST = {
    float: FLOAT_TYPES,
    int: INTEGER_TYPES,
    datetime: DATETIME_TYPES,
    time: TIME_TYPES,
    date: DATE_TYPES,
    str: STRING_TYPES,
    list: LIST_TYPES,
    tuple: TUPLE_TYPES,
    dict: DICT_TYPES,
    DataFrame: DATAFRAME_TYPES,
}


def is_type(__type: TypeHint, __types: TypeList) -> bool:
    return (__type.lower() if isinstance(__type, str) else __type) in __types

def is_float_type(__type: TypeHint) -> bool:
    return is_type(__type, FLOAT_TYPES)

def is_int_type(__type: TypeHint) -> bool:
    return is_type(__type, INTEGER_TYPES)

def is_datetime_type(__type: TypeHint) -> bool:
    return is_type(__type, DATETIME_TYPES)

def is_time_type(__type: TypeHint) -> bool:
    return is_type(__type, TIME_TYPES)

def is_timestamp_type(__type: TypeHint) -> bool:
    return is_type(__type, TIMESTAMP_TYPES)

def is_date_type(__type: TypeHint) -> bool:
    return is_type(__type, DATE_TYPES)

def is_str_type(__type: TypeHint) -> bool:
    return is_type(__type, STRING_TYPES)

def is_list_type(__type: TypeHint) -> bool:
    return is_type(__type, LIST_TYPES)

def is_tuple_type(__type: TypeHint) -> bool:
    return is_type(__type, TUPLE_TYPES)

def is_dict_type(__type: TypeHint) -> bool:
    return is_type(__type, DICT_TYPES)

def is_records_type(__type: TypeHint) -> bool:
    return is_type(__type, RECORDS_TYPES)

def is_dataframe_type(__type: TypeHint) -> bool:
    return is_type(__type, DATAFRAME_TYPES)


###################################################################
######################### Array Type Check ########################
###################################################################

def is_array(__object) -> bool:
    return isinstance(__object, OBJECT_SEQUENCE)

def isin_instance(__object: Sequence, __type: Type, empty=False) -> bool:
    return (empty or __object) and any(map(lambda x: isinstance(x, __type), __object))

def type_isin_instance(__object: Sequence, __type: TypeHint, empty=False) -> bool:
    return (empty or __object) and any(map(lambda x: is_type(__type, TYPE_LIST[type(x)]), __object))

def allin_instance(__object: Sequence, __type: Type, empty=False) -> bool:
    return (empty or __object) and all(map(lambda x: isinstance(x, __type), __object))

def type_allin_instance(__object: Sequence, __type: TypeHint, empty=False) -> bool:
    return (empty or __object) and all(map(lambda x: is_type(__type, TYPE_LIST[type(x)]), __object))

def is_nested_in(__object, __type: Type, how="any", empty=False) -> bool:
    return isinstance(__object, OBJECT_SEQUENCE) and (
        isin_instance(__object, __type, empty) if how == "any" else allin_instance(__object, __type, empty))

def is_nested_map(__object, empty=False) -> bool:
    return isinstance(__object, Dict) and (empty or (bool(__object) and isin_instance(__object.values(), Dict)))

def is_float_array(__object, how="any", empty=False) -> bool:
    return is_nested_in(__object, float, how=how, empty=empty)

def is_int_array(__object, how="any", empty=False) -> bool:
    return is_nested_in(__object, int, how=how, empty=empty)

def is_datetime_array(__object, how="any", empty=False) -> bool:
    return is_nested_in(__object, datetime, how=how, empty=empty)

def is_time_array(__object, how="any", empty=False) -> bool:
    return is_nested_in(__object, time, how=how, empty=empty)

def is_date_array(__object, how="any", empty=False) -> bool:
    return is_nested_in(__object, date, how=how, empty=empty)

def is_str_array(__object, how="any", empty=False) -> bool:
    return is_nested_in(__object, str, how=how, empty=empty)

def is_2darray(__object, how="any", empty=False) -> bool:
    return is_nested_in(__object, OBJECT_SEQUENCE, how=how, empty=empty)

def is_records(__object, how="any", empty=True) -> bool:
    return is_nested_in(__object, Dict, how=how, empty=empty)

def is_dfarray(__object, how="any", empty=True) -> bool:
    return is_nested_in(__object, DataFrame, how=how, empty=empty)

def is_df(__object) -> bool:
    return isinstance(__object, DataFrame)
