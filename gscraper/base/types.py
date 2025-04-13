from typing import Callable, Hashable, Literal, Optional, Sequence, Tuple, Type, TypeVar, Union
from typing import Any, Dict, List, Set, get_type_hints, get_origin, get_args

from bs4.element import Tag
from datetime import datetime, date, time, timedelta
from numbers import Real
from pandas import DataFrame, Series
from pytz import BaseTzInfo

from inspect import Parameter
import inspect
import re


INVALID_TYPE_HINT_MSG = lambda x: f"'{x}' is not valid type hint."


###################################################################
########################## General Types ##########################
###################################################################

_KT = TypeVar("_KT", Sequence[Hashable], Hashable)
_VT = TypeVar("_VT", Sequence[Any], Any)
_PASS = TypeVar("_PASS")

_TYPE = TypeVar("_TYPE", Sequence[Type], Type)
_BOOL = TypeVar("_BOOL", Sequence[bool], bool)

Comparable = TypeVar("Comparable")
ClassInstance = TypeVar("ClassInstance")

Arguments = Tuple[Any,...]
ArgsMapper = Callable[[Arguments],Arguments]

Context = Dict[Any,Any]
ContextMapper = Callable[[Context],Context]

LogLevel = Union[str,int]

TypeHint = Union[Type, str]
TypeList = Sequence[TypeHint]

GENERAL_TYPES = {
    "_KT":_KT, "_VT":_VT, "_PASS":_PASS, "_TYPE":_TYPE, "_BOOL":_BOOL, "Comparable":Comparable,
    "ClassInstance":ClassInstance, "Arguments":Arguments, "ArgsMapper":ArgsMapper,
    "Context":Context, "ContextMapper":ContextMapper, "LogLevel":LogLevel, "TypeHint":TypeHint, "TypeList":TypeList
}


###################################################################
############################ Key Types ############################
###################################################################

Index = Union[Sequence[int], int]
IndexLabel = Union[Sequence[Hashable], Hashable]

Column = Union[Sequence[str], str]
Keyword = Union[Sequence[str], str]
Id = Union[Sequence[str], str]
Url = Union[Sequence[str], str]
Token = Union[Sequence[str], str]
EncryptedKey = str
DecryptedKey = Union[Dict, str]

Logic = Union[Sequence[bool], bool]
Status = Union[Sequence[int], int]
Shape = Union[Sequence[int], int]
Unit = Union[Sequence[int], int]
Range = Union[Sequence[Real], Real]


KEY_TYPES = {
    "Index":Index, "IndexLabel":IndexLabel, "Column":Column, "Keyword":Keyword,
    "Id":Id, "Url":Url, "Token":Token, "EncryptedKey":EncryptedKey, "DecryptedKey":DecryptedKey,
    "Logic":Logic, "Status":Status, "Shape":Shape, "Unit":Unit, "Range":Range
}


###################################################################
########################## Datetime Types #########################
###################################################################

Datetime = Union[datetime, date]
Timestamp = Union[float, int]
DateNumeric = Union[datetime, date, time, float, int]
DateFormat = Union[datetime, date, time, float, int, str]
DateUnit = Literal["second", "minute", "hour", "day", "month", "year"]

DateQuery = Dict[str,datetime]
Timedelta = Union[str, int, timedelta]
Timezone = Union[BaseTzInfo, str]

DATETIME_TYPES = {
    "Datetime":Datetime, "Timestamp":Timestamp, "DateNumeric":DateNumeric,
    "DateFormat":DateFormat, "DateUnit":DateUnit, "DateQuery":DateQuery,
    "Timedelta":Timedelta, "Timezone":Timezone
}


###################################################################
########################## Sequence Types #########################
###################################################################

ObjectSequence = Union[List, Tuple]
OBJECT_SEQUENCE = (List, Tuple)

IndexedSequence = Union[List, Tuple]
INDEXED_SEQUENCE = (List, Tuple)

SequenceSet = Union[ObjectSequence, Set]
SEQUENCE_SET = (List, Set, Tuple)

NestedSequence = Sequence[Sequence]
Records = List[Dict]

SEQUENCE_TYPES = {
    "ObjectSequence":ObjectSequence, "IndexedSequence":IndexedSequence,
    "SequenceSet":SequenceSet, "NestedSequence":NestedSequence, "Records":Records
}


###################################################################
############################ Data Types ###########################
###################################################################

NestedDict = Dict[_KT,Dict]
RenameMap = Dict[str,str]
TypeMap = Dict[str,Type]

NestedData = Union[NestedSequence, NestedDict]
TabularData = Union[Records, DataFrame]
MappingData = Union[Records, DataFrame, Dict]

Data = Union[Records, DataFrame, Dict, List, NestedSequence, NestedDict]
MappedData = Dict[str,Data]
MappedFields = Dict[str,IndexLabel]

JsonData = Union[Dict, List]
JSON_DATA = (Dict, List)

RedirectData = Dict[str,Records]
REDIRECT_DATA = (str, Dict, List)

PandasData = Union[DataFrame, Series]
PANDAS_DATA = (DataFrame, Series)

HtmlData = Union[str, Tag, List[str], List[Tag]]
HTML_DATA = (str, Tag, List)

Account = Dict[str,str]
PostData = Union[Dict[str,Any], str]

ResponseData = Union[bytes, str, JsonData, Records, DataFrame, Tag, List[str], List[Tag]]

DATA_TYPES = {
    "NestedDict":NestedDict, "RenameMap":RenameMap, "TypeMap":TypeMap, "NestedData":NestedData,
    "TabularData":TabularData, "MappingData":MappingData, "Data":Data,
    "MappedData":MappedData, "MappedFields":MappedFields, "JsonData":JsonData,
    "RedirectData":RedirectData, "HtmlData":HtmlData, "Account":Account, "PostData":PostData,
    "PandasData":PandasData, "ResponseData":ResponseData
}


###################################################################
########################## Special Types ##########################
###################################################################

Pagination = Union[bool, str]
Pages = Union[Tuple[int,...], Tuple[Tuple[int,int,int],...]]

RegexFormat = Union[str, re.Pattern]

CastError = (ValueError, TypeError)

ApplyFunction = Union[Callable[[Any],Any], Sequence[Callable[[Any],Any]], Dict]
MatchFunction = Union[Callable[[Any],bool], Sequence[Callable[[Any],bool]]]

SPECIAL_TYPES = {
    "Pagination":Pagination, "Pages":Pages, "RegexFormat":RegexFormat,
    "ApplyFunction":ApplyFunction, "MatchFunction":MatchFunction
}


###################################################################
############################ Type Check ###########################
###################################################################

CUSTOM_TYPES = dict(
    **GENERAL_TYPES, **KEY_TYPES, **DATETIME_TYPES, **SEQUENCE_TYPES, **DATA_TYPES, **SPECIAL_TYPES)

BOOLEAN_TYPES = [bool, "bool", "boolean"]
FLOAT_TYPES = [float, "float"]
INTEGER_TYPES = [int, "int", "integer"]

DATETIME_TYPES = [datetime, "datetime"]
TIME_TYPES = [time, "time"]
TIMESTAMP_TYPES = ["timestamp"]
DATE_TYPES = [date, "date"]

STRING_TYPES = [str, "str", "string", "literal"]
LIST_TYPES = [list, List, "list"]
TUPLE_TYPES = [tuple, Tuple, "tuple"]
SET_TYPES = [set, Set, "set"]

DICT_TYPES = [Dict, dict, "dict", "dictionary"]
RECORDS_TYPES = [Records, "records"]
DATAFRAME_TYPES = [DataFrame, "dataframe", "df", "pandas", "pd"]
TAG_TYPES = [Tag, "tag", "bs4", "html", "source"]
BYTE_TYPES = [bytes, "bytes"]

TYPE_LIST = {
    bool: BOOLEAN_TYPES,
    float: FLOAT_TYPES,
    int: INTEGER_TYPES,
    datetime: DATETIME_TYPES,
    time: TIME_TYPES,
    date: DATE_TYPES,
    str: STRING_TYPES,
    list: LIST_TYPES+RECORDS_TYPES,
    tuple: TUPLE_TYPES,
    set: SET_TYPES,
    dict: DICT_TYPES,
    DataFrame: DATAFRAME_TYPES,
    Tag: TAG_TYPES,
    bytes: BYTE_TYPES,
}

_abs_idx = lambda idx: abs(idx+1) if idx < 0 else idx
_arg_get = lambda arr, idx: arr[idx if _abs_idx(idx) < len(arr) else -1] if arr else None


def get_type(__object: Union[Type,TypeHint,Any], argidx=0) -> Union[Type,Tuple[Type,...]]:
    if isinstance(__object, Type): return __object
    elif isinstance(__object, str): return _get_type_from_str(__object, argidx)
    else: return _get_type_from_origin(__object, argidx)


def _get_type_from_str(__object: str, argidx=0) -> Union[Type,Tuple[Type,...]]:
    if __object.startswith("Union[") and __object.endswith("]"):
        types = [get_type(__t) for __t in re.search(r"^Union\[([^]]+)\]$", __object).groups()[0].split(',')]
        return _arg_get(types, argidx)
    elif __object.startswith("Tuple[") and __object.endswith("]"):
        return tuple(get_type(__t) for __t in re.search(r"^Tuple\[([^]]+)\]$", __object).groups()[0].split(','))
    for __t, __hint in TYPE_LIST.items():
        if __object.lower() in __hint: return __t


def _get_type_from_origin(__object: Union[Type,TypeHint,Any], argidx=0) -> Union[Type,Tuple[Type,...]]:
    __type, args = get_origin(__object), get_args(__object)
    if __type in (Tuple, tuple):
        return tuple(get_type(arg) for arg in args)
    elif args:
        if __type == Union: return get_type(_arg_get(args, argidx))
        else: return get_type(__type)
    elif isinstance(__object, Callable):
        return get_type(__type if __type else get_type_hints(__object).get("return"))
    else: return type(__object)


def init_origin(__object: Union[Type,TypeHint,Any], default=None, argidx=0) -> Any:
    __type = get_type(__object, argidx)
    if isinstance(__type, Tuple):
        return tuple(init_type(__t, default) for __t in __type)
    else: return init_type(__type, default)


def init_type(__type: Type, default=None) -> Any:
    try: return __type()
    except TypeError:
        if __type == datetime: return default if default else datetime.now()
        elif __type == date: return default if default else date.today()
        else: return default


def is_type(__type: TypeHint, __types: TypeList) -> bool:
    return (__type.lower() if isinstance(__type, str) else __type) in __types

def is_object_type(__object, __types: TypeList) -> bool:
    return type(__object) in __types

def is_bool_type(__type: TypeHint) -> bool:
    return is_type(__type, BOOLEAN_TYPES)

def is_float_type(__type: TypeHint) -> bool:
    return is_type(__type, FLOAT_TYPES)

def is_int_type(__type: TypeHint) -> bool:
    return is_type(__type, INTEGER_TYPES)

def is_numeric_type(__type: TypeHint) -> bool:
    return is_type(__type, FLOAT_TYPES+INTEGER_TYPES)

def is_datetime_type(__type: TypeHint) -> bool:
    return is_type(__type, DATETIME_TYPES)

def is_time_type(__type: TypeHint) -> bool:
    return is_type(__type, TIME_TYPES)

def is_timestamp_type(__type: TypeHint) -> bool:
    return is_type(__type, TIMESTAMP_TYPES)

def is_date_type(__type: TypeHint) -> bool:
    return is_type(__type, DATE_TYPES)

def is_numeric_or_date_type(__type: TypeHint) -> bool:
    return is_type(__type, FLOAT_TYPES+INTEGER_TYPES+DATETIME_TYPES+DATE_TYPES)

def is_str_type(__type: TypeHint) -> bool:
    return is_type(__type, STRING_TYPES)

def is_list_type(__type: TypeHint) -> bool:
    return is_type(__type, LIST_TYPES)

def is_tuple_type(__type: TypeHint) -> bool:
    return is_type(__type, TUPLE_TYPES)

def is_set_type(__type: TypeHint) -> bool:
    return is_type(__type, SET_TYPES)

def is_array_type(__type: TypeHint) -> bool:
    return is_type(__type, LIST_TYPES+TUPLE_TYPES)

def is_dict_type(__type: TypeHint) -> bool:
    return is_type(__type, DICT_TYPES)

def is_records_type(__type: TypeHint) -> bool:
    return is_type(__type, RECORDS_TYPES)

def is_json_type(__type: TypeHint) -> bool:
    return is_type(__type, STRING_TYPES+DICT_TYPES+RECORDS_TYPES+["json"])

def is_dataframe_type(__type: TypeHint) -> bool:
    return is_type(__type, DATAFRAME_TYPES)

def is_tag_type(__type: TypeHint) -> bool:
    return is_type(__type, TAG_TYPES)

def is_bytes_type(__type: TypeHint) -> bool:
    return is_type(__type, BYTE_TYPES)


def is_comparable(__object) -> bool:
    try: __object > __object
    except TypeError: return False
    return True


def is_supported_type(__type: TypeHint) -> bool:
    for __types in TYPE_LIST.values():
        if is_type(__type, __types): return True
    return False


def get_type_list(__type: TypeHint) -> TypeList:
    for __types in TYPE_LIST.values():
        if is_type(__type, __types): return __types
    return list()


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

def is_nested_in(__object, __type: Type, how: Literal["any","all"]="any", empty=False) -> bool:
    return is_array(__object) and (
        isin_instance(__object, __type, empty) if how == "any" else allin_instance(__object, __type, empty))

def is_nested_map(__object, empty=False) -> bool:
    return isinstance(__object, Dict) and (empty or (bool(__object) and isin_instance(__object.values(), Dict)))

def is_bool_array(__object, how: Literal["any","all"]="any", empty=False) -> bool:
    return is_nested_in(__object, bool, how=how, empty=empty)

def is_float_array(__object, how: Literal["any","all"]="any", empty=False) -> bool:
    return is_nested_in(__object, float, how=how, empty=empty)

def is_int_array(__object, how: Literal["any","all"]="any", empty=False) -> bool:
    return is_nested_in(__object, int, how=how, empty=empty)

def is_numeric_array(__object, how: Literal["any","all"]="any", empty=False) -> bool:
    return is_nested_in(__object, (bool,float,int), how=how, empty=empty)

def is_datetime_array(__object, how: Literal["any","all"]="any", empty=False) -> bool:
    return is_nested_in(__object, datetime, how=how, empty=empty)

def is_time_array(__object, how: Literal["any","all"]="any", empty=False) -> bool:
    return is_nested_in(__object, time, how=how, empty=empty)

def is_date_array(__object, how: Literal["any","all"]="any", empty=False) -> bool:
    return is_nested_in(__object, date, how=how, empty=empty)

def is_str_array(__object, how: Literal["any","all"]="any", empty=False) -> bool:
    return is_nested_in(__object, str, how=how, empty=empty)

def is_func_array(__object, how: Literal["any","all"]="any", empty=False) -> bool:
    return is_nested_in(__object, Callable, how=how, empty=empty)

def is_2darray(__object, how: Literal["any","all"]="any", empty=False) -> bool:
    return is_nested_in(__object, OBJECT_SEQUENCE, how=how, empty=empty)

def is_records(__object, how: Literal["any","all"]="any", empty=True) -> bool:
    return is_nested_in(__object, Dict, how=how, empty=empty)

def is_json_object(__object) -> bool:
    return isinstance(__object, JSON_DATA)

def is_dfarray(__object, how: Literal["any","all"]="any", empty=False) -> bool:
    return is_nested_in(__object, DataFrame, how=how, empty=empty)

def is_df(__object) -> bool:
    return isinstance(__object, DataFrame)

def is_series(__object) -> bool:
    return isinstance(__object, Series)

def is_df_sequence(__object) -> bool:
    return isinstance(__object, Series) or is_array(__object)

def is_df_object(__object) -> bool:
    return isinstance(__object, (DataFrame, Series))

def is_tag_array(__object, how: Literal["any","all"]="any", empty=False) -> bool:
    return is_nested_in(__object, Tag, how=how, empty=empty)

def is_bytes_array(__object, how: Literal["any","all"]="any", empty=False) -> bool:
    return is_nested_in(__object, bytes, how=how, empty=empty)


###################################################################
############################# Inspect #############################
###################################################################

AVAILABLE_ANNOTATIONS = [
    "literal", "dict", "datetime", "date", "int", "float", "bool", "str", 
    "callable", "dataframe", "sequence", "list", "tuple", "set"]


def _decap(annotation: str, __type="Optional", split=str()) -> Union[str,List[str]]:
    annotation = str(annotation).strip()
    if annotation.startswith(__type) or annotation.startswith(f"typing.{__type}"):
        annotation = re.search(r"{}\[(.*)\]$".format(__type), annotation).groups()[0]
        if split and (split in annotation):
            annotation = list(map(lambda x: x.strip(), annotation.split(split)))
    return annotation


def get_annotation_name(annotation, custom_type=False) -> str:
    if annotation == inspect.Parameter.empty: return str()
    elif isinstance(annotation, Type): return annotation.__name__
    annotation = _decap(annotation, "Optional")
    if custom_type and (annotation in CUSTOM_TYPES):
        return get_annotation_name(CUSTOM_TYPES[annotation])
    return annotation


def get_annotation_typehint(annotation, custom_type=True) -> Union[str,List[str]]:
    annotation = get_annotation_name(annotation, custom_type=custom_type)
    return _decap(annotation, "Union", split=',')


def get_annotation_type(annotation, custom_type=True) -> Union[str,List[str]]:
    name = get_annotation_typehint(annotation, custom_type=custom_type)
    if isinstance(name, List): return [__name for __name in map(get_annotation_type, name) if __name]
    name = name.replace("datetime.","").lower()
    for annotation in AVAILABLE_ANNOTATIONS:
        if annotation in name:
            return annotation
    return str()


def is_iterable_annotation(annotation, custom_type=True) -> bool:
    type_hint = get_annotation_typehint(annotation, custom_type=custom_type)
    name = (', '.join(type_hint) if isinstance(type_hint, List) else type_hint).lower()
    iterable = ["iterable", "sequence", "list", "tuple", "set"]
    return any(map(lambda x: x in name, iterable))


def inspect_parameter(parameter: Parameter, __type: Optional[TypeHint]=None) -> Dict:
    annotation = __type if __type else parameter.annotation
    info = dict(annotation=get_annotation_name(annotation), type=get_annotation_type(annotation))
    if parameter.default != inspect.Parameter.empty:
        info["default"] = parameter.default
        if parameter.annotation == inspect.Parameter.empty:
            annotation = get_annotation_type(type(parameter.default))
            info["type"] = annotation
    if is_iterable_annotation(annotation):
        info["iterable"] = True
    return info


def inspect_annotation(annotation, __type: Optional[TypeHint]=None) -> Dict:
    annotation = __type if __type else annotation
    info = dict(annotation=get_annotation_name(annotation), type=get_annotation_type(annotation))
    if is_iterable_annotation(annotation):
        info["iterable"] = True
    return info


def is_args_allowed(func: Callable) -> bool:
    for name, parameter in inspect.signature(func).parameters.items():
        if parameter.kind == inspect.Parameter.VAR_POSITIONAL: return True
    return False


def is_kwargs_allowed(func: Callable) -> bool:
    for name, parameter in inspect.signature(func).parameters.items():
        if parameter.kind == inspect.Parameter.VAR_KEYWORD: return True
    return False


def from_literal(__literal) -> List[str]:
    if get_origin(__literal) == Union:
        return [__s for __l in get_args(__literal) for __s in get_args(__l)]
    else: return list(get_args(__literal))
