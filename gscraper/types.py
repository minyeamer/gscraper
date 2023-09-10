from typing import Callable, Hashable, Literal, Sequence, Tuple, Type, TypedDict, TypeVar, Union
from typing import Any, Dict, List, Set, get_type_hints, get_origin, get_args

from datetime import datetime, date, time, timedelta
from pandas import DataFrame, Series, isna
from pytz import BaseTzInfo
from re import search


INVALID_TYPE_HINT_MSG = lambda x: f"'{x}' is not valid type hint."


###################################################################
########################## General Types ##########################
###################################################################

_BOOL = TypeVar("_BOOL", Sequence[bool], bool)
_TYPE = TypeVar("_TYPE", Sequence[Type], Type)

_KT = TypeVar("_KT", Sequence[Hashable], Hashable)
_VT = TypeVar("_VT", Sequence[Any], Any)
_PASS = None

Comparable = TypeVar("Comparable")
ClassInstance = TypeVar("ClassInstance")

Arugments = Tuple[Any]
ArgsMapper = Callable[[Arugments],Arugments]

Context = Dict[Any,Any]
ContextMapper = Callable[[Context],Context]

LogLevel = Union[str,int]
LogMessage = Dict[str,str]

TypeHint = Union[Type, str]
TypeList = Sequence[TypeHint]

GENERAL_TYPES = {
    "_BOOL":_BOOL, "_TYPE":_TYPE, "_KT":_KT, "_VT":_VT, "_PASS":_PASS, "Comparable":Comparable,
    "ClassInstance":ClassInstance, "Arugments":Arugments, "ArgsMapper":ArgsMapper,
    "Context":Context, "ContextMapper":ContextMapper, "LogLevel":LogLevel, "LogMessage":LogMessage,
    "TypeHint":TypeHint, "TypeList":TypeList}


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
    "SequenceSet":SequenceSet, "NestedSequence":NestedSequence, "Records":Records}


###################################################################
############################ Data Types ###########################
###################################################################

NestedDict = Dict[_KT,Dict]
RenameMap = Dict[str,str]

NestedData = Union[NestedSequence, NestedDict]
TabularData = Union[Records, DataFrame]
MappingData = Union[Records, DataFrame, Dict]
Data = Union[Records, DataFrame, Dict, List, NestedSequence, NestedDict]

JsonData = Union[Dict, List]
RedirectData = Dict[str,Records]

DATA_TYPES = {
    "NestedDict":NestedDict, "RenameMap":RenameMap, "NestedDict":NestedDict,
    "TabularData":TabularData, "MappingData":MappingData, "Data":Data,
    "JsonData":JsonData, "RedirectData":RedirectData}


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

Status = Union[Sequence[int], int]
Shape = Union[Sequence[int], int]
Unit = Union[Sequence[int], int]

KEY_TYPES = {
    "Index":Index, "IndexLabel":IndexLabel, "Column":Column, "Keyword":Keyword,
    "Id":Id, "Url":Url, "Token":Token, "EncryptedKey":EncryptedKey,
    "Status":Status, "Shape":Shape, "Unit":Unit}


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
    "Timedelta":Timedelta, "Timezone":Timezone}


###################################################################
########################### Schema Types ##########################
###################################################################

SchemaPath = Union[_KT, _VT, Tuple[_KT,_KT], Tuple[_VT,_VT], Callable]
PathType = Literal["path", "value", "tuple", "iterate", "callable", "global"]

ApplyFunction = Union[Callable[[Any],Any], Sequence[Callable[[Any],Any]], Dict]
MatchFunction = Union[Callable[[Any],bool], Sequence[Callable[[Any],bool]]]
SpecialApply = str

class SchemaApply(TypedDict):
    func: Union[ApplyFunction, SpecialApply]
    default: Any

class SchemaMatch(TypedDict):
    func: MatchFunction
    path: _KT
    value: Any
    flip: bool
    strict: bool

class SchemaField(TypedDict):
    name: _KT
    path: SchemaPath
    type: TypeHint
    mode: str
    cast: bool
    strict: bool
    default: Any
    apply: SchemaApply
    match: SchemaMatch

Schema = Sequence[SchemaField]

class SchemaContext(TypedDict):
    schema: Schema
    root: _KT
    path: _KT
    strict: bool
    index: _KT
    match: MatchFunction
    rename: RenameMap
    discard: bool

SchemaInfo = Dict[_KT, SchemaContext]

SCHEMA_TYPES = {
    "SchemaPath":SchemaPath, "PathType":PathType, "ApplyFunction":ApplyFunction,
    "MatchFunction":MatchFunction, "SpecialApply":SpecialApply,
    "SchemaApply":SchemaApply, "SchemaMatch":SchemaMatch, "SchemaField":SchemaField,
    "Schema":Schema, "SchemaContext":SchemaContext, "SchemaInfo":SchemaInfo}


###################################################################
######################## Google Cloud Types #######################
###################################################################

Account = Union[Dict[str,str], str]
PostData = Union[Dict[str,Any],str]

GspreadMode = Literal["replace", "append"]
NumericiseIgnore = Union[Sequence[int], bool]

BigQueryMode = Literal["fail", "replace", "append", "upsert"]
BigQuerySchema = List[Dict[str,str]]

class GspreadReadContext(TypedDict):
    key: str
    sheet: str
    fields: IndexLabel
    str_cols: NumericiseIgnore
    arr_cols: Sequence[IndexLabel]

class GspreadUpdateContext(TypedDict):
    key: str
    sheet: str
    mode: Literal["replace", "append"]
    base_sheet: str
    cell: str

GspreadReadInfo = Dict[_KT, GspreadReadContext]
GspreadUpdateInfo = Dict[_KT, GspreadUpdateContext]

class BigQueryContext(TypedDict):
    table: str
    project_id: str
    mode: Literal["fail", "replace", "append", "upsert"]
    schema: BigQuerySchema
    progress: bool
    partition: str
    partition_by: Literal["auto", "second", "minute", "hour", "day", "month", "year", "date"]

BigQueryInfo = Dict[_KT, BigQueryContext]
UploadInfo = Dict[_KT, Union[GspreadUpdateContext, BigQueryContext]]

GOOGLE_CLOUD_TYPES = {
    "Account":Account, "PostData":PostData, "GspreadMode":GspreadMode,
    "NumericiseIgnore":NumericiseIgnore, "BigQueryMode":BigQueryMode, "BigQuerySchema":BigQuerySchema,
    "GspreadReadContext":GspreadReadContext, "GspreadUpdateContext":GspreadUpdateContext,
    "GspreadReadInfo":GspreadReadInfo, "GspreadUpdateInfo":GspreadUpdateInfo,
    "BigQueryContext":BigQueryContext, "BigQueryInfo":BigQueryInfo, "UploadInfo":UploadInfo}


###################################################################
########################## Special Types ##########################
###################################################################

Pagination = Union[bool, str]
Pages = Tuple[Tuple[int,int,int]]

RegexFormat = str
BetweenRange = Union[Sequence[Tuple], Sequence[Dict]]

CastError = (ValueError, TypeError)

ViewType = Literal["naverSearch", "naverView", "naverBlog", "naverCafe", "naverNews", "naverQnA"]
ApiViewType = Literal["naverShopping", "naverBlog", "naverCafe", "naverNews", "naverQnA"]

SPECIAL_TYPES = {
    "Pagination":Pagination, "Pages":Pages, "RegexFormat":RegexFormat,
    "BetweenRange":BetweenRange, "ViewType":ViewType, "ApiViewType":ApiViewType}


def is_na(__object, strict=True) -> bool:
    if isinstance(__object, float):
        return isna(__object) and (True if strict else (not __object))
    return (__object == None) if strict else (not __object)


def not_na(__object, strict=True) -> bool:
    return not is_na(__object, strict=strict)


###################################################################
############################ Type Check ###########################
###################################################################

CUSTOM_TYPES = dict(
    **GENERAL_TYPES, **SEQUENCE_TYPES, **DATA_TYPES, **KEY_TYPES, **DATETIME_TYPES,
    **SCHEMA_TYPES, **GOOGLE_CLOUD_TYPES, **SPECIAL_TYPES)

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
}

abs_idx = lambda idx: abs(idx+1) if idx < 0 else idx


def get_type(__object: Any, argidx=0) -> Type:
    if isinstance(__object, Type): return __object
    elif isinstance(__object, str):
        types = [__t for __t, __hint in TYPE_LIST.items() if __object.lower() in __hint]
        if types: return types[0]
        else: raise ValueError(INVALID_TYPE_HINT_MSG(__object))
    __type, args = get_origin(__object), get_args(__object)
    if args:
        return get_type(args[argidx if abs_idx(argidx) < len(args) else -1])
    elif isinstance(__object, Callable):
        return get_type(__type if __type else get_type_hints(__object).get("return"))
    else: return type(__object)


def init_origin(__object, argidx=0, default=None) -> Any:
    __type = get_type(__object, argidx)
    try: return __type()
    except TypeError:
        if __type == datetime: return default if default else datetime.now()
        elif __type == date: return default if default else date.today()
        else: return default


def is_type(__type: TypeHint, __types: TypeList) -> bool:
    return (__type.lower() if isinstance(__type, str) else __type) in __types

def is_supported(__type: TypeHint) -> bool:
    for __types in TYPE_LIST.values():
        if is_type(__type, __types): return True
    return False

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

def is_set_type(__type: TypeHint) -> bool:
    return is_type(__type, SET_TYPES)

def is_array_type(__type: TypeHint) -> bool:
    return is_type(__type, LIST_TYPES+TUPLE_TYPES)

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
    return is_nested_in(__object, (float,int), how=how, empty=empty)

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

def is_dfarray(__object, how: Literal["any","all"]="any", empty=True) -> bool:
    return is_nested_in(__object, DataFrame, how=how, empty=empty)

def is_df(__object) -> bool:
    return isinstance(__object, DataFrame)

def is_series(__object) -> bool:
    return isinstance(__object, Series)

def is_df_sequence(__object) -> bool:
    return isinstance(__object, Series) or is_array(__object)

def is_df_object(__object) -> bool:
    return isinstance(__object, (DataFrame, Series))


###################################################################
############################ Annotation ###########################
###################################################################

AVAILABLE_ANNOTATIONS = [
    "bool", "int", "float", "datetime", "date", "str", "literal",
    "sequence", "list", "tuple", "set", "dict", "dataframe", "callable"]


def get_type_name(__object) -> str:
    if isinstance(__object, str):
        if "Optional" in __object:
            __object = search("Optional\[([^]]*)\]", __object).groups()[0]
        if __object in CUSTOM_TYPES:
            __object = CUSTOM_TYPES[__object]
    return __object.__name__ if isinstance(__object, Type) else str(__object)


def get_annotation(__object) -> str:
    name = get_type_name(__object).lower()
    for annotation in AVAILABLE_ANNOTATIONS:
        if annotation in name:
            return annotation
    return str()


def is_iterable_annotation(__object) -> bool:
    name = get_type_name(__object).lower()
    iterable = ["sequence", "list", "tuple", "set"]
    return any(map(lambda x: x in name, iterable))
