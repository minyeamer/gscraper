from __future__ import annotations
from gscraper.base.abstract import CustomDict, TypedDict, TypedRecords, OptionalDict, Value, ValueSet, Query
from gscraper.base.abstract import INVALID_INSTANCE_MSG

from gscraper.base.types import _KT, _VT, _PASS, Context, LogLevel, TypeHint, TypeList
from gscraper.base.types import IndexLabel, Keyword, Pagination, Pages, Unit, DateFormat, DateQuery, Timedelta, Timezone
from gscraper.base.types import RenameMap, TypeMap, Records, NestedDict, Data, ResponseData, PandasData, PANDAS_DATA
from gscraper.base.types import ApplyFunction, MatchFunction, RegexFormat
from gscraper.base.types import get_type, init_origin, is_type, is_bool_type, is_float_type, is_numeric_type
from gscraper.base.types import is_numeric_or_date_type, is_dict_type, is_records_type, is_dataframe_type, is_tag_type
from gscraper.base.types import is_array, allin_instance, is_str_array

from gscraper.utils import isna, notna, exists
from gscraper.utils.cast import cast_object, cast_str, cast_list, cast_tuple, cast_float, cast_int, cast_int1
from gscraper.utils.date import now, today, get_date, get_date_pair, set_date, is_daily_frequency, get_date_range
from gscraper.utils.logs import CustomLogger, dumps_data, log_exception, log_data
from gscraper.utils.map import isna_plus, exists_one, howin, safe_apply, safe_len, get_scala, unique
from gscraper.utils.map import re_get, replace_map, startswith, endswith, arg_and, union, diff
from gscraper.utils.map import iloc, fill_array, is_same_length, unit_array, concat_array, transpose_array
from gscraper.utils.map import kloc, is_single_path, hier_get, notna_dict, chain_dict, drop_dict, apply_dict
from gscraper.utils.map import vloc, concat_df, safe_apply_df, match_df, to_dataframe, to_series
from gscraper.utils.map import get_value, filter_data, set_data, isin_data, chain_exists, groupby_data, is_single_selector

from abc import ABCMeta
from ast import literal_eval
from math import ceil
from itertools import product
import copy
import functools
import logging
import os

from typing import Any, Dict, Callable, Iterable, List, Literal, Optional, Sequence, Tuple, Type, Union
from bs4 import BeautifulSoup
from bs4.element import Tag
import datetime as dt
import json
import pandas as pd
import re


CHECKPOINT = [
    "all", "context", "crawl", "params", "iterator", "iterator_count", "gather", "gather_count",
    "redirect", "request", "response", "parse", "map", "schema", "field", "group",
    "apply", "[origin]_apply" "match", "[origin]_match", "login", "exception"]
CHECKPOINT_PATH = "saved/"

PAGE_ITERATOR = ["page", "start", "dataSize"]
PAGE_PARAMS = ["size", "pageSize", "pageStart", "offset"]

DATE_ITERATOR = ["startDate", "endDate"]
DATE_PARAMS = ["startDate", "endDate", "interval"]

SCHEMA, ROOT, MATCH, RANK = "schema", "root", "match", "rank"
SCHEMA_KEY = "__key"
SCHEMA_KEYS = [SCHEMA, ROOT, MATCH]

NAME, PATH, TYPE, MODE, DESC = "name", "path", "type", "mode", "desc"
APPLY, MATCH, CAST = "apply", "match", "cast"
HOW, VALUE, TUPLE, ITERATE, CALLABLE = "how", "value", "tuple", "iterate", "callable"

QUERY, INDEX, LABEL = "QUERY", "INDEX", "LABEL"
NULLABLE, NOTNULL, NOTZERO = "NULLABLE", "NOTNULL", "NOTZERO"
REQUIRED, OPTIONAL = "REQUIRED", "OPTIONAL"

MATCH_QUERY, EXACT, INCLUDE, EXCLUDE, FLIP, COUNT = "query", "exact", "include", "exclude", "flip", "count"

FUNC = "func"
__CAST__ = "__CAST__"
__EXISTS__ = "__EXISTS__"
__JOIN__ = "__JOIN__"
__REGEX__ = "__REGEX__"
__RENAME__ = "__RENAME__"
__SPLIT__ = "__SPLIT__"
__STAT__ = "__STAT__"
__SUM__ = "__SUM__"
__MAP__ = "__MAP__"
__MISMATCH__ = "__MISMATCH__"

ITER_INDEX = "__index"
ITER_SUFFIX = lambda context: f"_{context[ITER_INDEX]}" if ITER_INDEX in context else str()
ITER_MSG = lambda context: {ITER_INDEX: context[ITER_INDEX]} if ITER_INDEX in context else dict()

COUNT_INDEX = "__i"
_NESTED_SUFFIX = lambda context: \
    f"[{context[COUNT_INDEX]}]" if (ITER_INDEX in context) and isinstance(context[ITER_INDEX], str) else f"-{context[COUNT_INDEX]}"
_COUNT_SUFFIX = lambda context: _NESTED_SUFFIX(context) if COUNT_INDEX in context else str()

_SCHEMA_SUFFIX = lambda context: f"_{context[SCHEMA_KEY]}" if SCHEMA_KEY in context else str()
_FIELD_SUFFIX = lambda field: f"_{field[NAME]}" if NAME in field else str()
_NAME_SUFFIX = lambda name: f"_{name}" if name else str()
SUFFIX = lambda context, field=dict(), name=str(): \
    _SCHEMA_SUFFIX(context) + _FIELD_SUFFIX(field) + _NAME_SUFFIX(name) + ITER_SUFFIX(context) + _COUNT_SUFFIX(context)

TZINFO = None
START, END = 0, 1


###################################################################
############################# Messages ############################
###################################################################

USER_INTERRUPT_MSG = lambda where: f"Interrupt occurred on {where} by user."

PAGINATION_ERROR_MSG = "Pagination params, size or pageStart are not valid."
EMPTY_CONTEXT_QUERY_MSG = "One or more queries for crawling do not exist."

INVALID_MATCH_KEY_MSG = "Match function requires at least one parameter: func, path, and query."
INVALID_PATH_TYPE_MSG = lambda path: f"'{path}' is not supported type for schema path."

FOR_SCHEMA = lambda context: f" for the '{context[SCHEMA_KEY]}' schema" if SCHEMA_KEY in context else str()
FROM_SCHEMA = lambda context: f" from the '{context[SCHEMA_KEY]}' schema" if SCHEMA_KEY in context else str()

FOR_NAME = lambda name: f" for the '{name}' field" if name else str()
FOR_FIELD = lambda field, name: f" fror the '{field[NAME]}' field" if NAME in field else FOR_NAME(name)

OF_NAME = lambda name: f" of the '{name}' field" if name else str()
OF_FIELD = lambda field, name: f" of the '{field[NAME]}' field" if NAME in field else OF_NAME(name)

WITH_NAME = lambda name: f" on the '{name}' field" if name else str()
WITH_FIELD = lambda field, name: f" on the '{field[NAME]}' field" if NAME in field else WITH_NAME(name)

WHERE = lambda context, field=dict(), name=str(): FOR_FIELD(field, name) + FROM_SCHEMA(context)

INVALID_DATA_TYPE_MSG = lambda data, context: \
    f"'{type(data)}' is not a valid type{FOR_SCHEMA(context)}."
INVALID_APPLY_TYPE_MSG = lambda apply, context=dict(), field=dict(), name=str(): \
    f"'{type(apply)}' is not a valid Apply object{WHERE(context, field, name)}."
INVALID_APPLY_SPECIAL_MSG = lambda func, context=dict(), field=dict(), name=str(): \
    f"'{func}' is not a valid Special Apply{WHERE(context, field, name)}."
INVALID_VALUE_TYPE_MSG = lambda value, context=dict(), field=dict(), name=str(): \
    f"'{type(value)}' is a not valid value type{WHERE(context, field, name)}."

EXCEPTION_MSG = lambda context=dict(), field=dict(), name=str(): \
    f"Exception occurred{WITH_FIELD(field, name)}{FROM_SCHEMA(context)}."

REQUIRED_MSG = lambda context=dict(), field=dict(), name=str(): \
    f"Value{OF_FIELD(field, name)} is required{FROM_SCHEMA(context)}."

UPDATE_DATE, UPDATE_TIME = "updateDate", "updateTime"


###################################################################
############################# Function ############################
###################################################################

class Function(OptionalDict):
    __metaclass__ = ABCMeta

    def raise_ctype_error(self, __object, __type=str()):
        __type = __type if __type else self.__class__.__name__
        raise TypeError(INVALID_INSTANCE_MSG(__object, __type))


class Apply(Function):
    def __init__(self, func: Union[ApplyFunction, str], default: Optional[Any]=None, **context):
        super().__init__(**self.validate_function(func), optional=dict(default=default), **context)

    def validate_function(self, func: Union[ApplyFunction, str]) -> Context:
        if not isinstance(func, (Callable,str)):
            self.raise_ctype_error(func)
        else: return dict(func=func)


class Match(Function):
    def __init__(self, func: Optional[MatchFunction]=None, path: Optional[Union[_KT,Tuple[_KT]]]=None,
                query: Optional[Union[_KT,Tuple[_KT]]]=None, value: Optional[Any]=None,
                exact: Optional[_VT]=None, include: Optional[_VT]=None, exclude: Optional[_VT]=None,
                flip=False, strict=False, how: Literal["any","all"]="any", if_null=False,
                hier=True, default=False, **context):
        super().__init__(**self.validate_function(func, path, query), **context,
            optional=dict(
                value=value, exact=exact, include=include, exclude=exclude, flip=flip, strict=strict,
                how=how, if_null=if_null, hier=hier, default=default),
            null_if=dict(flip=False, strict=False, how="any", if_null=False, hier=True, default=False))

    def validate_function(self, func: Optional[MatchFunction]=None, path: Optional[_KT]=None, query: Optional[_KT]=None) -> Context:
        if isna(func) and isna(path) and isna(query):
            raise ValueError(INVALID_MATCH_KEY_MSG)
        elif notna(func) and not isinstance(func, Callable):
            self.raise_ctype_error(func)
        else: return notna_dict(func=func, path=path, query=query)


class Cast(Apply):
    def __init__(self, type: TypeHint, default: Optional[Any]=None, strict=True, **context):
        Function.__init__(self, func=__CAST__, type=type, default=default, **context,
            optional=dict(strict=strict), null_if=dict(strict=True))


class Exists(Apply):
    def __init__(self, keys: _KT=list(), default: Optional[Any]=None, hier=False):
        Function.__init__(self, func=__EXISTS__, keys=keys, default=default,
            optional=dict(hier=hier), null_if=dict(hier=False))


class Join(Apply):
    def __init__(self, keys: _KT=list(), sep=',', default: Optional[Any]=None,
                hier=False, strip=True, split: Optional[str]=None):
        Function.__init__(self, func=__JOIN__, keys=keys, sep=sep, default=default,
            optional=dict(hier=hier, strip=strip, split=split), null_if=dict(hier=False, strip=True))


class Regex(Apply):
    def __init__(self, pattern: RegexFormat, default=None, index: Optional[int]=0,
                repl: Optional[str]=None, strip=False):
        Function.__init__(self, func=__REGEX__, pattern=pattern, default=default,
            optional=dict(index=index, repl=repl, strip=strip), null_if=dict(index=0, strip=False))


class Rename(Apply):
    def __init__(self, rename: RenameMap, path: Optional[_KT]=None,
                if_null: Union[Literal["null","pass","error"],Any]="null"):
        Function.__init__(self, func=__RENAME__, rename=rename,
            optional=dict(path=path, if_null=if_null), null_if=dict(if_null="null"))


class Split(Apply):
    def __init__(self, sep=',', maxsplit=-1, default: Optional[Any]=None,
                strict=True, index: Optional[int]=None, type: Optional[TypeHint]=None):
        Function.__init__(self, func=__SPLIT__, sep=sep, default=default,
            optional=dict(maxsplit=maxsplit, strict=strict, index=index, type=type),
            null_if=dict(maxsplit=-1, strict=True))


class Stat(Apply):
    def __init__(self, stat: Callable, keys: _KT=list(), default: Optional[Any]=None,
                hier=False, type: Optional[TypeHint]=None, strict=True):
        Function.__init__(self, func=__STAT__, stat=stat, keys=keys, default=default,
            optional=dict(hier=hier, type=type, strict=strict), null_if=dict(hier=False, strict=True))


class Sum(Stat):
    def __init__(self, keys: _KT=list(), default: Optional[Any]=None,
                hier=False, type: Optional[TypeHint]=None, strict=True):
        super().__init__(stat=sum, keys=keys, default=default, hier=hier, type=type, strict=strict)


class Map(Apply):
    def __init__(self, schema: Schema, type: Optional[TypeHint]=None, root: Optional[_KT]=None,
                match: Optional[Match]=None, groupby: Optional[_KT]=None, groupSize: Optional[NestedDict]=None,
                countby: Literal["page","start"]="start", page=1, start=1,
                submatch: Optional[Union[MatchFunction,bool]]=True, discard=True) -> Data:
        Function.__init__(self, func=__MAP__, schema=schema,
            optional=dict(
                type=type, root=root, match=match, groupby=groupby, groupSize=groupSize,
                countby=countby, page=page, start=start, submatch=submatch, discard=discard),
            null_if=dict(countby="start", page=1, start=1, submatch=True, discard=True))


###################################################################
############################## Field ##############################
###################################################################

SchemaPath = Union[_KT, _VT, Tuple[_KT,_KT], Tuple[_VT,_VT], Callable]
SchemaMode = Literal["QUERY", "INDEX", "LABEL", "NULLABLE", "NOTNULL", "NOTZERO", "REQUIRED", "OPTIONAL"]
PathType = Literal["path", "value", "tuple", "iterate", "callable", "global"]

class Field(Value):
    typeCast = True

    def __init__(self, name: _KT, type: TypeHint, desc: Optional[str]=None, mode: Optional[SchemaMode]=None,
                path: Optional[SchemaPath]=None, default: Optional[Any]=None, cast: Optional[bool]=None, strict=True,
                apply: Optional[Apply]=None, match: Optional[Match]=None, **kwargs):
        super().__init__(name=name, type=type)
        self.update_optional(
            desc=desc, mode=mode, path=path, default=default, cast=cast, strict=strict, apply=apply, match=match, **kwargs)

    def update_optional(self, desc: Optional[str]=None, mode: Optional[SchemaMode]=None, path: Optional[SchemaPath]=None,
                        default: Optional[Any]=None, cast: Optional[bool]=None, strict=True,
                        apply: Optional[Apply]=None, match: Optional[Match]=None, inplace=True, **kwargs):
        if (mode is None) or (path is None):
            return self.update_notna(desc=desc, **kwargs)
        elif mode in (QUERY,LABEL): pass
        elif mode == INDEX: path = [COUNT_INDEX]
        elif mode == NOTNULL: default, cast = init_origin(self[TYPE]), True
        elif mode == NOTZERO: cast, strict = True, False
        elif is_numeric_or_date_type(self[TYPE]):
            if cast is None: cast = True
            if is_bool_type(self[TYPE]): strict = False
        self.update_notna(
            desc=desc, mode=mode, path=path, default=default, cast=cast, strict=(None if strict is True else strict),
            apply=_to_apply_func(apply), match=_to_match_func(match), how=_get_path_type(path), **kwargs)

    def copy(self) -> Field:
        return copy.deepcopy(self)

    def update(self, __m: Dict=dict(), inplace=True, self_var=False, **kwargs) -> Field:
        return super().update(__m, inplace=inplace, self_var=self_var, **kwargs)


def _get_path_type(path: SchemaPath) -> PathType:
    if isinstance(path, Sequence):
        if isinstance(path, Tuple) and len(path) == 2: return TUPLE
        elif len(path) > 0 and is_array(path[-1]): return ITERATE
        else: return PATH
    elif isinstance(path, Callable): return CALLABLE
    else: return VALUE


def _to_apply_func(apply: Any) -> Apply:
    if isinstance(apply, Apply) or (not apply): return apply
    elif isinstance(apply, (Callable,str)): return Apply(func=apply)
    elif isinstance(apply, Dict): return Apply(**apply)
    elif isinstance(apply, Sequence):
        return [_to_apply_func(func) for func in apply]
    else: raise TypeError(INVALID_INSTANCE_MSG(apply, apply.__class__.__name__))


def _to_match_func(match: Any) -> Match:
    if isinstance(match, Match) or (not match): return match
    elif isinstance(match, Callable): return Match(func=match)
    elif is_array(match): return Match(path=match)
    else: raise TypeError(INVALID_INSTANCE_MSG(match, match.__class__.__name__))


COMMON_FIELDS = [
    Field(name="updateDate", type="DATE", desc=UPDATE_DATE),
    Field(name="updateTime", type="DATETIME", desc=UPDATE_TIME),
]


###################################################################
############################## Schema #############################
###################################################################

class Schema(ValueSet):
    dtype = Field
    typeCheck = True

    def __init__(self, *fields: Field):
        super().__init__(*fields)

    def copy(self) -> Schema:
        return copy.deepcopy(self)

    def update(self, __iterable: Iterable[Field], inplace=True) -> Schema:
        return super().update(__iterable, inplace=inplace)

    def get(self, __key: _KT, default=None, if_null: Literal["drop","pass"]="pass", reorder=True,
            values_only=True, common_fields=False) -> Union[Records,List]:
        schema = (self + COMMON_FIELDS) if common_fields else self
        return vloc(schema, __key, default=default, if_null=if_null, reorder=reorder, values_only=values_only)

    def map(self, key: str, value: _KT, default=None, if_null: Literal["drop","pass"]="drop", reorder=True,
            values_only=False, common_fields=True) -> Dict[_VT,_VT]:
        schema = (self + COMMON_FIELDS) if common_fields else self
        return ValueSet.map(schema, key, value, default=default, if_null=if_null, reorder=reorder, values_only=values_only)

    def rename(self, __s: str, to: Optional[Literal["name","desc"]]="desc",
                if_null: Union[Literal["pass"],Any]="pass", common_fields=True) -> str:
        renameMap = self.get_rename_map(to=to, common_fields=common_fields)
        if renameMap and (__s in renameMap): return renameMap[__s]
        else: return __s if if_null == "pass" else if_null

    def get_rename_map(self, to: Literal["name","desc"]="desc", common_fields=True) -> RenameMap:
        key, value = (DESC, NAME) if to == NAME else (NAME, DESC)
        return self.map(key, value, common_fields=common_fields)

    def get_type_map(self, key: Literal["name","desc"]="name", common_fields=True) -> TypeMap:
        return self.map(key, TYPE, common_fields=common_fields)


###################################################################
############################### Info ##############################
###################################################################

class Info(TypedDict):
    dtype = (Query, Schema)
    typeCheck = True

    def __init__(self, **kwargs: Union[Query,Schema]):
        super().__init__(**kwargs)

    def validate_dtype(self, __object: Iterable) -> Union[Query,Schema]:
        if isinstance(__object, self.dtype): return __object
        elif isinstance(__object, Iterable): return Schema(*__object)
        else: self.raise_dtype_error(__object, Schema)

    ###################################################################
    ############################### Get ###############################
    ###################################################################

    def get_query(self, match: Optional[MatchFunction]=None, **match_by_key) -> Query:
        if not isinstance(self.get(QUERY), Query):
            query = get_scala(list(self.get_by_dtype(Query).values()))
            if not isinstance(query, Query): return Query()
        else: query = self[QUERY]
        query.filter(match, **match_by_key, inplace=True)
        return query

    def get_schema(self, keys: _KT=list(), keep: Literal["fist","last",True,False]=True,
                    match: Optional[MatchFunction]=None, **match_by_key) -> Schema:
        if (not is_array(keys)) and isinstance(self.get(keys), Schema): schema = self[keys]
        else: schema = Schema(*union(*self.get_by_dtype(dtype=Schema, keys=keys).values()))
        schema.unique(keep=keep, inplace=True)
        schema.filter(match, **match_by_key, inplace=True)
        return schema

    def get_by_dtype(self, dtype: Type, keys: _KT=list()) -> Dict:
        keys = cast_list(keys)
        if not keys: return {__k: __s for __k, __s in self.items() if isinstance(__s, dtype)}
        else: return {__k: self[__k] for __k in keys if (__k in self) and isinstance(self[__k], dtype)}

    def get_schema_by_name(self, name: _KT, keys: _KT=list(), keep: Literal["fist","last",True,False]=True) -> Schema:
        match = lambda __value: __value in cast_list(name)
        return self.get_schema(keys, keep=keep, name=match)

    def get_schema_by_type(self, __type: Union[TypeHint,TypeList], keys: _KT=list(),
                            keep: Literal["fist","last",True,False]=True) -> Union[Schema,_VT]:
        __type = tuple(map(get_type, __type)) if is_array(__type) else get_type(__type)
        match = lambda __value: is_type(__value, __type)
        return self.get_schema(keys, keep=keep, type=match)

    ###################################################################
    ############################### Map ###############################
    ###################################################################

    def map(self, key: str, value: _KT, query=False, keys: _KT=list(), common_fields=True,
            keep: Literal["fist","last",False]="first", if_null: Literal["drop","pass"]="drop", reorder=True,
            values_only=False, match: Optional[MatchFunction]=None, **match_by_key) -> Dict[_VT,Union[_VT,Dict]]:
        context = dict(if_null=if_null, reorder=reorder, values_only=values_only, match=match, **match_by_key)
        if query: return self.map_query(key, value, **context)
        else: return self.map_schema(key, value, keys=keys, common_fields=common_fields, keep=keep, **context)

    def map_query(self, key: str, value: _KT, if_null: Literal["drop","pass"]="drop", reorder=True,
                    values_only=False, match: Optional[MatchFunction]=None, **match_by_key) -> Dict[_VT,Union[_VT,Dict]]:
        query = self.get_query(match=match, **match_by_key)
        return query.map(key, value, if_null=if_null, reorder=reorder, values_only=values_only)

    def map_schema(self, key: str, value: _KT, keys: _KT=list(), common_fields=True,
                    keep: Literal["fist","last",False]="first", if_null: Literal["drop","pass"]="drop", reorder=True,
                    values_only=False, match: Optional[MatchFunction]=None, **match_by_key) -> Dict[_VT,Union[_VT,Dict]]:
        schema = self.get_schema(keys, keep=keep, match=match, **match_by_key)
        return schema.map(key, value, if_null=if_null, reorder=reorder, values_only=values_only, common_fields=common_fields)

    def rename(self, __s: str, to: Optional[Literal["name","desc"]]="desc", query=False, common_fields=True,
                if_null: Union[Literal["pass"],Any]="pass", keep: Literal["fist","last",False]="first") -> str:
        renameMap = self.get_rename_map(to=to, query=query, common_fields=common_fields, keep=keep)
        if renameMap and (__s in renameMap): return renameMap[__s]
        else: return __s if if_null == "pass" else if_null

    def get_rename_map(self, to: Literal["name","desc"]="desc", query=False, keys: _KT=list(), common_fields=True,
                        keep: Literal["fist","last",False]="first") -> RenameMap:
        key, value = (DESC, NAME) if to == NAME else (NAME, DESC)
        return self.map(key, value, keys=keys, query=query, common_fields=common_fields, keep=keep)

    def get_type_map(self, key: Literal["name","desc"]="name", query=False, keys: _KT=list(), common_fields=True,
                        keep: Literal["fist","last",False]="first") -> TypeMap:
        return self.map(key, TYPE, keys=keys, query=query, common_fields=common_fields, keep=keep)


###################################################################
############################# Session #############################
###################################################################

class BaseSession(CustomDict):
    __metaclass__ = ABCMeta
    operation = "session"
    tzinfo = TZINFO
    datetimeUnit = "second"
    errors = list()
    info = Info()

    def __init__(self, tzinfo: Optional[Timezone]=None, datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debug: Optional[Keyword]=None, extraSave: Optional[Keyword]=None, interrupt: Optional[Keyword]=None, **context):
        self.set_init_time(tzinfo, datetimeUnit)
        self.set_logger(logName, logLevel, logFile, debug, extraSave, interrupt, localSave)
        super().__init__(context)

    def set_init_time(self, tzinfo: Optional[Timezone]=None, datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None):
        self.tzinfo = tzinfo if tzinfo else self.tzinfo
        self.datetimeUnit = datetimeUnit if datetimeUnit else self.datetimeUnit
        self.initTime = now(tzinfo=self.tzinfo, droptz=True)

    def set_logger(self, logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debug: Optional[Keyword]=None, extraSave: Optional[Keyword]=None, interrupt: Optional[Keyword]=None):
        logName = logName if logName else self.operation
        self.logLevel = int(logLevel) if str(logLevel).isdigit() else logging.getLevelName(str(logLevel).upper())
        self.logFile = logFile
        self.logJson = bool(logFile)
        self.logger = CustomLogger(name=logName, level=self.logLevel, file=self.logFile)
        self.localSave = localSave
        self.debug = cast_list(debug)
        self.extraSave = cast_list(extraSave)
        self.interrupt = cast_list(interrupt)

    def from_locals(self, locals: Dict=dict(), drop: _KT=list(), **context) -> Context:
        drop_keys = cast_list(drop)
        if locals:
            if "context" in drop_keys:
                locals.pop("context", None)
                drop_keys.pop(drop_keys.index("context"))
            else: locals = dict(locals, **locals.pop("context", dict()))
            context = dict(locals, **context)
            context.pop("self", None)
        return drop_dict(context, drop_keys, inplace=False) if drop_keys else context

    ###################################################################
    ############################# Datetime ############################
    ###################################################################

    def now(self, __format=str(), days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0,
            hours=0, weeks=0, droptz=True, droptime=False, unit=str()) -> Union[dt.datetime,dt.date,str]:
        return now(__format, days, seconds, microseconds, milliseconds, minutes, hours, weeks,
                    tzinfo=self.tzinfo, droptz=droptz, droptime=droptime, unit=(unit if unit else self.datetimeUnit))

    def today(self, __format=str(), days=0, weeks=0, droptime=True) -> Union[dt.datetime,dt.date,str]:
        return today(__format, days, weeks, tzinfo=self.tzinfo, droptime=droptime)

    def get_date(self, date: Optional[DateFormat]=None, if_null: Optional[Union[int,str]]=None, busdate=False) -> dt.date:
        return get_date(date, if_null=if_null, tzinfo=self.tzinfo, busdate=busdate)

    def get_date_pair(self, startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                        if_null: Optional[Unit]=None, busdate=False) -> Tuple[dt.date,dt.date]:
        return get_date_pair(startDate, endDate, if_null=if_null, busdate=busdate)

    def set_date(self, date: Optional[DateFormat]=None, __format="%Y-%m-%d",
                if_null: Optional[Union[int,str]]=None, busdate=False) -> str:
        date = self.get_date(date, if_null=if_null, busdate=busdate)
        return set_date(date, __format)

    def set_date_pair(self, startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None, __format="%Y-%m-%d",
                        if_null: Optional[Unit]=None, busdate=False) -> Tuple[str,str]:
        startDate, endDate = self.get_date_pair(startDate, endDate, if_null=if_null, busdate=busdate)
        return set_date(startDate, __format), set_date(endDate, __format)

    def set_update_time(self, __data: Data, date: Optional[dt.date]=None, datetime: Optional[dt.datetime]=None) -> Data:
        updateDate = date if isinstance(date, dt.date) else self.today()
        updateTime = datetime if isinstance(datetime, dt.datetime) else self.now()
        return set_data(__data, if_exists="ignore", updateDate=updateDate, updateTime=updateTime)

    ###################################################################
    ############################## Schema #############################
    ###################################################################

    def get_query(self, match: Optional[MatchFunction]=None, **match_by_key) -> Query:
        return self.info.get_query(match=match, **match_by_key)

    def get_schema(self, keys: _KT=list(), keep: Literal["fist","last",True,False]=True, common_fields=True,
                    match: Optional[MatchFunction]=None, **match_by_key) -> Schema:
        return self.info.get_schema(keys, common_fields=common_fields, keep=keep, match=match, **match_by_key)

    def get_info_map(self, key: str, value: _KT, query=False, keys: _KT=list(), common_fields=True,
                    keep: Literal["fist","last",False]="first", if_null: Literal["drop","pass"]="drop", reorder=True,
                    values_only=False, match: Optional[MatchFunction]=None, **match_by_key) -> Dict[_VT,Union[_VT,Dict]]:
        context = dict(if_null=if_null, reorder=reorder, values_only=values_only, match=match, **match_by_key)
        return self.info.map(key, value, keys=keys, query=query, common_fields=common_fields, keep=keep, **context)

    def get_query_map(self, key: str, value: _KT, if_null: Literal["drop","pass"]="drop", reorder=True,
                    values_only=False, match: Optional[MatchFunction]=None, **match_by_key) -> Dict[_VT,Union[_VT,Dict]]:
        context = dict(if_null=if_null, reorder=reorder, values_only=values_only, match=match, **match_by_key)
        return self.info.map_query(key, value, **context)

    def get_schema_map(self, key: str, value: _KT, keys: _KT=list(), common_fields=True,
                    keep: Literal["fist","last",False]="first", if_null: Literal["drop","pass"]="drop", reorder=True,
                    values_only=False, match: Optional[MatchFunction]=None, **match_by_key) -> Dict[_VT,Union[_VT,Dict]]:
        context = dict(if_null=if_null, reorder=reorder, values_only=values_only, match=match, **match_by_key)
        return self.info.map_schema(key, value, keys=keys, common_fields=common_fields, keep=keep, **context)

    def rename(self, __s: str, to: Optional[Literal["name","desc"]]="desc", query=False, common_fields=True,
                if_null: Union[Literal["pass"],Any]="pass", keep: Literal["fist","last",False]="first") -> str:
        return self.info.rename(__s, to=to, query=query, common_fields=common_fields, if_null=if_null, keep=keep)

    def get_rename_map(self, to: Literal["name","desc"]="desc", query=False, keys: _KT=list(), common_fields=True,
                        keep: Literal["fist","last",False]="first") -> RenameMap:
        return self.info.get_rename_map(to=to, keys=keys, query=query, common_fields=common_fields, keep=keep)

    def get_type_map(self, key: Literal["name","desc"]="name", query=False, keys: _KT=list(), common_fields=True,
                        keep: Literal["fist","last",False]="first") -> TypeMap:
        return self.info.get_type_map(key=key, keys=keys, query=query, common_fields=common_fields, keep=keep)

    ###################################################################
    ############################ Checkpoint ###########################
    ###################################################################

    def checkpoint(self, point: str, where: str, msg: Dict,
                    save: Optional[Data]=None, ext: Optional[TypeHint]=None):
        if self.debug and self._isin_log_list(point, self.debug):
            self.logger.warning(dict(point=f"({point})", **dumps_data(msg, limit=0)))
        if self.extraSave and self._isin_log_list(point, self.extraSave) and notna(save):
            save, ext = self._validate_extension(save, ext)
            self._validate_dir(CHECKPOINT_PATH)
            self.save_data(save, prefix=CHECKPOINT_PATH+str(point).replace('.','_'), ext=ext)
        if self.interrupt and self._isin_log_list(point, self.interrupt):
            raise KeyboardInterrupt(USER_INTERRUPT_MSG(where))

    def save_data(self, data: Data, prefix=str(), ext: Optional[TypeHint]=None):
        prefix = prefix if prefix else self.operation
        file_name = prefix+'_'+self.now("%Y%m%d%H%M%S")
        ext = ext if ext else type(data)
        if is_dataframe_type(ext):
            self.save_dataframe(data, file_name+".xlsx")
        elif is_tag_type(ext):
            self.save_source(data, file_name+".html")
        else: self.save_json(data, file_name+".json")

    def save_json(self, data: Data, file_name: str):
        file_name = self._validate_file(file_name)
        if isinstance(data, pd.DataFrame): data = data.to_dict("records")
        with open(file_name, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)

    def save_dataframe(self, data: Data, file_name: str, sheet_name="Sheet1",
                        rename: Optional[Union[Literal["name","desc"],Dict]]="desc"):
        file_name = self._validate_file(file_name)
        rename = rename if isinstance(rename, Dict) else self.get_rename_map(to=rename)
        data = to_dataframe(data).rename(columns=rename)
        try: data.to_excel(file_name, sheet_name=sheet_name, index=False)
        except: self._write_dataframe(data, file_name)

    def _write_dataframe(self, data: pd.DataFrame, file_name: str, sheet_name="Sheet1"):
        writer = pd.ExcelWriter(file_name, engine="xlsxwriter", engine_context={"options":{"strings_to_urls":False}})
        data.to_excel(writer, sheet_name=sheet_name, index=False)
        writer.close()

    def save_source(self, data: Union[str,Tag], file_name: str):
        file_name = self._validate_file(file_name)
        if not isinstance(data, Tag):
            data = BeautifulSoup(data, "html.parser")
        with open(file_name, "w", encoding="utf-8") as f:
            f.write(str(data.prettify()))

    def _isin_log_list(self, point: str, log_list: Keyword) -> bool:
        log_list = cast_list(log_list)
        if (point in log_list) or ("all" in log_list): return True
        for log_name in log_list:
            if point.startswith('['):
                if any([str(name).startswith('[') for name in cast_list(log_name)]): pass
                else: continue
            if self._isin_log_name(point, log_name): return True
        return False

    def _isin_log_name(self, point: str, log_name: Keyword) -> bool:
        if is_array(log_name): return arg_and(*[self._isin_log_name(point, name) for name in log_name])
        elif log_name.startswith('_') and log_name.endswith('_'): return log_name in point
        elif endswith(log_name, ['_','-']): return point.startswith(log_name)
        elif startswith(log_name, ['_','-','[']): return point.endswith(log_name)
        else: return False

    def _validate_extension(self, data: Data, ext: Optional[TypeHint]=None) -> Tuple[Data, TypeHint]:
        if ext: return data, ext
        elif isinstance(data, str) and data:
            try: return json.loads(data), "json"
            except: return data, "html"
        elif isinstance(data, pd.DataFrame):
            return data, "dataframe"
        elif isinstance(data, Tag):
            return data, "html"
        else: return data, "json"

    def _validate_dir(self, dir: str):
        if not os.path.exists(dir):
            os.mkdir(dir)

    def _validate_file(self, file: str):
        file, ext = os.path.splitext(file)
        __i, suffix = 1, str()
        while True:
            if os.path.exists(file+suffix+ext):
                suffix, __i = f"_{__i}", __i+1
            else: break
        return file+suffix+ext

    ###################################################################
    ########################### Log Managers ##########################
    ###################################################################

    def catch_exception(func):
        @functools.wraps(func)
        def wrapper(self: BaseSession, *args, **context):
            try: return func(self, *args, **context)
            except KeyboardInterrupt as interrupt:
                raise interrupt
            except Exception as exception:
                return self.pass_exception(exception, func=func, msg={"args":args, "context":context})
        return wrapper

    def ignore_exception(func):
        @functools.wraps(func)
        def wrapper(self: BaseSession, *args, **context):
            try: return func(self, *args, **context)
            except: return init_origin(func)
        return wrapper

    def pass_exception(self, exception: Exception, func: Callable, msg: Dict) -> Any:
        self.log_errors(func=func, msg=msg)
        if ("exception" in self.debug) or ("all" in self.debug):
            self.checkpoint("exception", where=func.__name__, msg=msg)
            raise exception
        return init_origin(func)

    def log_errors(self, func: Callable, msg: Dict):
        func_name = f"{func.__name__}({self.__class__.__name__})"
        self.logger.error(log_exception(func_name, json=self.logJson, **msg))
        self.errors.append(msg)

    ###################################################################
    ############################ Print Log ############################
    ###################################################################

    def print_log(self, log_string: str, func="checkpoint", path: Optional[_KT]=None, drop: Optional[_KT]=None,
                    indent=2, step=2, sep=' '):
        log_object = self._eval_log(log_string, func=func)
        self.print(log_object, path=path, drop=drop, indent=indent, step=step, sep=sep)

    def _eval_log(self, log_string: str, func="checkpoint") -> Records:
        log_string = re_get(f"(?<={func} - )"+r"({[^|]*?})(?= | \d{4}-\d{2}-\d{2})", log_string, index=None)
        log_string = self._eval_function(f"[{','.join(log_string)}]")
        log_string = self._eval_datetime(log_string, "datetime")
        log_string = self._eval_datetime(log_string, "date")
        log_string = self._eval_exception(log_string)
        try: return literal_eval(log_string)
        except: return list()

    def _eval_function(self, log_string: str) -> str:
        func_objects = re_get(r"\<[^>]+\>", log_string, index=None)
        if func_objects:
            return replace_map(log_string, **{__o: f"\"{__o}\"" for __o in func_objects})
        else: return log_string

    def _eval_datetime(self, log_string: str, __type: Literal["datetime","date"]="datetime") -> str:
        datetime_objects = re_get(r"datetime.{}\([^)]+\)".format(__type), log_string, index=None)
        if datetime_objects:
            __init = dt.datetime if __type == "datetime" else dt.date
            __format = "%Y-%m-%d" + (" %H:%M:%S" if __type == "datetime" else str())
            get_date_tuple = lambda __o: literal_eval(str(__o).replace('datetime.'+__type,''))
            format_date = lambda __o: __init(*get_date_tuple(__o)).strftime(__format)
            return replace_map(log_string, **{__o: f"\"{format_date(__o)}\"" for __o in datetime_objects})
        else: return log_string

    def _eval_exception(self, log_string: str) -> str:
        exception = re_get(re.compile(r"'(Traceback.*)'}]$", re.DOTALL | re.MULTILINE), log_string)
        if exception:
            return log_string.replace(f"'{exception}'", f"\"\"\"{exception}\"\"\"")
        else: return log_string


###################################################################
############################# Iterator ############################
###################################################################

class Iterator(CustomDict):
    iterateArgs = list()
    iterateProduct = list()
    iterateUnit = 1
    pagination = False
    pageFrom = 1
    offsetFrom = 1
    pageUnit = 0
    pageLimit = 0
    interval = str()

    def __init__(self, iterateUnit: Optional[Unit]=0, interval: Optional[Timedelta]=str()):
        self.set_iterator_unit(iterateUnit, interval)
        super().__init__()

    def set_iterator_unit(self, iterateUnit: Unit=0, interval: Timedelta=str()):
        self.iterateUnit = iterateUnit if iterateUnit else self.iterateUnit
        self.interval = interval if interval else self.interval

    ###################################################################
    ########################### Set Iterator ##########################
    ###################################################################

    def get_iterator(self, _args=True, _page=True, _date=True, _product=True, _params=False, _index=False,
                    keys_only=False, values_only=False, if_null: Literal["drop","pass"]="drop",
                    **context) -> Union[Context,_KT,_VT]:
        iterateArgs = self.iterateArgs * int(_args)
        iteratePage = (PAGE_PARAMS if _params else PAGE_ITERATOR) * int(bool(self.pagination)) * int(_page)
        iterateDate = (DATE_PARAMS if _params else DATE_ITERATOR) * int(bool(self.interval)) * int(_date)
        iterateProduct = self.iterateProduct * int(_product)
        index = [ITER_INDEX] if _index else []
        query = unique(*iterateArgs, *iteratePage, *iterateDate, *iterateProduct, *index)
        if keys_only: return query
        else: return kloc(context, query, if_null=if_null, values_only=values_only)

    @BaseSession.ignore_exception
    def set_iterator(self, *args: Sequence, iterateArgs: List[_KT]=list(), iterateProduct: List[_KT]=list(),
                    iterateUnit: Unit=1, pagination: Pagination=False, interval: Timedelta=str(), indexing=True,
                    **context) -> Tuple[List[Context],Context]:
        arguments, periods, ranges, iterateUnit = list(), list(), list(), cast_list(iterateUnit)
        args_context = self._check_args(*args, iterateArgs=iterateArgs, pagination=pagination)
        if args_context:
            arguments, context = self._from_args(*args, **args_context, iterateUnit=iterateUnit, **context)
            iterateProduct = diff(iterateProduct, iterateArgs, PAGE_ITERATOR)
            if len(iterateProduct) > 1: iterateUnit = iterateUnit[1:]
        if interval:
            periods, context = self._from_date(interval=interval, **context)
            iterateProduct = diff(iterateProduct, DATE_ITERATOR+["date"])
        ranges, context = self._from_context(iterateProduct=iterateProduct, iterateUnit=iterateUnit, **context)
        iterator = self._product_iterator(arguments, periods, ranges, indexing=indexing)
        return iterator, context

    ###################################################################
    ########################## From Arguments #########################
    ###################################################################

    def _check_args(self, *args, iterateArgs: List[_KT], pagination: Pagination=False) -> Context:
        match_query = is_same_length(args, iterateArgs)
        match_args = is_same_length(*args)
        valid_args = (match_query and match_args) or ((not iterateArgs) and pagination)
        valid_pages = isinstance(pagination, bool) or (isinstance(pagination, str) and (pagination in iterateArgs))
        return dict(iterateArgs=iterateArgs, pagination=pagination) if valid_args and valid_pages else dict()

    def _from_args(self, *args: Sequence, iterateArgs: List[_KT], iterateUnit: Unit=1,
                pagination: Pagination=False, **context) -> Tuple[List[Context],Context]:
        if not (is_same_length(*args) or pagination): return list(), context
        argnames, pagenames = self._split_argnames(*args, iterateArgs=iterateArgs, pagination=pagination, **context)
        if pagination:
            how = "numeric" if pagenames == PAGE_ITERATOR else "categorical"
            args, context = self._from_pages(*args, how=how, iterateArgs=iterateArgs, pagination=pagination, **context)
        args = self._product_args(*args)
        if get_scala(iterateUnit) > 1:
            args = list(map(lambda __s: unit_array(__s, unit=get_scala(iterateUnit)), args))
        args = [dict(zip(argnames, values)) for values in zip(*args)]
        return (self._map_pages(*args, keys=pagenames) if pagenames else args), context

    def _split_argnames(self, *args, iterateArgs: List[_KT], pagination: Pagination=False,
                        size: Optional[Unit]=None, **context) -> Tuple[List[_KT],List[_KT]]:
        argnames = iterateArgs.copy()
        if isinstance(pagination, str) and is_str_array(args[iterateArgs.index(pagination)]):
            argnames.pop(iterateArgs.index(pagination))
            return argnames+["pages"], [pagination]+PAGE_ITERATOR+PAGE_PARAMS
        elif pagination:
            return argnames+["pages"], PAGE_ITERATOR
        else: return argnames, list()

    def _product_args(self, *args: Sequence) -> List[List]:
        tuple_idx = list(map(lambda x: allin_instance(x, Tuple, empty=False), args))
        if not any(tuple_idx): return args
        __args = list()
        for __arg in zip(*args):
            tuples, others = iloc(__arg, tuple_idx), iloc(__arg, list(map(lambda x: (not x), tuple_idx)))
            __product = list(product((others,), *tuples))
            __args += [concat_array(__s[1:], __s[0], tuple_idx) for __s in __product]
        return transpose_array(__args, count=len(args))

    ###################################################################
    ############################ From Pages ###########################
    ###################################################################

    def _from_pages(self, *args: Sequence, how: Literal["numeric","categorical"], iterateArgs: List[_KT],
                    pagination: Pagination=False, size: Optional[Unit]=None, **context) -> Tuple[List[List],Context]:
        if how == "numeric":
            if isna(size) and isinstance(pagination, str):
                size = args[iterateArgs.index(pagination)]
            return self._from_numeric_pages(*args, size=size, **context)
        base = list(args)
        labels = cast_list(base.pop(iterateArgs.index(pagination)))
        return self._from_categorical_pages(*base, labels=labels, pagination=pagination, size=size, **context)

    def _from_numeric_pages(self, *args, size: Unit, pageSize=0, pageStart=1, offset=1, **context) -> Tuple[List[List],Context]:
        pageSize = self._validate_page_size(pageSize, self.pageUnit, self.pageLimit)
        pages = self._get_pages(size, pageSize, pageStart, offset, how="all")
        if isinstance(size, int):
            pages = [pages] * len(get_scala(args, default=[0]))
        pages = list(map(lambda __s: tuple(map(tuple, __s)), map(transpose_array, pages)))
        return args+(pages,), dict(context, pageSize=pageSize)

    def _from_categorical_pages(self, *args, labels: List, pagination: str, size: Optional[int]=None,
                                pageSize=0, pageStart=1, offset=1, **context) -> Tuple[List[List],Context]:
        pages = list()
        for label in labels:
            size = self.get_size_by_label(label, size=size, pagination=pagination, **context)
            pageSize = self.get_page_size_by_label(label, pageSize=pageSize, pagination=pagination, **context)
            pageStart = self.get_page_start_by_label(label, pageStart=pageStart, pagination=pagination, **context)
            offset = self.get_offset_by_label(label, offset=offset, pagination=pagination, **context)
            iterator = self._get_pages(size, pageSize, pageStart, offset, how="all")
            num_pages = len(iterator[0])
            params = ((size,)*num_pages, (pageSize,)*num_pages, (pageStart,)*num_pages, (offset,)*num_pages)
            pages.append(tuple(map(tuple, transpose_array(((label,)*num_pages,)+iterator+params))))
        return args+(pages,), context

    def _get_pages(self, size: Unit, pageSize: int, pageStart=1, offset=1, pageUnit=0, pageLimit=0,
                    how: Literal["all","page","start"]="all") -> Union[Pages,List[Pages]]:
        pageSize = self._validate_page_size(pageSize, pageUnit, pageLimit)
        pageStart, offset = self._validate_page_start(size, pageSize, pageStart, offset)
        if isinstance(size, int):
            return self._calc_pages(cast_int1(size), pageSize, pageStart, offset, how)
        elif is_array(size):
            return [self._calc_pages(cast_int1(__sz), pageSize, pageStart, offset, how) for __sz in size]
        else: return tuple()

    def _catch_pagination_error(func):
        @functools.wraps(func)
        def wrapper(self: Iterator, *args, **context):
            try: return func(self, *args, **context)
            except: raise ValueError(PAGINATION_ERROR_MSG)
        return wrapper

    @_catch_pagination_error
    def _validate_page_size(self, pageSize: int, pageUnit=0, pageLimit=0) -> int:
        if (pageUnit > 0) and (pageSize & pageUnit != 0):
            pageSize = ceil(pageSize / pageUnit) * pageUnit
        if pageLimit > 0:
            pageSize = min(pageSize, pageLimit)
        return pageSize

    @_catch_pagination_error
    def _validate_page_start(self, size: int, pageSize: int, pageStart=1, offset=1) -> Tuple[int,int]:
        if pageStart == self.pageFrom:
            pageStart = pageStart + (offset-self.offsetFrom)//pageSize
        if offset == self.offsetFrom:
            offset = offset + (pageStart-self.pageFrom)*size
        return pageStart, offset

    def _calc_pages(self, size: int, pageSize: int, pageStart=1, offset=1,
                    how: Literal["all","page","start"]="all") -> Pages:
        pages = tuple(range(pageStart, (((size-1)//pageSize)+1)+pageStart))
        if how == "page": return pages
        starts = tuple(range(offset, size+offset, pageSize))
        if how == "start": return starts
        size = size + (offset-self.offsetFrom)
        dataSize = tuple(min(size-start+1, pageSize) for start in starts)
        return (pages, starts, dataSize)

    def get_size_by_label(self, label: Any, size: Optional[int]=None, **context) -> int:
        return size

    def get_page_size_by_label(self, label: Any, pageSize=0, **context) -> int:
        return pageSize

    def get_page_start_by_label(self, label: Any, pageStart=1, **context) -> int:
        return pageStart

    def get_offset_by_label(self, label: Any, offset=1, **context) -> int:
        return offset

    def _map_pages(self, *args: Context, keys: List[_KT]) -> List[Context]:
        base = list()
        for __i in range(len(args)):
            pages = args[__i].pop("pages")
            base.append(dict(args[__i], **dict(zip(keys, pages))))
        return base

    ###################################################################
    ############################ From Date ############################
    ###################################################################

    def _from_date(self, startDate: Optional[dt.date]=None, endDate: Optional[dt.date]=None,
                    interval: Timedelta="D", date: _PASS=None, **context) -> Tuple[List[DateQuery],Context]:
        date_range = get_date_range(*get_date_pair(startDate, endDate), interval=interval, paired=True)
        map_pair = lambda pair: dict(startDate=pair[START], endDate=pair[END], date=pair[START])
        return list(map(map_pair, date_range)), dict(context, interval=interval)

    ###################################################################
    ########################### From Context ##########################
    ###################################################################

    def _from_context(self, iterateProduct: List[_KT], iterateUnit: Unit=1, **context) -> Tuple[List[Context],Context]:
        if not iterateProduct: return list(), context
        query, context = kloc(context, iterateProduct, if_null="drop"), drop_dict(context, iterateProduct, inplace=False)
        if not all(query.values()): raise ValueError(EMPTY_CONTEXT_QUERY_MSG)
        if any(map(lambda x: x>1, iterateUnit)): query = self._group_context(iterateProduct, iterateUnit, **query)
        else: query = [dict(zip(query.keys(), values)) for values in product(*map(cast_tuple, query.values()))]
        return query, context

    def _group_context(self, iterateProduct: List[_KT], iterateUnit: Unit=1, **context) -> List[Context]:
        query = apply_dict(context, apply=cast_list, all_keys=True)
        keys, unit = query.keys(), fill_array(iterateUnit, count=len(iterateProduct), value=1)
        combinations = product(*[range(0, len(query[key]), unit[i]) for i, key in enumerate(keys)])
        return [{key: query[key][index:index+unit[i]] for i, (key, index) in enumerate(zip(keys, indices))}
                for indices in combinations]

    def _product_iterator(self, *ranges: Sequence[Context], indexing=True) -> List[Context]:
        if sum(map(len, ranges)) == 0: return list()
        ranges_array = map((lambda x: x if x else [{}]), ranges)
        __indexing = lambda __i: {ITER_INDEX: __i} if indexing else dict()
        return [dict(**__indexing(__i), **chain_dict(query)) for __i, query in enumerate(product(*ranges_array))]


###################################################################
############################# Map Flow ############################
###################################################################

class Process(OptionalDict):
    def __init__(self, name: str, schema: Optional[Schema]=None, root: Optional[_KT]=None, match: Optional[Match]=None):
        super().__init__(name=name, optional=dict(schema=schema, root=root, match=_to_match_func(match)))


class Flow(TypedRecords):
    dtype = Process
    typeCheck = True

    def __init__(self, *args: Union[Process,str]):
        super().__init__(*args)

    def validate_dtype(self, __object: Union[Dict,str]) -> Process:
        if isinstance(__object, self.dtype): return __object
        elif isinstance(__object, str): return self.dtype(name=__object)
        elif isinstance(__object, Dict): return self.dtype(*__object)
        else: self.raise_dtype_error(__object)


###################################################################
############################## Mapper #############################
###################################################################

class Mapper(BaseSession):
    __metaclass__ = ABCMeta
    operation = "mapper"
    pageFrom = 1
    offsetFrom = 1
    responseType = "dict"
    root = list()
    info = Info()
    flow = Flow()

    def map(self, data: ResponseData, flow: Optional[Flow]=None, responseType: Optional[TypeHint]=None,
            root: Optional[_KT]=None, discard=True, updateTime=True, fields: IndexLabel=list(), **context) -> Data:
        data, context = self._init_flow(data, flow, responseType, root, discard, **context)
        data = self._map_response(data, **context)
        if updateTime: data = self._set_update_time_by_interval(data, **context)
        return filter_data(data, fields=fields, if_null="pass")

    def get_root(self, root: Optional[_KT]=None, **context) -> _KT:
        return root if notna(root) else self.root

    def get_flow(self, flow: Optional[Flow]=None, **context) -> Flow:
        if not isinstance(flow, Flow): flow = self.flow.copy()
        for process in flow:
            if SCHEMA not in process:
                process[SCHEMA] = self.info.get(process[NAME])
        return flow

    def _init_flow(self, data: ResponseData, flow: Optional[Flow]=None, responseType: Optional[TypeHint]=None,
                    root: Optional[_KT]=None, discard=True, **context) -> Union[Data,Context]:
        root = self.get_root(root=root, **context)
        if root: data = get_value(data, root)
        flow = self.get_flow(flow=flow, **context)
        self.checkpoint("map"+SUFFIX(context), where="map", msg={"root":root, "data":data, "flow":flow}, save=data)
        return data, dict(context, flow=flow, responseType=responseType, discard=discard)

    def _set_update_time_by_interval(self, __data: Data, date: Optional[dt.date]=None, datetime: Optional[dt.datetime]=None,
                                    interval: Timedelta=str(), **context) -> Data:
        updateDate = date if isinstance(date, dt.date) and is_daily_frequency(interval) else self.today()
        return self.set_update_time(__data, date=updateDate, datetime=datetime)

    ###################################################################
    ########################### Map Response ##########################
    ###################################################################

    def _map_response(self, data: ResponseData, flow: Flow, responseType: Optional[TypeHint]=None,
                    match: Optional[Union[MatchFunction,bool]]=None, discard=True, **context) -> Data:
        responseType = _get_response_type(responseType if responseType else self.responseType)
        __base = _get_response_origin(responseType)
        if not self._match_response(data, match, **context): return __base
        for process in flow:
            if not (isinstance(process, Process) and isinstance(process[SCHEMA], Schema)): continue
            else: __base = self._process(data, __base, process, responseType, __key=process[NAME], **context)
        if discard or isinstance(data, Tag): return self.map_base_data(__base, **context)
        else: return self.map_merged_data(self._merge_base(data, __base), **context)

    def _match_response(self, data: ResponseData,
                    match: Optional[Union[MatchFunction,bool]]=None, **context) -> bool:
        if match is not None:
            if isinstance(match, Callable): return match(data, **context)
            else: return bool(match)
        else: return self.match(data, **context)

    def match(self, data: ResponseData, **context) -> bool:
        return True

    def _process(self, data: ResponseData, __base: Data, process: Process, responseType: Optional[TypeHint]=None,
                **context) -> Data:
        data = get_value(data, process[ROOT]) if ROOT in process else data
        self.checkpoint("schema"+SUFFIX(context), where="map_context", msg={"data":data, "schema":process[SCHEMA]}, save=data)
        if not isinstance(data, _get_response_type(responseType if responseType else self.responseType)):
            if not data: return __base
            else: raise TypeError(INVALID_DATA_TYPE_MSG(data, context))
        data = self._merge_base(data, __base)
        if self._match_data(data, process.get(MATCH), context=context, log=True):
            return self._map_schema(data, __base, process[SCHEMA], **context)
        else: return __base

    def _merge_base(self, data: ResponseData, __base: Data) -> Data:
        if isinstance(data, Dict):
            return chain_dict([__base, data], keep="first")
        elif isinstance(data, pd.DataFrame) and isinstance(__base, pd.DataFrame):
            return concat_df([__base, data], axis=1, keep="first")
        else: return data

    def map_base_data(self, data: Data, **context) -> Data:
        return data

    def map_merged_data(self, data: Data, **context) -> Data:
        return data

    ###################################################################
    ############################ Map Schema ###########################
    ###################################################################

    def _map_schema(self, data: ResponseData, __base: Data, schema: Schema, **context) -> Data:
        for field in schema:
            if not (isinstance(field, Field) and (MODE in field) and (PATH in field)): continue
            elif field[MODE] in (QUERY, INDEX):
                __value = self._get_value(context, **field, context=context, log=True)
                __base[field[NAME]] = __value if __value != __MISMATCH__ else None
                continue
            data = self._merge_base(data, __base)
            try: __base = self._map_field(data, __base, field, **context)
            except Exception as exception:
                self.logger.error(EXCEPTION_MSG(context, field))
                raise exception
        return __base

    ###################################################################
    ############################ Map Field ############################
    ###################################################################

    def _map_field(self, data: ResponseData, __base: Data, field: Field, **context) -> Data:
        path_type = field[HOW] if HOW in field else _get_path_type(field[PATH])
        if path_type in (PATH,CALLABLE):
            __value = self._get_value(data, **field, context=context, log=True)
        elif path_type == VALUE:
            __value = field[PATH]
        elif path_type == TUPLE:
            if isinstance(data, pd.DataFrame): __value = self._get_value_tuple_df(data, **field, context=context)
            else: __value = self._get_value_tuple(data, **field, context=context)
        elif (path_type == ITERATE) and (not isinstance(data, pd.DataFrame)):
            __value = self._get_value_iterate(data, **field, context=context)
        else: raise TypeError(INVALID_PATH_TYPE_MSG(field[PATH]))
        self.checkpoint("field"+SUFFIX(context, field), where="_map_field", msg={"value":__value, "field":field})
        if (__value == __MISMATCH__) or (field[MODE] in (OPTIONAL,REQUIRED) and isna_plus(__value)):
            if field[MODE] == REQUIRED: raise ValueError(REQUIRED_MSG(context, field))
            else: return __base
        __base[field[NAME]] = __value
        return __base

    def _get_value(self, data: ResponseData, path=list(), type: Optional[TypeHint]=None, default=None,
                    apply: Apply=dict(), match: Match=dict(), cast=False, strict=True, sep=str(), strip=True,
                    context: Context=dict(), name=str(), log=False, **field) -> _VT:
        if isinstance(data, pd.DataFrame):
            if match:
                data = data[self._match_data(data, match, context=context, field=field, name=name, log=log)]
                if data.empty: return __MISMATCH__
        elif not self._match_data(data, match, context=context, name=name, log=log): return __MISMATCH__
        default = self._get_value_by_path(data, default, None, sep, strip, context) if notna(default) else None
        __value = self._get_value_by_path(data, path, default, sep, strip, context)
        return self._apply_value(__value, apply, type, default, cast, strict, context, name, log, **field)

    def _get_value_by_path(self, data: ResponseData, path=list(), default=None, sep=str(), strip=True,
                            context: Context=dict()) -> _VT:
        if is_array(path):
            return get_value(data, path, default=default, sep=sep, strip=strip) if path else data
        elif isinstance(path, Callable):
            __apply = safe_apply_df if isinstance(data, PANDAS_DATA) else safe_apply
            return __apply(data, path, default, **context)
        else: return to_series(path, data.index) if isinstance(data, PANDAS_DATA) else path

    def _apply_value(self, __value: _VT, apply: Apply=dict(), type: Optional[TypeHint]=None, default=None,
                    cast=False, strict=True, context: Context=dict(), name=str(), log=False, **field) -> _VT:
        __apply = cast_list(apply, strict=False)
        if cast: __apply += [Cast(type, default, strict)]
        field = dict(field, type=type, default=default, strict=strict)
        return self._apply_data(__value, __apply, context=context, field=field, name=name, log=log)

    def _get_value_tuple(self, data: ResponseData, path: Tuple, apply: Apply=dict(), match: Match=dict(),
                        context: Context=dict(), name=str(), **field) -> _VT:
        __match = int(self._match_data(data, match, context=context, name=name, log=True))-1
        __apply = get_scala(apply, index=__match, default=dict())
        return self._get_value(data, path[__match], apply=__apply, context=context, name=name, log=True, **field)

    def _get_value_tuple_df(self, data: pd.DataFrame, path: Tuple, apply: Apply=dict(), match: Match=dict(),
                            context: Context=dict(), name=str(), **field) -> pd.Series:
        __match = self._match_data(data, match, context=context, name=name, log=True)
        df_true, df_false = data[__match], data[~__match]
        apply_true, apply_false = get_scala(apply, index=0, default=dict()), get_scala(apply, index=-1, default=dict())
        df_true = self._get_value(df_true, path[0], apply=apply_true, context=context, name=name, log=True, **field)
        df_false = self._get_value(df_false, path[1], apply=apply_false, context=context, name=name, log=False, **field)
        try: return concat_df([df_true, df_false]).sort_index()
        except: return __MISMATCH__

    def _get_value_iterate(self, data: ResponseData, path: Sequence, apply: Apply=dict(), match: Match=dict(),
                            context: Context=dict(), name=str(), **field) -> _VT:
        __value = get_value(data, path[:-1])
        if not isinstance(__value, Sequence):
            raise TypeError(INVALID_VALUE_TYPE_MSG(__value, context, name=name))
        sub_path = path[-1] if (len(path) > 0) and is_array(path[-1]) else __value
        log_context = dict(context=context, name=name, log=False)
        return [self._get_value(__e, sub_path, apply=apply, **field, **log_context)
                for __e in __value if self._match_data(__e, match, field=field, **log_context)]

    ###################################################################
    ############################ Apply Data ###########################
    ###################################################################

    def _validate_apply(func):
        @functools.wraps(func)
        def wrapper(self: Mapper, data: ResponseData, apply: Union[Apply,Sequence[Apply]], context: Context=dict(),
                    field: Optional[Field]=dict(), name: Optional[str]=str(), log=False):
            if not (isinstance(apply, (Dict,List,Tuple)) and apply): return data
            log_context = dict(context=context, field=field, name=name, log=log)
            self._log_origin(data, apply, point="apply", where=func.__name__, **log_context)
            apply, __result = cast_list(apply), data
            for __apply in apply:
                __result = func(self, data=__result, apply=__apply, **log_context)
            self._log_result(__result, apply, point="apply", where=func.__name__, **log_context)
            return __result
        return wrapper

    @_validate_apply
    def _apply_data(self, data: ResponseData, apply: Union[Apply,Sequence[Apply]], context: Context=dict(),
                    field: Optional[Field]=dict(), name: Optional[str]=str(), log=False) -> _VT:
        if isinstance(apply[FUNC], Callable):
            __apply = safe_apply_df if isinstance(data, PANDAS_DATA) else safe_apply
            __applyFunc, apply = apply[FUNC], drop_dict(apply, FUNC, inplace=False)
            return __apply(data, __applyFunc, **dict(context, **apply))
        elif isinstance(apply[FUNC], str):
            return self._special_apply(data, **apply, context=context, field=field, name=name)
        else: raise TypeError(INVALID_APPLY_TYPE_MSG(apply[FUNC], context, field, name))

    def _special_apply(self, __object, func: str, context: Context=dict(),
                        field: Optional[Field]=dict(), name: Optional[str]=str(), **kwargs) -> _VT:
        if func == __CAST__: return self.__cast__(__object, **kwargs, context=context)
        elif func == __EXISTS__: return self.__exists__(__object, **kwargs)
        elif func == __JOIN__: return self.__join__(__object, **kwargs)
        elif func == __REGEX__: return self.__regex__(__object, **kwargs)
        elif func == __RENAME__: return self.__rename__(__object, **kwargs)
        elif func == __SPLIT__: return self.__split__(__object, **kwargs)
        elif func == __STAT__: return self.__stat__(__object, **kwargs)
        elif func == __SUM__: return self.__stat__(__object, **kwargs)
        elif func == __MAP__:
            field = dict(type=field[TYPE], name=name)
            return self.__map__(__object, **dict(field, **kwargs), context=context)
        else: raise ValueError(INVALID_APPLY_SPECIAL_MSG(func, context, field, name))

    def __cast__(self, __object, type: TypeHint, default=None, strict=True,
                context: Context=dict(), **kwargs) -> _VT:
        context = dict(context, default=default, strict=strict)
        if isinstance(__object, PANDAS_DATA):
            return safe_apply_df(__object, cast_object, by="cell", **context)
        elif isinstance(__object, List):
            return [cast_object(__e, type, **context) for __e in __object]
        else: return cast_object(__object, type, **context)

    def __exists__(self, __object, keys: _KT=list(), default=None, hier=False, **kwargs) -> Any:
        if keys: __object = filter_data(__object, keys, if_null="drop", values_only=True, hier=hier)
        return exists_one(*__object, default) if is_array(__object) else default

    def __join__(self, __object, keys: _KT=list(), sep=',', default=None, hier=False,
                strip=True, split: Optional[str]=None, **kwargs) -> str:
        if keys: __object = filter_data(__object, keys, if_null="drop", values_only=True, hier=hier)
        if split: __object = cast_str(__object).split(split)
        if not is_array(__object): return default
        __object = [__s for __s in [cast_str(__e, strict=True, strip=strip) for __e in __object] if __s]
        return sep.join(__object) if __object else default

    def __regex__(self, __object, pattern: RegexFormat, default=None, index: Optional[int]=0,
                    repl: Optional[str]=None, strip=False, **kwargs) -> str:
        __object = cast_str(__object, strict=True)
        __pattern = pattern if isinstance(pattern, re.Pattern) else re.compile(pattern)
        if isinstance(repl, str): __object = __pattern.sub(repl, __object)
        else: return re_get(pattern, __object, default=default, index=index)
        return __object.strip() if strip else __object

    def __rename__(self, __object, rename: RenameMap, path: _KT=list(),
                    if_null: Union[Literal["null","pass","error"],Any]="null", **kwargs) -> str:
        if if_null == "null": value = rename.get(__object)
        elif if_null == "pass": value = rename.get(__object, __object)
        elif if_null == "error": value = rename[__object]
        else: value = rename.get(__object, if_null)
        return hier_get(value, path) if path else value

    def __split__(self, __object, sep=',', maxsplit=-1, default=None, strict=True, index: Optional[int]=None,
                    type: Optional[TypeHint]=None, **kwargs) -> Union[List,_VT]:
        __object = cast_str(__object, strict=True).split(sep, maxsplit)
        if type: __object = [self.__cast__(__e, type, default, strict) for __e in __object]
        return get_scala(__object, index) if isinstance(index, int) else __object

    def __stat__(self, __object, stat: Callable, keys: _KT=list(), default=None, hier=False,
                type: Optional[TypeHint]=None, strict=True, **kwargs) -> Union[Any,int,float]:
        if keys: __object = filter_data(__object, keys, if_null="drop", values_only=True, hier=hier)
        if not is_array(__object): return default
        elif is_numeric_type(type):
            __cast = cast_float if is_float_type(type) else cast_int
            __object = [__cast(__e, strict=strict) for __e in __object]
        else: __object = [__n for __n in __object if isinstance(__n, (float,int))]
        return stat(__object) if __object else default

    def __map__(self, __object, schema: Schema, type: TypeHint, root: Optional[_KT]=None,
                match: Optional[Match]=None, context: Context=dict(), name=str(), **kwargs) -> Data:
        flow = Flow(Process(name=name, schema=schema, root=root, match=match))
        context.pop(SCHEMA_KEY, None)
        return self.map(__object, flow, responseType=type, **dict(context, discard=True, updateTime=False))

    ###################################################################
    ############################ Match Data ###########################
    ###################################################################

    def _validate_match(func):
        @functools.wraps(func)
        def wrapper(self: Mapper, data: ResponseData, match: Match, context: Context=dict(),
                    field: Optional[Field]=dict(), name: Optional[str]=str(), log=False):
            if not (isinstance(match, Dict) and match): return True
            elif (MATCH_QUERY not in match) and (PATH not in match) and (FUNC not in match): return True
            log_context = dict(context=context, field=field, name=name, log=log)
            __match = func(self, data, match=match, **log_context)
            self._log_result(__match, match, point="match", where=func.__name__, **log_context)
            return __match
        return wrapper

    @_validate_match
    def _match_data(self, data: ResponseData, match: Match, context: Context=dict(),
                    field: Optional[Field]=dict(), name: Optional[str]=str(), log=False) -> Union[bool,pd.Series]:
        log_context = dict(context=context, field=field, name=name, log=log)
        if MATCH_QUERY in match: return self._match_query(**log_context, **match)
        elif FUNC in match: return self._match_function(data, **match, **log_context)
        elif (EXACT in match) or (INCLUDE in match) or (EXCLUDE in match):
            msg = kloc(match, [EXACT, INCLUDE, EXCLUDE], if_null="drop")
            self._log_origin(data, match, point="match", where="isin_data", msg=msg, **log_context)
            return _toggle(isin_data(data, **match), flip=match.get(FLIP, False))
        else: return self._match_value(data, **match, **log_context)

    def _match_query(self, context: Context, query: _KT, field: Optional[Field]=dict(),
                    name: Optional[str]=str(), log=False, **match) -> bool:
        match = dict(match, path=query)
        if log:
            __value = filter_data(context, query, hier=match.get("hier", True))
            self._log_origin(__value, match, point="match", where="match_query", context=context, field=field, name=name, log=log)
        return self._match_data(context, match, context=context, field=field, name=name, log=False)

    def _match_function(self, data: ResponseData, func: Callable, path: Optional[_KT]=None, default=False,
                        flip=False, hier=True, context: Context=dict(), field: Optional[Field]=dict(),
                        name: Optional[str]=str(), log=False, **kwargs) -> Union[bool,pd.Series]:
        if not isinstance(func, Callable): return default
        if notna(path): data = filter_data(data, path, hier=hier)
        if log:
            match = dict(func=func, path=path, default=default, flip=flip, hier=hier)
            self._log_origin(data, match, point="match", where="match_function", context=context, field=field, name=name, log=True)
        __apply = safe_apply_df if isinstance(data, PANDAS_DATA) else safe_apply
        return _toggle(__apply(data, func, default, **context), flip=flip)

    def _match_value(self, data: ResponseData, path: _KT, value: Optional[Any]=None, flip=False, strict=False,
                    how: Literal["any","all"]="any", if_null=False, hier=True, context: Context=dict(),
                    field: Optional[Field]=dict(), name: Optional[str]=str(), log=False, **kwargs) -> Union[bool,pd.Series]:
        if not _is_single_path_by_data(data, path, hier=hier):
            args = (value, flip, strict, how, if_null, hier)
            return howin([self._match_value(data, __k, args, log=log, **kwargs) for __k in path], how=how)
        __value = get_value(data, path)
        if log:
            match = dict(path=path, flip=flip, strict=strict, how=how, if_null=if_null, hier=hier)
            self._log_origin(__value, match, point="match", where="match_value", context=context, field=field, name=name, log=True)
        if isna(__value): return _toggle(if_null, flip=flip)
        elif notna(value): return _toggle((__value == value), flip=flip)
        elif isinstance(data, pd.DataFrame):
            return _toggle(match_df(data, match=(lambda x: exists(x, strict=strict)), all_cols=True), flip=flip)
        else: return _toggle(exists(__value, strict=strict), flip=flip)

    def _log_origin(self, __value: _VT, __object: Any, point: str, where: str, msg=dict(), context: Context=dict(),
                    field: Optional[Field]=dict(), name: Optional[str]=str(), log=False, **kwargs):
        if not log: return
        msg = dict({"value":__value, point:__object}, **msg)
        self.checkpoint(f"[origin]_{point}"+SUFFIX(context, field, name), where=where, msg=msg)

    def _log_result(self, __result: _VT, __object: Any, point: str, where: str, msg=dict(), context: Context=dict(),
                    field: Optional[Field]=dict(), name: Optional[str]=str(), log=False, **kwargs):
        if not log: return
        msg = dict({"result":__result, point:__object}, **msg)
        self.checkpoint(point+SUFFIX(context, field, name), where=where, msg=msg)


def _get_response_type(responseType: TypeHint) -> Type:
    if isinstance(responseType, Type): return responseType
    elif is_dict_type(responseType): return dict
    elif is_dataframe_type(responseType): return pd.DataFrame
    elif is_tag_type(responseType): return Tag
    else: return dict


def _get_response_origin(responseType: TypeHint) -> Data:
    if not isinstance(responseType, Type):
        responseType = _get_response_type(responseType)
    elif responseType == pd.DataFrame: return pd.DataFrame()
    else: return dict()


def _toggle(__bool: Union[bool,PandasData], flip=False) -> Union[bool,pd.Series]:
    if isinstance(__bool, PANDAS_DATA):
        if isinstance(__bool, pd.DataFrame): __bool = __bool.any(axis=1)
        return (~__bool) if flip else __bool
    return (not __bool) if flip else bool(__bool)


def _is_single_path_by_data(data: ResponseData, path: _KT, hier=False) -> bool:
    if not path: return True
    elif isinstance(data, Dict): return is_single_path(path, hier=hier)
    elif isinstance(data, pd.DataFrame): return not (is_array(path) and (len(path) > 1))
    elif isinstance(data, Tag): return is_single_selector(path, hier=hier)
    else: True


###################################################################
######################### Sequence Mapper #########################
###################################################################

class SequenceMapper(Mapper):
    __metaclass__ = ABCMeta
    operation = "mapper"
    pageFrom = 1
    offsetFrom = 1
    responseType = "records"
    root = list()
    groupby = list()
    groupSize = dict()
    countby = str()
    info = Info()
    flow = Flow()

    def map(self, data: ResponseData, flow: Optional[Flow]=None, responseType: Optional[TypeHint]=None,
            root: Optional[_KT]=None, groupby: Optional[Union[Dict[_KT,_KT],_KT]]=None, groupSize: Optional[NestedDict]=None,
            countby: Optional[Literal["page","start"]]=None, discard=True, updateTime=True,
            fields: IndexLabel=list(), **context) -> Data:
        data, context = self._init_flow(data, flow, responseType, root, discard, **context)
        if isinstance(data, (Sequence,pd.DataFrame)):
            context = self._get_sequence_context(groupby, groupSize, countby, **context)
            data = self._map_sequence(data, **context)
        else: data = self._map_response(data, **context)
        if updateTime: data = self._set_update_time_by_interval(data, **context)
        return filter_data(data, fields=fields, if_null="pass")

    def get_groupby(self, groupby: Optional[Union[Dict[_KT,_KT],_KT]]=None, **context) -> Union[Dict[_KT,_KT],_KT]:
        return groupby if notna(groupby) else self.groupby

    def get_group_size(self, groupSize: Optional[NestedDict]=None, **context) -> NestedDict:
        return groupSize if notna(groupSize) else self.groupSize

    def get_countby(self, countby: Optional[Literal["page","start"]]=None, **context) -> str:
        return countby if notna(countby) else self.countby

    def _get_sequence_context(self, groupby: Optional[Union[Dict[_KT,_KT],_KT]]=None, groupSize: Optional[NestedDict]=None,
                            countby: Optional[Literal["page","start"]]=None, **context) -> Context:
        groupby = dict(groupby=self.get_groupby(groupby=groupby, **context))
        groupSize = dict(groupSize=self.get_group_size(groupSize=groupSize, **context))
        countby = dict(countby=self.get_countby(countby=countby, **context))
        return dict(context, **groupby, **groupSize, **countby)

    ###################################################################
    ########################### Map Sequence ##########################
    ###################################################################

    def _map_sequence(self, data: ResponseData, flow: Flow, responseType: Optional[TypeHint]=None,
                    groupby: Union[Dict[_KT,_KT],_KT]=list(), groupSize: NestedDict=dict(),
                    countby: Optional[Literal["page","start"]]=None,
                    match: Optional[Union[MatchFunction,bool]]=None, discard=True, **context) -> Data:
        context = dict(context, flow=flow, countby=countby, match=match, discard=discard)
        responseType = _get_response_type(responseType if responseType else self.responseType)
        if groupby: return self._groupby_data(data, groupby, groupSize, responseType=responseType, **context)
        elif is_records_type(responseType): return self._map_records(data, **context)
        elif is_dataframe_type(responseType): return self._map_dataframe(data, **context)
        elif is_tag_type(responseType): return self._map_tag_list(data, **context)
        else: return self._map_records(data, **context)

    def _groupby_data(self, data: ResponseData, groupby: Union[Dict[_KT,_KT],_KT], groupSize: NestedDict=dict(),
                    if_null: Literal["drop","pass"]="drop", hier=False, **context) -> Data:
        groups = groupby_data(data, by=groupby, if_null=if_null, hier=hier)
        log_msg = {"groups":list(groups.keys()), "groupSize":[safe_len(group) for group in groups.values()]}
        self.checkpoint("group"+SUFFIX(context), where="groupby_data", msg=log_msg)
        results = [self._map_sequence(__data, **dict(context, group=group, dataSize=groupSize.get(group)))
                    for group, __data in groups.items()]
        return chain_exists(results)

    ###################################################################
    ######################## Map Sequence Data ########################
    ###################################################################

    def _map_records(self, __r: Records, flow: Flow, countby: Optional[Literal["page","start"]]=None,
                    match: Optional[Union[MatchFunction,bool]]=None, discard=True, **context) -> Records:
        data = list()
        __r = self._limit_data_size(__r, **context)
        start = self._get_start_by(countby, count=len(__r), **context)
        for __i, __m in enumerate(__r, start=(start if start else 0)):
            if not self._match_response(__m, match, **context): continue
            __i = __m[RANK] if isinstance(__m.get(RANK), int) else __i
            kwargs = dict(context, discard=discard, count=len(__r), __i=__i)
            data.append(self._map_response(__m, flow, responseType="dict", match=True, **kwargs))
        return data

    def _map_dataframe(self, df: pd.DataFrame, flow: Flow, countby: Optional[Literal["page","start"]]=None,
                    match: Optional[Union[MatchFunction,bool]]=None, discard=True, **context) -> pd.DataFrame:
        df = self._limit_data_size(df, **context)
        start = self._get_start_by(countby, count=len(df), **context)
        if isinstance(start, int) and (RANK not in df):
            df[RANK] = range(start, len(df)+start)
        df = df[self._match_response(df, match, **context)]
        context = dict(context, responseType="dataframe", match=True, discard=discard, count=len(df))
        return self._map_response(df, flow, **context)

    def _map_tag_list(self, tag_list: Sequence[Tag], flow: Flow, countby: Optional[Literal["page","start"]]=None,
                    match: Optional[Union[MatchFunction,bool]]=None, discard=True, **context) -> Records:
        data = list()
        tag_list = self._limit_data_size(tag_list, **context)
        start = self._get_start_by(countby, count=len(tag_list), **context)
        for __i, __s in enumerate(tag_list, start=(start if start else 0)):
            if not (isinstance(__s, Tag) and self._match_response(__s, match, **context)): continue
            kwargs = dict(context, count=len(tag_list), __i=__i)
            data.append(self._map_response(__s, flow, responseType="tag", match=True, **kwargs))
        return data

    def _limit_data_size(self, data: Data, size: Optional[int]=None, dataSize: Optional[int]=None, **context) -> Data:
        if isinstance(dataSize, int): return data[:dataSize]
        elif isinstance(size, int) and (size < len(data)): return data[:size]
        else: return data

    def _get_start_by(self, by: Optional[Literal["page","start"]]=None, count: Optional[int]=0,
                    page=1, start=1, dataSize: Optional[int]=None, **context) -> int:
        if (by == "page") and isinstance(page, int):
            dataSize = dataSize if isinstance(dataSize, int) else count
            return (page if self.pageFrom == 0 else page-1)*dataSize+1
        elif by == "start" and isinstance(start, int):
            return start+1 if self.offsetFrom == 0 else start
        else: return None

    ###################################################################
    ############################ Apply Data ###########################
    ###################################################################

    def __map__(self, __object, schema: Schema, type: TypeHint, root: Optional[_KT]=None,
                match: Optional[Match]=None, groupby: _KT=list(), groupSize: NestedDict=dict(),
                countby: Optional[Literal["page","start"]]="start", page=1, start=1,
                submatch: Optional[Union[MatchFunction,bool]]=True, discard=True,
                context: Context=dict(), name=str(), **kwargs) -> Data:
        flow = Flow(Process(name=name, schema=schema, root=root, match=match))
        context = dict(context, groupby=groupby, groupSize=groupSize, countby=countby,
                        page=page, start=start, match=submatch, discard=discard)
        context.pop(SCHEMA_KEY, None)
        context.pop(COUNT, None)
        context[ITER_INDEX] = f"{context.get(ITER_INDEX,0)}-{context.pop(COUNT_INDEX,0)}"
        return self.map(__object, flow, responseType=type, **dict(context, discard=True, updateTime=False))


###################################################################
############################## Parser #############################
###################################################################

class Parser(SequenceMapper):
    __metaclass__ = ABCMeta
    operation = "parser"
    fields = list()
    iterateCount = dict()
    pageFrom = 1
    offsetFrom = 1
    responseType = "records"
    root = list()
    groupby = list()
    groupSize = dict()
    countby = str()
    info = Info()
    flow = Flow()

    ###################################################################
    ######################### Parse Response #########################
    ###################################################################

    def validate_response(func):
        @functools.wraps(func)
        def wrapper(self: Parser, response: Any, *args, countPath: Optional[_KT]=None, returnPath: Optional[_KT]=None, **context):
            is_valid = self.is_valid_response(response)
            if notna(countPath) and (ITER_INDEX in context):
                self.iterateCount[context[ITER_INDEX]] = cast_int(get_value(response, countPath))
            if notna(returnPath):
                return get_value(response, returnPath)
            data = func(self, response, *args, **context) if is_valid else init_origin(func)
            self.checkpoint(f"parse"+ITER_SUFFIX(context), where=func.__name__, msg={"data":data}, save=data)
            self.log_results(data, **context)
            return data
        return wrapper

    def is_valid_response(self, response: Any) -> bool:
        return True

    def log_results(self, data: Data, **context):
        self.logger.info(log_data(data, **context))

    @validate_response
    def parse(self, response: Any, countPath: Optional[_KT]=None, returnPath: Optional[_KT]=None, **context) -> Data:
        return self.map(response, **context)
