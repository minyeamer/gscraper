from __future__ import annotations
from gscraper.base.session import TypedDict, TypedRecords, BaseSession, ITER_INDEX, ITER_SUFFIX, to_default

from gscraper.base.types import _KT, _VT, Context, LogLevel, TypeHint, TypeList, PANDAS_DATA
from gscraper.base.types import IndexLabel, Timedelta, RenameMap, RegexFormat, ResponseData, PandasData
from gscraper.base.types import Records, NestedDict, Data, JsonData, ApplyFunction, MatchFunction
from gscraper.base.types import get_type, init_origin, is_type, is_float_type
from gscraper.base.types import is_numeric_type, is_numeric_or_date_type, is_bool_type
from gscraper.base.types import is_array, is_records, is_dict_type, is_records_type, is_dataframe_type, is_tag_type

from gscraper.utils import isna, notna, exists
from gscraper.utils.cast import cast_object, cast_str, cast_list, cast_tuple, cast_float, cast_int
from gscraper.utils.date import is_daily_frequency
from gscraper.utils.logs import log_data
from gscraper.utils.map import isna_plus, exists_one, df_exists, howin, safe_apply, safe_len, get_scala, re_get, union
from gscraper.utils.map import kloc, vloc, is_single_path, chain_dict, drop_dict, hier_get, drop_duplicates
from gscraper.utils.map import concat_df, safe_apply_df, match_df, to_series
from gscraper.utils.map import get_value, filter_data, set_data, isin_data, chain_exists, groupby_data, is_single_selector

from abc import ABCMeta
from ast import literal_eval
import functools

from typing import Any, Dict, Callable, List, Literal, Optional, Sequence, Tuple, Type, Union
from bs4.element import Tag
import datetime as dt
import json
import pandas as pd
import re


SCHEMA = "schema"
ROOT = "root"
MATCH = "match"
RANK = "rank"
SCHEMA_KEY = "__key"
SCHEMA_KEYS = [SCHEMA, ROOT, MATCH]

NAME = "name"
PATH = "path"
TYPE = "type"
MODE = "mode"
DESC = "description"
CAST = "cast"
STRICT = "strict"
DEFAULT = "default"
APPLY = "apply"
MATCH = "match"
MATCH_QUERY = "query"

HOW = "how"
VALUE = "value"
TUPLE = "tuple"
ITERATE = "iterate"
CALLABLE = "callable"

QUERY = "QUERY"
INDEX = "INDEX"
LABEL = "LABEL"
NULLABLE = "NULLABLE"
NOTNULL = "NOTNULL"
NOTZERO = "NOTZERO"
REQUIRED = "REQUIRED"
OPTIONAL = "OPTIONAL"

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

EXACT = "exact"
INCLUDE = "include"
EXCLUDE = "exclude"
FLIP = "flip"

COUNT = "count"
COUNT_INDEX = "__i"
_NESTED_SUFFIX = lambda context: \
    f"[{context[COUNT_INDEX]}]" if (ITER_INDEX in context) and isinstance(context[ITER_INDEX], str) else f"-{context[COUNT_INDEX]}"
_COUNT_SUFFIX = lambda context: _NESTED_SUFFIX(context) if COUNT_INDEX in context else str()
_SCHEMA_SUFFIX = lambda context: f"_{context[SCHEMA_KEY]}" if SCHEMA_KEY in context else str()
_FIELD_SUFFIX = lambda field: f"_{field[NAME]}" if NAME in field else str()
_NAME_SUFFIX = lambda name: f"_{name}" if name else str()
SUFFIX = lambda context, field=dict(), name=str(): \
    _SCHEMA_SUFFIX(context) + _FIELD_SUFFIX(field) + _NAME_SUFFIX(name) + ITER_SUFFIX(context) + _COUNT_SUFFIX(context)


###################################################################
############################# Messages ############################
###################################################################

INVALID_OBJECT_MSG = lambda __object, __name: f"'{__object}' is not a valid {__name} object."
INVALID_OBJECT_TYPE_MSG = lambda __object, __type: f"'{type(__object)}' is not a valid type for {__type} object."

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

INFO_OBJECT = "SchemaInfo"
CONTEXT_OBJECT = "SchemaContext"
SCHEMA_OBJECT = "Schema"
FIELD_OBJECT = "SchemaField"
APPLY_FUNCTION = "ApplyFunction"
MATCH_FUNCTION = "MatchFunction"


###################################################################
############################### Json ##############################
###################################################################

class LazyDecoder(json.JSONDecoder):
    def decode(s, **kwargs):
        regex_replacements = [
            (re.compile(r'([^\\])\\([^\\])'), r'\1\\\\\2'),
            (re.compile(r',(\s*])'), r'\1'),
        ]
        for regex, replacement in regex_replacements:
            s = regex.sub(replacement, s)
        return super().decode(s, **kwargs)


def validate_json(data: JsonData, __path: IndexLabel, default=dict()) -> JsonData:
    __m = data.copy()
    try:
        for key in __path:
            __m = __m[key]
            if isinstance(__m, str):
                try: __m = json.loads(__m)
                except json.JSONDecodeError: return json.loads(__m, cls=LazyDecoder)
        return __m
    except: return default


def parse_invalid_json(raw_json: str, key: str, value_type: Literal["any","dict"]="dict") -> JsonData:
    rep_bool = lambda s: str(s).replace("null","None").replace("true","True").replace("false","False")
    try:
        if value_type == "dict" and re.search("\""+key+"\":\{[^\}]*\}+",raw_json):
            return literal_eval(rep_bool("{"+re.search("\""+key+"\":\{[^\}]*\}+",raw_json).group()+"}"))
        elif value_type == "any" and re.search(f"(?<=\"{key}\":)"+"([^,}])+(?=[,}])",raw_json):
            return literal_eval(rep_bool(re.search(f"(?<=\"{key}\":)"+"([^,}])+(?=[,}])",raw_json).group()))
        else: return
    except: return dict() if value_type == "dict" else None


###################################################################
########################### Schema Apply ##########################
###################################################################

class Apply(TypedDict):
    def __init__(self, func: Union[ApplyFunction, str], default: Optional[Any]=None, **context):
        self.validate(func)
        super().__init__(func=func)
        self.update_notna(default=default)
        self.update(context)

    def validate(self, func: Union[ApplyFunction, str]):
        if not isinstance(func, (Callable,str)):
            raise TypeError(INVALID_OBJECT_TYPE_MSG(func, APPLY_FUNCTION))


class Match(TypedDict):
    def __init__(self, func: Optional[MatchFunction]=None, path: Optional[Union[_KT,Tuple[_KT]]]=None,
                query: Optional[Union[_KT,Tuple[_KT]]]=None, value: Optional[Any]=None,
                exact: Optional[_VT]=None, include: Optional[_VT]=None, exclude: Optional[_VT]=None,
                flip=False, strict=False, how: Literal["any","all"]="any", if_null=False,
                hier=True, default=False, **context):
        self.validate(func, path, query, value)
        super().__init__()
        self.update_default(dict(flip=False, strict=False, how="any", if_null=False, hier=True, default=False),
            func=func, path=path, query=query, value=value, exact=exact, include=include, exclude=exclude,
            flip=flip, strict=strict, how=how, if_null=if_null, hier=hier, default=default)
        self.update(context)

    def validate(self, func: Optional[MatchFunction]=None, path: Optional[_KT]=None,
                query: Optional[_KT]=None, value: Optional[Any]=None):
        if isna(func) and isna(path) and isna(query):
            raise ValueError(INVALID_MATCH_KEY_MSG)
        elif notna(func) and not isinstance(func, Callable):
            raise TypeError(INVALID_OBJECT_TYPE_MSG(func, MATCH_FUNCTION))
        else: pass


class Cast(Apply):
    def __init__(self, type: TypeHint, default: Optional[Any]=None, strict=True, **context):
        super().__init__(func=__CAST__, type=type)
        self.update_default(dict(strict=True), default=default, strict=strict, **context)


class Exists(Apply):
    def __init__(self, keys: _KT=list(), default: Optional[Any]=None, hier=False):
        super().__init__(func=__EXISTS__, keys=keys)
        self.update_default(dict(hier=False), default=default, hier=hier)


class Join(Apply):
    def __init__(self, keys: _KT=list(), sep=',', default: Optional[Any]=None,
                hier=False, strip=True, split: Optional[str]=None):
        super().__init__(func=__JOIN__, keys=keys, sep=sep)
        self.update_default(dict(hier=False, strip=True),
            default=default, hier=hier, strip=strip, split=split)


class Regex(Apply):
    def __init__(self, pattern: RegexFormat, how: Literal["search","findall","sub"]="search",
                default=None, index: Optional[int]=0, repl: Optional[str]=None):
        if notna(repl): how = "sub"
        elif not isinstance(index, int): how = "findall"
        super().__init__(func=__REGEX__, pattern=pattern, how=how)
        self.update_default(dict(index=0), default=default, index=index, repl=repl)


class Rename(Apply):
    def __init__(self, rename: RenameMap, path: Optional[_KT]=None,
                if_null: Union[Literal["null","pass","error"],Any]="null"):
        super().__init__(func=__RENAME__, rename=rename)
        self.update_default(dict(if_null="null"), path=path, if_null=if_null)


class Split(Apply):
    def __init__(self, sep=',', maxsplit=-1, default: Optional[Any]=None,
                strict=True, index: Optional[int]=None, type: Optional[TypeHint]=None):
        super().__init__(func=__SPLIT__, sep=sep)
        self.update_default(dict(maxsplit=-1, strict=True),
            maxsplit=maxsplit, default=default, strict=strict, index=index, type=type)


class Stat(Apply):
    def __init__(self, stat: Callable, keys: _KT=list(), default: Optional[Any]=None,
                hier=False, type: Optional[TypeHint]=None, strict=True):
        super().__init__(func=__STAT__, stat=stat, keys=keys)
        self.update_default(dict(hier=False, strict=True),
            default=default, hier=hier, type=type, strict=strict)


class Sum(Stat):
    def __init__(self, keys: _KT=list(), default: Optional[Any]=None,
                hier=False, type: Optional[TypeHint]=None, strict=True):
        super().__init__(stat=sum, keys=keys, default=default, hier=hier, type=type, strict=strict)


class Map(Apply):
    def __init__(self, schema: Schema, type: Optional[TypeHint]=None, root: Optional[_KT]=None,
                match: Optional[Match]=None, groupby: Optional[_KT]=None, groupSize: Optional[NestedDict]=None,
                countby: Literal["page","start"]="start", page=1, start=1,
                submatch: Optional[Union[MatchFunction,bool]]=True, discard=True) -> Data:
        super().__init__(func=__MAP__, schema=schema)
        self.update_default(dict(countby="start", page=1, start=1, submatch=True, discard=True),
            type=type, root=root, match=match, groupby=groupby, groupSize=groupSize,
            countby=countby, page=page, start=start, submatch=submatch, discard=discard)


###################################################################
########################### Schema Field ##########################
###################################################################

SchemaPath = Union[_KT, _VT, Tuple[_KT,_KT], Tuple[_VT,_VT], Callable]
SchemaMode = Literal["QUERY", "INDEX", "LABEL", "NULLABLE", "NOTNULL", "NOTZERO", "REQUIRED", "OPTIONAL"]
PathType = Literal["path", "value", "tuple", "iterate", "callable", "global"]

class Field(TypedDict):
    def __init__(self, name: _KT, path: SchemaPath, type: TypeHint, mode: SchemaMode, desc: Optional[str]=None,
                default: Optional[Any]=None, cast=False, strict=True, apply: Optional[Apply]=None,
                match: Optional[Match]=None, how: Optional[PathType]=None, description: Optional[str]=None):
        super().__init__(name=name, path=path, type=type, mode=mode)
        self.update_default(dict(cast=False, strict=True),
            description=(desc if desc else description), cast=cast, strict=strict, default=default,
            apply=to_apply_func(apply), match=to_match_func(match), how=(how if how else get_path_type(path)))
        self.init_field()

    def init_field(self):
        self.update(type=get_type(self[TYPE], argidx=-1))
        if self[MODE] in (QUERY,LABEL): return
        elif self[MODE] == INDEX:
            self.update(path=[COUNT_INDEX])
        elif self[MODE] == NOTNULL:
            self.update(default=init_origin(self[TYPE]), cast=True)
        elif self[MODE] == NOTZERO:
            self.update(cast=True, strict=False)
        elif is_numeric_or_date_type(self[TYPE]) and (CAST not in self):
            self.update(cast=True)
            if is_bool_type(self[TYPE]):
                self.update(strict=False)
        else: return


def validate_field(field: Any) -> Field:
    if isinstance(field, Field): return field
    elif isinstance(field, Dict): return Field(**field)
    else: raise TypeError(INVALID_OBJECT_TYPE_MSG(field, FIELD_OBJECT))


def get_path_type(path: SchemaPath) -> PathType:
    if isinstance(path, str): return VALUE
    elif isinstance(path, Sequence):
        if isinstance(path, Tuple) and len(path) == 2: return TUPLE
        elif len(path) > 0 and is_array(path[-1]): return ITERATE
        else: return PATH
    elif isinstance(path, Callable): return CALLABLE
    else: raise TypeError(INVALID_PATH_TYPE_MSG(path))


def to_apply_func(apply: Any) -> Apply:
    if isinstance(apply, Apply) or (not apply): return apply
    elif isinstance(apply, (Callable,str)): return Apply(func=apply)
    elif isinstance(apply, Dict): return Apply(**apply)
    elif isinstance(apply, Sequence):
        return [to_apply_func(func) for func in apply]
    else: raise TypeError(INVALID_OBJECT_TYPE_MSG(apply, APPLY_FUNCTION))


def to_match_func(match: Any) -> Match:
    if isinstance(match, Match) or (not match): return match
    elif isinstance(match, Callable): return Match(func=match)
    elif is_array(match): return Match(path=match)
    else: raise TypeError(INVALID_OBJECT_TYPE_MSG(match, MATCH_FUNCTION))


###################################################################
############################## Schema #############################
###################################################################

class Schema(TypedRecords):
    def __init__(self, *args: Field):
        super().__init__(*[validate_field(field) for field in args])

    def get(self, __key: _KT, values_only=False, keep: Literal["fist","last",True,False]=True,
            match: Optional[MatchFunction]=None, **match_by_key) -> Union[Schema,List,Dict,str]:
        schema = self.unique(keep=keep).filter(match, **match_by_key)
        if not (values_only or __key): return schema
        else: return vloc(list(schema), __key, if_null="drop", values_only=values_only)

    @TypedRecords.copyable
    def unique(self, keep: Literal["fist","last",True,False]="first", inplace=False) -> Union[bool,Schema]:
        return drop_duplicates(self, "name", keep=keep) if keep != True else self


def validate_schema(schema: Any) -> Schema:
    if isinstance(schema, Schema): return schema
    elif isinstance(schema, List): return Schema(*schema)
    else: raise TypeError(INVALID_OBJECT_TYPE_MSG(schema, SCHEMA_OBJECT))


###################################################################
########################## Schema Context #########################
###################################################################

class SchemaContext(TypedDict):
    def __init__(self, schema: Schema, root: Optional[_KT]=None, match: Optional[Match]=None):
        super().__init__(schema=validate_schema(schema))
        self.update_notna(root=root, match=to_match_func(match))


def validate_context(context: Any) -> SchemaContext:
    if isinstance(context, SchemaContext): return context
    elif isinstance(context, Dict): return SchemaContext(**context)
    else: raise TypeError(INVALID_OBJECT_TYPE_MSG(context, CONTEXT_OBJECT))


###################################################################
########################### Schema Info ###########################
###################################################################

class SchemaInfo(TypedDict):
    def __init__(self, **kwargs: SchemaContext):
        super().__init__({
            name: validate_context(context) for name, context in kwargs.items()})

    def get_schema(self, keys: _KT=list(), values_only=False, schema_names: _KT=list(),
                    keep: Literal["fist","last",True,False]=True, match: Optional[MatchFunction]=None,
                    **match_by_key) -> Union[Schema,_VT]:
        context = kloc(self, cast_tuple(schema_names), default=dict(), if_null="pass", values_only=True)
        schema = Schema(*union(*vloc(context, "schema", default=list(), if_null="drop", values_only=True)))
        return schema.get(keys, values_only=values_only, keep=keep, match=match, **match_by_key)

    def get_schema_by_name(self, name: _KT, keys: _KT=list(), values_only=False, schema_names: _KT=list(),
                            keep: Literal["fist","last",True,False]=True) -> Union[Schema,_VT]:
        match = lambda __name: __name in cast_tuple(name)
        return self.get_schema(keys, values_only=values_only, schema_names=schema_names, keep=keep, name=match)

    def get_schema_by_type(self, __type: Union[TypeHint,TypeList], keys: _KT=list(), values_only=False,
                            schema_names: _KT=list(), keep: Literal["fist","last",True,False]=True) -> Union[Schema,_VT]:
        __types = tuple(map(get_type, cast_tuple(__type)))
        match = lambda __type: is_type(__type, __types)
        return self.get_schema(keys, values_only=values_only, schema_names=schema_names, keep=keep, type=match)

    def get_names_by_type(self, __type: Union[TypeHint,Sequence[TypeHint]], schema_names: _KT=list(),
                            keep: Literal["fist","last",True,False]=True) -> List[str]:
        return self.get_schema_by_type(__type, keys="name", values_only=True, schema_names=schema_names, keep=keep)


def validate_info(info: Any) -> SchemaInfo:
    if isinstance(info, SchemaInfo): return info
    elif isinstance(info, Dict): return SchemaInfo(**info)
    else: raise TypeError(INVALID_OBJECT_TYPE_MSG(info, INFO_OBJECT))


def pretty_schema(__object, indent=2, step=2) -> str:
    if isinstance(__object, (Field,Apply,Match)):
        return str({__k: to_default(__v) for __k, __v in __object.items()})
    elif isinstance(__object, TypedDict):
        return '{\n'+',\n'.join([' '*indent+f"'{__k}': {pretty_schema(__v, indent=indent+step, step=step)}"
                    for __k, __v in __object.items()])+'\n'+' '*(indent-step)+'}'
    elif isinstance(__object, TypedRecords):
        return '[\n'+',\n'.join([' '*indent+pretty_schema(__e, indent=indent, step=step)
                for __e in __object])+'\n'+' '*(indent-step)+']'
    elif isinstance(__object, str): return f"'{__object}'"
    else: return str(__object)


def pretty_print(__object, path: Optional[_KT]=None, drop: Optional[_KT]=None):
    if notna(path): __object = hier_get(__object, path)
    if notna(drop): __object = drop_dict(__object, drop)
    if isinstance(__object, (TypedDict,TypedRecords)):
        print(pretty_schema(__object))
    elif isinstance(__object, Dict):
        print(pretty_schema(TypedDict(**__object)))
    elif is_records(__object, how="all"):
        print(pretty_schema(TypedRecords(*__object)))
    elif isinstance(__object, pd.DataFrame):
        print("pd.DataFrame("+pretty_schema(TypedRecords(*__object.to_dict("records")))+")")
    elif isinstance(__object, pd.Series):
        print("pd.Series("+pretty_schema(__object.tolist())+")")
    else: print(pretty_schema(__object))


###################################################################
############################## Mapper #############################
###################################################################

class Mapper(BaseSession):
    __metaclass__ = ABCMeta
    operation = "mapper"
    fields = list()
    responseType = "dict"
    root = list()
    schemaInfo = SchemaInfo()

    def __init__(self, fields: IndexLabel=list(), logName=str(), logLevel: LogLevel="WARN", logFile=str(),
                debug: List[str]=list(), extraSave: List[str]=list(), interrupt=str(), localSave=False, **context):
        BaseSession.__init__(self, **self.from_locals(locals()))
        if not isinstance(self.schemaInfo, SchemaInfo):
            self.update(schemaInfo=validate_info(self.schemaInfo))

    def map(self, data: ResponseData, schemaInfo: Optional[SchemaInfo]=None, responseType: Optional[TypeHint]=None,
            root: Optional[_KT]=None, discard=True, updateTime=True, fields: IndexLabel=list(), **context) -> Data:
        if notna(root) or self.root:
            root = root if notna(root) else self.root
            data = get_value(data, root)
        schemaInfo = validate_info(schemaInfo) if notna(schemaInfo) else self.schemaInfo
        self.checkpoint("map"+SUFFIX(context), where="map", msg={"root":root, "data":data, "schemaInfo":schemaInfo})
        data = self.map_info(data, schemaInfo, responseType, discard=discard, **context)
        if updateTime: data = self.set_update_time(data, **context)
        return filter_data(data, fields=fields, if_null="pass")

    def set_update_time(self, data: Data, date: Optional[dt.date]=None, interval=str(), **context) -> Data:
        updateDate = date if date and is_daily_frequency(interval) else self.today()
        return set_data(data, if_exists="ignore", updateDate=updateDate, updateTime=self.now())

    ###################################################################
    ######################### Map Schema Info #########################
    ###################################################################

    def map_info(self, data: ResponseData, schemaInfo: SchemaInfo, responseType: Optional[TypeHint]=None,
                match: Optional[Union[MatchFunction,bool]]=None, discard=True, **context) -> Data:
        responseType = get_response_type(responseType if responseType else self.responseType)
        __base = get_response_origin(responseType)
        if not self._match_schema(data, match, **context): return __base
        for __key, schemaContext in schemaInfo.items():
            if not (isinstance(schemaContext, Dict) and (SCHEMA in schemaContext)): continue
            __base = self.map_context(data, __base, schemaContext, responseType, __key=__key, **context)
        return __base if discard or isinstance(data, Tag) else self._merge_base(data, __base)

    def _match_schema(self, data: ResponseData,
                    match: Optional[Union[MatchFunction,bool]]=None, **context) -> bool:
        if match is not None:
            if isinstance(match, Callable): return match(data, **context)
            else: return bool(match)
        else: return self.match(data, **context)

    def _merge_base(self, data: ResponseData, __base: Data) -> Data:
        if isinstance(data, Dict):
            return chain_dict([__base, data], keep="first")
        elif isinstance(data, pd.DataFrame) and df_exists(__base):
            return concat_df([__base, data], axis=1, keep="first")
        else: return data

    ###################################################################
    ######################## Map Schema Context #######################
    ###################################################################

    def map_context(self, data: ResponseData, __base: Data, schemaContext: SchemaContext,
                    responseType: Optional[TypeHint]=None, **context) -> Data:
        data = get_value(data, schemaContext[ROOT]) if ROOT in schemaContext else data
        self.checkpoint("schema"+SUFFIX(context), where="map_context", msg={"data":data, "schema":schemaContext[SCHEMA]})
        if not isinstance(data, get_response_type(responseType if responseType else self.responseType)):
            if not data: return __base
            else: raise TypeError(INVALID_DATA_TYPE_MSG(data, context))
        data = self._merge_base(data, __base)
        if self.match_data(data, schemaContext.get(MATCH), context=context, log=True):
            return self.map_schema(data, __base, schemaContext[SCHEMA], **context)
        else: return __base

    ###################################################################
    ############################ Map Schema ###########################
    ###################################################################

    def map_schema(self, data: ResponseData, __base: Data, schema: Schema, **context) -> Data:
        for field in schema:
            if not isinstance(field, Dict): continue
            elif field[MODE] in (QUERY, INDEX):
                __value = self.get_value(context, **field, context=context, log=True)
                __base[field[NAME]] = __value if __value != __MISMATCH__ else None
                continue
            data = self._merge_base(data, __base)
            try: __base = self.map_field(data, __base, field, **context)
            except Exception as exception:
                self.logger.error(EXCEPTION_MSG(context, field))
                raise exception
        return __base

    ###################################################################
    ############################ Map Field ############################
    ###################################################################

    def map_field(self, data: ResponseData, __base: Data, field: Field, **context) -> Data:
        path_type = field[HOW] if HOW in field else get_path_type(field[PATH])
        if path_type in (PATH,CALLABLE):
            __value = self.get_value(data, **field, context=context, log=True)
        elif path_type == VALUE:
            __value = field[PATH]
        elif path_type == TUPLE:
            if isinstance(data, pd.DataFrame): __value = self._get_value_tuple_df(data, **field, context=context)
            else: __value = self._get_value_tuple(data, **field, context=context)
        elif (path_type == ITERATE) and (not isinstance(data, pd.DataFrame)):
            __value = self._get_value_iterate(data, **field, context=context)
        else: raise TypeError(INVALID_PATH_TYPE_MSG(field[PATH]))
        self.checkpoint("field"+SUFFIX(context, field), where="map_field", msg={"value":__value, "field":field})
        if (__value == __MISMATCH__) or (field[MODE] in (OPTIONAL,REQUIRED) and isna_plus(__value)):
            if field[MODE] == REQUIRED: raise ValueError(REQUIRED_MSG(context, field))
            else: return __base
        __base[field[NAME]] = __value
        return __base

    def get_value(self, data: ResponseData, path=list(), type: Optional[TypeHint]=None, default=None,
                    apply: Apply=dict(), match: Match=dict(), strict=True, cast=False,
                    context: Context=dict(), name=str(), log=False, **field) -> _VT:
        if isinstance(data, pd.DataFrame):
            if match:
                data = data[self.match_data(data, match, context=context, field=field, name=name, log=log)]
                if data.empty: return __MISMATCH__
        elif not self.match_data(data, match, context=context, name=name, log=log): return __MISMATCH__
        default = self._get_value_by_path(data, default) if notna(default) else None
        __value = self._get_value_by_path(data, path, default, context)
        return self._apply_value(__value, apply, type, default, strict, cast, context, name, log, **field)

    def _get_value_by_path(self, data: ResponseData, path=list(), default=None, context: Context=dict()) -> _VT:
        if is_array(path):
            return get_value(data, path, default=default) if path else data
        elif isinstance(path, Callable):
            __apply = safe_apply_df if isinstance(data, PANDAS_DATA) else safe_apply
            return __apply(data, path, default, **context)
        else: return to_series(path, data.index) if isinstance(data, PANDAS_DATA) else path

    def _apply_value(self, __value: _VT, apply: Apply=dict(), type: Optional[TypeHint]=None, default=None,
                    strict=True, cast=False, context: Context=dict(), name=str(), log=False, **field) -> _VT:
        __apply = cast_list(apply, strict=False)
        if cast: __apply += [Cast(type, default, strict)]
        field = dict(field, type=type, default=default, strict=strict)
        return self.apply_data(__value, __apply, context=context, field=field, name=name, log=log)

    def _get_value_tuple(self, data: ResponseData, path: Tuple, apply: Apply=dict(), match: Match=dict(),
                        context: Context=dict(), name=str(), **field) -> _VT:
        __match = int(self.match_data(data, match, context=context, name=name, log=True))-1
        __apply = get_scala(apply, index=__match, default=dict())
        return self.get_value(data, path[__match], apply=__apply, context=context, name=name, log=True, **field)

    def _get_value_tuple_df(self, data: pd.DataFrame, path: Tuple, apply: Apply=dict(), match: Match=dict(),
                            context: Context=dict(), name=str(), **field) -> pd.Series:
        __match = self.match_data(data, match, context=context, name=name, log=True)
        df_true, df_false = data[__match], data[~__match]
        apply_true, apply_false = get_scala(apply, index=0, default=dict()), get_scala(apply, index=-1, default=dict())
        df_true = self.get_value(df_true, path[0], apply=apply_true, context=context, name=name, log=True, **field)
        df_false = self.get_value(df_false, path[1], apply=apply_false, context=context, name=name, log=False, **field)
        try: return pd.concat([df for df in data if df_exists(df)]).sort_index()
        except: return __MISMATCH__

    def _get_value_iterate(self, data: ResponseData, path: Sequence, apply: Apply=dict(), match: Match=dict(),
                            context: Context=dict(), name=str(), **field) -> _VT:
        __value = get_value(data, path[:-1])
        if not isinstance(__value, Sequence):
            raise TypeError(INVALID_VALUE_TYPE_MSG(__value, context, name=name))
        sub_path = path[-1] if (len(path) > 0) and is_array(path[-1]) else __value
        log_context = dict(context=context, name=name, log=False)
        return [self.get_value(__e, sub_path, apply=apply, **field, **log_context)
                for __e in __value if self.match_data(__e, match, field=field, **log_context)]

    ###################################################################
    ############################ Apply Data ###########################
    ###################################################################

    def validate_apply(func):
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

    @validate_apply
    def apply_data(self, data: ResponseData, apply: Union[Apply,Sequence[Apply]], context: Context=dict(),
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

    def __regex__(self, __object, pattern: RegexFormat, how: Literal["search","findall","sub"]="search",
                    default=None, index: Optional[int]=0, repl: Optional[str]=None, **kwargs) -> str:
        __object = cast_str(__object, strict=True)
        __pattern = pattern if isinstance(pattern, re.Pattern) else re.compile(pattern)
        if how == "sub": return __pattern.sub(repl, __object)
        elif how == "findall": index = None
        return re_get(pattern, __object, default=default, index=index)

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
        schemaInfo = SchemaInfo(**{name: SchemaContext(schema=schema, root=root, match=match)})
        context.pop(SCHEMA_KEY, None)
        return self.map(__object, schemaInfo, responseType=type, **context)

    ###################################################################
    ############################ Match Data ###########################
    ###################################################################

    def validate_match(func):
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

    @validate_match
    def match_data(self, data: ResponseData, match: Match, context: Context=dict(),
                    field: Optional[Field]=dict(), name: Optional[str]=str(), log=False) -> Union[bool,pd.Series]:
        log_context = dict(context=context, field=field, name=name, log=log)
        if MATCH_QUERY in match: return self._match_query(**log_context, **match)
        elif FUNC in match: return self._match_function(data, **match, **log_context)
        elif (EXACT in match) or (INCLUDE in match) or (EXCLUDE in match):
            msg = kloc(match, [EXACT, INCLUDE, EXCLUDE], if_null="drop")
            self._log_origin(data, match, point="match", where="isin_data", msg=msg, **log_context)
            return toggle(isin_data(data, **match), flip=match.get(FLIP, False))
        else: return self._match_value(data, **match, **log_context)

    def _match_query(self, context: Context, query: _KT, field: Optional[Field]=dict(),
                    name: Optional[str]=str(), log=False, **match) -> bool:
        match = dict(match, path=query)
        if log:
            __value = filter_data(context, query, hier=match.get("hier", True))
            self._log_origin(__value, match, point="match", where="match_query", context=context, field=field, name=name, log=log)
        return self.match_data(context, match, context=context, field=field, name=name, log=False)

    def _match_function(self, data: ResponseData, func: Callable, path: Optional[_KT]=None, default=False,
                        flip=False, hier=True, context: Context=dict(), field: Optional[Field]=dict(),
                        name: Optional[str]=str(), log=False, **kwargs) -> Union[bool,pd.Series]:
        if not isinstance(func, Callable): return default
        if notna(path): data = filter_data(data, path, hier=hier)
        if log:
            match = dict(func=func, path=path, default=default, flip=flip, hier=hier)
            self._log_origin(data, match, point="match", where="match_function", context=context, field=field, name=name, log=True)
        __apply = safe_apply_df if isinstance(data, PANDAS_DATA) else safe_apply
        return toggle(__apply(data, func, default, **context), flip=flip)

    def _match_value(self, data: ResponseData, path: _KT, value: Optional[Any]=None, flip=False, strict=False,
                    how: Literal["any","all"]="any", if_null=False, hier=True, context: Context=dict(),
                    field: Optional[Field]=dict(), name: Optional[str]=str(), log=False, **kwargs) -> Union[bool,pd.Series]:
        if not is_single_path_by_data(data, path, hier=hier):
            args = (value, flip, strict, how, if_null, hier)
            return howin([self._match_value(data, __k, args, log=log, **kwargs) for __k in path], how=how)
        __value = get_value(data, path)
        if log:
            match = dict(path=path, flip=flip, strict=strict, how=how, if_null=if_null, hier=hier)
            self._log_origin(__value, match, point="match", where="match_value", context=context, field=field, name=name, log=True)
        if isna(__value): return toggle(if_null, flip=flip)
        elif notna(value): return toggle((__value == value), flip=flip)
        elif isinstance(data, pd.DataFrame):
            return toggle(match_df(data, match=(lambda x: exists(x, strict=strict)), all_cols=True), flip=flip)
        else: return toggle(exists(__value, strict=strict), flip=flip)

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


def get_response_type(responseType: TypeHint) -> Type:
    if isinstance(responseType, Type): return responseType
    elif is_dict_type(responseType): return dict
    elif is_dataframe_type(responseType): return pd.DataFrame
    elif is_tag_type(responseType): return Tag
    else: return dict


def get_response_origin(responseType: TypeHint) -> Data:
    if not isinstance(responseType, Type):
        responseType = get_response_type(responseType)
    elif responseType == pd.DataFrame: return pd.DataFrame()
    else: return dict()


def toggle(__bool: Union[bool,PandasData], flip=False) -> Union[bool,pd.Series]:
    if isinstance(__bool, PANDAS_DATA):
        if isinstance(__bool, pd.DataFrame): __bool = __bool.any(axis=1)
        return (~__bool) if flip else __bool
    return (not __bool) if flip else bool(__bool)


def is_single_path_by_data(data: ResponseData, path: _KT, hier=False) -> bool:
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
    fields = list()
    responseType = "records"
    root = list()
    groupby = list()
    groupSize = dict()
    countby = str()
    schemaInfo = SchemaInfo()

    def map(self, data: ResponseData, schemaInfo: Optional[SchemaInfo]=None, responseType: Optional[TypeHint]=None,
            root: Optional[_KT]=None, groupby: Optional[_KT]=None, groupSize: Optional[NestedDict]=None,
            countby: Optional[Literal["page","start"]]=None, discard=True, updateTime=True,
            fields: IndexLabel=list(), **context) -> Data:
        if notna(root) or self.root:
            root = root if notna(root) else self.root
            data = get_value(data, root)
        schemaInfo = validate_info(schemaInfo) if notna(schemaInfo) else self.schemaInfo
        self.checkpoint("map"+SUFFIX(context), where="map", msg={"root":root, "data":data, "schemaInfo":schemaInfo})
        if isinstance(data, (Sequence,pd.DataFrame)):
            groupby = dict(groupby=(groupby if notna(groupby) else self.groupby))
            groupSize = dict(groupSize=(groupSize if notna(groupSize) else self.groupSize))
            countby = dict(countby=(countby if notna(countby) else self.countby))
            context = dict(context, **groupby, **groupSize, **countby)
            data = self.map_sequence(data, schemaInfo, responseType, discard=discard, **context)
        else: data = self.map_info(data, schemaInfo, responseType, discard=discard, **context)
        if updateTime:
            updateDate = context.get("date") if is_daily_frequency(context.get("interval")) else self.today()
            data = set_data(data, if_exists="ignore", updateDate=updateDate, updateTime=self.now())
        return filter_data(data, fields=fields, if_null="pass")

    ###################################################################
    ########################### Map Sequence ##########################
    ###################################################################

    def map_sequence(self, data: ResponseData, schemaInfo: SchemaInfo, responseType: Optional[TypeHint]=None,
                    groupby: _KT=list(), groupSize: NestedDict=dict(), countby: Optional[Literal["page","start"]]=None,
                    match: Optional[Union[MatchFunction,bool]]=None, discard=False, **context) -> Data:
        context = dict(context, schemaInfo=schemaInfo, countby=countby, match=match, discard=discard)
        responseType = get_response_type(responseType if responseType else self.responseType)
        if groupby: return self.groupby_data(data, groupby, groupSize, responseType=responseType, **context)
        elif is_records_type(responseType): return self.map_records(data, **context)
        elif is_dataframe_type(responseType): return self.map_dataframe(data, **context)
        elif is_tag_type(responseType): return self.map_tag_list(data, **context)
        else: return self.map_records(data, **context)

    def groupby_data(self, data: ResponseData, groupby: _KT, groupSize: NestedDict=dict(),
                    if_null: Literal["drop","pass"]="drop", hier=False, **context) -> Data:
        groups = groupby_data(data, by=groupby, if_null=if_null, hier=hier)
        log_msg = {"groups":list(groups.keys()), "groupSize":[safe_len(group) for group in groups.values()]}
        self.checkpoint("group"+SUFFIX(context), where="groupby_data", msg=log_msg)
        results = [self.map_sequence(__data, **dict(context, group=group, dataSize=groupSize.get(group)))
                    for group, __data in groups.items()]
        return chain_exists(results)

    ###################################################################
    ######################## Map Sequence Data ########################
    ###################################################################

    def map_records(self, __r: Records, schemaInfo: SchemaInfo, countby: Optional[Literal["page","start"]]=None,
                    match: Optional[Union[MatchFunction,bool]]=None, discard=False, **context) -> Records:
        data = list()
        start = self.get_start_by(countby, count=len(__r), **context)
        for __i, __m in enumerate(__r, start=(start if start else 0)):
            if not self._match_schema(__m, match, **context): continue
            __i = __m[RANK] if isinstance(__m.get(RANK), int) else __i
            kwargs = dict(context, discard=discard, count=len(__r), __i=__i)
            data.append(self.map_info(__m, schemaInfo, responseType="dict", match=True, **kwargs))
        return data

    def map_dataframe(self, df: pd.DataFrame, schemaInfo: SchemaInfo, countby: Optional[Literal["page","start"]]=None,
                    match: Optional[Union[MatchFunction,bool]]=None, discard=False, **context) -> pd.DataFrame:
        start = self.get_start_by(countby, count=len(df), **context)
        if isinstance(start, int) and (RANK not in df):
            df[RANK] = range(start, len(df)+start)
        df = df[self._match_schema(df, match, **context)]
        context = dict(context, responseType="dataframe", match=True, discard=discard, count=len(df))
        return self.map_info(df, schemaInfo, **context)

    def map_tag_list(self, tag_list: Sequence[Tag], schemaInfo: SchemaInfo, countby: Optional[Literal["page","start"]]=None,
                    match: Optional[Union[MatchFunction,bool]]=None, discard=False, **context) -> Records:
        data = list()
        start = self.get_start_by(countby, count=len(tag_list), **context)
        for __i, __s in enumerate(tag_list, start=(start if start else 0)):
            if not (isinstance(__s, Tag) and self._match_schema(__s, match, **context)): continue
            kwargs = dict(context, count=len(tag_list), __i=__i)
            data.append(self.map_info(__s, schemaInfo, responseType="tag", match=True, **kwargs))
        return data

    def get_start_by(self, by: Optional[Literal["page","start"]]=None, count: Optional[int]=0,
                    page=1, start=1, pageStart=1, offset=1, dataSize: Optional[int]=None, **context) -> int:
        if (by == "page") and isinstance(page, int):
            dataSize = dataSize if isinstance(dataSize, int) else count
            return (page if pageStart == 0 else page-1)*dataSize+1
        elif by == "start" and isinstance(start, int):
            return start+1 if offset == 0 else start
        else: return None

    ###################################################################
    ############################ Apply Data ###########################
    ###################################################################

    def __map__(self, __object, schema: Schema, type: TypeHint, root: Optional[_KT]=None,
                match: Optional[Match]=None, groupby: _KT=list(), groupSize: NestedDict=dict(),
                countby: Optional[Literal["page","start"]]="start", page=1, start=1,
                submatch: Optional[Union[MatchFunction,bool]]=True, discard=True,
                context: Context=dict(), name=str(), **kwargs) -> Data:
        schemaInfo = SchemaInfo(**{name: SchemaContext(schema=schema, root=root, match=match)})
        context = dict(context, groupby=groupby, groupSize=groupSize, countby=countby,
                        page=page, start=start, match=submatch, discard=discard)
        context.pop(SCHEMA_KEY, None)
        context.pop(COUNT, None)
        context[ITER_INDEX] = f"{context.get(ITER_INDEX,0)}-{context.pop(COUNT_INDEX,0)}"
        return self.map(__object, schemaInfo, responseType=type, **context)


###################################################################
############################## Parser #############################
###################################################################

class Parser(SequenceMapper):
    __metaclass__ = ABCMeta
    operation = "parser"
    host = str()
    fields = list()
    responseType = "records"
    root = list()
    groupby = list()
    groupSize = dict()
    countby = str()
    schemaInfo = SchemaInfo()

    def get_rename_map(self, to: Optional[Literal["desc","name"]]=None, schema_names: _KT=list(),
                        keep: Literal["fist","last",False]="first", renameMap: RenameMap=dict(), **context) -> RenameMap:
        if to in ("desc", "name"):
            __from, __to = ("desc", "name") if to == "name" else ("name", "desc")
            schema = self.schemaInfo.get_schema(schema_names=schema_names, keep=keep)
            return {field[__from]: field[__to] for field in schema}
        else: return renameMap

    def print(self, __object, path: Optional[_KT]=None, drop: Optional[_KT]=None):
        pretty_print(__object, path=path, drop=drop)

    ###################################################################
    ######################## Response Validator #######################
    ###################################################################

    def validate_response(func):
        @functools.wraps(func)
        def wrapper(self: Parser, response: Any, *args, **context):
            is_valid = self.is_valid_response(response)
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
    def parse(self, response: Any, **context) -> Data:
        return self.map(response, **context)
