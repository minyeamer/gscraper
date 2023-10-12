from __future__ import annotations
from gscraper.base.context import SCHEMA_CONTEXT
from gscraper.base.session import BaseSession

from gscraper.base.types import _KT, _VT, _PASS, Context, TypeHint, LogLevel, IndexLabel, Timezone, RenameMap
from gscraper.base.types import Records, NestedDict, MappingData, Data, JsonData, HtmlData, ApplyFunction, MatchFunction
from gscraper.base.types import not_na, get_type, init_origin, is_type, is_numeric_or_date_type, is_bool_type
from gscraper.base.types import is_array, is_records, is_json_object, is_df, is_df_sequence

from gscraper.utils.cast import cast_object, cast_str, cast_tuple
from gscraper.utils.logs import log_data
from gscraper.utils.map import safe_apply, get_scala, exists_one, union
from gscraper.utils.map import kloc, chain_dict, drop_dict, exists_dict, hier_get
from gscraper.utils.map import vloc, match_records, groupby_records, drop_duplicates
from gscraper.utils.map import concat_df, fillna_each, safe_apply_df, groupby_df, filter_data, set_data
from gscraper.utils.map import select_one, hier_select, exists_source, include_source, groupby_source

from abc import ABCMeta
from ast import literal_eval
import functools

from typing import Any, Dict, Callable, List, Literal, Optional, Sequence, Tuple, Type, Union
from bs4.element import Tag
import json
import pandas as pd
import re


SCHEMA = "schema"
ROOT = "root"
MATCH = "match"
RANK = "rank"
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
GLOBAL = "global"

QUERY = "QUERY"
INDEX = "INDEX"
NULLABLE = "NULLABLE"
NOTNULL = "NOTNULL"
NOTZERO = "NOTZERO"
OPTIONAL = "OPTIONAL"

FUNC = "func"
__EXISTS__ = "__EXISTS__"
__JOIN__ = "__JOIN__"
__SPLIT__ = "__SPLIT__"
__MAP__ = "__MAP__"


###################################################################
############################# Messages ############################
###################################################################

INVALID_DATA_TYPE_MSG = lambda data, __type: f"'{type(data)}' is not a valid data type for parsing {__type}."
INVALID_VALUE_TYPE_MSG = lambda ret, __t, name=str(): \
    f"'{type(ret)}' type is not a valid value type{FOR_NAME(name)}. '{__t}' type is desired."

INVALID_SCHEMA_TYPE_MSG = "Schema must be a sequence of dictionaries."
INVALID_FIELD_TYPE_MSG = "Schema field must be a dictionary type."
INVALID_FIELD_KEY_MSG = "Schema field must have name, path, type, and mode key-values."
INVALID_PATH_TYPE_MSG = lambda path: f"'{path}' is not supported type for schema path."

FOR_NAME = lambda name: f" for the '{name}'" if name else str()

INVALID_APPLY_TYPE_MSG = lambda apply, name=str(): f"'{type(apply)}' is not a valid apply object type{FOR_NAME(name)}."
INVALID_APPLY_FUNC_MSG = lambda func, name=str(): f"'{type(func)}' is not a valid apply function type{FOR_NAME(name)}."
INVALID_APPLY_SPECIAL_MSG = lambda func, name=str(): f"'{func}' is not a valid special apply function{FOR_NAME(name)}."
INVALID_APPLY_RETURN_MSG = lambda ret, __t, name=str(): \
    f"'{type(ret)}' type is not a valid return type of apply function{FOR_NAME(name)}. '{__t}' type is desired."

INVALID_MATCH_TYPE_MSG = lambda match, name=str(): f"'{type(match)}' is not a valid match object type{FOR_NAME(name)}."
INVALID_MATCH_FUNC_MSG = lambda func, name=str(): f"'{type(func)}' is not a valid match function type{FOR_NAME(name)}."
INVALID_MATCH_KEY_MSG = lambda name=str(): f"Match function{FOR_NAME(name)} requires at least one parameter: func, path, value, query"

EXCEPTION_ON_NAME_MSG = lambda name: f"Exception occured from '{name}'."

RECORDS, RECORD = "records", "a record"
SEQUENCE, DICTIONARY = "Sequence", "Dictionary"
PANDAS_SERIES = "Pandas Series"
PANDAS_OBJECT = "Pandas DataFrame or Series"
HTML_SOURCE = "HTML source"


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
############################## Schema #############################
###################################################################

SchemaPath = Union[_KT, _VT, Tuple[_KT,_KT], Tuple[_VT,_VT], Callable]
PathType = Literal["path", "value", "tuple", "iterate", "callable", "global"]


class Apply(dict):
    def __init__(self, func: Union[ApplyFunction, str], default: Optional[Any]=None):
        self.update(func=func, **exists_dict(dict(default=default), strict=True))


class Match(dict):
    def __init__(self, func: Optional[MatchFunction]=None, path: Optional[_KT]=None,
                value: Optional[Any]=None, flip: Optional[bool]=None, default: Optional[bool]=None,
                strict: Optional[bool]=None, query: Optional[_KT]=None):
        self.update(exists_dict(
            dict(func=func, path=path, value=value, flip=flip, default=default,
                strict=strict, query=query), strict=True))


class Exists(Apply):
    def __init__(self, keys=str()):
        self.update(func=__EXISTS__, **exists_dict(dict(keys=keys), strict=True))


class Join(Apply):
    def __init__(self, keys: _KT=list(), sep=',', split=','):
        self.update(func=__JOIN__, **exists_dict(dict(keys=keys, sep=sep, split=split), strict=True))


class Split(Apply):
    def __init__(self, sep=',', maxsplit=-1, type: Optional[Type]=None, default=None, strict=True,
                index: Optional[int]=None):
        self.update(func=__SPLIT__, **exists_dict(
            dict(sep=sep, maxsplit=maxsplit, type=type, default=default, strict=strict,
                index=index), strict=True))


class Map(dict):
    def __init__(self, schema: Schema, root: Optional[_KT]=None, match: Optional[Match]=None,
                groupby: _KT=list(), groupSize: NestedDict=dict(),
                rankby: Optional[Literal["page","start"]]="start", page=1, start=1,
                submatch: Optional[MatchFunction]=None, discard=True) -> Data:
        self.update(func=__MAP__, schema=schema, **exists_dict(
            dict(root=root, match=match, groupby=groupby, groupSize=groupSize,
                rankby=rankby, page=page, start=start, submatch=submatch, discard=discard), strict=False))


class Field(dict):
    def __init__(self, name: _KT, path: SchemaPath, type: TypeHint, mode: str, desc: Optional[str]=None,
                cast: Optional[bool]=None, strict: Optional[bool]=None, default: Optional[Any]=None,
                apply: Optional[Apply]=None, match: Optional[Match]=None):
        self.update(name=name, path=path, type=type, mode=mode, description=desc, **exists_dict(
            dict(cast=cast, strict=strict, default=default, apply=apply, match=match), strict=True))


class Schema(list):
    def __init__(self, *args: Field):
        for __e in args:
            self.append(__e)


class SchemaContext(dict):
    def __init__(self, schema: Schema, root: Optional[_KT]=None, match: Optional[Match]=None):
        self.update(schema=schema, **exists_dict(dict(root=root, match=match), strict=True))


class SchemaInfo(dict):
    def __init__(self, **context: SchemaContext):
        self.update(context)


###################################################################
############################## Parser #############################
###################################################################

class Parser(BaseSession):
    __metaclass__ = ABCMeta
    operation = "parser"
    host = str()
    fields = list()
    root = list()
    groupby = list()
    groupSize = dict()
    rankby = str()
    schemaInfo = SchemaInfo()

    def __init__(self, fields: IndexLabel=list(),
                tzinfo: Optional[Timezone]=None, datetimeUnit: Literal["second","minute","hour","day"]="second",
                returnType: Optional[TypeHint]=None, logName=str(), logLevel: LogLevel="WARN", logFile=str(),
                debug: List[str]=list(), extraSave: List[str]=list(), interrupt=str(), localSave=False,
                **context):
        BaseSession.__init__(
            self, fields=fields, tzinfo=tzinfo, datetimeUnit=datetimeUnit, returnType=returnType,
            logName=logName, logLevel=logLevel, logFile=logFile,
            debug=debug, extraSave=extraSave, interrupt=interrupt, localSave=localSave, **context)
        self.schemaInfo = validate_schema_info(self.schemaInfo)

    def get_unique_fields(self, keys: _KT=list(), values_only=False, schema_names: _KT=list(),
                            keep: Literal["fist","last",False]="first", match: Optional[MatchFunction]=None,
                            **match_by_key) -> List[Field]:
        context = kloc(self.schemaInfo, cast_tuple(schema_names), default=list(), if_null="pass", values_only=True)
        fields = union(*vloc(context, "schema", if_null="drop", values_only=True))
        fields = drop_duplicates(fields, keep=keep)
        if isinstance(match, Callable) or match_by_key:
            all_keys = isinstance(match, Callable)
            fields = match_records(fields, all_keys=all_keys, match=match, **match_by_key)
        return vloc(fields, keys, if_null="drop", values_only=values_only) if keys else fields

    def get_fields_by_type(self, __type: Union[TypeHint,Sequence[TypeHint]],
                            keys: _KT=list(), values_only=False, schema_names: _KT=list(),
                            keep: Literal["fist","last",False]="first") -> List[Field]:
        __types = tuple(map(get_type, cast_tuple(__type)))
        fields = self.get_unique_fields(schema_names=schema_names, keep=keep)
        fields = [field for field in fields if is_type(field[TYPE], __types)]
        return vloc(fields, keys, if_null="drop", values_only=values_only) if keys else fields

    def get_names_by_type(self, __type: Union[TypeHint,Sequence[TypeHint]], schema_names: _KT=list(),
                            keep: Literal["fist","last",False]="first") -> List[str]:
        return self.get_fields_by_type(__type, keys="name", values_only=True, schema_names=schema_names, keep=keep)

    def get_rename_map(self, to: Optional[Literal["en","ko","desc","name"]]=None, schema_names: _KT=list(),
                        keep: Literal["fist","last",False]="first", **context) -> RenameMap:
        if to in ("desc", "name"):
            __from, __to = ("description", "name") if to == "name" else ("name", "description")
            return {field[__from]: field[__to] for field in self.get_unique_fields(schema_names=schema_names, keep=keep)}
        else: return dict()

    ###################################################################
    ######################## Response Validator #######################
    ###################################################################

    def validate_response(func):
        @functools.wraps(func)
        def wrapper(self: Parser, response: Any, *args, **context):
            is_valid = self.is_valid_response(response)
            data = func(self, response, *args, **context) if is_valid else init_origin(func)
            suffix = f"_{context.get('index')}" if context.get("index") else str()
            self.checkpoint("parse"+suffix, where=func.__name__, msg={"data":data}, save=data)
            self.log_results(data, **context)
            return data
        return wrapper

    def is_valid_response(self, response: Any) -> bool:
        return True

    def log_results(self, data: Data, **context):
        self.logger.info(log_data(data, **context))

    ###################################################################
    ########################## Parse Response #########################
    ###################################################################

    @validate_response
    def parse(self, response: Any, **context) -> Data:
        return self.map(response, **context)

    def map(self, data: Data, discard=False, updateTime=True, fields: IndexLabel=list(), **context) -> Data:
        context = dict(context, match=(self.match if self.match != super().match else None), discard=discard)
        if is_json_object(data):
            if self.root: data = hier_get(data, self.root)
            if isinstance(data, List):
                data = map_records(data, self.schemaInfo, self.groupby, self.groupSize, self.rankby, **context)
            else: data = map_dict(data, self.schemaInfo, **context)
        elif isinstance(data, pd.DataFrame):
            data = map_df(data, self.schemaInfo, self.groupby, self.groupSize, self.rankby, **context)
        elif isinstance(data, Tag):
            if self.root: data = hier_select(data, self.root)
            data = map_source(data, self.schemaInfo, self.groupby, self.groupSize, self.rankby, **context)
        else: return data
        if updateTime:
            data = set_data(data, updateDate=self.today(), updateTime=self.now())
        return filter_data(data, fields=fields, if_null="pass")

###################################################################
######################### Validate Schema #########################
###################################################################

def validate_schema_info(schemaInfo: SchemaInfo) -> SchemaInfo:
    schemaInfo = schemaInfo.copy()
    for __key, schemaContext in schemaInfo.items():
        schemaInfo[__key]["schema"] = validate_schema(schemaContext["schema"])
        if "match" in schemaContext:
            schemaInfo[__key]["match"] = validate_match(schemaContext["match"], __key)
    return schemaInfo


def validate_schema(schema: Schema) -> Schema:
    if not isinstance(schema, Sequence):
        raise TypeError(INVALID_SCHEMA_TYPE_MSG)
    return Schema(*[validate_field(field) for field in schema])


def validate_field(field: Field) -> Field:
    if not isinstance(field, Dict):
        raise TypeError(INVALID_FIELD_TYPE_MSG)
    if len(kloc(field, [NAME, PATH, TYPE, MODE], if_null="drop")) != 4:
        raise ValueError(INVALID_FIELD_KEY_MSG)
    field = field.copy()
    field[HOW] = _get_path_type(field[PATH])
    field = _init_field(field)
    if APPLY in field:
        field[APPLY] = validate_apply(**field)
    if MATCH in field:
        field[MATCH] = validate_match(**field)
    return field


def _get_path_type(path: SchemaPath) -> PathType:
    if isinstance(path, str): return VALUE
    elif isinstance(path, Sequence):
        if not path: return GLOBAL
        if isinstance(path, Tuple) and len(path) == 2: return TUPLE
        elif len(path) > 0 and is_array(path[-1]): return ITERATE
        else: return PATH
    elif isinstance(path, Callable): return CALLABLE
    else: raise TypeError(INVALID_PATH_TYPE_MSG(path))


def _init_field(field: Field) -> Field:
    field[TYPE] = get_type(field[TYPE], argidx=-1)
    if field[MODE] == INDEX: pass
    elif field[MODE] == NOTNULL:
        field[DEFAULT] = init_origin(field[TYPE])
        field[CAST] = True
    elif field[MODE] == NOTZERO:
        field[CAST] = True
        field[STRICT] = False
    elif is_numeric_or_date_type(field[TYPE]) and (CAST not in field):
        field[CAST] = True
        if is_bool_type(field[TYPE]):
            field[STRICT] = False
    return field


def validate_apply(apply: Apply, name=str(), **context) -> Apply:
    if isinstance(apply, (Callable, Dict)):
        if isinstance(apply, Callable):
            apply = dict(func=apply)
        if not isinstance(apply.get(FUNC), (Callable, str)):
            raise INVALID_APPLY_FUNC_MSG(apply[FUNC], name)
        elif apply[FUNC] == __MAP__:
            apply[SCHEMA] = validate_schema(apply[SCHEMA])
        return apply
    elif isinstance(apply, Tuple):
        return tuple(validate_apply(func, name) for func in apply)
    else: raise TypeError(INVALID_APPLY_TYPE_MSG(apply, name))


def validate_match(match: Match, name=str(), **context) -> Match:
    if isinstance(match, (Callable, Dict)):
        if isinstance(match, Callable):
            match = dict(func=match)
        if not ((FUNC in match) or (PATH in match) or (VALUE in match) or (MATCH_QUERY in match)):
            raise ValueError(INVALID_MATCH_KEY_MSG(name))
        if (FUNC in match) and not isinstance(match[FUNC], Callable):
            raise TypeError(INVALID_MATCH_FUNC_MSG(match[FUNC], name))
        return match
    else: raise TypeError(INVALID_MATCH_TYPE_MSG(match, name))


###################################################################
########################## Parse Records ##########################
###################################################################

def map_records(__r: Records, schemaInfo: SchemaInfo, groupby: _KT=list(), groupSize: NestedDict=dict(),
                rankby=str(), match: Optional[MatchFunction]=None, discard=False, **context) -> Records:
    context = SCHEMA_CONTEXT(**context)
    if groupby:
        return _groupby_records(__r, schemaInfo, groupby, groupSize, rankby, match, discard, **context)
    data = list()
    start = get_start(len(__r), rankby, **context)
    for __i, __m in enumerate(__r, start=(start if start else 0)):
        if not isinstance(__m, Dict) or (match and not match(__m, **context)): continue
        context[RANK] = __m[RANK] if isinstance(__m.get(RANK), int) else __i
        data.append(map_dict(__m, schemaInfo, discard=discard, count=len(__r), **context))
    return data


def _groupby_records(__r: Records, schemaInfo: SchemaInfo, groupby: _KT=list(), groupSize: NestedDict=dict(),
                    rankby=str(), match: Optional[MatchFunction]=None, discard=False, **context) -> Records:
    context = dict(context, schemaInfo=schemaInfo, rankby=rankby, match=match, discard=discard)
    groups = groupby_records(__r, by=groupby, if_null="pass")
    return union(*[
        map_records(__r, **dict(context, dataSize=hier_get(groupSize, group))) for group, __r in groups.items()])


def get_start(count: Optional[int]=0, by: Optional[Literal["page","start"]]=None,
                page=1, start=1, pageStart=1, offset=1, dataSize: Optional[int]=None, **context) -> int:
    if (by == "page") and isinstance(page, int):
        dataSize = dataSize if isinstance(dataSize, int) else count
        return (page if pageStart == 0 else page-1)*dataSize+1
    elif by == "start" and isinstance(start, int):
        return start+1 if offset == 0 else start
    else: return None


###################################################################
############################ Parse Dict ###########################
###################################################################

def map_dict(__m: Dict, schemaInfo: SchemaInfo, match: Optional[MatchFunction]=None, discard=False, **context) -> Dict:
    context = SCHEMA_CONTEXT(**context)
    __base = dict()
    if match and not match(__m, **context): return __base
    for __key, schema_context in schemaInfo.items():
        schema, root, match = kloc(schema_context, SCHEMA_KEYS, if_null="pass", values_only=True)
        __bm = hier_get(__m, root) if root else __m
        if not __bm: continue
        elif not isinstance(__bm, Dict):
            raise TypeError(INVALID_DATA_TYPE_MSG(__m, DICTIONARY))
        __bm = chain_dict([__base, context, __bm], keep="first")
        if isinstance(match, Dict) and not _match_dict(__bm, **match, **context): continue
        __base = _map_dict_schema(__bm, __base, schema, **context)
    return __base if discard else chain_dict([__base, __m], keep="first")


def _map_dict_schema(__m: Dict, __base: Dict, schema: Schema, **context) -> Dict:
    for field in schema:
        __m = dict(__m, **__base)
        if (field[MODE] in (QUERY, INDEX)) and (field[NAME] in context):
            if not ((CAST in field) or (APPLY in field) or (MATCH in field)):
                __base[field[NAME]] = context[field[NAME]]
                continue
            else: __m[field[NAME]] = context[field[NAME]]
        try: __base = _map_dict_field(__m, __base, **field, **context)
        except Exception as exception:
            print(EXCEPTION_ON_NAME_MSG(field[NAME]))
            raise exception
        if (field[MODE] == OPTIONAL) and (field[NAME] in __base) and pd.isna(__base[field[NAME]]):
            __base.pop(field[NAME])
    return __base


def _map_dict_field(__m: Dict, __base: Dict, name: _KT, path: SchemaPath, how: Optional[PathType]=None,
                    type: Optional[Type]=None, mode: _PASS=None, description: _PASS=None, cast=False, strict=True,
                    default=None, apply: Apply=dict(), match: Match=dict(), **context) -> Dict:
    path_type = how if how else _get_path_type(path)
    context = dict(type=type, cast=cast, strict=strict, default=default, apply=apply, match=match, query=context)
    if path_type in (PATH,CALLABLE): return _set_dict_value(__m, __base, name, path, **context)
    elif path_type == VALUE: return dict(__base, **{name: path})
    elif path_type == TUPLE: return _set_dict_tuple(__m, __base, name, path, **context)
    elif path_type == ITERATE: return _set_dict_iterate(__m, __base, name, path, **context)
    elif path_type == GLOBAL: return _set_dict_global(__m, __base, name, **context)
    else: raise TypeError(INVALID_PATH_TYPE_MSG(path))


def _from_dict(__m: Dict, path: SchemaPath, type: Optional[Type]=None,
                cast=False, strict=True, default=None, **context) -> _VT:
    default = _from_dict(__m, default) if not_na(default) else default
    if is_array(path): value = hier_get(__m, path, default, empty=False)
    elif isinstance(path, Callable): value = safe_apply(__m, path, default, **context)
    else: value = path
    return cast_object(value, type, default=default, strict=strict) if type and cast else value


def _set_dict_value(__m: Dict, __base: Dict, name: _KT, path: _KT, default=None,
                    apply: Apply=dict(), match: Match=dict(), query: Context=dict(), **context) -> Dict:
    if not _match_dict(__m, **dict(query, **match)): return __base
    value = _from_dict(__m, path, default=default, **context)
    __base[name] = _apply_schema(value, **dict(query, **apply))
    return __base


def _set_dict_tuple(__m: Dict, __base: Dict, name: _KT, path: Tuple, default=None,
                    apply: Apply=dict(), match: Match=dict(), query: Context=dict(), **context) -> Dict:
    __match = int(_match_dict(__m, **dict(query, **match)))-1
    value = _from_dict(__m, path[__match], default=default, **context)
    __base[name] = _apply_schema(value, **dict(query, **get_scala(apply, index=__match, default=dict())))
    return __base


def _set_dict_iterate(__m: Dict, __base: Dict, name: _KT, path: Sequence[_KT], default=None,
                    apply: Apply=dict(), match: Match=dict(), query: Context=dict(), **context) -> Dict:
    value = hier_get(__m, path[:-1])
    if not isinstance(value, Sequence):
        raise TypeError(INVALID_VALUE_TYPE_MSG(value, SEQUENCE, name))
    sub_path = path[-1] if (len(path) > 0) and is_array(path[-1]) else value
    context = dict(context, path=sub_path, default=default)
    value = [_from_dict(__e, **context) for __e in value if _match_dict(__e, **dict(query, **match))]
    __base[name] = [_apply_schema(__e, **dict(query, **apply)) for __e in value] if apply else value
    return __base


def _set_dict_global(__m: Dict, __base: Dict, name: Optional[_KT]=None,
                    apply: Apply=dict(), match: Match=dict(), query: Context=dict(), **context) -> Dict:
    if _match_dict(__m, **dict(query, **match)):
        data = _apply_schema(__m, **dict(query, **apply))
        if name: __base[name] = data
        elif isinstance(data, Dict):
            __base = dict(__base, **data)
        else: raise TypeError(INVALID_APPLY_RETURN_MSG(data, DICTIONARY, "GLOBAL"))
    return __base


def _match_dict(__m: Dict, func: Optional[MatchFunction]=None, path: Optional[_KT]=None,
                value: Optional[_VT]=None, flip=False, default=False, strict=False,
                query: Optional[_KT]=None, **context) -> bool:
    if not (func or path or value or query): return True
    elif query: __m = hier_get(context, query)
    elif path: __m = hier_get(__m, path)
    toggle = (lambda x: not x) if flip else (lambda x: bool(x))

    toggle = (lambda x: not x) if flip else (lambda x: bool(x))
    if func: return toggle(_apply_schema(__m, func, default=default, **context))
    elif isinstance(value, bool): return toggle(value)
    elif not_na(value, strict=True): return toggle(__m == value)
    else: return toggle(not_na(__m, strict=strict))


###################################################################
########################### Apply Schema ##########################
###################################################################

def _apply_schema(__object, func: Optional[ApplyFunction]=None, default=None, name=str(), **context) -> Any:
    if not func: return __object
    if isinstance(func, Callable): return safe_apply(__object, func, default, **context)
    elif isinstance(func, str): return _special_apply(__object, func, name, **context)
    else: raise TypeError(INVALID_APPLY_TYPE_MSG(func, name))


def _special_apply(__object, func: str, name=str(), **context) -> _VT:
    if func == __EXISTS__: return __exists__(__object, **context)
    elif func == __JOIN__: return __join__(__object, **context)
    elif func == __SPLIT__: return __split__(__object, **context)
    elif func == __MAP__: return __map__(__object, **context)
    else: raise ValueError(INVALID_APPLY_SPECIAL_MSG(func, name))


def __exists__(__object, keys: _KT=list(), **context):
    return exists_one(*kloc(__object, keys, if_null="drop", values_only=True), None)


def __join__(__object, keys: _KT=list(), sep=',', split=',', **context) -> str:
    if isinstance(__object, Sequence):
        __iterable = [cast_str((hier_get(__e, keys) if keys else __e), strict=True) for __e in __object]
        return str(sep).join([__e for __e in __iterable if __e])
    elif isinstance(__object, Dict):
        if isinstance(keys, str): __object = __object.get(keys)
        elif isinstance(keys, Sequence):
            if_null = "pass" if not keys else "drop"
            __object = kloc(__object, keys, if_null=if_null, values_only=True)
        else: return str()
        return __join__(__object, sep=sep, split=split)
    elif isinstance(__object, str):
        return __join__(list(map(lambda x: x.strip(), __object.split(split))), sep=sep)
    else: return str()


def __split__(__object, sep=',', maxsplit=-1, type: Optional[Type]=None, default=None, strict=True,
                index: Optional[int]=None, **context) -> Union[List,_VT]:
    __s = cast_str(__object, strict=True).split(sep, maxsplit)
    if type: __s = list(map(lambda x: cast_object(x, type, default=default, strict=strict), __s))
    return get_scala(__s, index) if isinstance(index, int) else __s


def __map__(__object, schema: Schema, root: Optional[_KT]=None, match: Optional[Match]=None,
            groupby: _KT=list(), groupSize: NestedDict=dict(), rankby: Optional[Literal["page","start"]]="start",
            page=1, start=1, submatch: Optional[MatchFunction]=None, discard=True, **context) -> Data:
    schemaInfo = SchemaInfo(schema=SchemaContext(schema=schema, root=root, match=match))
    if is_records(__object):
        return map_records(__object, schemaInfo, groupby, groupSize, rankby, submatch, discard, page=page, start=start, **context)
    elif isinstance(__object, Dict):
        return map_dict(__object, schemaInfo, submatch, discard, **context)
    else: return __object


###################################################################
######################### Parse DataFrame #########################
###################################################################

def map_df(df: pd.DataFrame, schemaInfo: SchemaInfo, groupby: _KT=list(), groupSize: NestedDict=dict(),
            rankby=str(), match: Optional[MatchFunction]=None, discard=False, **context) -> pd.DataFrame:
    context = SCHEMA_CONTEXT(**context)
    if groupby:
        return _groupby_df(df, schemaInfo, groupby, groupSize, rankby, discard, **context)
    __base = pd.DataFrame()
    df = _set_df(df, rankby, match, **context)
    for __key, schema_context in schemaInfo.items():
        schema, _, match = kloc(schema_context, SCHEMA_KEYS, if_null="pass")
        if isinstance(match, Dict) and _match_df(df, **match, **context).empty: continue
        __base = _map_df_schema(df, __base, schema, count=len(df), **context)
    return __base if discard else concat_df([__base, df], axis=1, keep="first")


def _groupby_df(df: pd.DataFrame, schemaInfo: SchemaInfo, groupby: _KT=list(), groupSize: NestedDict=dict(),
                rankby=str(), discard=False, dataSize: _PASS=None, **context) -> pd.DataFrame:
    context = dict(context, schemaInfo=schemaInfo, rankby=rankby, discard=discard)
    groups = groupby_df(df, by=groupby, if_null="drop")
    return pd.concat([
        map_df(df, dataSize=hier_get(groupSize, group), **context) for group, df in groups.items()])


def _set_df(df: pd.DataFrame, rankby=str(), match: Optional[MatchFunction]=None, **context) -> pd.DataFrame:
    start = get_start(len(df), rankby, **context)
    if isinstance(start, int) and (RANK not in df):
        df[RANK] = range(start, len(df)+start)
    return df[match(df, **context)] if match else df


def _map_df_schema(df: pd.DataFrame, __base: pd.DataFrame, schema: Schema, **context) -> pd.DataFrame:
    for field in schema:
        df = concat_df([__base, df], axis=1, keep="first")
        if (field[MODE] == QUERY) and (NAME in context):
            if not ((APPLY in field) or (MATCH in field)):
                __base[field[NAME]] = context[field[NAME]]
                continue
            else: df[field[NAME]] = context[field[NAME]]
        try: __base = _map_df_field(df, __base, **field, **context)
        except Exception as exception:
            print(EXCEPTION_ON_NAME_MSG(field[NAME]))
            raise exception
        if (field[MODE] == OPTIONAL) and (field[NAME] in __base) and pd.isna(__base[field[NAME]]):
            __base.drop(columns=field[NAME])
    return __base


def _map_df_field(df: pd.DataFrame, __base: pd.DataFrame, name: _KT, path: SchemaPath, how: Optional[PathType]=None,
                    type: Optional[Type]=None, mode: _PASS=None, cast=False, strict=True, default=None,
                    apply: Apply=dict(), match: Match=dict(), **context) -> Dict:
    path_type = how if how else _get_path_type(path)
    context = dict(type=type, cast=cast, strict=strict, default=default, apply=apply, match=match, query=context)
    if path_type in (PATH,CALLABLE): return _set_df_value(df, __base, name, path, **context)
    elif path_type == VALUE:
        __base[name] = path
        return __base
    elif path_type == TUPLE: return _set_df_tuple(df, __base, name, path, **context)
    elif path_type == GLOBAL: return _set_df_global(df, __base, name, **context)
    else: raise TypeError(INVALID_PATH_TYPE_MSG(path))


def _from_df(df: pd.DataFrame, path: SchemaPath, type: Optional[Type]=None,
            cast=False, strict=True, default=None, **context) -> pd.Series:
    default = _from_df(df, default) if is_array(default) else default
    if is_array(path): series = fillna_each(df[path[0]], default)
    elif isinstance(path, Callable): series = safe_apply_df(df, path, default, **context)
    else: series = pd.Series([path]*len(df), index=df.index)
    return safe_apply_df(series, cast_object, default, __type=type, strict=strict) if type and cast else series


def _set_df_value(df: pd.DataFrame, __base: pd.DataFrame, name: _KT, path: _KT, default=None,
                apply: Apply=dict(), match: Match=dict(), query: Context=dict(), **context) -> pd.DataFrame:
    if _match_df(df, **dict(query, **match)).empty: return __base
    series = _from_df(df, path, default=default, **context)
    if apply:
        series = safe_apply_df(series, apply[FUNC], **dict(query, **drop_dict(apply, FUNC)))
    if not is_df_sequence(series):
        raise TypeError(INVALID_VALUE_TYPE_MSG(series, PANDAS_SERIES, name))
    __base[name] = series
    return __base


def _set_df_tuple(df: pd.DataFrame, __base: pd.DataFrame, name: _KT, path: Tuple, default=None,
                apply: Apply=dict(), match: Match=dict(), query: Context=dict(), **context) -> pd.DataFrame:
    matches = _match_df(df, **dict(query, **match)).index
    __base[name] = default
    for __i, row in df.iterrows():
        __match = int(__i in matches)-1
        value = _from_dict(row, path[__match], default=default, **context)
        if apply: value = _apply_schema(value, **dict(query, **get_scala(apply, index=__match, default=dict())))
        __base.at[__i,name] = value
    return __base


def _set_df_global(df: pd.DataFrame, __base: pd.DataFrame, name: Optional[_KT]=None,
                    apply: Apply=dict(), match: Match=dict(), query: Context=dict(), **context) -> pd.DataFrame:
    df = _match_df(df, **dict(query, **match))
    if not df.empty:
        data = safe_apply_df(df, apply[FUNC], **dict(query, **drop_dict(apply, FUNC)))
        if name and is_df_sequence(data):
            __base[name] = data
        elif is_df(data): df = pd.concat([df, data], axis=0)
        else: raise TypeError(INVALID_APPLY_RETURN_MSG(data, PANDAS_OBJECT, "GLOBAL"))
    return __base


def _match_df(df: pd.DataFrame, func: Optional[MatchFunction]=None, path: Optional[_KT]=None,
            value: Optional[_VT]=None, flip=False, default=False, strict=False,
            query: Optional[_KT]=None, **context) -> pd.DataFrame:
    if not (func or path or value or query): return df
    elif query: return _match_dict(context, func, query, value, flip, default, strict)
    elif path: df = df[path]

    if func: condition = safe_apply_df(df, func, **context)
    elif isinstance(value, bool): return df if value else pd.DataFrame(columns=df.columns)
    elif not_na(value, strict=True): condition = (df == value)
    else: condition = safe_apply_df(df, lambda x: not_na(x, strict=strict), by="cell")

    if isinstance(condition, pd.DataFrame):
        return df[~df.any(axis=1)] if flip else df[df.any(axis=1)]
    else: return df[~condition] if flip else df[condition]


###################################################################
########################### Parse Source ##########################
###################################################################

def map_source(source: HtmlData, schemaInfo: SchemaInfo, groupby: Union[_KT,Dict]=str(),
                groupSize: Dict[bool,int]=dict(), rankby=str(),
                match: Optional[MatchFunction]=None, **context) -> MappingData:
    context = SCHEMA_CONTEXT(**context)
    if not is_array(source):
        return _map_html(source, schemaInfo, **context)
    elif groupby:
        return _groupby_source(source, schemaInfo, groupby, groupSize, rankby, **context)
    data = list()
    start = get_start(len(source), rankby, **context)
    for __i, __t in enumerate(source, start=(start if start else 0)):
        if not isinstance(__t, Tag) or (match and not match(source, **context)): continue
        context[RANK] = __i
        data.append(_map_html(__t, schemaInfo, count=len(source), **context))
    return data


def _groupby_source(source: List[Tag], schemaInfo: SchemaInfo, groupby: Union[_KT,Dict]=str(),
                    groupSize: Dict[bool,int]=dict(), rankby=str(),
                    group: _PASS=None, dataSize: _PASS=None, **context) -> Records:
    context = dict(context, schemaInfo=schemaInfo, rankby=rankby)
    groups = groupby_source(source, groupby, how="exact")
    return union(*[
        map_source(source, group=group, dataSize=hier_get(groupSize, group), **context)
            for group, source in groups.items()])


def _map_html(source: Tag, schemaInfo: SchemaInfo, match: Optional[MatchFunction]=None, **context) -> Dict:
    __base = dict()
    if match and not match(source, **context): return __base
    for __key, schema_context in schemaInfo.items():
        schema, root, match = kloc(schema_context, SCHEMA_KEYS, if_null="pass", values_only=True)
        div = select_one(source, root) if root else source
        if not div: continue
        elif not isinstance(div, Tag):
            raise TypeError(INVALID_DATA_TYPE_MSG(div, HTML_SOURCE))
        if isinstance(match, Dict) and not _match_html(source, **match, **context): continue
        __base = _map_html_schema(div, __base, schema, **context)
    return __base


def _map_html_schema(source: Tag, __base: Dict, schema: Schema, **context) -> Dict:
    for field in schema:
        if field[MODE] in (QUERY, INDEX):
            if field[MODE] == INDEX: __base[field[NAME]] = context.get(field[NAME])
            else: __base = _map_dict_field(dict(context, **__base), __base, **field)
            continue
        try: __base = _map_html_field(source, __base, **field, **context)
        except Exception as exception:
            print(EXCEPTION_ON_NAME_MSG(field[NAME]))
            raise exception
        if (field[MODE] == OPTIONAL) and (field[NAME] in __base) and pd.isna(__base[field[NAME]]):
            __base.pop(field[NAME])
    return __base


def _map_html_field(source: Tag, __base: Dict, name: _KT, path: SchemaPath, how: Optional[PathType]=None,
                    type: Optional[Type]=None, mode: _PASS=None, description: _PASS=None, cast=False, strict=True,
                    default=None, apply: Apply=dict(), match: Match=dict(), **context) -> Dict:
    path_type = how if how else _get_path_type(path)
    context = dict(type=type, cast=cast, strict=strict, default=default, apply=apply, match=match, query=context)
    if path_type in (PATH,CALLABLE): return _set_html_value(source, __base, name, path, **context)
    elif path_type == VALUE: return dict(__base, **{name: path})
    elif path_type == TUPLE: return _set_html_tuple(source, __base, name, path, **context)
    elif path_type == ITERATE: return _set_html_iterate(source, __base, name, path, **context)
    elif path_type == GLOBAL: return _set_html_global(source, __base, name, **context)
    else: raise TypeError(INVALID_PATH_TYPE_MSG(path))


def _from_html(source: Tag, path: SchemaPath, type: Optional[Type]=None,
                cast=False, strict=True, default=None, **context) -> Union[Tag,Any]:
    default = _from_html(source, default) if not_na(default) else default
    if is_array(path): value = hier_select(source, path, default, empty=False)
    elif isinstance(path, Callable): value = safe_apply(source, path, default, **context)
    else: value = path
    return cast_object(value, type, default=default, strict=strict) if type and cast else value


def _set_html_value(source: Tag, __base: Dict, name: _KT, path: _KT, default=None,
                    apply: Apply=dict(), match: Match=dict(), query: Context=dict(), **context) -> Dict:
    if not _match_html(source, **dict(query, **match)): return __base
    value = _from_html(source, path, default=default, **context)
    __base[name] = _apply_schema(value, **dict(query, **apply))
    return __base


def _set_html_tuple(source: Tag, __base: Dict, name: _KT, path: Tuple, default=None,
                    apply: Apply=dict(), match: Match=dict(), query: Context=dict(), **context) -> Dict:
    __match = int(_match_html(source, **dict(query, **match)))-1
    value = _from_html(source, path[__match], default=default, **context)
    __base[name] = _apply_schema(value, **dict(query, **get_scala(apply, index=__match, default=dict())))
    return __base


def _set_html_iterate(source: Tag, __base: Dict, name: _KT, path: Sequence[_KT], default=None,
                    apply: Apply=dict(), match: Match=dict(), query: Context=dict(), **context) -> Dict:
    source = hier_select(source, path[:-1])
    if not isinstance(source, Sequence):
        raise TypeError(INVALID_VALUE_TYPE_MSG(value, SEQUENCE, name))
    sub_path = path[-1] if (len(path) > 0) and is_array(path[-1]) else value
    context = dict(context, path=sub_path, default=default)
    value = [_from_html(__t, **context) for __t in source if _match_html(__t, **dict(query, **match))]
    __base[name] = [_apply_schema(__e, **dict(query, **apply)) for __e in value] if apply else value
    return __base


def _set_html_global(source: Tag, __base: Dict, name: Optional[_KT]=None,
                    apply: Apply=dict(), match: Match=dict(), query: Context=dict(), **context) -> Dict:
    if _match_html(source, **dict(query, **match)):
        data = _apply_schema(source, **dict(query, **apply))
        if name: __base[name] = data
        elif isinstance(data, Dict):
            __base = dict(__base, **data)
        else: raise TypeError(INVALID_APPLY_RETURN_MSG(data, DICTIONARY, "GLOBAL"))
    return __base


def _match_html(source: Tag, func: Optional[MatchFunction]=None, path: Optional[_KT]=None,
                value: Optional[_VT]=None, flip=False, default=False, strict=False,
                query: Optional[_KT]=None, **context) -> bool:
    if not (func or path or value or query): return True
    elif query: return _match_dict(context, func, query, value, flip, default, strict)

    toggle = (lambda x: not x) if flip else (lambda x: bool(x))
    if func: return toggle(_apply_schema(hier_select(source, path), func, default=default, **context))
    elif isinstance(value, bool): return toggle(value)
    elif not_na(value, strict=True):
        return toggle(include_source(source, path, value=value, how="exact"))
    else: return toggle(exists_source(source, path, strict=strict))
