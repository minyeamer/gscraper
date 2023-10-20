from __future__ import annotations
from gscraper.base import SCHEMA_CONTEXT
from gscraper.base.session import TypedDict, TypedRecords, BaseSession, custom_str

from gscraper.base.types import _KT, _VT, _PASS, Context, LogLevel, TypeHint, TypeList
from gscraper.base.types import IndexLabel, Timezone, RenameMap, RegexFormat
from gscraper.base.types import Records, NestedDict, MappingData, Data, JsonData, HtmlData, ApplyFunction, MatchFunction
from gscraper.base.types import get_type, init_origin, is_type, is_float_type
from gscraper.base.types import is_numeric_type, is_numeric_or_date_type, is_bool_type
from gscraper.base.types import is_array, is_records, is_json_object, is_df, is_df_sequence

from gscraper.utils import isna, notna, exists
from gscraper.utils.cast import cast_object, cast_str, cast_list, cast_tuple, cast_float, cast_int
from gscraper.utils.date import is_daily_frequency
from gscraper.utils.logs import log_data
from gscraper.utils.map import exists_one, howin, safe_apply, get_scala, re_get, union
from gscraper.utils.map import kloc, isin_dict, is_single_path, chain_dict, drop_dict, hier_get
from gscraper.utils.map import vloc, groupby_records, drop_duplicates
from gscraper.utils.map import cloc, concat_df, fillna_each, safe_apply_df, groupby_df, filter_data, set_data, isin_df, match_df
from gscraper.utils.map import select_one, select_by, hier_select, isin_source, is_single_selector, groupby_source

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
__REGEX__ = "__REGEX__"
__RENAME__ = "__RENAME__"
__SPLIT__ = "__SPLIT__"
__STAT__ = "__STAT__"
__SUM__ = "__SUM__"
__MAP__ = "__MAP__"


###################################################################
############################# Messages ############################
###################################################################

INVALID_OBJECT_MSG = lambda __object, __name: f"'{__object}' is not a valid {__name} object."
INVALID_OBJECT_TYPE_MSG = lambda __object, __type: f"'{type(__object)}' is not a valid type for {__type} object."

INVALID_DATA_TYPE_MSG = lambda data, __type: f"'{type(data)}' is not a valid data type for parsing {__type}."
INVALID_VALUE_TYPE_MSG = lambda ret, __t, name=str(): \
    f"'{type(ret)}' type is not a valid value type{FOR_NAME(name)}. '{__t}' type is desired."

INVALID_PATH_TYPE_MSG = lambda path: f"'{path}' is not supported type for schema path."

FOR_NAME = lambda name: f" for the '{name}'" if name else str()

INVALID_APPLY_TYPE_MSG = lambda apply, name=str(): f"'{type(apply)}' is not a valid apply object type{FOR_NAME(name)}."
INVALID_APPLY_SPECIAL_MSG = lambda func, name=str(): f"'{func}' is not a valid special apply function{FOR_NAME(name)}."
INVALID_APPLY_RETURN_MSG = lambda ret, __t, name=str(): \
    f"'{type(ret)}' type is not a valid return type of apply function{FOR_NAME(name)}. '{__t}' type is desired."
INVALID_MATCH_KEY_MSG = lambda name=str(): f"Match function{FOR_NAME(name)} requires at least one parameter: func, path, and query."

EXCEPTION_ON_NAME_MSG = lambda name: f"Exception occured from '{name}'."

INFO_OBJECT = "SchemaInfo"
CONTEXT_OBJECT = "SchemaContext"
SCHEMA_OBJECT = "Schema"
FIELD_OBJECT = "SchemaField"
APPLY_FUNCTION = "ApplyFunction"
MATCH_FUNCTION = "MatchFunction"

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
        super().__init__(func=__STAT__, stat=sum, keys=keys,
            default=default, hier=hier, type=type, strict=strict)


class Map(Apply):
    def __init__(self, schema: Schema, root: Optional[_KT]=None, match: Optional[Match]=None,
                groupby: Optional[_KT]=None, groupSize: Optional[NestedDict]=None,
                rankby: Literal["page","start"]="start", page=1, start=1,
                submatch: Optional[MatchFunction]=None, discard=True) -> Data:
        super().__init__(func=__MAP__, schema=schema)
        self.update_default(dict(rankby="start", page=1, start=1, discard=True),
            root=root, match=match, groupby=groupby, groupSize=groupSize,
            rankby=rankby, page=page, start=start, submatch=submatch, discard=discard)


###################################################################
########################### Schema Field ##########################
###################################################################

SchemaPath = Union[_KT, _VT, Tuple[_KT,_KT], Tuple[_VT,_VT], Callable]
PathType = Literal["path", "value", "tuple", "iterate", "callable", "global"]

class Field(TypedDict):
    def __init__(self, name: _KT, path: SchemaPath, type: TypeHint, mode: str, desc: Optional[str]=None,
                cast=False, strict=True, default: Optional[Any]=None, apply: Optional[Apply]=None,
                match: Optional[Match]=None, how: Optional[PathType]=None, description: Optional[str]=None):
        super().__init__(name=name, path=path, type=type, mode=mode)
        self.update_default(dict(cast=False, strict=True),
            description=(desc if desc else description), cast=cast, strict=strict, default=default,
            apply=to_apply_func(apply), match=to_match_func(match), how=(how if how else get_path_type(path)))
        self.init_field()

    def init_field(self):
        self[TYPE] = get_type(self[TYPE], argidx=-1)
        if self[MODE] == INDEX: pass
        elif self[MODE] == NOTNULL:
            self[DEFAULT] = init_origin(self[TYPE])
            self[CAST] = True
        elif self[MODE] == NOTZERO:
            self[CAST] = True
            self[STRICT] = False
        elif is_numeric_or_date_type(self[TYPE]) and (CAST not in self):
            self[CAST] = True
            if is_bool_type(self[TYPE]):
                self[STRICT] = False
        else: pass


def validate_field(field: Any) -> Field:
    if isinstance(field, Field): return field
    elif isinstance(field, Dict): return Field(**field)
    else: raise TypeError(INVALID_OBJECT_TYPE_MSG(field, FIELD_OBJECT))


def get_path_type(path: SchemaPath) -> PathType:
    if isinstance(path, str): return VALUE
    elif isinstance(path, Sequence):
        if not path: return GLOBAL
        if isinstance(path, Tuple) and len(path) == 2: return TUPLE
        elif len(path) > 0 and is_array(path[-1]): return ITERATE
        else: return PATH
    elif isinstance(path, Callable): return CALLABLE
    else: raise TypeError(INVALID_PATH_TYPE_MSG(path))


def to_apply_func(apply: Any) -> Apply:
    if isinstance(apply, Apply) or (not apply): return apply
    elif isinstance(apply, (Callable,str)): return Apply(func=apply)
    elif isinstance(apply, Dict): return Apply(**apply)
    elif isinstance(apply, Tuple):
        return tuple(to_apply_func(func) for func in apply)
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

    def __str__(self, indent=2, step=2) -> str:
        str_value = lambda __k, __v: (
            custom_str(__v, indent=indent, step=step) if __k != "match" else str(dict(__v)))
        return '{\n'+',\n'.join([' '*indent+f"'{__k}': {str_value(__k, __v)}"
                for __k, __v in self.items()])+'\n'+' '*(indent-step)+'}'


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
                debug: List[str]=list(), extraSave: List[str]=list(), interrupt=str(), localSave=False, **context):
        BaseSession.__init__(self, **self.from_locals(locals()))
        self.update(schemaInfo=validate_info(self.schemaInfo))

    def get_rename_map(self, to: Optional[Literal["desc","name"]]=None, schema_names: _KT=list(),
                        keep: Literal["fist","last",False]="first", renameMap: RenameMap=dict(), **context) -> RenameMap:
        if to in ("desc", "name"):
            __from, __to = ("desc", "name") if to == "name" else ("name", "desc")
            schema = self.schemaInfo.get_schema(schema_names=schema_names, keep=keep)
            return {field[__from]: field[__to] for field in schema}
        else: return renameMap

    ###################################################################
    ######################## Response Validator #######################
    ###################################################################

    def validate_response(func):
        @functools.wraps(func)
        def wrapper(self: Parser, response: Any, *args, **context):
            is_valid = self.is_valid_response(response)
            data = func(self, response, *args, **context) if is_valid else init_origin(func)
            suffix = f"_{context['index']}" if context.get("index") else str()
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
            updateDate = context.get("startDate") if is_daily_frequency(context.get("interval")) else self.today()
            data = set_data(data, if_exists="ignore", updateDate=updateDate, updateTime=self.now())
        return filter_data(data, fields=fields, if_null="pass")


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
        if (field[MODE] == OPTIONAL) and (field[NAME] in __base) and isna(__base[field[NAME]]):
            __base.pop(field[NAME])
    return __base


def _map_dict_field(__m: Dict, __base: Dict, name: _KT, path: SchemaPath, how: Optional[PathType]=None,
                    type: Optional[TypeHint]=None, mode: _PASS=None, description: _PASS=None, cast=False, strict=True,
                    default=None, apply: Apply=dict(), match: Match=dict(), **context) -> Dict:
    path_type = how if how else get_path_type(path)
    context = dict(type=type, cast=cast, strict=strict, default=default, apply=apply, match=match, query=context)
    if path_type in (PATH,CALLABLE): return _set_dict_value(__m, __base, name, path, **context)
    elif path_type == VALUE: return dict(__base, **{name: path})
    elif path_type == TUPLE: return _set_dict_tuple(__m, __base, name, path, **context)
    elif path_type == ITERATE: return _set_dict_iterate(__m, __base, name, path, **context)
    elif path_type == GLOBAL: return _set_dict_global(__m, __base, name, **context)
    else: raise TypeError(INVALID_PATH_TYPE_MSG(path))


def _cast_value(value: _VT, type: Optional[TypeHint]=None, default=None, strict=True,
                cast=True, trunc=2, **context) -> _VT:
    context = dict(context, default=default, strict=strict, trunc=trunc)
    return cast_object(value, type, **context) if type and cast else value


def _from_dict(__m: Dict, path: SchemaPath, type: Optional[TypeHint]=None, cast=False, strict=True,
                default=None, apply: Apply=dict(), query: Context=dict()) -> _VT:
    default = _from_dict(__m, default) if notna(default) else default
    if is_array(path): value = hier_get(__m, path, default, empty=False)
    elif isinstance(path, Callable): value = safe_apply(__m, path, default, **query)
    else: value = path
    value = apply_schema(value, **dict(query, **apply)) if apply else value
    return _cast_value(value, type, default=default, strict=strict, cast=cast, **query)


def _set_dict_value(__m: Dict, __base: Dict, name: _KT, path: _KT, type: Optional[TypeHint]=None,
                    cast=False, strict=True, default=None, apply: Apply=dict(), match: Match=dict(),
                    query: Context=dict()) -> Dict:
    if not _match_dict(__m, **dict(query, **match)): return __base
    __base[name] = _from_dict(__m, path, type, cast, strict, default, apply, query)
    return __base


def _set_dict_tuple(__m: Dict, __base: Dict, name: _KT, path: Tuple, type: Optional[TypeHint]=None,
                    cast=False, strict=True, default=None, apply: Apply=dict(), match: Match=dict(),
                    query: Context=dict()) -> Dict:
    __match = int(_match_dict(__m, **dict(query, **match)))-1
    __apply = get_scala(apply, index=__match, default=dict())
    __base[name] = _from_dict(__m, path[__match], type, cast, strict, default, __apply, query)
    return __base


def _set_dict_iterate(__m: Dict, __base: Dict, name: _KT, path: Sequence[_KT], type: Optional[TypeHint]=None,
                    cast=False, strict=True, default=None, apply: Apply=dict(), match: Match=dict(),
                    query: Context=dict()) -> Dict:
    value = hier_get(__m, path[:-1])
    if not isinstance(value, Sequence):
        raise TypeError(INVALID_VALUE_TYPE_MSG(value, SEQUENCE, name))
    sub_path = path[-1] if (len(path) > 0) and is_array(path[-1]) else value
    args = (sub_path, type, cast, strict, default, apply, query)
    __base[name] = [_from_dict(__e, *args) for __e in value if _match_dict(__e, **dict(query, **match))]
    return __base


def _set_dict_global(__m: Dict, __base: Dict, name: Optional[_KT]=None, type: Optional[TypeHint]=None,
                    cast=False, strict=True, default=None, apply: Apply=dict(), match: Match=dict(),
                    query: Context=dict()) -> Dict:
    if _match_dict(__m, **dict(query, **match)):
        data = apply_schema(__m, **dict(query, **apply))
        if name:
            __base[name] = _cast_value(data, type, default=default, strict=strict, cast=cast, **query)
        elif isinstance(data, Dict):
            __base = dict(__base, **data)
        else: raise TypeError(INVALID_APPLY_RETURN_MSG(data, DICTIONARY, "GLOBAL"))
    return __base


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
        if (field[MODE] == OPTIONAL) and (field[NAME] in __base) and isna(__base[field[NAME]]):
            __base.drop(columns=field[NAME])
    return __base


def _map_df_field(df: pd.DataFrame, __base: pd.DataFrame, name: _KT, path: SchemaPath, how: Optional[PathType]=None,
                type: Optional[TypeHint]=None, mode: _PASS=None, description: _PASS=None, cast=False, strict=True, default=None,
                apply: Apply=dict(), match: Match=dict(), **context) -> Dict:
    path_type = how if how else get_path_type(path)
    context = dict(type=type, cast=cast, strict=strict, default=default, apply=apply, match=match, query=context)
    if path_type in (PATH,CALLABLE): return _set_df_value(df, __base, name, path, **context)
    elif path_type == VALUE:
        __base[name] = path
        return __base
    elif path_type == TUPLE: return _set_df_tuple(df, __base, name, path, **context)
    elif path_type == GLOBAL: return _set_df_global(df, __base, name, **context)
    else: raise TypeError(INVALID_PATH_TYPE_MSG(path))


def _cast_df(df: Union[pd.DataFrame,pd.Series], type: Optional[TypeHint]=None, default=None, strict=True,
            cast=True, trunc=2, **context) -> Union[pd.DataFrame,pd.Series]:
    context = dict(context, default=default, strict=strict, trunc=trunc)
    df = safe_apply_df(df, cast_object, default, **context) if type and cast else df


def _from_df(df: pd.DataFrame, path: SchemaPath, type: Optional[TypeHint]=None, cast=False, strict=True,
            default=None, apply: Apply=dict(), query: Context=dict()) -> pd.Series:
    default = _from_df(df, default) if is_array(default) else default
    if is_array(path): series = fillna_each(df[path[0]], default)
    elif isinstance(path, Callable): series = safe_apply_df(df, path, default, **query)
    else: series = pd.Series([path]*len(df), index=df.index)
    series = safe_apply_df(series, apply[FUNC], **dict(query, **drop_dict(apply, FUNC))) if apply else series
    return _cast_df(df, type, default=default, strict=strict, cast=cast, **query)


def _set_df_value(df: pd.DataFrame, __base: pd.DataFrame, name: _KT, path: _KT, type: Optional[TypeHint]=None,
                cast=False, strict=True, default=None, apply: Apply=dict(), match: Match=dict(),
                query: Context=dict()) -> pd.DataFrame:
    if _match_df(df, **dict(query, **match)).empty: return __base
    series = _from_df(df, path, type, cast, strict, default, apply, query)
    if not (is_df_sequence(series) and (len(series) != len(__base))):
        raise TypeError(INVALID_VALUE_TYPE_MSG(series, PANDAS_SERIES, name))
    __base[name] = series
    return __base


def _set_df_tuple(df: pd.DataFrame, __base: pd.DataFrame, name: _KT, path: Tuple, type: Optional[TypeHint]=None,
                cast=False, strict=True, default=None, apply: Apply=dict(), match: Match=dict(),
                query: Context=dict()) -> pd.DataFrame:
    matches = _match_df(df, **dict(query, **match)).index
    __base[name] = default
    for __i, row in df.iterrows():
        __match = int(__i in matches)-1
        __apply = get_scala(apply, index=__match, default=dict())
        value = _from_dict(row, path[__match], type, cast, strict, default, __apply, query)
        __base.at[__i,name] = value
    return __base


def _set_df_global(df: pd.DataFrame, __base: pd.DataFrame, name: Optional[_KT]=None, type: Optional[TypeHint]=None,
                    cast=False, strict=True, default=None, apply: Apply=dict(), match: Match=dict(),
                    query: Context=dict()) -> pd.DataFrame:
    df = _match_df(df, **dict(query, **match))
    if not df.empty:
        data = safe_apply_df(df, apply[FUNC], **dict(query, **drop_dict(apply, FUNC)))
        if name and is_df_sequence(data):
            __base[name] = _cast_df(data, type, default=default, strict=strict, cast=cast, **query)
        elif is_df(data): df = pd.concat([df, data], axis=0)
        else: raise TypeError(INVALID_APPLY_RETURN_MSG(data, PANDAS_OBJECT, "GLOBAL"))
    return __base


###################################################################
########################### Parse Source ##########################
###################################################################

def map_source(source: HtmlData, schemaInfo: SchemaInfo, groupby: Union[_KT,Dict]=str(),
                groupSize: Dict[bool,int]=dict(), rankby=str(),
                match: Optional[MatchFunction]=None, **context) -> MappingData:
    context = SCHEMA_CONTEXT(**context)
    if not is_array(source):
        return map_html(source, schemaInfo, **context)
    elif groupby:
        return _groupby_source(source, schemaInfo, groupby, groupSize, rankby, **context)
    data = list()
    start = get_start(len(source), rankby, **context)
    for __i, __t in enumerate(source, start=(start if start else 0)):
        if not isinstance(__t, Tag) or (match and not match(source, **context)): continue
        context[RANK] = __i
        data.append(map_html(__t, schemaInfo, count=len(source), **context))
    return data


def _groupby_source(source: List[Tag], schemaInfo: SchemaInfo, groupby: Union[_KT,Dict]=str(),
                    groupSize: Dict[bool,int]=dict(), rankby=str(),
                    group: _PASS=None, dataSize: _PASS=None, **context) -> Records:
    context = dict(context, schemaInfo=schemaInfo, rankby=rankby)
    groups = groupby_source(source, groupby, if_null="drop")
    return union(*[
        map_source(source, group=group, dataSize=hier_get(groupSize, group), **context)
            for group, source in groups.items()])


def map_html(source: Tag, schemaInfo: SchemaInfo, match: Optional[MatchFunction]=None, **context) -> Dict:
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
        if (field[MODE] == OPTIONAL) and (field[NAME] in __base) and isna(__base[field[NAME]]):
            __base.pop(field[NAME])
    return __base


def _map_html_field(source: Tag, __base: Dict, name: _KT, path: SchemaPath, how: Optional[PathType]=None,
                    type: Optional[TypeHint]=None, mode: _PASS=None, description: _PASS=None, cast=False, strict=True,
                    default=None, apply: Apply=dict(), match: Match=dict(), **context) -> Dict:
    path_type = how if how else get_path_type(path)
    context = dict(type=type, cast=cast, strict=strict, default=default, apply=apply, match=match, query=context)
    if path_type in (PATH,CALLABLE): return _set_html_value(source, __base, name, path, **context)
    elif path_type == VALUE: return dict(__base, **{name: path})
    elif path_type == TUPLE: return _set_html_tuple(source, __base, name, path, **context)
    elif path_type == ITERATE: return _set_html_iterate(source, __base, name, path, **context)
    elif path_type == GLOBAL: return _set_html_global(source, __base, name, **context)
    else: raise TypeError(INVALID_PATH_TYPE_MSG(path))


def _from_html(source: Tag, path: SchemaPath, type: Optional[TypeHint]=None, cast=False, strict=True,
                default=None, apply: Apply=dict(), query: Context=dict()) -> Union[Tag,Any]:
    default = _from_html(source, default) if notna(default) else default
    if is_array(path): value = hier_select(source, path, default, empty=False)
    elif isinstance(path, Callable): value = safe_apply(source, path, default, **query)
    else: value = path
    value = apply_schema(value, **dict(query, **apply)) if apply else value
    return _cast_value(value, type, default=default, strict=strict, cast=cast, **query)


def _set_html_value(source: Tag, __base: Dict, name: _KT, path: _KT, type: Optional[TypeHint]=None,
                    cast=False, strict=True, default=None, apply: Apply=dict(), match: Match=dict(),
                    query: Context=dict()) -> Dict:
    if not _match_html(source, **dict(query, **match)): return __base
    __base[name] = _from_html(source, path, type, cast, strict, default, apply, query)
    return __base


def _set_html_tuple(source: Tag, __base: Dict, name: _KT, path: Tuple, type: Optional[TypeHint]=None,
                    cast=False, strict=True, default=None, apply: Apply=dict(), match: Match=dict(),
                    query: Context=dict()) -> Dict:
    __match = int(_match_html(source, **dict(query, **match)))-1
    __apply = get_scala(apply, index=__match, default=dict())
    __base[name] = _from_html(source, path[__match], type, cast, strict, default, __apply, query)
    return __base


def _set_html_iterate(source: Tag, __base: Dict, name: _KT, path: Sequence[_KT], type: Optional[TypeHint]=None,
                    cast=False, strict=True, default=None, apply: Apply=dict(), match: Match=dict(),
                    query: Context=dict()) -> Dict:
    source = hier_select(source, path[:-1])
    if not isinstance(source, Sequence):
        raise TypeError(INVALID_VALUE_TYPE_MSG(value, SEQUENCE, name))
    sub_path = path[-1] if (len(path) > 0) and is_array(path[-1]) else value
    args = (sub_path, type, cast, strict, default, apply, query)
    value = [_from_html(__t, *args) for __t in source if _match_html(__t, **dict(query, **match))]
    return __base


def _set_html_global(source: Tag, __base: Dict, name: Optional[_KT]=None, type: Optional[TypeHint]=None,
                    cast=False, strict=True, default=None, apply: Apply=dict(), match: Match=dict(),
                    query: Context=dict()) -> Dict:
    if _match_html(source, **dict(query, **match)):
        data = apply_schema(source, **dict(query, **apply))
        if name:
            __base[name] = _cast_value(data, type, default=default, strict=strict, cast=cast, **query)
        elif isinstance(data, Dict):
            __base = dict(__base, **data)
        else: raise TypeError(INVALID_APPLY_RETURN_MSG(data, DICTIONARY, "GLOBAL"))
    return __base


###################################################################
########################### Apply Schema ##########################
###################################################################

def apply_schema(__object, func: Optional[ApplyFunction]=None, default=None, name=str(), **context) -> Any:
    if not func: return __object
    elif isinstance(func, Callable): return safe_apply(__object, func, default, **context)
    elif isinstance(func, str): return _special_apply(__object, func, name, **context)
    else: raise TypeError(INVALID_APPLY_TYPE_MSG(func, name))


def _special_apply(__object, func: str, name=str(), **context) -> _VT:
    if func == __EXISTS__: return __exists__(__object, **context)
    elif func == __JOIN__: return __join__(__object, **context)
    elif func == __REGEX__: return __regex__(__object, **context)
    elif func == __RENAME__: return __rename__(__object, **context)
    elif func == __SPLIT__: return __split__(__object, **context)
    elif func == __STAT__: return __stat__(__object, **context)
    elif func == __SUM__: return __stat__(__object, **context)
    elif func == __MAP__: return __map__(__object, **context)
    else: raise ValueError(INVALID_OBJECT_MSG(func, name))


def __exists__(__object, keys: _KT=list(), default=None, hier=False, **context) -> Any:
    if keys: __object = filter_data(__object, cast_list(keys), if_null="drop", values_only=True, hier=hier)
    return exists_one(*__object, default) if is_array(__object) else default


def __join__(__object, keys: _KT=list(), sep=',', default=None, hier=False,
            strip=True, split: Optional[str]=None, **context) -> str:
    if keys: __object = filter_data(__object, cast_list(keys), if_null="drop", values_only=True, hier=hier)
    if split: __object = cast_str(__object).split(split)
    if not is_array(__object): return default
    __object = [__s for __s in [cast_str(__e, strict=True, strip=strip) for __e in __object] if __s]
    return sep.join(__object) if __object else default


def __regex__(__object, pattern: RegexFormat, how: Literal["search","findall","sub"]="search",
                default=None, index: Optional[int]=0, repl: Optional[str]=None, **context) -> str:
    __object = cast_str(__object, strict=True)
    __pattern = pattern if isinstance(pattern, re.Pattern) else re.compile(pattern)
    if how == "sub": return __pattern.sub(repl, __object)
    elif how == "findall": index = None
    return re_get(pattern, __object, default=default, index=index)


def __rename__(__object, rename: RenameMap, path: _KT=list(),
                if_null: Union[Literal["null","pass","error"],Any]="null", **context) -> str:
    if if_null == "null": value = rename.get(__object)
    elif if_null == "pass": value = rename.get(__object, __object)
    elif if_null == "error": value = rename[__object]
    else: value = rename.get(__object, if_null)
    return hier_get(value, path) if path else value


def __split__(__object, sep=',', maxsplit=-1, default=None, strict=True, index: Optional[int]=None,
                type: Optional[TypeHint]=None, cast: _PASS=True, **context) -> Union[List,_VT]:
    __object = cast_str(__object, strict=True).split(sep, maxsplit)
    if type: __object = [_cast_value(__e, type, default=default, strict=strict, **context) for __e in __object]
    return get_scala(__object, index) if isinstance(index, int) else __object


def __stat__(__object, stat: Callable, keys: _KT=list(), default=None, hier=False,
            type: Optional[TypeHint]=None, strict=True, cast: _PASS=True, **context) -> Union[Any,int,float]:
    if keys: __object = filter_data(__object, cast_list(keys), if_null="drop", values_only=True, hier=hier)
    if not is_array(__object): return default
    elif is_numeric_type(type):
        __cast = cast_float if is_float_type(type) else cast_int
        __object = [__cast(__e, strict=strict) for __e in __object]
    else: __object = [__n for __n in __object if isinstance(__n, (float,int))]
    return stat(__object) if __object else default


def __map__(__object, schema: Schema, root: Optional[_KT]=None, match: Optional[Match]=None,
            groupby: _KT=list(), groupSize: NestedDict=dict(), rankby: Optional[Literal["page","start"]]="start",
            page=1, start=1, submatch: Optional[MatchFunction]=None, discard=True, **context) -> Data:
    schemaInfo = SchemaInfo(schema=SchemaContext(schema=schema, root=root, match=match))
    context = dict(context, submatch=submatch, discard=discard)
    if is_records(__object):
        return map_records(__object, schemaInfo, groupby, groupSize, rankby, page=page, start=start, **context)
    elif isinstance(__object, Dict):
        return map_dict(__object, schemaInfo, **context)
    else: return __object


###################################################################
########################### Match Schema ##########################
###################################################################

def toggle(__bool: bool, flip=False) -> bool:
    return (not __bool) if flip else bool(__bool)


def _match_dict(__m: Dict, func: Optional[MatchFunction]=None, path: Optional[Union[_KT,Tuple[_KT]]]=None,
                query: Optional[Union[_KT,Tuple[_KT]]]=None, value: Optional[Any]=None,
                exact: Optional[_VT]=None, include: Optional[_VT]=None, exclude: Optional[_VT]=None,
                flip=False, strict=False, how: Literal["any","all"]="any", if_null=False,
                hier=True, default=False, **context) -> bool:
    if isna(func) and isna(path) and isna(query): return True
    elif query: return _match_query(**locals())
    elif func:
        if notna(path): __m = hier_get(__m, path) if hier else kloc(__m, path)
        return toggle(apply_schema(__m, func, default=default, **context), flip=flip)
    elif isna(exact) and isna(include) and isna(exclude):
        if not is_single_path(path, hier=hier):
            return howin([_match_dict(**dict(locals(), path=__k)) for __k in path], how=how)
        __value = hier_get(__m, path) if hier else kloc(__m, path)
        if isna(__value): return toggle(if_null, flip=flip)
        elif notna(value): return toggle((__value == value), flip=flip)
        else: return toggle(exists(__value, strict=strict), flip=flip)
    exact = value if notna(value) else exact
    return toggle(isin_dict(__m, path, exact, include, exclude, how=how, if_null=if_null, hier=hier), flip=flip)


def _match_query(context: Context, query: _KT, __m: _PASS=None, source: _PASS=None, path: _PASS=None, **kwargs) -> bool:
    return _match_dict(context, query, **kwargs)


def toggle_df(df: pd.DataFrame, __bool: Union[pd.DataFrame,pd.Series,bool], flip=False) -> pd.DataFrame:
    if isinstance(__bool, bool):
        return pd.DataFrame(columns=df.columns) if flip else df
    if isinstance(__bool, pd.DataFrame): __bool = __bool.any(axis=1)
    return df[~__bool] if flip else df[__bool]


def _match_df(df: pd.DataFrame, func: Optional[MatchFunction]=None, path: Optional[Union[_KT,Tuple[_KT]]]=None,
            query: Optional[Union[_KT,Tuple[_KT]]]=None, value: Optional[Any]=None,
            exact: Optional[_VT]=None, include: Optional[_VT]=None, exclude: Optional[_VT]=None,
            flip=False, strict=False, how: Literal["any","all"]="any", if_null=False,
            default=False, **context) -> pd.DataFrame:
    if isna(func) and isna(path) and isna(query): return df
    elif query: return toggle_df(df, _match_query(**locals()))
    elif func:
        if notna(path): df = cloc(df, path)
        return toggle_df(df, safe_apply_df(df, func, default=default, **context), flip=flip)
    elif isna(exact) and isna(include) and isna(exclude):
        df = cloc(df, path, if_null="drop") if path else df
        if df.empty: return toggle_df(df, if_null, flip=flip)
        elif notna(value): return toggle_df(df, (df == value), filp=flip)
        else: return toggle_df(df, match_df(df, match=(lambda x: exists(x, strict=strict)), all_cols=True), flip=flip)
    exact = value if notna(value) else exact
    return toggle_df(df, isin_df(df, path, exact, include, exclude, how=how, if_null=if_null, filter=False), flip=flip)


def _match_html(source: Tag, func: Optional[MatchFunction]=None, path: Optional[Union[_KT,Tuple[_KT]]]=None,
                query: Optional[Union[_KT,Tuple[_KT]]]=None, value: Optional[Any]=None,
                exact: Optional[_VT]=None, include: Optional[_VT]=None, exclude: Optional[_VT]=None,
                flip=False, strict=False, how: Literal["any","all"]="any", if_null=False,
                hier=True, default=False, **context) -> bool:
    if isna(func) and isna(path) and isna(query): return True
    elif query: return _match_query(**locals())
    elif func:
        if notna(path): source = hier_select(source, path) if hier else select_by(source, path)
        return toggle(apply_schema(source, func, default=default, **context), flip=flip)
    elif isna(exact) and isna(include) and isna(exclude):
        if not is_single_selector(path, hier=hier):
            __locals = drop_dict(locals(), "path", inplace=False)
            return howin([_match_html(path=__s, **__locals) for __s in path], how=how)
        __value = hier_select(source, path) if hier else select_by(source, path)
        if isna(__value): return toggle(if_null, flip=flip)
        elif is_array(__value): return toggle((value in __value), flip=flip)
        elif notna(value): return toggle((__value == value), flip=flip)
        else: return toggle(exists(__value, strict=strict), flip=flip)
    exact = value if notna(value) else exact
    return toggle(isin_source(source, path, exact, include, exclude, how=how, if_null=if_null, hier=hier), flip=flip)
