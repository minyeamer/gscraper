from __future__ import annotations
from gscraper.base.context import SCHEMA_CONTEXT
from gscraper.base.session import BaseSession

from gscraper.base.types import _KT, _VT, _PASS, TypeHint, LogLevel, IndexLabel, Timezone
from gscraper.base.types import Records, Data, JsonData, ApplyFunction, MatchFunction
from gscraper.base.types import not_na, get_type, init_origin, is_numeric_type, is_array, is_records, is_df, is_df_sequence

from gscraper.utils.cast import cast_str, cast_object, cast_datetime, cast_date
from gscraper.utils.logs import log_data
from gscraper.utils.map import safe_apply, get_scala, exists_one, union
from gscraper.utils.map import kloc, chain_dict, drop_dict, exists_dict, hier_get, groupby_records
from gscraper.utils.map import concat_df, fillna_each, safe_apply_df, groupby_df, filter_data, set_data

from abc import ABCMeta
from ast import literal_eval
import functools

from typing import Any, Dict, Callable, List, Literal, Optional, Sequence, Tuple, Type, Union
from bs4 import BeautifulSoup
from bs4.element import Tag
import datetime as dt
import json
import pandas as pd
import re

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')


SCHEMA = "schema"
ROOT = "root"
MATCH = "match"
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

FUNC = "func"
__EXISTS__ = "__EXISTS__"
__JOIN__ = "__JOIN__"
__SPLIT__ = "__SPLIT__"
__MAP__ = "__MAP__"


###################################################################
############################# Messages ############################
###################################################################

INVALID_DATA_TYPE_MSG = lambda data, __type: f"'{type(data)}' 타입은 {__type} 파싱을 위해 유효한 데이터 타입이 아닙니다."
INVALID_VALUE_TYPE_MSG = lambda ret, __t, name=str(): \
    f"'{type(ret)}' 타입은 {FOR_NAME(name)}유효한 값의 형식이 아닙니다. '{__t}' 타입이 필요합니다."

INVALID_SCHEMA_TYPE_MSG = "스키마는 딕셔너리 배열로 구성된 레코드 타입이어야 합니다."
INVALID_FIELD_TYPE_MSG = "스키마 필드는 딕셔너리 타입이어야 합니다."
INVALID_FIELD_KEY_MSG = "스키마 필드는 'name', 'path', 'type', 'mode' 키값을 포함해야 합니다."
INVALID_PATH_TYPE_MSG = lambda path: f"'{path}' 타입은 지원하지 않는 스키마 경로 타입 입니다."

FOR_NAME = lambda name: f"'{name}' 대상에 대해 " if name else str()
FOR_NAME_ADJ = lambda name: f"'{name}' 대상에 대한 " if name else str()

INVALID_APPLY_TYPE_MSG = lambda apply, name=str(): f"'{type(apply)}' 타입은 {FOR_NAME(name)}유효한 Apply 객체가 아닙니다."
INVALID_APPLY_FUNC_MSG = lambda func, name=str(): f"'{type(func)}' 타입은 {FOR_NAME(name)}유효한 Apply 함수가 아닙니다."
INVALID_APPLY_SPECIAL_MSG = lambda func, name=str(): f"'{func}' 타입은 {FOR_NAME(name)}유효한 Apply 명령어가 객체가 아닙니다."
INVALID_APPLY_RETURN_MSG = lambda ret, __t, name=str(): \
    f"'{type(ret)}' 타입은 {FOR_NAME(name)}유효한 Apply 함수의 반환 형식이 아닙니다. '{__t}' 타입이 필요합니다."

INVALID_MATCH_TYPE_MSG = lambda match, name=str(): f"'{type(match)}' 타입은 {FOR_NAME(name)}유효한 Match 객체가 아닙니다."
INVALID_MATCH_FUNC_MSG = lambda func, name=str(): f"'{type(func)}' 타입은 {FOR_NAME(name)}유효한 Match 함수가 아닙니다."
INVALID_MATCH_KEY_MSG = lambda name=str(): f"{FOR_NAME_ADJ(name)}Match 함수는 'func', 'path', 'value' 중 최소 한 개의 파라미터가 요구됩니다."

EXCEPTION_ON_NAME_MSG = lambda name: f"Exception occured from gscraper.'{name}'."

RECORDS, RECORD = "레코드", "딕셔너리"
SEQUENCE, DICTIONARY = "배열", "딕셔너리"
PANDAS_SERIES = "시리즈"
PANDAS_OBJECT = "데이터프레임 또는 시리즈"


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
########################## Beautiful Soup #########################
###################################################################

clean_html = lambda html: BeautifulSoup(str(html), "lxml").text
clean_tag = lambda source: re.sub("<[^>]*>", "", str(source))


def select_text(source: Tag, selector: str, pattern='\n', sub=' ', multiple=False) -> Union[str,List[str]]:
    try:
        if multiple: return [re.sub(pattern, sub, select.text).strip() for select in source.select(selector)]
        else: return re.sub(pattern, sub, source.select_one(selector).text).strip()
    except (AttributeError, IndexError, TypeError):
        return list() if multiple else str()


def select_attr(source: Tag, selector: str, key: str, default=None, multiple=False) -> Union[str,List[str]]:
    try:
        if multiple: return [select.attrs.get(key,default).strip() for select in source.select(selector)]
        else: return source.select_one(selector).attrs.get(key,default).strip()
    except (AttributeError, IndexError, TypeError):
        return list() if multiple else str()


def select_datetime(source: Tag, selector: str, default=None, multiple=False) -> dt.datetime:
    if multiple:
        return [cast_datetime(text, default) for text in select_text(source, selector, multiple=True)]
    else:
        return cast_datetime(select_text(source, selector, multiple=False), default)


def select_date(source: Tag, selector: str, default=None, multiple=False) -> dt.datetime:
    if multiple:
        return [cast_date(text, default) for text in select_text(source, selector, multiple=True)]
    else:
        return cast_date(select_text(source, selector, multiple=False), default)


def match_class(source: Tag, class_name: str) -> bool:
    try:
        class_list = source.attrs.get("class")
        if isinstance(class_list, List):
            return class_name in class_list
        return class_name == class_list
    except (AttributeError, TypeError):
        return False


###################################################################
############################## Schema #############################
###################################################################

SchemaPath = Union[_KT, _VT, Tuple[_KT,_KT], Tuple[_VT,_VT], Callable]
PathType = Literal["path", "value", "tuple", "iterate", "callable", "global"]


class Apply(dict):
    def __init__(self, func: Union[ApplyFunction, str], default: Optional[Any]=None):
        self.update(func=func, **exists_dict(dict(default=default), strict=True))


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
                groupby: _KT=list(), rankby: Optional[Literal["page","start"]]="start",
                page=1, start=1, discard=True) -> Data:
        self.update(func=__MAP__, schema=schema, **exists_dict(
            dict(root=root, match=match, groupby=groupby, rankby=rankby, page=page, start=start,
                discard=discard), strict=True))


class Match(dict):
    def __init__(self, func: Optional[MatchFunction]=None, path: Optional[_KT]=None, value: Optional[Any]=None,
                flip: Optional[bool]=None, strict: Optional[bool]=None):
        self.update(exists_dict(dict(func=func, path=path, value=value, flip=flip, strict=strict), strict=True))


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
        return not_na(response, strict=False)

    def log_results(self, data: Data, **context):
        self.logger.info(log_data(data, **context))

    ###################################################################
    ########################## Parse Response #########################
    ###################################################################

    @validate_response
    def parse(self, response: Any, **context) -> Data:
        data = hier_get(response, self.root) if self.root else response
        return self.map(data, **context)

    def map(self, data: Data, discard=False, updateTime=True, fields: IndexLabel=list(), **context) -> Data:
        if is_records(data):
            data = map_records(data, self.schemaInfo, self.groupby, self.rankby, discard, **context)
        elif isinstance(data, Dict):
            data = map_dict(data, self.schemaInfo, discard, **context)
        elif isinstance(data, pd.DataFrame):
            data = map_df(data, self.schemaInfo, self.groupby, self.rankby, discard, **context)
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
    if field[MODE] == NOTNULL:
        field[DEFAULT] = init_origin(field[TYPE])
        field[CAST] = True
    elif field[MODE] == NOTZERO:
        field[CAST] = True
        field[STRICT] = False
    elif is_numeric_type(field[TYPE]) and (CAST not in field):
        field[CAST] = True
    return field


def validate_apply(apply: Apply, name=str(), **context) -> Apply:
    if isinstance(apply, (Callable, Dict)):
        if isinstance(apply, Callable):
            apply = dict(func=apply)
        if not isinstance(apply.get(FUNC), (Callable, str)):
            raise INVALID_APPLY_FUNC_MSG(apply[FUNC], name)
        return apply
    elif isinstance(apply, Tuple):
        return tuple(validate_apply(func, name) for func in apply)
    else: raise TypeError(INVALID_APPLY_TYPE_MSG(apply, name))


def validate_match(match: Match, name=str(), **context) -> Match:
    if isinstance(match, (Callable, Dict)):
        if isinstance(match, Callable):
            match = dict(func=match)
        if not ((FUNC in match) or (PATH in match) or (VALUE in match)):
            raise ValueError(INVALID_MATCH_KEY_MSG(name))
        if (FUNC in match) and not isinstance(match[FUNC], Callable):
            raise TypeError(INVALID_MATCH_FUNC_MSG(match[FUNC], name))
        return match
    else: raise TypeError(INVALID_MATCH_TYPE_MSG(match, name))


###################################################################
########################## Parse Records ##########################
###################################################################

def map_records(__r: Records, schemaInfo: SchemaInfo, groupby: _KT=list(), rankby=str(),
                discard=False, **context) -> Records:
    context = SCHEMA_CONTEXT(**context)
    if groupby:
        return _groupby_records(__r, schemaInfo, groupby, rankby, discard, **context)
    data = list()
    start = get_start(len(__r), rankby, **context)
    for __i, __m in enumerate(__r, start=(start if start else 0)):
        if not isinstance(__m, Dict): continue
        if isinstance(start, int): __m["rank"] = __i
        data.append(map_dict(__m, schemaInfo, discard=discard, **context))
    return data


def _groupby_records(__r: Records, schemaInfo: SchemaInfo, groupby: _KT=list(), rankby=str(),
                    discard=False, **context) -> Records:
    context = dict(context, rankby=rankby, discard=discard)
    groups = groupby_records(__r, by=groupby, if_null="pass")
    return union(*[map_records(group, schemaInfo, **context) for group in groups.values()])


def get_start(count: int, by: Optional[Literal["page","start"]]=None,
                page=1, start=1, pageStart=1, offset=1, **context) -> int:
    if (by == "page") and isinstance(page, int):
        return (page if pageStart == 0 else page-1)*count+1
    elif by == "start" and isinstance(start, int):
        return start+1 if offset == 0 else start
    else: return None


###################################################################
############################ Parse Dict ###########################
###################################################################

def map_dict(__m: Dict, schemaInfo: SchemaInfo, discard=False, **context) -> Dict:
    context = SCHEMA_CONTEXT(**context)
    __base = dict()
    for key, schema_context in schemaInfo.items():
        schema, root, match = kloc(schema_context, SCHEMA_KEYS, if_null="pass", values_only=True)
        __bm = hier_get(__m, root) if root else __m
        if not __bm: continue
        elif not isinstance(__bm, Dict):
            raise TypeError(INVALID_DATA_TYPE_MSG(__m, DICTIONARY))
        __bm = chain_dict([__base, context, __bm], keep="first")
        if isinstance(match, Dict) and not _match_dict(__bm, **match): continue
        __base = _map_dict_schema(__bm, __base, schema, **context)
    return __base if discard else chain_dict([__base, __m], keep="first")


def _map_dict_schema(__m: Dict, __base: Dict, schema: Schema, **context) -> Dict:
    for field in schema:
        __m = dict(__m, **__base)
        if (field[MODE] == QUERY) and (field[NAME] in context):
            __m[field[NAME]] = context[field[NAME]]
            if not ((CAST in field) or (APPLY in field) or (MATCH in field)):
                __base[field[NAME]] = context[field[NAME]]
                continue
        elif (field[MODE] == INDEX) and (field[NAME] in __m):
            __base[field[NAME]] = __m[field[NAME]]
            continue
        try: __base = _map_dict_field(__m, __base, **field, **context)
        except Exception as exception:
            print(EXCEPTION_ON_NAME_MSG(field[NAME]))
            raise exception
    return __base


def _map_dict_field(__m: Dict, __base: Dict, name: _KT, path: SchemaPath, how: Optional[PathType]=None,
                    type: Optional[Type]=None, mode: _PASS=None, description: _PASS=None, cast=False, strict=True,
                    default=None, apply: Apply=dict(), match: Match=dict(), **context) -> Dict:
    path_type = how if how else _get_path_type(path)
    context = dict(context, type=type, cast=cast, strict=strict, default=default, apply=apply, match=match)
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
                    apply: Apply=dict(), match: Match=dict(), **context) -> Dict:
    if not _match_dict(__m, **match): return __base
    value = _from_dict(__m, path, default=default, **context)
    __base[name] = _apply_schema(value, **apply)
    return __base


def _set_dict_tuple(__m: Dict, __base: Dict, name: _KT, path: Tuple, default=None,
                    apply: Apply=dict(), match: Match=dict(), **context) -> Dict:
    __match = int(_match_dict(__m, **match))-1
    value = _from_dict(__m, path[__match], default=default, **context)
    __base[name] = _apply_schema(value, **get_scala(apply, index=__match, default=dict()))
    return __base


def _set_dict_iterate(__m: Dict, __base: Dict, name: _KT, path: Sequence[_KT], default=None,
                    apply: Apply=dict(), match: Match=dict(), **context) -> Dict:
    value = hier_get(__m, path[:-1])
    if not isinstance(value, Sequence):
        raise TypeError(INVALID_VALUE_TYPE_MSG(value, SEQUENCE, name))
    sub_path = path[-1] if (len(path) > 0) and is_array(path[-1]) else value
    context = dict(context, path=sub_path, default=default)
    value = [_from_dict(__e, **context) for __e in value if _match_dict(__e, **match)]
    __base[name] = [_apply_schema(__e, **apply) for __e in value] if apply else value
    return __base


def _set_dict_global(__m: Dict, __base: Dict, name: Optional[_KT]=None,
                    apply: Apply=dict(), match: Match=dict(), **context) -> Dict:
    __m = _from_dict(__m, [], **context)
    if _match_dict(__m, **match):
        results = _apply_schema(__m, **apply)
        if name: __base[name] = results
        elif isinstance(results, Dict):
            __base = dict(__base, **results)
        else: raise TypeError(INVALID_APPLY_RETURN_MSG(results, DICTIONARY, "GLOBAL"))
    return __base


def _match_dict(__m: Dict, func: Optional[MatchFunction]=None, path: Optional[_KT]=None,
                value: Optional[_VT]=None, flip=False, strict=True, default=False, **context) -> bool:
    if not (func or path or value): return True
    __object = hier_get(__m, path) if path else __m

    toggle = (lambda x: not x) if flip else (lambda x: bool(x))
    if func: return toggle(_apply_schema(__object, func, default=False, **context))
    elif isinstance(value, bool): return toggle(value)
    elif not_na(value, strict=True): return toggle(__object == value)
    else: return toggle(not_na(__object, strict=strict))


###################################################################
############################ Apply Dict ###########################
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
            groupby: _KT=list(), rankby: Optional[Literal["page","start"]]="start",
            page=1, start=1, discard=True, **context) -> Data:
    schema = validate_schema(schema)
    schemaInfo = SchemaInfo(schema=SchemaContext(schema=schema, root=root, match=match))
    if is_records(__object):
        return map_records(__object, schemaInfo, groupby, rankby, discard, page=page, start=start, **context)
    elif isinstance(__object, Dict):
        return map_dict(__object, schemaInfo, discard, **context)
    else: return __object


###################################################################
######################### Parse DataFrame #########################
###################################################################

def map_df(df: pd.DataFrame, schemaInfo: SchemaInfo, groupby: _KT=list(), rankby=str(),
            discard=False, **context) -> pd.DataFrame:
    context = SCHEMA_CONTEXT(**context)
    if groupby:
        return _groupby_df(df, schemaInfo, groupby, rankby, discard, **context)
    __base = pd.DataFrame()
    start = get_start(len(df), rankby, **context)
    if isinstance(start, int): df["rank"] = range(start, len(df)+start)
    for schema_context in schemaInfo.values():
        schema, _, match = kloc(schema_context, SCHEMA_KEYS, if_null="pass")
        if isinstance(match, Dict) and _match_df(df, **match).empty: continue
        __base = _map_df_schema(df, __base, schema, **context)
    return __base if discard else concat_df([__base, df], axis=1, keep="first")


def _groupby_df(df: pd.DataFrame, schemaInfo: SchemaInfo, groupby: _KT=list(), rankby=str(),
                discard=False, **context) -> pd.DataFrame:
    context = dict(context, rankby=rankby, discard=discard)
    groups = groupby_df(df, by=groupby, if_null="drop")
    return pd.concat([map_df(group, schemaInfo, **context) for group in groups.values()])


def _map_df_schema(df: pd.DataFrame, __base: pd.DataFrame, schema: Schema, **context) -> pd.DataFrame:
    for field in schema:
        df = concat_df([__base, df], axis=1, keep="first")
        if (field[MODE] == QUERY) and (NAME in context):
            df[field[NAME]] = context.get(NAME)
            if not ((APPLY in field) or (MATCH in field)):
                __base[field[NAME]] = context[field[NAME]]
                continue
        try: __base = _map_df_field(df, __base, **field, **context)
        except Exception as exception:
            print(EXCEPTION_ON_NAME_MSG(field[NAME]))
            raise exception
    return __base


def _map_df_field(df: pd.DataFrame, __base: pd.DataFrame, name: _KT, path: SchemaPath, how: Optional[PathType]=None,
                    type: Optional[Type]=None, mode: _PASS=None, cast=False, strict=True, default=None,
                    apply: Apply=dict(), match: Match=dict(), **context) -> Dict:
    path_type = how if how else _get_path_type(path)
    context = dict(context, type=type, cast=cast, strict=strict, default=default, apply=apply, match=match)
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
                apply: Apply=dict(), match: Match=dict(), **context) -> pd.DataFrame:
    if _match_df(df, **match).empty: return __base
    series = _from_df(df, path, default=default, **context)
    if apply:
        series = safe_apply_df(series, apply[FUNC], **drop_dict(apply, FUNC))
    if not is_df_sequence(series):
        raise TypeError(INVALID_VALUE_TYPE_MSG(series, PANDAS_SERIES, name))
    __base[name] = series
    return __base


def _set_df_tuple(df: pd.DataFrame, __base: pd.DataFrame, name: _KT, path: Tuple, default=None,
                apply: Apply=dict(), match: Match=dict(), **context) -> pd.DataFrame:
    matches = _match_df(df, **match).index
    __base[name] = default
    for __i, row in df.iterrows():
        __match = int(__i in matches)-1
        value = _from_dict(row, path[__match], default=default, **context)
        if apply: value = _apply_schema(value, **get_scala(apply, index=__match, default=dict()))
        __base.at[__i,name] = value
    return __base


def _set_df_global(df: pd.DataFrame, __base: pd.DataFrame, name: Optional[_KT]=None,
                    apply: Apply=dict(), match: Match=dict(), **context) -> pd.DataFrame:
    df = _match_df(df, **match)
    if not df.empty:
        results = safe_apply_df(df, apply[FUNC], **drop_dict(apply, FUNC))
        if name and is_df_sequence(results):
            __base[name] = results
        elif is_df(results): df = pd.concat([df, results], axis=0)
        else: raise TypeError(INVALID_APPLY_RETURN_MSG(results, PANDAS_OBJECT, "GLOBAL"))
    return __base


def _match_df(df: pd.DataFrame, func: Optional[MatchFunction]=None, path: Optional[_KT]=None,
            value: Optional[_VT]=None, flip=False, strict=True, **context) -> pd.DataFrame:
    if not (func or path or value): return df
    if path: df = df[path[0]]

    if func: condition = safe_apply_df(df, func, **context)
    elif isinstance(value, bool): return df if value else pd.DataFrame()
    elif not_na(value, strict=True): condition = (df == value)
    else: condition = safe_apply_df(df, lambda x: not_na(x, strict=strict), by="cell")

    if isinstance(condition, pd.DataFrame):
        return df[~df.all(axis=1)] if flip else df[df.all(axis=1)]
    else: return df[~condition] if flip else df[condition]
