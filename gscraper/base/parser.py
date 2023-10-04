from __future__ import annotations
from gscraper.base.session import BaseSession, INVALID_VALUE_MSG
from gscraper.base.context import SCHEMA_CONTEXT
from gscraper.base.types import _KT, _VT, _PASS, TypeHint, LogLevel, IndexLabel, Timezone
from gscraper.base.types import Records, Data, ApplyFunction, MatchFunction
from gscraper.base.types import not_na, get_type, init_origin, is_numeric_type, is_array, is_records, is_df, is_df_sequence

from gscraper.utils.cast import cast_str, cast_object
from gscraper.utils.logs import log_data
from gscraper.utils.map import safe_apply, get_scala, exists_one, union
from gscraper.utils.map import kloc, chain_dict, drop_dict, exists_dict, hier_get, groupby_records
from gscraper.utils.map import concat_df, fillna_each, safe_apply_df, groupby_df, filter_data, set_data

from abc import ABCMeta
import functools

from typing import Any, Dict, Callable, List, Literal, Optional, Sequence, Tuple, Type, Union
import pandas as pd


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
########################### Schema Type ###########################
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
    def __init__(self, schema: Schema, root: Optional[_KT]=None, match: Optional[MatchFunction]=None):
        self.update(schema=schema, **exists_dict(dict(root=root, match=match), strict=True))


class SchemaInfo(dict):
    def __init__(self, **context: SchemaContext):
        self.update(context)


def get_path_type(path: SchemaPath) -> PathType:
    if isinstance(path, str): return VALUE
    elif isinstance(path, Sequence):
        if not path: return GLOBAL
        if isinstance(path, Tuple) and len(path) == 2: return TUPLE
        elif len(path) > 0 and is_array(path[-1]): return ITERATE
        else: return PATH
    elif isinstance(path, Callable): return CALLABLE
    else: raise TypeError(INVALID_PATH_TYPE_MSG(path))


###################################################################
############################# Parsers #############################
###################################################################

class Parser(BaseSession):
    __metaclass__ = ABCMeta
    operation = "parser"
    fields = list()
    pagination = False
    root = list()
    path = list()
    groupby = list()
    match = None
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
        self._validate_schema_info()

    ###################################################################
    ######################### Schema Validator ########################
    ###################################################################

    def _validate_schema_info(self):
        for __key, schemaContext in self.schemaInfo.copy().items():
            self.schemaInfo[__key]["schema"] = self._validate_schema(schemaContext["schema"])

    def _validate_schema(self, schema: Schema) -> Schema:
        if not isinstance(schema, Sequence):
            raise TypeError(INVALID_SCHEMA_TYPE_MSG)
        return [self._validate_field(field) for field in schema]

    def _validate_field(self, field: Field) -> Field:
        if not isinstance(field, Dict):
            raise TypeError(INVALID_FIELD_TYPE_MSG)
        if len(kloc(field, [NAME, PATH, TYPE, MODE], if_null="drop")) != 4:
            raise ValueError(INVALID_FIELD_KEY_MSG)
        field = field.copy()
        field[HOW] = get_path_type(field[PATH])
        field = self._init_field(field)
        if APPLY in field:
            field = self._validate_apply(field)
        if MATCH in field:
            field = self._validate_match(field)
        return field

    def _init_field(self, field: Field) -> Field:
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

    def _validate_apply(self, field: Field) -> Field:
        apply_func = field[APPLY]
        if not isinstance(apply_func, (Callable, Dict, Tuple)):
            raise TypeError(INVALID_APPLY_TYPE_MSG(apply_func, field[NAME]))
        elif isinstance(apply_func, (Callable, Dict)):
            if isinstance(apply_func, Callable):
                apply_func = dict(func=apply_func)
            if not FUNC in apply_func or not isinstance(apply_func[FUNC], (Callable, str)):
                raise INVALID_APPLY_FUNC_MSG(apply_func[FUNC], field[NAME])
            if DEFAULT in field:
                field[APPLY][DEFAULT] = field[DEFAULT]
        elif isinstance(apply_func, Tuple):
            if not is_records(apply_func, how="all", empty=False):
                raise TypeError(INVALID_APPLY_TYPE_MSG(get_scala(apply_func), field[NAME]))
            for __i, __apply in enumerate(apply_func):
                if not FUNC in __apply or not isinstance(__apply[FUNC], (Callable, str)):
                    raise INVALID_APPLY_FUNC_MSG(__apply[FUNC], field[NAME])
            if DEFAULT in field:
                field[APPLY][__i][DEFAULT] = field[DEFAULT]
        return field

    def _validate_match(self, field: Field) -> Field:
        match_func = field[MATCH]
        if isinstance(match_func, Dict):
            if not (FUNC in match_func or PATH in match_func or VALUE in match_func):
                raise ValueError(INVALID_MATCH_KEY_MSG(field[NAME]))
            elif FUNC in match_func and not isinstance(match_func[FUNC], Callable):
                raise TypeError(INVALID_MATCH_FUNC_MSG(match_func[FUNC], field[NAME]))
        else: raise TypeError(INVALID_MATCH_TYPE_MSG(match_func, field[NAME]))
        return field

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
    def parse(self, response: Any, strict=False, discard=False, updateTime=True,
                fields: IndexLabel=list(), **context) -> Data:
        data = self.parse_response(response, **context)
        return self.parse_data(data, strict, discard, updateTime, fields, **context)

    def parse_response(self, response: Any, **context) -> Data:
        return response

    def parse_data(self, data: Data, strict=False, discard=False, updateTime=True,
                    fields: IndexLabel=list(), **context) -> Data:
        schema_args = (self.schemaInfo, self.root, self.path, self.groupby, self.match, self.rankby)
        data = parse_data(data, *schema_args, strict=strict, discard=discard, **context)
        if updateTime:
            data = set_data(data, updateDate=self.today(), updateTime=self.now())
        return filter_data(data, fields=fields, if_null="pass")


###################################################################
############################ Parse Data ###########################
###################################################################

def parse_data(data: Data, schemaInfo: SchemaInfo, root: _KT=list(), path: _KT=list(), groupby: _KT=list(),
                match: Optional[MatchFunction]=None, rankby=str(), strict=False, discard=False, **context) -> Data:
    if root: data = hier_get(data, root)
    if is_records(data):
        return parse_records(data, schemaInfo, list(), path, groupby, match, rankby, strict, discard, **context)
    elif isinstance(data, Dict):
        return parse_dict(data, schemaInfo, list(), discard, **context)
    elif isinstance(data, pd.DataFrame):
        return parse_df(data, schemaInfo, groupby, match, rankby, discard, **context)
    else: return data


def parse_records(__r: Records, schemaInfo: SchemaInfo, root: _KT=list(), path: _KT=list(), groupby: _KT=list(),
                match: Optional[MatchFunction]=None, rankby=str(), strict=False, discard=False, **context) -> Records:
    context = SCHEMA_CONTEXT(**context)
    if root: __r = hier_get(__r, root, default=list())
    if groupby: return _groupby_records(__r, schemaInfo, groupby, path, match, rankby, strict, discard, **context)
    data = list()
    rankStart = calc_start(len(__r), rankby, **context)
    for __i, __m in enumerate(__r, start=(rankStart if rankStart else 0)):
        if path: __m = hier_get(__m, path, dict())
        if not isinstance(__m, Dict):
            if strict: raise TypeError(INVALID_DATA_TYPE_MSG(__m, RECORD))
            else: continue
        if isinstance(match, Callable) and not safe_apply(__m, match, **context): continue
        if isinstance(rankStart, int): __m["rank"] = __i
        data.append(parse_dict(__m, schemaInfo, discard=discard, **context))
    return data


def _groupby_records(__r: Records, schemaInfo: SchemaInfo, groupby: _KT=list(), path: _KT=list(),
                    match: Optional[MatchFunction]=None, rankby=str(), strict=False, discard=False, **context) -> Records:
    groups = groupby_records(__r, by=groupby, if_null=("drop" if strict else "pass"))
    params = dict(path=path, match=match, rankby=rankby, strict=strict, discard=discard)
    return union(*[parse_records(group, schemaInfo, **params, **context) for group in groups.values()])


def calc_start(count: int, by: Literal["page","start"]=None, page=1, start=1, pageStart=1, offset=1, **context) -> int:
    if (by == "page") and isinstance(page, int):
        return (page if pageStart == 0 else page-1)*count+1
    elif by == "start" and isinstance(start, int):
        return start+1 if offset == 0 else start
    else: return None


###################################################################
############################ Parse Dict ###########################
###################################################################

def parse_dict(__m: Dict, schemaInfo: SchemaInfo, root: _KT=list(), discard=False, **context) -> Dict:
    context = SCHEMA_CONTEXT(**context)
    if root: __m = hier_get(__m, root, default=dict())
    __base = dict()
    for schema_context in schemaInfo.values():
        schema, schema_root, schema_match = kloc(schema_context, SCHEMA_KEYS, if_null="pass", values_only=True)
        __bm = hier_get(__m, schema_root, default=dict()) if schema_root else __m
        if not __bm: continue
        __bm = dict(__bm, **context)
        if isinstance(schema_match, Callable) and not safe_apply(__bm, schema_match, **context): continue
        __base = _parse_dict_schema(__bm, __base, schema, **context)
    return __base if discard else chain_dict([__base, __m], keep="first")


def _parse_dict_schema(__m: Dict, __base: Dict, schema: Schema, **context) -> Dict:
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
        try: __base = _parse_dict_field(__m, __base, **field, **context)
        except Exception as exception:
            print(EXCEPTION_ON_NAME_MSG(field[NAME]))
            raise exception
    return __base


def _parse_dict_field(__m: Dict, __base: Dict, name: _KT, path: SchemaPath, how: Optional[PathType]=None,
                    type: Optional[Type]=None, mode: _PASS=None, description: _PASS=None, cast=False, strict=True,
                    default=None, apply: Apply=dict(), match: Match=dict(), **context) -> Dict:
    path_type = how if how else get_path_type(path)
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


###################################################################
######################### Parse DataFrame #########################
###################################################################

def parse_df(df: pd.DataFrame, schemaInfo: SchemaInfo, groupby: _KT=list(),
            match: Optional[MatchFunction]=None, rankby=str(), discard=False, **context) -> pd.DataFrame:
    context = SCHEMA_CONTEXT(**context)
    if groupby: return _groupby_df(df, schemaInfo, groupby, match, rankby, discard, **context)
    __base = pd.DataFrame()
    rankStart = calc_start(len(df), rankby, **context)
    if isinstance(rankStart, int): df["rank"] = range(rankStart, len(df)+rankStart)
    if isinstance(match, Callable): df = df[df.apply(lambda row: safe_apply(row, match, **context), axis=1)]
    for schema_context in schemaInfo.values():
        schema, _, schema_match = kloc(schema_context, SCHEMA_KEYS, if_null="pass")
        if isinstance(schema_match, Callable) and not schema_match(df): continue
        __base = _parse_df_schema(df, __base, schema, **context)
    return __base if discard else concat_df([__base, df], axis=1, keep="first")


def _groupby_df(df: pd.DataFrame, schemaInfo: SchemaInfo, groupby: _KT=list(),
                match: Optional[MatchFunction]=None, rankby=str(), discard=False, **context) -> pd.DataFrame:
    groups = groupby_df(df, by=groupby, if_null="drop")
    params = dict(match=match, rankby=rankby, discard=discard)
    return pd.concat([parse_df(group, schemaInfo, **params, **context) for group in groups.values()])


def _parse_df_schema(df: pd.DataFrame, __base: pd.DataFrame, schema: Schema, **context) -> pd.DataFrame:
    for field in schema:
        df = concat_df([__base, df], axis=1, keep="first")
        if (field[MODE] == QUERY) and (NAME in context):
            df[field[NAME]] = context.get(NAME)
            if not ((APPLY in field) or (MATCH in field)):
                __base[field[NAME]] = context[field[NAME]]
                continue
        try: __base = _parse_df_field(df, __base, **field, **context)
        except Exception as exception:
            print(EXCEPTION_ON_NAME_MSG(field[NAME]))
            raise exception
    return __base


def _parse_df_field(df: pd.DataFrame, __base: pd.DataFrame, name: _KT, path: SchemaPath, how: Optional[PathType]=None,
                    type: Optional[Type]=None, mode: _PASS=None, cast=False, strict=True, default=None,
                    apply: Apply=dict(), match: Match=dict(), **context) -> Dict:
    path_type = how if how else get_path_type(path)
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
            value: Optional[_VT]=None, flip=False, strict=True, default=False, **context) -> pd.DataFrame:
    if not (func or path or value): return df
    if path: df = df[path[0]]

    if func: condition = safe_apply_df(df, func, **context)
    elif isinstance(value, bool): return df if value else pd.DataFrame()
    elif not_na(value, strict=True): condition = (df == value)
    else: condition = safe_apply_df(df, lambda x: not_na(x, strict=strict), by="cell")

    if isinstance(condition, pd.DataFrame):
        return df[~df.all(axis=1)] if flip else df[df.all(axis=1)]
    else: return df[~condition] if flip else df[condition]
