from .types import _KT, _VT, _PASS, not_na, get_type, init_origin, is_array, is_records, is_df, is_df_sequence
from .types import is_array_type, is_dict_type, is_records_type, is_dataframe_type
from .types import Context, TypeHint, IndexLabel, RenameMap, JsonData, Records, Data
from .types import SchemaPath, PathType, ApplyFunction, MatchFunction, SchemaApply, SchemaMatch
from .types import SchemaField, Schema, SchemaInfo

from .cast import cast_datetime, cast_date, cast_object
from .map import re_get, safe_apply, get_scala, exists_one
from .map import kloc, chain_dict, rename_dict, drop_dict, exists_dict, hier_get
from .map import concat_df, fillna_each, safe_apply_df

from typing import Any, Dict, Callable, List, Literal, Optional, Sequence, Tuple, Type, Union
from ast import literal_eval
from bs4 import BeautifulSoup
from bs4.element import Tag
from http.cookies import SimpleCookie
from requests.cookies import RequestsCookieJar
from urllib.parse import quote, urlencode, urlparse

import datetime as dt
import json
import pandas as pd
import re

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')


UNSUPPORTED_TYPE_MSG = lambda __t: f"'{__t}' is not supported data type for parsing."
INVALID_DATA_TYPE_MSG = lambda data, __type: f"'{type(data)}' is not a valid data type for parsing {__type}."
INVALID_VALUE_TYPE_MSG = lambda ret, __t, name=str(): \
    f"'{type(ret)}' type is not a valid value type{FOR_NAME(name)}. '{__t}' type is desired."

INVALID_SCHEMA_TYPE_MSG = "Schema must be a sequence of dictionaries."
INVALID_FIELD_TYPE_MSG = "Schema field must be a dictionary type."
INVALID_FIELD_KEY_MSG = "Schema field must have 'name' and 'path' key-values."
INVALID_PATH_TYPE_MSG = lambda path: f"'{path}' is not supported type for schema path."

FOR_NAME = lambda name: f" for the '{name}'" if name else str()

INVALID_APPLY_TYPE_MSG = lambda apply, name=str(): f"'{type(apply)}' is not a valid apply object type{FOR_NAME(name)}."
INVALID_APPLY_FUNC_MSG = lambda func, name=str(): f"'{type(func)}' is not a valid apply function type{FOR_NAME(name)}."
INVALID_APPLY_SPECIAL_MSG = lambda func, name=str(): f"'{func}' is not a valid special apply function{FOR_NAME(name)}."
INVALID_APPLY_RETURN_MSG = lambda ret, __t, name=str(): \
    f"'{type(ret)}' type is not a valid return type of apply function{FOR_NAME(name)}. '{__t}' type is desired."

INVALID_MATCH_TYPE_MSG = lambda match, name=str(): f"'{type(match)}' is not a valid match object type{FOR_NAME(name)}."
INVALID_MATCH_FUNC_MSG = lambda func, name=str(): f"'{type(func)}' is not a valid match function type{FOR_NAME(name)}."
INVALID_MATCH_KEY_MSG = lambda name=str(): f"Match function{FOR_NAME(name)} requires at least one parameter: func, path, value"

EXCEPTION_ON_NAME_MSG = lambda name: f"Exception occured from '{name}'."

RECORDS, RECORD = "records", "a record"
SEQUENCE, DICTIONARY = "Sequence", "Dictionary"
PANDAS_SERIES = "Pandas Series"
PANDAS_OBJECT = "Pandas DataFrame or Series"


###################################################################
############################# Requests ############################
###################################################################

clean_html = lambda html: BeautifulSoup(str(html), "lxml").text
clean_tag = lambda source: re.sub("<[^>]*>", "", str(source))


def parse_cookies(cookies: Union[RequestsCookieJar,SimpleCookie]) -> str:
    return '; '.join([str(key)+"="+str(value) for key, value in cookies.items()])


def parse_parth(url: str) -> str:
    return re.sub(urlparse(url).path+'$','',url)


def parse_origin(url: str) -> str:
    return re_get(f"(.*)(?={urlparse(url).path})", url) if urlparse(url).path else url


def encode_cookies(cookies: Union[Dict,str], **kwargs) -> str:
    return '; '.join(
    [(parse_cookies(data) if isinstance(data, Dict) else str(data)) for data in [cookies,kwargs] if data])


def decode_cookies(cookies: str, **kwargs) -> Dict:
    if not cookies: return kwargs
    return dict({__key: __value for cookie in cookies.split('; ') for __key, __value in (cookie.split('=')[:2],)}, **kwargs)


def encode_params(url=str(), params: Dict=dict(), encode=True) -> str:
    if encode: params = urlencode(params)
    else: params = '&'.join([f"{key}={value}" for key, value in params.items()])
    return url+'?'+params if url else params


def encode_object(__object: str) -> str:
    return quote(str(__object).replace('\'','\"'))


###################################################################
############################## Source #############################
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
        else: return dict()
    except:
        return dict()


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
########################### Schema Base ###########################
###################################################################

NAME = "name"
PATH = "path"
TYPE = "type"
MODE = "mode"
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
INDEX = "index"
NULLABLE = "NULLABLE"
NOTNULL = "NOTNULL"

FUNC = "func"
CONDITION = "condition"
__EXISTS__ = "__EXISTS__"
__JOIN__ = "__JOIN__"

FIELD_KEYS = [NAME, PATH, TYPE, MODE, STRICT, DEFAULT, APPLY, MATCH]
FUNC_KEYS = [FUNC, CONDITION]

SCHEMA = "schema"
DATATYPE = "dataType"
ROOT = "root"
PATH = "path"
START = "start"
RENAME = "rename"
DISCARD = "discard"

SCHEMA_KEYS = [SCHEMA, DATATYPE, ROOT, PATH, START, RENAME, DISCARD]


def get_path_type(path: SchemaPath) -> PathType:
    if isinstance(path, str): return VALUE
    elif isinstance(path, Sequence):
        if not path: return GLOBAL
        if isinstance(path, Tuple) and len(path) == 2: return TUPLE
        elif len(path) > 0 and is_array(path[-1]): return ITERATE
        else: return PATH
    elif isinstance(path, Callable): return CALLABLE
    else: raise TypeError(INVALID_PATH_TYPE_MSG(path))


def validate_schema(schema: Schema) -> Schema:
    if not isinstance(schema, Sequence):
        raise TypeError(INVALID_SCHEMA_TYPE_MSG)
    return [validate_field(field) for field in schema]


def validate_field(field: SchemaField) -> SchemaField:
    if not isinstance(field, Dict):
        raise TypeError(INVALID_FIELD_TYPE_MSG)
    if (not NAME in field) or (not PATH in field):
        raise ValueError(INVALID_FIELD_KEY_MSG)
    field = field.copy()
    field[HOW] = get_path_type(field[PATH])
    field = init_field(field)
    if APPLY in field:
        field = validate_apply(field)
    if MATCH in field:
        field = validate_match(field)
    return field


def init_field(field: SchemaField) -> SchemaField:
    if TYPE in field:
        field[TYPE] = get_type(field[TYPE], argidx=-1)
        if (field.get(MODE) == NOTNULL) and not is_array_type(field[TYPE]):
            field[DEFAULT] = init_origin(field[TYPE])
            field[CAST] = True
    return field


def validate_apply(field: SchemaField) -> SchemaField:
    apply_func = field[APPLY]
    if not isinstance(apply_func, (Dict, Tuple)):
        raise TypeError(INVALID_APPLY_TYPE_MSG(apply_func, field[NAME]))
    elif isinstance(apply_func, Dict):
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


def validate_match(field: SchemaField) -> SchemaField:
    match_func = field[MATCH]
    if isinstance(match_func, Dict):
        if not (FUNC in match_func or PATH in match_func or VALUE in match_func):
            raise ValueError(INVALID_MATCH_KEY_MSG(field[NAME]))
        elif FUNC in match_func and not isinstance(match_func[FUNC], Callable):
            raise TypeError(INVALID_MATCH_FUNC_MSG(match_func[FUNC], field[NAME]))
    else: raise TypeError(INVALID_MATCH_TYPE_MSG(match_func, field[NAME]))
    return field


###################################################################
############################ Parse Data ###########################
###################################################################

def parse_schema(data: Data, schema_info: SchemaInfo, data_type: TypeHint="records",
                root: _KT=list(), rename: RenameMap=dict(), discard=False, **context) -> Data:
    results = init_origin(data_type)
    if not schema_info: return results
    context = drop_dict(context, SCHEMA_KEYS, inplace=False)
    for schema_context in schema_info.values():
        base = parse_context(schema_context[SCHEMA], **context)
        schema_context = dict(schema_context, **exists_dict({ROOT:root, RENAME:rename, DISCARD:discard}))
        if is_records_type(data_type):
            results += parse_records(data, base, **schema_context, **context)
        elif is_dict_type(data_type):
            results = dict(results, **parse_dict(data, base, **schema_context, **context))
        elif is_dataframe_type(data_type):
            results = pd.concat([results, parse_dataframe(data, base, **schema_context, **context)])
        else: raise TypeError(UNSUPPORTED_TYPE_MSG(data_type))
    return results


def parse_context(schema: Schema, **context) -> Context:
    if not context: return dict()
    base = dict()
    for field in schema:
        if field[MODE] == QUERY:
            base[field[NAME]] = kloc(context, field[PATH], field.get(DEFAULT))
    return base


def parse_records(__r: Union[Dict,Records], __base: Dict, schema: Schema,
                    root: _KT=list(), path: _KT=list(), strict=True, index=str(), start=1,
                    match: Optional[MatchFunction]=None, rename: RenameMap=dict(),
                    discard=False, **context) -> Records:
    __r = hier_get(__r, root, default=list()) if root else __r.copy()
    if not is_records(__r, how=("all" if strict else "any"), empty=True):
        raise TypeError(INVALID_DATA_TYPE_MSG(__r, RECORDS))
    results = list()
    for __i, __m in enumerate(__r, start=start):
        if path:
            __m = hier_get(__m, path)
        if not isinstance(__m, Dict):
            if strict: raise TypeError(INVALID_DATA_TYPE_MSG(__m, RECORD))
            else: continue
        if index:
            __base[index] = __i
        if isinstance(match, Callable):
            if not match(dict(__m, **__base), **context): continue
        results.append(parse_dict(__m, __base, schema, rename=rename, discard=discard, **context))
    return results


###################################################################
############################ Parse Dict ###########################
###################################################################

def parse_dict(__m: Dict, __base: Dict, schema: Schema, root: _KT=list(),
                rename: RenameMap=dict(), discard=False, **context) -> Dict:
    __m = hier_get(__m, root) if root else __m.copy()
    context = drop_dict(context, FIELD_KEYS+FUNC_KEYS, inplace=False)
    for field in schema:
        if field[MODE] == QUERY: continue
        __m = dict(__m, **__base)
        try: __base = set_dict_schema(__m, __base, **field, **context)
        except Exception as exception:
            print(EXCEPTION_ON_NAME_MSG(field[NAME]))
            raise exception
    __m = __base if discard else chain_dict([__base, __m], keep="first")
    return rename_dict(__m, rename) if rename else __m


def from_dict(__m: Dict, path: SchemaPath, type: Optional[Type]=None,
                cast=False, strict=True, default=None, **context) -> _VT:
    if not_na(default): default = from_dict(__m, default)

    if is_array(path): value = hier_get(__m, path, default, empty=False)
    elif isinstance(path, Callable): value = safe_apply(__m, path, default, **context)
    else: value = path
    return cast_object(value, type, default=default, strict=strict) if type and cast else value


def set_dict_schema(__m: Dict, __base: Dict, name: _KT, path: SchemaPath, how: Optional[PathType]=None,
                    type: Optional[Type]=None, mode: _PASS=None, cast=False, strict=True, default=None,
                    apply: SchemaApply=dict(), match: SchemaMatch=dict(), **context) -> Dict:
    path_type = how if how else get_path_type(path)
    context = dict(context, type=type, cast=cast, strict=strict, default=default, apply=apply, match=match)
    if path_type in (PATH,CALLABLE): return set_dict_value(__m, __base, name, path, **context)
    elif path_type == VALUE: return dict(__base, **{name: path})
    elif path_type == TUPLE: return set_dict_tuple(__m, __base, name, path, **context)
    elif path_type == ITERATE: return set_dict_iterate(__m, __base, name, path, **context)
    elif path_type == GLOBAL: return set_dict_global(__m, __base, name, **context)
    else: raise TypeError(INVALID_PATH_TYPE_MSG(path))


def set_dict_value(__m: Dict, __base: Dict, name: _KT, path: _KT, default=None,
                    apply: SchemaApply=dict(), match: SchemaMatch=dict(), **context) -> Dict:
    if not match_dict(__m, **match): return __base
    value = from_dict(__m, path, default=default, **context)
    __base[name] = apply_schema(value, **apply)
    return __base


def set_dict_tuple(__m: Dict, __base: Dict, name: _KT, path: Tuple, default=None,
                    apply: SchemaApply=dict(), match: SchemaMatch=dict(), **context) -> Dict:
    __match = int(match_dict(__m, **match))-1
    value = from_dict(__m, path[__match], default=default, **context)
    __base[name] = apply_schema(value, **get_scala(apply, index=__match, default=dict()))
    return __base


def set_dict_iterate(__m: Dict, __base: Dict, name: _KT, path: Sequence[_KT], default=None,
                    apply: SchemaApply=dict(), match: SchemaMatch=dict(), **context) -> Dict:
    value = hier_get(__m, path[:-1])
    if not isinstance(value, Sequence):
        raise TypeError(INVALID_VALUE_TYPE_MSG(value, SEQUENCE, name))
    sub_path = path[-1] if (len(path) > 0) and is_array(path[-1]) else value
    context = dict(context, path=sub_path, default=default)
    value = [from_dict(__e, **context) for __e in value if match_dict(__e, **match)]
    __base[name] = [apply_schema(__e, **apply) for __e in value] if apply else value
    return __base


def set_dict_global(__m: Dict, __base: Dict, name: Optional[_KT]=None,
                    apply: SchemaApply=dict(), match: SchemaMatch=dict(), **context) -> Dict:
    if match_dict(__m, **match):
        results = apply_schema(__m, **apply, **context)
        if name: __base[name] = results
        elif isinstance(results, Dict):
            __base = dict(__base, **results)
        else: raise TypeError(INVALID_APPLY_RETURN_MSG(results, DICTIONARY, "GLOBAL"))
    return __base


def match_dict(__m: Dict, func: Optional[MatchFunction]=None, path: Optional[_KT]=None,
                value: Optional[_VT]=None, flip=False, strict=True, default=False, **context) -> bool:
    if not (func or path or value): return True
    __object = hier_get(__m, path) if path else __m

    toggle = (lambda x: not x) if flip else (lambda x: bool(x))
    if func: return toggle(apply_schema(__object, func, default=False, **context))
    elif isinstance(value, bool): return toggle(value)
    elif not_na(value, strict=True): return toggle(__object == value)
    else: return toggle(not_na(__object, strict=strict))


###################################################################
############################ Apply Dict ###########################
###################################################################

def apply_schema(__object, func: Optional[ApplyFunction]=None, default=None, name=str(), **context) -> Any:
    if not func: return __object
    elif isinstance(func, Callable): return safe_apply(__object, func, default, **context)
    elif isinstance(func, str): return special_apply(__object, func, name, **context)
    else: raise TypeError(INVALID_APPLY_TYPE_MSG(func, name))


def special_apply(__object, func: str, name=str(), **context) -> _VT:
    if func == __EXISTS__: return __exists__(__object, **drop_dict(context, ["keys"], inplace=False))
    elif func == __JOIN__: return __join__(__object, **drop_dict(context, ["sep","split"], inplace=False))
    else: raise ValueError(INVALID_APPLY_SPECIAL_MSG(func, name))


def __exists__(__object, keys: _KT=list(), **context):
    return exists_one(*kloc(__object, keys, if_null="drop", values_only=True), None)


def __join__(__object, keys: _KT=list(), sep=',', split=',', **context) -> str:
    if isinstance(__object, Sequence):
        if keys: return str(sep).join([str(hier_get(__e, keys)) for __e in __object])
        else: return str(sep).join(map(str, __object))
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


###################################################################
######################### Parse DataFrame #########################
###################################################################

def parse_dataframe(df: pd.DataFrame, __base: Dict, schema: Schema,
                    rename: RenameMap=dict(), discard=False, **context) -> pd.DataFrame:
    df, base = df.copy(), pd.DataFrame([__base]*len(df))
    context = drop_dict(context, FIELD_KEYS+FUNC_KEYS, inplace=False)
    for field in schema:
        if field[MODE] == QUERY: continue
        if not base.empty:
            df = concat_df([base, df], axis=1, keep="first")
        try: base = set_df_schema(df, base, **field, **context)
        except Exception as exception:
            print(EXCEPTION_ON_NAME_MSG(field[NAME]))
            raise exception
    df = base if discard else concat_df([base, df], axis=1, keep="first")
    return df.rename(columns=rename) if rename else df


def from_df(df: pd.DataFrame, path: SchemaPath, type: Optional[Type]=None,
            cast=False, strict=True, default=None, **context) -> pd.Series:
    if is_array(default): default = from_df(df, default)

    if is_array(path): series = fillna_each(df[path[0]], default)
    elif isinstance(path, Callable): series = safe_apply_df(df, path, default, **context)
    else: series = pd.Series([path]*len(df), index=df.index)
    return safe_apply_df(series, cast_object, default, __type=type, strict=strict) if type and cast else series


def set_df_schema(df: pd.DataFrame, __base: pd.DataFrame, name: _KT, path: SchemaPath, how: Optional[PathType]=None,
                    type: Optional[Type]=None, mode: _PASS=None, cast=False, strict=True, default=None,
                    apply: SchemaApply=dict(), match: SchemaMatch=dict(), **context) -> pd.DataFrame:
    path_type = how if how else get_path_type(path)
    context = dict(context, type=type, cast=cast, strict=strict, default=default, apply=apply, match=match)
    if path_type in (PATH,CALLABLE): return set_df_value(df, __base, name, path, **context)
    elif path_type == TUPLE: return set_df_tuple(df, __base, name, path, **context)
    elif path_type == GLOBAL: return set_df_global(df, __base, name, **context)
    elif path_type == VALUE:
        __base[name] = path
        return __base
    else: raise TypeError(INVALID_PATH_TYPE_MSG(path))


def set_df_value(df: pd.DataFrame, __base: pd.DataFrame, name: _KT, path: _KT, default=None,
                apply: SchemaApply=dict(), match: SchemaMatch=dict(), **context) -> pd.DataFrame:
    if match_df(df, **match).empty: return __base
    series = from_df(df, path, default=default, **context)
    if apply:
        series = safe_apply_df(series, apply[FUNC], **drop_dict(apply, FUNC))
    if not is_df_sequence(series):
        raise TypeError(INVALID_VALUE_TYPE_MSG(series, PANDAS_SERIES, name))
    __base[name] = series
    return __base


def set_df_tuple(df: pd.DataFrame, __base: pd.DataFrame, name: _KT, path: Tuple, default=None,
                apply: SchemaApply=dict(), match: SchemaMatch=dict(), **context) -> pd.DataFrame:
    matches = match_df(df, **match).index
    __base[name] = default
    for __i, row in df.iterrows():
        __match = int(__i in matches)-1
        value = from_dict(row, path[__match], default=default, **context)
        if apply: value = apply_schema(value, **get_scala(apply, index=__match, default=dict()))
        __base.at[__i,name] = value
    return __base


def set_df_global(df: pd.DataFrame, __base: pd.DataFrame, name: Optional[_KT]=None,
                    apply: SchemaApply=dict(), match: SchemaMatch=dict(), **context) -> pd.DataFrame:
    df = match_df(df, **match)
    if not df.empty:
        results = safe_apply_df(df, apply[FUNC], **drop_dict(apply, FUNC))
        if name and is_df_sequence(results):
            __base[name] = results
        elif is_df(results): df = pd.concat([df, results], axis=0)
        else: raise TypeError(INVALID_APPLY_RETURN_MSG(results, PANDAS_OBJECT, "GLOBAL"))
    return __base


def match_df(df: pd.DataFrame, func: Optional[MatchFunction]=None, path: Optional[_KT]=None,
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
