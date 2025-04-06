from gscraper.base.types import _KT, _VT, _PASS, _BOOL, _TYPE, Comparable, Context, TypeHint
from gscraper.base.types import Index, IndexLabel, Keyword, Unit, IndexedSequence, RenameMap, TypeMap
from gscraper.base.types import Records, MappingData, TabularData, Data, JsonData, PandasData, HtmlData, ResponseData
from gscraper.base.types import ApplyFunction, MatchFunction, RegexFormat, PANDAS_DATA
from gscraper.base.types import is_bool_array, is_int_array, is_array, is_2darray, is_records, is_dfarray, is_tag_array
from gscraper.base.types import init_origin, is_list_type, is_dict_type, is_records_type, is_dataframe_type, is_bytes_type
from gscraper.base.types import is_comparable, is_kwargs_allowed

from gscraper.utils import isna, notna, is_empty, exists
from gscraper.utils import isna_plus, notna_plus, is_empty_plus, exists_plus
from gscraper.utils.cast import cast_str, cast_list, cast_tuple, cast_set

from typing import Any, Dict, List, Set
from typing import Callable, Iterable, Literal, Optional, Sequence, Tuple, Union
from numbers import Real

from ast import literal_eval
from bs4 import BeautifulSoup, Tag
from io import BytesIO, StringIO
import numpy as np
import json

from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from openpyxl.utils.exceptions import IllegalCharacterError
from pandas.core.indexes.base import Index as PandasIndex
import pandas as pd

from collections import defaultdict
from itertools import chain
import base64
import functools
import os
import random as rand
import re

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')


INVALID_ISIN_MSG = "Isin function requires at least one parameter: exact, include, and exclude."
SOURCE_SEQUENCE_TYPE_MSG = "Required array of HTML source as input."
LENGTH_MISMATCH_MSG = lambda left, right: f"Length of two input values are mismatched: [{left}, {right}]"

FILE_PATTERN_MISMATCH_MSG = "No files matching the filename pattern exist."
TABLE_PATH_ERROR_MSG = "Table path does not exist."
TABLE_TYPE_ERROR_MSG = "Table type is not valid."


def arg_or(*args: Union[bool,int,Set]) -> bool:
    if not args: return False
    else: return functools.reduce(lambda x,y: x|y, args)

def arg_and(*args: Union[bool,int,Set]) -> bool:
    if not args: return False
    else: return functools.reduce(lambda x,y: x&y, args)

def union(*arrays) -> List:
    if not arrays: return list()
    else: return functools.reduce(lambda x,y: x+y, arrays)

def inter(*arrays: Sequence) -> List:
    if not arrays: return list()
    else: return functools.reduce(lambda x,y: [e for e in x if e in y], arrays)

def diff(*arrays: Sequence) -> List:
    if not arrays: return list()
    else: return functools.reduce(lambda x,y: [e for e in x if e not in y], arrays)


###################################################################
############################## Exists #############################
###################################################################

def notna_one(*args) -> Any:
    for arg in args:
        if notna(arg): return arg
    if args: return args[-1]


def exists_one(*args) -> Any:
    for arg in args:
        if exists(arg): return arg
    if args: return args[-1]


def dropna(array: Sequence, default=None) -> List:
    if default is None: return [__e for __e in array if notna(__e)]
    else: return [__e if notna(__e) else default for __e in array]


def drop_empty(array: Sequence, default=None) -> List:
    if default is None: return [__e for __e in array if exists(__e)]
    else: return [__e if exists(__e) else default for __e in array]


def notna_dict(__m: Optional[Dict]=dict(), null_if: Dict=dict(), **context) -> Dict:
    if not null_if:
        return {__k: __v for __k, __v in dict(__m, **context).items() if notna(__v)}
    else: return {__k: __v for __k, __v in dict(__m, **context).items() if notna(__v) and (__v != null_if.get(__k))}


def exists_dict(__m: Optional[Dict]=dict(), null_if: Dict=dict(), **context) -> Dict:
    if not null_if:
        return {__k: __v for __k, __v in dict(__m, **context).items() if exists(__v)}
    else: return {__k: __v for __k, __v in dict(__m, **context).items() if exists(__v) and (__v != null_if.get(__k))}


def df_empty(df: pd.DataFrame, dropna=False, how: Literal["any","all"]="all") -> bool:
    return isinstance(df, pd.DataFrame) and (df.dropna(axis=0, how=how) if dropna else df).empty


def df_exists(df: pd.DataFrame, dropna=False, how: Literal["any","all"]="all") -> bool:
    return isinstance(df, pd.DataFrame) and not (df.dropna(axis=0, how=how) if dropna else df).empty


###################################################################
############################# Convert #############################
###################################################################

def to_array(__object, default=None, dropna=False, drop_empty=False, unique=False) -> List:
    if unique: return _unique(__object, dropna=dropna, drop_empty=drop_empty)
    elif drop_empty: return _drop_empty(__object, default)
    elif dropna: return _dropna(__object, default)
    else: return cast_list(__object)

def _dropna(__object, default=None) -> List:
    return dropna(cast_list(__object), default=default)

def _drop_empty(__object, default=None) -> List:
    return drop_empty(cast_list(__object), default=default)

def _unique(__object, dropna=False, drop_empty=False, unroll=False) -> List:
    return unique(*cast_list(__object), dropna=dropna, drop_empty=drop_empty, unroll=unroll)


def to_dict(__object: MappingData, orient: Optional[Literal["dict","list","index"]]="dict", depth=1) -> Dict:
    if isinstance(__object, Dict): return __object
    elif is_records(__object, empty=True): return chain_dict(__object, keep="first")
    elif isinstance(__object, pd.DataFrame):
        return fillna_dict(__object.to_dict(orient), depth=depth)
    else: return dict()


def to_records(__object: MappingData, depth=1) -> Records:
    if is_records(__object, empty=True): return __object
    elif isinstance(__object, pd.DataFrame):
        return fillna_records(__object.to_dict("records"), depth=depth)
    elif isinstance(__object, Dict): return [__object]
    else: return list()


def to_json(__object: Union[bytes,str,JsonData], default=None) -> JsonData:
    if isinstance(__object, (bytes,str)):
        try: return json.loads(__object)
        except:
            try: return literal_eval(__object) if isinstance(__object, str) else default
            except: return default
    else: return __object if isinstance(__object, (Dict,List)) else default


def to_dataframe(__object: MappingData, columns: Optional[IndexLabel]=None, index: Optional[Sequence]=None) -> pd.DataFrame:
    if isinstance(__object, pd.DataFrame): pass
    elif is_records(__object, empty=False): __object = convert_dtypes(pd.DataFrame(align_records(__object)))
    elif isinstance(__object, (dict,pd.Series)): __object = pd.DataFrame([__object])
    elif isinstance(__object, bytes) and __object:
        try: __object = pd.read_excel(__object)
        except: return pd.DataFrame(columns=cast_columns(columns))
    else: return pd.DataFrame(columns=cast_columns(columns))
    if (index is not None) and (safe_len(index, -1) == len(__object)): __object.index = index
    return __object


def to_series(__object: Union[pd.Series,Sequence,Any], index: Optional[Sequence]=None) -> pd.Series:
    if isinstance(__object, pd.Series): pass
    elif is_array(__object): __object = pd.Series(__object)
    else: __object = pd.Series([__object]*safe_len(index, 1), dtype="object")
    if safe_len(index, -1) == len(__object): __object.index = index
    return __object


def to_bytes(__object: MappingData) -> bytes:
    if isinstance(__object, bytes): return __object
    elif is_records(__object, empty=False): __object = pd.DataFrame(__object)
    elif not isinstance(__object, pd.DataFrame): return str(__object).encode()
    with BytesIO() as output:
        __object.to_excel(output, index=False)
        return output.getvalue()


def convert_data(data: Data, return_type: Optional[TypeHint]=None, depth=1) -> Data:
    if not return_type: return data
    elif is_records_type(return_type): return to_records(data, depth=depth)
    elif is_dataframe_type(return_type): return to_dataframe(data)
    elif is_dict_type(return_type): return to_dict(data, depth=depth)
    elif is_list_type(return_type): return cast_list(data)
    elif is_bytes_type(return_type): return to_bytes(data)
    try: return data if data else init_origin(return_type)
    except: return None


###################################################################
############################## Rename #############################
###################################################################

def rename_value(__s: str, rename: RenameMap, to: Optional[Literal["key","value"]]="value",
                if_null: Union[Literal["pass"],Any]="pass") -> str:
    renameMap = flip_dict(rename) if to == "key" else rename
    if renameMap and (__s in renameMap): return renameMap[__s]
    else: return __s if if_null == "pass" else if_null

def rename_dict(__m: Dict, rename: RenameMap) -> Dict:
    return {rename.get(__key,__key): __value for __key, __value in __m.items()}


def rename_records(__r: Records, rename: RenameMap) -> Records:
    if not rename: return __r
    return [rename_dict(__m, rename=rename) for __m in __r]


def rename_data(data: MappingData, rename: RenameMap) -> MappingData:
    if not rename: return data
    elif isinstance(data, Dict): return rename_dict(data, rename)
    elif is_records(data): return rename_records(data, rename)
    elif isinstance(data, PANDAS_DATA): return data.rename(columns=rename)
    elif is_array(data): return [rename.get(__value,__value) for __value in data]
    else: return rename.get(data,data)


###################################################################
############################### Get ###############################
###################################################################

def iloc(__s: IndexedSequence, indices: Index, default=None, if_null: Literal["drop","pass"]="drop") -> _VT:
    __indices, __length = map_index(indices), len(__s)
    if isinstance(__indices, int):
        return __s[__indices] if abs_idx(__indices) < __length else default
    elif not __indices:
        if if_null == "pass": return __s
        else: return list() if is_array(__indices) else default
    elif if_null == "drop":
        return [__s[__i] for __i in __indices if abs_idx(__i) < __length]
    else: return [(__s[__i] if abs_idx(__i) < __length else default) for __i in __indices]


def kloc(__m: Dict, keys: _KT, default=None, if_null: Literal["drop","pass"]="drop", reorder=True,
        values_only=False, hier=False, key_alias: Sequence[_KT]=list()) -> Union[_VT,Dict]:
    if hier:
        return hloc(__m, keys, default, if_null=if_null, values_only=values_only, key_alias=key_alias)
    elif not is_array(keys):
        return __m.get(keys, default)
    elif keys:
        keys = keys if reorder else unique(*__m.keys(), *keys)
        if if_null == "drop": __m = {__key: __m[__key] for __key in keys if __key in __m}
        else: __m = {__key: __m.get(__key, default) for __key in keys}
    else: __m = __m if if_null == "pass" else dict()
    return list(__m.values()) if values_only else __m


def hloc(__m: Dict, keys: Sequence[_KT], default=None, if_null: Literal["drop","pass"]="drop",
        values_only=False, key_alias: Sequence[_KT]=list()) -> Union[_VT,Dict]:
    if is_single_path(keys, hier=True):
        return hier_get(__m, keys, default)
    elif allin(map(exists, keys)):
        if len(key_alias) != len(keys):
            key_alias = [tuple(map(str, cast_list(__path))) for __path in keys]
        __m = {__key: hier_get(__m, __path, default) for __key, __path in zip(key_alias, keys)}
        if if_null == "drop": __m = notna_dict(__m)
    else: return __m if if_null == "pass" else dict()
    return list(__m.values()) if values_only else __m


def vloc(__r: Records, keys: _KT, default=None, if_null: Literal["drop","pass"]="drop", reorder=True,
        values_only=False, hier=False, key_alias: Sequence[_KT]=list(), axis=0) -> Union[Records,List,Dict[_KT,List]]:
    if axis == 1:
        __m = {__key: vloc(__r, __key, default=default, if_null=if_null, hier=hier) for __key in cast_tuple(keys)}
        if if_null == "drop": return {__k: __s for __k, __s in __m.items() if __s}
        else: return {__k: (__s if __s else list()) for __k, __s in __m.items()}
    context = dict(if_null=if_null, reorder=reorder, values_only=values_only, hier=hier, key_alias=key_alias)
    __r = [kloc(__m, keys, default=default, **context) for __m in __r]
    default = default if values_only or not is_array(keys) else dict()
    if if_null == "drop": return [__m for __m in __r if __m]
    else: return [(__m if __m else default) for __m in __r]


def cloc(df: PandasData, columns: IndexLabel, default=None, if_null: Literal["drop","pass"]="drop",
        reorder=True, values_only=False) -> Union[PandasData,_VT]:
    columns = cast_columns(columns)
    if isinstance(columns, str):
        if columns in df: df = df[columns]
        else: return init_df(df, columns=list()) if if_null == "drop" else default
    elif isinstance(df, pd.DataFrame):
        df = filter_columns(df, columns, if_null, reorder)
    df = fillna_data(df, default)
    if isinstance(df, pd.Series): return df.tolist() if values_only else df
    else: return list(df.to_dict("list").values()) if values_only else df


def filter_columns(df: pd.DataFrame, columns: List[str], if_null: Literal["drop","pass"]="drop", reorder=True) -> pd.DataFrame:
    __inter = inter(columns, get_columns(df)) if reorder else inter(get_columns(df), columns)
    if not __inter:
        return init_df(df, columns=list()) if if_null == "drop" else df
    elif not is_array(columns): return df[columns]
    elif (if_null == "pass") and (len(columns) != len(__inter)):
        columns = columns if reorder else unique(*__inter, *columns)
        return pd.concat([init_df(df, columns=columns), df[__inter]])
    else: return df[__inter]


def sloc(source: Tag, selectors: Sequence[_KT], default=None, if_null: Literal["drop","pass"]="drop",
        values_only=False, hier=False, key_alias: Sequence[_KT]=list(), index: Optional[Unit]=None,
        key=str(), text=False, sep=str(), strip=True, **kwargs) -> Union[_VT,Dict]:
    if not selectors:
        return source if if_null == "pass" else default
    loc = hier_select if hier else select_path
    context = dict(default=default, index=index, key=key, text=text, sep=sep, strip=strip)
    if is_single_selector(selectors, hier=hier, index=index):
        return loc(source, selectors, **context)
    if len(key_alias) != len(selectors):
        key_alias = [_get_selector_name(selector, hier=hier) for selector in selectors]
    __m = {__key: loc(source, __path, **context) for __key, __path in zip(key_alias, selectors)}
    if if_null == "drop": __m = exists_dict(__m)
    return list(__m.values()) if values_only else __m


def get_value(data: ResponseData, field: Union[_KT,Index], default=None, hier=False, **kwargs) -> _VT:
    if not field:
        return default
    elif isinstance(data, Dict):
        return hier_get(data, field, default)
    elif is_records(data):
        return hier_get(data, field, default) if hier else [hier_get(__m, field, default) for __m in data]
    elif isinstance(data, PANDAS_DATA):
        column = get_scala(field)
        return fillna_data(data[column], default) if column in data else default
    elif isinstance(data, Tag):
        return hier_select(data, field, default, **kwargs)
    elif is_array(data):
        index = get_scala(field)
        return data[index] if index < len(data) else default
    else: return default


def filter_data(data: ResponseData, fields: Optional[Union[_KT,Index]]=list(), default=None,
                if_null: Literal["drop","pass"]="drop", reorder=True, values_only=False, hier=False,
                key_alias: Sequence[_KT]=list(), **kwargs) -> Data:
    context = dict(default=default, if_null=if_null, values_only=values_only)
    map_context = dict(hier=hier, key_alias=key_alias)
    if not fields: return data if if_null == "pass" else default
    elif isinstance(data, Dict): return kloc(data, fields, reorder=reorder, **context, **map_context)
    elif is_records(data): return vloc(data, fields, reorder=reorder, **context, **map_context)
    elif isinstance(data, PANDAS_DATA): return cloc(data, fields, reorder=reorder, **context)
    elif isinstance(data, Tag): return sloc(data, fields, **context, **map_context, **kwargs)
    elif is_array(data): return iloc(data, fields, default=default, if_null=if_null)
    else: return data


###################################################################
############################### Set ###############################
###################################################################

def set_dict(__m: Dict, __keys: Optional[_KT]=list(), __values: Optional[_VT]=list(),
            if_exists: Literal["replace","ignore"]="replace", inplace=True, **context) -> Dict:
    if not inplace: __m = __m.copy()
    for __key, __value in dict(zip(cast_tuple(__keys), cast_tuple(__values)), **context).items():
        if (if_exists == "ignore") and (__key in __m): continue
        else: __m[__key] = __value
    if not inplace: return __m


def set_records(__r: Records, __keys: Optional[_KT]=list(), __values: Optional[_VT]=list(),
                if_exists: Literal["replace","ignore"]="replace", **context) -> Records:
    return [set_dict(__m, __keys, __values, if_exists=if_exists, inplace=False, **context) for __m in __r]


def set_df(df: PandasData, __columns: Optional[IndexLabel]=list(), __values: Optional[_VT]=list(),
            if_exists: Literal["replace","ignore"]="replace", **context) -> pd.DataFrame:
    df = df.copy()
    for __column, __value in dict(zip(cast_tuple(__columns), cast_tuple(__values)), **context).items():
        if (if_exists == "ignore") and (__column in df): continue
        else: df[__column] = __value
    return df


def set_data(data: MappingData, __keys: Optional[_KT]=list(), __values: Optional[_VT]=list(),
            if_exists: Literal["replace","ignore"]="replace", **context) -> Data:
    if is_records(data): return set_records(data, __keys, __values, if_exists=if_exists, **context)
    elif isinstance(data, PANDAS_DATA): return set_df(data, __keys, __values, if_exists=if_exists, **context)
    elif isinstance(data, Dict): return set_dict(data, __keys, __values, if_exists=if_exists, inplace=False, **context)
    else: return data


###################################################################
########################### Hierachy Get ##########################
###################################################################

def is_single_path(path: _KT, hier=False) -> bool:
    if not path: return True
    elif hier: return not is_2darray(path, how="any")
    else: return (not is_array(path)) or (len(path) == 1)


def hier_get(__m: Dict, __path: _KT, default=None) -> _VT:
    try:
        if not is_array(__path):
            return __m.get(__path, default)
        __m = __m.copy()
        for __key in __path:
            __m = __m[__key]
        return __m
    except: return default


def hier_set(__m: Dict, __path: _KT, value: _VT, inplace=True) -> Dict:
    try:
        if not inplace: __m = __m.copy()
        if is_array(__path):
            for __key in __path[:-1]:
                __m = __m[__key]
            __m[__path[-1]] = value
        else: __m[__path] = value
    except: pass
    if not inplace: return __m


###################################################################
############################### Isin ##############################
###################################################################

def allin(__iterable: Sequence[bool]) -> bool:
    return __iterable and all(__iterable)


def howin(__iterable: Sequence[bool], how: Literal["any","all"]="all") -> bool:
    return allin(__iterable) if how == "all" else any(__iterable)


def isin(__iterable: Iterable, exact: Optional[Keyword]=None, include: Optional[Keyword]=None,
        exclude: Optional[Keyword]=None, how: Literal["any","all"]="any", join=False, **kwargs) -> bool:
    __match = True
    if notna(exact): __match &= _isin(__iterable, exact, "exact", (how == "all"), join)
    if __match and notna(include): __match &= _isin(__iterable, include, "include", (how == "all"), join)
    if __match and notna(exclude): __match &= _isin(__iterable, exclude, "exclude", True, join)
    return __match


def _isin(__iterable: Iterable, values: Keyword, how: Literal["exact","include","exclude"]="exact", all=False, join=False) -> bool:
    if is_array(values):
        if how == "exact":
            how, all = "include", True
        return howin([_isin(__iterable, __v, how, all, join) for __v in values], how=("all" if all else "any"))
    elif is_array(__iterable):
        if isinstance(join, str): return _isin_value(concat(*__iterable, sep=join), values, how)
        elif join: return _isin_value(concat(*__iterable, sep=' '), values, how)
        else: return howin([_isin_value(__i, values, how) for __i in __iterable], how=("all" if all else "any"))
    else: return _isin_value(__iterable, values, how)


def _isin_value(__object: Any, __value: Any, how: Literal["exact","include","exclude"]="exact") -> bool:
    if how == "exact": return __value == __object
    elif how == "include": return __value in __object
    elif how == "exclude": return __value not in __object
    else: raise ValueError(INVALID_ISIN_MSG)


def isin_dict(__m: Dict, keys: Optional[_KT]=None, exact: Optional[Keyword]=None, include: Optional[Keyword]=None,
            exclude: Optional[Keyword]=None, how: Literal["any","all"]="any", join=False, if_null=False, hier=False) -> bool:
    if isna(exact) and isna(include) and isna(exclude): return True
    keys = keys if keys or hier else list(__m.keys())
    __values = kloc(__m, keys, if_null="drop", values_only=True, hier=hier)
    if isna(__values): return if_null
    elif is_single_path(keys, hier=hier): pass
    elif (how == "all") and (not if_null) and (len(keys) != len(__values)): return False
    return isin(__values, exact, include, exclude, how, join)


def isin_records(__r: Records, keys: Optional[_KT]=None, exact: Optional[Keyword]=None, include: Optional[Keyword]=None,
                exclude: Optional[Keyword]=None, how: Literal["any","all"]="any", join=False, if_null=False,
                hier=False, filter=True) -> Union[Records,List[bool]]:
    if isna(exact) and isna(include) and isna(exclude): return __r
    __matches = [isin_dict(__m, keys, exact, include, exclude, how, join, if_null, hier) for __m in __r]
    return [__m for __match, __m in zip(__matches, __r) if __match] if filter else __matches


def keyin_records(__r: Records, keys: _KT) -> Union[bool,Dict[_KT,bool]]:
    all_keys = union(*[list(__m.keys()) for __m in __r])
    return {__key: (__key in all_keys) for __key in keys} if is_array(keys) else keys in all_keys


def isin_df(df: pd.DataFrame, columns: Optional[IndexLabel]=None, exact: Optional[Keyword]=None,
        include: Optional[Keyword]=None, exclude: Optional[Keyword]=None, how: Literal["any","all"]="any",
        join=False, if_null=False, filter=True) -> PandasData:
    if isna(exact) and isna(include) and isna(exclude):
        return df if filter else pd.Series([True] * len(df), index=df.index)
    __columns = inter(cast_list(columns), df.columns) if columns or (not if_null) else df.columns.tolist()
    if df.empty or (not __columns):
        return pd.DataFrame(columns=df.columns) if filter else pd.Series([False] * len(df), index=df.index)
    __matches = pd.Series([True] * len(df), index=df.index)
    for __how, __values in zip(["exact", "include", "exclude"], [exact, include, exclude]):
        __all = True if __how == "exclude" else (how == "all")
        if isna(__values): continue
        elif join: __match = _isin_joined_columns(df[__matches], columns, __values, __how, __all, if_null)
        else: __match = _isin_columns(df[__matches], columns, __values, __how, __all, if_null)
        __matches = __matches & __match
    return df[__matches] if filter else __matches


def _isin_columns(df: pd.DataFrame, columns: IndexLabel, values: Keyword,
                how: Literal["exact","include","exclude"]="exact", all=False, if_null=False) -> pd.Series:
    __matches = pd.Series([all] * len(df), index=df.index)
    for __column in columns:
        if __column not in df: continue
        __match = _isin_series(df[__column], values, how, all).fillna(if_null)
        __matches = (__matches & __match) if all else (__matches | __match)
    return __matches


def _isin_joined_columns(df: pd.DataFrame, columns: IndexLabel, values: Keyword,
                        how: Literal["exact","include","exclude"]="exact", all=False, if_null=False) -> pd.Series:
    series = df[columns].apply(lambda row: concat(*row), axis=1)
    return _isin_series(series, values, how, all).fillna(if_null)


def _isin_series(series: pd.Series, values: Keyword, how: Literal["exact","include","exclude"]="exact", all=False) -> pd.Series:
    if is_array(values):
        if how == "exact": how, all = "include", True
        __op = arg_and if all else arg_or
        return __op(*[_isin_series(series, __v, how, all) for __v in values])
    else: return _isin_str_series(series.astype(str), str(values), how)


def _isin_str_series(series: pd.Series, value: str, how: Literal["exact","include","exclude"]="exact") -> pd.Series:
    if how == "exact": return series == value
    elif how == "include": return series.str.contains(value)
    elif how == "exclude": return ~series.str.contains(value)
    else: raise ValueError(INVALID_ISIN_MSG)


def isin_source(source: Union[Tag,Sequence[Tag]], selectors: _KT, exact: Optional[Keyword]=None,
                include: Optional[Keyword]=None, exclude: Optional[Keyword]=None, how: Literal["any","all"]="any",
                join=False, if_null=False, hier=False, filter=True, **kwargs) -> Union[bool,List[Tag],List[bool]]:
    if isna(exact) and isna(include) and isna(exclude):
        return source if filter else ([True] * len(source) if is_array(source) else True)
    args = (selectors, exact, include, exclude, how, join, if_null, hier)
    if is_array(source):
        __matches = [_isin_source_text(__s, *args, **kwargs) for __s in source]
        return [__s for __match, __s in zip(__matches, source) if __match] if filter else __matches
    else: return _isin_source_text(source, *args, **kwargs)


def _isin_source_text(source: Tag, selectors: _KT, exact: Optional[Keyword]=None, include: Optional[Keyword]=None,
                    exclude: Optional[Keyword]=None, how: Literal["any","all"]="any",
                    join=False, if_null=False, hier=False, **kwargs) -> bool:
    __values = sloc(source, selectors, if_null="drop", values_only=True, hier=hier, **kwargs)
    if not __values: return if_null
    elif is_single_selector(selectors, hier=hier, index=kwargs.get("index")): pass
    elif (how == "all") and (not if_null) and (len(selectors) != len(__values)): return False
    return isin(__values, exact, include, exclude, how, join)


def isin_data(data: ResponseData, path: Optional[Union[_KT,IndexLabel]]=None, exact: Optional[Keyword]=None,
            include: Optional[Keyword]=None, exclude: Optional[Keyword]=None, how: Literal["any","all"]="any",
            join=False, if_null=False, hier=False, filter=False, **kwargs) -> Union[bool,List[bool],pd.Series]:
    context = dict(exact=exact, include=include, exclude=exclude, how=how, join=join, if_null=if_null)
    if isinstance(data, Dict): return isin_dict(data, path, hier=hier, **context)
    elif is_records(data): return isin_records(data, path, hier=hier, filter=filter, **context)
    elif isinstance(data, pd.DataFrame): return isin_df(data, path, filter=filter, **context)
    elif isinstance(data, Tag): return isin_source(data, hier=hier, **context, **kwargs)
    else: return isin(data, **context)


###################################################################
############################## Chain ##############################
###################################################################

def unique(*elements, dropna=False, drop_empty=False, unroll=False) -> List:
    array = list()
    for __e in (flatten(*elements) if unroll else elements):
        if drop_empty and is_empty(__e): continue
        elif dropna and isna(__e): continue
        elif __e in array: continue
        else: array.append(__e)
    return array


def unique_keys(__r: Records, forward=True) -> _KT:
    keys = list(__r[0].keys()) if __r else list()
    for __keys in map(lambda x: list(x.keys()), __r[1:]):
        for __i, __key in enumerate(__keys):
            if __key in keys: continue
            try:
                for __forward in range(-1, __i)[::-1]:
                    for __backward in range(__i+1, len(__keys)+1):
                        if __keys[__backward] in keys: break
                    if __keys[__forward] in keys: break
                keys.insert(keys.index(__keys[(__forward if forward else __backward)]), __key)
            except: keys.append(__key)
    return keys


def filter_exists(__object, drop_empty=True) -> Any:
    if isinstance(__object, List):
        return [__value for __value in __object if (exists(__value) if drop_empty else notna(__value))]
    if isinstance(__object, Tuple):
        return tuple(__value for __value in __object if (exists(__value) if drop_empty else notna(__value)))
    elif isinstance(__object, Dict):
        return {key: __value for key, __value in __object.items() if (exists(__value) if drop_empty else notna(__value))}
    elif isinstance(__object, pd.DataFrame):
        return __object.dropna(axis=1)
    elif (exists(__object) if drop_empty else notna(__object)): return __object
    else: return None


def concat_array(left: Sequence, right: Sequence, left_index: Sequence[int]) -> List:
    left, right = cast_list(left).copy(), cast_list(right).copy()
    return [left.pop(0) if is_left else right.pop(0) for is_left in left_index]


def chain_dict(__object: Sequence[Dict], keep: Literal["fist","last"]="first",
                from_iterable=False, from_dataframe=False) -> Dict:
    if from_iterable: return _chain_iter_dict(__object)
    elif from_dataframe: return _chain_df_dict(__object)
    else: base = dict()
    for __m in __object:
        if not isinstance(__m, Dict): continue
        for __key, __value in __m.items():
            if (keep == "first") and (__key in base): continue
            else: base[__key] = __value
    return base


def _chain_iter_dict(__object: Sequence[Dict[str,Iterable]]) -> Dict[str,Iterable]:
    base = dict()
    for __m in __object:
        if not isinstance(__m, Dict): continue
        for __key, __value in __m.items():
            if isinstance(__value, Iterable):
                if __key not in base:
                    base[__key] = __value
                else: base[__key] += __value
    return base


def _chain_df_dict(__object: Sequence[Dict[str,pd.DataFrame]]) -> Dict[str,pd.DataFrame]:
    base = dict()
    for __m in __object:
        if not isinstance(__m, Dict): continue
        for __key, __value in __m.items():
            if df_exists(__value):
                if __key not in base:
                    base[__key] = __value
                else: base[__key] = pd.concat([base[__key], __value])
    return base


def concat_df(__object: Sequence[pd.DataFrame], axis=0, keep: Literal["fist","last"]="first") -> pd.DataFrame:
    columns = inter(*[get_columns(df) for df in __object])
    if axis == 1: __object = diff_df(*__object, keep=keep)
    else: __object = [df for df in __object if df_exists(df)]
    return pd.concat(__object, axis=axis) if __object else pd.DataFrame(columns=columns)


def chain_exists(data: Data, data_type: Optional[TypeHint]=None, keep: Literal["fist","last"]="first") -> Data:
    if is_dfarray(data): return concat_df([df for df in data if df_exists(df)])
    elif is_2darray(data): return list(chain.from_iterable([__s for __s in data if is_array(__s)]))
    elif data_type and is_dict_type(data_type) and is_records(data, how="any"):
        return chain_dict([__m for __m in data if isinstance(__m, Dict)], keep=keep)
    else: return filter_exists(data)


###################################################################
############################# Groupby #############################
###################################################################

def groupby_records(__r: Records, by: _KT, if_null: Literal["drop","pass"]="drop", drop=False, hier=False) -> Dict[_KT,Records]:
    by = cast_tuple(by)
    groups = defaultdict(list)
    for __m in __r:
        values = kloc(__m, by, default=str(), if_null=if_null, values_only=True, hier=hier)
        if len(values) != len(by): continue
        else: groups[values[0] if len(values) == 1 else tuple(values)].append(drop_dict(__m, by) if drop else __m)
    return dict(groups)


def groupby_df(df: pd.DataFrame, by: _KT, if_null: Literal["drop","pass"]="drop") -> Dict[_KT,pd.DataFrame]:
    counts = cloc(df, cast_list(by), if_null=if_null).value_counts().reset_index(name="count")
    keys = counts.drop(columns="count").to_dict("split")["data"]
    return {key if len(key) == 1 else tuple(key): match_df(df, **dict(zip(by, map(lambda x: (lambda y: x == y), key))))
            for key in keys}


def groupby_source(source: Sequence[Tag], by: Union[Dict[_KT,_KT],_KT],
                    if_null: Literal["drop","pass"]="drop", hier=False) -> Dict[_KT,Tag]:
    if not is_array(source): raise TypeError(SOURCE_SEQUENCE_TYPE_MSG)
    if not isinstance(by, Dict):
        by = {0: by} if is_single_selector(by, hier=hier) else dict(enumerate(cast_tuple(by)))
    groups = dict()
    for __key, selector in by.items():
        name = _get_class_name(selector)
        loc = hier_select if hier else select_path
        __match = lambda __value: (name in cast_tuple(__value)) if name else bool(__value)
        __matches = [__match(loc(__s, selector)) for __s in source]
        groups[__key] = [__s for __i, __s in enumerate(source) if __matches[__i]]
        source = [__s for __i, __s in enumerate(source) if not __matches[__i]]
    if (if_null == "pass") and source: groups[None] = source
    return groups


def groupby_data(data: Data, by: Union[Dict[_KT,_KT],_KT], if_null: Literal["drop","pass"]="drop",
                hier=False, **kwargs) -> Dict[_KT,Data]:
    if is_records(data): return groupby_records(data, by, if_null=if_null, hier=hier)
    elif is_dfarray(data): return groupby_df(data, by, if_null=if_null)
    elif is_tag_array(data): return groupby_source(data, by, if_null=if_null, hier=hier)
    else: return dict()


###################################################################
############################## Apply ##############################
###################################################################

def safe_apply(__object, __applyFunc: ApplyFunction, default=None, **context) -> Any:
    try: return __applyFunc(__object, **context) if context and is_kwargs_allowed(__applyFunc) else __applyFunc(__object)
    except: return default


def map_context(__keys: Optional[_KT]=list(), __values: Optional[_VT]=list(), __default=None, **context) -> Context:
    if context: return context
    elif not (__keys and (__values or __default)): return dict()
    __keys, __values = cast_tuple(__keys), (__values if __values else __default)
    if not is_array(__values): __values = [__values]*len(__keys)
    elif len(__keys) != len(__values): return dict()
    return {__key:__value for __key, __value in zip(__keys, __values)}


def apply_array(__s: IndexedSequence, __indices: Optional[Index]=list(), __applyFunc: Optional[ApplyFunction]=list(),
                apply: Optional[ApplyFunction]=None, all_indices=False, __default: _PASS=None, **context) -> IndexedSequence:
    if not __s: return __s
    __s = __s.copy()
    if all_indices: return [apply(__e) for __e in __s]
    for __i, __apply in map_context(__indices, __applyFunc, __default=apply, **context).items():
        if abs_idx(__i) < len(__s):
            __s[__i] = __apply(__s[__i])
    return __s


def apply_dict(__m: Dict, __keys: Optional[_KT]=list(), __applyFunc: Optional[ApplyFunction]=list(),
                apply: Optional[ApplyFunction]=None, all_keys=False, __default: _PASS=None, **context) -> Dict:
    if not __m: return __m
    __m = __m.copy()
    if all_keys: return {__key: apply(__values) for __key, __values in __m.items()}
    for __key, __apply in map_context(__keys, __applyFunc, __default=apply, **context).items():
        if __key in __m:
            __m[__key] = __apply(__m[__key])
    return __m


def apply_records(__r: Records, __keys: Optional[_KT]=list(), __applyFunc: Optional[ApplyFunction]=list(),
                    apply: Optional[ApplyFunction]=None, all_keys=False, scope: Literal["keys","dict"]="keys",
                    __default=None, **context) -> Records:
    if not __r: return __r
    elif scope == "keys":
        return [apply_dict(__m, __keys, __applyFunc, all_keys=all_keys, apply=apply, **context) for __m in __r]
    elif scope == "dict":
        return [apply(__m) for __m in __r]
    else: return list()


def apply_df(df: PandasData, __columns: Optional[IndexLabel]=list(), __applyFunc: Optional[ApplyFunction]=list(),
            apply: Optional[ApplyFunction]=None, all_cols=False, __default: _PASS=None, **context) -> pd.DataFrame:
    if df.empty: return df
    elif isinstance(df, pd.Series):
        return pd.Series(apply_dict(df.to_dict(), __columns, __applyFunc, apply, all_cols, **context))
    if all_cols: return df.copy().apply({__column: apply for __column in df.columns})
    context = map_context(__columns, __applyFunc, __default=apply, **context)
    context = {str(__column): __apply for __column, __apply in context.items() if __column in df}
    return df.copy().apply(context)


def safe_apply_df(__object: PandasData, __applyFunc: ApplyFunction, default: Union[PandasData,Sequence,Any]=None,
                    by: Literal["row","cell"]="row", **context) -> PandasData:
    __object = __object.copy()
    if isinstance(__object, pd.Series):
        __object = __object.apply(lambda x: safe_apply(x, __applyFunc, **context))
    elif isinstance(__object, pd.DataFrame):
        if by == "row":
            __object = __object.apply(lambda x: safe_apply(x, __applyFunc, **context), axis=1)
        elif by == "cell":
            for __column in __object.columns:
                __object[__column] = safe_apply_df(__object[__column].copy(), __applyFunc, **context)
    return fillna_data(__object, default)


def apply_data(data: Data, __keys: Optional[Union[_KT,Index]]=list(), __applyFunc: Optional[ApplyFunction]=list(),
                apply: Optional[ApplyFunction]=None, all_keys=False, default=None, **context) -> Data:
    if isinstance(data, Dict): return apply_dict(data, __keys, __applyFunc, apply=apply, all_keys=all_keys, **context)
    if is_records(data): return apply_records(data, __keys, __applyFunc, apply=apply, all_keys=all_keys, **context)
    elif isinstance(data, PANDAS_DATA): return apply_df(data, __keys, __applyFunc, apply=apply, all_cols=all_keys, **context)
    elif is_array(data): return apply_array(data, __keys, __applyFunc, apply=apply, all_indices=all_keys, **context)
    else: return safe_apply(data, apply, default=default, **context)


###################################################################
############################## Match ##############################
###################################################################

def map_match(__keys: Optional[_KT]=list(), __matchFunc: Optional[MatchFunction]=list(),
                __default=None, **context) -> Context:
    context = map_context(__keys, __matchFunc, __default, **context)
    return {__key: (__match if isinstance(__match, Callable) else lambda x: x == __match)
            for __key, __match in context.items()}


def match_array(__s: IndexedSequence, __indices: Optional[Index]=list(), __matchFunc: Optional[MatchFunction]=list(),
                match: Optional[MatchFunction]=None, all_indices=False, how: Literal["filter","all","indexer"]="filter",
                __default=None, **context) -> Union[IndexedSequence,_BOOL]:
    if not __s: return False if how == "all" else list()
    elif all_indices:
        return match_array(__s, how=how, **dict(list(range(len(__s))), [match]*len(__s)))
    context = map_match(__indices, __matchFunc, __default=match, **context)
    matches = {__i: safe_apply(__s[__i], __match, False) for __i, __match in context.items() if abs_idx(__i) < len(__s)}
    if how == "filter":
        return [__s[__i] for __i, __match in matches.items() if __match]
    else: return all(matches.values()) if how == "all" else list(matches.values())


def match_dict(__m: Dict, __keys: Optional[_KT]=list(), __matchFunc: Optional[MatchFunction]=list(),
                match: Optional[MatchFunction]=None, all_keys=False, how: Literal["filter","all","indexer"]="filter",
                __default: _PASS=None, **context) -> Union[Dict,bool]:
    if not __m: return False if how == "all" else dict()
    elif all_keys:
        return safe_apply(__m, match, False)
    context = map_match(__keys, __matchFunc, __default=match, **context)
    matches = {__key: safe_apply(__m[__key], __match, False) for __key, __match in context.items() if __key in __m}
    if how == "filter":
        return {__key: __m[__key] for __key, __match in matches.items() if __match}
    else: return all(matches.values()) if how == "all" else matches


def match_records(__r: Records, __keys: Optional[_KT]=list(), __matchFunc: Optional[MatchFunction]=list(),
                match: Optional[MatchFunction]=None, all_keys=False, how: Literal["filter","all","indexer"]="filter",
                scope: Literal["keys","dict"]="keys", __default=None, **context) -> Union[Records,_BOOL]:
    if not __r: return False if how == "all" else list()
    elif scope == "keys":
        matches = [match_dict(__m, __keys, __matchFunc, all_keys, match, how="all", **context) for __m in __r]
    elif scope == "dict":
        matches = [safe_apply(__m, match, default=False, **context) for __m in __r]
    else: matches = [False] * len(__r)
    return iloc(__r, matches, if_null="drop") if how == "filter" else (all(matches) if how == "all" else matches)


def match_df(df: PandasData, __columns: Optional[IndexLabel]=list(), __matchFunc: Optional[MatchFunction]=list(),
            match: Optional[MatchFunction]=None, all_cols=False, how: Literal["filter","all","indexer"]="filter",
            __default: _PASS=None, **context) -> Union[pd.DataFrame,pd.Series,bool]:
    if df.empty: return False if how == "all" else (df if how == "filter" else pd.Series(dtype=bool))
    context = map_match(__columns, __matchFunc, __default=match, **context)
    matches = apply_df(df, all_cols=all_cols, **context).all(axis=1)
    if how == "filter": return df[matches]
    else: return matches.all(axis=0) if how == "all" else matches


def match_data(data: Data, __keys: Optional[Union[_KT,Index]]=list(), __matchFunc: Optional[MatchFunction]=list(),
                match: Optional[MatchFunction]=None, all_keys=False, how: Literal["filter","all","indexer"]="filter",
                fields: Optional[Union[_KT,Index]]=list(), default=None, **context) -> Data:
    if isinstance(data, Dict): return match_dict(data, __keys, __matchFunc, match=match, all_keys=all_keys, how=how, **context)
    if is_records(data): return match_records(data, __keys, __matchFunc, match=match, all_keys=all_keys, how=how, **context)
    elif isinstance(data, PANDAS_DATA): return match_df(data, __keys, __matchFunc, match=match, all_cols=all_keys, how=how, **context)
    elif is_array(data): return match_array(data, __keys, __matchFunc, match=match, all_indices=all_keys, how=how, **context)
    else: return safe_apply(data, match, default=default, **context)


###################################################################
############################# Select ##############################
###################################################################

def clean_html(__object: str) -> str:
    return BeautifulSoup(cast_str(__object), "lxml").get_text()


def clean_tag(__object: str) -> str:
    return re.sub("<[^>]*>", "", cast_str(__object))


def _from_selector(selector: _KT, index: Optional[Unit]=None) -> Tuple[str,Unit]:
    if not (isinstance(selector, (List,str)) and selector): return str(), None
    elif not isinstance(selector, List): return selector, index
    selector = selector.copy()
    index = selector.pop() if isinstance(selector[-1], (int,List,Tuple)) else index
    return ' > '.join(selector), index


def is_single_selector(selector: _KT, hier=False, index: Optional[Unit]=None) -> bool:
    if isinstance(index, int): return True
    elif not hier: return not is_2darray(selector, how="any")
    elif not is_array(selector): return True
    elif len(selector) == 1: return not is_array(selector[0])
    else: return all(
        [(not is_2darray(__s, how="any")) and isinstance(_from_selector(__s)[1], int) for __s in selector[:-1]])


def select(source: Tag, selector: _KT, index: Optional[Unit]=None) -> Union[Tag,List[Tag]]:
    selector, index = _from_selector(selector, index=index)
    if not selector: return source
    elif index == 0: return source.select_one(selector)
    elif not index: return source.select(selector)
    else: return iloc(source.select(selector), index)


def select_one(source: Tag, selector: _KT) -> Tag:
    selector, _ = _from_selector(selector)
    if not selector: return source
    else: return source.select_one(selector)


def select_text(source: Tag, selector: _KT, index: Optional[Index]=None, sep=str(), strip=True) -> Union[str,List[str]]:
    source = select(source, selector, index=index)
    try:
        if is_array(source):
            return [__s.get_text(sep, strip=strip) for __s in source if isinstance(__s, Tag)]
        else: return source.get_text(sep, strip=strip)
    except (AttributeError, IndexError, TypeError):
        return list() if is_array(source) else str()


def select_attr(source: Tag, selector: str, key: str, index: Optional[Index]=None) -> Union[str,List[str],List[List[str]]]:
    source = select(source, selector, index=index)
    try:
        if is_array(source):
            return [__s.attrs.get(key,str()) for __s in source if isinstance(__s, Tag)]
        else: return source.attrs.get(key,str())
    except (AttributeError, IndexError, TypeError):
        return list() if is_array(source) else str()


###################################################################
########################### Select Path ###########################
###################################################################

def _from_selector_path(__path: _KT, key=str(), text=False) -> Tuple[_KT,str,str]:
    if isinstance(__path, str):
        if key: return (__path, key, "attr")
        elif __path == "text()": return (str(), str(), "text")
        elif __path.startswith('@'): return (str(), __path[1:], "attr")
        elif __path.startswith('.'): return (str(), "class", "attr")
        elif __path.startswith('#'): return (str(), "id", "attr")
        else: return (__path, str(), ("text" if text else "source"))
    elif is_array(__path) and __path:
        selector, key, by = _from_selector_path(__path[-1])
        return ((__path[:-1]+[selector] if selector else __path[:-1]), key, by)
    else: return (str(), str(), "source")


def _get_selector_name(__path: _KT, key=str(), hier=False) -> str:
    path = (' > '.join([_get_selector_name(__s, key) if is_array(__s) else str(__s) for __s in __path[:-1]]) if hier else str())
    if hier: __path = __path[-1]
    selector, key, by = _from_selector_path(__path, key)
    selector, index = _from_selector(selector)
    if path: selector = ' > '.join(path, selector) if selector else path
    return selector + (f"[{index}]" if notna(index) else str()) + (f"[@{key}]" if key else str())


def _get_class_name(selector: _KT) -> str:
    tag = str(get_scala(selector, -1))
    return tag[1:] if tag.startswith('.') or tag.startswith('#') else str()


def select_path(source: Tag, __path: _KT, default=None, index: Optional[Unit]=None, key=str(),
                text=False, sep=str(), strip=True, **kwargs) -> HtmlData:
    selector, key, by = _from_selector_path(__path, key, text)
    try:
        if by == "text": return select_text(source, selector, index=index, sep=sep, strip=strip)
        elif by == "attr": return select_attr(source, selector, key, index=index)
        else: return select(source, selector, index=index)
    except: return default


def hier_select(source: Tag, __path: _KT, default=None, index: Optional[Unit]=None, key=str(),
                text=False, sep=str(), strip=True, **kwargs) -> HtmlData:
    try:
        if is_array(__path):
            for selector in __path[:-1]:
                source = select(source, selector)
            __path = __path[-1] if __path else str()
        return select_path(source, __path, default, index, key, text, sep, strip)
    except: return default


###################################################################
############################ Aggregate ############################
###################################################################

def aggregate_array(__s: Iterable[Real], func: Union[Callable,str], if_null: Optional[Real]=None) -> Real:
    if if_null is not None:
        __s = fillna_array(__s, if_null, fill_empty=False)
    __s = [__e for __e in __s if isinstance(__e, Real)]
    if func == "count": return len(__s)
    elif func == "first": return __s[0]
    elif func == "last": return __s[-1]
    try: return func(__s) if isinstance(func, Callable) else getattr(np, func)(__s)
    except: return None


def aggregate_series(series: pd.Series, func: Union[Callable,str], if_null: Optional[Real]=None) -> Real:
    series = (series if if_null is None else series.fillna(if_null)).dropna()
    if func == "count": return len(series)
    elif func == "first": return series.iloc[0]
    elif func == "last": return series.iloc[-1]
    try: return func(series) if isinstance(func, Callable) else series.agg(func)
    except: return None


def aggregate_data(data: Union[Iterable[Real],pd.Series], func: Union[Callable,str], if_null: Optional[Real]=None) -> Real:
    if isinstance(data, pd.Series): return aggregate_series(data, func, if_null)
    elif isinstance(data, Iterable): return aggregate_array(data, func, if_null)
    else: return None


###################################################################
############################## Align ##############################
###################################################################

def align_array(*args: Sequence, alignment: Literal["min","max","first"]="min", default=None,
                dropna=False, drop_empty=False, unique=False) -> Tuple[List,...]:
    if not args: return tuple()
    arrays = _fill_arrays(args, alignment, default)
    indices = set(range(0, len(arrays[0])))
    targets = (arrays[0],) if alignment == "first" else arrays
    if unique: indices = _get_unique_index(targets, dropna, drop_empty)
    elif drop_empty: indices = sorted(arg_and(indices, *[{__i for __i, __e in enumerate(__s) if exists(__e)} for __s in targets]))
    elif dropna: indices = sorted(arg_and(indices, *[{__i for __i, __e in enumerate(__s) if notna(__e)} for __s in targets]))
    else: return arrays
    return tuple([__s[__i] for __i in indices] for __s in arrays)


def _fill_arrays(arrays: Tuple[Sequence,...], alignment: Literal["min","max","first"]="min", default=None) -> Tuple[List,...]:
    arrays = tuple(cast_list(__s) for __s in arrays)
    count = len(arrays[0]) if alignment == "first" else (max(map(len, arrays)) if alignment == "max" else min(map(len, arrays)))
    default = fill_array(default, count=len(arrays), value=None) if isinstance(default, Tuple) else [default]*len(arrays)
    return tuple(fill_array(cast_list(__s), count, __default) for __s, __default in zip(arrays, default))


def _get_unique_index(arrays: Tuple[Sequence,...], dropna=False, drop_empty=False) -> List[int]:
    indices, memory = list(), set()
    for __i, __e in enumerate(zip(*arrays)):
        if drop_empty and any(map(is_empty, __e)): continue
        elif dropna and any(map(isna, __e)): continue
        elif __e not in memory:
            memory.add(__e)
            indices.append(__i)
    return indices


def align_dict(__m: Dict[_KT,Sequence], alignment: Literal["min","max"]="min", default=None,
                dropna=False, drop_empty=False, unique=False) -> Dict[_KT,List]:
    if not __m: return dict()
    keys = list(__m.keys())
    values = align_array(*[__m[__key] for __key in keys], alignment, default, dropna, drop_empty, unique)
    return dict(zip(keys, values))


def align_records(__r: Records, default=None, forward=True) -> Records:
    keys = unique_keys(__r, forward=forward)
    return [kloc(__m, keys, default=default, if_null="pass") for __m in __r if isinstance(__m, Dict)]


###################################################################
############################# Between #############################
###################################################################

def between(__object: Comparable, left: Optional[Real]=None, right: Optional[Real]=None,
            inclusive: Literal["both","neither","left","right"]="both", null=False) -> bool:
    try:
        match_left = ((left is None) or (__object >= left if inclusive in ["both","left"] else __object > left))
        match_right = ((right is None) or (__object <= right if inclusive in ["both","right"] else __object < right))
        return match_left & match_right
    except: return null


def between_records(__r: Records, __key: Optional[_KT]=list(), left: Optional[Real]=None, right: Optional[Real]=None,
            inclusive: Literal["both","neither","left","right"]="both", null=False) -> Records:
    if __key not in unique_keys(__r): return __r
    else: return [__m for __m in __r if between(__m.get(__key), left, right, inclusive, null)]


def between_df(df: pd.DataFrame, __column: Optional[IndexLabel]=list(), left: Optional[Real]=None, right: Optional[Real]=None,
            inclusive: Literal["both","neither","left","right"]="both", null=False) -> pd.DataFrame:
    if __column not in df: return df
    else: return df[df[__column].apply(lambda x: between(x, left, right, inclusive, null))]


def between_data(data: MappingData, field: Optional[_KT]=list(), left: Optional[Real]=None, right: Optional[Real]=None,
                inclusive: Literal["both","neither","left","right"]="both", null=False) -> MappingData:
    if is_records(data): return between_records(data, field, left, right, inclusive, null)
    elif isinstance(data, pd.DataFrame): return between_df(data, field, left, right, inclusive, null)
    else: return data


###################################################################
############################### Drop ##############################
###################################################################

def drop_dict(__m: Dict, keys: _KT, inplace=False) -> Dict:
    if not inplace: __m = __m.copy()
    for __key in cast_tuple(keys):
        if __key in __m:
            __m.pop(__key, None)
    if not inplace: return __m


def split_dict(__m: Dict, keys: _KT) -> Tuple[Dict,Dict]:
    keys = cast_tuple(keys)
    return kloc(__m, keys, if_null="drop"), drop_dict(__m, keys, inplace=False)


def drop_records(__r: Records, keys: _KT) -> Records:
    return [drop_dict(__m, keys, inplace=False) for __m in __r]


def drop_df(df: PandasData, columns: IndexLabel) -> PandasData:
    if isinstance(df, pd.DataFrame):
        return df.drop(columns=inter(df.columns, cast_tuple(columns)))
    elif isinstance(df, pd.Series):
        return df.drop(inter(df.index, cast_tuple(columns)))
    else: return df


def drop_data(data: MappingData, __keys: Optional[_KT]=list()) -> Data:
    if is_records(data): return drop_records(data, __keys)
    elif isinstance(data, PANDAS_DATA): return drop_df(data, __keys)
    elif isinstance(data, Dict): return drop_dict(data, __keys, inplace=False)
    else: return data


###################################################################
############################ Duplicated ###########################
###################################################################

def diff_dict(*args: Dict, skip: IndexLabel=list(), keep: Literal["fist","last"]="fist") -> Tuple[Dict,...]:
    duplicates = arg_and(*map(lambda x: set(x.keys), args)) - cast_set(skip)
    keep = len(args)-1 if keep == "last" else 0
    return tuple((__m if __i == keep else drop_dict(__m, duplicates, inplace=False)) for __i, __m in enumerate(args))


def diff_df(*args: pd.DataFrame, skip: IndexLabel=list(),
            keep: Literal["fist","last"]="first") -> Tuple[pd.DataFrame,...]:
    duplicates = arg_and(*map(lambda x: set(x.columns), args)) - cast_set(skip)
    keep = len(args)-1 if keep == "last" else 0
    return tuple((df if __i == keep else df.drop(columns=duplicates)) for __i, df in enumerate(args))


def drop_duplicates(__r: Records, keys: Optional[_KT]=list(), keep: Literal["fist","last",False]="first") -> Records:
    if keep == False: return _drop_duplicates_all(__r, keys)
    base, history, keys = list(), list(), cast_list(keys)
    for __m in (__r[::-1] if keep == "last" else __r):
        values = tuple(kloc(__m, keys, if_null="pass").values())
        if values not in history:
            base.append(__m)
            history.append(values)
    return (base[::-1] if keep == "last" else base)


def _drop_duplicates_all(__r: Records, keys: Optional[_KT]=list()) -> Records:
    history, keys = defaultdict(list), cast_list(keys)
    to_comparable = lambda x: x if is_comparable(x) else str(x)
    for __i, __m in enumerate(__r):
        values = tuple(map(to_comparable, kloc(__m, keys, if_null="pass").values()))
        history[values].append(__i)
    indices = sorted(union(*[index for index in history.values() if len(index) == 1]))
    return iloc(__r, indices, if_null="drop")


###################################################################
############################### Fill ##############################
###################################################################

def fill_array(__s: Sequence, count: int, value=None) -> List:
    return [(__s[__i] if __i < len(__s) else value) for __i in range(count)]


def fillna_array(__s: Sequence, value=None, fill_empty=False, depth=1) -> List:
    if depth < 1: return __s
    fillna = lambda __value: fillna_data(__value, value, fill_empty=fill_empty, depth=depth-1) if depth > 1 else __value
    return [(fillna(__e) if (exists(__e) if fill_empty else notna(__e)) else value) for __e in __s]


def fillna_dict(__m: Dict, value=None, fill_empty=False, depth=1) -> Dict:
    if depth < 1: return __m
    fillna = lambda __value: fillna_data(__value, value, fill_empty=fill_empty, depth=depth-1) if depth > 1 else __value
    return {__key: (fillna(__value) if (exists(__value) if fill_empty else notna(__value)) else value) for __key, __value in __m.items()}


def fillna_records(__r: Records, value=None, fill_empty=False, depth=1) -> Records:
    if depth < 1: return __r
    return [fillna_dict(__m, value, fill_empty=fill_empty, depth=depth) for __m in __r]


def fillna_df(__object: pd.DataFrame, value: Union[PandasData,Sequence,Any]=None) -> pd.DataFrame:
    __object = __object.copy()
    if isinstance(value, (pd.DataFrame,pd.Series,Sequence)):
        if len(__object) != len(value):
            raise ValueError(LENGTH_MISMATCH_MSG(__object.shape, value.shape))
        elif isinstance(value, (pd.Series,Sequence)):
            value = to_series(value, index=__object.index)
            for __column in __object.columns:
                __object[__column] = __object[__column].combine_first(value)
            return __object
        else: return __object.combine_first(to_dataframe(value, index=__object.index))
    else: return __object.where(pd.notna(__object), value)


def fillna_series(__object: pd.Series, value: Union[pd.Series,Sequence,Any]=None) -> pd.Series:
    __object = __object.copy()
    if isinstance(value, (pd.Series,Sequence)):
        value = to_series(value, index=__object.index)
        if len(__object) != len(value):
            raise ValueError(LENGTH_MISMATCH_MSG(__object.shape, value.shape))
        else: return __object.combine_first(value)
    else: return __object.fillna(value)


def fillna_data(__object: Union[PandasData,Records], value: Union[PandasData,Sequence,Any]=None,
                fill_empty=False, depth=1) -> Union[PandasData,Records]:
    if isna_plus(value) and isinstance(__object, PANDAS_DATA): return __object
    elif isinstance(__object, pd.DataFrame): return fillna_df(__object, value)
    elif isinstance(__object, pd.Series): return fillna_series(__object, value)
    elif is_records(__object): return fillna_records(__object, value, fill_empty=fill_empty, depth=depth)
    elif isinstance(__object, Dict): return fillna_dict(__object, value, fill_empty=fill_empty, depth=depth)
    elif is_array(__object): return fillna_array(__object, fill_empty=fill_empty, depth=depth)
    else: return __object


###################################################################
############################### Sort ##############################
###################################################################

def values_to_back(__s: Sequence, values: _VT) -> List:
    return sorted(__s, key=functools.cmp_to_key(lambda x, y: -1 if y in cast_tuple(values) else 0))


def keys_to_back(__m: Dict, keys: _KT) -> Dict:
    return dict(sorted(__m.items(), key=functools.cmp_to_key(lambda x, y: -1 if y[0] in cast_tuple(keys) else 0)))


def flip_dict(__m: Dict) -> Dict:
    return {__v: __k for __k, __v in __m.items()}


def sort_records(__r: Records, by: _KT, ascending=True) -> Records:
    return sorted(__r, key=lambda x: kloc(x, cast_tuple(by), values_only=True), reverse=(not ascending))


def sort_values(data: TabularData, by: _KT, ascending: _BOOL=True) -> Data:
    asc = ascending if isinstance(ascending, bool) else bool(get_scala(ascending))
    if is_records(data): return sort_records(data, by=by, ascending=asc)
    elif isinstance(data, pd.DataFrame): return data.sort_values(by, ascending=ascending)
    elif isinstance(data, Dict): return dict(sorted(data.items(), key=lambda x: x[0], reverse=(not asc)))
    else: return sorted(data, reverse=(not asc))


###################################################################
############################## String #############################
###################################################################

def concat(*args: str, sep=',', drop_empty=False) -> str:
    return sep.join([str(__s) for __s in args if (exists(__s) if drop_empty else notna(__s))])


def regex_get(pattern: RegexFormat, string: str, indices: Index=None, groups: Index=None, default=str(),
            if_null: Literal["drop","pass"]="pass") -> Union[str,List[str]]:
    __pattern = pattern if isinstance(pattern, re.Pattern) else re.compile(pattern, re.DOTALL)
    if indices is not None:
        return iloc(__pattern.findall(string), indices, default=default, if_null=if_null)
    elif groups is not None:
        __match = __pattern.search(string)
        __group = __match.groups() if __match else list()
        return iloc(__group, groups, default=default, if_null=if_null)
    else: return default


def replace_map(__string: str, **context) -> str:
    for __old, __new in context.items():
        __string = __string.replace(__old, __new)
    return __string


def search_keyword(__string: str, __keyword: Keyword) -> bool:
    if not __keyword: return True
    keyword = cast_list(__keyword)
    if len(keyword) == 1: return keyword[0] in __string
    else: return bool(re.search('|'.join(map(re.escape, keyword)), __string))


def match_keyword(__string: str, __keyword: Keyword) -> bool:
    if not __keyword: return True
    keyword = cast_list(__keyword)
    if len(keyword) == 1: return keyword[0] == __string
    else: return bool(re.match('^'+'|'.join(map(re.escape, keyword))+'$', __string))


def startswith(__string: str, __keyword: Keyword) -> bool:
    for __s in cast_list(__keyword):
        if __string.startswith(__s): return True
    else: return False


def endswith(__string: str, __keyword: Keyword) -> bool:
    for __s in cast_list(__keyword):
        if __string.endswith(__s): return True
    else: return False


def encrypt(__object, count=1) -> str:
    return encrypt(_b64encode(__object).decode("utf-8"), count-1) if count else __object


def decrypt(__object, count=1) -> str:
    return decrypt(_b64decode(__object).decode("utf-8"), count-1) if count else __object


def _b64encode(__object) -> bytes:
    return base64.b64encode(__object if isinstance(__object, bytes) else str(__object).encode("utf-8"))


def _b64decode(__object) -> bytes:
    return base64.b64decode(__object if isinstance(__object, bytes) else str(__object))


###################################################################
############################## Array ##############################
###################################################################

def abs_idx(idx: int) -> int:
    return abs(idx+1) if idx < 0 else idx


def get_index(__s: IndexedSequence, values: _VT, default=None, if_null: Literal["drop","pass"]="drop") -> Index:
    if not is_array(values):
        return __s.index(values) if values in __s else default
    elif not values:
        return __s if if_null != "drop" else list()
    elif if_null == "drop":
        return [__s.index(__value) for __value in values if __value in __s]
    else: return [(__s.index(__value) if __value in __s else default) for __value in values]


def get_scala(__object, index: Optional[int]=0, default=None, random=False) -> _VT:
    if not is_array(__object): return __object
    elif isinstance(__object, Set): return __object.copy().pop()
    elif random: return rand.choice(__object) if __object else default
    else: return iloc(__object, index, default=default)


def map_index(__indices: Index) -> Index:
    if isinstance(__indices, int): return __indices
    elif is_bool_array(__indices, how="all", empty=False):
        return [__i for __i, __bool in enumerate(__indices) if __bool]
    elif is_int_array(__indices, how="all", empty=False): return __indices
    else: return


def flatten(*args, iter_type: _TYPE=(List,Set,Tuple)) -> List:
    return [__e for __object in args for __e in (
            __object if isinstance(__object, iter_type) else cast_tuple(__object))]


def filter_array(__s: Sequence, match: MatchFunction, apply: Optional[ApplyFunction]=None) -> List:
    __apply = apply if apply else (lambda x: x)
    return [__apply(__e) for __e in __s if match(__s)]


def safe_len(__object, default=0) -> int:
    try: return len(__object)
    except: return default


def is_same_length(*args: IndexedSequence, empty=True) -> bool:
    __length = set(map(safe_len, args))
    return (len(__length) == 1) and (empty or (0 not in __length))


def transpose_array(__s: Sequence[Sequence], count: Optional[int]=None, unroll=False) -> List[List]:
    if not __s: return list()
    if unroll: __s = list(map(lambda arr: flatten(*arr), __s))
    count = count if isinstance(count, int) and count > 0 else len(__s[0])
    return [list(map(lambda x: x[__i], __s)) for __i in range(count)]


def unit_array(__s: Sequence, unit=1) -> List[Sequence]:
    return [__s[__i:__i+unit] for __i in range(0, len(__s), unit)]


def unit_records(__r: Records, unit=1, keys: _KT=list()) -> Records:
    keys = cast_list(keys) if keys else unique_keys(__r)
    rows = list()
    for __i in range(0, len(__r), unit):
        __m = {__key: list() for __key in keys}
        for row in __r[__i:__i+unit]:
            for __key in __m.keys():
                __m[__key].append(row.get(__key))
        rows.append(__m)
    return rows


###################################################################
############################### Map ###############################
###################################################################

def traversal_dict(left: Union[Dict,Any], right: Union[Dict,Any], apply: Optional[Callable]=None,
                    primary: Literal["left","right"]="left", dropna=False, **context) -> Union[Dict,Any]:
    if isinstance(left, Dict):
        if isinstance(right, Dict):
            return _traversal_next(*((left, right) if primary == "left" else (right, left)), apply, primary, dropna, **context)
        else: return {__k1: traversal_dict(__v1, right, apply, **context) for __k1, __v1, in left.items()}
    elif isinstance(apply, Callable): return apply(left, right, **context)
    else: return (left if primary == "left" else right)


def _traversal_next(__m1: Dict, __m2: Dict, apply: Optional[Callable]=None,
                    primary: Literal["left","right"]="left", dropna=False, **context) -> Dict:
    args = lambda __v1, __v2: (__v1, __v2) if primary == "left" else (__v2, __v1)
    context.update(apply=apply, primary=primary, dropna=dropna)
    if dropna: return {__k1: traversal_dict(*args(__v1, __m2[__k1]), **context) for __k1, __v1 in __m1.items() if __k1 in __m2}
    else: return {__k1: (traversal_dict(*args(__v1, __m2[__k1]), **context) if __k1 in __m2 else __v1) for __k1, __v1 in __m1.items()}


###################################################################
############################ DataFrame ############################
###################################################################

def init_df(df: Optional[PandasData]=None, columns: Optional[IndexLabel]=None) -> PandasData:
    if isinstance(df, pd.DataFrame):
        return pd.DataFrame(columns=(columns if is_columns(columns) else df.columns))
    elif isinstance(df, pd.Series):
        return pd.Series(index=df.index, dtype=df.dtypes, name=get_scala(columns))
    elif isinstance(columns, str): return pd.Series(name=columns)
    else: return pd.DataFrame(columns=cast_columns(columns))


def is_columns(columns: IndexLabel) -> bool:
    return is_array(columns) or isinstance(columns, PandasIndex)


def cast_columns(__object, unique=False) -> IndexLabel:
    if isinstance(__object, PandasIndex):
        return (__object.unique() if unique else __object).tolist()
    else: return __object if isinstance(__object, str) else cast_list(__object)


def get_columns(df: PandasData) -> PandasIndex:
    if isinstance(df, pd.DataFrame): return df.columns
    elif isinstance(df, pd.Series): return df.index
    else: return list()


def convert_dtypes(df: PandasData) -> PandasData:
    if isinstance(df, pd.DataFrame):
        df = df.copy()
        for column in df.columns:
            df[column] = convert_dtypes(df[column])
        return df
    elif isinstance(df, pd.Series):
        if "float" in df.dtype.name:
            return df.convert_dtypes()
        else: return df.where(pd.notna(df), None)
    else: return pd.Series(dtype="object")


def merge_first(left: pd.DataFrame, right: pd.DataFrame, first: Literal["left","right"]="left",
                how: Literal["left","right","outer","inner","cross"]="inner",
                on: Optional[IndexLabel]=None) -> pd.DataFrame:
    columns = [str(column) for column in unique(*cast_list(on), *left.columns, *right.columns)]
    df = left.merge(right, how=how, on=on)
    if len(df.columns) == len(columns):
        return df
    for column in columns:
        FIRST, SECOND = ("_y", "_x") if first == "right" else ("_x", "_y")
        if ((column+FIRST) in df) and ((column+SECOND) in df):
            __first, __second = df.pop(column+FIRST), df.pop(column+SECOND)
            if __second.isna().all(): df[column] = __first
            elif __first.isna().all(): df[column] = __second
            else: df[column] = __first.combine_first(__second)
        else: df[column] = df.pop(column)
    return df


def merge_subset(left: pd.DataFrame, right: pd.DataFrame, how: Literal["left","right"]="left",
                on: Optional[IndexLabel]=None, subset: Optional[IndexLabel]=None) -> pd.DataFrame:
    if how == "right": left, right = right, left
    on, subset = cast_list(on), cast_list(subset)
    df = left.merge(right.drop_duplicates(on+subset, keep="first"), how=how, on=on+subset)
    for column in subset:
        subright = right.drop(columns=diff(subset,[column])).drop_duplicates(on+[column], keep="first")
        merged = merge_first(left, subright, how="left", on=on+[column])[df.columns]
        df = df.combine_first(merged)
    return df


def merge_unique(left: pd.DataFrame, right: pd.DataFrame, drop: Literal["left","right"]="right",
                how: Literal["left","right","outer","inner","cross"]="inner", on: Optional[IndexLabel]=None) -> pd.DataFrame:
    if set(left.columns) == set(right.columns):
        return left if drop == "right" else right
    elif drop == "right":
        right = right[diff(right.columns, diff(left.columns, cast_list(on)))]
    else: left = left[diff(left.columns, diff(right.columns, cast_list(on)))]
    return left.merge(right, how=how, on=on)


def unroll_df(df: pd.DataFrame, columns: IndexLabel, values: _VT) -> pd.DataFrame:
    columns, values = cast_tuple(columns), cast_tuple(values)
    __get = lambda row: [row[__value] for __value in values]
    __len = lambda row: min(map(len, __get(row)))
    unroll_row = lambda row: [[row[__column]]*__len(row) for __column in columns]+__get(row)
    map_subrow = lambda subrow: {__key: __value for __key, __value in zip(columns+values,subrow)}
    map_row = lambda row: pd.DataFrame([map_subrow(subrow) for subrow in zip(*unroll_row(row))])
    return pd.concat([map_row(row) for _,row in df.iterrows()])


def round_df(df: pd.DataFrame, columns: IndexLabel, trunc=2) -> pd.DataFrame:
    if not isinstance(trunc, int): return df
    __round = lambda x: round(x,trunc) if isinstance(x,float) else x
    return apply_df(df, **{__column: __round for __column in cast_tuple(columns)})


def to_excel(df: pd.DataFrame, file_name: str, sheet_name="Sheet1", index=False, engine="openpyxl",
            if_error: Literal["error","ignore","to_csv"]="to_csv", **engine_kwargs):
    try:
        try: df.to_excel(file_name, sheet_name=sheet_name, index=index)
        except:
            with pd.ExcelWriter(file_name, engine=engine, engine_kwargs=engine_kwargs) as writer:
                write_df(df, writer, sheet_name=sheet_name, index=index)
    except Exception as exception:
        if os.path.exists(file_name):
            os.remove(file_name)
        if if_error == "to_csv":
            _escape_newline(df).to_csv(os.path.splitext(file_name)[0]+".csv", index=index)
        elif if_error == "error":
            raise exception
        else: return


def _escape_newline(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for __column in df.columns:
        if df[__column].dtype == "object":
            try: df[__column] = df[__column].str.replace('\n', '\\n')
            except: pass
    return df


def write_df(df: pd.DataFrame, writer: pd.ExcelWriter, sheet_name="Sheet1", index=False):
    try: df.to_excel(writer, sheet_name=sheet_name, index=index)
    except IllegalCharacterError:
        df = _remove_illegal_characters(df)
        df.to_excel(writer, sheet_name=sheet_name, index=index)


def _remove_illegal_characters(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for __column in df.columns:
        if df[__column].dtype == "object":
            df[__column] = df[__column].apply(lambda x: ILLEGAL_CHARACTERS_RE.sub('', x) if isinstance(x, str) else x)
    return df


###################################################################
############################## Table ##############################
###################################################################

def read_table(io: Union[bytes,str], file_type: Literal["auto","xlsx","csv","html","xml"]="auto",
            sheet_name: Optional[Union[str,int,List]]=0, header=0, dtype=None, engine=None,
            parse_dates: Optional[IndexLabel]=None, columns: Optional[IndexLabel]=list(), default=None,
            if_null: Literal["drop","pass"]="pass", reorder=True, return_type: Optional[TypeHint]="dataframe",
            rename: RenameMap=dict(), regex_path=False, reverse_path=False, **options) -> Union[Data,Dict[str,Data]]:
    io, file_type = _validate_table(io, file_type, regex_path, reverse_path)
    table = _read_table(io, file_type, sheet_name, header, dtype, engine, parse_dates, **options)
    args = (columns, default, if_null, reorder, return_type, rename)
    if isinstance(table, Dict): return {__sheet: _to_data(__data, *args) for __sheet, __data in table.items()}
    else: return _to_data(table, *args)


def read_bytes(__object: Union[str,MappingData], regex_path=False, reverse_path=False) -> bytes:
    if isinstance(__object, str):
        file_path = get_file_path(__object, regex_path, reverse_path)
        if os.path.exists(file_path):
            with open(file_path, "rb") as file:
                return file.read()
        else: raise ValueError(TABLE_PATH_ERROR_MSG)
    else: return to_bytes(__object)


def get_file_path(file_path: str, regex_path=False, reverse_path=False) -> str:
    if not regex_path: return file_path
    dir_name, file_name = os.path.split(file_path)
    for file in sorted(os.listdir(dir_name if dir_name else None), reverse=reverse_path):
        if re.search(file_name, file): return os.path.join(dir_name, file)
    raise ValueError(FILE_PATTERN_MISMATCH_MSG)


def _validate_table(io: Union[bytes,str], file_type: Literal["auto","xlsx","csv","html","xml"]="auto",
                    regex_path=False, reverse_path=False) -> Tuple[BytesIO,str]:
    if isinstance(io, str):
        if (file_type == "auto") and (os.path.splitext(io)[1] in (".xlsx",".html")):
            return BytesIO(read_bytes(io, regex_path, reverse_path)), os.path.splitext(io)[1][1:]
        elif file_type in ("csv","html","xml"):
            if io.endswith(f".{file_type}"):
                return BytesIO(read_bytes(io, regex_path, reverse_path)), os.path.splitext(io)[1][1:]
            else: return StringIO(io), file_type
        else: raise ValueError(TABLE_TYPE_ERROR_MSG)
    elif isinstance(io, bytes): return BytesIO(io), file_type
    else: raise ValueError(TABLE_TYPE_ERROR_MSG)


def _read_table(io: BytesIO, file_type: Literal["auto","xlsx","csv","html","xml"]="auto", sheet_name: Optional[Union[str,int,List]]=0,
                header=0, dtype=None, engine=None, parse_dates: Optional[IndexLabel]=None, **options) -> pd.DataFrame:
    if file_type == "xlsx":
        if engine == "xlrd": return _read_xlrd(io, **notna_dict(sheet_name=sheet_name, header=header, dtype=dtype, parse_dates=parse_dates), **options)
        else: return pd.read_excel(io, **notna_dict(sheet_name=sheet_name, header=header, dtype=dtype, engine=engine, parse_dates=parse_dates), **options)
    elif file_type == "csv": return pd.read_csv(io, **notna_dict(header=header, dtype=dtype, engine=engine, parse_dates=parse_dates), **options)
    elif file_type == "html":
        table_idx = sheet_name if isinstance(sheet_name, int) else 0
        return pd.read_html(io, **notna_dict(header=header, converters=dtype, parse_dates=parse_dates), **options)[table_idx]
    elif file_type == "xml": return pd.read_xml(io, **notna_dict(converters=dtype, parse_dates=parse_dates), **options)
    else: return _read_table_auto(io, sheet_name=sheet_name, header=header, dtype=dtype, engine=engine, parse_dates=parse_dates, **options)


def _read_table_auto(io: BytesIO, **options) -> pd.DataFrame:
    try: return _read_table(io, "html", **options)
    except: return _read_table(io, "xlsx", **options)


def _read_xlrd(io: BytesIO, **options) -> pd.DataFrame:
    from xlrd import open_workbook
    with open_workbook(file_contents=io.getvalue(), logfile=open(os.devnull, "w")) as wb:
        return pd.read_excel(wb, engine="xlrd", **options)


def _to_data(df: pd.DataFrame, columns: Optional[IndexLabel]=list(),
            default=None, if_null: Literal["drop","pass"]="pass", reorder=True,
            return_type: Optional[TypeHint]="dataframe", rename: RenameMap=dict()) -> Data:
    data = convert_data(df, return_type)
    data = rename_data(data, rename)
    return filter_data(data, columns, default=default, if_null=if_null, reorder=reorder)


def read_table_bulk(dir: str, file_pattern=None, file_type: Literal["auto","xlsx","csv","html","xml"]="auto",
                    sheet_name: Optional[Union[str,int,List]]=0, header=0, dtype: Optional[TypeMap]=None, engine=None,
                    parse_dates: Optional[IndexLabel]=None, columns: Optional[IndexLabel]=list(), default=None,
                    if_null: Literal["drop","pass"]="pass", reorder=True, return_type: Optional[TypeHint]="dataframe",
                    rename: RenameMap=dict(), progress=True, start=None, size=None) -> pd.DataFrame:
    kwargs = drop_dict(locals(), ["dir","file_pattern","progress","start","size"])
    table = pd.DataFrame()
    from tqdm.auto import tqdm
    for file in tqdm(sorted(os.listdir(dir))[start:size], disable=(not progress)):
        if (not file_pattern) or (re.search(file_pattern, file)):
            table = pd.concat([table, read_table(os.path.join(dir, file), **kwargs)])
    return table.reset_index(drop=True)
