from gscraper.base.types import _KT, _VT, _PASS, _BOOL, _TYPE, Comparable, Context, TypeHint
from gscraper.base.types import Index, IndexLabel, Keyword, Unit, IndexedSequence
from gscraper.base.types import Records, MappingData, TabularData, Data, HtmlData, RenameMap
from gscraper.base.types import ApplyFunction, MatchFunction, BetweenRange, RegexFormat
from gscraper.base.types import is_bool_array, is_int_array, is_array, is_2darray, is_records, is_dfarray
from gscraper.base.types import is_comparable, is_list_type, is_dict_type, is_records_type, is_dataframe_type
from gscraper.base.types import is_kwargs_allowed

from gscraper.utils import isna, notna, empty, exists
from gscraper.utils.cast import cast_str, cast_datetime, cast_date, cast_list, cast_tuple, cast_set

from typing import Any, Callable, Dict, List, Set
from typing import Iterable, Literal, Optional, Sequence, Tuple, Union
from bs4 import BeautifulSoup, Tag
import datetime as dt
import pandas as pd

from collections import defaultdict
from itertools import chain
import functools
import random as rand
import re

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')


BETWEEN_RANGE_TYPE_MSG = "Between condition must be an iterable or a dictionary."
INVALID_ISIN_MSG = "Isin function requires at least one parameter: exact, include, and exclude."
SOURCE_SEQUENCE_TYPE_MSG = "Required array of HTML source as input."


arg_or = lambda *args: functools.reduce(lambda x,y: x|y, args)
arg_and = lambda *args: functools.reduce(lambda x,y: x&y, args)
union = lambda *arrays: functools.reduce(lambda x,y: x+y, arrays)
inter = lambda *arrays: functools.reduce(lambda x,y: [e for e in x if e in y], arrays)
diff = lambda *arrays: functools.reduce(lambda x,y: [e for e in x if e not in y], arrays)


###################################################################
############################# Convert #############################
###################################################################

def to_array(__object, default=None, dropna=False, strict=False, unique=False) -> List:
    __s = cast_list(__object)
    if unique: return _unique(*__s, strict=strict)
    elif dropna: return [__e for __e in __s if exists(__e, strict=strict)]
    elif notna(default):
        return [__e if exists(__e, strict=strict) else default for __e in __s]
    else: return __s


def to_dict(__object: MappingData) -> Dict:
    if isinstance(__object, Dict): return __object
    elif is_records(__object, empty=True): return chain_dict(__object, keep="first")
    elif isinstance(__object, pd.DataFrame): return __object.to_dict("dict")
    else: return dict()


def to_records(__object: MappingData) -> Records:
    if is_records(__object, empty=True): return __object
    elif isinstance(__object, pd.DataFrame): return __object.to_dict("records")
    elif isinstance(__object, dict): return [__object]
    else: return list()


def to_dataframe(__object: MappingData) -> pd.DataFrame:
    if isinstance(__object, pd.DataFrame): return __object
    elif is_records(__object, empty=True): return pd.DataFrame(align_records(__object)).convert_dtypes()
    elif isinstance(__object, dict): return pd.DataFrame([__object])
    else: return pd.DataFrame()


def convert_data(data: Data, return_type: Optional[TypeHint]=None) -> Data:
    if not return_type: return data
    elif is_records_type(return_type): return to_records(data)
    elif is_dataframe_type(return_type): return to_dataframe(data)
    elif is_dict_type(return_type): return to_dict(data)
    elif is_list_type(return_type): return cast_list(data)
    else: return data


def multitype_allowed(func):
    @functools.wraps(func)
    def wrapper(data: Data, *args, return_type: Optional[TypeHint]=None, convert_first=False, **kwargs):
        if not return_type: return func(data, *args, **kwargs)
        if convert_first: data, return_type = convert_data(data, return_type), None
        data = func(data, *args, **kwargs)
        if not convert_first: data = convert_data(data, return_type)
        return data
    return wrapper


###################################################################
############################## Rename #############################
###################################################################

def rename_dict(__m: Dict, rename: RenameMap) -> Dict:
    return {rename.get(__key,__key): __value for __key, __value in __m.items()}


def rename_records(__r: Records, rename: RenameMap) -> Records:
    if not rename: return __r
    return [rename_dict(__m, rename=rename) for __m in __r]


@multitype_allowed
def rename_data(data: MappingData, rename: RenameMap,
                return_type: Optional[TypeHint]=None, convert_first=False) -> MappingData:
    if not rename: return data
    elif is_records(data): return rename_records(data, rename)
    elif isinstance(data, pd.DataFrame): return data.rename(columns=rename)
    elif isinstance(data, Dict): return rename_dict(data, rename)
    elif is_array(data): return [rename.get(__value,__value) for __value in data]
    else: return rename.get(data,data)


def multitype_rename(func):
    @functools.wraps(func)
    def wrapper(data: Data, *args, rename: RenameMap=dict(), rename_first=False, **kwargs):
        if not rename: return func(data, *args, **kwargs)
        if rename_first: data = rename_data(data, rename)
        data = func(data, *args, **kwargs)
        if not rename_first: data = rename_data(data, rename)
        return data
    return wrapper


###################################################################
############################### Get ###############################
###################################################################

def iloc(__s: IndexedSequence, indices: Index, default=None, if_null: Literal["drop","pass"]="drop") -> _VT:
    __indices, __length = map_index(indices), len(__s)
    if isinstance(__indices, int):
        return __s[__indices] if abs_idx(__indices) < __length else default
    elif not __indices:
        if if_null != "drop": return __s
        else: return list() if is_array(__indices) else default
    elif if_null == "drop":
        return [__s[__i] for __i in __indices if abs_idx(__i) < __length]
    else: return [(__s[__i] if abs_idx(__i) < __length else default) for __i in __indices]


def kloc(__m: Dict, keys: _KT, default=None, if_null: Literal["drop","pass"]="drop",
        reorder=True, values_only=False, hier=False, alias: Sequence[_KT]=list(), sep=' > ') -> Union[_VT,Dict]:
    if hier:
        return hloc(__m, keys, default, if_null=if_null, values_only=values_only, alias=alias, sep=sep)
    elif not is_array(keys):
        return __m.get(keys, default)
    elif keys:
        keys = keys if reorder else unique(*__m.keys(), *keys)
        if if_null == "drop": __m = {__key: __m[__key] for __key in keys if __key in __m}
        else: __m = {__key: __m.get(__key, default) for __key in keys}
    else: __m = __m if if_null != "drop" else dict()
    return list(__m.values()) if values_only else __m


def hloc(__m: Dict, keys: Sequence[_KT], default=None, if_null: Literal["drop","pass"]="drop",
        values_only=False, alias: Sequence[_KT]=list(), sep=' > ') -> Union[_VT,Dict]:
    if is_single_path(keys, hier=True):
        return hier_get(__m, keys, default)
    elif allin(map(exists, keys)):
        if len(alias) != len(keys):
            alias = [sep.join(map(str, cast_list(__path))) for __path in keys]
        __m = {__key: hier_get(__m, __path, default) for __key, __path in zip(alias, keys)}
        if if_null == "drop": __m = notna_dict(__m)
    else: return __m if if_null != "drop" else dict()
    return list(__m.values()) if values_only else __m


def vloc(__r: List[Dict], keys: _KT, default=None, if_null: Literal["drop","pass"]="drop", reorder=True,
        values_only=False, hier=False, alias: Sequence[_KT]=list(), sep=' > ') -> Union[Records,List]:
    context = dict(if_null=if_null, reorder=reorder, values_only=values_only, hier=hier, alias=alias, sep=sep)
    __r = [kloc(__m, keys, default=default, **context) for __m in __r]
    if if_null == "drop": return [__m for __m in __r if __m]
    else: return [__m if __m else dict() for __m in __r]


def cloc(df: pd.DataFrame, columns: IndexLabel, default=None, if_null: Literal["drop","pass"]="drop",
        reorder=True, values_only=False) -> Union[pd.DataFrame,pd.Series,_VT]:
    __inter = inter(cast_tuple(columns), df.columns) if reorder else inter(df.columns, cast_tuple(columns))
    if not __inter:
        return pd.DataFrame() if if_null == "drop" else df
    elif not is_array(columns): df = df[columns]
    elif (if_null == "pass") and (len(columns) != len(__inter)):
        if reorder: df = pd.concat([pd.DataFrame(columns=columns),df])
        else: df = pd.concat([pd.DataFrame(columns=unique(*__inter,*columns)),df])
    else: df = df[__inter]
    if notna(default): df = df.where(pd.notna(df), default)
    if isinstance(df, pd.Series): return df.tolist() if values_only else df
    else: return list(df.to_dict("list").values()) if values_only else df


def sloc(source: Tag, selectors: Sequence[_KT], default=None, if_null: Literal["drop","pass"]="drop",
        values_only=False, hier=False, alias: Sequence[_KT]=list(), sep=' > ', index: Optional[Unit]=None,
        by: Literal["source","text"]="source", lines: Literal["ignore","split","raw"]="ignore",
        strip=True, replace: RenameMap=dict()) -> Union[_VT,Dict]:
    if not (allin(map(exists, selectors)) if hier else selectors):
        return source if if_null != "drop" else default
    loc = hier_select if hier else select_by
    context = dict(default=default, sep=sep, index=index, by=by, lines=lines, strip=strip, replace=replace)
    if is_single_selector(selectors, hier=hier, index=index):
        return loc(source, selectors, **context)
    elif len(alias) != len(selectors):
        alias = [_get_selector_name(selector, sep=sep, hier=hier) for selector in selectors]
    __m = {key: loc(source, selector, **context) for key, selector in zip(alias, selectors)}
    if if_null == "drop": __m = exists_dict(__m)
    return list(__m.values()) if values_only else __m


@multitype_allowed
@multitype_rename
def filter_data(data: Data, fields: Optional[Union[_KT,Index]]=list(), default=None,
                if_null: Literal["drop","pass"]="drop", reorder=True, values_only=False, hier=False,
                return_type: Optional[TypeHint]=None, rename: RenameMap=dict(),
                convert_first=False, rename_first=False) -> Data:
    if not fields: return data if if_null != "drop" else list()
    if is_records(data): return vloc(data, keys=fields, default=default, if_null=if_null, reorder=reorder, values_only=values_only, hier=hier)
    elif isinstance(data, pd.DataFrame): return cloc(data, columns=fields, default=default, if_null=if_null, reorder=reorder, values_only=values_only)
    elif isinstance(data, Dict): return kloc(data, keys=fields, default=default, if_null=if_null, reorder=reorder, values_only=values_only, hier=hier)
    elif is_array(data): return iloc(data, indices=fields, default=default, if_null=if_null)
    elif isinstance(data, Tag): return sloc(data, indices=fields, default=default, if_null=if_null, values_only=values_only, hier=hier)
    else: return list()


def multitype_filter(func):
    @functools.wraps(func)
    def wrapper(data: Data, *args, fields: Optional[Union[_KT,Index]]=list(), default=None,
                if_null: Literal["drop","pass"]="pass", reorder=True, values_only=False, hier=False,
                filter_first=False, **kwargs):
        if not fields: return func(data, *args, **kwargs) if if_null != "drop" else list()
        if filter_first: data = filter_data(data, fields, default, if_null, reorder)
        data = func(data, *args, **kwargs)
        if not filter_first: data = filter_data(data, fields, default, if_null, reorder, values_only, hier)
        return data
    return wrapper


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


def set_df(df: pd.DataFrame, __columns: Optional[IndexLabel]=list(), __values: Optional[_VT]=list(),
            if_exists: Literal["replace","ignore"]="replace", **context) -> pd.DataFrame:
    df = df.copy()
    for __column, __value in dict(zip(cast_tuple(__columns), cast_tuple(__values)), **context).items():
        if (if_exists == "ignore") and (__column in df): continue
        else: df[__column] = __value
    return df


@multitype_allowed
def set_data(data: MappingData, __keys: Optional[_KT]=list(), __values: Optional[_VT]=list(),
            if_exists: Literal["replace","ignore"]="replace",
            return_type: Optional[TypeHint]=None, convert_first=False, **context) -> Data:
    if is_records(data): return set_records(data, __keys, __values, if_exists=if_exists, **context)
    elif isinstance(data, pd.DataFrame): return set_df(data, __keys, __values, if_exists=if_exists, **context)
    elif isinstance(data, Dict): return set_dict(data, __keys, __values, if_exists=if_exists, inplace=False, **context)
    else: return data


###################################################################
########################### Hierachy Get ##########################
###################################################################

def is_single_path(path: _KT, hier=False) -> bool:
    if not path: return True
    elif hier: return not is_2darray(path, how="any")
    else: return (not is_array(path)) or (len(path) == 1)


def hier_get(__m: Dict, __path: _KT, default=None, apply: Optional[ApplyFunction]=None,
            __type: Optional[_TYPE]=None, empty=True, strict=True, **context) -> _VT:
    __m = __m.copy()
    try:
        for __key in cast_tuple(__path):
            __m = __m[__key]
    except: return default
    value = safe_apply(__m, apply, default, **context) if apply else __m
    if __type and not isinstance(value, __type): return default
    return value if empty or notna(value, strict=strict) else default


def hier_set(__m: Dict, __path: _KT, value: _VT, empty=True, strict=True, inplace=True) -> Dict:
    if not inplace: __m = __m.copy()
    if empty or notna(value, strict=strict):
        try:
            for __key in cast_tuple(__path)[:-1]:
                __m = __m[__key]
        except: return __m if not inplace else None
        __m[__path[-1]] = value
    if not inplace: return __m


def hier_get_set(__m: Dict, __get_path: _KT, __set_path: _KT, default=None,
                apply: Optional[ApplyFunction]=None, __type: Optional[_TYPE]=None,
                empty=True, strict=True, inplace=True, **context) -> Dict:
    value = hier_get(__m, __get_path, default, apply, __type, empty, strict, **context)
    if empty or notna(value, strict=strict):
        return hier_set(__m, __set_path, value, inplace=inplace)
    elif not inplace: return __m


###################################################################
############################## Exists #############################
###################################################################

def notna_one(*args, strict=True) -> Any:
    for arg in args:
        if notna(arg, strict=strict): return arg
    if args: return args[-1]


def exists_one(*args, strict=False) -> Any:
    for arg in args:
        if exists(arg, strict=strict): return arg
    if args: return args[-1]


def notna_dict(__m: Optional[Dict]=dict(), strict=True, **context) -> Dict:
    return {__k: __v for __k, __v in dict(__m, **context).items() if notna(__v, strict=strict)}


def exists_dict(__m: Optional[Dict]=dict(), strict=False, **context) -> Dict:
    return {__k: __v for __k, __v in dict(__m, **context).items() if exists(__v, strict=strict)}


def df_empty(df: pd.DataFrame, drop_na=False, how: Literal["any","all"]="all") -> bool:
    return isinstance(df, pd.DataFrame) and (df.dropna(axis=0, how=how) if drop_na else df).empty


def df_exists(df: pd.DataFrame, drop_na=False, how: Literal["any","all"]="all") -> bool:
    return isinstance(df, pd.DataFrame) and not (df.dropna(axis=0, how=how) if drop_na else df).empty


def data_exists(data: Data, drop_na=False, how: Literal["any","all"]="all") -> bool:
    if isinstance(data, (Dict,List,pd.DataFrame)):
        return df_exists(data, drop_na, how) if isinstance(data, pd.DataFrame) else bool(data)
    else: return False


def data_empty(data: Data, drop_na=False, how: Literal["any","all"]="all") -> bool:
    if isinstance(data, (Dict,List,pd.DataFrame)):
        return df_empty(data, drop_na, how) if isinstance(data, pd.DataFrame) else (not data)
    else: return False


###################################################################
############################### Isin ##############################
###################################################################

def allin(__iterable: Sequence[bool]) -> bool:
    return __iterable and all(__iterable)


def howin(__iterable: Sequence[bool], how: Literal["any","all"]="all") -> bool:
    return allin(__iterable) if how == "all" else any(__iterable)


def isin(__iterable: Iterable, exact: Optional[_VT]=None, include: Optional[_VT]=None,
        exclude: Optional[_VT]=None, how: Literal["any","all"]="any", **kwargs) -> bool:
    match, __all = (True, (how == "all"))
    if notna(exact): match &= _isin(__iterable, exact, how="exact", all=__all)
    if match and notna(include): match &= _isin(__iterable, include, how="include", all=__all)
    if match and notna(exclude): match &= _isin(__iterable, exclude, how="exclude", all=True)
    return match


def _isin(__iterable: Iterable, __values: Optional[_VT]=None,
            how: Literal["exact","include","exclude"]="exact", all=False, strict=False) -> bool:
    if is_array(__iterable):
        return howin([_isin(__i, __values, how, all, strict) for __i in __iterable], how=("all" if all else "any"))
    elif is_array(__values):
        return howin([_isin(__iterable, __v, how, all, strict) for __v in __values], how=("all" if all else "any"))
    elif isna(__values): return exists(__iterable, strict=strict)
    elif not isinstance(__iterable, Iterable): return False
    elif how == "exact": return __values == __iterable
    elif how == "include": return __values in __iterable
    elif how == "exclude": return __values not in __iterable
    else: raise ValueError(INVALID_ISIN_MSG)


def isin_dict(__m: Dict, keys: Optional[_KT]=None,
                exact: Optional[_VT]=None, include: Optional[_VT]=None, exclude: Optional[_VT]=None,
                how: Literal["any","all"]="any", if_null=False, hier=False, **kwargs) -> bool:
    if isna(exact) and isna(include) and isna(exclude): return True
    keys = keys if keys or hier else list(__m.keys())
    __values = kloc(__m, keys, if_null="drop", values_only=True, hier=hier)
    if isna(__values): return if_null
    elif is_single_path(keys, hier=hier): pass
    elif (how == "all") and (not if_null) and (len(keys) != len(__values)): return False
    return isin(__values, exact, include, exclude, how=how)


def isin_records(__r: Records, keys: Optional[_KT]=None,
                exact: Optional[_VT]=None, include: Optional[_VT]=None, exclude: Optional[_VT]=None,
                how: Literal["any","all"]="any", if_null=False, hier=False,
                filter=True, **kwargs) -> Union[Records,List[bool]]:
    if isna(exact) and isna(include) and isna(exclude): return __r
    __matches = [isin_dict(__m, keys, exact, include, exclude, how, if_null, hier) for __m in __r]
    return [__m for __match, __m in zip(__matches, __r) if __match] if filter else __matches


def keyin_records(__r: Records, keys: _KT) -> Union[bool,Dict[_KT,bool]]:
    all_keys = union(*[list(__m.keys()) for __m in __r])
    return {__key: (__key in all_keys) for __key in keys} if is_array(keys) else keys in all_keys


def isin_df(df: pd.DataFrame, columns: Optional[IndexLabel]=None,
            exact: Optional[_VT]=None, include: Optional[_VT]=None, exclude: Optional[_VT]=None,
            how: Literal["any","all"]="any", if_null=False,
            filter=True, **kwargs) -> Union[pd.DataFrame,pd.Series]:
    if isna(exact) and isna(include) and isna(exclude): return df
    __matches, _all = pd.Series([True] * len(df), index=df.index), (how == "all")
    for __column in (cast_tuple(columns) if columns else df.columns):
        if __column not in df:
            if if_null: continue
            elif filter: return pd.DataFrame(columns=df.columns)
            else: return  pd.Series([False] * len(df), index=df.index)
        __match = df[__column].notna()
        if __match.any() and notna(exact):
            __match = __match & _isin_series(df[__match][__column], exact, how="exact", all=_all)
        if __match.any() and notna(include):
            __match = __match & _isin_series(df[__match][__column], include, how="include", all=_all)
        if __match.any() and notna(exclude):
            __match = __match & _isin_series(df[__match][__column], exclude, how="exclude", all=_all)
        if if_null: __match = __match & df[__column].isna()
        __matches = __matches & __match if how == "all" else __matches | __match
        if not __matches.any(): break
    return df[__matches] if filter else __matches


def _isin_series(series: pd.Series, __values: Optional[_VT]=None,
                how: Literal["exact","include","exclude"]="exact", all=False, strict=False) -> pd.Series:
    if is_array(__values):
        __op = arg_and if all else arg_or
        return __op(*[_isin_series(series, __v, how, all, strict) for __v in __values])
    elif isna(__values): return series.apply(lambda x: exists(x, strict=strict))
    elif how == "exact": return series == __values
    elif how == "include":
        return series.astype(str).str.contains(__values)
    elif how == "exclude": return ~series.astype(str).str.contains(__values)
    else: raise ValueError(INVALID_ISIN_MSG)


def isin_source(source: Union[Tag,Sequence[Tag]], selectors: _KT, exact: Optional[_VT]=None,
                include: Optional[_VT]=None, exclude: Optional[_VT]=None, how: Literal["any","all"]="any",
                if_null=False, sep=' > ', index: Optional[Unit]=None, hier=False,
                filter=True, **kwargs) -> Union[bool,List[Tag],List[bool]]:
    if is_array(source):
        __matches = [isin_source(__s, selectors, exact, include, exclude, how, if_null, index, hier) for __s in source]
        return [__s for __match, __s in zip(__matches, source) if __match] if filter else __matches
    if isna(exact) and isna(include) and isna(exclude):
        name = exists_one(*map(_get_class_name, (selectors if hier else cast_tuple(selectors))))
        if not name: return True
        else: exact = name
    context = dict(sep=sep, index=index, by="text", lines="raw")
    __values = sloc(source, selectors, if_null="drop", values_only=True, hier=hier, **context)
    if not __values: return if_null
    elif is_single_selector(selectors, hier=hier, index=index): pass
    elif (how == "all") and (not if_null) and (len(selectors) != len(__values)): return False
    return isin(__values, exact, include, exclude, how=how)


def _get_class_name(selector: _KT) -> str:
    tag = str(get_scala(selector, -1))
    return tag[1:] if tag.startswith('.') or tag.startswith('#') else str()


###################################################################
############################## Chain ##############################
###################################################################

def unique(*elements, strict=True, unroll=False) -> List:
    array = list()
    for __e in (flatten(*elements) if unroll else elements):
        if isna(__e, strict=strict): continue
        if __e not in array: array.append(__e)
    return array


def _unique(*elements, strict=True, unroll=False) -> List:
    return unique(*elements, strict=strict, unroll=unroll)


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


def filter_exists(__object, strict=False) -> Any:
    if is_array(__object):
        return type(__object)([__value for __value in __object if exists(__value, strict=strict)])
    elif isinstance(__object, Dict):
        return {key: __value for key, __value in __object.items() if exists(__value, strict=strict)}
    elif isinstance(__object, pd.DataFrame):
        return __object.dropna(axis=1, how=("all" if strict else "any"))
    elif exists(__object, strict=strict): return __object


def concat_array(left: Sequence, right: Sequence, left_index: Sequence[int]) -> List:
    left, right = cast_list(left).copy(), cast_list(right).copy()
    return [left.pop(0) if is_left else right.pop(0) for is_left in left_index]


def chain_dict(__object: Sequence[Dict], keep: Literal["fist","last"]="first") -> Dict:
    base = dict()
    for __m in __object:
        if not isinstance(__m, Dict): continue
        for __key, __value in __m.items():
            if (keep == "first") and (__key in base): continue
            else: base[__key] = __value
    return base


def concat_df(__object: Sequence[pd.DataFrame], axis=0, keep: Literal["fist","last"]="first") -> pd.DataFrame:
    if axis == 1: __object = diff_df(*__object, keep=keep)
    else: __object = [df for df in __object if df_exists(df)]
    return pd.concat(__object, axis=axis) if __object else pd.DataFrame()


@multitype_allowed
@multitype_rename
@multitype_filter
def chain_exists(data: Data, data_type: Optional[TypeHint]=None, keep: Literal["fist","last"]="first",
                fields: Optional[Union[_KT,Index]]=list(), default=None, if_null: Literal["drop","pass"]="drop",
                reorder=True, values_only=False, hier=False, return_type: Optional[TypeHint]=None,
                rename: RenameMap=dict(), convert_first=False, rename_first=False, filter_first=False) -> Data:
    if is_dfarray(data): return concat_df([df for df in data if df_exists(df)])
    elif is_2darray(data): return list(chain.from_iterable([__s for __s in data if is_array(__s)]))
    elif data_type and is_dict_type(data_type) and is_records(data, how="any"):
        return chain_dict([__m for __m in data if isinstance(__m, Dict)], keep=keep)
    else: return filter_exists(data)


###################################################################
############################# Groupby #############################
###################################################################

def groupby_records(__r: Records, by: _KT, if_null: Literal["drop","pass"]="drop", hier=False) -> Dict[_KT,Records]:
    by = cast_tuple(by)
    groups = defaultdict(list)
    for __m in __r:
        values = kloc(__m, by, default=str(), if_null=if_null, values_only=True, hier=hier)
        if len(values) != len(by): continue
        else: groups[tuple(values)].append(__m)
    return dict(groups)


def groupby_df(df: pd.DataFrame, by: _KT, if_null: Literal["drop","pass"]="drop") -> Dict[_KT,pd.DataFrame]:
    counts = cloc(df, cast_list(by), if_null=if_null).value_counts().reset_index(name="count")
    keys = counts.drop(columns="count").to_dict("split")["data"]
    return {tuple(key): match_df(df, **dict(zip(by, map(lambda x: (lambda y: x == y), key)))) for key in keys}


def groupby_source(source: Sequence[Tag], by: Union[Dict[_KT,_KT],_KT],
                    if_null: Literal["drop","pass"]="drop", hier=False, sep=' > ') -> Dict[_KT,Tag]:
    if not is_array(source): raise TypeError(SOURCE_SEQUENCE_TYPE_MSG)
    if not isinstance(by, Dict):
        by = {0: by} if is_single_selector(by, hier=hier) else dict(enumerate(cast_tuple(by)))
    groups = dict()
    for __key, selector in by.items():
        name = _get_class_name(selector)
        loc = hier_select if hier else select_by
        __match = lambda __value: (name in cast_tuple(__value)) if name else bool(__value)
        __matches = [__match(loc(__s, selector, sep=sep)) for __s in source]
        groups[__key] = [__s for __i, __s in enumerate(source) if __matches[__i]]
        source = [__s for __i, __s in enumerate(source) if not __matches[__i]]
    if (if_null == "pass") and source: groups[None] = source
    return groups


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
    __s = __s.copy()
    if all_indices: return [apply(__e) for __e in __s]
    for __i, __apply in map_context(__indices, __applyFunc, __default=apply, **context).items():
        if abs_idx(__i) < len(__s):
            __s[__i] = __apply(__s[__i])
    return __s


def apply_dict(__m: Dict, __keys: Optional[_KT]=list(), __applyFunc: Optional[ApplyFunction]=list(),
                apply: Optional[ApplyFunction]=None, all_keys=False, __default: _PASS=None, **context) -> Dict:
    __m = __m.copy()
    if all_keys: return {__key: apply(__values) for __key, __values in __m.items()}
    for __key, __apply in map_context(__keys, __applyFunc, __default=apply, **context).items():
        if __key in __m:
            __m[__key] = __apply(__m[__key])
    return __m


def apply_records(__r: List[Dict], __keys: Optional[_KT]=list(), __applyFunc: Optional[ApplyFunction]=list(),
                    apply: Optional[ApplyFunction]=None, all_keys=False, scope: Literal["keys","dict"]="keys",
                    __default=None, **context) -> Records:
    if scope == "keys":
        return [apply_dict(__m, __keys, __applyFunc, all_keys=all_keys, apply=apply, **context) for __m in __r]
    elif scope == "dict":
        return [apply(__m) for __m in __r]
    else: return list()


def apply_df(df: pd.DataFrame, __columns: Optional[IndexLabel]=list(), __applyFunc: Optional[ApplyFunction]=list(),
            apply: Optional[ApplyFunction]=None, all_cols=False, __default: _PASS=None, **context) -> pd.DataFrame:
    df = df.copy()
    if all_cols: return df.apply({__column: apply for __column in df.columns})
    context = map_context(__columns, __applyFunc, __default=apply, **context)
    context = {str(__column): __apply for __column, __apply in context.items() if __column in df}
    return df.apply(context)


def safe_apply_df(__object: Union[pd.DataFrame,pd.Series], __applyFunc: ApplyFunction,
                    default: Union[pd.DataFrame,pd.Series,Sequence,Any]=None, by: Literal["row","cell"]="row",
                    **context) -> Union[pd.DataFrame,pd.Series]:
    __object = __object.copy()
    if isinstance(__object, pd.Series):
        __object = __object.apply(lambda x: safe_apply(x, __applyFunc, **context))
    elif isinstance(__object, pd.DataFrame):
        if by == "row":
            __object = __object.apply(lambda x: safe_apply(x, __applyFunc, **context), axis=1)
        elif by == "cell":
            for __column in __object.columns:
                __object[__column] = safe_apply_df(__object[__column].copy(), __applyFunc, **context)
    return fillna_each(__object, default)


@multitype_allowed
@multitype_rename
@multitype_filter
def apply_data(data: Data, __keys: Optional[Union[_KT,Index]]=list(), __applyFunc: Optional[ApplyFunction]=list(),
                apply: Optional[ApplyFunction]=None, all_keys=False, fields: Optional[Union[_KT,Index]]=list(),
                default=None, if_null: Literal["drop","pass"]="drop", reorder=True, values_only=False, hier=False,
                return_type: Optional[TypeHint]=None, rename: RenameMap=dict(),
                convert_first=False, rename_first=False, filter_first=False, **context) -> Data:
    if is_records(data): return apply_records(data, __keys, __applyFunc, apply=apply, all_keys=all_keys, **context)
    elif isinstance(data, pd.DataFrame): return apply_df(data, __keys, __applyFunc, apply=apply, all_cols=all_keys, **context)
    elif isinstance(data, Dict): return apply_dict(data, __keys, __applyFunc, apply=apply, all_keys=all_keys, **context)
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
    if all_indices:
        return match_array(__s, how=how, **dict(list(range(len(__s))), [match]*len(__s)))
    context = map_match(__indices, __matchFunc, __default=match, **context)
    matches = {__i: safe_apply(__s[__i], __match, False) for __i, __match in context.items() if abs_idx(__i) < len(__s)}
    if how == "filter":
        return [__s[__i] for __i, __match in matches.items() if __match]
    else: return all(matches.values()) if how == "all" else list(matches.values())


def match_dict(__m: Dict, __keys: Optional[_KT]=list(), __matchFunc: Optional[MatchFunction]=list(),
                match: Optional[MatchFunction]=None, all_keys=False, how: Literal["filter","all","indexer"]="filter",
                __default: _PASS=None, **context) -> Union[Dict,_BOOL]:
    if all_keys:
        return safe_apply(__m, match, False)
    context = map_match(__keys, __matchFunc, __default=match, **context)
    matches = {__key: safe_apply(__m[__key], __match, False) for __key, __match in context.items() if __key in __m}
    if how == "filter":
        return {__key: __m[__key] for __key, __match in matches.items() if __match}
    else: return all(matches.values()) if how == "all" else matches


def match_records(__r: List[Dict], __keys: Optional[_KT]=list(), __matchFunc: Optional[MatchFunction]=list(),
                match: Optional[MatchFunction]=None, all_keys=False, how: Literal["filter","all","indexer"]="filter",
                scope: Literal["keys","dict"]="keys", __default=None, **context) -> Union[Records,_BOOL]:
    if scope == "keys":
        matches = [match_dict(__m, __keys, __matchFunc, all_keys, match, how="all", **context) for __m in __r]
    elif scope == "dict":
        matches = [safe_apply(__m, match, default=False, **context) for __m in __r]
    else: matches = [False] * len(__r)
    return iloc(__r, matches, if_null="drop") if how == "filter" else (all(matches) if how == "all" else matches)


def match_df(df: pd.DataFrame, __columns: Optional[IndexLabel]=list(), __matchFunc: Optional[MatchFunction]=list(),
            match: Optional[MatchFunction]=None, all_cols=False, how: Literal["filter","all","indexer"]="filter",
            __default: _PASS=None, **context) -> Union[pd.DataFrame,pd.Series,bool]:
    context = map_match(__columns, __matchFunc, __default=match, **context)
    matches = apply_df(df, all_cols=all_cols, **context).all(axis=1)
    if how == "filter": return df[matches]
    else: return matches.all(axis=0) if how == "all" else matches


@multitype_allowed
@multitype_rename
@multitype_filter
def match_data(data: Data, __keys: Optional[Union[_KT,Index]]=list(), __matchFunc: Optional[MatchFunction]=list(),
                match: Optional[MatchFunction]=None, all_keys=False, how: Literal["filter","all","indexer"]="filter",
                fields: Optional[Union[_KT,Index]]=list(), default=None, if_null: Literal["drop","pass"]="drop",
                reorder=True, values_only=False, hier=False, return_type: Optional[TypeHint]=None, rename: RenameMap=dict(),
                convert_first=False, rename_first=False, filter_first=False, **context) -> Data:
    if is_records(data): return match_records(data, __keys, __matchFunc, match=match, all_keys=all_keys, how=how, **context)
    elif isinstance(data, pd.DataFrame): return match_df(data, __keys, __matchFunc, match=match, all_cols=all_keys, how=how, **context)
    elif isinstance(data, Dict): return match_dict(data, __keys, __matchFunc, match=match, all_keys=all_keys, how=how, **context)
    elif is_array(data): return match_array(data, __keys, __matchFunc, match=match, all_indices=all_keys, how=how, **context)
    else: return safe_apply(data, match, default=default, **context)


###################################################################
############################# Select ##############################
###################################################################

def clean_html(__object: str) -> str:
    return BeautifulSoup(cast_str(__object), "lxml").text


def clean_tag(__object: str) -> str:
    return re.sub("<[^>]*>", "", cast_str(__object))


def _get_selector(selector: _KT, sep=' > ', index: Optional[Unit]=None) -> Tuple[str,Unit]:
    if not (isinstance(selector, (List,str)) and selector): return str(), None
    elif not isinstance(selector, List): return selector, index
    selector = selector.copy()
    index = selector.pop() if isinstance(selector[-1], (int,List,Tuple)) else index
    return sep.join(selector), index


def select(source: Tag, selector: _KT, sep=' > ', index: Optional[Unit]=None) -> Union[Tag,List[Tag]]:
    selector, index = _get_selector(selector, sep=sep, index=index)
    if not selector: return source
    elif index == 0: return source.select_one(selector)
    elif not index: return source.select(selector)
    else: return iloc(source.select(selector), index)


def select_one(source: Tag, selector: _KT, sep=' > ') -> Tag:
    selector, _ = _get_selector(selector, sep=sep)
    if not selector: return source
    else: return source.select_one(selector)


def parse_text(string: Keyword, strip=False, replace: RenameMap=dict()) -> Union[str,List[str]]:
    if is_array(string): return [replace_map(__s, strip, **replace) for __s in string]
    else: return replace_map(string, strip, **replace)


def parse_lines(string: str, how: Literal["ignore","split","raw"]="ignore", sep='\n',
                strip=False, replace: RenameMap=dict()) -> Union[str,List[str]]:
    if how == "ignore": return parse_text(string.replace('\n', ' '), strip, **replace)
    elif how == "split": return parse_text(string.split(sep), strip, **replace)
    else: return parse_text(string, strip, **replace)


def select_text(source: Tag, selector: _KT, sep=' > ', index: Optional[Index]=None,
                lines: Literal["ignore","split","raw"]="ignore", strip=True,
                replace: RenameMap=dict()) -> Union[str,List[str]]:
    source = select(source, selector, sep=sep, index=index)
    try:
        if is_array(source):
            return [parse_lines(__s.text, how=lines, strip=strip, **replace)
                    for __s in source if isinstance(__s, Tag)]
        else: return parse_lines(source.text, how=lines, strip=strip, **replace)
    except (AttributeError, IndexError, TypeError):
        return list() if is_array(source) else str()


def select_attr(source: Tag, selector: str, key: str, sep=' > ',
                index: Optional[Index]=None) -> Union[str,List[str],List[List[str]]]:
    source = select(source, selector, sep=sep, index=index)
    try:
        if is_array(source):
            return [__s.attrs.get(key,str()) for __s in source if isinstance(__s, Tag)]
        else: return source.attrs.get(key,str())
    except (AttributeError, IndexError, TypeError):
        return list() if is_array(source) else str()


def select_datetime(source: Tag, selector: _KT, sep=' > ', index: Optional[Index]=None,
                    lines: Literal["ignore","split","raw"]="ignore", strip=True, default=None,
                    replace: RenameMap=dict()) -> Union[dt.datetime,List[dt.datetime]]:
    text = select_text(source, selector, sep=sep, index=index, lines=lines, strip=strip, replace=replace)
    if is_array(text): return [cast_datetime(text, default) for text in text]
    else: return cast_datetime(text, default)


def select_date(source: Tag, selector: _KT, sep=' > ', index: Optional[Index]=None,
                lines: Literal["ignore","split","raw"]="ignore", strip=True, default=None,
                replace: RenameMap=dict()) -> Union[dt.date,List[dt.date]]:
    text = select_text(source, selector, sep=sep, index=index, lines=lines, strip=strip, replace=replace)
    if is_array(text): return [cast_date(text, default) for text in text]
    else: return cast_date(text, default)


###################################################################
############################ Select By ############################
###################################################################

def is_single_selector(selector: _KT, hier=False, index: Optional[Index]=None) -> bool:
    if (not selector) or isinstance(index, int): return True
    elif not hier: return not is_2darray(selector, how="any")
    elif not is_array(selector): return True
    elif len(selector) == 1: return not is_array(selector[0])
    else: return all(
        [(not is_2darray(__s, how="any")) and isinstance(_get_selector(__s)[1], int) for __s in selector[:-1]])


def _get_selector_by(selector: _KT, key=str(), by: Literal["source","text"]="source") -> Tuple[_KT,str,str]:
    if isinstance(selector, str):
        if selector == "text()": return (str(), str(), "text")
        elif selector.startswith('@'): return (str(), selector[1:], by)
        elif selector.startswith('.'): return (str(), "class", by)
        elif selector.startswith('#'): return (str(), "id", by)
        else: return (selector, key, by)
    elif is_array(selector) and selector:
        tag, key, by = _get_selector_by(selector[-1], key, by)
        return ((selector+[tag] if tag else selector), key, by)
    else: return (str(), str(), by)


def _get_selector_name(selector: _KT, key=str(), by: Literal["source","text"]="source", sep=' > ', hier=False) -> str:
    path = (sep.join([_get_selector_name(__s, sep=sep) if is_array(__s) else str(__s)
                    for __s in selector[:-1]]) if hier else str())
    if hier: selector = selector[-1]
    selector, key, by = _get_selector_by(selector, key, by)
    selector, index = _get_selector(selector, sep=sep)
    if path: selector = sep.join(path, selector) if selector else path
    return selector + (f"[{index}]" if notna(index) else str()) + (f"[@{key}]" if key else str())


def select_by(source: Tag, selector: _KT, default=None, key=str(), sep=' > ', index: Optional[Unit]=None,
            by: Literal["source","text"]="source", lines: Literal["ignore","split","raw"]="ignore",
            strip=True, replace: RenameMap=dict()) -> HtmlData:
    try: return _select_by(**locals())
    except: return default


def _select_by(source: Tag, selector: _KT, default=None, key=str(), sep=' > ', index: Optional[Unit]=None,
            by: Literal["source","text"]="source", lines: Literal["ignore","split","raw"]="ignore",
            strip=True, replace: RenameMap=dict()) -> HtmlData:
    selector, key, by = _get_selector_by(selector, key, by)
    context = dict(sep=sep, index=index, lines=lines, strip=strip, replace=replace)
    if key: return select_attr(source, selector, key, sep=sep, index=index)
    elif by == "text": return select_text(source, selector, **context)
    else: return select(source, selector, sep=sep, index=index)


def hier_select(source: Tag, path: _KT, default=None, key=str(), sep=' > ', index: Optional[Unit]=None,
                by: Literal["source","text"]="source", lines: Literal["ignore","split","raw"]="ignore",
                strip=True, replace: RenameMap=dict(), apply: Optional[ApplyFunction]=None,
                __type: Optional[_TYPE]=None, empty=True, strict=True, **context) -> HtmlData:
    path = cast_tuple(path)
    try:
        for selector in path[:-1]:
            source = select_one(source, selector, sep=sep)
        selector = path[-1] if path else str()
        data = _select_by(source, selector, default, key, sep, index, by, lines, strip, replace)
    except: return default
    data = safe_apply(data, apply, default, **context) if apply else data
    if __type and not isinstance(data, __type): return default
    return data if empty or notna(data, strict=strict) else default


###################################################################
############################## Align ##############################
###################################################################

def get_exists_index(__s: Sequence, strict=False) -> List[int]:
    return [__i for __i, __e in enumerate(__s) if exists(__e, strict=strict)]


def get_unique_index(__s: Sequence) -> List[int]:
    indices, __mem = list(), set()
    for __i, __e in enumerate(__s):
        if __e not in __mem:
            __mem.add(__e)
            indices.append(__i)
    return indices


def align_index(*args: Sequence, how: Literal["min","max","first"]="min",
                dropna=False, strict=False, unique=False) -> List[int]:
    args = (args[0],) if how == "first" else args
    count = max(map(len, args)) if how == "max" else min(map(len, args))
    indices = set(range(0, count))
    if dropna: indices = arg_and(indices, *map(set, map(lambda __s: get_exists_index(__s, strict=strict), args)))
    if unique: indices = arg_and(indices, *map(set, map(get_unique_index, args)))
    return sorted(indices)


def align_array(*args: Sequence, how: Literal["min","max","first"]="min", default=None,
                dropna=False, strict=False, unique=False) -> Tuple[List]:
    args = [cast_list(__s) for __s in args]
    indices = align_index(*args, how=how, dropna=dropna, strict=strict, unique=unique)
    return tuple([__s[__i] if __i < len(__s) else default for __i in indices] for __s in args)


def align_dict(__m: Dict[_KT,Sequence], how: Literal["min","max"]="min", default=None,
                strict=False, dropna=False, unique=False) -> Dict[_KT,List]:
    if not __m: return dict()
    indices = align_index(*map(list, __m.values()), how=how, strict=strict, dropna=dropna, unique=False)
    if dropna or unique:
        return {__key: iloc(__value, indices, if_null="drop") for __key, __value in __m.items()}
    else: return {__key: iloc(__value, indices, default=default, if_null="pass") for __key, __value in __m.items()}


def align_records(__r: Records, default=None, forward=True, reorder=True) -> Records:
    keys = unique_keys(__r, forward=forward)
    return [kloc(__m, keys, default=default, if_null="pass", reorder=reorder)
            for __m in __r if isinstance(__m, Dict)]


###################################################################
############################# Between #############################
###################################################################

def between(__object: Comparable, left=None, right=None,
            inclusive: Literal["both","neither","left","right"]="both", **kwargs) -> bool:
    match_left = ((left is None) or (__object >= left if inclusive in ["both","left"] else __object > left))
    match_right = ((right is None) or (__object <= right if inclusive in ["both","right"] else __object < right))
    return match_left & match_right


def between_dict(__m: Dict, __keys: Optional[_KT]=list(), __ranges: Optional[BetweenRange]=list(),
                inclusive: Literal["both","neither","left","right"]="both", null=False,
                __default: _PASS=None, **context) -> bool:
    match = True
    for __key, __range in map_context(__keys, __ranges, **context).items():
        if __key in __m:
            if is_array(__range): match &= between(__m[__key], *__range[:2], inclusive=inclusive)
            elif isinstance(__range, dict): match &= between(__m[__key], **__range, inclusive=inclusive)
            else: raise ValueError(BETWEEN_RANGE_TYPE_MSG)
        elif not null: return False
    return match


def between_records(__r: Records, __keys: Optional[_KT]=list(), __ranges: Optional[BetweenRange]=list(),
                    inclusive: Literal["both","neither","left","right"]="both", null=False,
                    __default=None, **context) -> Records:
    return [__m for __m in __r
            if between_dict(__m, __keys, __ranges, inclusive=inclusive, null=null, **context)]


def between_df(df: pd.DataFrame, __columns: Optional[IndexLabel]=list(), __ranges: Optional[BetweenRange]=list(),
                inclusive: Literal["both","neither","left","right"]="both", null=False,
                __default: _PASS=None, **context) -> pd.DataFrame:
    df, df.copy()
    kwargs = {"inclusive": inclusive}
    for __column, __range in map_context(__columns, __ranges, **context).items():
        if __column in df and isinstance(__column, str):
            if_na = pd.Series([False]*len(df), index=df.index) if not null else df[__column].isna()
            if is_array(__range): df = df[df[__column].apply(lambda x: between(x, *__range[:2], **kwargs))|if_na]
            elif isinstance(__range, dict): df = df[df[__column].apply(lambda x: between(x, **__range, **kwargs))|if_na]
            else: raise ValueError(BETWEEN_RANGE_TYPE_MSG)
    return df


@multitype_allowed
@multitype_rename
@multitype_filter
def between_data(data: MappingData, inclusive: Literal["both","neither","left","right"]="both", null=False,
                fields: Optional[Union[_KT,Index]]=list(), default=None, if_null: Literal["drop","pass"]="drop",
                reorder=True, values_only=False, hier=False, return_type: Optional[TypeHint]=None, rename: RenameMap=dict(),
                convert_first=False, rename_first=False, filter_first=False, **context) -> MappingData:
    if is_records(data): return between_records(data, inclusive=inclusive, null=null, **context)
    elif isinstance(data, pd.DataFrame): return between_df(data, inclusive=inclusive, null=null, **context)
    elif isinstance(data, Dict): return data if between_dict(data, inclusive=inclusive, null=null, **context) else dict()
    else: return between(data, inclusive=inclusive, **context)


###################################################################
############################### Drop ##############################
###################################################################

def drop_dict(__m: Dict, keys: _KT, inplace=False) -> Dict:
    if not inplace: __m = __m.copy()
    for __key in cast_tuple(keys):
        if __key in __m:
            __m.pop(__key, None)
    if not inplace: return __m


def drop_records(__r: Records, keys: _KT) -> Records:
    return [drop_dict(__m, keys, inplace=False) for __m in __r]


def drop_df(df: pd.DataFrame, columns: IndexLabel) -> pd.DataFrame:
    return df.drop(columns=inter(df.columns, cast_tuple(columns)))


@multitype_allowed
def drop_data(data: MappingData, __keys: Optional[_KT]=list(),
            return_type: Optional[TypeHint]=None, convert_first=False, **context) -> Data:
    if is_records(data): return drop_records(data, __keys)
    elif isinstance(data, pd.DataFrame): return drop_df(data, __keys)
    elif isinstance(data, Dict): return drop_dict(data, __keys, inplace=False)
    else: return data


###################################################################
############################ Duplicated ###########################
###################################################################

def diff_dict(*args: Dict, skip: IndexLabel=list(), keep: Literal["fist","last"]="fist") -> Tuple[Dict]:
    duplicates = arg_and(*map(lambda x: set(x.keys), args)) - cast_set(skip)
    keep = len(args)-1 if keep == "last" else 0
    return (__m if __i == keep else drop_dict(__m, duplicates, inplace=False) for __i, __m in enumerate(args))


def diff_df(*args: pd.DataFrame, skip: IndexLabel=list(),
            keep: Literal["fist","last"]="first") -> Tuple[pd.DataFrame]:
    duplicates = arg_and(*map(lambda x: set(x.columns), args)) - cast_set(skip)
    keep = len(args)-1 if keep == "last" else 0
    return (df if __i == keep else df.drop(columns=duplicates) for __i, df in enumerate(args))


def drop_duplicates(__r: Records, keys: Optional[_KT]=list(), keep: Literal["fist","last",False]="first") -> Records:
    if keep == False: return _drop_duplicates_all(__r, keys)
    base, history, keys = list(), list(), cast_tuple(keys)
    for __m in (__r[::-1] if keep == "last" else __r):
        values = tuple(kloc(__m, keys, if_null="pass").values())
        if values not in history:
            base.append(__m)
            history.append(values)
    return (base[::-1] if keep == "last" else base)


def _drop_duplicates_all(__r: Records, keys: Optional[_KT]=list()) -> Records:
    history, keys = defaultdict(list), cast_tuple(keys)
    to_comparable = lambda x: x if is_comparable(x) else str(x)
    for __i, __m in enumerate(__r):
        values = tuple(map(to_comparable, kloc(__m, keys, if_null="pass").values()))
        history[values].append(__i)
    indices = sorted(union(*[index for index in history.values() if len(index) == 1]))
    return iloc(__r, indices, if_null="drop")


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


@multitype_allowed
@multitype_rename
@multitype_filter
def sort_values(data: TabularData, by: _KT, ascending: _BOOL=True, fields: Optional[Union[_KT,Index]]=list(),
                default=None, if_null: Literal["drop","pass"]="drop", reorder=True, values_only=False, hier=False,
                return_type: Optional[TypeHint]=None, rename: RenameMap=dict(), convert_first=False, rename_first=False,
                filter_first=False, **context) -> Data:
    asc = ascending if isinstance(ascending, bool) else bool(get_scala(ascending))
    if is_records(data): return sort_records(data, by=by, ascending=asc)
    elif isinstance(data, pd.DataFrame): return data.sort_values(by, ascending=ascending)
    elif isinstance(data, Dict): return dict(sorted(data.items(), key=lambda x: x[0], reverse=(not asc)))
    else: return sorted(data, reverse=(not asc))


###################################################################
############################## String #############################
###################################################################

def re_get(pattern: RegexFormat, string: str, default=str(), index: Optional[int]=0) -> Union[str,List[str]]:
    __pattern = pattern if isinstance(pattern, re.Pattern) else re.compile(pattern)
    if not isinstance(index, int): return __pattern.findall(string)
    catch = __pattern.search(string)
    return __pattern.search(string).groups()[index] if catch else default


def replace_map(__string: str, strip=False, **context) -> str:
    for __old, __new in context.items():
        __string = __string.replace(__old, __new)
    return __string.strip() if strip else __string


def match_keyword(__string: str, __keyword: Keyword) -> bool:
    pattern = re.compile('|'.join(map(re.escape, cast_list(__keyword))))
    return bool(pattern.search(__string))


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
    elif __object and random: return rand.choice(__object) if __object else default
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


def fill_array(__s: Sequence, count: int, value=None) -> List:
    return [__s[__i] if __i < len(__s) else value for __i in range(count)]


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
    return [__s[__i:__i+unit] for __i in range(0,len(__s),unit)]


###################################################################
############################ DataFrame ############################
###################################################################

def merge_drop(left: pd.DataFrame, right: pd.DataFrame, drop: Literal["left","right"]="right",
                how: Literal["left","right","outer","inner","cross"]="inner",
                on: Optional[IndexLabel]=None, **kwargs) -> pd.DataFrame:
    if df_empty(left) or df_empty(right):
        return right if drop == "left" else left

    key_in = lambda columns: not (set(cast_tuple(on)) & set(columns))
    if not on or key_in(left.columns) or key_in(right.columns):
        return right if drop == "left" else left

    duplicates = list((set(left.columns) & set(right.columns)) - set(cast_tuple(on)))
    if drop == "left": left = left.drop(columns=duplicates)
    elif drop == "right": right = right.drop(columns=duplicates)
    return left.merge(right, how=how, on=on, **kwargs)


def unroll_df(df: pd.DataFrame, columns: IndexLabel, values: _VT) -> pd.DataFrame:
    columns, values = cast_tuple(columns), cast_tuple(values)
    get_values = lambda row: [row[__value] for __value in values]
    len_values = lambda row: min(map(len, get_values(row)))
    unroll_row = lambda row: [[row[__column]]*len_values(row) for __column in columns]+get_values(row)
    map_subrow = lambda subrow: {__key: __value for __key, __value in zip(columns+values,subrow)}
    map_row = lambda row: pd.DataFrame([map_subrow(subrow) for subrow in zip(*unroll_row(row))])
    return pd.concat([map_row(row) for _,row in df.iterrows()])


def round_df(df: pd.DataFrame, columns: IndexLabel, trunc=2) -> pd.DataFrame:
    if not isinstance(trunc, int): return df
    __round = lambda x: round(x,trunc) if isinstance(x,float) else x
    return apply_df(df, **{__column: __round for __column in cast_tuple(columns)})


def fillna_each(__object: Union[pd.DataFrame,pd.Series],
                default: Union[pd.DataFrame,pd.Series,Sequence,Any]=None) -> Union[pd.DataFrame,pd.Series]:
    if isna(default): return __object
    elif isinstance(default, (pd.DataFrame, pd.Series)): default.index = __object.index
    elif is_array(default): default = pd.Series(default, index=__object.index)
    else: return __object.fillna(default)

    if isinstance(__object, pd.Series):
        return __object.combine_first(default)
    elif isinstance(__object, pd.DataFrame):
        if isinstance(default, pd.Series):
            for __column in __object.columns:
                __object[__column] = __object[__column].combine_first(default)
        else: return __object.combine_first(default)
    return __object
