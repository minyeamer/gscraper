from gscraper.base.types import _KT, _VT, _PASS, _BOOL, _TYPE, Comparable, Context, TypeHint
from gscraper.base.types import Index, IndexLabel, Keyword, Unit, IndexedSequence, RenameMap
from gscraper.base.types import Records, MappingData, TabularData, Data, PandasData, HtmlData, ResponseData
from gscraper.base.types import ApplyFunction, MatchFunction, RegexFormat, PANDAS_DATA
from gscraper.base.types import is_bool_array, is_int_array, is_array, is_2darray, is_records, is_dfarray, is_tag_array
from gscraper.base.types import init_origin, is_list_type, is_dict_type, is_records_type, is_dataframe_type
from gscraper.base.types import is_comparable, is_kwargs_allowed

from gscraper.utils import isna, notna, empty, exists
from gscraper.utils.cast import cast_str, cast_list, cast_tuple, cast_set

from typing import Any, Callable, Dict, List, Set
from typing import Iterable, Literal, Optional, Sequence, Tuple, Union
from numbers import Real
from bs4 import BeautifulSoup, Tag
from pandas.core.indexes.base import Index as PandasIndex
import pandas as pd

from collections import defaultdict
from itertools import chain
import functools
import random as rand
import re

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')


INVALID_ISIN_MSG = "Isin function requires at least one parameter: exact, include, and exclude."
SOURCE_SEQUENCE_TYPE_MSG = "Required array of HTML source as input."
LENGTH_MISMATCH_MSG = lambda left, right: f"Length of two input values are mismatched: [{left}, {right}]"


def arg_or(*args: bool) -> bool:
    if not args: return False
    else: return functools.reduce(lambda x,y: x|y, args)

def arg_and(*args: bool) -> bool:
    if not args: return False
    else: return functools.reduce(lambda x,y: x&y, args)

def union(*arrays) -> Any:
    if not arrays: return None
    else: return functools.reduce(lambda x,y: x+y, arrays)

def inter(*arrays: Sequence) -> List:
    if not arrays: return list()
    else: return functools.reduce(lambda x,y: [e for e in x if e in y], arrays)

def diff(*arrays: Sequence) -> List:
    if not arrays: return list()
    else: return functools.reduce(lambda x,y: [e for e in x if e not in y], arrays)


def isna_plus(__object, strict=True) -> bool:
    if is_array(__object):
        if not __object: return True
        elif strict: return all(pd.isna(__object))
        else: return empty(__object)
    if isinstance(__object, pd.Series):
        if __object.empty: return True
        elif strict: return __object.isna().all()
        else: return __object.apply(empty).all()
    if isinstance(__object, pd.DataFrame):
        if __object.empty: return True
        elif strict: return __object.isna().all().all()
        else: return apply_df(__object, empty, all_cols=True).all().all()
    else: return isna(__object, strict=strict)


def notna_plus(__object, strict=True) -> bool:
    if is_array(__object):
        if not __object: return False
        elif strict: return any(pd.notna(__object))
        else: return exists(__object)
    if isinstance(__object, pd.Series):
        if __object.empty: return False
        elif strict: return __object.notna().any()
        else: return __object.apply(exists).any()
    if isinstance(__object, pd.DataFrame):
        if __object.empty: return False
        elif strict: return __object.notna().any().any()
        else: return apply_df(__object, exists, all_cols=True).any().any()
    else: return notna(__object, strict=strict)


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


def to_dict(__object: MappingData, orient: Optional[Literal["dict","list","index"]]="dict", depth=1) -> Dict:
    if isinstance(__object, Dict): return __object
    elif is_records(__object, empty=True): return chain_dict(__object, keep="first")
    elif isinstance(__object, pd.DataFrame):
        return fillna_dict(__object.to_dict(orient), value=None, strict=True, depth=depth)
    else: return dict()


def to_records(__object: MappingData, depth=1) -> Records:
    if is_records(__object, empty=True): return __object
    elif isinstance(__object, pd.DataFrame):
        return fillna_records(__object.to_dict("records"), value=None, strict=True, depth=depth)
    elif isinstance(__object, Dict): return [__object]
    else: return list()


def to_dataframe(__object: MappingData, columns: Optional[IndexLabel]=None, index: Optional[Sequence]=None) -> pd.DataFrame:
    if isinstance(__object, pd.DataFrame): pass
    elif is_records(__object, empty=False): __object = convert_dtypes(pd.DataFrame(align_records(__object)))
    elif isinstance(__object, (dict,pd.Series)): __object = pd.DataFrame([__object])
    else: return pd.DataFrame(columns=cast_columns(columns))
    if (index is not None) and (safe_len(index, -1) == len(__object)): __object.index = index
    return __object


def to_series(__object: Union[pd.Series,Sequence,Any], index: Optional[Sequence]=None) -> pd.Series:
    if isinstance(__object, pd.Series): pass
    elif is_array(__object): __object = pd.Series(__object)
    else: __object = pd.Series([__object]*safe_len(index, 1), dtype="object")
    if safe_len(index, -1) == len(__object): __object.index = index
    return __object


def convert_data(data: Data, return_type: Optional[TypeHint]=None, depth=1) -> Data:
    if not return_type: return data
    elif is_records_type(return_type): return to_records(data, depth=depth)
    elif is_dataframe_type(return_type): return to_dataframe(data)
    elif is_dict_type(return_type): return to_dict(data, depth=depth)
    elif is_list_type(return_type): return cast_list(data)
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
    __inter = inter(columns, get_columns(df)) if reorder else inter(get_columns(df), columns)
    if not __inter:
        return init_df(df, columns=list()) if if_null == "drop" else df
    elif not is_array(columns): df = df[columns]
    elif (if_null == "pass") and (len(columns) != len(__inter)):
        columns = columns if reorder else unique(*__inter, *columns)
        df = pd.concat([init_df(df, columns=columns), df[__inter]])
    else: df = df[__inter]
    df = fillna_data(df, default)
    if isinstance(df, pd.Series): return df.tolist() if values_only else df
    else: return list(df.to_dict("list").values()) if values_only else df


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


def get_value(data: ResponseData, field: Union[_KT,Index], default=None, **kwargs) -> _VT:
    if not field: return default
    elif isinstance(data, Dict): return hier_get(data, field, default)
    elif is_records(data): return [hier_get(__m, field, default) for __m in data]
    elif isinstance(data, PANDAS_DATA):
        column = get_scala(field)
        return fillna_data(data[column], default) if column in data else default
    elif isinstance(data, Tag): return hier_select(data, field, default, **kwargs)
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


def notna_dict(__m: Optional[Dict]=dict(), null_if: Dict=dict(), strict=True, **context) -> Dict:
    if not null_if:
        return {__k: __v for __k, __v in dict(__m, **context).items() if notna(__v, strict=strict)}
    else: return {__k: __v for __k, __v in dict(__m, **context).items() if notna(__v, strict=strict) and (__v != null_if.get(__k))}


def exists_dict(__m: Optional[Dict]=dict(), null_if: Dict=dict(), strict=False, **context) -> Dict:
    if not null_if:
        return {__k: __v for __k, __v in dict(__m, **context).items() if exists(__v, strict=strict)}
    else: return {__k: __v for __k, __v in dict(__m, **context).items() if exists(__v, strict=strict) and (__v != null_if.get(__k))}


def df_empty(df: pd.DataFrame, drop_na=False, how: Literal["any","all"]="all") -> bool:
    return isinstance(df, pd.DataFrame) and (df.dropna(axis=0, how=how) if drop_na else df).empty


def df_exists(df: pd.DataFrame, drop_na=False, how: Literal["any","all"]="all") -> bool:
    return isinstance(df, pd.DataFrame) and not (df.dropna(axis=0, how=how) if drop_na else df).empty


def data_exists(data: Data, strict=False) -> bool:
    if isinstance(data, (pd.DataFrame, pd.Series)): return not data.empty
    else: return exists(data, strict=strict)


def data_empty(data: Data, strict=False) -> bool:
    if isinstance(data, (pd.DataFrame, pd.Series)): return data.empty
    else: return empty(data, strict=strict)


###################################################################
############################### Isin ##############################
###################################################################

def allin(__iterable: Sequence[bool]) -> bool:
    return __iterable and all(__iterable)


def howin(__iterable: Sequence[bool], how: Literal["any","all"]="all") -> bool:
    return allin(__iterable) if how == "all" else any(__iterable)


def isin(__iterable: Iterable, exact: Optional[_VT]=None, include: Optional[_VT]=None,
        exclude: Optional[_VT]=None, how: Literal["any","all"]="any", **kwargs) -> bool:
    __match, __all = (True, (how == "all"))
    if notna(exact): __match &= _isin(__iterable, exact, how="exact", all=__all)
    if __match and notna(include): __match &= _isin(__iterable, include, how="include", all=__all)
    if __match and notna(exclude): __match &= _isin(__iterable, exclude, how="exclude", all=True)
    return __match


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
                how: Literal["any","all"]="any", if_null=False, hier=False) -> bool:
    if isna(exact) and isna(include) and isna(exclude): return True
    keys = keys if keys or hier else list(__m.keys())
    __values = kloc(__m, keys, if_null="drop", values_only=True, hier=hier)
    if isna(__values): return if_null
    elif is_single_path(keys, hier=hier): pass
    elif (how == "all") and (not if_null) and (len(keys) != len(__values)): return False
    return isin(__values, exact, include, exclude, how=how)


def isin_records(__r: Records, keys: Optional[_KT]=None,
                exact: Optional[_VT]=None, include: Optional[_VT]=None, exclude: Optional[_VT]=None,
                how: Literal["any","all"]="any", if_null=False, hier=False, filter=True) -> Union[Records,List[bool]]:
    if isna(exact) and isna(include) and isna(exclude): return __r
    __matches = [isin_dict(__m, keys, exact, include, exclude, how, if_null, hier) for __m in __r]
    return [__m for __match, __m in zip(__matches, __r) if __match] if filter else __matches


def keyin_records(__r: Records, keys: _KT) -> Union[bool,Dict[_KT,bool]]:
    all_keys = union(*[list(__m.keys()) for __m in __r])
    return {__key: (__key in all_keys) for __key in keys} if is_array(keys) else keys in all_keys


def isin_df(df: pd.DataFrame, columns: Optional[IndexLabel]=None,
            exact: Optional[_VT]=None, include: Optional[_VT]=None, exclude: Optional[_VT]=None,
            how: Literal["any","all"]="any", if_null=False, filter=True) -> PandasData:
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


def _isin_series(series: pd.Series, __values: Optional[IndexLabel]=None,
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
                if_null=False, hier=False, index: Optional[Unit]=None, key=str(), text=True, sep=' ', strip=True,
                filter=True, **kwargs) -> Union[bool,List[Tag],List[bool]]:
    if is_array(source):
        context = locals()
        __matches = [isin_source(dict(context, source=__s)) for __s in source]
        return [__s for __match, __s in zip(__matches, source) if __match] if filter else __matches
    if isna(exact) and isna(include) and isna(exclude):
        name = exists_one(*map(_get_class_name, (selectors if hier else cast_tuple(selectors))))
        if not name: return True
        else: exact = name
    context = dict(index=index, key=key, text=text, sep=sep, strip=strip)
    __values = sloc(source, selectors, if_null="drop", values_only=True, hier=hier, **context)
    if not __values: return if_null
    elif is_single_selector(selectors, hier=hier, index=index): pass
    elif (how == "all") and (not if_null) and (len(selectors) != len(__values)): return False
    return isin(__values, exact, include, exclude, how=how)


def _get_class_name(selector: _KT) -> str:
    tag = str(get_scala(selector, -1))
    return tag[1:] if tag.startswith('.') or tag.startswith('#') else str()


def isin_data(data: ResponseData, path: Optional[Union[_KT,IndexLabel]]=None,
                exact: Optional[_VT]=None, include: Optional[_VT]=None, exclude: Optional[_VT]=None,
                how: Literal["any","all"]="any", if_null=False, hier=False, filter=False,
                **kwargs) -> Union[bool,List[bool],pd.Series]:
    context = dict(exact=exact, include=include, exclude=exclude, how=how, if_null=if_null)
    if isinstance(data, Dict): return isin_dict(data, path, hier=hier, **context)
    elif is_records(data): return isin_records(data, path, filter=filter, **context)
    elif isinstance(data, pd.DataFrame): return isin_df(data, path, filter=filter, **context)
    elif isinstance(data, Tag): return isin_source(data, hier=hier, **context, **kwargs)
    else: return isin(data, **context)


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
    if isinstance(__object, List):
        return [__value for __value in __object if exists(__value, strict=strict)]
    if isinstance(__object, Tuple):
        return tuple(__value for __value in __object if exists(__value, strict=strict))
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


def chain_exists(data: Data, data_type: Optional[TypeHint]=None, keep: Literal["fist","last"]="first") -> Data:
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
        else: groups[values[0] if len(values) == 1 else tuple(values)].append(__m)
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

def diff_dict(*args: Dict, skip: IndexLabel=list(), keep: Literal["fist","last"]="fist") -> Tuple[Dict]:
    duplicates = arg_and(*map(lambda x: set(x.keys), args)) - cast_set(skip)
    keep = len(args)-1 if keep == "last" else 0
    return tuple((__m if __i == keep else drop_dict(__m, duplicates, inplace=False)) for __i, __m in enumerate(args))


def diff_df(*args: pd.DataFrame, skip: IndexLabel=list(),
            keep: Literal["fist","last"]="first") -> Tuple[pd.DataFrame]:
    duplicates = arg_and(*map(lambda x: set(x.columns), args)) - cast_set(skip)
    keep = len(args)-1 if keep == "last" else 0
    return tuple((df if __i == keep else df.drop(columns=duplicates)) for __i, df in enumerate(args))


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
############################### Fill ##############################
###################################################################

def fill_array(__s: Sequence, count: int, value=None) -> List:
    return [(__s[__i] if __i < len(__s) else value) for __i in range(count)]


def fillna_array(__s: Sequence, value=None, strict=True, depth=1) -> List:
    if depth < 1: return __s
    fillna = lambda __value: fillna_data(__value, value, strict=strict, depth=depth-1) if depth > 1 else __value
    return [(fillna(__e) if notna(__e, strict=strict) else value) for __e in __s]


def fillna_dict(__m: Dict, value=None, strict=True, depth=1) -> Dict:
    if depth < 1: return __m
    fillna = lambda __value: fillna_data(__value, value, strict=strict, depth=depth-1) if depth > 1 else __value
    return {__key: (fillna(__value) if notna(__value, strict=strict) else value) for __key, __value in __m.items()}


def fillna_records(__r: Records, value=None, strict=True, depth=1) -> Records:
    if depth < 1: return __r
    return [fillna_dict(__m, value, strict=strict, depth=depth) for __m in __r]


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
                strict=True, depth=1) -> Union[PandasData,Records]:
    if isna_plus(value) and isinstance(__object, PANDAS_DATA): return __object
    elif isinstance(__object, pd.DataFrame): return fillna_df(__object, value)
    elif isinstance(__object, pd.Series): return fillna_series(__object, value)
    elif is_records(__object): return fillna_records(__object, value, strict=strict, depth=depth)
    elif isinstance(__object, Dict): return fillna_dict(__object, value, strict=strict, depth=depth)
    elif is_array(__object): return fillna_array(__object, strict=strict, depth=depth)
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

def re_get(pattern: RegexFormat, string: str, default=str(), index: Optional[int]=0) -> Union[str,List[str]]:
    __pattern = pattern if isinstance(pattern, re.Pattern) else re.compile(pattern)
    if not isinstance(index, int): return __pattern.findall(string)
    catch = __pattern.search(string)
    return __pattern.search(string).groups()[index] if catch else default


def replace_map(__string: str, **context) -> str:
    for __old, __new in context.items():
        __string = __string.replace(__old, __new)
    return __string


def match_keyword(__string: str, __keyword: Keyword) -> bool:
    pattern = re.compile('|'.join(map(re.escape, cast_list(__keyword))))
    return bool(pattern.search(__string))


def startswith(__string: str, __keyword: Keyword) -> bool:
    for __s in __keyword:
        if __string.startswith(__s): return True
    else: return False


def endswith(__string: str, __keyword: Keyword) -> bool:
    for __s in __keyword:
        if __string.endswith(__s): return True
    else: return False


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
############################ DataFrame ############################
###################################################################

def init_df(df: Optional[PandasData]=None, columns: Optional[IndexLabel]=None) -> PandasData:
    if isinstance(df, pd.DataFrame):
        return pd.DataFrame(columns=(columns if is_columns(columns) else df.columns))
    elif isinstance(df, pd.Series):
        return pd.Series(index=(columns if columns else df.index), dtype=df.dtypes)
    else: return pd.DataFrame(columns=cast_columns(columns))


def is_columns(columns: IndexLabel) -> bool:
    return is_array(columns) or isinstance(columns, PandasIndex)


def cast_columns(__object, unique=False) -> IndexLabel:
    if isinstance(__object, PandasIndex):
        return (__object.unique() if unique else __object).tolist()
    else: return cast_list(__object)


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
        try: df[column] = df.pop(column+FIRST).combine_first(df.pop(column+SECOND))
        except: df[column] = df.pop(column)
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
