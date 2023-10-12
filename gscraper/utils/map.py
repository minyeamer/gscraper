from gscraper.base.types import _KT, _VT, _BOOL, _TYPE, Comparable, Context, TypeHint, Index, IndexLabel, Keyword, Unit
from gscraper.base.types import IndexedSequence, Records, MappingData, TabularData, Data, HtmlData
from gscraper.base.types import ApplyFunction, MatchFunction, BetweenRange, RenameMap, RegexFormat
from gscraper.base.types import not_na, is_na, is_bool_array, is_int_array, is_array, is_2darray, is_records, is_dfarray
from gscraper.base.types import is_list_type, is_dict_type, is_records_type, is_dataframe_type

from gscraper.utils.cast import cast_str, cast_datetime, cast_date, cast_list, cast_tuple, cast_set

from typing import Any, Callable, Dict, List, Set
from typing import Iterable, Literal, Optional, Sequence, Tuple, Union
from bs4 import BeautifulSoup, Tag
import datetime as dt
import pandas as pd

from collections import defaultdict
from itertools import chain
import functools
import inspect
import random as rand
import re

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')


INCLUDE = 0
EXCLUDE = 1

BETWEEN_RANGE_TYPE_MSG = "Between condition must be an iterable or a dictionary."

__or = lambda *args: functools.reduce(lambda x,y: x|y, args)
__and = lambda *args: functools.reduce(lambda x,y: x&y, args)
union = lambda *arrays: functools.reduce(lambda x,y: x+y, arrays)
inter = lambda *arrays: functools.reduce(lambda x,y: [e for e in x if e in y], arrays)
diff = lambda *arrays: functools.reduce(lambda x,y: [e for e in x if e not in y], arrays)


def allin(__object: Iterable) -> bool:
    return bool(tuple(__object)) and all(__object)


def exists(__object, strict=False) -> bool:
    if is_array(__object): return bool(__object) if strict else (len([__e for __e in __object if not_na(__e)]) > 0)
    else: return not_na(__object, strict=strict)


def is_empty(__object, strict=False) -> bool:
    return (not exists(__object, strict=strict))


def str_na(__object, strict=False) -> str:
    return str(__object) if exists(__object, strict=strict) else str()


def df_exists(df: pd.DataFrame, drop_na=False, how: Literal["any","all"]="all") -> bool:
    if not isinstance(df, pd.DataFrame): return False
    return not (df.dropna(axis=0, how=how) if drop_na else df).empty


def df_empty(df: pd.DataFrame, drop_na=True, how: Literal["any","all"]="all") -> bool:
    if not isinstance(df, pd.DataFrame): return False
    return (df.dropna(axis=0, how=how) if drop_na else df).empty


def data_exists(data: Data, drop_na=True, how: Literal["any","all"]="all") -> bool:
    return df_exists(data, drop_na, how) if isinstance(data, pd.DataFrame) else bool(data)


def data_empty(data: Data, drop_na=True, how: Literal["any","all"]="all") -> bool:
    return df_empty(data, drop_na, how) if isinstance(data, pd.DataFrame) else (not data)


def is_context_allowed(func: Callable) -> bool:
    for name, parameter in inspect.signature(func).parameters.items():
        if parameter.kind == inspect.Parameter.VAR_KEYWORD: return True
    return False


def safe_apply(__object, __applyFunc: ApplyFunction, default=None, **context) -> Any:
    try: return __applyFunc(__object, **context) if context and is_context_allowed(__applyFunc) else __applyFunc(__object)
    except: return default


def safe_len(__object, default=0) -> int:
    try: return len(__object)
    except: return default


def between(__object: Comparable, left=None, right=None,
            inclusive: Literal["both","neither","left","right"]="both", **kwargs) -> bool:
    match_left = ((left == None) or (__object >= left if inclusive in ["both","left"] else __object > left))
    match_right = ((right == None) or (__object <= right if inclusive in ["both","right"] else __object < right))
    return match_left & match_right


def map_index(__indices: Index) -> Index:
    if isinstance(__indices, int): return __indices
    elif is_bool_array(__indices, how="all", empty=False):
        return [__i for __i, __match in enumerate(__indices) if __match]
    elif is_int_array(__indices, how="all", empty=False): return __indices
    else: return


def map_context(__keys: _KT, __values: _VT, __default=None, **context) -> Context:
    if context: return context
    elif not (__keys and (__values or __default)): return dict()

    __keys, __values = cast_tuple(__keys), (__values if __values else __default)
    if not is_array(__values): __values = [__values]*len(__keys)
    elif len(__keys) != len(__values): return dict()
    return {__key:__value for __key, __value in zip(__keys, __values)}


###################################################################
############################## String #############################
###################################################################

def re_get(pattern: RegexFormat, string: str, default=str(), groups=False) -> Union[str,List[str]]:
    __pattern = pattern if isinstance(pattern, re.Pattern) else re.compile(pattern)
    if groups: return __pattern.findall(string)
    catch = __pattern.search(string)
    return __pattern.search(string).groups()[0] if catch else default


def replace_map(string: str, strip=False, **context) -> str:
    for __old, __new in context.items():
        string = string.replace(__old, __new)
    return string.strip() if strip else string


def include_text(string: str, value=str(), how: Literal["include","exact"]="include") -> bool:
    if not (isinstance(string, str) and isinstance(value, str)): return False
    return (value in string) if how == "include" else (value == string)


def match_keywords(string: str, keywords: Sequence[str]) -> bool:
    pattern = re.compile('|'.join(map(re.escape, keywords)))
    return bool(pattern.search(string))


###################################################################
############################## Array ##############################
###################################################################

abs_idx = lambda idx: abs(idx+1) if idx < 0 else idx


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


def flatten(*args, iter_type: _TYPE=(List,Set,Tuple)) -> List:
    return [__e for __object in args for __e in (
            __object if isinstance(__object, iter_type) else cast_tuple(__object))]


def unique(*elements, strict=True, unroll=False) -> List:
    array = list()
    for __e in (flatten(*elements) if unroll else elements):
        if not exists(__e, strict=strict): continue
        if __e not in array: array.append(__e)
    return array


def _unique(*elements, strict=True, unroll=False) -> List:
    return unique(*elements, strict=strict, unroll=unroll)


def to_array(__object, default=None, dropna=False, strict=False, unique=False) -> List:
    __s = cast_list(__object)
    if unique: return _unique(*__s, strict=strict)
    elif dropna: return [__e for __e in __s if exists(__e, strict=strict)]
    elif not_na(default, strict=True):
        return [__e if exists(__e, strict=strict) else default for __e in __s]
    else: return __s


def apply_array(__s: IndexedSequence, __indices: Optional[Index]=list(), __applyFunc: Optional[ApplyFunction]=list(),
                all_indices=False, apply: Optional[ApplyFunction]=None, __default=None, **context) -> IndexedSequence:
    __s = __s.copy()
    if all_indices: return [safe_apply(__e, apply) for __e in __s]
    for __i, __apply in map_context(__indices, __applyFunc, __default=apply, **context).items():
        if abs_idx(__i) < len(__s):
            __s[__i] = safe_apply(__s[__i], __apply)
    return __s


def match_array(__s: IndexedSequence, __indices: Optional[Index]=list(), __matchFunc: Optional[MatchFunction]=list(),
                all_indices=False, match: Optional[MatchFunction]=None, how: Literal["filter","all","indexer"]="filter",
                __default=None, **context) -> Union[IndexedSequence,_BOOL]:
    if all_indices:
        return match_array(__s, how=how, **dict(list(range(len(__s))), [match]*len(__s)))
    context = map_context(__indices, __matchFunc, __default=match, **context)
    matches = {__i: safe_apply(__s[__i], match, False) if abs_idx(__i) < len(__s) else False for __i, match in context.items()}
    if how == "filter":
        return [__s[__i] for __i, __match in matches.items() if __match]
    else: return all(matches.values()) if how == "all" else list(matches.values())


def values_to_back(__s: Sequence, values: _VT) -> List:
    return sorted(__s, key=functools.cmp_to_key(lambda x, y: -1 if y in cast_tuple(values) else 0))


def fill_array(__s: Sequence, count: int, value=None) -> List:
    return [__s[__i] if __i < len(__s) else value for __i in range(count)]


def filter_array(__s: Sequence, match: MatchFunction, apply: Optional[ApplyFunction]=None) -> Sequence:
    __apply = apply if apply else (lambda x: x)
    return [__apply(__e) for __e in __s if match(__s)]


def is_same_length(*args: IndexedSequence, empty=True) -> bool:
    __length = set(map(safe_len, args))
    return (len(__length) == 1) and (empty or (0 not in __length))


def unit_array(__s: Sequence, unit=1) -> List[Sequence]:
    return [__s[__i:__i+unit] for __i in range(0,len(__s),unit)]


def concat_array(left: Sequence, right: Sequence, left_index: Sequence[int]) -> List:
    left, right = cast_list(left).copy(), cast_list(right).copy()
    return [left.pop(0) if is_left else right.pop(0) for is_left in left_index]


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
    if dropna: indices = __and(indices, *map(set, map(lambda __s: get_exists_index(__s, strict=strict), args)))
    if unique: indices = __and(indices, *map(set, map(get_unique_index, args)))
    return sorted(indices)


def align_array(*args: Sequence, how: Literal["min","max","first"]="min", default=None,
                dropna=False, strict=False, unique=False) -> Tuple[List]:
    args = [cast_list(__s) for __s in args]
    indices = align_index(*args, how=how, dropna=dropna, strict=strict, unique=unique)
    return tuple([__s[__i] if __i < len(__s) else default for __i in indices] for __s in args)


def transpose_array(__s: Sequence[Sequence], count: Optional[int]=None, unroll=False) -> List[List]:
    if not __s: return list()
    if unroll: __s = list(map(lambda arr: flatten(*arr), __s))
    count = count if isinstance(count, int) and count > 0 else len(__s[0])
    return [list(map(lambda x: x[__i], __s)) for __i in range(count)]


###################################################################
############################### Map ###############################
###################################################################

def kloc(__m: Dict, keys: _KT, default=None, if_null: Literal["drop","pass"]="drop",
        reorder=True, values_only=False) -> Union[_VT,Dict]:
    if not is_array(keys): return __m.get(keys, default)
    elif keys:
        keys = keys if reorder else unique(*__m.keys(), *keys)
        if if_null == "drop": __m = {__key: __m[__key] for __key in keys if __key in __m}
        else: __m = {__key: __m.get(__key, default) for __key in keys}
    else: __m = __m if if_null != "drop" else dict()
    return list(__m.values()) if values_only else __m


def to_dict(__object: MappingData) -> Dict:
    if isinstance(__object, Dict): return __object
    elif is_records(__object, empty=True): return chain_dict(__object, keep="first")
    elif isinstance(__object, pd.DataFrame): return __object.to_dict("dict")
    else: return dict()


def rename_dict(__m: Dict, rename: RenameMap) -> Dict:
    return {rename.get(__key,__key): __value for __key, __value in __m.items()}


def diff_dict(*args: Dict, skip: IndexLabel=list(), keep: Literal["fist","last"]="fist") -> Tuple[Dict]:
    duplicates = __and(*map(lambda x: set(x.keys), args)) - cast_set(skip)
    keep = len(args)-1 if keep == "last" else 0
    return (__m if __i == keep else drop_dict(__m, duplicates, inplace=False) for __i, __m in enumerate(args))


def chain_dict(__object: Sequence[Dict], keep: Literal["fist","last"]="first") -> Dict:
    base = dict()
    for __m in __object:
        if not isinstance(__m, Dict): continue
        for __key, __value in __m.items():
            if (keep == "first") and (__key in base): continue
            else: base[__key] = __value
    return base


def set_dict(__m: Dict, __keys: Optional[_KT]=list(), __values: Optional[_VT]=list(),
            empty=True, inplace=True, **context) -> Dict:
    if not inplace: __m = __m.copy()
    for __key, __value in dict(zip(cast_tuple(__keys), cast_tuple(__values)), **context).items():
        if exists(__value, strict=True) or empty: __m[__key] = __value
    if not inplace: return __m


def drop_dict(__m: Dict, keys: _KT, inplace=False) -> Dict:
    if not inplace: __m = __m.copy()
    for __key in cast_tuple(keys):
        if __key in __m:
            __m.pop(__key, None)
    if not inplace: return __m


def apply_dict(__m: Dict, __keys: Optional[_KT]=list(), __applyFunc: Optional[ApplyFunction]=list(),
                all_keys=False, apply: Optional[ApplyFunction]=None, __default=None, **context) -> Dict:
    __m = __m.copy()
    if all_keys: return {__key: apply(__values, **context) for __key, __values in __m.items()}
    for __key, __apply in map_context(__keys, __applyFunc, __default=apply, **context).items():
        if __key in __m:
            __m[__key] = safe_apply(__m[__key], __apply)
    return __m


def match_dict(__m: Dict, __keys: Optional[_KT]=list(), __matchFunc: Optional[MatchFunction]=list(),
                all_keys=False, match: Optional[MatchFunction]=None, how: Literal["filter","all","indexer"]="filter",
                __default=None, **context) -> Union[Dict,_BOOL]:
    if all_keys:
        return safe_apply(__m, match, False)
    context = map_context(__keys, __matchFunc, __default=match, **context)
    matches = {__key: safe_apply(__m[__key], match, False) if __key in __m else False for __key, match in context.items()}
    if how == "filter":
        return {__key: __m[__key] for __key, __match in matches.items() if __match}
    else: return all(matches.values()) if how == "all" else matches


def between_dict(__m: Dict, __keys: Optional[_KT]=list(), __ranges: Optional[BetweenRange]=list(),
                inclusive: Literal["both","neither","left","right"]="both", null=False,
                __default=None, **context) -> bool:
    match = True
    for __key, __range in map_context(__keys, __ranges, **context).items():
        if __key in __m:
            if is_array(__range): match &= between(__m[__key], *__range[:2], inclusive=inclusive)
            elif isinstance(__range, dict): match &= between(__m[__key], **__range, inclusive=inclusive)
            else: raise ValueError(BETWEEN_RANGE_TYPE_MSG)
        elif not null: return False
    return match


def keys_to_back(__m: Dict, keys: _KT) -> Dict:
    return dict(sorted(__m.items(), key=functools.cmp_to_key(lambda x, y: -1 if y[0] in cast_tuple(keys) else 0)))


def flip_dict(__m: Dict) -> Dict:
    return {__v: __k for __k, __v in __m.items()}


def exists_dict(__m: Optional[Dict]=dict(), strict=False, **context) -> Dict:
    return {__k: __v for __k, __v in dict(__m, **context).items() if exists(__v, strict=strict)}


def hier_get(__m: Dict, __path: _KT, default=None, apply: Optional[ApplyFunction]=None,
            __type: Optional[_TYPE]=None, empty=True, strict=True, **context) -> _VT:
    __m = __m.copy()
    try:
        for __key in cast_tuple(__path):
            __m = __m[__key]
    except: return default
    value = safe_apply(__m, apply, default, **context) if apply else __m
    if __type and not isinstance(value, __type): return default
    return value if empty or exists(value, strict=strict) else default


def hier_set(__m: Dict, __path: _KT, value: _VT, empty=True, strict=True, inplace=True) -> Dict:
    if not inplace: __m = __m.copy()
    if empty or exists(value, strict=strict):
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
    if empty or exists(value, strict=strict):
        return hier_set(__m, __set_path, value, inplace=inplace)
    elif not inplace: return __m


def align_dict(__m: Dict[_KT,Sequence], how: Literal["min","max"]="min", default=None,
                strict=False, dropna=False, unique=False) -> Dict[_KT,List]:
    if not __m: return dict()
    indices = align_index(*map(list, __m.values()), how=how, strict=strict, dropna=dropna, unique=False)
    if dropna or unique:
        return {__key: iloc(__value, indices, if_null="drop") for __key, __value in __m.items()}
    else: return {__key: iloc(__value, indices, default=default, if_null="pass") for __key, __value in __m.items()}


def include_dict(__m: Dict, keys: _KT, include: Optional[Keyword]=list(), exclude: Optional[Keyword]=list(),
                how: Literal["any","all"]="any", if_null=False) -> bool:
    if not (include or exclude): return True
    include, exclude, __match = cast_tuple(include, strict=True), cast_tuple(exclude, strict=True), [False, False]

    for condition, keywords in enumerate([include, exclude]):
        if not keywords: continue
        pattern = re.compile('|'.join(map(re.escape, keywords)))
        matches = tuple(map(pattern.search, map(cast_str, kloc(__m, keys, values_only=True))))
        if len(matches): __match[condition] = all(matches) if how == "all" else any(matches)
        else: __match[condition] = if_null
    return (__match[INCLUDE] or not include) and not (__match[EXCLUDE] and exclude)


###################################################################
############################# Records #############################
###################################################################

def vloc(__r: List[Dict], keys: _KT, default=None, if_null: Literal["drop","pass"]="drop", reorder=True,
        values_only=False) -> Union[Records,List]:
    __r = [kloc(__m, keys, default=default, if_null=if_null, reorder=reorder, values_only=values_only)
            for __m in __r]
    return [__m for __m in __r if __m] if if_null == "drop" else __r


def to_records(__object: MappingData) -> Records:
    if is_records(__object, empty=True): return __object
    elif isinstance(__object, pd.DataFrame): return __object.to_dict("records")
    elif isinstance(__object, dict): return [__object]
    else: return list()


def rename_records(__r: Records, rename: RenameMap) -> Records:
    if not rename: return __r
    return [rename_dict(__m, rename=rename) for __m in __r]


def set_records(__r: Records, __keys: Optional[_KT]=list(), __values: Optional[_VT]=list(),
                empty=True, **context) -> Records:
    return [set_dict(__m, __keys, __values, empty=empty, inplace=False, **context) for __m in __r]


def drop_records(__r: Records, keys: _KT) -> Records:
    return [drop_dict(__m, keys, inplace=False) for __m in __r]


def apply_records(__r: List[Dict], __keys: Optional[_KT]=list(), __applyFunc: Optional[ApplyFunction]=list(),
                    all_keys=False, apply: Optional[ApplyFunction]=None, scope: Literal["keys","dict"]="keys",
                    __default=None, **context) -> Records:
    if scope == "keys":
        return [apply_dict(__m, __keys, __applyFunc, all_keys=all_keys, apply=apply, **context) for __m in __r]
    elif scope == "dict":
        return [safe_apply(__m, apply, **context) for __m in __r]
    else: return list()


def match_records(__r: List[Dict], __keys: Optional[_KT]=list(), __matchFunc: Optional[MatchFunction]=list(),
                all_keys=False, match: Optional[MatchFunction]=None, how: Literal["filter","all","indexer"]="filter",
                scope: Literal["keys","dict"]="keys", __default=None, **context) -> Union[Records,_BOOL]:
    if scope == "keys":
        matches = [match_dict(__m, __keys, __matchFunc, all_keys, match, how="all", **context) for __m in __r]
    elif scope == "dict":
        matches = [safe_apply(__m, match, default=False, **context) for __m in __r]
    else: matches = [False] * len(__r)

    if how == "filter":
        return [__m for __m, __match in zip(__r, matches) if __match]
    else: return all(matches) if how == "all" else matches


def between_records(__r: Records, __keys: Optional[_KT]=list(), __ranges: Optional[BetweenRange]=list(),
                    inclusive: Literal["both","neither","left","right"]="both", null=False,
                    __default=None, **context) -> Records:
    return [__m for __m in __r
            if between_dict(__m, __keys, __ranges, inclusive=inclusive, null=null, **context)]


def sort_records(__r: Records, by: _KT, ascending=True) -> Records:
    return sorted(__r, key=lambda x: kloc(x, cast_tuple(by), values_only=True), reverse=(not ascending))


def groupby_records(__r: Records, by: _KT, if_null: Literal["drop","pass"]="drop") -> Dict[_KT,Records]:
    by = cast_tuple(by)
    groups = defaultdict(list)
    for __m in __r:
        values = kloc(__m, by, default=str(), if_null=if_null, values_only=True)
        if len(values) != len(by): continue
        else: groups[tuple(values)].append(__m)
    return dict(groups)


def isin_records(__r: Records, keys: _KT, how: Literal["any","all"]="any") -> _BOOL:
    if not is_array(keys):
        isin = [keys in __m for __m in __r]
        return allin(isin) if how == "all" else any(isin)
    elif not keys: return list()
    else: return [isin_records(__r, __key, how=how) for __key in keys]


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
    for __i, __m in enumerate(__r):
        values = tuple(kloc(__m, keys, if_null="pass").values())
        history[values].append(__i)
    indices = sorted(union(*[index for index in history.values() if len(index) == 1]))
    return iloc(__r, indices, if_null="drop")


def include_records(__r: Records, keys: _KT, include: Optional[Keyword]=list(), exclude: Optional[Keyword]=list(),
                    how: Literal["any","all"]="any", if_null=False) -> Records:
    if not (include or exclude): return __r
    include, exclude = cast_tuple(include, strict=True), cast_tuple(exclude, strict=True)
    return [__m for __m in __r if include_dict(__m, keys, include=include, exclude=exclude, how=how, if_null=if_null)]


###################################################################
############################ DataFrame ############################
###################################################################

def cloc(df: pd.DataFrame, columns: IndexLabel, default=None, if_null: Literal["drop","pass"]="drop",
        reorder=True) -> pd.DataFrame:
    columns = [inter(cast_tuple(columns), df.columns) if reorder else inter(df.columns, cast_tuple(columns))]
    if not columns: return df if if_null != "drop" else pd.DataFrame()
    elif (len(df.columns) != len(columns)) and if_null != "drop":
        if reorder: df = pd.concat([pd.DataFrame(columns=columns),df])
        else: df = pd.concat([pd.DataFrame(columns=unique(*df.columns,*columns)),df]).fillna(default)
    else: return df[columns]


def to_dataframe(__object: MappingData) -> pd.DataFrame:
    if isinstance(__object, pd.DataFrame): return __object
    elif is_records(__object, empty=True): return pd.DataFrame(__object)
    elif isinstance(__object, dict): return pd.DataFrame([__object])
    else: return pd.DataFrame()


def diff_df(*args: pd.DataFrame, skip: IndexLabel=list(),
            keep: Literal["fist","last"]="first") -> Tuple[pd.DataFrame]:
    duplicates = __and(*map(lambda x: set(x.columns), args)) - cast_set(skip)
    keep = len(args)-1 if keep == "last" else 0
    return (df if __i == keep else df.drop(columns=duplicates) for __i, df in enumerate(args))


def concat_df(__object: Sequence[pd.DataFrame], axis=0, keep: Literal["fist","last"]="first") -> pd.DataFrame:
    if axis == 1: __object = diff_df(*__object, keep=keep)
    else: __object = [df for df in __object if df_exists(df)]
    return pd.concat(__object, axis=axis) if __object else pd.DataFrame()


def set_df(df: pd.DataFrame, __columns: Optional[IndexLabel]=list(), __values: Optional[_VT]=list(),
            empty=True, **context) -> pd.DataFrame:
    df = df.copy()
    for __column, __value in dict(zip(cast_tuple(__columns), cast_tuple(__values)), **context).items():
        if exists(__value, strict=True) or empty: df[__column] = __value
    return df


def drop_df(df: pd.DataFrame, columns: IndexLabel) -> pd.DataFrame:
    return df.drop(columns=inter(df.columns, cast_tuple(columns)))


def apply_df(df: pd.DataFrame, __columns: Optional[IndexLabel]=list(), __applyFunc: Optional[ApplyFunction]=list(),
            all_cols=False, apply: Optional[ApplyFunction]=None, __default=None, **context) -> pd.DataFrame:
    df = df.copy()
    if all_cols: return df.apply(apply, **context)
    context = map_context(__columns, __applyFunc, __default=apply, **context)
    context = {str(__column): __apply for __column, __apply in context.items() if __column in df}
    return df.apply(context)


def match_df(df: pd.DataFrame, __columns: Optional[IndexLabel]=list(), __matchFunc: Optional[MatchFunction]=list(),
            all_cols=False, match: Optional[MatchFunction]=None, how: Literal["filter","all","indexer"]="filter",
            __default=None, **context) -> Union[pd.DataFrame,pd.Series,bool]:
    matches = apply_df(df, __columns, __matchFunc, all_cols=all_cols, match=match, **context).all(axis=1)
    if how == "filter": return df[matches]
    else: return matches.all(axis=0) if how == "all" else matches


def between_df(df: pd.DataFrame, __columns: Optional[IndexLabel]=list(), __ranges: Optional[BetweenRange]=list(),
                inclusive: Literal["both","neither","left","right"]="both", null=False,
                __default=None, **context) -> pd.DataFrame:
    df, df.copy()
    kwargs = {"inclusive": inclusive}
    for __column, __range in map_context(__columns, __ranges, **context).items():
        if __column in df and isinstance(__column, str):
            if_na = pd.Series([False]*len(df), index=df.index) if not null else df[__column].isna()
            if is_array(__range): df = df[df[__column].apply(lambda x: between(x, *__range[:2], **kwargs))|if_na]
            elif isinstance(__range, dict): df = df[df[__column].apply(lambda x: between(x, **__range, **kwargs))|if_na]
            else: raise ValueError(BETWEEN_RANGE_TYPE_MSG)
    return df


def groupby_df(df: pd.DataFrame, by: _KT, if_null: Literal["drop","pass"]="drop") -> Dict[_KT,pd.DataFrame]:
    counts = cloc(df, cast_list(by), if_null=if_null).value_counts().reset_index(name="count")
    keys = counts.drop(columns="count").to_dict("split")["data"]
    return {tuple(key): match_df(df, **dict(zip(by, map(lambda x: (lambda y: x == y), key)))) for key in keys}


def include_df(df: pd.DataFrame, columns: IndexLabel, include: Optional[Keyword]=list(), exclude: Optional[Keyword]=list(),
                how: Literal["any","all"]="any", if_null=False) -> pd.DataFrame:
    if not (include or exclude): return df
    include, exclude = cast_tuple(include, strict=True), cast_tuple(exclude, strict=True)
    __op = __and if how == "all" else __or
    for __column in columns:
        if __column not in df: return df if if_null else pd.DataFrame(columns=df.columns)
        df = df[((__op(*[df[str(__column)].astype(str).str.contains(__inc) for __inc in include]) if include else True)&
                ~(__op(*[df[str(__column)].astype(str).str.contains(__exc) for __exc in exclude]) if exclude else False))|
                (df[str(__column)].isna() if if_null else False)]
    return df


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
    if is_na(default, strict=True): return __object
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
            __object = __object.apply(lambda x: apply_df(x, apply=__applyFunc, all_cols=True, **context))
    return fillna_each(__object, default)


###################################################################
########################## Beautiful Soup #########################
###################################################################

def clean_html(__object: str) -> str:
    return BeautifulSoup(cast_str(__object), "lxml").text


def clean_tag(__object: str) -> str:
    return re.sub("<[^>]*>", "", cast_str(__object))


def get_selector(selector: _KT, sep=' > ', index: Optional[Unit]=None) -> Tuple[str,Unit]:
    if not (isinstance(selector, (List,str)) and selector): return str(), None
    elif not isinstance(selector, List): return selector, index
    selector = selector.copy()
    index = selector.pop() if isinstance(selector[-1], (int,List,Tuple)) else index
    return sep.join(selector), index


def select(source: Tag, selector: _KT, sep=' > ', index: Optional[Unit]=None) -> Union[Tag,List[Tag]]:
    selector, index = get_selector(selector, sep=sep, index=index)
    if not selector: return source
    elif index == 0: return source.select_one(selector)
    elif not index: return source.select(selector)
    else: return iloc(source.select(selector), index)


def select_one(source: Tag, selector: _KT, sep=' > ') -> Tag:
    selector, _ = get_selector(selector, sep=sep)
    if not selector: return source
    else: return source.select_one(selector)


def parse_text(string: Union[str,Sequence[str]], strip=False, replace: RenameMap=dict()) -> Union[str,List[str]]:
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


def _selector_by(selector: _KT, key=str(), by: Literal["source","text"]="source") -> Tuple[_KT,str,str]:
    if isinstance(selector, str):
        if selector == "text()": return (str(), str(), "text")
        elif selector.startswith('@'): return (str(), selector[1:], by)
        elif selector.startswith('.'): return (str(), "class", by)
        elif selector.startswith('#'): return (str(), "id", by)
        else: return (selector, key, by)
    elif is_array(selector) and selector:
        tag, key, by = _selector_by(selector[-1], key, by)
        return ((selector+[tag] if tag else selector), key, by)
    else: return (str(), str(), by)


def select_by(source: Tag, selector: _KT, key=str(), sep=' > ', index: Optional[Unit]=None,
            by: Literal["source","text"]="source", lines: Literal["ignore","split","raw"]="ignore",
            strip=True, replace: RenameMap=dict()) -> HtmlData:
    selector, key, by = _selector_by(selector, key, by)
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
        data = select_by(source, selector, key, sep, index, by, lines, strip, replace)
    except: return default
    data = safe_apply(data, apply, default, **context) if apply else data
    if __type and not isinstance(data, __type): return default
    return data if empty or exists(data, strict=strict) else default


def exists_source(source: Tag, selector: _KT, key=str(), sep=' > ', index: Optional[Unit]=0,
                    by: Literal["source","text"]="source", strip=True, replace: RenameMap=dict(),
                    strict=False) -> bool:
    context = dict(key=key, sep=sep, index=index, by=by, lines="raw", strip=strip, replace=replace)
    source = hier_select(source, selector, **context)
    return not_na(source, strict=strict)


def _get_class_name(selector: _KT) -> str:
    tag = str(get_scala(selector, -1))
    return tag[1:] if tag.startswith('.') or tag.startswith('#') else str()


def include_source(source: Tag, selector: _KT, key=str(), sep=' > ', index: Optional[Unit]=0,
                    by: Literal["source","text"]="source", strip=True, replace: RenameMap=dict(),
                    how: Literal["include","exact"]="exact", value=str()) -> bool:
    context = dict(key=key, sep=sep, index=index, by=by, lines="raw", strip=strip, replace=replace)
    source = hier_select(source, selector, **context)
    name = _get_class_name(selector)
    value = name if name else value
    if not (value and isinstance(value, str)): return bool(source)
    elif is_array(source): return any([include_text(str(__t), value, how) for __t in source])
    else: return include_text(str(source), value, how)


def match_source(source: Tag, selector: _KT, key=str(), sep=' > ', index: Optional[Unit]=0,
                by: Literal["source","text"]="source", strip=True, replace: RenameMap=dict(),
                how: Literal["include","exact","exist"]="exact", value=str(),
                strict=False, flip=False) -> bool:
    args = (source, selector, key, sep, index, by, strip, replace)
    if how == "exist": match = exists_source(*args, strict=strict)
    else: match = include_source(*args, how=how, value=value)
    return (not match) if flip else match


def filter_source(__s: List[Tag], selector: _KT, key=str(), sep=' > ', index: Optional[Unit]=0,
                by: Literal["source","text"]="source", strip=True, replace: RenameMap=dict(),
                how: Literal["include","exact","exist"]="exact", value=str(),
                strict=False, flip=False) -> List[Tag]:
    array = list()
    args = (selector, key, sep, index, by, strip, replace, how, value, strict, flip)
    for __t in __s:
        if match_source(__t, *args):
            array.append(__t)
    return array


def groupby_source(__s: List[Tag], groupby: Union[_KT,Dict], sep=' > ', index: Optional[Unit]=0,
                by: Literal["source","text"]="source", strip=True, replace: RenameMap=dict(),
                how: Literal["include","exact","exist"]="exact", value=str(),
                strict=False, flip=False) -> Dict[_KT,Tag]:
    groups, others = defaultdict(list), list()
    args = (str(), sep, index, by, strip, replace, how, value, strict, flip)
    if not isinstance(groupby, Dict):
        groupby = dict(enumerate(cast_list(groupby), start=1))
    for __t in __s:
        match = False
        for group, selector in groupby.items():
            match |= match_source(__t, selector, *args)
            if match:
                groups[group].append(__t)
                break
        if not match: others.append(__t)
    if others: groups[None] = others
    return groups


###################################################################
############################ Multitype ############################
###################################################################

def exists_one(*args, strict=False) -> Any:
    for arg in args:
        if exists(arg, strict=strict): return arg
    if args: return args[-1]


def filter_exists(__object, strict=False) -> Any:
    if is_array(__object):
        return type(__object)([__value for __value in __object if exists(__value, strict=strict)])
    elif isinstance(__object, Dict):
        return {key: __value for key, __value in __object.items() if exists(__value, strict=strict)}
    elif isinstance(__object, pd.DataFrame):
        return __object.dropna(axis=1, how=("all" if strict else "any"))
    elif exists(__object, strict=strict): return __object


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
        return convert_data(data, return_type)
    return wrapper


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


@multitype_allowed
@multitype_rename
def filter_data(data: Data, fields: Optional[Union[_KT,Index]]=list(), default=None,
                if_null: Literal["drop","pass"]="drop", reorder=True, return_type: Optional[TypeHint]=None,
                rename: RenameMap=dict(), convert_first=False, rename_first=False) -> Data:
    if not fields: return data if if_null != "drop" else list()
    if is_records(data): return vloc(data, keys=fields, default=default, if_null=if_null, reorder=reorder)
    elif isinstance(data, pd.DataFrame): return cloc(data, columns=fields, default=default, if_null=if_null, reorder=reorder)
    elif isinstance(data, Dict): return kloc(data, keys=fields, default=default, if_null=if_null, reorder=reorder)
    elif is_array(data): return iloc(data, indices=fields, default=default, if_null=if_null)
    else: return list()


def multitype_filter(func):
    @functools.wraps(func)
    def wrapper(data: Data, *args, fields: Optional[Union[_KT,Index]]=list(), default=None,
                if_null: Literal["drop","pass"]="pass", reorder=True, filter_first=False, **kwargs):
        if not fields: return func(data, *args, **kwargs) if if_null != "drop" else list()
        if filter_first: data = filter_data(data, fields, default, if_null, reorder)
        data = func(data, *args, **kwargs)
        if not filter_first: data = filter_data(data, fields, default, if_null, reorder)
        return data
    return wrapper


@multitype_allowed
@multitype_rename
@multitype_filter
def chain_exists(data: Data, data_type: Optional[TypeHint]=None, keep: Literal["fist","last"]="first",
                fields: Optional[Union[_KT,Index]]=list(), default=None, if_null: Literal["drop","pass"]="drop",
                reorder=True, return_type: Optional[TypeHint]=None, rename: RenameMap=dict(),
                convert_first=False, rename_first=False, filter_first=False) -> Data:
    if is_dfarray(data): return concat_df([df for df in data if df_exists(df)])
    elif is_2darray(data): return list(chain.from_iterable([__s for __s in data if is_array(__s)]))
    elif data_type and is_dict_type(data_type) and is_records(data, how="any"):
        return chain_dict([__m for __m in data if isinstance(__m, Dict)], keep=keep)
    else: return filter_exists(data)


@multitype_allowed
def set_data(data: MappingData, __keys: Optional[_KT]=list(), __values: Optional[_VT]=list(), empty=True,
            return_type: Optional[TypeHint]=None, convert_first=False, **context) -> Data:
    if is_records(data): return set_records(data, __keys, __values, empty=empty, **context)
    elif isinstance(data, pd.DataFrame): return set_df(data, __keys, __values, empty=empty, **context)
    elif isinstance(data, Dict): return set_dict(data, __keys, __values, empty=empty, inplace=False, **context)
    else: return data


@multitype_allowed
def drop_data(data: MappingData, __keys: Optional[_KT]=list(),
            return_type: Optional[TypeHint]=None, convert_first=False, **context) -> Data:
    if is_records(data): return drop_records(data, __keys)
    elif isinstance(data, pd.DataFrame): return drop_df(data, __keys)
    elif isinstance(data, Dict): return drop_dict(data, __keys, inplace=False)
    else: return data


@multitype_allowed
@multitype_rename
@multitype_filter
def apply_data(data: Data, __keys: Optional[Union[_KT,Index]]=list(), __applyFunc: Optional[ApplyFunction]=list(),
                all_keys=False, apply: Optional[ApplyFunction]=None, fields: Optional[Union[_KT,Index]]=list(),
                default=None, if_null: Literal["drop","pass"]="drop", reorder=True, return_type: Optional[TypeHint]=None,
                rename: RenameMap=dict(), convert_first=False, rename_first=False, filter_first=False, **context) -> Data:
    if is_records(data): return apply_records(data, __keys, __applyFunc, all_keys=all_keys, apply=apply, **context)
    elif isinstance(data, pd.DataFrame): return apply_df(data, __keys, __applyFunc, all_cols=all_keys, apply=apply, **context)
    elif isinstance(data, Dict): return apply_dict(data, __keys, __applyFunc, all_keys=all_keys, apply=apply, **context)
    elif is_array(data): return apply_array(data, __keys, __applyFunc, all_indices=all_keys, apply=apply, **context)
    else: return data


@multitype_allowed
@multitype_rename
@multitype_filter
def match_data(data: Data, __keys: Optional[Union[_KT,Index]]=list(), __matchFunc: Optional[MatchFunction]=list(),
                all_keys=False, match: Optional[MatchFunction]=None, how: Literal["filter","all","indexer"]="filter",
                fields: Optional[Union[_KT,Index]]=list(), default=None, if_null: Literal["drop","pass"]="drop",
                reorder=True, return_type: Optional[TypeHint]=None, rename: RenameMap=dict(),
                convert_first=False, rename_first=False, filter_first=False, **context) -> Data:
    if is_records(data): return match_records(data, __keys, __matchFunc, all_keys=all_keys, match=match, how=how, **context)
    elif isinstance(data, pd.DataFrame): return match_df(data, __keys, __matchFunc, all_cols=all_keys, match=match, how=how, **context)
    elif isinstance(data, Dict): return match_dict(data, __keys, __matchFunc, all_keys=all_keys, match=match, how=how, **context)
    elif is_array(data): return match_array(data, __keys, __matchFunc, all_indices=all_keys, match=match, how=how, **context)
    else: return data


@multitype_allowed
@multitype_rename
@multitype_filter
def between_data(data: MappingData, inclusive: Literal["both","neither","left","right"]="both", null=False,
                fields: Optional[Union[_KT,Index]]=list(), default=None, if_null: Literal["drop","pass"]="drop",
                reorder=True, return_type: Optional[TypeHint]=None, rename: RenameMap=dict(), convert_first=False,
                rename_first=False, filter_first=False, **context) -> MappingData:
    if is_records(data): return between_records(data, inclusive=inclusive, null=null, **context)
    elif isinstance(data, pd.DataFrame): return between_df(data, inclusive=inclusive, null=null)
    elif isinstance(data, Dict): return data if between_dict(data, inclusive=inclusive, null=null) else dict()
    else: return data


@multitype_allowed
@multitype_rename
@multitype_filter
def sort_values(data: TabularData, by: _KT, ascending: _BOOL=True, fields: Optional[Union[_KT,Index]]=list(),
                default=None, if_null: Literal["drop","pass"]="drop", reorder=True, return_type: Optional[TypeHint]=None,
                rename: RenameMap=dict(), convert_first=False, rename_first=False, filter_first=False, **context) -> Data:
    if is_records(data):
        ascending = bool(iloc(ascending, 0)) if isinstance(ascending, Sequence) else ascending
        return sort_records(data, by=by, ascending=ascending)
    elif isinstance(data, pd.DataFrame): return data.sort_values(by, ascending=ascending)
    elif isinstance(data, Dict): return {__key: data[__key] for __key in sorted(data.keys())}
    elif is_array(data): return sorted(data)
    else: return data
