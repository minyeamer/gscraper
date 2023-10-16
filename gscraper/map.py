from .types import _KT, _VT, _bool, _type, Comparable, Index, IndexLabel, Keyword, RegexFormat, TypeHint
from .types import Context, IndexedSequence, Records, MappingData, TabularData, Data
from .types import ApplyFunction, MatchFunction, BetweenRange, RenameDict
from .types import not_na, is_bool_array, is_int_array, is_array, is_2darray, is_records, is_dfarray
from .types import is_list_type, is_dict_type, is_records_type, is_dataframe_type

from .cast import cast_list, cast_tuple, cast_str

from typing import Any, Dict, List, Set
from typing import Iterable, Literal, Optional, Sequence, Tuple, Union
from itertools import chain
import functools
import pandas as pd
import random as rand
import re


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


def safe_apply(__object, __applyFunc: ApplyFunction, default=None, **context) -> Any:
    try: return __applyFunc(__object, **context)
    except: return default


def between(__object: Comparable, left=None, right=None,
            inclusive: Literal["both","neither","left","right"]="both", **kwargs) -> bool:
    match_left = ((left == None) or (__object >= left if inclusive in ["both","left"] else __object > left))
    match_right = ((right == None) or (__object <= right if inclusive in ["both","right"] else __object < right))
    return match_left & match_right


def map_index(__indices: Index) -> Index:
    if isinstance(__indices, int): return __indices
    elif is_int_array(__indices, how="all", empty=False): return __indices
    elif is_bool_array(__indices, how="all", empty=False):
        return [__i for __i, __match in enumerate(__indices) if __match]
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

def re_get(pattern: RegexFormat, string: str, default=str(), groups=False) -> str:
    if not re.search(pattern, string): return default
    catch = re.search(pattern, string).groups()
    return catch[0] if catch and not groups else catch


def replace_map(string: str, __m: dict) -> str:
    for __old, __new in __m.items():
        string = string.replace(__old, __new)
    return string


###################################################################
############################## Array ##############################
###################################################################

abs_idx = lambda idx: abs(idx+1) if idx < 0 else idx


def iloc(__s: IndexedSequence, __indices: Index, default=None, if_null: Literal["drop","pass"]="drop") -> _VT:
    __indices, __length = map_index(__indices), len(__s)
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
        return [__s.index(__v) for __v in values if __v in __s]
    else: return [(__s.index(__v) if __v in __s else default) for __v in values]


def get_scala(__object, index: Optional[int]=None, default=None, random=False) -> _VT:
    if isinstance(__object, Set): return __object.copy().pop()
    elif not is_array(__object): return __object
    elif __object and random: return rand.choice(__object)
    else: return iloc(__object, (index if isinstance(index, int) else 0), default=default)


def flatten(*args, iter_type: _type=(List,Set,Tuple)) -> List:
    return [__e for __object in args for __e in (
            __object if isinstance(__object, iter_type) else cast_tuple(__object))]


def unique(*elements, strict=True) -> List:
    array = list()
    for __e in elements:
        if not exists(__e, strict=strict): continue
        if __e not in array: array.append(__e)
    return array


def apply_array(__s: IndexedSequence, __indices: Optional[Index]=list(), __applyFunc: Optional[ApplyFunction]=list(),
                all_indices=False, apply: Optional[ApplyFunction]=None, __default=None, **context) -> IndexedSequence:
    __s = __s.copy()
    if all_indices: return [safe_apply(__e, apply) for __e in __s]
    for __i, __apply in map_context(__indices, __applyFunc, __default=apply, **context).items():
        if abs_idx(__i) < len(__s):
            __s[__i] = safe_apply(__s[__i], __apply)
    return __s


def match_array(__s: IndexedSequence, __indices: Optional[Index]=list(), __matchFunc: Optional[MatchFunction]=list(),
                all_indices=False, match: Optional[MatchFunction]=None, __default=None, **context) -> _bool:
    if all_indices: return all(map(match, __s))
    context = map_context(__indices, __matchFunc, __default=match, **context)
    return all([safe_apply(__s[__i], match, False) if abs_idx(__i) < len(__s) else False for __i, match in context.items()])


def values_to_back(__s: Sequence, values: _VT) -> List:
    values = cast_tuple(values)
    return sorted(__s, key=functools.cmp_to_key(lambda x, y: -1 if y in values else 0))


def fill_array(__s: Sequence, count: int, value=None) -> List:
    return [__s[__i] if __i < len(__s) else value for __i in range(count)]


def filter_array(__s: Sequence, match: MatchFunction, apply: Optional[ApplyFunction]=None) -> Sequence:
    __apply = apply if apply else (lambda x: x)
    return [__apply(__e) for __e in __s if match(__s)]


def is_same_length(*args: IndexedSequence, empty=True) -> bool:
    __length = set(map(len, args))
    return (len(__length) == 1) and (empty or (0 not in __length))


def unit_array(__s: Sequence, unit=1) -> List[Sequence]:
    return [__s[__i:__i+unit] for __i in range(0,len(__s),unit)]


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
                strict=False, dropna=False, unique=False) -> List[int]:
    count = len(args[0]) if how == "first" else (max(map(len, args)) if how == "max" else min(map(len, args)))
    indices = set(range(0, count))
    if dropna: indices = __and(indices, *map(set, map(lambda __s: get_exists_index(__s, strict=strict), args)))
    if unique: indices = __and(indices, *map(set, map(get_unique_index, args)))
    return sorted(indices)


def align_array(*args: Sequence, how: Literal["min","max","first"]="min", default=None,
                strict=False, dropna=False, unique=False) -> Tuple[List]:
    indices = align_index(*args, how=how, strict=strict, dropna=dropna, unique=unique)
    if dropna or unique:
        return tuple([__s[__i] for __i in indices if __i < len(__s)] for __s in args)
    else: return tuple([__s[__i] if __i < len(__s) else default for __i in indices] for __s in args)


###################################################################
############################### Map ###############################
###################################################################

def kloc(__m: Dict, __keys: _KT, default=None, if_null: Literal["drop","pass"]="drop",
        reorder=True, values_only=False) -> Union[_VT,Dict]:
    if not is_array(__keys): return __m.get(__keys, default)
    elif __keys:
        __keys = __keys if reorder else unique(*__m.keys(), *__keys)
        if if_null == "drop": __m = {__key: __m[__key] for __key in __keys if __key in __m}
        else: __m = {__key: __m.get(__key, default) for __key in __keys}
    else: __m = __m if if_null != "drop" else dict()
    return list(__m.values()) if values_only else __m


def to_dict(__object: MappingData) -> Dict:
    if isinstance(__object, Dict): return __object
    elif is_records(__object, empty=True): return chain_dict(__object, keep="first")
    elif isinstance(__object, pd.DataFrame): return __object.to_dict("dict")
    else: return dict()


def rename_dict(__m: Dict, rename: RenameDict) -> Dict:
    return {rename.get(__key,__key): __value for __key, __value in __m.items()}


def diff_dict(__m: Dict, **kwargs) -> Dict:
    return {key:value for key,value in __m.items() if key not in kwargs}


def chain_dict(__object: Sequence[Dict], keep: Literal["fist","last"]="first", empty=True) -> Dict:
    base = dict()
    for __m in __object:
        if not (empty or __m): continue
        base = dict(base, **(diff_dict(__m, **base) if keep == "first" else __m))
    return base


def apply_dict(__m: Dict, __keys: Optional[_KT]=list(), __applyFunc: Optional[ApplyFunction]=list(),
                all_keys=False, apply: Optional[ApplyFunction]=None, __default=None, **context) -> Dict:
    __m = __m.copy()
    if all_keys: return {__key: apply(__values) for __key, __values in __m.items()}
    for __key, __apply in map_context(__keys, __applyFunc, __default=apply, **context).items():
        if __key in __m:
            __m[__key] = safe_apply(__m[__key], __apply)
    return __m


def match_dict(__m: Dict, __keys: Optional[_KT]=list(), __matchFunc: Optional[MatchFunction]=list(),
                all_keys=False, match: Optional[MatchFunction]=None, __default=None, **context) -> bool:
    if all_keys: return all(map(match, __m.values()))
    context = map_context(__keys, __matchFunc, __default=match, **context)
    return all([safe_apply(__m[__k], match, False) if __k in __m else False for __k, match in context.items()])


def between_dict(__m: Dict, __keys: Optional[_KT]=list(), __ranges: Optional[BetweenRange]=list(),
                inclusive: Literal["both","neither","left","right"]="both", null=False, __default=None, **context) -> bool:
    match = True
    for __key, __range in map_context(__keys, __ranges, **context).items():
        if __key in __m:
            if is_array(__range): match &= between(__m[__key], *__range[:2], inclusive=inclusive)
            elif isinstance(__range, dict): match &= between(__m[__key], **__range, inclusive=inclusive)
            else: raise ValueError(BETWEEN_RANGE_TYPE_MSG)
        elif not null: return False
    return match


def keys_to_back(__m: Dict, __keys: _KT) -> Dict:
    __keys = cast_tuple(__keys)
    return dict(sorted(__m.items(), key=functools.cmp_to_key(lambda x, y: -1 if y[0] in __keys else 0)))


def set_dict(__m: Dict, __keys: _KT, values: _VT, empty=True, inplace=True) -> Dict:
    if not inplace: __m = __m.copy()
    for __key, value in zip(cast_tuple(__keys), cast_tuple(values)):
        if value or empty: __m[__key] = value
    if not inplace: return __m


def drop_dict(__m: Dict, __keys: _KT, inplace=False) -> Dict:
    if not inplace: __m = __m.copy()
    for __key in cast_tuple(__keys): __m.pop(__key, None)
    if not inplace: return __m


def hier_get(__m: Dict, __path: _KT, default=None, apply: Optional[ApplyFunction]=None,
            __type: Optional[_type]=None, empty=True, strict=True, **context) -> _VT:
    try:
        for __key in cast_tuple(__path):
            __m = __m[__key]
        value = safe_apply(__m, apply, default, **context) if apply else __m
        if __type and not isinstance(value, __type): return default
        return value if empty or exists(value, strict=strict) else default
    except: return default


def hier_set(__m: Dict, __path: _KT, value: _VT, empty=True, strict=True, inplace=True) -> Dict:
    if not inplace: __m = __m.copy()
    for __key in cast_tuple(__path)[:-1]:
        if empty or exists(value, strict=strict): __m = __m[__key]
    __m[__path[-1]] = value
    if not inplace: return __m


def hier_get_set(__m: Dict, __get_path: _KT, __set_path: _KT, default=None,
                apply: Optional[ApplyFunction]=None, __type: Optional[_type]=None,
                empty=True, strict=True, inplace=True, **context) -> Dict:
    value = hier_get(__m, __get_path, default, apply, __type, empty, strict, **context)
    if empty or exists(value, strict=strict):
        return hier_set(__m, __set_path, value, inplace=inplace)
    else: return __m


def align_dict(__m: Dict[_KT,Sequence], how: Literal["min","max"]="min", default=None,
                strict=False, dropna=False, unique=False) -> Dict[_KT,List]:
    if not __m: return dict()
    indices = align_index(*map(list, __m.values()), how=how, strict=strict, dropna=dropna, unique=False)
    if dropna or unique:
        return {__k: iloc(__v, indices, if_null="drop") for __k, __v in __m.items()}
    else: return {__k: iloc(__v, indices, default=default, if_null="pass") for __k, __v in __m.items()}


def match_keywords(__m: Dict, __keys: _KT, include: Optional[Keyword]=list(), exclude: Optional[Keyword]=list(),
                    how: Literal["any","all"]="any", if_null=False) -> bool:
    if not (include or exclude): return True
    include, exclude, matches = cast_tuple(include, strict=True), cast_tuple(exclude, strict=True), [False, False]

    for condition, keywords in enumerate([include, exclude]):
        if not keywords: continue
        pattern = re.compile('|'.join(map(re.escape, keywords)))
        match = tuple(map(pattern.search, map(cast_str, kloc(__m, __keys, values_only=True))))
        if len(match): matches[condition] = all(match) if how == "all" else any(match)
        else: matches[condition] = if_null
    return (matches[INCLUDE] or not include) and not (matches[EXCLUDE] and exclude)


###################################################################
############################# Records #############################
###################################################################

def vloc(__r: List[Dict], __keys: _KT, default=None, if_null: Literal["drop","pass"]="drop", reorder=True,
        values_only=False) -> Union[Records,List]:
    __r = [kloc(__m, __keys, default=default, if_null=if_null, reorder=reorder, values_only=values_only)
            for __m in __r]
    return [__m for __m in __r if __m] if if_null == "drop" else __r


def to_records(__object: MappingData) -> Records:
    if is_records(__object, empty=True): return __object
    elif isinstance(__object, pd.DataFrame): return __object.to_dict("records")
    elif isinstance(__object, dict): return [__object]
    else: return list()


def rename_records(__r: Records, rename: RenameDict) -> Records:
    if not rename: return __r
    return [rename_dict(__m, rename=rename) for __m in __r]


def apply_dict(__m: Dict, __keys: Optional[_KT]=list(), __applyFunc: Optional[ApplyFunction]=list(),
                all_keys=False, apply: Optional[ApplyFunction]=None, __default=None, **context) -> Dict:
    if all_keys: return {__key: apply(__values) for __key, __values in __m.items()}
    context = map_context(__keys, __applyFunc, __default=apply, **context)
    return {__k: safe_apply(__m[__k], apply) if __k in __m else None for __k, apply in context.items()}


def apply_records(__r: List[Dict], __keys: Optional[_KT]=list(), __applyFunc: Optional[ApplyFunction]=list(),
                    all_keys=False, apply: Optional[ApplyFunction]=None, __default=None, **context) -> Records:
    return [apply_dict(__m, __keys, __applyFunc, all_keys=all_keys, apply=apply, **context) for __m in __r]


def match_records(__r: List[Dict], __keys: Optional[_KT]=list(), __matchFunc: Optional[MatchFunction]=list(),
                    all_keys=False, match: Optional[MatchFunction]=None, __default=None, **context) -> Sequence[bool]:
    return [match_dict(__m, __keys, __matchFunc, all_keys=all_keys, match=match, **context) for __m in __r]


def between_records(__r: Records, __keys: Optional[_KT]=list(), __ranges: Optional[BetweenRange]=list(),
                    inclusive: Literal["both","neither","left","right"]="both", null=False,
                    __default=None, **context) -> Records:
    return [__m for __m in __r
            if between_dict(__m, __keys, __ranges, inclusive=inclusive, null=null, **context)]


def sort_records(__r: Records, by: _KT, ascending=True) -> Records:
    return sorted(__r, key=lambda x: kloc(x, cast_tuple(by), values_only=True), reverse=(not ascending))


def filter_records(__r: List[Dict], __keys: Optional[_KT]=list(), __matchFunc: Optional[MatchFunction]=list(),
                    all_keys=False, match: Optional[MatchFunction]=None, __default=None, **context) -> Sequence[bool]:
    return [__m for __m in __r if match_dict(__m, __keys, __matchFunc, all_keys=all_keys, match=match, **context)]


def isin_records(__r: Records, __keys: _KT, how: Literal["any","all"]="any") -> _bool:
    if not is_array(__keys):
        isin = [__keys in __m for __m in __r]
        return allin(isin) if how == "all" else any(isin)
    elif not __keys: return list()
    else: return [isin_records(__r, __key, how=how) for __key in __keys]


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


def concat_df(__object: Sequence[pd.DataFrame]) -> pd.DataFrame:
    __object = [df for df in __object if df_exists(df)]
    return pd.concat(__object) if __object else pd.DataFrame()


def apply_df(df: pd.DataFrame, __columns: Optional[IndexLabel]=list(), __applyFunc: Optional[ApplyFunction]=list(),
            all_cols=False, apply: Optional[ApplyFunction]=None, __default=None, **context) -> pd.DataFrame:
    df = df.copy()
    if all_cols: return df.apply(apply)
    context = map_context(__columns, __applyFunc, __default=apply, **context)
    context = {str(__column): __apply for __column, __apply in context.items() if __column in df}
    return df.apply(context)


def match_df(df: pd.DataFrame, __columns: Optional[IndexLabel]=list(), __matchFunc: Optional[MatchFunction]=list(),
            all_cols=False, match: Optional[MatchFunction]=None, __default=None, **context) -> pd.DataFrame:
    return df[apply_df(df, __columns, __matchFunc, all_cols=all_cols, match=match, **context).all(axis=1)]


def between_df(df: pd.DataFrame, __columns: Optional[_KT]=list(), __ranges: Optional[BetweenRange]=list(),
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
    get_values = lambda row: [row[value] for value in values]
    len_values = lambda row: min(map(len, get_values(row)))
    unroll_row = lambda row: [[row[col]]*len_values(row) for col in columns]+get_values(row)
    map_subrow = lambda subrow: {key:value for key, value in zip(columns+values,subrow)}
    map_row = lambda row: pd.DataFrame([map_subrow(subrow) for subrow in zip(*unroll_row(row))])
    return pd.concat([map_row(row) for _,row in df.iterrows()])


def round_df(df: pd.DataFrame, columns: IndexLabel, trunc=2) -> pd.DataFrame:
    if not isinstance(trunc, int): return df
    __round = lambda x: round(x,trunc) if isinstance(x,float) else x
    return apply_df(df, **{column: __round for column in cast_tuple(columns)})


###################################################################
############################ Multitype ############################
###################################################################

def exists_one(*args) -> Any:
    for arg in args:
        if arg: return arg
    return args[-1]


def filter_exists(__object, strict=False) -> Any:
    if is_array(__object):
        return type(__object)([value for value in __object if exists(value, strict=strict)])
    elif isinstance(__object, Dict):
        return {key:value for key,value in __object.items() if exists(value, strict=strict)}
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
def rename_data(data: MappingData, rename: RenameDict,
                return_type: Optional[TypeHint]=None, convert_first=False) -> MappingData:
    if not rename: return data
    elif is_records(data): return rename_records(data, rename)
    elif isinstance(data, pd.DataFrame): return data.rename(columns=rename)
    elif isinstance(data, Dict): return rename_dict(data, rename)
    else: return data


def multitype_rename(func):
    @functools.wraps(func)
    def wrapper(data: Data, *args, rename: RenameDict=dict(), rename_first=False, **kwargs):
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
                rename: RenameDict=dict(), convert_first=False, rename_first=False) -> Data:
    if not fields: return data if if_null != "drop" else list()
    if is_records(data): return vloc(data, __keys=fields, default=default, if_null=if_null, reorder=reorder)
    elif isinstance(data, pd.DataFrame): return cloc(data, columns=fields, default=default, if_null=if_null, reorder=reorder)
    elif isinstance(data, Dict): return kloc(data, __keys=fields, default=default, if_null=if_null, reorder=reorder)
    elif isinstance(data, List): return iloc(data, index=fields, default=default, if_null=if_null)
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
def chain_exists(data: Data, fields: Optional[Union[_KT,Index]]=list(), default=None,
                if_null: Literal["drop","pass"]="drop", reorder=True, return_type: Optional[TypeHint]=None,
                rename: RenameDict=dict(), convert_first=False, rename_first=False, filter_first=False) -> Data:
    if is_dfarray(data): return concat_df(data)
    elif is_2darray(data): return list(chain.from_iterable(data))
    else: return filter_exists(data)


@multitype_allowed
@multitype_rename
@multitype_filter
def apply_data(data: Data, __keys: Optional[Union[_KT,Index]]=list(), __applyFunc: Optional[ApplyFunction]=list(),
                all_keys=False, apply: Optional[ApplyFunction]=None, fields: Optional[Union[_KT,Index]]=list(),
                default=None, if_null: Literal["drop","pass"]="drop", reorder=True, return_type: Optional[TypeHint]=None,
                rename: RenameDict=dict(), convert_first=False, rename_first=False, filter_first=False, **context) -> Data:
    if is_records(data): return apply_records(data, __keys, __applyFunc, all_keys=all_keys, apply=apply, **context)
    elif isinstance(data, pd.DataFrame): return apply_df(data, __keys, __applyFunc, all_cols=all_keys, apply=apply, **context)
    elif isinstance(data, Dict): return apply_dict(data, __keys, __applyFunc, all_keys=all_keys, apply=apply, **context)
    elif isinstance(data, Sequence): return apply_array(data, __keys, __applyFunc, all_indices=all_keys, apply=apply, **context)
    else: return data


@multitype_allowed
@multitype_rename
@multitype_filter
def match_data(data: Data, __keys: Optional[Union[_KT,Index]]=list(), __matchFunc: Optional[MatchFunction]=list(),
                all_keys=False, match: Optional[MatchFunction]=None, fields: Optional[Union[_KT,Index]]=list(),
                default=None, if_null: Literal["drop","pass"]="drop", reorder=True, return_type: Optional[TypeHint]=None,
                rename: RenameDict=dict(), convert_first=False, rename_first=False, filter_first=False, **context) -> Data:
    if is_records(data): return is_records_type(data, __keys, __matchFunc, all_keys=all_keys, match=match, **context)
    elif isinstance(data, pd.DataFrame): return match_df(data, __keys, __matchFunc, all_cols=all_keys, match=match, **context)
    elif isinstance(data, Dict): return match_dict(data, __keys, __matchFunc, all_keys=all_keys, match=match, **context)
    elif isinstance(data, List): return match_array(data, __keys, __matchFunc, all_indices=all_keys, match=match, **context)
    else: return data


@multitype_allowed
@multitype_rename
@multitype_filter
def between_data(data: MappingData, inclusive: Literal["both","neither","left","right"]="both", null=False,
                fields: Optional[Union[_KT,Index]]=list(), default=None, if_null: Literal["drop","pass"]="drop",
                reorder=True, return_type: Optional[TypeHint]=None, rename: RenameDict=dict(), convert_first=False,
                rename_first=False, filter_first=False, **context) -> MappingData:
    if is_records(data): return between_records(data, inclusive=inclusive, null=null, **context)
    elif isinstance(data, pd.DataFrame): return between_df(data, inclusive=inclusive, null=null)
    elif isinstance(data, Dict): return data if between_dict(data, inclusive=inclusive, null=null) else dict()
    else: return data


@multitype_allowed
@multitype_rename
@multitype_filter
def sort_values(data: TabularData, by: _KT, ascending: _bool=True, fields: Optional[Union[_KT,Index]]=list(),
                default=None, if_null: Literal["drop","pass"]="drop", reorder=True, return_type: Optional[TypeHint]=None,
                rename: RenameDict=dict(), convert_first=False, rename_first=False, filter_first=False, **context) -> Data:
    if is_records(data):
        ascending = bool(iloc(ascending, 0)) if isinstance(ascending, Sequence) else ascending
        return sort_records(data, by=by, ascending=ascending)
    elif isinstance(data, pd.DataFrame): return data.sort_values(by, ascending=ascending)
    elif isinstance(data, Dict): return {__key: data[__key] for __key in sorted(data.keys())}
    elif isinstance(data, List): return sorted(data)
    else: return data
