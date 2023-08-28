from .cast import cast_list, cast_tuple, cast_str
from .types import _KT, _VT, _bool, _type, Comparable, Index, IndexLabel, Keyword, RegexFormat, TypeHint
from .types import Context, NestedSequence, IndexedSequence, Records, TabularData, MappingData, Data
from .types import ApplyFunction, MatchFunction, BetweenRange
from .types import not_na, is_bool_array, is_int_array, is_array, is_2darray, is_records, is_dfarray
from .types import is_list_type, is_dict_type, is_records_type, is_dataframe_type

from typing import Any, Dict, List, Set
from typing import Callable, Iterable, Optional, Sequence, Tuple, Type, Union
from itertools import chain
from functools import cmp_to_key, reduce
import pandas as pd
import random as rand
import re


INCLUDE = 0
EXCLUDE = 1

__or = lambda *args: reduce(lambda x,y: x|y, args)
__and = lambda *args: reduce(lambda x,y: x&y, args)
union = lambda *arrays: reduce(lambda x,y: x+y, arrays)
inter = lambda *arrays: reduce(lambda x,y: [e for e in x if e in y], arrays)
diff = lambda *arrays: reduce(lambda x,y: [e for e in x if e not in y], arrays)


def __apply(__object, __applyFunc: ApplyFunction, default=None, **context) -> Any:
    try: return __applyFunc(__object, **context)
    except: return default


def allin(__object: Iterable) -> bool:
    return bool(tuple(__object)) and all(__object)


def exists(__object, strict=False) -> bool:
    if is_array(__object): return len([__e for __e in __object if not_na(__e, strict=strict)]) > 0
    else: return not_na(__object, strict=strict)


def is_empty(__object, strict=False) -> bool:
    return (not exists(__object, strict=strict))


def str_na(__object, strict=False) -> str:
    return str(__object) if exists(__object, strict=strict) else str()


def df_exists(df: pd.DataFrame, drop_na=False, how="all") -> bool:
    if not isinstance(df, pd.DataFrame): return False
    return not (df.dropna(axis=0, how=how) if drop_na else df).empty


def df_empty(df: pd.DataFrame, drop_na=True, how="all") -> bool:
    if not isinstance(df, pd.DataFrame): return False
    return (df.dropna(axis=0, how=how) if drop_na else df).empty


def data_exists(data: Data, drop_na=True, how="all") -> bool:
    return df_exists(data, drop_na, how) if isinstance(data, pd.DataFrame) else bool(data)


def data_empty(data: Data, drop_na=True, how="all") -> bool:
    return df_empty(data, drop_na, how) if isinstance(data, pd.DataFrame) else (not data)


INCLUSIVE = ("both", "neither", "left", "right")

def between(__object: Comparable, left=None, right=None, inclusive="both", **kwargs) -> bool:
    match_left = ((left == None) or (__object >= left if inclusive in ["both","left"] else __object > left))
    match_right = ((right == None) or (__object <= right if inclusive in ["both","right"] else __object < right))
    return match_left & match_right


def map_context(__keys: _KT, __values: _VT, **context) -> Context:
    if context: return context
    if not (__keys and __values): return dict()
    __keys = cast_tuple(__keys)
    if not is_array(__values): __values = (__values,)*len(__keys)
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


def iloc(__s: IndexedSequence, index: Index, default=None, if_null="drop",
        apply: Optional[ApplyFunction]=None) -> _VT:
    length = len(__s)
    _apply = apply if isinstance(apply, Callable) else (lambda x: x)
    if isinstance(index, int):
        return _apply(__s[index]) if abs_idx(index) < length else default
    elif not is_int_array(index, how="all", empty=False):
        if is_array(index): return __s if if_null != "drop" else list()
        else: return __s if if_null != "drop" else default
    elif if_null == "drop": return [_apply(__s[i]) for i in index if abs_idx(i) < length]
    else: return [_apply(__s[i]) if abs_idx(i) < length else default for i in index]


def flatten(*args, iter_type: Tuple[_type]=(List,Set,Tuple)) -> List:
    return [__e for __object in args for __e in (
            __object if isinstance(__object, iter_type) else cast_tuple(__object))]


def unique(*elements, strict=True) -> List:
    array = list()
    for element in elements:
        if not exists(element, strict=strict): continue
        if element not in array: array.append(element)
    return array


def apply_array(__s: IndexedSequence, __indices: Optional[Index]=list(), __applyFunc: Optional[ApplyFunction]=list(),
                all_indices=False, apply: Optional[ApplyFunction]=None, **context) -> IndexedSequence:
    __s = __s.copy()
    if all_indices:
        if not isinstance(apply, Callable): return __s
        else: return [apply(__e) for __e in __s]
    for __index, applyFunc in map_context(__indices, __applyFunc, **context).items():
        if isinstance(applyFunc, Callable): continue
        elif not isinstance(__index, int): __s = applyFunc(__s)
        elif abs_idx(__index) not in __s: continue
        else: __s[__index] = applyFunc(__s[__index])
    return __s


def match_array(__s: IndexedSequence, __indices: Optional[Index]=list(), __matchFunc: Optional[ApplyFunction]=list(),
                all_indices=False, **context) -> bool:
    if all_indices:
        if not isinstance(__matchFunc, Callable): return False
        else: return __and(map(__matchFunc, __s))
    match = True
    for __index, matchFunc in map_context(__indices, __matchFunc, **context).items():
        if not isinstance(matchFunc, Callable): continue
        elif not isinstance(__index, int): match &= matchFunc(__s)
        elif abs_idx(__index) not in __s: continue
        else: match &= matchFunc(__s[__index])
    return match


def chain_array(__s: NestedSequence, empty=True) -> List:
    if not __s: return list()
    elif not empty: __s = [__e for __e in __s if exists(__e)]
    return list(chain.from_iterable(__s))


def get_index(__s: IndexedSequence, values: _VT, default=None, if_null="drop",
                apply: Optional[ApplyFunction]=None, multiple=True) -> Index:
    _apply = apply if isinstance(apply, Callable) else (lambda x: x)
    if not (is_array(values) and multiple):
        return _apply(__s.index(values)) if values in __s else default
    elif not values: return _apply(__s) if if_null != "drop" else list()
    elif if_null == "drop": return [_apply(__s.index(value)) for value in values if value in __s]
    else: return [_apply(__s.index(value)) if value in __s else default for value in values]


def get_scala(__object, index: Optional[int]=None, default=None, random=False) -> _VT:
    if isinstance(__object, Set): return __object.copy().pop()
    elif not is_array(__object): return __object
    elif __object and random: return rand.choice(__object)
    else: return iloc(__object, (index if isinstance(index, int) else 0), default=default)


def values_to_back(__s: Sequence, values: _VT) -> List:
    values = cast_tuple(values)
    return sorted(__s, key=cmp_to_key(lambda x, y: -1 if y in values else 0))


def fill_array(__s: Sequence, count: int, value=None) -> List:
    return [__s[i] if i < len(__s) else value for i in range(count)]


def is_same_length(*args: IndexedSequence, empty=True) -> bool:
    __l = set()
    for __s in args:
        if not is_array(__s): return False
        elif not __s and not empty: return False
        else: __l.add(len(__s))
    return len(__l) <= 1


def filter_array(__s: Sequence, match: Union[Sequence[bool],MatchFunction],
                apply: Optional[ApplyFunction]=None) -> List:
    _apply = apply if apply else (lambda x: x)
    if is_bool_array(match, how="all", empty=False):
        if is_same_length(__s, match, empty=False):
            __s = [_apply(__e) for __e, __match in zip(__s, match) if __match]
        else: return list()
    else: return [_apply(__e) for __e in __s if match(__e)]


def unit_array(__s: Sequence, unit=1) -> List[Sequence]:
    return [__s[i:i+unit] for i in range(0,len(__s),unit)]


def align_index(*__s: Sequence, how="all", unique=False, strict=False) -> List[int]:
    __iter, __set, count = __s, [set() for _ in __s], 0
    is_valid = not_na if strict else exists
    if len(__iter) > 0 and how == "all":
        count = min([len(array) for array in __iter])
    elif len(__iter) > 0 and how == "first":
        count = len(__iter[0])
        __iter, __set = [__s[0]], [set()]
    if unique:
        index = [set() for _ in __iter]
        for cur, array in enumerate(__iter):
            for idx, __e in enumerate(array):
                if is_empty(__e) or (__e in __set[cur]): continue
                index[cur].add(idx)
                __set[cur].add(__e)
        return sorted(__and(*index))
    else: return [idx for idx in range(count) if all([is_valid(array[idx]) for array in __iter])]


def align_array(*__s: Sequence, how="all", default: Optional[Union[Any,Sequence]]=None,
                empty=True, unique=False, strict=False) -> Tuple[List]:
    is_valid = not_na if strict else exists
    get_default = (lambda idx: iloc(default, idx)) if is_array(default) and default else (lambda _: default)
    if len(__s) == 1:
        return __s[0] if empty else filter_array(__s[0], is_valid)
    elif len(__s) > 1 and how == "all" and empty:
        return tuple(fill_array(array, max([len(array) for array in __s]), get_default(i)) for i, array in enumerate(__s))
    elif len(__s) > 1 and how == "first" and empty:
        return tuple([__s[0]]+[fill_array(array, len(__s[0]), get_default(i)) for i, array in enumerate(__s[1:])])
    elif len(__s) > 1 and how == "dropna":
        if len(set(map(len,__s))) != 1: return __s
        matches = [idx for idx in range(len(__s[0])) if any(map(lambda x: is_valid(x[idx]), __s))]
        return tuple([array[:max(matches)+1] if matches else list() for array in __s])
    valid = align_index(*__s, how=how, unique=unique, strict=strict)
    return tuple([array[idx] if idx < len(array) else get_default(i) for idx in valid] for i, array in enumerate(__s))


###################################################################
############################### Map ###############################
###################################################################

def kloc(__m: Dict, __keys: _KT, default=None, if_null="drop", values_only=False,
        apply: Optional[ApplyFunction]=None) -> Union[_VT,Dict]:
    if isinstance(apply, Dict): _apply = (lambda x: apply[x](x) if isinstance(apply.get(x), Callable) else x)
    else: _apply = apply if isinstance(apply, Callable) else (lambda x: x)
    if not is_array(__keys): return _apply(__m.get(__keys, default))
    elif __keys and values_only:
        if if_null == "drop": return [_apply(__m[key]) for key in __keys if key in __m]
        else: return [_apply(__m.get(key, default)) for key in __keys]
    elif __keys and not values_only:
        if if_null == "drop": return {key:_apply(__m[key]) for key in __keys if key in __m}
        else: return {key:_apply(__m.get(key, default)) for key in __keys}
    else: return __m if if_null != "drop" else dict()


def apply_dict(__m: Dict, __keys: Optional[_KT]=list(), __applyFunc: Optional[ApplyFunction]=list(),
                all_keys=False, apply: Optional[ApplyFunction]=None, **context) -> Dict:
    __m = __m.copy()
    if all_keys:
        if not isinstance(apply, Callable): return __m
        return {__key:apply(__values) for __key, __values in __m.items()}
    for __key, applyFunc in map_context(__keys, __applyFunc, **context).items():
        if not isinstance(applyFunc, Callable): continue
        elif not __key: __m = applyFunc(__m)
        elif __key not in __m: continue
        else: __m[__key] = applyFunc(__m[__key])
    return __m


def match_dict(__m: Dict, __keys: Optional[_KT]=list(), __matchFunc: Optional[MatchFunction]=list(),
                all_keys=False, match: Optional[MatchFunction]=None, **context) -> bool:
    if all_keys:
        if not isinstance(match, Callable): return False
        else: return __and(map(match, __m.values()))
    match = True
    for __key, matchFunc in map_context(__keys, __matchFunc, **context).items():
        if not isinstance(matchFunc, Callable): continue
        elif not __key: match &= matchFunc(__m)
        elif __key not in __m: continue
        else: match &= matchFunc(__m[__key])
    return match


def diff_dict(__m: Dict, **kwargs) -> Dict:
    return {key:value for key,value in __m.items() if key not in kwargs}


def chain_dict(__object: Sequence[Dict], keep="first", empty=True) -> Dict:
    base = dict()
    for __m in __object:
        if not (empty or __m): continue
        base = dict(base, **(diff_dict(__m, **base) if keep == "first" else __m))
    return base


def to_dict(__object: MappingData) -> Dict:
    if isinstance(__object, Dict): return __object
    elif is_records(__object, empty=True): return chain_dict(__object, keep="first")
    elif isinstance(__object, pd.DataFrame): return __object.to_dict("dict")
    else: return dict()


def keys_to_back(__m: Dict, __keys: _KT) -> Dict:
    __keys = cast_tuple(__keys)
    return dict(sorted(__m.items(), key=cmp_to_key(lambda x, y: -1 if y[0] in __keys else 0)))


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
        for key in cast_tuple(__path):
            __m = __m[key]
        value = __apply(__m, apply, default, **context) if apply else __m
        if __type and not isinstance(value, __type): return default
        return value if empty or (not_na(value) if strict else exists(value)) else default
    except: return default


def hier_set(__m: Dict, __path: _KT, value: _VT, empty=True, strict=True, inplace=True) -> Dict:
    if not inplace: __m = __m.copy()
    for key in cast_tuple(__path)[:-1]:
        if empty or (not_na(value) if strict else exists(value)): __m = __m[key]
    __m[__path[-1]] = value
    if not inplace: return __m


def hier_get_set(__m: Dict, __get_path: _KT, __set_path: _KT, default=None,
                apply: Optional[ApplyFunction]=None, __type: Optional[_type]=None,
                empty=True, strict=True, inplace=True, **context) -> Dict:
    value = hier_get(__m, __get_path, default, apply, __type, empty, strict, **context)
    if empty or (not_na(value) if strict else exists(value)):
        return hier_set(__m, __set_path, value, inplace=inplace)
    else: return __m


def align_dict(__m: Dict[_KT,Sequence], default=None, empty=True) -> Dict[_KT,List]:
    if not __m: return dict()
    elif empty:
        count = max([len(values) for _, values in __m.items()])
        return {key: fill_array(values, count=count, value=default) for key, values in __m.items()}
    else:
        valid = __and(*[{i for i, value in enumerate(values) if value} for key, values in __m.items()])
        return {key: [values[i] for i in valid] for key, values in __m.items()}


def match_keywords(__m: Dict, __keys: _KT, include: Optional[Keyword]=list(), exclude: Optional[Keyword]=list(),
                    how="any", if_null=False) -> bool:
    if not (include or exclude): return True
    include, exclude, matches = cast_tuple(include, strict=True), cast_tuple(exclude, strict=True), [False, False]
    for condition, keywords in enumerate([include, exclude]):
        if not keywords: continue
        pattern = re.compile('|'.join(map(re.escape, keywords)))
        match = tuple(map(pattern.search, map(cast_str, kloc(__m, __keys, values_only=True))))
        if len(match): matches[condition] = any(match) if how == "any" else all(match)
        else: matches[condition] = if_null
    return (matches[INCLUDE] or not include) and not (matches[EXCLUDE] and exclude)


###################################################################
############################# Records #############################
###################################################################

def vloc(__r: List[Dict], __keys: _KT, default=None, if_null="drop", values_only=False,
        apply: Optional[ApplyFunction]=None) -> Union[Records,List]:
    base, __keys = list(), cast_tuple(__keys)
    for __m in __r:
        values = kloc(__m, __keys, default=default, if_null=if_null, values_only=values_only, apply=apply)
        if not values and if_null == "drop": continue
        else: base.append(values)
    return base


def isin_records(__r: Records, __keys: _KT, how="any") -> _bool:
    if not is_array(__keys):
        isin = [__keys in __m for __m in __r]
        return allin(isin) if how == "all" else any(isin)
    elif not __keys: return [True]*len(__r)
    else: return [isin_records(__r, __key, how=how) for __key in __keys if not is_array(__key)]


def to_records(__object: MappingData) -> Records:
    if is_records(__object, empty=True): return __object
    elif isinstance(__object, pd.DataFrame): return __object.to_dict("records")
    elif isinstance(__object, dict): return [__object]
    else: return list()


def apply_records(__r: List[Dict], __keys: Optional[_KT]=list(), __applyFunc: Optional[ApplyFunction]=list(),
                    all_keys=False, apply: Optional[ApplyFunction]=None, **context) -> Records:
    __r = __r.copy()
    if all_keys:
        if not isinstance(apply, Callable): return __r
        else: return [{__key:apply(__values) for __key, __values in __m.items()} for __m in __r]
    for idx, __m in enumerate(__r.copy()):
        for __key, applyFunc in map_context(__keys, __applyFunc, **context).items():
            if not isinstance(applyFunc, Callable): continue
            elif not __key: __r[idx] = applyFunc(__m)
            elif __key not in __m: continue
            else: __r[idx][__key] = applyFunc(__m[__key])
    return __r


def match_records(__r: List[Dict], __keys: Optional[_KT]=list(), __matchFunc: Optional[MatchFunction]=list(),
                    all_keys=False, match: Optional[MatchFunction]=None, **context) -> Sequence[bool]:
    __s = list()
    if all_keys:
        if not isinstance(match, Callable): return [False]*len(__r)
        else: return [__and(map(match, __m.values())) for __m in __r]
    context = map_context(__keys, __matchFunc, **context)
    context = {__key: __value for __key, __value in context.items() if isin_records(__r, __key)}
    for __m in __r:
        match = True
        for __key, matchFunc in context.items():
            if not isinstance(matchFunc, Callable): continue
            elif not __key: match &= matchFunc(__m)
            elif __key not in __m: continue
            else: match &= matchFunc(__m[__key])
        __s.append(__m)
    return __s


def between_records(__r: Records, __keys: Optional[_KT]=list(), __ranges: Optional[BetweenRange]=list(),
                    inclusive="both", if_null="drop", **context) -> Records:
    __s = list()
    for __m in __r:
        match = True
        for __key, __range in map_context(__keys, __ranges, **context).items():
            if __key in __m:
                if is_array(__range): match &= between(__m[__key], *__range[:2], inclusive=inclusive)
                elif isinstance(__range, dict): match &= between(__m[__key], **__range, inclusive=inclusive)
                else: raise ValueError("Between condition must be an iterable or a dictionary")
            elif if_null == "drop":
                match = False
                break
        if match: __s.append(__m)
    return __s


def filter_records(__r: Records, match: Union[Sequence[bool],MatchFunction],
                    apply: Optional[ApplyFunction]=None) -> Records:
    if isinstance(apply, Dict): _apply = lambda x: apply_dict(x, **apply)
    else: _apply = apply if isinstance(apply, Callable) else (lambda x: x)
    if is_bool_array(match, how="all", empty=False):
        if is_same_length(__r, match, empty=False):
            return [_apply(__m) for __m, __match in zip(__r, match) if __match]
        else: return list()
    else: return [_apply(__m) for __m in __r if match(__m)]


def sort_records(__r: Records, by: _KT, ascending=True) -> Records:
    return sorted(__r, key=lambda x: kloc(x, cast_tuple(by), values_only=True), reverse=(not ascending))


###################################################################
############################ DataFrame ############################
###################################################################

def cloc(df: pd.DataFrame, columns: IndexLabel, default=None, if_null="drop", reorder=True,
        apply: Optional[ApplyFunction]=None) -> pd.DataFrame:
    columns = cast_tuple(columns)
    df = df[[inter(columns, df.columns) if reorder else inter(df.columns, columns)]]
    if (len(df.columns) != len(columns)) and if_null != "drop":
        if reorder: df = pd.concat([pd.DataFrame(columns=columns),df])
        else: df = pd.concat([pd.DataFrame(columns=unique(*df.columns,*columns)),df])
    if isinstance(apply, (str,Type,Dict)): df = df.astype(apply)
    elif isinstance(apply, Callable): df = apply_df(df, all_cols=True, apply=apply)
    return df


def to_dataframe(__object: MappingData) -> pd.DataFrame:
    if isinstance(__object, pd.DataFrame): return __object
    elif is_records(__object, empty=True): return pd.DataFrame(__object)
    elif isinstance(__object, dict): return pd.DataFrame([__object])
    else: return pd.DataFrame()


def apply_df(df: pd.DataFrame, __columns: Optional[IndexLabel]=list(), __applyFunc: Optional[ApplyFunction]=list(),
            all_cols=False, apply: Optional[ApplyFunction]=None, **context) -> pd.DataFrame:
    df = df.copy()
    if all_cols:
        if not isinstance(apply, Callable): return df
        for column in df.columns:
            df[column] = df[column].apply(apply)
        return df
    for column, applyFunc in map_context(__columns, __applyFunc, **context).items():
        if not (isinstance(column, str) and isinstance(applyFunc, Callable)): continue
        elif not column: df = df.apply(applyFunc, axis=1)
        elif column not in df: continue
        else: df[column] = df[column].apply(applyFunc)
    return df


def match_df(df: pd.DataFrame, __columns: Optional[IndexLabel]=list(), __matchFunc: Optional[MatchFunction]=list(),
            all_cols=False, match: Optional[MatchFunction]=None, **context) -> pd.DataFrame:
    df, match = df.copy(), pd.Series([True]*len(df), index=df.index)
    if all_cols:
        if not isinstance(match, Callable): pd.Series([False]*len(df), index=df.index)
        for column in df.columns:
            match &= df[column].apply(match)
        return df[match]
    for column, matchFunc in map_context(__columns, __matchFunc, **context).items():
        if not (isinstance(column, str) and isinstance(matchFunc, Callable)): continue
        elif not column: match &= df.apply(matchFunc, axis=1)
        elif column not in df: continue
        else: match &= df[column].apply(matchFunc)
    return df[match]


def concat_df(__object: Sequence[pd.DataFrame]) -> pd.DataFrame:
    __object = [df for df in __object if df_exists(df)]
    return pd.concat(__object) if __object else pd.DataFrame()


def between_df(df: pd.DataFrame, inclusive="both", if_null="drop", **context) -> pd.DataFrame:
    df, kwargs, default = df.copy(), {"inclusive":inclusive}, pd.Series([False]*len(df), index=df.index)
    for column, args in context.items():
        if column in df:
            if_na = default if if_null == "drop" else df[column].isna()
            if is_array(args):
                df = df[df[column].apply(lambda x: between(x, *args[:2], **kwargs))|if_na]
            elif isinstance(args, dict):
                df = df[df[column].apply(lambda x: between(x, **args, **kwargs))|if_na]
            else: raise ValueError("Between condition must be an iterable or a dictionary")
    return df


def merge_drop(left: pd.DataFrame, right: pd.DataFrame, drop="right", how="inner",
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
    roundFunc = lambda x: round(x,trunc) if isinstance(x,float) else x
    return apply_df(df, **{column:roundFunc for column in cast_tuple(columns)})


###################################################################
############################ Multitype ############################
###################################################################

def exists_one(*args) -> Any:
    for arg in args:
        if arg: return arg
    return args[-1]


def filter_exists(__object, strict=False) -> Any:
    is_valid = not_na if strict else exists
    if is_array(__object):
        return type(__object)([value for value in __object if is_valid(value)])
    elif isinstance(__object, Dict):
        return {key:value for key,value in __object.items() if is_valid(value)}
    elif isinstance(__object, pd.DataFrame):
        return __object.dropna(axis=1, how="any")
    elif is_valid(__object): return __object


def convert_data(data: Data, return_type: Optional[TypeHint]=None) -> Data:
    if not return_type: return data
    elif is_records_type(return_type): return to_records(data)
    elif is_dataframe_type(return_type): return to_dataframe(data)
    elif is_dict_type(return_type): return to_dict(data)
    elif is_list_type(return_type): return cast_list(data)
    else: return data


def chain_exists(data: Data, return_type: Optional[TypeHint]=None) -> Data:
    if is_dfarray(data): data = concat_df(data)
    elif is_2darray(data): data = chain_array(data, empty=False)
    else: data = filter_exists(data)
    return convert_data(data, return_type)


def filter_data(data: Data, filter: Optional[Union[_KT,Index]]=list(), default=None,
                if_null="drop", apply: Optional[ApplyFunction]=None, return_type: Optional[TypeHint]=None) -> Data:
    if not filter: return convert_data(data, return_type)
    filter = cast_tuple(filter)
    if is_records(data): data = vloc(data, __keys=filter, default=default, if_null=if_null, apply=apply)
    elif isinstance(data, pd.DataFrame): data = cloc(data, columns=filter, default=default, if_null=if_null, apply=apply)
    elif isinstance(data, Dict): data = kloc(data, __keys=filter, default=default, if_null=if_null, apply=apply)
    elif isinstance(data, List): data = iloc(data, index=filter, default=default, if_null=if_null, apply=apply)
    return convert_data(data, return_type)


def apply_data(data: Data, __keys: Optional[Union[_KT,Index]]=list(), __applyFunc: Optional[ApplyFunction]=list(),
                all_keys=False, apply: Optional[ApplyFunction]=None, return_type: Optional[TypeHint]=None, **context) -> Data:
    if is_records(data): data = apply_records(data, __keys, __applyFunc, all_keys=all_keys, apply=apply, **context)
    elif isinstance(data, pd.DataFrame): data = apply_df(data, __keys, __applyFunc, all_cols=all_keys, apply=apply, **context)
    elif isinstance(data, Dict): data = apply_dict(data, __keys, __applyFunc, all_keys=all_keys, apply=apply, **context)
    elif isinstance(data, List): data = apply_array(data, __keys, __applyFunc, all_indices=all_keys, apply=apply, **context)
    return convert_data(data, return_type)


def match_data(data: Data, __keys: Optional[Union[_KT,Index]]=list(), __matchFunc: Optional[MatchFunction]=list(),
                all_keys=False, match: Optional[MatchFunction]=None, return_type: Optional[TypeHint]=None, **context) -> Data:
    if is_records(data): data = is_records_type(data, __keys, __matchFunc, all_keys=all_keys, match=match, **context)
    elif isinstance(data, pd.DataFrame): data = match_df(data, __keys, __matchFunc, all_cols=all_keys, match=match, **context)
    elif isinstance(data, Dict): data = match_dict(data, __keys, __matchFunc, all_keys=all_keys, match=match, **context)
    elif isinstance(data, List): data = match_array(data, __keys, __matchFunc, all_indices=all_keys, match=match, **context)
    return convert_data(data, return_type)


def between_data(data: TabularData, inclusive="both", if_null="drop",
                return_type: Optional[TypeHint]=None, **kwargs) -> TabularData:
    if is_records(data): data = between_records(data, inclusive=inclusive, if_null=if_null, **kwargs)
    elif isinstance(data, pd.DataFrame): data = between_df(data, inclusive=inclusive, if_null=if_null)
    return convert_data(data, return_type)


def sort_values(data: TabularData, by: _KT, ascending: _bool=True, return_type: Optional[TypeHint]=None) -> TabularData:
    if is_records(data):
        ascending = bool(iloc(ascending, 0)) if isinstance(ascending, Sequence) else ascending
        data = sort_records(data, by=by, ascending=ascending)
    elif isinstance(data, pd.DataFrame): data = data.sort_values(by, ascending=ascending)
    return convert_data(data, return_type)
