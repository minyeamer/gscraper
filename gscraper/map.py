from .cast import cast_list, cast_str

from typing import Callable, Iterable, Optional, TypeVar, Union
from typing import Any, Dict, Hashable, List, Sequence, Tuple
from itertools import chain
from functools import cmp_to_key, reduce
import datetime as dt
import pandas as pd
import re

_KT = TypeVar("_KT")
_VT = TypeVar("_VT")
IndexLabel = Union[Hashable, Sequence[Hashable]]

_GTE = TypeVar("_GTE")
_LTE = TypeVar("_LTE")

is_na = lambda s: ((not isinstance(s, float) and not s) or (isinstance(s, float) and pd.isna(s)))
not_na = lambda s: ((not isinstance(s, float) and bool(s)) or (isinstance(s, float) and pd.notna(s)))
str_na = lambda s: str(s) if not_na(s) else str()

is_df = lambda data: isinstance(data, pd.DataFrame)
is_records = lambda data: isinstance(data, List) and isinstance(index_get(data, 0, default=dict()), Dict)

exist = lambda data: not data.empty if isinstance(data, pd.DataFrame) else bool(data)
is_empty = lambda data: data.empty if isinstance(data, pd.DataFrame) else not data
df_exist = lambda df: not df.empty if isinstance(df, pd.DataFrame) else False
df_empty = lambda df: df.empty if isinstance(df, pd.DataFrame) else False

union = lambda *__s: reduce(lambda x,y: x|y, __s)
inter = lambda *__s: reduce(lambda x,y: x&y, __s)
values_to_back = lambda array, values: sorted(array, key=cmp_to_key(lambda x, y: -1 if y in values else 0))
keys_to_back = lambda __m, keys: dict(sorted(__m.items(), key=cmp_to_key(lambda x, y: -1 if y[0] in keys else 0)))
dateftime = lambda time: dt.datetime(*time.timetuple()[:6]).date() if isinstance(time, dt.datetime) else time


def exists_one(*args, **kwargs) -> Any:
    for arg in args:
        if arg: return arg
    return args[-1]


def unique(*elements, empty=False, **kwargs) -> List:
    array = list()
    for element in elements:
        if not (empty or not_na(element)): continue
        if element not in array: array.append(element)
    return array


def filter_exists(__object, **kwargs) -> Any:
    if isinstance(__object, list):
        return [value for value in __object if not_na(value)]
    elif isinstance(__object, dict):
        return {key:value for key,value in __object.items() if not_na(value)}
    elif isinstance(__object, pd.DataFrame):
        return __object.dropna(axis=1, how="any")
    elif isinstance(__object, set):
        return {value for value in __object if not_na(value)}
    elif isinstance(__object, Iterable):
        return [value for value in __object if not_na(value)]
    elif not_na(__object): return __object


###################################################################
############################## Array ##############################
###################################################################

ITERABLE = (list, set, tuple)

is_2darray = lambda array: isinstance(array, ITERABLE) and array and isinstance(array[0], ITERABLE)
iloc = lambda array, index: [array[i] for i in (range(index) if isinstance(index, int) else index) if i < len(array)]


def chain_list(iterable: Iterable[Iterable], empty=True, **kwargs) -> List:
    if not iterable: return list()
    elif not empty: iterable = [array for array in iterable if not_na(array)]
    return list(chain.from_iterable(iterable))


def chain_exists(iterable: Iterable[Iterable], **kwargs) -> List:
    return chain_list(iterable, empty=False, **kwargs) if is_2darray(iterable) else filter_exists(iterable)


def match_list(iterable: Iterable[Iterable], how="all", value=None, empty=True, unique=False, **kwargs) -> Tuple[List]:
    if len(iterable) == 1:
        return fill_empty(iterable[0], value) if empty else filter_array(iterable[0], not_na)
    elif len(iterable) > 1 and how == "all" and empty:
        return tuple(fill_array(array, max([len(array) for array in iterable]), value) for array in iterable)
    elif len(iterable) > 1 and how == "first" and empty:
        return tuple([iterable[0]]+[fill_array(array, len(iterable[0]), value) for array in iterable[1:]])
    elif len(iterable) > 1 and how == "dropna":
        if len(set(map(len,iterable))) != 1: return iterable
        matches = [idx for idx in range(len(iterable[0])) if True in map(lambda x: not_na(x[idx]), iterable)]
        return tuple([array[:max(matches)+1] if matches else list() for array in iterable])
    valid = match_index(iterable, how=how, unique=unique)
    return tuple([array[idx] if idx < len(array) else value for idx in valid] for array in iterable)


def match_index(iterable: Iterable[Iterable], how="all", unique=False) -> List[int]:
    __l, __s, count = iterable, [set() for _ in iterable], 0
    if len(__l) > 0 and how == "all":
        count = min([len(array) for array in __l])
    elif len(__l) > 0 and how == "first":
        count = len(__l[0])
        __l, __s = [iterable[0]], [set()]
    if unique:
        index = [set() for _ in __l]
        for cur, array in enumerate(__l):
            for idx, element in enumerate(array):
                if is_na(element) or element in __s[cur]: continue
                index[cur].add(idx)
                __s[cur].add(element)
        return sorted(inter(*index))
    else: return [idx for idx in range(count) if False not in [not_na(array[idx]) for array in __l]]


def match_keywords(__m: Dict, keys: List[str], include=list(), exclude=list(), **kwargs) -> bool:
    if not keys or not (include or exclude): return True
    INCLUDE, EXCLUDE = 0, 1
    include = include if include else list()
    exclude = exclude if exclude else list()
    match = [True, False]
    for condition, keywords in enumerate([include, exclude]):
        pattern = re.compile('|'.join(map(re.escape, keywords)))
        match[condition] = False
        for key in keys:
            match[condition] |= bool(pattern.search(cast_str(__m.get(key))))
    return (match[INCLUDE] or not include) and not (match[EXCLUDE] and exclude)


def index_get(array: List, index: int, default=None) -> Any:
    abs_index = abs(index+1) if index < 0 else index
    return array[index] if abs_index < len(array) else default


def get_index(array: List, value: Any, default=None) -> Any:
    return array.index(value) if value in array else default


def fill_array(array: List, count: int, value=None) -> List:
    return [array[i] if i < len(array) else value for i in range(count)]


def fill_empty(array: List, default=None) -> List:
    return [element if not_na(element) else default for element in array]


def filter_array(array: List, mapFunc: Callable, apply: Optional[Callable]=None, **kwargs) -> List:
    if apply: return [apply(element) for element in array if mapFunc(element)]
    else: [element for element in array if mapFunc(element)]


def parse_scala(array: List, iter_type: Optional[type]=list) -> Union[Any,List]:
    return array[0] if isinstance(array, iter_type) and len(array) == 1 else array


###################################################################
############################### Map ###############################
###################################################################

def unique_keys(__m: Dict, **kwargs) -> Dict:
    return {key:value for key,value in __m.items() if key not in kwargs}


def chain_dict(args: Iterable[Dict], keep="first", empty=True, **kwargs) -> Dict:
    base = dict()
    for __m in args:
        if not (empty or __m): continue
        base = dict(base, **(unique_keys(__m, **base) if keep == "first" else __m))
    return base


def match_dict(__m: Dict[str,List], value=None, empty=True, **kwargs) -> Dict:
    if not __m: return dict()
    elif empty:
        count = max([len(values) for _, values in __m.items()])
        return {key: fill_array(values, count, value) for key, values in __m.items()}
    else:
        valid = inter([{i for i, value in enumerate(values) if value} for key, values in __m.items()])
        return {key: [values[i] for i in valid] for key, values in __m.items()}


def cast_get(__m: Dict, __key: _KT, __type: type, default=None, **kwargs) -> Union[Dict,List,str]:
    value = __m.get(__key, default)
    return value if isinstance(value, __type) else __type(value)


def list_get(__m: Dict, __keys: Iterable[_KT], default=None, cast=False, **kwargs) -> List[Union[Dict,List,str]]:
    if cast: return [cast_get(__m, __key, default.__class__, default) for __key in __keys]
    else: return [__m.get(__key, default) for __key in __keys]


def list_set(__m: Dict, __keys: Iterable[_KT], values: Iterable[_VT],
            empty=True, inplace=True, **kwargs) -> List[Union[Dict,List,str]]:
    if not inplace: __m = __m.copy()
    for __key, value in zip(__keys, values):
        if value or empty: __m[__key] = value
    if not inplace: return __m


def list_pop(__m: Dict, __keys: Iterable[_KT], **kwargs) -> Dict:
    for __key in __keys: __m.pop(__key, None)
    return __m


def apply_func(value, func: Callable, default=None, **kwargs) -> Any:
    try: return func(value, **kwargs)
    except: return default


def hier_get(__m: Dict, __path: Iterable[_KT], default=None, apply: Optional[Callable]=None,
            instance: Optional[type]=None, empty=True, null=True, **kwargs) -> _VT:
    try:
        for key in __path:
            __m = __m[key]
        value = apply_func(__m, apply, default, **kwargs) if apply else __m
        value = default if instance and not isinstance(value, instance) else value
        return value if (value or empty) and (value is not None or null) else default
    except: return default


def hier_set(__m: Dict, __path: Iterable[_KT], value: _VT, empty=True, null=True, inplace=True, **kwargs) -> Union[Dict,None]:
    if not inplace: __m = __m.copy()
    for key in __path[:-1]:
        if (value or empty) and (value is not None or null): __m = __m[key]
    __m[__path[-1]] = value
    if not inplace: return __m


def hier_get_set(__m: Dict, __get_path: Iterable[_KT], __set_path: Iterable[_KT], default=None,
                apply: Optional[Callable]=None, instance: Optional[type]=None,
                empty=True, null=True, inplace=True, **kwargs) -> Union[Dict,None]:
    value = hier_get(__m, __get_path, default, apply, instance, empty, null, **kwargs)
    return hier_set(__m, __set_path, value, inplace=inplace) if (value or empty) and (value is not None or null) else __m


def filter_map(__m: Dict, filter: List[str], **kwargs) -> Dict:
    return {key:__m[key] for key in filter if key in __m} if filter else __m


def re_get(pattern: str, string: str, default=str(), groups=False, **kwargs) -> str:
    if not re.search(pattern, string): return default
    catch = re.search(pattern, string).groups()
    return catch[0] if catch and not groups else catch


def replace_map(string: str, __m: dict, **kwargs) -> str:
    for __old, __new in __m.items():
        string = string.replace(__old, __new)
    return string


###################################################################
############################# Records #############################
###################################################################

def chain_records(records: List[Dict[str,List]]) -> Dict[str,List]:
    __m = dict()
    for record in records:
        if isinstance(record, dict):
            for key, values in record.items():
                if values and isinstance(values, list):
                    if key not in __m: __m[key] = values
                    else: __m[key] += values
    return __m


def get_feature(records: List[Dict], __key: _KT, default=None, match: Optional[Dict]=dict(), **kwargs) -> List:
    return [record.get(__key, default) for record in records
            if isinstance(record,dict) and (False not in [record.get(k) == v for k,v in match.items()])]


def filter_records(records: List[Dict], filter: List[str], **kwargs) -> List[Dict]:
    return [{key:record[key] for key in filter if key in record} for record in records] if filter else records


def sort_records(records: List[Dict], by: List[str], **kwargs) -> List[Dict]:
    return sorted(records, key=lambda x: list_get(x, by))


###################################################################
############################ DataFrame ############################
###################################################################

def astype_str(df: pd.DataFrame, str_cols: Optional[List[str]]=list(),) -> pd.DataFrame:
    df = df.copy()
    for column in str_cols:
        if column in df: df[column] = df[column].apply(str_na)
    return df


def filter_data(data: Union[List[Dict],pd.DataFrame], filter: Optional[List[str]]=list(),
                duplicates: Optional[List[str]]=list(), return_type="records", **kwargs) -> Union[List[Dict],pd.DataFrame]:
    if is_records(data):
        data = filter_records(data, filter)
        return pd.DataFrame(data) if return_type == "dataframe" else data
    elif isinstance(data, pd.DataFrame):
        columns = [column for column in filter if column in data.columns] if filter else data.columns
        data = data[columns]
        duplicates = [column for column in duplicates if column in columns]
        data = data.drop_duplicates(duplicates) if duplicates else data
        return data.to_dict('records') if return_type == "records" else data
    else: return list() if return_type == "records" else pd.DataFrame()


def filter_range(data: Union[List[Dict],pd.DataFrame], ranges: Dict[str,Tuple[_GTE,_LTE]]=dict(),
                return_type="records", **kwargs) -> Union[List[Dict],pd.DataFrame]:
    if is_records(data):
        data = [record for record in data
                if False not in [is_valid_range(record, key, gte, lte) for key, (gte, lte) in ranges.items()]]
        return pd.DataFrame(data) if return_type == "dataframe" else data
    elif isinstance(data, pd.DataFrame):
        for key, (gte, lte) in ranges.items():
            data = data[is_valid_range(data, key, gte, lte)]
        return data
    else: return list() if return_type == "records" else pd.DataFrame()


def is_valid_range(data: Union[Dict,pd.DataFrame], key: str, gte, lte, **kwargs) -> Union[bool,pd.Series]:
    is_df = isinstance(data, pd.DataFrame)
    if not key in data: return [True]*len(data) if is_df else True
    valid_format = lambda x: dateftime(x) if isinstance(x, dt.datetime) else x
    target = data[key].apply(valid_format) if is_df else valid_format(data[key])
    is_gte = (not gte) | (target >= gte)
    is_lte = (not lte) | (target <= lte)
    return is_gte & is_lte


def filter_na(__object, **kwargs) -> Any:
    if isinstance(__object, list):
        return [value for value in __object if not_na(value)]
    elif isinstance(__object, dict):
        return {key:value for key,value in __object.items() if not_na(value)}
    elif not_na(__object): return __object


def sort_values(data: Union[List[Dict],pd.DataFrame], by: Union[str,List[str]],
                values: Union[Iterable,Dict[str,Iterable]],
                ascending: Optional[Union[bool,List[bool]]]=True, **kwargs) -> pd.DataFrame:
    is_df = isinstance(data, pd.DataFrame)
    data = data.copy() if is_df else pd.DataFrame(data)
    map_order = lambda col=pd.Series(dtype="object"): col.map(
        {v: i for i, v in enumerate(values.get(col.name,list()) if isinstance(values, dict) else values)})
    data = data.sort_values(by, ascending=ascending, key=map_order)
    return data if is_df else data.to_dict("records")


def merge_drop(left: pd.DataFrame, right: pd.DataFrame, drop="right", how="inner",
                on: Optional[IndexLabel]=None, **kwargs) -> pd.DataFrame:
    col_empty = lambda columns: not inter(set(cast_list(on)), set(columns))
    if not on or col_empty(left.columns) or col_empty(right.columns):
        return right if how == "right" else left
    duplicates = list(inter(set(left.columns), set(right.columns))-set(cast_list(on)))
    if drop == "left": left = left.drop(columns=duplicates)
    elif drop == "right": right = right.drop(columns=duplicates)
    return left.merge(right, how=how, on=on, **kwargs)


def unroll_df(df: pd.DataFrame, columns: Union[Iterable[str],str],
                values: Union[Iterable[str],str], **kwargs) -> pd.DataFrame:
    columns, values = cast_list(columns), cast_list(values)
    get_values = lambda row: [row[value] for value in values]
    len_values = lambda row: min(map(len, get_values(row)))
    unroll_row = lambda row: [[row[col]]*len_values(row) for col in columns]+get_values(row)
    map_subrow = lambda subrow: {key:value for key, value in zip(columns+values,subrow)}
    map_row = lambda row: pd.DataFrame([map_subrow(subrow) for subrow in zip(*unroll_row(row))])
    return pd.concat([map_row(row) for _,row in df.iterrows()])
