from .cast import cast_list, cast_str

from typing import Callable, Iterable, Optional, Type, TypeVar, Union
from typing import Any, Dict, Hashable, List, Sequence, Tuple
from itertools import chain
from functools import cmp_to_key, reduce
import pandas as pd
import re

_KT = TypeVar("_KT")
_VT = TypeVar("_VT")
ITERABLE = (list, set, tuple)
DATA = Union[Dict,List[Dict],pd.DataFrame]
IndexLabel = Union[Hashable, Sequence[Hashable]]

INCLUDE = 0
EXCLUDE = 1

exists = lambda x: not x or pd.isna(x)
is_empty = lambda x: bool(x) and pd.notna(x)
str_na = lambda x: str(x) if pd.notna(x) else str()

isin_instance = lambda array, __type: True in map(lambda x: isinstance(x, __type), array)
allin_instance = lambda array, __type: False not in map(lambda x: isinstance(x, __type), array)

is_records = lambda array, empty=True: isinstance(array, ITERABLE) and (empty or not array or isin_instance(array, dict))
is_2darray = lambda array, empty=False: isinstance(array, ITERABLE) and (empty or not array or isin_instance(array, ITERABLE))
is_dfarray = lambda array, empty=False: isinstance(array, ITERABLE) and (empty or not array or isin_instance(array, pd.DataFrame))

is_intarray = lambda array, empty=True: isinstance(array, ITERABLE) and (empty or not array or allin_instance(array, int))
is_floatarray = lambda array, empty=True: isinstance(array, ITERABLE) and (empty or not array or allin_instance(array, float))
is_strarray = lambda array, empty=True: isinstance(array, ITERABLE) and (empty or not array or allin_instance(array, str))

is_df = lambda data: isinstance(data, pd.DataFrame)
df_exist = lambda df: not df.empty if isinstance(df, pd.DataFrame) else False
df_empty = lambda df: df.empty if isinstance(df, pd.DataFrame) else False

__or = lambda *args: reduce(lambda x,y: x|y, args)
__and = lambda *args: reduce(lambda x,y: x&y, args)
union = lambda *arrays: reduce(lambda x,y: x+y, arrays)
inter = lambda *arrays: reduce(lambda x,y: [e for e in x if e in y], arrays)

abs_idx = lambda idx: abs(idx+1) if idx < 0 else idx
flatten = lambda *args: [element for array in args for element in (array if isinstance(array, ITERABLE) else cast_list(array))]

INCLUSIVE = ("both", "neither", "left", "right")
between = lambda __object, left=None, right=None, inclusive="both", **kwargs: (
    (left == None or (__object >= left if inclusive in ["both","left"] else __object > left)) &
    (right == None or (__object <= right if inclusive in ["both","right"] else __object < right)))
unzip = lambda __object: (
    (__object[0] if isinstance(__object, tuple) else __object.copy().pop())
        if isinstance(__object, ITERABLE) and __object else __object)

values_to_back = lambda array, values: sorted(array, key=cmp_to_key(lambda x, y: -1 if y in values else 0))
keys_to_back = lambda __m, __keys: dict(sorted(__m.items(), key=cmp_to_key(lambda x, y: -1 if y[0] in __keys else 0)))


def unique(*elements, empty=False, null_only=False, **kwargs) -> List:
    array = list()
    is_valid = pd.notna if null_only else exists
    for element in elements:
        if not (empty or is_valid(element)): continue
        if element not in array: array.append(element)
    return array


def exists_one(*args, **kwargs) -> Any:
    for arg in args:
        if arg: return arg
    return args[-1]


def map_context(__keys: Iterable, __values: Iterable, **context) -> Dict:
    if context: return context
    elif not (isinstance(__keys, ITERABLE) and isinstance(__values, ITERABLE)): return dict()
    elif not (__keys and __values and (len(__keys) == len(__values))): return dict()
    else: {__key:__value for __key, __value in zip(__keys, __values)}


###################################################################
############################## String #############################
###################################################################

def re_get(pattern: str, string: str, default=str(), groups=False, **kwargs) -> str:
    if not re.search(pattern, string): return default
    catch = re.search(pattern, string).groups()
    return catch[0] if catch and not groups else catch


def replace_map(string: str, __m: dict, **kwargs) -> str:
    for __old, __new in __m.items():
        string = string.replace(__old, __new)
    return string


###################################################################
############################## Array ##############################
###################################################################

def iloc(array: Union[List,Tuple], index: Union[int,Iterable], default=None, **kwargs) -> Union[Any,List]:
    length = len(array)
    if isinstance(index, int):
        return array[index] if abs_idx(index) < length else (default if default != "pass" else None)
    elif not is_intarray(index, empty=False): return array
    elif default == "pass": return [array[i] for i in index if abs_idx(i) < length]
    else: return [array[i] if abs_idx(i) < length else default for i in index]


def get_index(array: Union[List,Tuple], values: Union[Any,Iterable], default=None, multiple=True, **kwargs) -> Union[Any,List]:
    if not (isinstance(values, ITERABLE) and multiple):
        return array.index(values) if values in array else (default if default != "pass" else None)
    elif not values: return array
    elif default == "pass": return [array.index(value) for value in values if value in array]
    else: return [array.index(value) if value in array else default for value in values]


def chain_list(iterable: Iterable[Iterable], empty=True, **kwargs) -> List:
    if not iterable: return list()
    elif not empty: iterable = [array for array in iterable if exists(array)]
    return list(chain.from_iterable(iterable))


def fill_array(array: List, value=None, count: Optional[int]=None, **kwargs) -> List:
    count = count if isinstance(count, int) else len(array)
    return [array[i] if i < len(array) else value for i in range(count)]


def filter_array(array: List, mapFunc: Callable[[Any],bool], apply: Optional[Callable]=None, **kwargs) -> List:
    if apply: return [apply(element) for element in array if mapFunc(element)]
    else: [element for element in array if mapFunc(element)]


def match_list(iterable: Iterable[Iterable], how="all", value=None, empty=True,
                unique=False, null_only=False, **kwargs) -> Tuple[List]:
    is_valid = pd.notna if null_only else exists
    if len(iterable) == 1:
        return fill_array(iterable[0], value) if empty else filter_array(iterable[0], is_valid)
    elif len(iterable) > 1 and how == "all" and empty:
        return tuple(fill_array(array, value, count=max([len(array) for array in iterable])) for array in iterable)
    elif len(iterable) > 1 and how == "first" and empty:
        return tuple([iterable[0]]+[fill_array(array, value, count=len(iterable[0])) for array in iterable[1:]])
    elif len(iterable) > 1 and how == "dropna":
        if len(set(map(len,iterable))) != 1: return iterable
        matches = [idx for idx in range(len(iterable[0])) if True in map(lambda x: is_valid(x[idx]), iterable)]
        return tuple([array[:max(matches)+1] if matches else list() for array in iterable])
    valid = match_index(iterable, how=how, unique=unique, null_only=null_only,)
    return tuple([array[idx] if idx < len(array) else value for idx in valid] for array in iterable)


def match_index(iterable: Iterable[Iterable], how="all", unique=False, null_only=False, **kwargs) -> List[int]:
    __l, __s, count = iterable, [set() for _ in iterable], 0
    is_valid = pd.notna if null_only else exists
    if len(__l) > 0 and how == "all":
        count = min([len(array) for array in __l])
    elif len(__l) > 0 and how == "first":
        count = len(__l[0])
        __l, __s = [iterable[0]], [set()]
    if unique:
        index = [set() for _ in __l]
        for cur, array in enumerate(__l):
            for idx, element in enumerate(array):
                if is_empty(element) or element in __s[cur]: continue
                index[cur].add(idx)
                __s[cur].add(element)
        return sorted(__and(*index))
    else: return [idx for idx in range(count) if False not in [is_valid(array[idx]) for array in __l]]


###################################################################
############################### Map ###############################
###################################################################

def kloc(__m: Dict, __keys: Union[_KT,Iterable[_KT]], default=None, **kwargs) -> Union[Any,Dict]:
    if not isinstance(__keys, ITERABLE): return __m.get(__keys, (default if default != "pass" else None))
    elif not __keys: return __m
    elif default == "pass": return {key:__m[key] for key in __keys if key in __m}
    else: return {key:__m.get(key, default) for key in __keys}


def unique_keys(__m: Dict, **kwargs) -> Dict:
    return {key:value for key,value in __m.items() if key not in kwargs}


def chain_dict(args: Iterable[Dict], keep="first", empty=True, **kwargs) -> Dict:
    base = dict()
    for __m in args:
        if not (empty or __m): continue
        base = dict(base, **(unique_keys(__m, **base) if keep == "first" else __m))
    return base


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
            instance: Optional[Type]=None, empty=True, null=True, **kwargs) -> _VT:
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
                apply: Optional[Callable]=None, instance: Optional[Type]=None,
                empty=True, null=True, inplace=True, **kwargs) -> Union[Dict,None]:
    value = hier_get(__m, __get_path, default, apply, instance, empty, null, **kwargs)
    return hier_set(__m, __set_path, value, inplace=inplace) if (value or empty) and (value is not None or null) else __m


def match_dict(__m: Dict[_KT,Iterable], value=None, empty=True, **kwargs) -> Dict[_KT,List]:
    if not __m: return dict()
    elif empty:
        count = max([len(values) for _, values in __m.items()])
        return {key: fill_array(values, value, count=count) for key, values in __m.items()}
    else:
        valid = __and(*[{i for i, value in enumerate(values) if value} for key, values in __m.items()])
        return {key: [values[i] for i in valid] for key, values in __m.items()}


def match_keywords(__m: Dict, __keys: Iterable[_KT], include=list(), exclude=list(), **kwargs) -> bool:
    if not (include or exclude): return True
    include, exclude, match = cast_list(include), cast_list(exclude), [False, False]
    for condition, keywords in enumerate([include, exclude]):
        pattern = re.compile('|'.join(map(re.escape, keywords)))
        for __key in __keys:
            match[condition] |= bool(pattern.search(cast_str(__m[__key])))
    return (match[INCLUDE] or not include) and not (match[EXCLUDE] and exclude)


###################################################################
############################# Records #############################
###################################################################

def vloc(records: List[Dict], __keys: Union[_KT,Iterable[_KT]], default=None, **kwargs) -> Union[List,List[Dict]]:
    base = list()
    for record in records:
        values = kloc(record, __keys, default=default)
        if default == "pass" and (values == None or values == dict()): continue
        else: base.append(values)
    return base


def to_records(__object: Union[Dict,List[Dict],pd.DataFrame], **kwargs) -> List[Dict]:
    if is_records(__object, empty=True): return __object
    elif isinstance(__object, pd.DataFrame): return __object.to_dict("records")
    elif isinstance(__object, dict): return [__object]
    else: return list()


def apply_records(records: List[Dict], __keys: Optional[List[Any]]=list(),
                    __applyFuncs: Optional[List[Callable]]=list(), **context) -> List[Dict]:
    records = records.copy()
    for idx, record in enumerate(records.copy()):
        for __key, applyFunc in map_context(__keys, __applyFuncs, **context).items():
            if not isinstance(applyFunc, Callable): continue
            elif not __key: records[idx] = applyFunc(record)
            elif __key not in record: continue
            else: records[idx][__key] = applyFunc(record[__key])
    return records


def match_records(records: List[Dict], __keys: Optional[List[Any]]=list(),
                    __matchFuncs: Optional[List[Callable]]=list(), **context) -> List[Dict]:
    array = list()
    for record in records:
        match = True
        for __key, matchFunc in map_context(__keys, __matchFuncs, **context).items():
            if not isinstance(matchFunc, Callable): continue
            elif not __key: match &= matchFunc(record)
            elif __key not in record: continue
            else: match &= matchFunc(record[__key])
        if match: array.append(record)
    return array


def isin_records(records: List[Dict], __keys: Union[_KT,Iterable[_KT]], how="any", **kwargs) -> Union[bool,List[bool]]:
    if not isinstance(__keys, ITERABLE):
        isin = [__keys in record for record in records]
        return False not in isin if how == "all" else True in isin
    elif not __keys: return [True]*len(records)
    else: return [isin_records(records, __key, how=how) for __key in __keys if not isinstance(__key, ITERABLE)]


def between_records(records: List[Dict], __keys: Optional[List[Any]]=list(),
                    __args: Optional[List[Union[Tuple,Dict]]]=list(),
                    inclusive="both", if_null="drop", **context) -> List[Dict]:
    array = list()
    for record in records:
        match = True
        for __key, args in map_context(__keys, __args, **context).items():
            if __key in record:
                if isinstance(args, ITERABLE): match &= between(record[__key], *args[:2], inclusive=inclusive)
                elif isinstance(args, dict): match &= between(record[__key], **args, inclusive=inclusive)
                else: raise ValueError("Between condition must be an iterable or a dictionary")
            elif if_null == "drop":
                match = False
                break
        if match: array.append(record)
    return array


def sort_records(records: List[Dict], by: List[str], ascending=True, **kwargs) -> List[Dict]:
    return sorted(records, key=lambda x: list_get(x, by), reverse=(not ascending))


###################################################################
############################ DataFrame ############################
###################################################################

def cloc(df: pd.DataFrame, columns: Union[str,Iterable[str]], default=None, **kwargs) -> pd.DataFrame:
    if isinstance(columns, str):
        if columns in df: return df[[columns]]
        elif default == "pass": return pd.DataFrame()
        else: return pd.DataFrame([default]*len(df), columns=[columns])
    elif columns:
        df = df[[column for column in columns if column in df]]
        if default == "pass": df = pd.concat([pd.DataFrame(columns=columns),df])
    return df


def to_dataframe(__object: Union[Dict,List[Dict],pd.DataFrame], **kwargs) -> pd.DataFrame:
    if isinstance(__object, pd.DataFrame): return __object
    elif is_records(__object, empty=True): return pd.DataFrame(__object)
    elif isinstance(__object, dict): return pd.DataFrame([__object])
    else: return pd.DataFrame()


def apply_df(df: pd.DataFrame, __columns: Optional[List[Any]]=list(),
            __applyFunc: Optional[List[Callable]]=list(), **context) -> pd.DataFrame:
    df = df.copy()
    for column, applyFunc in map_context(__columns, __applyFunc, **context).items():
        if not (isinstance(column, str) and isinstance(applyFunc, Callable)): continue
        elif not column: df = df.apply(applyFunc, axis=1)
        elif column not in df: continue
        else: df[column] = df[column].apply(applyFunc)
    return df


def match_df(df: pd.DataFrame, __columns: Optional[List[Any]]=list(),
            __matchFunc: Optional[List[Callable]]=list(), **context) -> pd.DataFrame:
    df, match = df.copy(), pd.Series([True]*len(df), index=df.index)
    for column, matchFunc in map_context(__columns, __matchFunc, **context).items():
        if not (isinstance(column, str) and isinstance(matchFunc, Callable)): continue
        elif not column: match &= df.apply(matchFunc, axis=1)
        elif column not in df: continue
        else: match &= df[column].apply(matchFunc)
    return df[match]


def between_df(df: pd.DataFrame, inclusive="both", if_null="drop", **context) -> pd.DataFrame:
    df, kwargs, default = df.copy(), {"inclusive":inclusive}, pd.Series([False]*len(df), index=df.index)
    for column, args in context.items():
        if column in df:
            if_na = default if if_null == "drop" else df[column].isna()
            if isinstance(args, ITERABLE): df = df[df[column].apply(lambda x: between(x, *args[:2], **kwargs))|if_na]
            elif isinstance(args, dict): df = df[df[column].apply(lambda x: between(x, **args, **kwargs))|if_na]
            else: raise ValueError("Between condition must be an iterable or a dictionary")
    return df


def merge_drop(left: pd.DataFrame, right: pd.DataFrame, drop="right", how="inner",
                on: Optional[IndexLabel]=None, **kwargs) -> pd.DataFrame:
    col_empty = lambda columns: not (set(cast_list(on)) & set(columns))
    if not on or col_empty(left.columns) or col_empty(right.columns):
        return right if how == "right" else left
    duplicates = list((set(left.columns) & set(right.columns)) - set(cast_list(on)))
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


def round_df(df: pd.DataFrame, columns: Union[Iterable[str],str], trunc=2, **kwargs) -> pd.DataFrame:
    if not isinstance(trunc, int): return df
    roundFunc = lambda x: round(x,trunc) if isinstance(x,float) else x
    return apply_df(df, **{column:roundFunc for column in cast_list(columns)})


###################################################################
############################ Multitype ############################
###################################################################

def filter_exists(__object, null_only=False, **kwargs) -> Any:
    is_valid = pd.notna if null_only else exists
    if isinstance(__object, dict):
        return {key:value for key,value in __object.items() if is_valid(value)}
    elif isinstance(__object, ITERABLE):
        return type(__object)([value for value in __object if is_valid(value)])
    elif isinstance(__object, pd.DataFrame):
        return __object.dropna(axis=1, how="any")
    elif is_valid(__object): return __object


def convert_data(data: Union[Dict,List[Dict],pd.DataFrame], return_type: Optional[Type]=None,
                **kwargs) -> Union[Dict,List[Dict],pd.DataFrame]:
    if return_type in (Dict,dict): return data if isinstance(data, dict) else dict()
    elif return_type in (List,List[Dict],list): return to_records(data)
    elif return_type == pd.DataFrame: return to_dataframe(data)
    else: return data


def chain_exists(data: Union[Dict,List[Dict],pd.DataFrame], return_type: Optional[Type]=None,
                **kwargs) -> Union[Dict,List[Dict],pd.DataFrame]:
    if is_dfarray(data):
        dfarray = [df for df in data if df_exist(df)]
        data = pd.concat(dfarray) if dfarray else pd.DataFrame()
    elif is_2darray(data): data = chain_list(data, empty=False)
    else: data = filter_exists(data)
    return convert_data(data, return_type)


def filter_data(data: Union[Dict,List[Dict],pd.DataFrame], filter: Optional[Iterable[str]]=list(),
                default=None, return_type: Optional[Type]=None, **kwargs) -> Union[Dict,List[Dict],pd.DataFrame]:
    filter = filter if isinstance(filter, ITERABLE) else [filter]
    if isinstance(data, dict): data = kloc(data, __keys=filter, default=default)
    elif is_records(data): data = vloc(data, __keys=filter, default=default)
    elif isinstance(data, pd.DataFrame): data = cloc(data, columns=filter, default=default)
    return convert_data(data, return_type)


def apply_data(data: Union[List[Dict],pd.DataFrame], __keys: Optional[List[Any]]=list(),
               __applyFuncs: Optional[List[Callable]]=list(), return_type: Optional[Type]=None,
               **context) -> Union[List[Dict],pd.DataFrame]:
    if is_records(data): data = apply_records(data, __keys, __applyFuncs, **context)
    elif isinstance(data, pd.DataFrame): data = apply_df(data, __keys, __applyFuncs, **context)
    return convert_data(data, return_type)


def match_data(data: Union[List[Dict],pd.DataFrame], __keys: Optional[List[Any]]=list(),
               __matchFuncs: Optional[List[Callable]]=list(), return_type: Optional[Type]=None,
               **context) -> Union[List[Dict],pd.DataFrame]:
    if is_records(data): data = match_records(data, __keys, __matchFuncs, **context)
    elif isinstance(data, pd.DataFrame): data = match_df(data, __keys, __matchFuncs, **context)
    return convert_data(data, return_type)


def between_data(data: Union[List[Dict],pd.DataFrame], inclusive="both", if_null="drop",
                return_type: Optional[Type]=None, **kwargs) -> Union[List[Dict],pd.DataFrame]:
    if is_records(data): data = between_records(data, inclusive=inclusive, if_null=if_null, **kwargs)
    elif isinstance(data, pd.DataFrame): data = between_df(data, inclusive=inclusive, if_null=if_null)
    return convert_data(data, return_type)


def sort_values(data: Union[List[Dict],pd.DataFrame], by: Union[str,List[str]],
                ascending: Optional[Union[bool,Iterable[bool]]]=True,
                return_type: Optional[Type]=None, **kwargs) -> pd.DataFrame:
    if is_records(data):
        ascending = bool(iloc(ascending, 0)) if isinstance(ascending, ITERABLE) else ascending
        data = sort_records(data, by=by, ascending=ascending)
    elif isinstance(data, pd.DataFrame): data = data.sort_values(by, ascending=ascending)
    return convert_data(data, return_type)
