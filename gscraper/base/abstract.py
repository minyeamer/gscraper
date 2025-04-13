from __future__ import annotations
from gscraper.base.types import _KT, _VT, TypeHint, MatchFunction
from gscraper.base.types import Records, RenameMap, TypeMap, get_type, is_array, allin_instance, is_records

from gscraper.utils import notna
from gscraper.utils.map import hier_get, notna_dict, exists_dict, drop_dict
from gscraper.utils.map import kloc, vloc, match_records, drop_duplicates

import abc
import copy
import functools

from typing import Any, Callable, Dict, Iterable, List, Literal, Optional, Sequence, Tuple, Union
import pandas as pd
import re


NAME, TYPE, DESC, DEFAULT, ALIAS = "name", "type", "desc", "default", "alias"
__NONE__ = "__NONE__"
__OPTIONAL__ = "__OPTIONAL__"

INVALID_MSG = lambda __name: f"'{__name}' is not valid."
INVALID_OBJECT_MSG = lambda __object, __name: f"'{__object}' is not a valid {__name} object."
INVALID_OBJECT_TYPE_MSG = lambda __object, __type: f"'{type(__object)}' is not a valid type for {__type} object."
INVALID_INSTANCE_MSG = lambda __object, __instance: f"'{type(__object)}' is not a valid instance for {__instance} type."


###################################################################
############################# Context #############################
###################################################################

BASE_CONTEXT = lambda self=None, operation=None, info=None, initTime=None, prefix=None, rename=None, \
                        inplace=None, self_var=None, update=None, **context: context


LOG_CONTEXT = lambda logger=None, interruptType=None, killType=None, errorType=None, errors=None, **context: context


ITERATOR_CONTEXT = lambda iterator=None, iterateArgs=None, iterateCount=None, iterateProduct=None, pagination=None, \
                        pageFrom=None, offsetFrom=None, pageUnit=None, pageLimit=None, fromNow=None, __i=None, \
                        **context: context


MAP_CONTEXT = lambda flow=None, schema=None, responseType=None, match=None, root=None, groupby=None, \
                    countby=None, __index=None, **context: context


SPIDER_CONTEXT = lambda asyncio=None, host=None, field=None, ssl=None, mappedReturn=None, \
                        maxLimit=None, redirectLimit=None, **context: context


ENCRYPTED_CONTEXT = lambda decryptedKey=None, auth=None, authKey=None, includeCookies=None, **context: context


PIPELINE_CONTEXT = lambda  derivFields=None, globalMessage=None, globalProgress=None, taskProgress=None, \
                            asyncMessage=None, asyncProgress=None, taskErrors=None, dags=None, **context: context


UNIQUE_CONTEXT = lambda **context: \
    PIPELINE_CONTEXT(**ENCRYPTED_CONTEXT(**SPIDER_CONTEXT(**MAP_CONTEXT(**ITERATOR_CONTEXT(**LOG_CONTEXT(**BASE_CONTEXT(**context)))))))


PARAMS_CONTEXT = lambda init=None, data=None, task=None, worker=None, locals=None, which=None, where=None, by=None, verb=None, \
                        default=None, clean=None, how=None, alignment=None, dropna=None, drop_empty=None, unique=None, drop=None, \
                        index=None, log=None, depth=None, hier=None, to=None, countPath=None, hasSize=None, primary=None, **context: context


REQUEST_CONTEXT = lambda session=None, semaphore=None, asynchronous=None, method=None, url=None, referer=None, messages=None, \
                        params=None, encode=None, data=None, json=None, headers=None, cookies=None, allow_redirects=None, \
                        validate=None, valid=None, invalid=None, close=None, encoding=None, features=None, \
                        table_type=None, table_idx=None, table_options=None, **context: context


RESPONSE_CONTEXT = lambda response=None, tzinfo=None, countryCode=None, iterateUnit=None, logName=None, logLevel=None, logFile=None, \
                        delay=None, progress=None, message=None, numTasks=None, apiRedirect=None, redirectUnit=None, **context: context


GCLOUD_CONTEXT = lambda name=None, data=None, account=None, key=None, sheet=None, table=None, query=None, project_id=None, \
                        columns=None, mode=None, primary_key=None, base=None, read=None, upload=None, \
                        cell=None, clear=None, default=None, head=None, headers=None, str_cols=None, arr_cols=None, \
                        rename=None, to=None, progress=None, partition=None, set_date=None, fields=None, **context: context


UPLOAD_CONTEXT = lambda queryList=None, uploadList=None, alertInfo=None, **context: context


TASK_CONTEXT = lambda func=None, exception=None, **context: UPLOAD_CONTEXT(**PARAMS_CONTEXT(**context))


SESSION_CONTEXT = lambda func=None, exception=None, session=None, semaphore=None, cookies=str(), **context: \
                        dict(UPLOAD_CONTEXT(**REQUEST_CONTEXT(**PARAMS_CONTEXT(**context))), **exists_dict(cookies=cookies))


PROXY_CONTEXT = lambda session=None, semaphore=None, **context: UNIQUE_CONTEXT(**UPLOAD_CONTEXT(**context))


LOCAL_CONTEXT = lambda __index=None, apiRedirect=None, returnType=None, localSave=None, **context: SESSION_CONTEXT(**context)


REDIRECT_CONTEXT = lambda logFile=None, **context: LOCAL_CONTEXT(**SESSION_CONTEXT(**context))


###################################################################
########################### Custom Dict ###########################
###################################################################

class CustomDict(dict):
    __metaclass__ = abc.ABCMeta

    def __init__(self, __m: Dict=dict(), self_var=True, **kwargs):
        super().__init__()
        self.update(__m, self_var=self_var, **kwargs)

    def copy(self) -> CustomDict:
        return copy.deepcopy(self)

    def update(self, __m: Dict=dict(), inplace=True, self_var=False, **kwargs) -> CustomDict:
        if not inplace: self = self.copy()
        for __key, __value in dict((dict(self.__dict__, **__m) if self_var else __m), **kwargs).items():
            self[__key] = __value
        if not inplace: return self

    def update_notna(self, __m: Dict=dict(), null_if: Dict=dict(), inplace=True, self_var=False, **kwargs) -> CustomDict:
        return self.update(notna_dict(dict(__m, **kwargs), null_if=null_if), inplace=inplace, self_var=self_var)

    def update_exists(self, __m: Dict=dict(), null_if: Dict=dict(), inplace=True, self_var=False, **kwargs) -> CustomDict:
        return self.update(exists_dict(dict(__m, **kwargs), null_if=null_if), inplace=inplace, self_var=self_var)

    def get(self, __key: _KT, default=None, if_null: Literal["drop","pass"]="pass", values_only=True) -> Union[_VT,Dict]:
        if not is_array(__key): return self[__key] if __key in self else default
        elif if_null == "drop": __m = {__k: self[__k] for __k in __key if __k in self}
        else: __m = {__k: (self[__k] if __k in self else default) for __k in __key}
        return list(__m.values()) if values_only else __m

    def print(self, *__object, path: Optional[_KT]=None, drop: Optional[_KT]=None, indent=2, step=2, double_quotes=False, sep=' '):
        __object = __object if __object else (self,)
        pretty_print(*__object, path=path, drop=drop, indent=indent, step=step, double_quotes=double_quotes, sep=sep)

    def __setitem__(self, __key: _KT, __value: _VT):
        super().__setitem__(__key, __value)
        setattr(self, __key, __value)


class OptionalDict(CustomDict):
    __metaclass__ = abc.ABCMeta

    def __init__(self, optional: Dict=dict(), null_if: Dict=dict(), self_var=False, **kwargs):
        super().__init__(kwargs, **notna_dict(optional, null_if=null_if), self_var=self_var)


class TypedDict(CustomDict):
    __metaclass__ = abc.ABCMeta
    dtype = None
    typeCheck = True

    def __init__(self, **kwargs):
        if not self.typeCheck: dict.__init__(self, kwargs)
        else: dict.__init__(self, {__key: self.validate_dtype(__value) for __key, __value in kwargs.items()})

    def validate_dtype(self, __object) -> Dict:
        if (not self.dtype) or isinstance(__object, self.dtype): return __object
        elif isinstance(__object, self.dtype): return __object
        else: self.raise_dtype_error(__object)

    def raise_dtype_error(self, __object, __type=str()):
        dtype = __type if __type else self.dtype.__name__
        raise TypeError(INVALID_OBJECT_TYPE_MSG(__object, dtype))


###################################################################
########################## Custom Records #########################
###################################################################

class CustomRecords(list):
    __metaclass__ = abc.ABCMeta

    def __init__(self, __iterable: Records):
        super().__init__(__iterable)

    def copy(self) -> CustomRecords:
        return copy.deepcopy(self)

    def update(self, __iterable: Iterable, inplace=True) -> CustomRecords:
        if not inplace: self = self.copy()
        self.clear()
        self.add(__iterable)
        if not inplace: return self

    def add(self, __iterable: Iterable):
        for __i in __iterable:
            self.append(__i)

    def get(self, __key: _KT, default=None, if_null: Literal["drop","pass"]="pass", reorder=True,
            values_only=True, hier=False, key_alias: Sequence[_KT]=list(), axis=0) -> Union[Records,List]:
        context = dict(if_null=if_null, reorder=reorder, values_only=values_only, hier=hier, key_alias=key_alias, axis=axis)
        return vloc(self, __key, default=default, **context)

    def map(self, key: str, value: _KT, default=None, if_null: Literal["drop","pass"]="drop", reorder=True,
            values_only=False, hier=False, key_alias: Sequence[_KT]=list()) -> Dict[_VT,Union[_VT,Dict]]:
        if is_array(value):
            context = dict(if_null=if_null, reorder=reorder, values_only=values_only, hier=hier, key_alias=key_alias)
            return {__m[key]: kloc(__m, value, default=default, **context) for __m in self if key in __m}
        elif if_null == "drop":
            return {__m[key]: __m[value] for __m in self if (key in __m) and (value in __m)}
        else: return {__m[key]: (__m[value] if value in __m else default) for __m in self if key in __m}

    def filter(self, __match: Optional[MatchFunction]=None, inplace=False, **match_by_key) -> CustomRecords:
        if isinstance(__match, Callable):
            return self.update(match_records(self, all_keys=True, match=__match), inplace=inplace)
        elif match_by_key:
            return self.update(match_records(self, **match_by_key), inplace=inplace)
        else: return None if inplace else self

    def unique(self, keys: Optional[_KT]=list(), keep: Literal["fist","last",True,False]="first",
                inplace=False) -> CustomRecords:
        if keep == True: return None if inplace else self
        else: return self.update(drop_duplicates(self, keys, keep=keep), inplace=inplace)

    def print(self, *__object, path: Optional[_KT]=None, drop: Optional[_KT]=None, indent=2, step=2, double_quotes=False, sep=' '):
        __object = __object if __object else (self,)
        pretty_print(*__object, path=path, drop=drop, indent=indent, step=step, double_quotes=double_quotes, sep=sep)


class TypedRecords(CustomRecords):
    __metaclass__ = abc.ABCMeta
    dtype = dict
    typeCheck = True

    def __init__(self, *args: Dict):
        if not self.typeCheck: list.__init__(self, args)
        else: list.__init__(self, [self.validate_dtype(__i) for __i in args])

    def validate_dtype(self, __object) -> Dict:
        if (not self.dtype) or isinstance(__object, self.dtype): return __object
        elif isinstance(__object, Dict): return self.dtype(**__object)
        else: raise self.raise_dtype_error(__object)

    def raise_dtype_error(self, __object, __type=str()):
        dtype = __type if __type else self.dtype.__name__
        raise TypeError(INVALID_OBJECT_TYPE_MSG(__object, dtype))

    def append(self, __object: Dict):
        super().append(self.validate_dtype(__object))


class NamedRecords(TypedRecords):
    __metaclass__ = abc.ABCMeta

    def rename(self, __s: str, to: Optional[Literal["name","desc"]]="desc",
                if_null: Union[Literal["pass"],Any]="pass") -> str:
        renameMap = self.get_rename_map(to=to)
        if renameMap and (__s in renameMap): return renameMap[__s]
        else: return __s if if_null == "pass" else if_null

    def get_rename_map(self, to: Literal["name","desc"]="desc") -> RenameMap:
        key, value = (DESC, NAME) if to == NAME else (NAME, DESC)
        return self.map(key, value)

    def get_type_map(self, key: Literal["name","desc"]="name") -> TypeMap:
        return self.map(key, TYPE)


###################################################################
############################## Value ##############################
###################################################################

class Value(OptionalDict):
    __metaclass__ = abc.ABCMeta
    typeCast = True

    def __init__(self, name: _KT, type: TypeHint, optional: Dict=dict(), null_if: Dict=dict(), self_var=False, **kwargs):
        type = get_type(type) if self.typeCast else type
        super().__init__(name=name, type=type, optional=optional, null_if=null_if, self_var=self_var, **kwargs)


class ValueSet(NamedRecords):
    __metaclass__ = abc.ABCMeta
    dtype = Value
    typeCheck = True


###################################################################
############################# Prettier ############################
###################################################################

def to_default(__object) -> Any:
    if isinstance(__object, Dict):
        return {__key: to_default(__value) for __key, __value in __object.items()}
    elif isinstance(__object, List):
        return [to_default(__e) for __e in __object]
    else: return __object


def _format_quote(func) -> str:
    @functools.wraps(func)
    def wrapper(*args, double_quotes=False, **context):
        __s = func(*args, **context)
        return __s.replace('\'', '\"') if double_quotes and isinstance(__s, str) else __s
    return wrapper


@_format_quote
def pretty_str(__object, indent=2, step=2, double_quotes=False) -> str:
    indent = max(indent, step)
    if isinstance(__object, Value):
        return str({__k: to_default(__v) for __k, __v in __object.items()})
    elif isinstance(__object, CustomDict):
        return '{\n'+',\n'.join([' '*indent+f"'{__k}': {pretty_str(__v, indent=indent+step, step=step)}"
                    for __k, __v in __object.items()])+'\n'+' '*(indent-step)+'}'
    elif isinstance(__object, CustomRecords):
        return '[\n'+',\n'.join([' '*indent+pretty_str(__e, indent=indent+step, step=step)
                for __e in __object])+'\n'+' '*(indent-step)+']'
    elif is_array(__object) and allin_instance(__object, (CustomDict,CustomRecords)):
        __s = str(__object)
        return f'{__s[0]}\n'+',\n'.join([' '*indent+pretty_str(__e, indent=indent+step, step=step)
                for __e in __object])+'\n'+' '*(indent-step)+f'{__s[-1]}'
    elif isinstance(__object, str): return f"'{__object}'"
    else: return str(__object).replace('\n', '\\n')


@_format_quote
def pretty_object(__object, path: Optional[_KT]=None, drop: Optional[_KT]=None, indent=2, step=2, double_quotes=False) -> str:
    if notna(path): __object = hier_get(__object, path)
    if notna(drop): __object = drop_dict(__object, drop)
    context = dict(indent=indent, step=step)
    if isinstance(__object, (CustomDict,CustomRecords)):
        return pretty_str(__object, **context)
    elif isinstance(__object, Dict):
        return pretty_str(CustomDict(__object), **context)
    elif is_records(__object, how="all"):
        return pretty_str(CustomRecords(__object), **context)
    elif isinstance(__object, pd.DataFrame):
        return "pd.DataFrame("+pretty_str(CustomRecords(__object.to_dict("records")), **context)+")"
    elif isinstance(__object, pd.Series):
        return "pd.Series("+pretty_str(__object.tolist(), **context)+")"
    else: return pretty_str(__object)


def pretty_print(*args, path: Optional[_KT]=None, drop: Optional[_KT]=None, indent=2, step=2, double_quotes=False, sep=' '):
    print(sep.join([
        pretty_object(__object, path=path, drop=drop, indent=indent, step=step, double_quotes=double_quotes) for __object in args]))


###################################################################
############################## Query ##############################
###################################################################

class Variable(Value):
    def __init__(self, name: _KT, type: TypeHint, desc: Optional[str]=None, default: Optional[Any]=__NONE__,
                iterable=False, arr_options: Optional[Dict]=None, enum: Union[Tuple[str,...],RenameMap]=None):
        type, iterable = self.validate_iterable(type, iterable)
        super().__init__(name=name, type=type)
        self.update_optional(desc, default, iterable, arr_options, enum)

    def validate_iterable(self, type: TypeHint, iterable=False) -> Union[TypeHint,bool]:
        if isinstance(type, str) and bool(re.match(r"\[[^]]*\]", type)):
            type = re.sub(r"^\[([^]]*)\]$", r"\g<1>", type)
            iterable = True
        return type, iterable

    def update_optional(self, desc: Optional[str]=None, default: Optional[Any]=__NONE__, iterable=False,
                        arr_options: Optional[Dict]=None, enum: Union[Tuple[str,...],RenameMap]=None):
        enum = enum if isinstance(enum, (Dict,Tuple)) else None
        if isinstance(arr_options, Dict):
            iterable = True
            if (arr_options.get("default") is not None) and (not arr_options.get("drop_empty")):
                arr_options["dropna"] = True
        elif iterable:
            if enum: arr_options = dict(dropna=True, unique=True)
            else: arr_options = dict(drop_empty=True, unique=True)
        self.update_notna(
            dict(desc=desc, default=default, iterable=iterable, arr_options=arr_options, enum=enum),
            null_if=dict(default=__NONE__))

    def copy(self) -> Variable:
        return copy.deepcopy(self)

    def update(self, __m: Dict=dict(), inplace=True, self_var=False, **kwargs) -> Variable:
        return super().update(__m, inplace=inplace, self_var=self_var, **kwargs)


class Query(ValueSet):
    dtype = Variable
    typeCheck = True

    def __init__(self, *variables: Variable):
        super().__init__(*variables)

    def copy(self) -> Query:
        return copy.deepcopy(self)

    def update(self, __iterable: Iterable[Variable], inplace=True) -> Query:
        return super().update(__iterable, inplace=inplace)

    def get_required_query(self) -> Query:
        optional_query = self.map(key="name", value="default")
        return Query(*[variable for variable in self if variable[NAME] not in optional_query])

    def get_optional_query(self) -> Query:
        optional_query = self.map(key="name", value="default")
        return Query(*[variable for variable in self if variable[NAME] in optional_query])


###################################################################
########################## Session Query ##########################
###################################################################

FILTER_QUERY = lambda mapped=False: Query(
    Variable(name="fields", type="STRING", iterable=(not mapped), default=(dict() if mapped else list())),
    Variable(name="ranges", type="[STRING]", default=list()),
    Variable(name="returnType", type="STRING", default=None),
)

TIME_QUERY = lambda: Query(
    Variable(name="tzinfo", type="STRING", default=None),
    Variable(name="countryCode", type="STRING", default=None),
    Variable(name="datetimeUnit", type="STRING", default=None),
)

LOG_QUERY = lambda: Query(
    Variable(name="logName", type="STRING", default=None),
    Variable(name="logLevel", type="STRING", default="WARN"),
    Variable(name="logFile", type="STRING", default=None),
    Variable(name="localSave", type="BOOLEAN", default=None),
    Variable(name="debugPoint", type="[STRING]", default=None),
    Variable(name="killPoint", type="[STRING]", default=None),
    Variable(name="extraSave", type="[STRING]", default=None),
    Variable(name="interrupt", type="[STRING]", default=None),
)

REQUEST_QUERY = lambda: Query(
    Variable(name="numRetries", type="INTEGER", default=None),
    Variable(name="delay", type="[FLOAT]", default=None),
    Variable(name="cookies", type="STRING", default=None),
)

GCLOUD_QUERY = lambda: Query(
    Variable(name="queryList", type="[DICT]", default=list()),
    Variable(name="uploadList", type="[DICT]", default=list()),
    Variable(name="account", type="STRING", default=None),
)


###################################################################
########################### Spider Query ##########################
###################################################################

ITERATOR_QUERY = lambda: Query(
    Variable(name="iterateUnit", type="INTEGER", default=None),
    Variable(name="interval", type="STRING", default=None),
)

TASK_QUERY = lambda: Query(
    Variable(name="fromNow", type="[INTEGER]", default=None),
    Variable(name="discard", type="BOOLEAN", default=True),
    Variable(name="progress", type="BOOLEAN", default=True),
)

GATHER_QUERY = lambda: Query(
    Variable(name="where", type="STRING", default=str()),
    Variable(name="which", type="STRING", default=str()),
    Variable(name="verb", type="STRING", default=str()),
    Variable(name="by", type="STRING", default=str()),
    Variable(name="message", type="STRING", default=str()),
)


###################################################################
############################ Base Query ###########################
###################################################################

ASYNC_QUERY = lambda: Query(
    Variable(name="numTasks", type="INTEGER", default=100),
    Variable(name="apiRedirect", type="BOOLEAN", default=False),
    Variable(name="redirectUnit", type="INTEGER", default=None),
)

ENCRYPTED_QUERY = lambda: Query(
    Variable(name="encryptedKey", type="STRING", default=None),
    Variable(name="decryptedKey", type="STRING", default=None),
)

PIPELINE_QUERY = lambda: Query(
    Variable(name="globalMessage", type="STRING", default=str()),
    Variable(name="globalProgress", type="BOOLEAN", default=False),
    Variable(name="asyncMessage", type="STRING", default=str()),
    Variable(name="asyncProgress", type="BOOLEAN", default=False),
    Variable(name="taskProgress", type="BOOLEAN", default=True),
)


def get_base_query(asyncio=False, encrypted=False, mapped=False, pipeline=False, **kwargs) -> Query:
    return Query(
        *FILTER_QUERY(mapped),
        *TIME_QUERY(),
        *LOG_QUERY(),
        *REQUEST_QUERY(),
        *(list() if pipeline else TASK_QUERY()),
        *(list() if pipeline else GATHER_QUERY()),
        *(ASYNC_QUERY() if asyncio else list()),
        *GCLOUD_QUERY(),
        *(ENCRYPTED_QUERY() if encrypted else list()),
        *(PIPELINE_QUERY() if pipeline else list()),
    )


###################################################################
########################### Extra Query ###########################
###################################################################

class OptionalQuery(Query):
    dtype = Variable
    typeCheck = True

    def __init__(self, *variables: Variable):
        super().__init__(*[
            variable for variable in variables if isinstance(variable, Dict) and (variable.get(DEFAULT) != __OPTIONAL__)])


DATE_RANGE_QUERY = lambda startDate=__NONE__, endDate=__NONE__, interval=__OPTIONAL__: OptionalQuery(
    Variable(name="startDate", type="DATE", default=startDate),
    Variable(name="endDate", type="DATE", default=endDate),
    Variable(name="interval", type="STRING", default=interval),
)

PAGE_RANGE_QUERY = lambda size=__OPTIONAL__, pageSize=__OPTIONAL__, pageStart=__OPTIONAL__, offset=__OPTIONAL__: OptionalQuery(
    Variable(name="size", type="INTEGER", default=size),
    Variable(name="pageSize", type="INTEGER", default=pageSize),
    Variable(name="pageStart", type="INTEGER", default=pageStart),
    Variable(name="offset", type="INTEGER", default=offset),
)

ISIN_KEYWORD_QUERY = lambda exact=__OPTIONAL__, include=__OPTIONAL__, exclude=__OPTIONAL__: OptionalQuery(
    Variable(name="exact", type="[STRING]", default=exact),
    Variable(name="include", type="[STRING]", default=include),
    Variable(name="exclude", type="[STRING]", default=exclude),
)

LOGIN_QUERY = lambda userid=__NONE__, passwd=__NONE__: Query(
    Variable(name="userid", type="STRING", default=userid),
    Variable(name="passwd", type="STRING", default=passwd),
)
