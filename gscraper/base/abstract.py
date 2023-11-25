from __future__ import annotations
from gscraper.base.types import _KT, _VT, ClassInstance, Index, MatchFunction

from gscraper.utils.map import iloc, kloc, notna_dict, exists_dict
from gscraper.utils.map import vloc, match_records, drop_duplicates

from typing import Any, Callable, Dict, Iterable, List, Literal, Optional, Union
import functools


###################################################################
############################# Context #############################
###################################################################

BASE_CONTEXT = lambda self=None, operation=None, initTime=None, prefix=None, rename=None, \
                        inplace=None, self_var=None, **context: context


LOG_CONTEXT = lambda logger=None, logJson=None, errors=None, func=None, **context: context


ITERATOR_CONTEXT = lambda iterator=None, iterateArgs=None, iterateCount=None, iterateProduct=None, pagination=None, \
                        pageFrom=None, offsetFrom=None, pageUnit=None, pageLimit=None, fromNow=None, __i=None, \
                        **context: context


MAP_CONTEXT = lambda responseType=None, match=None, root=None, groupby=None, countby=None, schemaInfo=None, \
                    schema=None, __index=None, **context: context


SPIDER_CONTEXT = lambda asyncio=None, host=None, field=None, ssl=None, mappedReturn=None, **context: context


ASYNCIO_CONTEXT = lambda redirectArgs=None, redirectProduct=None, maxLimit=None, redirectLimit=None, **context: context


ENCRYPTED_CONTEXT = lambda decryptedKey=None, auth=None, authKey=None, sessionCookies=None, **context: context


UNIQUE_CONTEXT = lambda derivFields=None, dags=None, **context: \
    ENCRYPTED_CONTEXT(**ASYNCIO_CONTEXT(**SPIDER_CONTEXT(
        **MAP_CONTEXT(**ITERATOR_CONTEXT(**LOG_CONTEXT(**BASE_CONTEXT(**context)))))))


PARAMS_CONTEXT = lambda init=None, data=None, task=None, worker=None, locals=None, which=None, where=None, by=None, \
                        how=None, default=None, dropna=None, strict=None, unique=None, drop=None, index=None, log=None, \
                        depth=None, hier=None, to=None, countPath=None, hasSize=None, **context: context


REQUEST_CONTEXT = lambda session=None, semaphore=None, method=None, url=None, referer=None, messages=None, \
                        params=None, encode=None, data=None, json=None, headers=None, cookies=None, \
                        allow_redirects=None, validate=None, exception=None, valid=None, invalid=None, \
                        close=None, encoding=None, features=None, html=None, table_header=None, table_idx=None, \
                        engine=None, **context: context


RESPONSE_CONTEXT = lambda iterateUnit=None, logName=None, logLevel=None, logFile=None, \
                        delay=None, progress=None, message=None, numTasks=None, apiRedirect=None, \
                        redirectUnit=None, **context: context


GCLOUD_CONTEXT = lambda name=None, key=None, sheet=None, mode=None, cell=None, base_sheet=None, clear=None, \
                        default=None, head=None, headers=None, numericise_ignore=None, str_cols=None, arr_cols=None, \
                        to=None, rename=None, table=None, project_id=None, schema=None, base_query=None, \
                        progress=None, partition=None, prtition_by=None, base=None, **context: context


UPLOAD_CONTEXT = lambda queryInfo=None, uploadInfo=None, reauth=None, audience=None, credentials=None, \
                        **context: context


TASK_CONTEXT = lambda **context: UPLOAD_CONTEXT(**PARAMS_CONTEXT(**context))


SESSION_CONTEXT = lambda session=None, semaphore=None, cookies=str(), **context: \
                        dict(UPLOAD_CONTEXT(**REQUEST_CONTEXT(**PARAMS_CONTEXT(**context))), cookies=cookies)


LOGIN_CONTEXT = lambda userid=None, passwd=None, **context: SESSION_CONTEXT(**context)


PROXY_CONTEXT = lambda session=None, semaphore=None, **context: UNIQUE_CONTEXT(**UPLOAD_CONTEXT(**context))


LOCAL_CONTEXT = lambda apiRedirect=None, returnType=None, **context: SESSION_CONTEXT(**context)


REDIRECT_CONTEXT = lambda apiRedirect=None, returnType=None, logFile=None, **context: SESSION_CONTEXT(**context)


###################################################################
########################### Custom Dict ###########################
###################################################################

class CustomDict(dict):
    def __init__(self, __m: Dict=dict(), **kwargs):
        super().update(self.__dict__)
        super().__init__(dict(__m, **kwargs))

    def copy(self, __instance: Optional[ClassInstance]=None) -> Union[CustomDict,Any]:
        if __instance: return __instance.__class__(**self)
        else: return self.__class__(**self)

    def get(self, __key: _KT, default=None, if_null: Literal["drop","pass"]="pass",
            reorder=True, values_only=True) -> Union[Any,Dict,List,str]:
        return kloc(dict(self), __key, default, if_null, reorder, values_only)

    def update(self, __m: Dict=dict(), inplace=True, self_var=False, **kwargs) -> CustomDict:
        if not inplace: self = self.copy()
        if self_var: dict.update(self, self.__dict__)
        for __k, __v in dict(__m, **kwargs).items():
            self[__k] = __v
        if not inplace: return self

    def updatable(func):
        @functools.wraps(func)
        def wrapper(self: CustomDict, *args, inplace=True, self_var=False, **kwargs):
            if not inplace: self = self.copy()
            context = dict(inplace=inplace, self_var=self_var)
            return self.update(func(self, *args, **kwargs, **context), **context)
        return wrapper

    def copyable(func):
        @functools.wraps(func)
        def wrapper(self: CustomDict, *args, inplace=False, self_var=False, **kwargs):
            if not inplace: self = self.copy()
            context = dict(inplace=inplace, self_var=self_var)
            return self.update(func(self, *args, **kwargs, **context), **context)
        return wrapper

    @updatable
    def update_notna(self, __m: Dict=dict(), inplace=True, self_var=False, **kwargs) -> CustomDict:
        return dict(__m, **notna_dict(kwargs))

    @updatable
    def update_exists(self, __m: Dict=dict(), inplace=True, self_var=False, **kwargs) -> CustomDict:
        return dict(__m, **exists_dict(kwargs))

    def __setitem__(self, __key: _KT, __value: _VT):
        super().__setitem__(__key, __value)
        setattr(self, __key, __value)


class TypedDict(CustomDict):
    def copy(self, __instance: Optional[ClassInstance]=None) -> Union[TypedDict,Any]:
        return super().copy(__instance)

    def update(self, __m: Dict=dict(), inplace=True, self_var=False, **kwargs) -> TypedDict:
        return super().update(__m, inplace=inplace, self_var=self_var, **kwargs)

    def update_default(self, __default: Dict=dict(), __how: Literal["notna","exists"]="notna",
                        inplace=True, self_var=False, **kwargs) -> Union[bool,TypedDict]:
        kwargs = {__k: __v for __k, __v in kwargs.items() if __default.get(__k) != __v}
        if __how == "notna": return self.update_notna(kwargs, inplace=inplace)
        else: return self.update_exists(kwargs, inplace=inplace, self_var=self_var)


###################################################################
########################### Custom List ###########################
###################################################################

class CustomList(list):
    def __init__(self,  __iterable: Iterable):
        super().__init__(__iterable)

    def copy(self, __instance: Optional[ClassInstance]=None) -> Union[CustomList,Any]:
        if __instance: return __instance.__class__(self)
        else: return self.__class__(self)

    def get(self, __key: Index, default=None, if_null: Literal["drop","pass"]="pass") -> Union[Any,List,str]:
        return iloc(list(self), __key, default, if_null)

    def add(self, __iterable: Iterable):
        for __i in __iterable:
            self.append(__i)

    def update(self, __iterable: Iterable, inplace=True):
        if not inplace: self = self.copy()
        self.clear()
        self.add(__iterable)
        if not inplace: return self

    def updatable(func):
        @functools.wraps(func)
        def wrapper(self: CustomList, *args, inplace=True, **kwargs):
            if not inplace: self = self.copy()
            return self.update(func(self, *args, inplace=inplace, **kwargs), inplace=inplace)
        return wrapper

    def copyable(func):
        @functools.wraps(func)
        def wrapper(self: CustomList, *args, inplace=False, **kwargs):
            if not inplace: self = self.copy()
            return self.update(func(self, *args, inplace=inplace, **kwargs), inplace=inplace)
        return wrapper


class TypedList(CustomList):
    def __init__(self,  *args):
        super().__init__(args)

    def copy(self, __instance: Optional[ClassInstance]=None) -> Union[TypedList,Any]:
        if __instance: return __instance.__class__(*self)
        else: return self.__class__(*self)


###################################################################
########################## Custom Records #########################
###################################################################

class CustomRecords(CustomList):
    def __init__(self,  __iterable: Iterable):
        super().__init__([__i for __i in __iterable if isinstance(__i, Dict)])

    def copy(self, __instance: Optional[ClassInstance]=None) -> Union[CustomRecords,Any]:
        if __instance: return __instance.__class__(self)
        else: return self.__class__(self)

    def get(self, __key: _KT, default=None, if_null: Literal["drop","pass"]="pass",
            reorder=True, values_only=True) -> Union[Any,List,Dict,str]:
        return vloc(list(self), __key, default, if_null, reorder, values_only)

    def add(self, __iterable: Iterable):
        for __i in __iterable:
            if isinstance(__i, Dict): self.append(__i)

    @CustomList.copyable
    def map(self, __func: Callable, inplace=False, **kwargs) -> Union[bool,CustomRecords]:
        return [__func(__m, **kwargs) for __m in self]

    @CustomList.copyable
    def filter(self, __match: Optional[MatchFunction]=None, inplace=False, **match_by_key) -> Union[bool,CustomRecords]:
        if isinstance(__match, Callable) or match_by_key:
            all_keys = isinstance(__match, Callable)
            return match_records(self, all_keys=all_keys, match=__match, **match_by_key)
        else: return self

    @CustomList.copyable
    def unique(self, keep: Literal["fist","last",True,False]="first", inplace=False) -> Union[bool,CustomRecords]:
        return drop_duplicates(self, keep=keep) if keep != True else self


class TypedRecords(CustomRecords):
    def __init__(self,  *args):
        super().__init__(args)

    def copy(self, __instance: Optional[ClassInstance]=None) -> Union[TypedRecords,Any]:
        if __instance: return __instance.__class__(*self)
        else: return self.__class__(*self)
