from __future__ import annotations
from gscraper.base.types import _KT, _VT, ClassInstance, Index, MatchFunction

from gscraper.utils.map import iloc, kloc, notna_dict, exists_dict
from gscraper.utils.map import vloc, match_records, drop_duplicates, exists_one

from typing import Any, Callable, Dict, Iterable, List, Literal, Optional, Union
import functools


###################################################################
############################# Context #############################
###################################################################

UNIQUE_CONTEXT = lambda self=None, asyncio=None, operation=None, host=None, where=None, which=None, by=None, \
                        initTime=None, contextFields=None, iterateArgs=None, iterateProduct=None, \
                        pagination=None, pageUnit=None, pageLimit=None, fromNow=None, responseType=None, \
                        debug=None, localSave=None, extraSave=None, interrupt=None, \
                        logger=None, logJson=None, errors=None, ssl=None, match=None, hier=None, \
                        redirectArgs=None, redirectProduct=None, maxLimit=None, redirectLimit=None, \
                        root=None, groupby=None, countby=None, schemaInfo=None, schema=None, field=None, \
                        crawler=None, decryptedKey=None, auth=None, sessionCookies=None, dependencies=None, \
                        func=None, inplace=None, self_var=None, prefix=None, rename=None, **context: context


TASK_CONTEXT = lambda locals=None, how=None, default=None, dropna=None, strict=None, unique=None, \
                        drop=None, index=None, count=None, to=None, **context: UNIQUE_CONTEXT(**context)


REQUEST_CONTEXT = lambda session=None, semaphore=None, method=None, url=None, referer=None, messages=None, \
                        params=None, encode=None, data=None, json=None, headers=None, cookies=None, \
                        allow_redirects=None, validate=None, exception=None, valid=None, invalid=None, \
                        close=None, encoding=None, features=None, html=None, table_header=None, table_idx=None, \
                        engine=None, **context: TASK_CONTEXT(**context)


LOGIN_CONTEXT = lambda userid=None, passwd=None, domain=None, naverId=None, naverPw=None, \
                        **context: REQUEST_CONTEXT(**context)


API_CONTEXT = lambda clientId=None, clientSecret=None, **context: REQUEST_CONTEXT(**context)


RESPONSE_CONTEXT = lambda iterateUnit=None, logName=None, logLevel=None, logFile=None, \
                        delay=None, progress=None, message=None, numTasks=None, apiRedirect=None, \
                        redirectUnit=None, index=0, **context: \
                        dict(REQUEST_CONTEXT(**context), index=index)


UPLOAD_CONTEXT = lambda name=None, key=None, sheet=None, mode=None, cell=None, base_sheet=None, clear=None, \
                        default=None, head=None, headers=None, numericise_ignore=None, str_cols=None, arr_cols=None, \
                        to=None, rename=None, table=None, project_id=None, schema=None, base_query=None, \
                        progress=None, partition=None, prtition_by=None, base=None, **context: context


PROXY_CONTEXT = lambda queryInfo=None, uploadInfo=None, reauth=None, audience=None, credentials=None, \
                        **context: context


REDIRECT_CONTEXT = lambda apiRedirect=None, returnType=None, logFile=None, cookies=str(), **context: \
    PROXY_CONTEXT(**REQUEST_CONTEXT(**context), cookies=cookies)


LOCAL_CONTEXT = lambda apiRedirect=None, returnType=None, session=None, semaphore=None, **context: \
    PROXY_CONTEXT(**context)


###################################################################
########################### Custom Dict ###########################
###################################################################

class CustomDict(dict):
    def __init__(self, __m: Dict=dict(), **kwargs):
        super().update(self.__dict__)
        super().__init__(dict(__m, **kwargs))

    def copy(self, __instance: Optional[ClassInstance]=None) -> Union[Any,CustomDict]:
        if __instance: return __instance.__class__(**self)
        else: return self.__class__(**self)

    def get(self, __key: _KT, default=None, if_null: Literal["drop","pass"]="pass",
            reorder=True, values_only=True) -> Union[Any,Dict,List,str]:
        return kloc(dict(self), __key, default, if_null, reorder, values_only)

    def update(self, __m: Dict=dict(), inplace=True, self_var=False, **kwargs) -> Union[bool,CustomDict]:
        if not inplace: self = self.copy()
        if self_var: dict.update(self, self.__dict__)
        for __k, __v in dict(__m, **kwargs).items():
            self[__k] = __v
        return exists_one(inplace, self)

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
    def update_notna(self, __m: Dict=dict(), inplace=True, self_var=False, **kwargs) -> Union[bool,CustomDict]:
        return dict(__m, **notna_dict(kwargs))

    @updatable
    def update_exists(self, __m: Dict=dict(), inplace=True, self_var=False, **kwargs) -> Union[bool,CustomDict]:
        return dict(__m, **exists_dict(kwargs))

    def __setitem__(self, __key: _KT, __value: _VT):
        super().__setitem__(__key, __value)
        setattr(self, __key, __value)


class TypedDict(CustomDict):
    def update_default(self, __default: Dict=dict(), __how: Literal["notna","exists"]="notna",
                        inplace=True, self_var=False, **kwargs) -> Union[bool,CustomDict]:
        kwargs = {__k: __v for __k, __v in kwargs.items() if __default.get(__k) != __v}
        if __how == "notna": return self.update_notna(kwargs, inplace=inplace)
        else: return self.update_exists(kwargs, inplace=inplace, self_var=self_var)


###################################################################
########################### Custom List ###########################
###################################################################

class CustomList(list):
    def __init__(self,  __iterable: Iterable):
        super().__init__(__iterable)

    def copy(self, __instance: Optional[ClassInstance]=None) -> Union[Any,CustomList]:
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
        return exists_one(inplace, self)

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


###################################################################
########################## Custom Records #########################
###################################################################

class CustomRecords(CustomList):
    def __init__(self,  __iterable: Iterable):
        super().__init__([__i for __i in __iterable if isinstance(__i, Dict)])

    def copy(self, __instance: Optional[ClassInstance]=None) -> Union[Any,CustomRecords]:
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

    def copy(self, __instance: Optional[ClassInstance]=None) -> Union[Any,TypedRecords]:
        if __instance: return __instance.__class__(*self)
        else: return self.__class__(*self)
