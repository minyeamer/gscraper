from __future__ import annotations
from gscraper.base.types import _KT, _VT, ClassInstance, Index, MatchFunction
from gscraper.base.types import is_array

from gscraper.utils.map import iloc, kloc, vloc, match_records, drop_duplicates, exists_one

from typing import Any, Callable, Dict, List, Literal, Optional, Union
import functools
import re


to_snake_case = lambda __s=str(): re.sub(r"(?<!^)(?=[A-Z])", '_', str(__s)).lower()
to_camel_case = lambda __s=str(): ''.join([s.capitalize() if __i > 0 else s for __i, s in enumerate(str(__s).split('_'))])


###################################################################
############################# Context #############################
###################################################################

UNIQUE_CONTEXT = lambda self=None, asyncio=None, operation=None, host=None, where=None, which=None, by=None, \
                        initTime=None, contextFields=None, iterateArgs=None, iterateProduct=None, \
                        pagination=None, pageUnit=None, pageLimit=None, fromNow=None, \
                        debug=None, localSave=None, extraSave=None, interrupt=None, \
                        logger=None, logJson=None, errors=None, ssl=None, \
                        redirectArgs=None, redirectProduct=None, maxLimit=None, redirectLimit=None, \
                        root=None, groupby=None, rankby=None, schemaInfo=None, \
                        crawler=None, decryptedKey=None, auth=None, sessionCookies=None, dependencies=None, \
                        self_var=None, prefix=None, rename=None, **context: context


TASK_CONTEXT = lambda locals=None, how=None, default=None, dropna=None, strict=None, unique=None, \
                        drop=None, index=None, count=None, **context: UNIQUE_CONTEXT(**context)


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


SCHEMA_CONTEXT = lambda schema=None, root=None, by=None, count=None, discard=None, name=None, path=None, \
                        type=None, mode=None, description=None, cast=None, strict=None, default=None, \
                        apply=None, match=None, how=None, query=None, **context: context


UPLOAD_CONTEXT = lambda key=None, sheet=None, mode=None, base_sheet=None, cell=None, \
                        table=None, project_id=None, schema=None, progress=None, \
                        partition=None, prtition_by=None, base=None, **context: context


PROXY_CONTEXT = lambda queryInfo=None, uploadInfo=None, **context: context


REDIRECT_CONTEXT = lambda apiRedirect=None, returnType=None, renameMap=None, logFile=None, **context: \
    PROXY_CONTEXT(**REQUEST_CONTEXT(**context))


###################################################################
########################### Custom Dict ###########################
###################################################################

def custom_str(__object, indent=0, step=2) -> str:
    if isinstance(__object, (CustomDict,CustomList)):
        return __object.__str__(indent=indent+step, step=step)
    elif isinstance(__object, str): return f"'{__object}'"
    else: return str(__object)


class CustomDict(dict):
    def __init__(self, **kwargs):
        super().update(self.__dict__)
        super().__init__(kwargs)

    def copy(self, __instance: Optional[ClassInstance]=None) -> Union[Any,CustomDict]:
        if __instance: return __instance.__class__(**self)
        else: return self.__class__(**self)

    def get(self, __key: _KT, default=None, if_null: Literal["drop","pass"]="pass",
            reorder=True, values_only=True) -> Union[Any,Dict,List,str]:
        return kloc(dict(self), __key, default, if_null, reorder, values_only)

    def update(self, __m: Optional[Dict]=dict(), inplace=True, **kwargs) -> Union[bool,CustomDict]:
        if not inplace: self = self.copy()
        for __key, __value in dict(kwargs, **__m).items():
            if inplace: setattr(self, __key, __value)
            else: self[__key] = __value
        if inplace: super().update(self.__dict__)
        return exists_one(inplace, self, strict=False)

    def __getitem__(self, __key: _KT) -> _VT:
        if isinstance(__key, List): return [self.__getitem__(__k) for __k in __key]
        else: return super().__getitem__(__key)

    def __setitem__(self, __key: _KT, __value: _VT):
        if isinstance(__key, List) and is_array(__value):
            [self.__setitem__(__k, __v) for __k, __v in zip(__key, __value)]
            return
        setattr(self, __key, __value)
        super().__setitem__(__key, __value)

    def __str__(self, indent=2, step=2) -> str:
        return '{\n'+',\n'.join([' '*indent+f"'{__k}': {custom_str(__v, indent=indent, step=step)}"
                for __k, __v in self.items()])+'\n'+' '*(indent-step)+'}'


###################################################################
########################### Custom List ###########################
###################################################################

class CustomList(list):
    def __init__(self, *args):
        super().__init__(args)

    def copy(self, __instance: Optional[ClassInstance]=None) -> Union[Any,CustomList]:
        if __instance: return __instance.__class__(*self)
        else: return self.__class__(*self)

    def get(self, __key: Index, default=None, if_null: Literal["drop","pass"]="pass") -> Union[Any,List,str]:
        return iloc(list(self), __key, default, if_null)

    def update(self, *args):
        self.__init__(*args)

    def __str__(self, indent=2, step=2) -> str:
        return '[\n'+',\n'.join([' '*indent+custom_str(__e, indent=indent, step=step)
                for __e in map(dict, self)])+'\n'+' '*(indent-step)+']'


class CustomRecords(CustomList):
    def __init__(self, *args):
        super().__init__(*[record for record in args if isinstance(record, Dict)])

    def copy(self, __instance: Optional[ClassInstance]=None) -> Union[Any,CustomRecords]:
        if __instance: return __instance.__class__(*self)
        else: return self.__class__(*self)

    def get(self, __key: _KT, default=None, if_null: Literal["drop","pass"]="pass",
            reorder=True, values_only=True) -> Union[Any,List,Dict,str]:
        return vloc(list(self), __key, default, if_null, reorder, values_only)

    def copy_or_update(func):
        @functools.wraps(func)
        def wrapper(self: CustomRecords, *args, inplace=False, **kwargs):
            if not inplace: self = self.copy()
            __r = func(self, *args, **kwargs)
            if inplace: self.update(*__r)
            else: self = self.__class__(*__r)
            return exists_one(inplace, self, strict=False)
        return wrapper

    @copy_or_update
    def map(self, __func: Callable, inplace=False, **kwargs) -> Union[bool,CustomRecords]:
        return [__func(__m, **kwargs) for __m in self]

    @copy_or_update
    def filter(self, __match: Optional[MatchFunction]=None, inplace=False, **match_by_key) -> Union[bool,CustomRecords]:
        if isinstance(__match, Callable) or match_by_key:
            all_keys = isinstance(__match, Callable)
            return match_records(self, all_keys=all_keys, match=__match, **match_by_key)
        else: return self

    @copy_or_update
    def unique(self, keep: Literal["fist","last",True,False]="first", inplace=False) -> Union[bool,CustomRecords]:
        return drop_duplicates(self, keep=keep) if keep != True else self

