from __future__ import annotations
from .context import UNIQUE_CONTEXT, REQUEST_CONTEXT, PROXY_CONTEXT, REDIRECT_CONTEXT
from .types import _KT, _VT, _PASS, ClassInstance, Context, ContextMapper, TypeHint, LogLevel
from .types import RenameMap, IndexLabel, EncryptedKey, Status, Pages, Unit, DateFormat, DateQuery, Timedelta, Timezone
from .types import JsonData, RedirectData, Records, TabularData, Data
from .types import SchemaInfo, Account, NumericiseIgnore, BigQuerySchema, GspreadReadInfo, UploadInfo
from .types import is_array, allin_instance, is_int_array, is_records, init_origin

from .cast import cast_list, cast_tuple, cast_int, cast_date, cast_datetime_format
from .date import now, get_date, get_date_range
from .gcloud import fetch_gcloud_authorization, update_gspread, read_gspread
from .gcloud import IDTokenCredentials, read_gbq, to_gbq, validate_upsert_key
from .logs import CustomLogger, log_encrypt, log_messages, log_response, log_client, log_data, log_exception, log_table
from .map import exists, is_empty, unique, to_array, diff
from .map import iloc, get_scala, fill_array, is_same_length, unit_array, concat_array, align_array, transpose_array
from .map import kloc, apply_dict, chain_dict, drop_dict, cloc, apply_df, merge_drop
from .map import exists_one, convert_data, filter_data, chain_exists, set_data, data_empty
from .parse import parse_cookies, parse_origin, validate_schema, parse_schema

from abc import ABCMeta, abstractmethod
from urllib.parse import urlparse
import asyncio
import aiohttp
import functools
import requests
import time

from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple, Union
from json import JSONDecodeError
import datetime as dt
import json
import logging
import pandas as pd

from tqdm.auto import tqdm
from itertools import product
import base64
import random


EXAMPLE_URL = "https://example.com"

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "sec-ch-ua": '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
}

MIN_ASYNC_TASK_LIMIT = 1
MAX_ASYNC_TASK_LIMIT = 100
MAX_REDIRECT_LIMIT = 10

GATHER_MSG = lambda which, where: f"Collecting {which} from {where}"
INVALID_STATUS_MSG = lambda where: f"Response from {where} is not valid."

REDIRECT_MSG = lambda operation: f"{operation} operation is redirecting"
INVALID_REDIRECT_LOG_MSG = "Please verify that the both results and errors are in redierct returns."

INVALID_USER_INFO_MSG = lambda where=str(): f"{where} user information is not valid.".strip()
LOGIN_REQUIRED_MSG = lambda where=str(): f"{where} login information is required.".strip()

DEPENDENCY_HAS_NO_NAME_MSG = "Dependency has no operation name. Please define operation name."

WHERE, WHICH = "urls", "data"

KEY, SHEET, FIELDS = "key", "sheet", "fields"
TABLE, PID = "table", "project_id"


encrypt = lambda s=str(), count=0, *args: encrypt(
    base64.b64encode(str(s).encode("utf-8")).decode("utf-8"), count-1) if count else s
decrypt = lambda s=str(), count=0, *args: decrypt(
    base64.b64decode(str(s).encode("utf-8")).decode("utf-8"), count-1) if count else s


def get_cookies(session, url=None) -> str:
    if isinstance(session, aiohttp.ClientSession):
        return "; ".join([str(key)+"="+str(value) for key,value in session.cookie_jar.filter_cookies(url).items()])
    elif isinstance(session, requests.Session):
        return "; ".join([str(key)+"="+str(value) for key,value in dict(session.cookies).items()])


def get_content_type(content_type=str(), urlencoded=False, utf8=False) -> str:
    if urlencoded or content_type == "urlencoded":
        __type = "application/x-www-form-urlencoded"
    elif content_type == "json": __type = "application/json"
    elif content_type == "text": __type = "text/plain"
    else: return str()
    return __type+(("; charset=UTF-8") if utf8 else str())


def get_headers(authority=str(), referer=str(), cookies=str(), host=str(),
                origin: Union[bool,str]=False, secure=False,
                content_type=str(), urlencoded=False, utf8=False, xml=False, **kwargs) -> Dict:
    headers = HEADERS.copy()
    if authority: headers["Authority"] = urlparse(authority).hostname
    if referer: headers["referer"] = referer
    if host: headers["Host"] = urlparse(host).hostname
    if origin: headers["Origin"] = parse_origin(origin if isinstance(origin, str) else (authority if authority else host))
    if cookies: headers["Cookie"] = cookies
    if secure: headers["Upgrade-Insecure-Requests"] = "1"
    if content_type or urlencoded:
        headers["Content-Type"] = get_content_type(content_type, urlencoded, utf8)
    if xml: headers["X-Requested-With"] = "XMLHttpRequest"
    return dict(headers, **kwargs)


def get_page_range(size: Union[int,Sequence[int]], pageSize: int, start=1, pageUnit=0, pageLimit=0) -> Pages:
    if (pageUnit > 0) and (pageSize & pageUnit != 0):
        pageSize = round(pageSize / pageUnit) * pageUnit
    if pageLimit > 0:
        pageSize = min(pageSize, pageLimit)
    if isinstance(size, int): return tuple(range(start, (((size-1)//pageSize)+1)+1))
    elif is_int_array(size, how="all"):
        return [tuple(range(start, (((__size-1)//pageSize)+1)+1)) for __size in size]


###################################################################
############################### Base ##############################
###################################################################

class CustomDict(dict):
    def __init__(self, **kwargs):
        super().__init__(kwargs)

    def copy(self, __instance: Optional[ClassInstance]=None) -> Any:
        if __instance: return __instance.__class__(**self.__dict__)
        else: return self.__class__(**self.__dict__)

    def get(self, __key: _KT, default=None, if_null: Literal["drop","pass"]="drop",
            reorder=True, values_only=True) -> Union[Any,Dict,List,str]:
        return kloc(self.__dict__, __key, default=default, if_null=if_null, reorder=reorder, values_only=values_only)

    def update(self, __m: Optional[Dict]=dict(), inplace=True, **kwargs) -> Union[bool,Dict]:
        if not inplace: self = self.copy()
        for __key, __value in dict(kwargs, **__m).items():
            setattr(self, __key, __value)
        super().update(self.__dict__)
        return exists_one(inplace, self)

    def __getitem__(self, __keys: _KT) -> _VT:
        if is_array(__keys):
            return [super().__getitem__(__key) for __key in __keys]
        else:
            return super().__getitem__(__keys)

    def __setitem__(self, __key: _KT, __value: _VT):
        setattr(self, __key, __value)
        super().__setitem__(__key, __value)


class BaseSession(CustomDict):
    __metaclass__ = ABCMeta
    operation = "session"
    where = WHERE
    which = WHICH
    fields = list()
    iterateArgs = list()
    iterateQuery = list()
    iterateUnit = 1
    interval = str()
    datetimeUnit = "second"
    tzinfo = None
    responseType = None
    returnType = None
    errors = list()
    renameMap = dict()
    schemaInfo = dict()

    def __init__(self, fields: IndexLabel=list(), contextFields: Optional[IndexLabel]=None,
                iterateUnit: Optional[Unit]=None, interval: Optional[Timedelta]=None,
                startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                datetimeUnit: Literal["second","minute","hour","day","month","year"]="second",
                tzinfo: Optional[Timezone]=None, returnType: Optional[TypeHint]=None,
                logName=str(), logLevel: LogLevel="WARN", logFile=str(), debug=False,
                renameMap: RenameMap=dict(), schemaInfo: SchemaInfo=dict(), **context):
        super().__init__()
        self.operation = self.operation
        self.fields = fields if fields else self.fields
        self.iterateUnit = iterateUnit if iterateUnit else self.iterateUnit
        self.interval = interval if interval else self.interval
        self.set_date(startDate=startDate, endDate=endDate, **context)
        self.datetimeUnit = datetimeUnit if datetimeUnit else self.datetimeUnit
        self.tzinfo = tzinfo if tzinfo else self.tzinfo
        self.initTime = now(tzinfo=self.tzinfo, droptz=True, unit=self.datetimeUnit)
        self.returnType = returnType if returnType else returnType
        self.renameMap = renameMap if renameMap else self.renameMap
        self.set_logger(logName, logLevel, logFile, debug, **context)
        self.set_schema(schemaInfo, **context)
        self.set_context(contextFields=contextFields, **context)

    def set_date(self, startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None, **context):
        if self.interval or (startDate != None) or (endDate != None):
            startDate, endDate = get_date(startDate, if_null=None), get_date(endDate, if_null=None)
            self.startDate = min(startDate, endDate) if startDate and endDate else startDate
            self.endDate = max(startDate, endDate) if startDate and endDate else endDate

    def set_logger(self, logName=str(), logLevel: LogLevel="WARN", logFile=str(), debug=False, **context):
        logName = logName if logName else self.operation
        self.logLevel = int(logLevel) if str(logLevel).isdigit() else logging.getLevelName(str(logLevel).upper())
        self.logFile = logFile
        self.logJson = bool(logFile)
        self.logger = CustomLogger(name=logName, level=self.logLevel, file=self.logFile)
        self.debug = debug

    def set_schema(self, schemaInfo: SchemaInfo=dict(), **context):
        schemaInfo = schemaInfo if schemaInfo else self.schemaInfo
        if schemaInfo:
            for __key, schemaContext in schemaInfo.copy().items():
                schemaInfo[__key]["schema"] = validate_schema(schemaContext["schema"])
            self.schemaInfo = schemaInfo

    def set_context(self, contextFields: Optional[IndexLabel]=None, **context):
        self.update(kloc(context, contextFields, if_null="pass") if contextFields else context)

    def now(self, __format=str(), days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0,
            hours=0, weeks=0, droptz=True) -> dt.datetime:
        return now(__format, days, seconds, microseconds, milliseconds, minutes, hours, weeks,
                    tzinfo=self.tzinfo, droptz=droptz, unit=self.datetimeUnit)

    def today(self, __format=str(), days=0, weeks=0, droptz=True) -> dt.date:
        unit = self.datetimeUnit if self.datetimeUnit in ["month","year"] else None
        return now(__format, days, weeks, tzinfo=self.tzinfo, droptz=droptz, droptime=True, unit=unit)

    def get_rename_map(self, to=None, **kwargs) -> RenameMap:
        return self.renameMap

    def extra_save(self, data: Data, prefix=str(), extraSave=False, to=None, **kwargs):
        if not extraSave: return
        file = prefix+'_' if prefix else str()+self.now("%Y%m%d%H%M%S")+".xlsx"
        renameMap = self.get_rename_map(to=to, **kwargs)
        convert_data(data, return_type="dataframe").rename(columns=renameMap).to_excel(file, index=False)

    ###################################################################
    ########################### Log Managers ##########################
    ###################################################################

    def log_errors(func):
        @functools.wraps(func)
        def wrapper(self: BaseSession, *args, **kwargs):
            try: return func(self, *args, **kwargs)
            except KeyboardInterrupt as interrupt:
                raise interrupt
            except Exception as exception:
                self.log_context(**kwargs)
                if self.debug: raise exception
                func_name = f"{func.__name__}({self.__class__.__name__})"
                self.logger.error(log_exception(func_name, json=self.logJson, **kwargs))
                return init_origin(func)
        return wrapper

    def log_context(self, **context: Context):
        query = kloc(context, self.iterateArgs+self.iterateQuery, if_null="drop")
        self.errors.append(query)

    def validate_response(func):
        @functools.wraps(func)
        def wrapper(self: BaseSession, response: Any, *args, **kwargs):
            is_valid = self.is_valid_response(response)
            results = func(self, response, *args, **kwargs) if is_valid else init_origin(func)
            self.logger.info(log_data(results, **self.get_iterator(**kwargs)))
            return results
        return wrapper

    def is_valid_response(self, response: Any) -> bool:
        return exists(response, strict=False)

    ###################################################################
    ############################# Iterator ############################
    ###################################################################

    def get_iterator(self, keys_only=False, values_only=False, **context) -> Union[Context,Sequence[_VT]]:
        iterateDate = ("startDate", "endDate", "date") if self.interval else tuple()
        query = unique(*self.iterateArgs, *self.iterateQuery, *iterateDate)
        if keys_only: return query
        else: return kloc(context, query, if_null="drop", values_only=values_only)

    def set_iterator(self, *args, iterateArgs: List[_KT]=list(), iterateQuery: List[_KT]=list(),
                    iterateUnit: Unit=1, interval: Timedelta=str(), **context) -> Tuple[List[Context],Context]:
        arguments, periods, iterator = list(), list(), list()
        if is_same_length(args, iterateArgs, empty=False) and is_same_length(*args):
            arguments = self.from_args(*args, iterateArgs=iterateArgs, iterateUnit=get_scala(iterateUnit))
            iterateQuery = diff(iterateQuery, iterateArgs)
            if is_array(iterateQuery) and len(iterateQuery) > 1: iterateUnit = iterateUnit[1:]
        if interval:
            periods, context = self.from_date(interval=interval, **context)
            iterateQuery = diff(iterateQuery, ("startDate", "endDate", "date", "interval"))
            context = dict(context, interval=interval)
        iterator, context = self.from_context(iterateQuery=iterateQuery, iterateUnit=iterateUnit, **context)
        iterator = self.product_iterator(arguments, periods, iterator)
        return iterator, context

    def from_args(self, *args, iterateArgs: List[_KT]=list(), iterateUnit: int=1) -> List[Context]:
        tuple_args = list(map(lambda x: allin_instance(x, Tuple, empty=False), args))
        if True in tuple_args:
            args = self.product_args(*args, index=tuple_args)
        if iterateUnit > 1:
            args = list(map(lambda __s: unit_array(__s, unit=iterateUnit), args))
        return [dict(zip(iterateArgs,values)) for values in zip(*args)]

    def product_args(self, *args, index: Sequence[bool]) -> List[List]:
        base = list()
        for __arg in zip(*args):
            tuples, others = iloc(__arg, index), iloc(__arg, list(map(lambda x: (not x), index)))
            __product = list(product((others,), *tuples))
            base += [concat_array(__s[1:], __s[0], index) for __s in __product]
        return transpose_array(base, count=len(args))

    def from_date(self, startDate: Optional[dt.date]=None, endDate: Optional[dt.date]=None,
                    interval: Timedelta="D", **context) -> Tuple[List[DateQuery],Context]:
        startDate = startDate if isinstance(startDate, dt.date) else cast_date(startDate, default=self.get("startDate"))
        endDate = endDate if isinstance(endDate, dt.date) else cast_date(endDate, default=self.get("endDate"))
        date_range = get_date_range(startDate, endDate, interval=interval)
        if (interval in ("D","1D")) or (str(interval).startswith("1 day")):
            period = [dict(date=date) for date in date_range]
            context = drop_dict(context, "date", inplace=False)
        elif len(date_range) > 1:
            period = [dict(startDate=start, endDate=(end-dt.timedelta(days=1)))
                        for start, end in zip(date_range, date_range[1:]+[endDate+dt.timedelta(days=1)])]
        else: period = [dict(startDate=startDate, endDate=endDate)]
        return period, context

    def from_context(self, iterateQuery: List[_KT], iterateUnit: Unit=1, **context) -> Tuple[List[Context],Context]:
        if not iterateQuery: return list(), context
        query, context = kloc(context, iterateQuery, if_null="drop"), drop_dict(context, iterateQuery, inplace=False)
        iterateUnit = cast_list(iterateUnit)
        if any(map(lambda x: x>1, iterateUnit)): query = self.group_context(iterateQuery, iterateUnit, **query)
        else: query = [dict(zip(query.keys(), values)) for values in product(*map(cast_tuple, query.values()))]
        return query, context

    def group_context(self, iterateQuery: List[_KT], iterateUnit: Unit=1, **context) -> List[Context]:
        query = apply_dict(context, apply=cast_list, all_keys=True)
        keys, unit = query.keys(), fill_array(iterateUnit, count=len(iterateQuery), value=1)
        combinations = product(*[range(0, len(query[key]), unit[i]) for i, key in enumerate(keys)])
        return [{key: query[key][index:index+unit[i]] for i, (key, index) in enumerate(zip(keys, indices))}
                for indices in combinations]

    def product_iterator(self, *iterator: Sequence[Context]) -> List[Context]:
        if sum(map(len, iterator)) == 0: return list()
        iterator_array = map((lambda x: x if x else [{}]), iterator)
        return list(map(chain_dict, product(*iterator_array)))


###################################################################
############################# Spiders #############################
###################################################################

class Spider(BaseSession):
    __metaclass__ = ABCMeta
    asyncio = False
    operation = "spider"
    where = WHERE
    which = WHICH
    fields = list()
    iterateArgs = list()
    iterateQuery = list()
    iterateUnit = 1
    interval = str()
    datetimeUnit = "second"
    tzinfo = None
    returnType = "records"
    renameMap = dict()
    message = str()

    def __init__(self, fields: IndexLabel=list(), contextFields: Optional[ContextMapper]=None,
                iterateUnit: Optional[Unit]=None, interval: Optional[Timedelta]=None,
                startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                datetimeUnit: Literal["second","minute","hour","day","month","year"]="second",
                tzinfo: Optional[Timezone]=None, returnType: Optional[TypeHint]=None,
                logName=str(), logLevel: LogLevel="WARN", logFile: Optional[str]=str(), debug=False,
                renameMap: RenameMap=dict(), schemaInfo: SchemaInfo=dict(), delay: Union[float,int,Tuple[int]]=1.,
                progress=True, message=str(), queryInfo: Optional[GspreadReadInfo]=dict(), **context):
        super().__init__(fields=fields, iterateUnit=iterateUnit, interval=interval, startDate=startDate, endDate=endDate,
                        datetimeUnit=datetimeUnit, tzinfo=tzinfo, returnType=returnType, logName=logName, logLevel=logLevel,
                        logFile=logFile, debug=debug, renameMap=renameMap, schemaInfo=schemaInfo)
        self.delay = delay
        self.progress = progress
        self.message = message if message else self.message
        self.set_context(contextFields=contextFields, **UNIQUE_CONTEXT(**context))
        self.set_query(queryInfo, **context)

    ###################################################################
    ######################### Session Managers ########################
    ###################################################################

    def requests_task(func):
        @functools.wraps(func)
        def wrapper(self: Spider, *args, self_var=True, uploadInfo: Optional[UploadInfo]=dict(), **kwargs):
            if self_var: kwargs = UNIQUE_CONTEXT(**dict(self.__dict__, **kwargs))
            data = func(self, *args, **kwargs)
            self.upload_data(data, uploadInfo, **kwargs)
            return data
        return wrapper

    def requests_session(func):
        @functools.wraps(func)
        def wrapper(self: Spider, *args, self_var=True, uploadInfo: Optional[UploadInfo]=dict(), **kwargs):
            if self_var: kwargs = UNIQUE_CONTEXT(**REQUEST_CONTEXT(**dict(self.__dict__, **kwargs)))
            with requests.Session() as session:
                data = func(self, *args, session=session, **kwargs)
            time.sleep(.25)
            self.upload_data(data, uploadInfo, **kwargs)
            return data
        return wrapper

    def requests_limit(func):
        @functools.wraps(func)
        def wrapper(self: Spider, *args, **kwargs):
            if self.delay: time.sleep(self.get_delay())
            return func(self, *args, **kwargs)
        return wrapper

    def get_delay(self, tsUnit: Literal["ms","s"]="ms") -> Union[float,int]:
        if isinstance(self.delay, (float,int)): return self.delay
        elif isinstance(self.delay, Tuple):
            random.randrange(*self.delay[:2])/(1000 if tsUnit == "ms" else 1)
        else: return 0.

    ###################################################################
    ######################### Gather Requests #########################
    ###################################################################

    @abstractmethod
    @requests_session
    def crawl(self, *args, **context) -> Data:
        return self.gather(*self.map_args(*args), **self.map_context(**context))

    def map_args(self, *args, how: Literal["min","max","first"]="min", default=None,
                dropna=True, strict=False, unique=True) -> Tuple[List]:
        if len(args) == 1: return (to_array(args[0], default=default, dropna=dropna, strict=strict, unique=unique),)
        elif len(args) > 1: return align_array(*args, how=how, default=default, dropna=dropna, strict=strict, unique=unique)
        else: return args

    def map_context(self, sequence: List[_KT]=list(), rename: List[_KT]=list(),
                    default=None, dropna=True, strict=False, unique=True, to=None, **context) -> Context:
        __m, sequence = dict(), self.iterateQuery+sequence
        for __key, __value in context.items():
            if __key in sequence:
                __value = to_array(__value, default=default, dropna=dropna, strict=strict, unique=unique)
            if __key in rename:
                renameMap = self.get_rename_map(to=to, **context)
                if isinstance(__value, str): renameMap.get(__value,__value)
                elif isinstance(__value, List): __value = [renameMap.get(__v,__v) for __v in __value]
                elif isinstance(__value, Tuple): __value = tuple([renameMap.get(__v,__v) for __v in __value])
            __m[__key] = __value
        return __m

    def gather(self, *args, message=str(), progress=True, iterateArgs: _PASS=None, iterateQuery: _PASS=None,
                fields: IndexLabel=list(), returnType: Optional[TypeHint]=None, **context) -> Data:
        message = self.get_message(*args, message=message, **context)
        iterator, context = self.set_iterator(*args, iterateArgs=self.iterateArgs, iterateQuery=self.iterateQuery, **context)
        data = [self.fetch(**__i, fields=fields, **context) for __i in tqdm(iterator, desc=message, disable=(not progress))]
        return self.map_reduce(data, fields=fields, returnType=returnType, **context)

    def get_message(self, *args, message=str(), **context) -> str:
        return exists_one(message, self.message, GATHER_MSG(self.which, self.where))

    def map_reduce(self, data: List[Data], fields: IndexLabel=list(),
                    returnType: Optional[TypeHint]=None, **context) -> Data:
        return filter_data(chain_exists(data), fields=fields, if_null="pass", return_type=returnType)

    ###################################################################
    ########################## Fetch Request ##########################
    ###################################################################

    @abstractmethod
    @BaseSession.log_errors
    @requests_limit
    def fetch(self, *args, **context) -> Data:
        ...

    def request(self, method: str, url: str, session: Optional[requests.Session]=None,
                params=None, data=None, json=None, headers=None, allow_redirects=True,
                close=False, **context) -> requests.Response:
        session = session if session else requests
        messages = dict(params=params, data=data, json=json, headers=headers)
        self.logger.debug(log_messages(**messages, logJson=self.logJson))
        response = session.request(method, url, **messages, allow_redirects=allow_redirects)
        return response.close() if close else response

    def request_content(self, method: str, url: str, session: Optional[requests.Session]=None,
                        params=None, data=None, json=None, headers=None, allow_redirects=True,
                        validate=False, exception: Literal["error","interupt"]="interupt",
                        valid: Optional[Status]=200, invalid: Optional[Status]=None, **context) -> bytes:
        session = session if session else requests
        messages = dict(params=params, data=data, json=json, headers=headers)
        self.logger.debug(log_messages(**messages, logJson=self.logJson))
        with session.request(method, url, **messages, allow_redirects=allow_redirects) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.content

    def request_text(self, method: str, url: str, session: Optional[requests.Session]=None,
                    params=None, data=None, json=None, headers=None, allow_redirects=True,
                    validate=False, exception: Literal["error","interupt"]="interupt",
                    valid: Optional[Status]=200, invalid: Optional[Status]=None, **context) -> str:
        session = session if session else requests
        messages = dict(params=params, data=data, json=json, headers=headers)
        self.logger.debug(log_messages(**messages, logJson=self.logJson))
        with session.request(method, url, **messages, allow_redirects=allow_redirects) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.text

    def request_json(self, method: str, url: str, session: Optional[requests.Session]=None,
                    params=None, data=None, json=None, headers=None, allow_redirects=True,
                    validate=False, exception: Literal["error","interupt"]="interupt",
                    valid: Optional[Status]=200, invalid: Optional[Status]=None, **context) -> JsonData:
        session = session if session else requests
        messages = dict(params=params, data=data, json=json, headers=headers)
        self.logger.debug(log_messages(**messages, logJson=self.logJson))
        with session.request(method, url, **messages, allow_redirects=allow_redirects) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.json()

    def request_headers(self, method: str, url: str, session: Optional[requests.Session]=None,
                        params=None, data=None, json=None, headers=None, allow_redirects=True,
                        validate=False, exception: Literal["error","interupt"]="interupt",
                        valid: Optional[Status]=200, invalid: Optional[Status]=None, **context) -> Dict:
        session = session if session else requests
        messages = dict(params=params, data=data, json=json, headers=headers)
        self.logger.debug(log_messages(**messages, logJson=self.logJson))
        with session.request(method, url, **messages, allow_redirects=allow_redirects) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.headers

    def request_table(self, method: str, url: str, session: Optional[requests.Session]=None,
                    params=None, data=None, json=None, headers=None, allow_redirects=True,
                    validate=False, exception: Literal["error","interupt"]="interupt",
                    valid: Optional[Status]=200, invalid: Optional[Status]=None,
                    bytes=True, engine: Optional[Literal["xlrd","openpyxl","odf","pyxlsb"]]=None, **context) -> pd.DataFrame:
        session = session if session else requests
        messages = dict(params=params, data=data, json=json, headers=headers)
        self.logger.debug(log_messages(**messages, logJson=self.logJson))
        with session.request(method, url, **messages, allow_redirects=allow_redirects) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            try: return pd.read_excel(response.content, engine=engine) if bytes else pd.read_html(response.text, header=0)[0]
            except: return pd.read_html(response.text, header=0)[0] if bytes else pd.read_excel(response.content, engine=engine)

    def validate_status(self, response: requests.Response, how: Literal["error","interupt"]="interupt",
                        valid: Optional[Status]=200, invalid: Optional[Status]=None):
        status = response.status_code
        if (valid and (status not in cast_tuple(valid))) or (invalid and (status in cast_tuple(invalid))):
            if how == "interupt": raise KeyboardInterrupt(INVALID_STATUS_MSG(self.where))
            else: raise requests.ConnectionError(INVALID_STATUS_MSG(self.where))

    ###################################################################
    ############################ Google API ###########################
    ###################################################################

    def gcloud_authorized(func):
        @functools.wraps(func)
        def wrapper(self: Spider, *args, redirectUrl=str(), authorization=str(), account: Account=dict(), **kwargs):
            if not authorization:
                authorization = fetch_gcloud_authorization(redirectUrl, account)
            return func(
                self, *args, redirectUrl=redirectUrl, authorization=authorization, account=account, **kwargs)
        return wrapper

    def set_query(self, queryInfo: GspreadReadInfo=dict(), account: Account=dict(), **context):
        if not queryInfo: return
        for queryContext in queryInfo.values():
            if len(kloc(queryContext, [KEY, SHEET, FIELDS], if_null="drop")) == 3:
                self.set_gs_query(**queryContext, account=account)

    def upload_data(self, data: TabularData, uploadInfo: UploadInfo=dict(), reauth=False,
                    account: Account=dict(), credentials: IDTokenCredentials=None,
                    key: _PASS=None, sheet: _PASS=None, mode: _PASS=None, base_sheet: _PASS=None, cell: _PASS=None,
                    table: _PASS=None, project_id: _PASS=None, schema: _PASS=None, progress: _PASS=None,
                    partition: _PASS=None, prtition_by: _PASS=None, base: _PASS=None, **context):
        if data_empty(data) or not uploadInfo: return
        for uploadContext in uploadInfo.values():
            if len(kloc(uploadContext, [KEY, SHEET], if_null="drop")) == 2:
                self.upload_gspread(data=data, **uploadContext, account=account, **context)
            elif len(kloc(uploadContext, [TABLE, PID], if_null="drop")) == 2:
                self.upload_gbq(data=data, **uploadContext, reauth=reauth, credentials=credentials, **context)

    ###################################################################
    ########################## Google Spread ##########################
    ###################################################################

    def set_gs_query(self, key: str, sheet: str, fields: IndexLabel, str_cols: NumericiseIgnore=list(),
                    arr_cols: Sequence[IndexLabel]=list(), to=None, account: Account=dict(), **context):
        renameMap = self.get_rename_map(to=to, **context)
        data = read_gspread(key, sheet, account, fields=cast_tuple(fields), if_null="drop",
                            numericise_ignore=str_cols, return_type="dataframe", rename=renameMap)
        self.logger.info(log_table(data, logJson=self.logJson))
        for field in cast_tuple(fields):
            values = list(data[field]) if field in data else None
            if (len(values) == 1) and (field not in arr_cols): values = values[0]
            self.update(field=values)

    @BaseSession.log_errors
    def upload_gspread(self, key: str, sheet: str, data: pd.DataFrame, mode: Literal["fail","replace","append"]="append",
                        base_sheet=str(), cell=str(), account: Account=dict(), clear: _PASS=None, **context):
        data = self.map_gs_data(data, **context)
        if base_sheet:
            data = self.map_gs_base(data, self.read_gs_base(key, base_sheet, account=account), **context)
        self.logger.info(log_table(data, key=key, sheet=sheet, logJson=self.logJson))
        cell, clear = ("A2" if mode == "replace" else (cell if cell else str())), (True if mode == "replace" else clear)
        update_gspread(key, sheet, data, account=account, cell=cell, clear=clear)

    def map_gs_data(self, data: pd.DataFrame, **context) -> pd.DataFrame:
        return data

    def read_gs_base(self, key: str, sheet: str, str_cols: NumericiseIgnore=list(),
                    to=None, account: Account=dict(), **context) -> pd.DataFrame:
        renameMap = self.get_rename_map(to=to, **context)
        data = read_gspread(key, sheet, account=account, numericise_ignore=str_cols, rename=renameMap)
        self.logger.info(log_table(data, key=key, sheet=sheet, logJson=self.logJson))
        return data

    def map_gs_base(self, data: pd.DataFrame, base: pd.DataFrame, **context) -> pd.DataFrame:
        return cloc(data, base.columns, if_null="drop")

    ###################################################################
    ######################### Google Bigquery #########################
    ###################################################################

    @BaseSession.log_errors
    def upload_gbq(self, table: str, project_id: str, data: pd.DataFrame,
                    mode: Literal["fail","replace","append","upsert"]="append",
                    schema: Optional[BigQuerySchema]=None, progress=True, partition=str(),
                    partition_by: Literal["auto","second","minute","hour","day","month","year","date"]="auto",
                    reauth=False, account: Account=dict(), credentials: Optional[IDTokenCredentials]=None, **context):
        schema = schema if schema and is_records(schema) else self.get_gbq_schema(mode=mode, **context)
        data = self.map_gbq_data(data, schema=schema, **context)
        context = dict(project_id=project_id, reauth=reauth, account=account, credentials=credentials)
        if mode == "upsert":
            data = self.map_gbq_base(data, self.read_gbq_base(query=table, **context), schema=schema, **context)
        self.logger.info(log_table(data, table=table, pid=project_id, mode=mode, schema=schema, logJson=self.logJson))
        to_gbq(table, project_id, data, if_exists=("replace" if mode == "upsert" else mode), schema=schema, progress=progress,
                partition=partition, partition_by=partition_by, **context)

    def get_gbq_schema(self, mode: Literal["fail","replace","append","upsert"]="append", **context) -> BigQuerySchema:
        ...

    def map_gbq_data(self, data: pd.DataFrame, schema: Optional[BigQuerySchema]=None, **context) -> pd.DataFrame:
        columns = (field["name"] for field in schema) if schema else tuple()
        return cloc(data, columns=columns, if_null="drop")

    def read_gbq_base(self, query: str, project_id: str, **context) -> pd.DataFrame:
        return read_gbq(query, project_id, **context)

    def map_gbq_base(self, data: pd.DataFrame, base: pd.DataFrame, key=str(),
                    schema: Optional[BigQuerySchema]=None, **context) -> pd.DataFrame:
        key = validate_upsert_key(data, base, key, schema)
        data = apply_df(data, apply=(lambda x: None if x == str() else x), all_cols=True)
        data = data.set_index(key).combine_first(base.set_index(key)).reset_index()
        return data[data[key].notna()].drop_duplicates(key)


###################################################################
########################## Async Spiders ##########################
###################################################################

class AsyncSpider(Spider):
    __metaclass__ = ABCMeta
    asyncio = True
    operation = "spider"
    where = WHERE
    which = WHICH
    fields = list()
    iterateArgs = list()
    redirectArgs = None
    iterateQuery = list()
    redirectQuery = None
    iterateUnit = 1
    redirectUnit = None
    interval = str()
    datetimeUnit = "second"
    tzinfo = None
    returnType = "records"
    renameMap = dict()
    maxLimit = MAX_ASYNC_TASK_LIMIT
    redirectLimit = MAX_REDIRECT_LIMIT
    message = str()

    def __init__(self, fields: IndexLabel=list(), contextFields: Optional[ContextMapper]=None,
                iterateUnit: Optional[Unit]=None, interval: Optional[Timedelta]=None,
                startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                datetimeUnit: Literal["second","minute","hour","day","month","year"]="second",
                tzinfo: Optional[Timezone]=None, returnType: Optional[TypeHint]=None,
                logName=str(), logLevel: LogLevel="WARN", logFile: Optional[str]=str(), debug=False,
                renameMap: RenameMap=dict(), schemaInfo: SchemaInfo=dict(), delay: Union[float,int,Tuple[int]]=1.,
                numTasks=100, progress=True, message=str(), queryInfo: Optional[GspreadReadInfo]=dict(),
                apiRedirect=False, redirectUnit: Optional[Unit]=None, **context):
        super().__init__(fields=fields, iterateUnit=iterateUnit, interval=interval, startDate=startDate, endDate=endDate,
                        datetimeUnit=datetimeUnit, tzinfo=tzinfo, returnType=returnType,
                        logName=logName, logLevel=logLevel, logFile=logFile, debug=debug, renameMap=renameMap,
                        schemaInfo=schemaInfo, delay=delay, progress=progress, message=message)
        self.numTasks = cast_int(numTasks, default=MIN_ASYNC_TASK_LIMIT)
        self.apiRedirect = apiRedirect
        if is_empty(self.redirectArgs, strict=True): self.redirectArgs = self.iterateArgs
        self.redirectUnit = exists_one(redirectUnit, self.redirectUnit, self.iterateUnit, strict=False)
        self.set_context(contextFields=contextFields, **UNIQUE_CONTEXT(**context))
        self.set_query(queryInfo, **context)

    ###################################################################
    ########################## Async Managers #########################
    ###################################################################

    def asyncio_task(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSpider, *args, self_var=True, uploadInfo: Optional[UploadInfo]=dict(), **kwargs):
            if self_var: kwargs = UNIQUE_CONTEXT(**dict(self.__dict__, **kwargs))
            semaphore = self.asyncio_semaphore(**kwargs)
            data = await func(self, *args, semaphore=semaphore, **kwargs)
            self.upload_data(data, uploadInfo, **kwargs)
            return data
        return wrapper

    def asyncio_session(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSpider, *args, self_var=True, uploadInfo: Optional[UploadInfo]=dict(), **kwargs):
            if self_var: kwargs = UNIQUE_CONTEXT(**REQUEST_CONTEXT(**dict(self.__dict__, **kwargs)))
            semaphore = self.asyncio_semaphore(**kwargs)
            async with aiohttp.ClientSession() as session:
                data = await func(self, *args, session=session, semaphore=semaphore, **kwargs)
            await asyncio.sleep(.25)
            self.upload_data(data, uploadInfo, **kwargs)
            return data
        return wrapper

    def asyncio_semaphore(self, numTasks: Optional[int]=None, apiRedirect=False, **kwargs) -> asyncio.Semaphore:
        numTasks = min(self.numTasks, (self.redirectLimit if apiRedirect else self.maxLimit), self.maxLimit)
        return asyncio.Semaphore(numTasks)

    def asyncio_errors(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSpider, *args, **kwargs):
            try: return await func(self, *args, **kwargs)
            except KeyboardInterrupt as interrupt:
                raise interrupt
            except Exception as exception:
                self.log_context(**kwargs)
                if self.debug: raise exception
                name = f"{func.__name__}({self.__class__.__name__})"
                self.logger.error(log_exception(name, *args, json=self.logJson, **kwargs))
                return init_origin(func)
        return wrapper

    def asyncio_redirect(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSpider, *args, apiRedirect=False, **kwargs):
            if apiRedirect: return await self.redirect(self, *args, **kwargs)
            else: return await func(self, *args, **kwargs)
        return wrapper

    def asyncio_limit(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSpider, *args, semaphore: Optional[asyncio.Semaphore]=None, **kwargs):
            if self.delay:
                await asyncio.sleep(self.delay)
            if semaphore:
                async with semaphore:
                    return await func(self, *args, **kwargs)
            return await func(self, *args, **kwargs)
        return wrapper

    ###################################################################
    ########################## Async Requests #########################
    ###################################################################

    @abstractmethod
    @asyncio_session
    async def crawl(self, **context) -> Data:
        args, context = self.map_context(*args, **context)
        return await self.gather(*args, **context)

    @asyncio_redirect
    async def gather(self, *args, message=str(), progress=True, iterateArgs: _PASS=None, iterateQuery: _PASS=None,
                    fields: IndexLabel=list(), returnType: Optional[TypeHint]=None, **context) -> Data:
        message = self.get_message(*args, message=message, **context)
        iterator, context = self.set_iterator(*args, iterateArgs=self.iterateArgs, iterateQuery=self.iterateQuery, **context)
        data = await tqdm.gather(*[
                self.fetch(**__i, fields=fields, **context) for __i in iterator], desc=message, disable=(not progress))
        return self.map_reduce(data, fields=fields, returnType=returnType, **context)

    @abstractmethod
    @asyncio_errors
    @asyncio_limit
    async def fetch(self, *args, **context) -> Data:
        ...

    async def request_content(self, method: str, url: str, session: Optional[aiohttp.ClientSession]=None,
                            params=None, data=None, json=None, headers=None, allow_redirects=True,
                            validate=False, exception: Literal["error","interupt"]="interupt",
                            valid: Optional[Status]=200, invalid: Optional[Status]=None, **context) -> bytes:
        session = session if session else aiohttp
        messages = dict(params=params, data=data, json=json, headers=headers)
        self.logger.debug(log_messages(**messages, logJson=self.logJson))
        async with session.request(method, url, **messages, allow_redirects=allow_redirects) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return await response.read()

    async def request_text(self, method: str, url: str, session: Optional[aiohttp.ClientSession]=None,
                            params=None, data=None, json=None, headers=None, allow_redirects=True,
                            validate=False, exception: Literal["error","interupt"]="interupt",
                            valid: Optional[Status]=200, invalid: Optional[Status]=None, **context) -> str:
        session = session if session else aiohttp
        messages = dict(params=params, data=data, json=json, headers=headers)
        self.logger.debug(log_messages(**messages, logJson=self.logJson))
        async with session.request(method, url, **messages, allow_redirects=allow_redirects) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return await response.text()

    async def request_json(self, method: str, url: str, session: Optional[aiohttp.ClientSession]=None,
                            params=None, data=None, json=None, headers=None, allow_redirects=True,
                            validate=False, exception: Literal["error","interupt"]="interupt",
                            valid: Optional[Status]=200, invalid: Optional[Status]=None, **context) -> JsonData:
        session = session if session else aiohttp
        messages = dict(params=params, data=data, json=json, headers=headers)
        self.logger.debug(log_messages(**messages, logJson=self.logJson))
        async with session.request(method, url, **messages, allow_redirects=allow_redirects) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return await response.json()

    async def request_headers(self, method: str, url: str, session: Optional[aiohttp.ClientSession]=None,
                            params=None, data=None, json=None, headers=None, allow_redirects=True,
                            validate=False, exception: Literal["error","interupt"]="interupt",
                            valid: Optional[Status]=200, invalid: Optional[Status]=None, **context) -> Dict:
        session = session if session else aiohttp
        messages = dict(params=params, data=data, json=json, headers=headers)
        self.logger.debug(log_messages(**messages, logJson=self.logJson))
        async with session.request(method, url, **messages, allow_redirects=allow_redirects) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.headers

    async def request_table(self, method: str, url: str, session: Optional[aiohttp.ClientSession]=None,
                            params=None, data=None, json=None, headers=None, allow_redirects=True,
                            validate=False, exception: Literal["error","interupt"]="interupt",
                            valid: Optional[Status]=200, invalid: Optional[Status]=None,
                            bytes=True, engine: Optional[Literal["xlrd","openpyxl","odf","pyxlsb"]]=None, **context) -> pd.DataFrame:
        session = session if session else requests
        messages = dict(params=params, data=data, json=json, headers=headers)
        self.logger.debug(log_messages(**messages, logJson=self.logJson))
        async with session.request(method, url, **messages, allow_redirects=allow_redirects) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            try: return pd.read_excel(await response.read(), engine=engine) if bytes else pd.read_html(await response.text(), header=0)[0]
            except: return pd.read_html(await response.text(), header=0)[0] if bytes else pd.read_excel(await response.read(), engine=engine)

    def validate_status(self, response: aiohttp.ClientResponse, how: Literal["error","interupt"]="interupt",
                        valid: Optional[Status]=200, invalid: Optional[Status]=None):
        status = response.status
        if (valid and (status not in cast_tuple(valid))) or (invalid and (status in cast_tuple(invalid))):
            if how == "interupt": raise KeyboardInterrupt(INVALID_STATUS_MSG(self.where))
            else: raise aiohttp.ServerConnectionError(INVALID_STATUS_MSG(self.where))

    ###################################################################
    ########################## Async Redirect #########################
    ###################################################################

    def gcloud_authorized(func):
        @functools.wraps(func)
        async def wrapper(self: Spider, *args, redirectUrl=str(), authorization=str(), account: Account=dict(), **kwargs):
            if not authorization:
                authorization = fetch_gcloud_authorization(redirectUrl, account)
            return await func(
                self, *args, redirectUrl=redirectUrl, authorization=authorization, account=account, **kwargs)
        return wrapper

    @gcloud_authorized
    async def redirect(self, *args, message=str(), progress=True, iterateArgs: _PASS=None, redirectArgs: _PASS=None,
                        iterateQuery: _PASS=None, redirectQuery: _PASS=None, iterateUnit: _PASS=None, redirectUnit: Unit=1,
                        fields: IndexLabel=list(), returnType: Optional[TypeHint]=None, **context) -> Data:
        message = self.get_redirect_message(*args, message=message, **context)
        redirect_context = dict(iterateArgs=self.redirectArgs, iterateQuery=self.redirectQuery, iterateUnit=redirectUnit)
        iterator, context = self.set_iterator(*args, **redirect_context, **context)
        data = await tqdm.gather(*[
                self.fetch_redirect(**__i, fields=fields, **context) for __i in iterator], desc=message, disable=(not progress))
        return self.map_reduce(data, fields=fields, returnType=returnType, **context)

    def get_redirect_message(self, *args, message=str(), **context) -> str:
        return message if message else REDIRECT_MSG(self.operation)

    @asyncio_errors
    @asyncio_limit
    @gcloud_authorized
    async def fetch_redirect(self, redirectUrl: str, authorization: str, session: Optional[aiohttp.ClientSession]=None,
                            account: Account=dict(), **context) -> Records:
        data = self.filter_redirect_data(redirectUrl, authorization, account, **context)
        response = self.request_text("POST", redirectUrl, session, json=data, headers=dict(Authorization=authorization))
        return self.parse_redirect(json.loads(response), **context)

    def filter_redirect_data(self, redirectUrl: str, authorization: str, account: Account=dict(), **context) -> Context:
        return dict(
            redirectUrl=redirectUrl,
            authorization=authorization,
            account=account,
            **REDIRECT_CONTEXT(**context))

    def parse_redirect(self, data: RedirectData, **context) -> Records:
        data = self.log_redirect_errors(data)
        results = self.map_redirect(data, **context)
        self.logger.debug(log_data(results))
        return results

    def log_redirect_errors(self, data: RedirectData, **context) -> Records:
        if not isinstance(data, Dict): raise ValueError(INVALID_REDIRECT_LOG_MSG)
        errors = data.get("errors", default=dict())
        for key, values in (errors if isinstance(errors, Dict) else dict()).items():
            self.errors[key] += values
        return data.get("data", default=list())

    def map_redirect(self, data: Records, **context) -> Records:
        if not isinstance(data, List): return list()
        cast_datetime_or_keep = lambda x: cast_datetime_format(x, default=x)
        return [apply_df(__m, apply=cast_datetime_or_keep, all_cols=True) for __m in data]

###################################################################
######################## Encrypted Spiders ########################
###################################################################

class LoginSpider(requests.Session, Spider):
    __metaclass__ = ABCMeta
    asyncio = False
    operation = "login"
    where = str()

    @abstractmethod
    def __init__(self, logName=str(), logLevel: LogLevel="WARN", logFile: Optional[str]=str(), **context):
        super().__init__()
        self.operation = self.operation
        self.logName = logName if logName else self.operation
        self.logLevel = int(logLevel) if str(logLevel).isdigit() else logging.getLevelName(str(logLevel).upper())
        self.logFile = logFile
        self.logJson = bool(logFile)
        self.logger = CustomLogger(name=self.logName, level=self.logLevel, file=self.logFile)

    @abstractmethod
    def login(self):
        ...

    def login_required(func):
        @functools.wraps(func)
        def wrapper(self: LoginSpider, *args, **kwargs):
            context = self.get_user_info(**kwargs)
            return func(self, *args, **kwargs, **context)
        return wrapper

    def get_user_info(self, cookies=str(), **kwargs) -> Context:
        cookies = cookies if cookies else self.get_cookies()
        if not cookies:
            self.login()
            cookies = self.get_cookies()
        if not cookies: raise KeyboardInterrupt(INVALID_USER_INFO_MSG(self.where))
        else: return dict(cookies=cookies)

    def get_cookies(self) -> str:
        return parse_cookies(self.cookies)


class EncryptedSpider(Spider):
    __metaclass__ = ABCMeta
    asyncio = False
    operation = "spider"
    where = WHERE
    which = WHICH
    fields = list()
    iterateArgs = list()
    iterateQuery = list()
    iterateUnit = 1
    interval = str()
    datetimeUnit = "second"
    tzinfo = None
    returnType = "records"
    renameMap = dict()
    message = str()
    auth = LoginSpider

    def __init__(self, fields: IndexLabel=list(), contextFields: Optional[ContextMapper]=None,
                iterateUnit: Optional[Unit]=None, interval: Optional[Timedelta]=None,
                startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                datetimeUnit: Literal["second","minute","hour","day","month","year"]="second",
                tzinfo: Optional[Timezone]=None, returnType: Optional[TypeHint]=None,
                logName=str(), logLevel: LogLevel="WARN", logFile: Optional[str]=str(), debug=False,
                renameMap: RenameMap=dict(), schemaInfo: SchemaInfo=dict(), delay: Union[float,int,Tuple[int]]=1.,
                progress=True, message=str(), queryInfo: Optional[GspreadReadInfo]=dict(),
                encryptedKey: EncryptedKey=str(), decryptedKey=str(), cookies=str(), **context):
        super().__init__(fields=fields, iterateUnit=iterateUnit, interval=interval, startDate=startDate, endDate=endDate,
                        datetimeUnit=datetimeUnit, tzinfo=tzinfo, returnType=returnType,
                        logName=logName, logLevel=logLevel, logFile=logFile, debug=debug, renameMap=renameMap,
                        schemaInfo=schemaInfo, delay=delay, progress=progress, message=message)
        self.cookies = cookies
        self.set_context(contextFields=contextFields, **UNIQUE_CONTEXT(**context))
        self.set_query(queryInfo, **context)
        if not self.cookies:
            self.set_secrets(encryptedKey, decryptedKey)

    def set_secrets(self, encryptedKey=str(), decryptedKey=str()):
        self.encryptedKey = encryptedKey
        try: self.decryptedKey = json.loads(exists_one(decryptedKey, self.get("decryptedKey"), decrypt(encryptedKey,1)))
        except JSONDecodeError: raise ValueError(INVALID_USER_INFO_MSG(self.where))
        if not self.encryptedKey:
            self.encryptedKey = encrypt(json.dumps(self.decryptedKey, ensure_ascii=False, default=1),1)
        self.logger.info(log_encrypt(**self.decryptedKey, show=3))

    def login(self, update=True) -> str:
        auth = self.auth(logFile=self.logFile, logLevel=self.logLevel, **self.decryptedKey)
        auth.login()
        cookies = auth.get_cookies()
        auth.close()
        return self.update(cookies=cookies) if update else cookies

    def login_required(func):
        @functools.wraps(func)
        def wrapper(self: EncryptedSpider, *args, **kwargs):
            context = self.get_user_info(**kwargs)
            return func(self, *args, **kwargs, **context)
        return wrapper

    def get_user_info(self, cookies=str(), **kwargs) -> Context:
        cookies = cookies if cookies else self.cookies
        if not cookies:
            self.login(update=True)
            cookies = self.cookies
        if not cookies:
            raise KeyboardInterrupt(LOGIN_REQUIRED_MSG(self.where))
        self.update(cookies=cookies)
        return dict(cookies=cookies)


class EncryptedAsyncSpider(AsyncSpider, EncryptedSpider):
    __metaclass__ = ABCMeta
    asyncio = True
    operation = "spider"
    where = WHERE
    which = WHICH
    fields = list()
    iterateArgs = list()
    redirectArgs = None
    iterateQuery = list()
    redirectQuery = None
    iterateUnit = 1
    redirectUnit = None
    interval = str()
    datetimeUnit = "second"
    tzinfo = None
    returnType = "records"
    renameMap = dict()
    maxLimit = MAX_ASYNC_TASK_LIMIT
    redirectLimit = MAX_REDIRECT_LIMIT
    message = str()
    auth = LoginSpider

    def __init__(self, fields: IndexLabel=list(), contextFields: Optional[ContextMapper]=None,
                iterateUnit: Optional[Unit]=None, interval: Optional[Timedelta]=None,
                startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                datetimeUnit: Literal["second","minute","hour","day","month","year"]="second",
                tzinfo: Optional[Timezone]=None, returnType: Optional[TypeHint]=None,
                logName=str(), logLevel: LogLevel="WARN", logFile: Optional[str]=str(), debug=False,
                renameMap: RenameMap=dict(), schemaInfo: SchemaInfo=dict(), delay: Union[float,int,Tuple[int]]=1.,
                numTasks=100, progress=True, message=str(), queryInfo: Optional[GspreadReadInfo]=dict(),
                apiRedirect=False, redirectUnit: Optional[Unit]=None,
                encryptedKey: EncryptedKey=str(), decryptedKey=str(), cookies=str(), **context):
        super().__init__(fields=fields, iterateUnit=iterateUnit, interval=interval, startDate=startDate, endDate=endDate,
                        datetimeUnit=datetimeUnit, tzinfo=tzinfo, returnType=returnType,
                        logName=logName, logLevel=logLevel, logFile=logFile, debug=debug, renameMap=renameMap,
                        schemaInfo=schemaInfo, delay=delay, numTasks=numTasks, progress=progress, message=message,
                        apiRedirect=apiRedirect, redirectUnit=redirectUnit)
        self.cookies = cookies
        self.set_context(contextFields=contextFields, **UNIQUE_CONTEXT(**context))
        self.set_query(queryInfo, **context)
        if not self.cookies:
            self.set_secrets(encryptedKey, decryptedKey)

    def login_required(func):
        @functools.wraps(func)
        async def wrapper(self: EncryptedAsyncSpider, *args, **kwargs):
            context = self.get_user_info(**kwargs)
            return await func(self, *args, **kwargs, **context)
        return wrapper


###################################################################
############################# Parsers #############################
###################################################################

class Parser(BaseSession):
    __metaclass__ = ABCMeta
    operation = "parser"
    fields = list()
    responseType = "dict"
    root = list()
    renameMap = dict()
    schemaInfo = dict()

    def __init__(self, fields: IndexLabel=list(), logName=str(), logLevel: LogLevel="WARN", logFile=str(),
                debug=False, responseType: _PASS=None, root: _KT=list(), renameMap: RenameMap=dict(),
                schemaInfo: SchemaInfo=dict(), **context):
        super().__init__(fields=fields, logName=logName, logLevel=logLevel, logFile=logFile,
                        debug=debug, renameMap=renameMap, schemaInfo=schemaInfo, **context)
        self.root = root if root else self.root

    @BaseSession.validate_response
    def parse(self, response: Any, schemaInfo: SchemaInfo=dict(), root: _KT=list(), discard=False,
                fields: IndexLabel=list(), **context) -> Data:
        data = self.parse_response(response, **context)
        return self.parse_data(data, schemaInfo, root, discard, fields, **context)

    def parse_response(self, response: Any, **context) -> Data:
        return json.loads(response)

    def parse_data(self, data: Data, schemaInfo: SchemaInfo=dict(), root: _KT=list(), discard=False,
                    fields: IndexLabel=list(), updateTime=True, to=None, **context) -> Data:
        renameMap = self.get_rename_map(to=to, **context)
        data = parse_schema(data, schemaInfo, self.responseType, root, renameMap, discard, **context)
        if updateTime:
            data = set_data(data, updateDate=self.today(), updateTime=self.now())
        return filter_data(data, fields=fields, if_null="pass")


###################################################################
############################ Pipelines ############################
###################################################################

class Pipeline(Spider):
    __metaclass__ = ABCMeta
    asyncio = False
    operation = "pipeline"
    dependencies = (Spider, Spider)
    fields = list()
    datetimeUnit = "second"
    tzinfo = None
    returnType = "dataframe"
    renameMap = dict()

    @abstractmethod
    @Spider.requests_task
    def crawl(self, **context) -> Data:
        return self.gather(**context)

    @abstractmethod
    def gather(self, **context) -> Data:
        args = list()
        for cralwer in self.dependencies:
            operation = self.get_operation(cralwer, **context)
            data = self.crawl_proxy(cralwer, prefix=operation, **context)
            args.append(data)
        return self.map_reduce(*args, **context)

    def get_operation(self, crawler: Spider, to=None, **context) -> str:
        operation = crawler.__dict__.get("operation")
        if not operation: raise ValueError(DEPENDENCY_HAS_NO_NAME_MSG)
        renameMap = self.get_rename_map(to=to, **context)
        return renameMap.get(operation, default=operation)

    def crawl_proxy(self, crawler: Spider, prefix=str(), extraSave=False,
                    appendix: Optional[pd.DataFrame]=None, drop: Literal["left","right"]="right",
                    how: Literal["left","right","outer","inner","cross"]="left", on=str(), **context) -> Data:
        query = crawler.get_iterator(keys_only=True)
        if query and any(map(is_empty, kloc(context, query, if_null="pass", values_only=True))): return pd.DataFrame()
        crawler = crawler(**context)
        data = pd.DataFrame(crawler.crawl(**PROXY_CONTEXT(**crawler.__dict__)))
        self.extra_save(data, prefix=prefix, extraSave=extraSave, **context)
        return merge_drop(data, appendix, drop=drop, how=how, on=on)

    def map_reduce(self, *data: Data, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None, **kwargs) -> Data:
        return filter_data(chain_exists(data), fields=fields, if_null="pass", return_type=returnType)


class AsyncPipeline(AsyncSpider, Pipeline):
    __metaclass__ = ABCMeta
    operation = "pipeline"
    dependencies = (AsyncSpider, AsyncSpider)
    fields = list()
    datetimeUnit = "second"
    tzinfo = None
    returnType = "dataframe"
    renameMap = dict()

    @abstractmethod
    @AsyncSpider.asyncio_task
    async def crawl(self, **context) -> Data:
        return await self.gather(**context)

    @abstractmethod
    async def gather(self, **context) -> Data:
        args = list()
        for cralwer in self.dependencies:
            operation = self.get_operation(cralwer, **context)
            if cralwer.asyncio: data = await self.async_proxy(cralwer, prefix=operation, **context)
            else: data = self.crawl_proxy(cralwer, prefix=operation, **context)
            args.append(data)
        return self.map_reduce(*args, **context)

    async def async_proxy(self, crawler: Spider, prefix=str(), extraSave=False,
                            appendix: Optional[pd.DataFrame]=None, drop: Literal["left","right"]="right",
                            how: Literal["left","right","outer","inner","cross"]="left", on=str(), **context) -> Data:
        query = crawler.get_iterator(keys_only=True)
        if query and any(map(is_empty, kloc(context, query, if_null="pass", values_only=True))): return pd.DataFrame()
        crawler = crawler(**context)
        data = pd.DataFrame(await crawler.crawl(**PROXY_CONTEXT(**crawler.__dict__)))
        self.extra_save(data, prefix=prefix, extraSave=extraSave, **context)
        return merge_drop(data, appendix, drop=drop, how=how, on=on)
