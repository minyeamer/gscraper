from __future__ import annotations
from .context import UNIQUE_CONTEXT, TASK_CONTEXT, REQUEST_CONTEXT, UPLOAD_CONTEXT, PROXY_CONTEXT, REDIRECT_CONTEXT
from .types import _KT, _VT, _PASS, ClassInstance, Arguments, Context, TypeHint, LogLevel
from .types import RenameMap, IndexLabel, EncryptedKey, Pagination, Pages
from .types import Status, Unit, DateFormat, DateQuery, Timedelta, Timezone
from .types import JsonData, RedirectData, Records, TabularData, Data
from .types import SchemaInfo, Account, NumericiseIgnore, BigQuerySchema, GspreadReadInfo, UploadInfo
from .types import is_array, allin_instance, is_str_array, is_records, init_origin, get_annotation, is_iterable_annotation

from .cast import cast_list, cast_tuple, cast_int, cast_int1, cast_date, cast_datetime_format
from .date import now, get_date, get_date_range, is_daily_frequency
from .gcloud import fetch_gcloud_authorization, update_gspread, read_gspread
from .gcloud import IDTokenCredentials, read_gbq, to_gbq, validate_upsert_key
from .logs import CustomLogger, log_encrypt, log_messages, log_response, log_client, log_data, log_exception, log_table
from .map import exists, is_empty, unique, to_array, get_scala, diff
from .map import iloc, fill_array, is_same_length, unit_array, concat_array, align_array, transpose_array
from .map import kloc, apply_dict, chain_dict, drop_dict, cloc, apply_df, merge_drop
from .map import exists_one, convert_data, rename_data, filter_data, chain_exists, set_data, data_empty
from .parse import parse_cookies, parse_origin, decode_cookies, encode_params, validate_schema, parse_schema
from abc import ABCMeta, abstractmethod
from urllib.parse import urlparse
import asyncio
import aiohttp
import functools
import requests
import time

from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple, Union
from json import JSONDecodeError
import datetime as dt
import json
import logging
import pandas as pd

from tqdm.auto import tqdm
from inspect import Parameter
from itertools import product
import base64
import inspect
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

DEFAULT_DAYS = 1

MIN_ASYNC_TASK_LIMIT = 1
MAX_ASYNC_TASK_LIMIT = 100
MAX_REDIRECT_LIMIT = 10

PAGE_ITERATOR = ["page", "start", "dataSize"]
PAGE_PARAMS = ["size", "pageSize", "pageStart", "start"]
DATE_ITERATOR = ["startDate", "endDate", "date"]

GATHER_MSG = lambda which, where: f"Collecting {which} from {where}"
INVALID_STATUS_MSG = lambda where: f"Response from {where} is not valid."

REDIRECT_MSG = lambda operation: f"{operation} operation is redirecting"
INVALID_REDIRECT_LOG_MSG = "Please verify that the both results and errors are in redierct returns."

INVALID_USER_INFO_MSG = lambda where=str(): f"{where} user information is not valid.".strip()

DEPENDENCY_HAS_NO_NAME_MSG = "Dependency has no operation name. Please define operation name."

INVALID_VALUE_MSG = lambda name, value: f"'{value}' value is not valid {name}."

WHERE, WHICH = "urls", "data"

ARGS, PAGES = 0, 1
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
            reorder=True, values_only=False) -> Union[Any,Dict,List,str]:
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


###################################################################
############################# Session #############################
###################################################################

class BaseSession(CustomDict):
    __metaclass__ = ABCMeta
    operation = "session"
    where = WHERE
    which = WHICH
    fields = list()
    datetimeUnit = "second"
    tzinfo = None
    responseType = None
    returnType = None
    errors = list()
    renameMap = dict()
    schemaInfo = dict()

    def __init__(self, fields: IndexLabel=list(),
                datetimeUnit: Literal["second","minute","hour","day","month","year"]="second",
                tzinfo: Optional[Timezone]=None, returnType: Optional[TypeHint]=None,
                logName=str(), logLevel: LogLevel="WARN", logFile=str(), debug=False,
                renameMap: RenameMap=dict(), schemaInfo: SchemaInfo=dict(), **kwargs):
        super().__init__(kwargs)
        self.operation = self.operation
        self.fields = fields if fields else self.fields
        self.datetimeUnit = datetimeUnit if datetimeUnit else self.datetimeUnit
        self.tzinfo = tzinfo if tzinfo else self.tzinfo
        self.initTime = now(tzinfo=self.tzinfo, droptz=True, unit=self.datetimeUnit)
        self.returnType = returnType if returnType else returnType
        self.renameMap = renameMap if renameMap else self.renameMap
        self.set_logger(logName, logLevel, logFile, debug)
        self.set_schema(schemaInfo)

    def set_logger(self, logName=str(), logLevel: LogLevel="WARN", logFile=str(), debug=False):
        logName = logName if logName else self.operation
        self.logLevel = int(logLevel) if str(logLevel).isdigit() else logging.getLevelName(str(logLevel).upper())
        self.logFile = logFile
        self.logJson = bool(logFile)
        self.logger = CustomLogger(name=logName, level=self.logLevel, file=self.logFile)
        self.debug = debug

    def set_schema(self, schemaInfo: SchemaInfo=dict()):
        schemaInfo = schemaInfo if schemaInfo else self.schemaInfo
        if schemaInfo:
            for __key, schemaContext in schemaInfo.copy().items():
                schemaInfo[__key]["schema"] = validate_schema(schemaContext["schema"])
            self.schemaInfo = schemaInfo

    def now(self, __format=str(), days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0,
            hours=0, weeks=0, droptz=True) -> dt.datetime:
        return now(__format, days, seconds, microseconds, milliseconds, minutes, hours, weeks,
                    tzinfo=self.tzinfo, droptz=droptz, unit=self.datetimeUnit)

    def today(self, __format=str(), days=0, weeks=0, droptz=True) -> dt.date:
        unit = self.datetimeUnit if self.datetimeUnit in ["month","year"] else None
        return now(__format, days, weeks, tzinfo=self.tzinfo, droptz=droptz, droptime=True, unit=unit)

    def get_rename_map(self, **kwargs) -> RenameMap:
        return self.renameMap

    def extra_save(self, data: Data, prefix=str(), extraSave=False, **kwargs):
        if not extraSave: return
        file = prefix+'_' if prefix else str()+self.now("%Y%m%d%H%M%S")+".xlsx"
        renameMap = self.get_rename_map(**kwargs)
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
                self.logger.error(log_exception(func_name, json=self.logJson, **REQUEST_CONTEXT(**kwargs)))
                return init_origin(func)
        return wrapper

    def log_context(self, **context):
        self.errors.append(context)

    def validate_response(func):
        @functools.wraps(func)
        def wrapper(self: BaseSession, response: Any, *args, **kwargs):
            is_valid = self.is_valid_response(response)
            data = func(self, response, *args, **kwargs) if is_valid else init_origin(func)
            self.log_response(data, **kwargs)
            return data
        return wrapper

    def log_response(self, data: Data, **context):
        self.logger.info(log_data(data, **context))

    def is_valid_response(self, response: Any) -> bool:
        return exists(response, strict=False)


###################################################################
############################# Iterator ############################
###################################################################

class Iterator(CustomDict):
    iterateArgs = list()
    iterateQuery = list()
    iterateUnit = 0
    pagination = False
    pageUnit = 0
    pageLimit = 0
    interval = str()
    fromNow = None

    def __init__(self, iterateUnit: Optional[Unit]=0, interval: Optional[Timedelta]=str(), fromNow: Optional[int]=None,
                startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None, **context):
        super().__init__()
        self.iterateUnit = iterateUnit if iterateUnit else self.iterateUnit
        self.interval = interval if interval else self.interval
        self.fromNow = fromNow if isinstance(fromNow, int) else self.fromNow
        self.set_date(startDate=startDate, endDate=endDate, **context)

    def set_date(self, startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None, **context):
        if_null = "today" if isinstance(self.fromNow, int) else None
        startDate = get_date(startDate, if_null=if_null, days=self.fromNow)
        endDate = get_date(endDate, if_null=if_null, days=self.fromNow)
        if startDate: self.startDate = min(startDate, endDate) if endDate else startDate
        if endDate: self.endDate = max(startDate, endDate) if endDate else endDate

    ###################################################################
    ########################### Set Iterator ##########################
    ###################################################################

    def get_iterator(self, _args=True, _page=True, _date=True, _query=True,
                    keys_only=False, values_only=False, **context) -> Union[Context,_KT,_VT]:
        iterateArgs = self.iterateArgs * int(_args)
        iteratePage = PAGE_ITERATOR * int(bool(self.pagination)) * int(_page)
        iterateDate = DATE_ITERATOR * int(bool(self.interval)) * int(_date)
        iterateQuery = self.iterateQuery * int(_query)
        query = unique(*iterateArgs, *iteratePage, *iterateDate, *iterateQuery)
        if keys_only: return query
        else: return kloc(context, query, if_null="drop", values_only=values_only)

    def set_iterator(self, *args: Sequence, iterateArgs: List[_KT]=list(), iterateQuery: List[_KT]=list(),
                    iterateUnit: Unit=1, pagination: Pagination=False, interval: Timedelta=str(),
                    **context) -> Tuple[List[Context],Context]:
        arguments, periods, iterator, iterateUnit = list(), list(), list(), cast_list(iterateUnit)
        args_context = self._validate_args(*args, iterateArgs=iterateArgs, pagination=pagination)
        if args_context:
            arguments, context = self._from_args(*args, **args_context, iterateUnit=iterateUnit, **context)
            iterateQuery = diff(iterateQuery, iterateArgs, PAGE_ITERATOR)
            if len(iterateQuery) > 1: iterateUnit = iterateUnit[1:]
        if interval:
            periods, context = self._from_date(interval=interval, **context)
            iterateQuery = diff(iterateQuery, DATE_ITERATOR)
        iterator, context = self._from_context(iterateQuery=iterateQuery, iterateUnit=iterateUnit, **context)
        iterator = self._product_iterator(arguments, periods, iterator)
        return iterator, context

    ###################################################################
    ########################## From Arguments #########################
    ###################################################################

    def _validate_args(self, *args, iterateArgs: List[_KT], pagination: Pagination=False) -> Context:
        match_query = is_same_length(args, iterateArgs)
        match_args = is_same_length(*args)
        valid_args = (match_query and match_args) or ((not iterateArgs) and pagination)
        valid_pages = isinstance(pagination, bool) or (isinstance(pagination, str) and (pagination in iterateArgs))
        return dict(iterateArgs=iterateArgs, pagination=pagination) if valid_args and valid_pages else dict()

    def _from_args(self, *args: Sequence, iterateArgs: List[_KT], iterateUnit: Unit=1,
                pagination: Pagination=False, **context) -> Tuple[List[Context],Context]:
        if not is_same_length(*args): return list(), context
        argnames, pagenames = self._split_argnames(*args, iterateArgs=iterateArgs, pagination=pagination)
        if pagination:
            how = "numeric" if pagenames == PAGE_ITERATOR else "categorical"
            args, context = self._from_pages(*args, how=how, iterateArgs=iterateArgs, pagination=pagination, **context)
        args = self._product_args(*args)
        if get_scala(iterateUnit) > 1:
            args = list(map(lambda __s: unit_array(__s, unit=get_scala(iterateUnit)), args))
        args = [dict(zip(argnames, values)) for values in zip(*args)]
        return (self._map_pages(*args, keys=pagenames) if pagenames else args), context

    def _split_argnames(self, *args, iterateArgs: List[_KT], pagination: Pagination=False) -> Tuple[List[_KT],List[_KT]]:
        argnames = iterateArgs.copy()
        if isinstance(pagination, str) and is_str_array(args[iterateArgs.index(pagination)]):
            argnames.pop(iterateArgs.index(pagination))
            return argnames+["pages"], [pagination]+PAGE_ITERATOR+PAGE_PARAMS
        elif pagination:
            return argnames+["pages"], PAGE_ITERATOR
        else: return argnames, list()

    def _product_args(self, *args: Sequence) -> List[List]:
        tuple_idx = list(map(lambda x: allin_instance(x, Tuple, empty=False), args))
        if not any(tuple_idx): return args
        __args = list()
        for __arg in zip(*args):
            tuples, others = iloc(__arg, tuple_idx), iloc(__arg, list(map(lambda x: (not x), tuple_idx)))
            __product = list(product((others,), *tuples))
            __args += [concat_array(__s[1:], __s[0], tuple_idx) for __s in __product]
        return transpose_array(__args, count=len(args))

    ###################################################################
    ############################ From Pages ###########################
    ###################################################################

    def _from_pages(self, *args: Sequence, how: Literal["numeric","categorical"], iterateArgs: List[_KT],
                    pagination: Pagination=False, size: Optional[Unit]=None, **context) -> Tuple[List[List],Context]:
        if how == "numeric":
            if isinstance(pagination, str):
                size = args[iterateArgs.index(pagination)]
            return self._from_numeric_pages(*args, size=size, **context)
        base = list(args)
        labels = cast_list(base.pop(iterateArgs.index(pagination)))
        return self._from_categorical_pages(*base, labels=labels, pagination=pagination, size=size, **context)

    def _from_numeric_pages(self, *args, size: Unit, pageSize=0, pageStart=1, start=1, **context) -> Tuple[List[List],Context]:
        pageSize = self.validate_page_size(pageSize, self.pageUnit, self.pageLimit)
        pages = self.get_pages(size, pageSize, pageStart, start, how="all")
        if isinstance(size, int):
            pages = [pages] * len(get_scala(args, default=[0]))
        pages = list(map(lambda __s: tuple(map(tuple, __s)), map(transpose_array, pages)))
        return args+(pages,), dict(context, pageSize=pageSize)

    def _from_categorical_pages(self, *args, labels: List, pagination: str, size: Optional[int]=None,
                                pageSize=0, pageStart=1, start=1, **context) -> Tuple[List[List],Context]:
        pages = list()
        for label in labels:
            size = self.get_size_by_label(label, size=size, **context)
            pageSize = self.get_page_size_by_label(label, pageSize=pageSize, **context)
            pageStart = self.get_page_start_by_label(label, pageStart=pageStart, **context)
            start = self.get_start_by_label(label, start=start, **context)
            iterator = self.get_pages(size, pageSize, pageStart, start, how="all")
            num_pages = len(iterator[0])
            params = ((size,)*num_pages, (pageSize,)*num_pages, (pageStart,)*num_pages, (start,)*num_pages)
            pages.append(tuple(map(tuple, transpose_array(((label,)*num_pages,)+iterator+params))))
        return args+(pages,), context

    def validate_page_size(self, pageSize: int, pageUnit=0, pageLimit=0) -> int:
        if (pageUnit > 0) and (pageSize & pageUnit != 0):
            pageSize = round(pageSize / pageUnit) * pageUnit
        if pageLimit > 0:
            pageSize = min(pageSize, pageLimit)
        return pageSize

    def get_pages(self, size: Unit, pageSize: int, pageStart=1, start=1, pageUnit=0, pageLimit=0,
                    how: Literal["all","page","start"]="all") -> Union[Pages,List[Pages]]:
        pageSize = self.validate_page_size(pageSize, pageUnit, pageLimit)
        if isinstance(size, int):
            return self.calc_pages(cast_int1(size), pageSize, pageStart, start, how)
        elif is_array(size):
            return [self.calc_pages(cast_int1(__sz), pageSize, pageStart, start, how) for __sz in size]
        else: return tuple()

    def calc_pages(self, size: int, pageSize: int, pageStart=1, start=1,
                    how: Literal["all","page","start"]="all") -> Pages:
        pages = tuple(range(pageStart, (((size-1)//pageSize)+1)+pageStart))
        if how == "page": return pages
        starts = tuple(range(start, size+start, pageSize))
        if how == "start": return starts
        dataSize = tuple(min(size-__start+1, pageSize) for __start in starts)
        return (pages, starts, dataSize)

    def get_size_by_label(self, label: Any, size: Optional[int]=None, **context) -> int:
        return size

    def get_page_size_by_label(self, label: Any, pageSize=0, **context) -> int:
        return pageSize

    def get_page_start_by_label(self, label: Any, pageStart=1, **context) -> int:
        return pageStart

    def get_start_by_label(self, label: Any, start=1, **context) -> int:
        return start

    def _map_pages(self, *args: Context, keys: List[_KT]) -> List[Context]:
        base = list()
        for __i in range(len(args)):
            pages = args[__i].pop("pages")
            base.append(dict(args[__i], **dict(zip(keys, pages))))
        return base

    ###################################################################
    ############################ From Date ############################
    ###################################################################

    def _from_date(self, startDate: Optional[dt.date]=None, endDate: Optional[dt.date]=None,
                    interval: Timedelta="D", date: _PASS=None, **context) -> Tuple[List[DateQuery],Context]:
        startDate = startDate if isinstance(startDate, dt.date) else cast_date(startDate, default=self.get("startDate"))
        endDate = endDate if isinstance(endDate, dt.date) else cast_date(endDate, default=self.get("endDate"))
        date_range = get_date_range(startDate, endDate, interval=interval)
        if is_daily_frequency(interval):
            period = [dict(startDate=date, endDate=date, date=date) for date in date_range]
        elif len(date_range) > 1:
            period = [dict(startDate=start, endDate=(end-dt.timedelta(days=1)))
                        for start, end in zip(date_range, date_range[1:]+[endDate+dt.timedelta(days=1)])]
        else: period = [dict(startDate=startDate, endDate=endDate)]
        return period, dict(context, interval=interval)

    ###################################################################
    ########################### From Context ##########################
    ###################################################################

    def _from_context(self, iterateQuery: List[_KT], iterateUnit: Unit=1, **context) -> Tuple[List[Context],Context]:
        if not iterateQuery: return list(), context
        query, context = kloc(context, iterateQuery, if_null="drop"), drop_dict(context, iterateQuery, inplace=False)
        if any(map(lambda x: x>1, iterateUnit)): query = self._group_context(iterateQuery, iterateUnit, **query)
        else: query = [dict(zip(query.keys(), values)) for values in product(*map(cast_tuple, query.values()))]
        return query, context

    def _group_context(self, iterateQuery: List[_KT], iterateUnit: Unit=1, **context) -> List[Context]:
        query = apply_dict(context, apply=cast_list, all_keys=True)
        keys, unit = query.keys(), fill_array(iterateUnit, count=len(iterateQuery), value=1)
        combinations = product(*[range(0, len(query[key]), unit[i]) for i, key in enumerate(keys)])
        return [{key: query[key][index:index+unit[i]] for i, (key, index) in enumerate(zip(keys, indices))}
                for indices in combinations]

    def _product_iterator(self, *iterator: Sequence[Context]) -> List[Context]:
        if sum(map(len, iterator)) == 0: return list()
        iterator_array = map((lambda x: x if x else [{}]), iterator)
        return list(map(chain_dict, product(*iterator_array)))



###################################################################
############################## Spider #############################
###################################################################

class Spider(BaseSession, Iterator):
    __metaclass__ = ABCMeta
    asyncio = False
    operation = "spider"
    where = WHERE
    which = WHICH
    fields = list()
    iterateArgs = list()
    iterateQuery = list()
    iterateUnit = 0
    pagination = False
    pageUnit = 0
    pageLimit = 0
    interval = str()
    fromNow = None
    tzinfo = None
    datetimeUnit = "second"
    returnType = "records"
    renameMap = dict()
    message = str()

    def __init__(self, fields: IndexLabel=list(), contextFields: IndexLabel=list(),
                iterateUnit: Unit=0, interval: Timedelta=str(), fromNow: Optional[int]=None,
                startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                datetimeUnit: Literal["second","minute","hour","day","month","year"]="second",
                tzinfo: Optional[Timezone]=None, returnType: Optional[TypeHint]=None,
                logName=str(), logLevel: LogLevel="WARN", logFile: Optional[str]=str(), debug=False,
                renameMap: RenameMap=dict(), schemaInfo: SchemaInfo=dict(), delay: Union[float,int,Tuple[int]]=1.,
                progress=True, message=str(), cookies=str(), queryInfo: Optional[GspreadReadInfo]=dict(), **context):
        BaseSession.__init__(
            self, fields=fields, datetimeUnit=datetimeUnit, tzinfo=tzinfo, returnType=returnType,
            logName=logName, logLevel=logLevel, logFile=logFile, debug=debug,
            renameMap=renameMap, schemaInfo=schemaInfo)
        Iterator.__init__(
            self, iterateUnit=iterateUnit, interval=interval, fromNow=fromNow,
            startDate=startDate, endDate=endDate, **context)
        self.delay = delay
        self.progress = progress
        self.message = message if message else self.message
        self.cookies = cookies
        if context: self.update(kloc(UNIQUE_CONTEXT(**context), contextFields, if_null="pass"))
        self.set_query(queryInfo, **context)

    def inspect(self, __key: Optional[Literal["crawl","required","iterable","literal","return"]]=None) -> Dict[_KT,Union[Dict,str]]:
        info = dict(parameter=list())
        info["crawl"] = self.inspect_params(self.crawl)
        info["required"] = {name: param for name, param in info["crawl"].items() if "default" not in param}
        info["iterable"] = {name: param for name, param in info["crawl"].items() if "iterable" in param}
        info["literal"] = {name: param for name, param in info["crawl"].items() if param.get("annotation") == "literal"}
        info["return"] = get_annotation(inspect.signature(self.crawl).return_annotation)
        return info[__key] if __key else info

    def inspect_params(self, func: Callable) -> Dict[_KT,Dict]:
        params = dict()
        signature = inspect.signature(func)
        for name, parameter in signature.parameters.items():
            if name in ["self", "context", "kwargs"]: continue
            else: params[name] = self._inspect_annotation(parameter)
        return params

    def _inspect_annotation(self, parameter: Parameter) -> Dict:
        annotation = parameter.annotation
        info = dict(annotation=get_annotation(annotation))
        if parameter.default != inspect.Parameter.empty:
            info["default"] = parameter.default
            if parameter.annotation == inspect.Parameter.empty:
                annotation = get_annotation(type(parameter.default))
                info["annotation"] = annotation
        if is_iterable_annotation(annotation):
            info["iterable"] = True
        return info

    def from_locals(self, locals: Dict=dict(), _how: Literal["all","args","context","request","parse","query"]="all",
                    _base=True, _args=None, _page=None, _date=None, _query=None, _param=None, _request=None,
                    values_only=False, **context) -> Union[Arguments,Context,Tuple]:
        base, context = locals.pop("context", dict()), UNIQUE_CONTEXT(**dict(locals, **context))
        context = dict(base, **context) if _base else context
        if is_array(_how): return tuple(self.from_locals(context, how) for how in _how)
        conditions = self._get_conditions(_how, _args, _page, _date, _query, _param, _request)
        if all(conditions): return list(context.values()) if values_only else context
        else: return self._filter_locals(context, _how, *conditions, values_only=values_only)

    def _get_conditions(self, how: str, args=None, page=None, date=None, query=None,
                        param=None, request=None) -> Tuple[bool]:
        t = lambda condition: condition if isinstance(condition, bool) else True
        if how == "all": return (True,)*6
        elif how == "request": return (True,)*4+(False,True)
        elif how == "parse": return (True,)*4+(True,False)
        elif how == "args": return (True,)+(False,)*5
        elif how == "context": return (False,)+(True,)*5
        elif how == "query": return (True,)*4+(False,False)
        return t(args), t(page), t(date), t(query), t(param), t(request)

    def _filter_locals(self, context: Context, how: str, args=None, page=None, date=None, query=None,
                        param=None, request=None, values_only=False) -> Union[Arguments,Context]:
        iterators, params = self.get_iterator(keys_only=True), list(self.inspect("crawl").keys())
        queries = self.get_iterator(args, page, date, query, keys_only=True)
        if how in ("args","query"):
            query = kloc(context, queries, if_null="drop", values_only=(how=="args"))
            return tuple(query) if how == "args" else query
        elif param: queries = queries + diff(params, iterators)
        request = drop_dict((context if request else REQUEST_CONTEXT(**context)), iterators+params, inplace=False)
        return dict(request, **kloc(context, queries, if_null="drop", values_only=values_only))

    def log_context(self, **context):
        self.errors.append(self.get_iterator(**context))

    def log_response(self, data: Data, **context):
        self.logger.info(log_data(data, **self.get_iterator(**context)))

    ###################################################################
    ######################### Session Managers ########################
    ###################################################################

    def requests_task(func):
        @functools.wraps(func)
        def wrapper(self: Spider, *args, self_var=True, uploadInfo: Optional[UploadInfo]=dict(), **kwargs):
            if self_var: kwargs = TASK_CONTEXT(**dict(self.__dict__, **kwargs))
            data = func(self, *args, **kwargs)
            self.upload_data(data, uploadInfo, **kwargs)
            return data
        return wrapper

    def requests_session(func):
        @functools.wraps(func)
        def wrapper(self: Spider, *args, self_var=True, uploadInfo: Optional[UploadInfo]=dict(), **kwargs):
            if self_var: kwargs = REQUEST_CONTEXT(**dict(self.__dict__, **kwargs))
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
    ######################## Parameter Managers #######################
    ###################################################################

    def set_var(self, locals: Dict=dict(), how: Literal["min","max","first"]="min", default=None, dropna=True,
                strict=False, unique=True, to: Literal["en","ko"]="en", **context) -> Tuple[Arguments,Context]:
        args, context = self.from_locals(locals, ["args","context"], **context)
        args = self.set_args(*args, how=how, default=default, dropna=dropna, strict=strict, unique=unique, **context)
        context = self.set_context(default=default, dropna=dropna, strict=strict, unique=unique, to=to, **context)
        return args, context

    def set_args(self, *args, how: Literal["min","max","first"]="min", default=None,
                dropna=True, strict=False, unique=True, **context) -> Arguments:
        if not args: return args
        elif len(args) == 1: return (to_array(args[0], default=default, dropna=dropna, strict=strict, unique=unique),)
        else: return align_array(*args, how=how, default=default, dropna=dropna, strict=strict, unique=unique)

    def set_context(self, locals: Dict=dict(), default=None, dropna=True, strict=False, unique=True,
                    to: Literal["en","ko"]="en", **context) -> Context:
        if locals: context = self.from_locals(locals, "context", **context)
        signature = self.inspect()
        sequence_keys, rename_keys = signature["iterable"].keys(), signature["literal"].keys()
        renameMap = self.get_rename_map(to=to, **context)
        for __key in list(context.keys()):
            if __key in sequence_keys:
                context[__key] = to_array(context[__key], default=default, dropna=dropna, strict=strict, unique=unique)
            if __key in rename_keys:
                context[__key] = rename_data(context[__key], rename=renameMap)
        return context

    ###################################################################
    ######################### Gather Requests #########################
    ###################################################################

    @abstractmethod
    @requests_session
    def crawl(self, *args, **context) -> Data:
        args, context = self.set_var(locals())
        return self.gather(*args, **context)

    def gather(self, *args, message=str(), progress=True, iterateArgs: _PASS=None, iterateQuery: _PASS=None,
                pagination: _PASS=None, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None, **context) -> Data:
        message = self.get_message(*args, message=message, **context)
        if self.debug: self.logger.warn({"args":args, "context":context})
        iterate_params = dict(iterateArgs=self.iterateArgs, iterateQuery=self.iterateQuery, pagination=self.pagination)
        iterator, context = self.set_iterator(*args, **iterate_params, **context)
        if self.debug: self.logger.warn({"iterator":iterator})
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

    def encode_messages(func):
        @functools.wraps(func)
        def wrapper(self: Spider, method: str, url: str, session: Optional[requests.Session]=None,
                    messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                    data=None, json=None, headers=None, cookies=str(), *args, **kwargs):
            session = session if session else requests
            url, params = self.encode_params(url, params, encode=encode)
            if headers and cookies: headers["Cookie"] = str(cookies)
            messages = messages if messages else dict(params=params, data=data, json=json, headers=headers)
            self.logger.debug(log_messages(**messages, logJson=self.logJson))
            return func(self, method=method, url=url, session=session, messages=messages, **kwargs)
        return wrapper

    @encode_messages
    def request(self, method: str, url: str, session: requests.Session, messages: Dict=dict(),
                params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                allow_redirects=True, validate=False, exception: Literal["error","interupt"]="interupt",
                valid: Optional[Status]=None, invalid: Optional[Status]=None, close=True, **context) -> requests.Response:
        response = session.request(method, url, **messages, allow_redirects=allow_redirects)
        self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
        if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
        return response.close() if close else response

    @encode_messages
    def request_status(self, method: str, url: str, session: requests.Session, messages: Dict=dict(),
                        params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                        allow_redirects=True, validate=False, exception: Literal["error","interupt"]="interupt",
                        valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> int:
        with session.request(method, url, **messages, allow_redirects=allow_redirects) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.status_code

    @encode_messages
    def request_content(self, method: str, url: str, session: requests.Session, messages: Dict=dict(),
                        params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                        allow_redirects=True, validate=False, exception: Literal["error","interupt"]="interupt",
                        valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> bytes:
        with session.request(method, url, **messages, allow_redirects=allow_redirects) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.content

    @encode_messages
    def request_text(self, method: str, url: str, session: requests.Session, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                    allow_redirects=True, validate=False, exception: Literal["error","interupt"]="interupt",
                    valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> str:
        with session.request(method, url, **messages, allow_redirects=allow_redirects) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.text

    @encode_messages
    def request_json(self, method: str, url: str, session: requests.Session, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                    allow_redirects=True, validate=False, exception: Literal["error","interupt"]="interupt",
                    valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> JsonData:
        with session.request(method, url, **messages, allow_redirects=allow_redirects) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.json()

    @encode_messages
    def request_headers(self, method: str, url: str, session: requests.Session, messages: Dict=dict(),
                        params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                        allow_redirects=True, validate=False, exception: Literal["error","interupt"]="interupt",
                        valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> Dict:
        with session.request(method, url, **messages, allow_redirects=allow_redirects) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.headers

    @encode_messages
    def request_table(self, method: str, url: str, session: requests.Session, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                    allow_redirects=True, validate=False, exception: Literal["error","interupt"]="interupt",
                    valid: Optional[Status]=None, invalid: Optional[Status]=None, html=True, table_header=0, table_idx=0,
                    engine: Optional[Literal["xlrd","openpyxl","odf","pyxlsb"]]=None, **context) -> pd.DataFrame:
        with session.request(method, url, **messages, allow_redirects=allow_redirects) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            if html: return pd.read_html(response.text, header=table_header)[table_idx]
            else: return pd.read_excel(response.content, engine=engine)

    def encode_params(self, url: str, params: Optional[Dict]=None, encode: Optional[bool]=None) -> Tuple[str,Dict]:
        if not params: return url, None
        elif not isinstance(encode, bool): return url, params
        else: return encode_params(url, params, encode=encode), None

    def validate_status(self, response: requests.Response, how: Literal["error","interupt"]="interupt",
                        valid: Optional[Status]=None, invalid: Optional[Status]=None):
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
                    account: Account=dict(), credentials: IDTokenCredentials=None, **context):
        if data_empty(data) or not uploadInfo: return
        context = UPLOAD_CONTEXT(**context)
        for uploadContext in uploadInfo.values():
            if len(kloc(uploadContext, [KEY, SHEET], if_null="drop")) == 2:
                self.upload_gspread(data=data, **uploadContext, account=account, **context)
            elif len(kloc(uploadContext, [TABLE, PID], if_null="drop")) == 2:
                self.upload_gbq(data=data, **uploadContext, reauth=reauth, credentials=credentials, **context)

    ###################################################################
    ########################## Google Spread ##########################
    ###################################################################

    def set_gs_query(self, key: str, sheet: str, fields: IndexLabel, str_cols: NumericiseIgnore=list(),
                    arr_cols: Sequence[IndexLabel]=list(), account: Account=dict(), **context):
        renameMap = self.get_rename_map(**context)
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
                    account: Account=dict(), **context) -> pd.DataFrame:
        renameMap = self.get_rename_map(**context)
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
        schema = schema if schema and is_records(schema) else self.get_gbq_schema(mode=mode, schema=schema, **context)
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
    iterateUnit = 0
    redirectUnit = 0
    pagination = False
    pageUnit = 0
    pageLimit = 0
    interval = str()
    fromNow = None
    tzinfo = None
    datetimeUnit = "second"
    returnType = "records"
    renameMap = dict()
    maxLimit = MAX_ASYNC_TASK_LIMIT
    redirectLimit = MAX_REDIRECT_LIMIT
    message = str()

    def __init__(self, fields: IndexLabel=list(), contextFields: IndexLabel=list(),
                iterateUnit: Unit=0, interval: Timedelta=str(), fromNow: Optional[int]=None,
                startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                datetimeUnit: Literal["second","minute","hour","day","month","year"]="second",
                tzinfo: Optional[Timezone]=None, returnType: Optional[TypeHint]=None,
                logName=str(), logLevel: LogLevel="WARN", logFile: Optional[str]=str(), debug=False,
                renameMap: RenameMap=dict(), schemaInfo: SchemaInfo=dict(), delay: Union[float,int,Tuple[int]]=1.,
                progress=True, message=str(), cookies=str(), numTasks=100, queryInfo: Optional[GspreadReadInfo]=dict(),
                apiRedirect=False, redirectUnit: Unit=0, **context):
        Spider.__init__(
            self, fields=fields, iterateUnit=iterateUnit, interval=interval, fromNow=fromNow,
            startDate=startDate, endDate=endDate, tzinfo=tzinfo, datetimeUnit=datetimeUnit, returnType=returnType,
            logName=logName, logLevel=logLevel, logFile=logFile, debug=debug, renameMap=renameMap,
            schemaInfo=schemaInfo, delay=delay, progress=progress, message=message, cookies=cookies)
        self.numTasks = cast_int(numTasks, default=MIN_ASYNC_TASK_LIMIT)
        self.apiRedirect = apiRedirect
        if is_empty(self.redirectArgs, strict=True): self.redirectArgs = self.iterateArgs
        self.redirectUnit = exists_one(redirectUnit, self.redirectUnit, self.iterateUnit, strict=False)
        if context: self.update(kloc(UNIQUE_CONTEXT(**context), contextFields, if_null="pass"))
        self.set_query(queryInfo, **context)

    ###################################################################
    ########################## Async Managers #########################
    ###################################################################

    def asyncio_task(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSpider, *args, self_var=True, uploadInfo: Optional[UploadInfo]=dict(), **kwargs):
            if self_var: kwargs = TASK_CONTEXT(**dict(self.__dict__, **kwargs))
            semaphore = self.asyncio_semaphore(**kwargs)
            data = await func(self, *args, semaphore=semaphore, **kwargs)
            self.upload_data(data, uploadInfo, **kwargs)
            return data
        return wrapper

    def asyncio_session(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSpider, *args, self_var=True, uploadInfo: Optional[UploadInfo]=dict(), **kwargs):
            if self_var: kwargs = REQUEST_CONTEXT(**dict(self.__dict__, **kwargs))
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
                self.logger.error(log_exception(name, *args, json=self.logJson, **REQUEST_CONTEXT(**kwargs)))
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
    async def crawl(self, *args, **context) -> Data:
        args, context = self.set_var(locals())
        return await self.gather(*args, **context)

    @asyncio_redirect
    async def gather(self, *args, message=str(), progress=True, iterateArgs: _PASS=None, iterateQuery: _PASS=None,
                    pagination: _PASS=None, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None, **context) -> Data:
        message = self.get_message(*args, message=message, **context)
        if self.debug: self.logger.warn({"args":args, "context":context})
        iterate_params = dict(iterateArgs=self.iterateArgs, iterateQuery=self.iterateQuery, pagination=self.pagination)
        iterator, context = self.set_iterator(*args, **iterate_params, **context)
        if self.debug: self.logger.warn({"iterator":iterator})
        data = await tqdm.gather(*[
                self.fetch(**__i, fields=fields, **context) for __i in iterator], desc=message, disable=(not progress))
        return self.map_reduce(data, fields=fields, returnType=returnType, **context)

    @abstractmethod
    @asyncio_errors
    @asyncio_limit
    async def fetch(self, *args, **context) -> Data:
        ...

    def encode_messages(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSpider, method: str, url: str, session: Optional[aiohttp.ClientSession]=None,
                        messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                        data=None, json=None, headers=None, cookies=str(), *args, **kwargs):
            session = session if session else aiohttp
            url, params = self.encode_params(url, params, encode=encode)
            if headers and cookies: headers["Cookie"] = str(cookies)
            messages = messages if messages else dict(params=params, data=data, json=json, headers=headers)
            self.logger.debug(log_messages(**messages, logJson=self.logJson))
            return await func(self, method=method, url=url, session=session, messages=messages, **kwargs)
        return wrapper

    @encode_messages
    async def request_status(self, method: str, url: str, session: aiohttp.ClientSession, messages: Dict=dict(),
                            params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                            allow_redirects=True, validate=False, exception: Literal["error","interupt"]="interupt",
                            valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> int:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.status

    @encode_messages
    async def request_content(self, method: str, url: str, session: aiohttp.ClientSession, messages: Dict=dict(),
                            params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                            allow_redirects=True, validate=False, exception: Literal["error","interupt"]="interupt", 
                            valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> bytes:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return await response.read()

    @encode_messages
    async def request_text(self, method: str, url: str, session: aiohttp.ClientSession, messages: Dict=dict(),
                            params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                            allow_redirects=True, validate=False, exception: Literal["error","interupt"]="interupt",
                            valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> str:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return await response.text()

    @encode_messages
    async def request_json(self, method: str, url: str, session: aiohttp.ClientSession, messages: Dict=dict(),
                            params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                            allow_redirects=True, validate=False, exception: Literal["error","interupt"]="interupt",
                            valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> JsonData:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return await response.json()

    @encode_messages
    async def request_headers(self, method: str, url: str, session: aiohttp.ClientSession, messages: Dict=dict(),
                            params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                            allow_redirects=True, validate=False, exception: Literal["error","interupt"]="interupt",
                            valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> Dict:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.headers

    @encode_messages
    async def request_table(self, method: str, url: str, session: aiohttp.ClientSession, messages: Dict=dict(),
                            params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                            allow_redirects=True, validate=False, exception: Literal["error","interupt"]="interupt",
                            valid: Optional[Status]=None, invalid: Optional[Status]=None, html=False, table_header=0, table_idx=0,
                            engine: Optional[Literal["xlrd","openpyxl","odf","pyxlsb"]]=None, **context) -> pd.DataFrame:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            if html: return pd.read_html(await response.text(), header=table_header)[table_idx]
            else: return pd.read_excel(await response.read(), engine=engine)

    def validate_status(self, response: aiohttp.ClientResponse, how: Literal["error","interupt"]="interupt",
                        valid: Optional[Status]=None, invalid: Optional[Status]=None):
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
    async def redirect(self, *args, message=str(), progress=True, iterateArgs: _PASS=None, iterateQuery: _PASS=None,
                        iterateUnit: _PASS=None, redirectUnit: Unit=1, pagination: _PASS=None,
                        fields: IndexLabel=list(), returnType: Optional[TypeHint]=None, **context) -> Data:
        message = self.get_redirect_message(*args, message=message, **context)
        if self.debug: self.logger.warn({"args":args, "context":context})
        redirect_context = dict(iterateArgs=self.redirectArgs, iterateQuery=self.redirectQuery, iterateUnit=redirectUnit)
        iterator, context = self.set_iterator(*args, **redirect_context, **context)
        if self.debug: self.logger.warn({"iterator":iterator})
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
        data = self._filter_redirect_data(redirectUrl, authorization, account, **context)
        response = self.request_text("POST", redirectUrl, session, json=data, headers=dict(Authorization=authorization))
        return self._parse_redirect(json.loads(response), **context)

    def _filter_redirect_data(self, redirectUrl: str, authorization: str, account: Account=dict(), **context) -> Context:
        return dict(
            redirectUrl=redirectUrl,
            authorization=authorization,
            account=account,
            **REDIRECT_CONTEXT(**context))

    def _parse_redirect(self, data: RedirectData, **context) -> Records:
        data = self._log_redirect_errors(data)
        results = self._map_redirect(data, **context)
        self.logger.debug(log_data(results))
        return results

    def _log_redirect_errors(self, data: RedirectData, **context) -> Records:
        if not isinstance(data, Dict): raise ValueError(INVALID_REDIRECT_LOG_MSG)
        errors = data.get("errors", default=dict())
        for key, values in (errors if isinstance(errors, Dict) else dict()).items():
            self.errors[key] += values
        return data.get("data", default=list())

    def _map_redirect(self, data: Records, **context) -> Records:
        if not isinstance(data, List): return list()
        cast_datetime_or_keep = lambda x: cast_datetime_format(x, default=x)
        return [apply_df(__m, apply=cast_datetime_or_keep, all_cols=True) for __m in data]


###################################################################
########################## Login Spiders ##########################
###################################################################

class LoginSpider(requests.Session, Spider):
    __metaclass__ = ABCMeta
    asyncio = False
    operation = "login"

    @abstractmethod
    def __init__(self, cookies=str(), logName=str(), logLevel: LogLevel="WARN", logFile: Optional[str]=str(), **context):
        super().__init__()
        self.operation = self.operation
        if cookies: self.cookies.update(decode_cookies(cookies))
        self.logName = logName if logName else self.operation
        self.logLevel = int(logLevel) if str(logLevel).isdigit() else logging.getLevelName(str(logLevel).upper())
        self.logFile = logFile
        self.logJson = bool(logFile)
        self.logger = CustomLogger(name=self.logName, level=self.logLevel, file=self.logFile)

    @abstractmethod
    def login(self):
        ...

    def get_cookies(self, returnType: Literal["str","dict"]="str") -> Union[str,Dict]:
        if returnType == "str": return parse_cookies(self.cookies)
        elif returnType == "dict": return dict(self.cookies.items())
        else: return self.cookies

    def update_cookies(self, cookies=str(), if_exists: Literal["ignore","replace"]="ignore", **kwargs):
        cookies = decode_cookies(cookies, **kwargs) if isinstance(cookies, str) else dict(cookies, **kwargs)
        for __key, __value in cookies.items():
            if (if_exists == "replace") or (__key not in self.cookies):
                self.cookies.set(__key, __value)

    ###################################################################
    ########################## Fetch Request ##########################
    ###################################################################

    def encode_messages(func):
        @functools.wraps(func)
        def wrapper(self: Spider, method: str, url: str,
                    messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                    data=None, json=None, headers=None, cookies=str(), *args, **kwargs):
            url, params = self.encode_params(url, params, encode=encode)
            if headers and cookies: headers["Cookie"] = str(cookies)
            messages = messages if messages else dict(params=params, data=data, json=json, headers=headers)
            self.logger.debug(log_messages(**messages, logJson=self.logJson))
            return func(self, method=method, url=url, messages=messages, **kwargs)
        return wrapper

    @encode_messages
    def request_url(self, method: str, url: str, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None,
                    headers=None, cookies=str(), allow_redirects=True, verify=None, **context):
        with self.request(method, url, **messages, allow_redirects=allow_redirects, verify=verify) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            self.logger.debug(dict(cookies=self.get_cookies()))

    @encode_messages
    def request_status(self, method: str, url: str, messages: Dict=dict(),
                        params=None, encode: Optional[bool]=None, data=None, json=None,
                        headers=None, cookies=str(), allow_redirects=True, verify=None, **context) -> int:
        with self.request(method, url, **messages, allow_redirects=allow_redirects, verify=verify) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            self.logger.debug(dict(cookies=self.get_cookies()))
            return response.status_code

    @encode_messages
    def request_content(self, method: str, url: str, messages: Dict=dict(),
                        params=None, encode: Optional[bool]=None, data=None, json=None,
                        headers=None, cookies=str(), allow_redirects=True, verify=None, **context) -> bytes:
        with self.request(method, url, **messages, allow_redirects=allow_redirects, verify=verify) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            self.logger.debug(dict(cookies=self.get_cookies()))
            return response.content

    @encode_messages
    def request_text(self, method: str, url: str, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None,
                    headers=None, cookies=str(), allow_redirects=True, verify=None, **context) -> str:
        with self.request(method, url, **messages, allow_redirects=allow_redirects, verify=verify) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            self.logger.debug(dict(cookies=self.get_cookies()))
            return response.text

    @encode_messages
    def request_json(self, method: str, url: str, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None,
                    headers=None, cookies=str(), allow_redirects=True, verify=None, **context) -> JsonData:
        with self.request(method, url, **messages, allow_redirects=allow_redirects, verify=verify) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            self.logger.debug(dict(cookies=self.get_cookies()))
            return response.json()

    @encode_messages
    def request_headers(self, method: str, url: str, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None,
                    headers=None, cookies=str(), allow_redirects=True, verify=None, **context) -> Dict:
        with self.request(method, url, **messages, allow_redirects=allow_redirects, verify=verify) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            self.logger.debug(dict(cookies=self.get_cookies()))
            return response.headers


class BaseLogin(LoginSpider):
    operation = "login"

    def __init__(self, cookies=str()):
        super().__init__()
        self.operation = self.operation
        self.update_cookies(cookies)

    def login(self):
        return


###################################################################
######################## Encrypted Spiders ########################
###################################################################

class EncryptedSpider(Spider):
    __metaclass__ = ABCMeta
    asyncio = False
    operation = "spider"
    where = WHERE
    which = WHICH
    fields = list()
    iterateArgs = list()
    iterateQuery = list()
    iterateUnit = 0
    pagination = False
    pageUnit = 0
    pageLimit = 0
    interval = str()
    fromNow = None
    tzinfo = None
    datetimeUnit = "second"
    returnType = "records"
    renameMap = dict()
    message = str()
    auth = LoginSpider
    decryptedKey = dict()

    def __init__(self, fields: IndexLabel=list(), contextFields: IndexLabel=list(),
                iterateUnit: Optional[Unit]=0, interval: Optional[Timedelta]=str(), fromNow: Optional[int]=None,
                startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                datetimeUnit: Literal["second","minute","hour","day","month","year"]="second",
                tzinfo: Optional[Timezone]=None, returnType: Optional[TypeHint]=None,
                logName=str(), logLevel: LogLevel="WARN", logFile: Optional[str]=str(), debug=False,
                renameMap: RenameMap=dict(), schemaInfo: SchemaInfo=dict(), delay: Union[float,int,Tuple[int]]=1.,
                progress=True, message=str(), cookies=str(), queryInfo: Optional[GspreadReadInfo]=dict(),
                encryptedKey: EncryptedKey=str(), decryptedKey: Union[str,Dict]=str(), **context):
        Spider.__init__(
            self, fields=fields, contextFields=contextFields, iterateUnit=iterateUnit, interval=interval, fromNow=fromNow,
            startDate=startDate, endDate=endDate, tzinfo=tzinfo, datetimeUnit=datetimeUnit, returnType=returnType,
            logName=logName, logLevel=logLevel, logFile=logFile, debug=debug, renameMap=renameMap, schemaInfo=schemaInfo,
            delay=delay, progress=progress, message=message, cookies=cookies, queryInfo=queryInfo, **context)
        if not cookies:
            self.set_secrets(encryptedKey, decryptedKey)

    def set_secrets(self, encryptedKey=str(), decryptedKey=str()):
        if not self.decryptedKey and not isinstance(decryptedKey, Dict):
            try: decryptedKey = json.loads(decryptedKey if decryptedKey else decrypt(encryptedKey,1))
            except JSONDecodeError: raise ValueError(INVALID_USER_INFO_MSG(self.where))
        self.decryptedKey = decryptedKey if decryptedKey else self.decryptedKey
        self.logger.info(log_encrypt(**self.decryptedKey, show=3))

    def login_session(func):
        @functools.wraps(func)
        def wrapper(self: EncryptedSpider, *args, self_var=True, cookies=str(),
                    uploadInfo: Optional[UploadInfo]=dict(), **kwargs):
            if self_var: kwargs = REQUEST_CONTEXT(**dict(self.__dict__, **kwargs))
            with (BaseLogin(cookies) if cookies else self.auth(**dict(kwargs, **self.decryptedKey))) as session:
                session.login()
                session.update_cookies(self.set_cookies(**kwargs), if_exists="replace")
                data = func(self, *args, session=session, cookies=cookies, **kwargs)
            time.sleep(.25)
            self.upload_data(data, uploadInfo, **kwargs)
            return data
        return wrapper

    def set_cookies(self, **context) -> Dict:
        return dict()


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
    iterateUnit = 0
    redirectUnit = 0
    pagination = False
    pageUnit = 0
    pageLimit = 0
    interval = str()
    fromNow = None
    tzinfo = None
    datetimeUnit = "second"
    returnType = "records"
    rename = dict()
    maxLimit = MAX_ASYNC_TASK_LIMIT
    redirectLimit = MAX_REDIRECT_LIMIT
    message = str()
    auth = LoginSpider
    decryptedKey = dict()

    def __init__(self, fields: IndexLabel=list(), contextFields: IndexLabel=list(),
                iterateUnit: Optional[Unit]=0, interval: Optional[Timedelta]=str(), fromNow: Optional[int]=None,
                startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                datetimeUnit: Literal["second","minute","hour","day","month","year"]="second",
                tzinfo: Optional[Timezone]=None, returnType: Optional[TypeHint]=None,
                logName=str(), logLevel: LogLevel="WARN", logFile: Optional[str]=str(), debug=False,
                renameMap: RenameMap=dict(), schemaInfo: SchemaInfo=dict(), delay: Union[float,int,Tuple[int]]=1.,
                progress=True, message=str(), cookies=str(), numTasks=100, queryInfo: Optional[GspreadReadInfo]=dict(),
                apiRedirect=False, redirectUnit: Unit=0, encryptedKey: EncryptedKey=str(), decryptedKey=str(), **context):
        AsyncSpider.__init__(
            self, fields=fields, contextFields=contextFields, iterateUnit=iterateUnit, interval=interval, fromNow=fromNow,
            startDate=startDate, endDate=endDate, tzinfo=tzinfo, datetimeUnit=datetimeUnit, returnType=returnType,
            logName=logName, logLevel=logLevel, logFile=logFile, debug=debug,
            renameMap=renameMap, schemaInfo=schemaInfo, delay=delay, progress=progress, message=message, cookies=cookies,
            numTasks=numTasks, queryInfo=queryInfo, apiRedirect=apiRedirect, redirectUnit=redirectUnit, **context)
        if not cookies:
            self.set_secrets(encryptedKey, decryptedKey)

    def login_session(func):
        @functools.wraps(func)
        async def wrapper(self: EncryptedAsyncSpider, *args, self_var=True, cookies=str(),
                            uploadInfo: Optional[UploadInfo]=dict(), **kwargs):
            if self_var: kwargs = REQUEST_CONTEXT(**dict(self.__dict__, **kwargs))
            semaphore = self.asyncio_semaphore(**kwargs)
            with (BaseLogin(cookies) if cookies else self.auth(**dict(kwargs, **self.decryptedKey))) as auth:
                auth.login()
                auth.update_cookies(self.set_cookies(**kwargs), if_exists="replace")
                async with aiohttp.ClientSession(cookies=auth.cookies) as session:
                    data = await func(self, *args, session=session, semaphore=semaphore, **kwargs)
            await asyncio.sleep(.25)
            self.upload_data(data, uploadInfo, **kwargs)
            return data
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
                debug=False, root: _KT=list(), renameMap: RenameMap=dict(), schemaInfo: SchemaInfo=dict(), **context):
        BaseSession.__init__(
            self, fields=fields, logName=logName, logLevel=logLevel, logFile=logFile, debug=debug,
            renameMap=renameMap, schemaInfo=schemaInfo, **context)
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
    iterateUnit = 0
    datetimeUnit = "second"
    tzinfo = None
    returnType = "dataframe"
    renameMap = dict()

    @abstractmethod
    @Spider.requests_task
    def crawl(self, *args, **context) -> Data:
        args, context = self.set_var(locals())
        return self.gather(*args, **context)

    @abstractmethod
    def gather(self, **context) -> Data:
        args = list()
        for cralwer in self.dependencies:
            operation = self.get_operation(cralwer, **context)
            data = self.crawl_proxy(cralwer, prefix=operation, **context)
            args.append(data)
        return self.map_reduce(*args, **context)

    def get_operation(self, crawler: Spider, **context) -> str:
        operation = crawler.__dict__.get("operation")
        if not operation: raise ValueError(DEPENDENCY_HAS_NO_NAME_MSG)
        renameMap = self.get_rename_map(**context)
        return renameMap.get(operation, operation)

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
    iterateUnit = 0
    datetimeUnit = "second"
    tzinfo = None
    returnType = "dataframe"
    renameMap = dict()

    @abstractmethod
    @AsyncSpider.asyncio_task
    async def crawl(self, *args, **context) -> Data:
        args, context = self.set_var(locals())
        return await self.gather(*args, **context)

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
