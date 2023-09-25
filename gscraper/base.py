from __future__ import annotations
from .context import UNIQUE_CONTEXT, TASK_CONTEXT, REQUEST_CONTEXT, LOGIN_CONTEXT, API_CONTEXT
from .context import RESPONSE_CONTEXT, UPLOAD_CONTEXT, PROXY_CONTEXT, REDIRECT_CONTEXT
from .types import _KT, _VT, _PASS, ClassInstance, Arguments, Context, TypeHint, LogLevel
from .types import RenameMap, IndexLabel, EncryptedKey, Pagination, Pages
from .types import Status, Unit, DateFormat, DateQuery, Timedelta, Timezone
from .types import JsonData, RedirectData, Records, TabularData, Data
from .types import SchemaInfo, Account, NumericiseIgnore, BigQuerySchema, GspreadReadInfo, UploadInfo
from .types import is_dataframe_type, allin_instance, is_array, is_str_array, is_records
from .types import init_origin, inspect_function

from .cast import cast_list, cast_tuple, cast_int, cast_int1, cast_datetime_format
from .date import now, get_date, get_date_range, is_daily_frequency
from .gcloud import fetch_gcloud_authorization, update_gspread, read_gspread
from .gcloud import IDTokenCredentials, read_gbq, to_gbq, validate_upsert_key
from .logs import CustomLogger, dumps_data
from .logs import log_encrypt, log_messages, log_response, log_client, log_data, log_exception, log_table
from .map import not_na, data_exists, unique, to_array, get_scala, diff
from .map import iloc, fill_array, is_same_length, unit_array, concat_array, align_array, transpose_array
from .map import kloc, apply_dict, chain_dict, drop_dict, exists_dict, cloc, apply_df
from .map import exists_one, convert_data, rename_data, filter_data, chain_exists, set_data, data_empty
from .parse import parse_cookies, parse_origin, decode_cookies, encode_params, validate_schema, parse_schema

from abc import ABCMeta, abstractmethod
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import asyncio
import aiohttp
import functools
import requests
import time

from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple, Type, Union
from json import JSONDecodeError
import datetime as dt
import json
import logging
import pandas as pd

from tqdm.auto import tqdm
from itertools import product
import base64
import inspect
import os
import random


GET = "GET"
POST = "POST"
OPTIONS = "OPTIONS"
HEAD = "HEAD"
PUT = "PUT"
PATCH = "PATCH"
DELETE = "DELETE"

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
PAGE_PARAMS = ["size", "pageSize", "pageStart", "offset"]

DATE_ITERATOR = ["startDate", "endDate"]
DATE_PARAMS = ["startDate", "endDate", "interval"]

CHECKPOINT = [
    "all", "context", "crawl", "params", "iterator", "gather", "gather_size", "redirect",
    "request", "response", "parse", "login", "api", "exception"]
CHECKPOINT_PATH = "saved/"

GATHER_MSG = lambda which, where=str(), by=str(): \
    f"Collecting {which}{(' by '+by) if by else str()}{(' from '+where) if where else str()}"
USER_INTERRUPT_MSG = lambda where: f"Interrupt occurred on {where} by user."
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
        self.update(self.__dict__)

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
    by = str()
    fields = list()
    tzinfo = None
    datetimeUnit = "second"
    responseType = None
    returnType = None
    errors = list()
    renameMap = dict()
    schemaInfo = dict()

    def __init__(self, fields: IndexLabel=list(),
                tzinfo: Optional[Timezone]=None, datetimeUnit: Literal["second","minute","hour","day"]="second",
                returnType: Optional[TypeHint]=None, logName=str(), logLevel: LogLevel="WARN", logFile=str(),
                debug: List[str]=list(), extraSave: List[str]=list(), interrupt=str(), localSave=False,
                renameMap: RenameMap=dict(), schemaInfo: SchemaInfo=dict(), **context):
        super().__init__(context)
        self.operation = self.operation
        self.fields = fields if fields else self.fields
        self.tzinfo = tzinfo if tzinfo else self.tzinfo
        self.datetimeUnit = datetimeUnit if datetimeUnit else self.datetimeUnit
        self.initTime = now(tzinfo=self.tzinfo, droptz=True, unit=self.datetimeUnit)
        self.returnType = returnType if returnType else returnType
        self.renameMap = renameMap if renameMap else self.renameMap
        self.set_logger(logName, logLevel, logFile, debug, extraSave, interrupt, localSave)
        self.set_schema(schemaInfo)

    def set_logger(self, logName=str(), logLevel: LogLevel="WARN", logFile=str(),
                    debug: List[str]=list(), extraSave: List[str]=list(), interrupt=str(), localSave=False):
        logName = logName if logName else self.operation
        self.logLevel = int(logLevel) if str(logLevel).isdigit() else logging.getLevelName(str(logLevel).upper())
        self.logFile = logFile
        self.logJson = bool(logFile)
        self.logger = CustomLogger(name=logName, level=self.logLevel, file=self.logFile)
        self.debug = cast_list(debug)
        self.extraSave = cast_list(extraSave)
        self.interrupt = interrupt
        self.localSave = localSave

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

    def get_rename_map(self, **context) -> RenameMap:
        return self.renameMap

    def rename(self, string: str, **context) -> str:
        renameMap = self.get_rename_map(**context)
        return renameMap[string] if renameMap and (string in renameMap) else string

    ###################################################################
    ############################ Checkpoint ###########################
    ###################################################################

    def checkpoint(self, point: str, where: str, msg: Dict,
                    save: Optional[Data]=None, ext: Optional[TypeHint]=None):
        if (point in self.debug) or ("all" in self.debug):
            self.logger.warning(dict(point=f"[{str(point).upper()}]", **dumps_data(msg, limit=0)))
        if ((point in self.extraSave) or ("all" in self.extraSave)) and data_exists(save):
            if (ext == "response"): save, ext = self._check_response(save)
            self._validate_dir(CHECKPOINT_PATH)
            self.save_data(save, prefix=CHECKPOINT_PATH+str(point).replace('.','_'), ext=ext)
        if self.interrupt == point:
            raise KeyboardInterrupt(USER_INTERRUPT_MSG(where))

    def save_data(self, data: Data, prefix=str(), ext: Optional[TypeHint]=None):
        prefix = self.rename(prefix if prefix else self.operation, to="ko")
        file = prefix+'_'+self.now("%Y%m%d%H%M%S")
        ext = ext if ext else type(data)
        if is_dataframe_type(ext):
            self.save_dataframe(data, file+".xlsx")
        elif ext == "html" and isinstance(data, str):
            self.save_source(data, file+".html")
        else: self.save_json(data, file+".json")

    def save_json(self, data: Data, file: str):
        file = self._validate_file(file)
        if isinstance(data, pd.DataFrame): data = data.to_dict("records")
        with open(file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)

    def save_dataframe(self, data: Data, file: str):
        file = self._validate_file(file)
        data = convert_data(data, return_type="dataframe")
        data.rename(columns=self.get_rename_map(to="ko")).to_excel(file, index=False)

    def save_source(self, data: str, file: str):
        file = self._validate_file(file)
        source = BeautifulSoup(data, "html.parser")
        with open(file, "w", encoding="utf-8") as f:
            f.write(str(source))

    def _validate_dir(self, dir: str):
        if not os.path.exists(dir):
            os.mkdir(dir)

    def _validate_file(self, file: str):
        file, ext = os.path.splitext(file)
        __i, suffix = 1, str()
        while True:
            if os.path.exists(file+suffix+ext):
                suffix, __i = f"_{__i}", __i+1
            else: break
        return file+suffix+ext

    def _check_response(self, response: Data) -> Tuple[Data, TypeHint]:
        if isinstance(response, str) and response:
            try: return json.loads(response), "json"
            except: response, "html"
        elif isinstance(response, pd.DataFrame):
            return response, "dataframe"
        else: return response, "json"

    ###################################################################
    ########################### Log Managers ##########################
    ###################################################################

    def catch_exception(func):
        @functools.wraps(func)
        def wrapper(self: BaseSession, *args, **context):
            try: return func(self, *args, **context)
            except KeyboardInterrupt as interrupt:
                raise interrupt
            except Exception as exception:
                if "exception" in self.debug:
                    self.checkpoint("exception", where=func.__name__, msg={"args":args, "context":context})
                    raise exception
                self.log_errors(*args, **context)
                func_name = f"{func.__name__}({self.__class__.__name__})"
                self.logger.error(log_exception(func_name, json=self.logJson, **REQUEST_CONTEXT(**context)))
                return init_origin(func)
        return wrapper

    def log_errors(self, *args, **context):
        self.errors.append(dict({"args":args}, **context))

    def validate_response(func):
        @functools.wraps(func)
        def wrapper(self: BaseSession, response: Any, *args, **context):
            is_valid = self.is_valid_response(response)
            data = func(self, response, *args, **context) if is_valid else init_origin(func)
            suffix = f"_{context.get('index')}" if context.get("index") else str()
            self.checkpoint("parse"+suffix, where=func.__name__, msg={"data":data}, save=data)
            self.log_results(data, **context)
            return data
        return wrapper

    def log_results(self, data: Data, **context):
        self.logger.info(log_data(data, **context))

    def is_valid_response(self, response: Any) -> bool:
        return not_na(response, strict=False)


###################################################################
############################# Iterator ############################
###################################################################

class Iterator(CustomDict):
    iterateArgs = list()
    iterateProduct = list()
    iterateUnit = 1
    pagination = False
    pageUnit = 0
    pageLimit = 0
    interval = str()
    fromNow = None

    def __init__(self, iterateUnit: Optional[Unit]=0, interval: Optional[Timedelta]=str(), fromNow: Optional[Unit]=None):
        super().__init__()
        self.iterateUnit = iterateUnit if iterateUnit else self.iterateUnit
        self.interval = interval if interval else self.interval
        self.fromNow = fromNow if not_na(fromNow, strict=True) else self.fromNow

    def get_date(self, date: Optional[DateFormat]=None, fromNow: Optional[Unit]=None, index=0) -> dt.date:
        fromNow = fromNow if fromNow else self.fromNow
        return get_date(date, if_null=get_scala(fromNow, index=index))

    def get_date_pair(self, startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                        fromNow: Optional[Unit]=None) -> Tuple[dt.date,dt.date]:
        startDate = self.get_date(startDate, fromNow=fromNow, index=0)
        endDate = self.get_date(endDate, fromNow=fromNow, index=1)
        if startDate: startDate = min(startDate, endDate) if endDate else startDate
        if endDate: endDate = max(startDate, endDate) if startDate else endDate
        return startDate, endDate

    ###################################################################
    ########################### Set Iterator ##########################
    ###################################################################

    def get_iterator(self, _args=True, _page=True, _date=True, _product=True, _params=False,
                    keys_only=False, values_only=False, if_null: Literal["drop","pass"]="drop",
                    **context) -> Union[Context,_KT,_VT]:
        iterateArgs = self.iterateArgs * int(_args)
        iteratePage = (PAGE_PARAMS if _params else PAGE_ITERATOR) * int(bool(self.pagination)) * int(_page)
        iterateDate = (DATE_PARAMS if _params else DATE_ITERATOR) * int(bool(self.interval)) * int(_date)
        iterateProduct = self.iterateProduct * int(_product)
        query = unique(*iterateArgs, *iteratePage, *iterateDate, *iterateProduct)
        if keys_only: return query
        else: return kloc(context, query, if_null=if_null, values_only=values_only)

    def set_iterator(self, *args: Sequence, iterateArgs: List[_KT]=list(), iterateProduct: List[_KT]=list(),
                    iterateUnit: Unit=1, pagination: Pagination=False, interval: Timedelta=str(), index=True,
                    **context) -> Tuple[List[Context],Context]:
        arguments, periods, ranges, iterateUnit = list(), list(), list(), cast_list(iterateUnit)
        args_context = self._check_args(*args, iterateArgs=iterateArgs, pagination=pagination)
        if args_context:
            arguments, context = self._from_args(*args, **args_context, iterateUnit=iterateUnit, **context)
            iterateProduct = diff(iterateProduct, iterateArgs, PAGE_ITERATOR)
            if len(iterateProduct) > 1: iterateUnit = iterateUnit[1:]
        if interval:
            periods, context = self._from_date(interval=interval, **context)
            iterateProduct = diff(iterateProduct, DATE_ITERATOR+["date"])
        ranges, context = self._from_context(iterateProduct=iterateProduct, iterateUnit=iterateUnit, **context)
        iterator = self._product_iterator(arguments, periods, ranges, index=index)
        return iterator, context

    ###################################################################
    ########################## From Arguments #########################
    ###################################################################

    def _check_args(self, *args, iterateArgs: List[_KT], pagination: Pagination=False) -> Context:
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

    def _from_numeric_pages(self, *args, size: Unit, pageSize=0, pageStart=1, offset=1, **context) -> Tuple[List[List],Context]:
        pageSize = self.validate_page_size(pageSize, self.pageUnit, self.pageLimit)
        pages = self.get_pages(size, pageSize, pageStart, offset, how="all")
        if isinstance(size, int):
            pages = [pages] * len(get_scala(args, default=[0]))
        pages = list(map(lambda __s: tuple(map(tuple, __s)), map(transpose_array, pages)))
        return args+(pages,), dict(context, pageSize=pageSize)

    def _from_categorical_pages(self, *args, labels: List, pagination: str, size: Optional[int]=None,
                                pageSize=0, pageStart=1, offset=1, **context) -> Tuple[List[List],Context]:
        pages = list()
        for label in labels:
            size = self.get_size_by_label(label, size=size, **context)
            pageSize = self.get_page_size_by_label(label, pageSize=pageSize, **context)
            pageStart = self.get_page_start_by_label(label, pageStart=pageStart, **context)
            offset = self.get_offset_by_label(label, offset=offset, **context)
            iterator = self.get_pages(size, pageSize, pageStart, offset, how="all")
            num_pages = len(iterator[0])
            params = ((size,)*num_pages, (pageSize,)*num_pages, (pageStart,)*num_pages, (offset,)*num_pages)
            pages.append(tuple(map(tuple, transpose_array(((label,)*num_pages,)+iterator+params))))
        return args+(pages,), context

    def validate_page_size(self, pageSize: int, pageUnit=0, pageLimit=0, **context) -> int:
        if (pageUnit > 0) and (pageSize & pageUnit != 0):
            pageSize = round(pageSize / pageUnit) * pageUnit
        if pageLimit > 0:
            pageSize = min(pageSize, pageLimit)
        return pageSize

    def get_pages(self, size: Unit, pageSize: int, pageStart=1, offset=1, pageUnit=0, pageLimit=0,
                    how: Literal["all","page","start"]="all", **context) -> Union[Pages,List[Pages]]:
        pageSize = self.validate_page_size(pageSize, pageUnit, pageLimit)
        if isinstance(size, int):
            return self.calc_pages(cast_int1(size), pageSize, pageStart, offset, how)
        elif is_array(size):
            return [self.calc_pages(cast_int1(__sz), pageSize, pageStart, offset, how) for __sz in size]
        else: return tuple()

    def calc_pages(self, size: int, pageSize: int, pageStart=1, offset=1,
                    how: Literal["all","page","start"]="all", **context) -> Pages:
        pages = tuple(range(pageStart, (((size-1)//pageSize)+1)+pageStart))
        if how == "page": return pages
        starts = tuple(range(offset, size+offset, pageSize))
        if how == "start": return starts
        dataSize = tuple(min(size-start+1, pageSize) for start in starts)
        return (pages, starts, dataSize)

    def get_size_by_label(self, label: Any, size: Optional[int]=None, **context) -> int:
        return size

    def get_page_size_by_label(self, label: Any, pageSize=0, **context) -> int:
        return pageSize

    def get_page_start_by_label(self, label: Any, pageStart=1, **context) -> int:
        return pageStart

    def get_offset_by_label(self, label: Any, offset=1, **context) -> int:
        return offset

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
        date_range = get_date_range(*self.get_date_pair(startDate, endDate), interval=interval)
        if is_daily_frequency(interval):
            period = [dict(startDate=date, endDate=date, date=date) for date in date_range]
        elif len(date_range) > 1:
            period = [dict(startDate=start, endDate=(end-dt.timedelta(days=1)), date=(end-dt.timedelta(days=1)))
                        for start, end in zip(date_range, date_range[1:]+[endDate+dt.timedelta(days=1)])]
        else: period = [dict(startDate=startDate, endDate=endDate, date=endDate)]
        return period, dict(context, interval=interval)

    ###################################################################
    ########################### From Context ##########################
    ###################################################################

    def _from_context(self, iterateProduct: List[_KT], iterateUnit: Unit=1, **context) -> Tuple[List[Context],Context]:
        if not iterateProduct: return list(), context
        query, context = kloc(context, iterateProduct, if_null="drop"), drop_dict(context, iterateProduct, inplace=False)
        if any(map(lambda x: x>1, iterateUnit)): query = self._group_context(iterateProduct, iterateUnit, **query)
        else: query = [dict(zip(query.keys(), values)) for values in product(*map(cast_tuple, query.values()))]
        return query, context

    def _group_context(self, iterateProduct: List[_KT], iterateUnit: Unit=1, **context) -> List[Context]:
        query = apply_dict(context, apply=cast_list, all_keys=True)
        keys, unit = query.keys(), fill_array(iterateUnit, count=len(iterateProduct), value=1)
        combinations = product(*[range(0, len(query[key]), unit[i]) for i, key in enumerate(keys)])
        return [{key: query[key][index:index+unit[i]] for i, (key, index) in enumerate(zip(keys, indices))}
                for indices in combinations]

    def _product_iterator(self, *ranges: Sequence[Context], index=True) -> List[Context]:
        if sum(map(len, ranges)) == 0: return list()
        ranges_array = map((lambda x: x if x else [{}]), ranges)
        indexing = lambda __i: dict(index=__i) if index else dict()
        return [dict(**indexing(__i), **chain_dict(query)) for __i, query in enumerate(product(*ranges_array))]


###################################################################
############################## Spider #############################
###################################################################

class Spider(BaseSession, Iterator):
    __metaclass__ = ABCMeta
    asyncio = False
    operation = "spider"
    where = WHERE
    which = WHICH
    by = str()
    fields = list()
    iterateArgs = list()
    iterateProduct = list()
    iterateUnit = 1
    pagination = False
    pageUnit = 0
    pageLimit = 0
    interval = str()
    fromNow = None
    tzinfo = None
    datetimeUnit = "second"
    returnType = "records"
    renameMap = dict()
    ssl = None

    def __init__(self, fields: IndexLabel=list(), contextFields: IndexLabel=list(),
                iterateUnit: Unit=0, interval: Timedelta=str(), fromNow: Optional[Unit]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Literal["second","minute","hour","day"]="second",
                returnType: Optional[TypeHint]=None, logName=str(), logLevel: LogLevel="WARN", logFile=str(),
                debug: List[str]=list(), extraSave: List[str]=list(), interrupt=str(), localSave=False,
                renameMap: RenameMap=dict(), schemaInfo: SchemaInfo=dict(), delay: Union[float,int,Tuple[int]]=1.,
                progress=True, cookies=str(), queryInfo: GspreadReadInfo=dict(), **context):
        BaseSession.__init__(
            self, fields=fields, tzinfo=tzinfo, datetimeUnit=datetimeUnit, returnType=returnType,
            logName=logName, logLevel=logLevel, logFile=logFile,
            debug=debug, extraSave=extraSave, interrupt=interrupt, localSave=localSave,
            renameMap=renameMap, schemaInfo=schemaInfo)
        Iterator.__init__(self, iterateUnit=iterateUnit, interval=interval, fromNow=fromNow)
        self.delay = delay
        self.progress = progress
        self.cookies = cookies
        if context: self.update(kloc(UNIQUE_CONTEXT(**context), contextFields, if_null="pass"))
        self.set_query(queryInfo, **context)
        self.disable_warnings()

    def disable_warnings(self):
        if self.ssl == False:
            from urllib3.exceptions import InsecureRequestWarning
            import urllib3
            urllib3.disable_warnings(InsecureRequestWarning)

    def inspect(self, method: str, __type: Optional[TypeHint]=None, annotation: Optional[TypeHint]=None,
                ignore: List[_KT]=list()) -> Dict[_KT,Dict]:
        method = getattr(self, method)
        info = inspect_function(method, ignore=["self","context","kwargs"]+ignore)
        if __type or annotation:
            info = drop_dict(info, "__return__", inplace=False)
            if __type == "required":
                return {name: param for name, param in info.items() if "default" not in param}
            elif __type == "iterable":
                return {name: param for name, param in info.items() if "iterable" in param}
            return {name: param for name, param in info.items()
                    if (annotation in cast_tuple(param["annotation"])) or (__type in cast_tuple(param["type"]))}
        return info

    ###################################################################
    ###################### Local Variable Looker ######################
    ###################################################################

    def from_locals(self, locals: Dict=dict(), how: Literal["all","args","context","params","request","response"]="all",
                    drop: _KT=list(), **context) -> Union[Arguments,Context,Tuple]:
        context = self._map_locals(locals, drop, **context)
        if is_array(how): return tuple(self.from_locals(how=how, drop=drop, **context) for how in how)
        elif how == "args": return self.local_args(locals, drop=drop, **context)
        elif how == "context": return self.local_context(locals, drop=drop, **context)
        elif how == "params": return self.local_params(locals, drop=drop, **context)
        elif how == "request": return self.local_request(locals, drop=drop, **context)
        elif how == "response": return self.local_response(locals, drop=drop, **context)
        else: return context

    def local_args(self, locals: Dict=dict(), drop: _KT=list(), **context) -> Arguments:
        context = self._map_locals(locals, drop, **context)
        return kloc(context, self.iterateArgs, if_null="drop", values_only=True)

    def local_context(self, locals: Dict=dict(), drop: _KT=list(), **context) -> Context:
        context = self._map_locals(locals, drop, **context)
        return drop_dict(context, self.iterateArgs, inplace=False)

    def local_params(self, locals: Dict=dict(), drop: _KT=list(), **context) -> Tuple[Arguments,Context]:
        context = self._map_locals(locals, drop, **context)
        return self.local_args(**context), self.local_context(**context)

    def local_request(self, locals: Dict=dict(), drop: _KT=list(), **context) -> Context:
        context = self._map_locals(locals, drop, **context)
        keys = unique("index", *self.get_iterator(keys_only=True), *inspect.getfullargspec(REQUEST_CONTEXT)[0])
        return kloc(context, keys, if_null="drop")

    def local_response(self, locals: Dict=dict(), drop: _KT=list(), **context) -> Context:
        context = self._map_locals(locals, drop, **context)
        return RESPONSE_CONTEXT(**context)

    def _map_locals(self, locals: Dict=dict(), drop: _KT=list(), **context) -> Context:
        if locals:
            context = dict(dict(locals.pop("context", dict()), **locals), **context)
            context.pop("self", None)
        return drop_dict(context, drop, inplace=False) if drop else context

    ###################################################################
    ######################### Session Managers ########################
    ###################################################################

    def requests_task(func):
        @functools.wraps(func)
        def wrapper(self: Spider, *args, self_var=True, uploadInfo: Optional[UploadInfo]=dict(), **context):
            context = TASK_CONTEXT(**(dict(self.__dict__, **context) if self_var else context))
            self.checkpoint("context", where=func.__name__, msg={"context":context})
            data = func(self, *args, **context)
            self.checkpoint("crawl", where=func.__name__, msg={"data":data}, save=data)
            self._with_data(data, uploadInfo, **context)
            return data
        return wrapper

    def requests_session(func):
        @functools.wraps(func)
        def wrapper(self: Spider, *args, self_var=True, uploadInfo: Optional[UploadInfo]=dict(), **context):
            context = REQUEST_CONTEXT(**(dict(self.__dict__, **context) if self_var else context))
            self.checkpoint("context", where=func.__name__, msg={"context":context})
            with requests.Session() as session:
                data = func(self, *args, session=session, **context)
            time.sleep(.25)
            self.checkpoint("crawl", where=func.__name__, msg={"data":data}, save=data)
            self._with_data(data, uploadInfo, **context)
            return data
        return wrapper

    def _with_data(self, data: Data, uploadInfo: Optional[UploadInfo]=dict(), **context):
        if self.localSave: self.save_data(data, ext="dataframe")
        self.upload_data(data, uploadInfo, **context)

    def requests_limit(func):
        @functools.wraps(func)
        def wrapper(self: Spider, *args, **context):
            if self.delay: time.sleep(self.get_delay())
            return func(self, *args, **context)
        return wrapper

    def get_delay(self, tsUnit: Literal["ms","s"]="ms") -> Union[float,int]:
        if isinstance(self.delay, (float,int)): return self.delay
        elif isinstance(self.delay, Tuple):
            random.randrange(*self.delay[:2])/(1000 if tsUnit == "ms" else 1)
        else: return 0.

    def log_results(self, data: Data, **context):
        self.logger.info(log_data(data, **self.get_iterator(**context)))

    ###################################################################
    ####################### Parameter Validator #######################
    ###################################################################

    def validate_params(self, locals: Dict=dict(), how: Literal["min","max","first"]="min", default=None,
                        dropna=False, strict=False, unique=True, drop: _KT=list(), rename: Dict[_KT,Dict]=dict(),
                        **context) -> Tuple[Arguments,Context]:
        args, context = self.local_params(locals, **context)
        array_context = dict(default=default, dropna=dropna, strict=strict, unique=unique, rename=rename)
        args = self.validate_args(*args, how=how, **array_context, **context)
        context = self.validate_context(**array_context, drop=drop, **context)
        return args, context

    def validate_args(self, *args, how: Literal["min","max","first"]="min", default=None,
                    dropna=False, strict=False, unique=True, rename: Dict[_KT,Dict]=dict(), **context) -> Arguments:
        if not args: return args
        elif len(args) == 1: args = (to_array(args[0], default=default, dropna=dropna, strict=strict, unique=unique),)
        else: args = align_array(*args, how=how, default=default, dropna=dropna, strict=strict, unique=unique)
        rename_args = lambda __s, __key: rename_data(__s, rename=rename[__key]) if __key in rename else __s
        return tuple(rename_args(args[__i], __key) for __i, __key in enumerate(self.iterateArgs[:len(args)]))

    def validate_context(self, locals: Dict=dict(), default=None, dropna=False, strict=False, unique=True,
                        drop: _KT=list(), rename: Dict[_KT,Dict]=dict(), **context) -> Context:
        if locals: context = self.local_context(locals, **context, drop=drop)
        sequence_keys = self.inspect("crawl", "iterable").keys()
        dateformat_keys = self.inspect("crawl", annotation="DateFormat").keys()
        for __key in list(context.keys()):
            if __key in sequence_keys:
                context[__key] = to_array(context[__key], default=default, dropna=dropna, strict=strict, unique=unique)
            elif __key in dateformat_keys:
                context[__key] = self.get_date(context[__key], index=int(str(__key).lower().endswith("enddate")))
            if __key in rename:
                context[__key] = rename_data(context[__key], rename=rename[__key])
        return context

    ###################################################################
    ######################### Gather Requests #########################
    ###################################################################

    @abstractmethod
    @requests_session
    def crawl(self, *args, **context) -> Data:
        args, context = self.validate_params(locals())
        return self.gather(*args, **context)

    def gather(self, *args, count: Optional[Pagination]=None, progress=True,
                fields: IndexLabel=list(), returnType: Optional[TypeHint]=None, **context) -> Data:
        self.checkpoint("params", where="gather", msg=dict(zip(["args","context"], self.local_params(locals()))))
        if count: iterator, context = self._init_count(*args, count=count, progress=progress, **context)
        else: iterator, context = self._init_iterator(args, context, self.iterateArgs, self.iterateProduct, self.pagination)
        message = self.get_gather_message(*args, **context)
        data = [self.fetch(**__i, fields=fields, **context) for __i in tqdm(iterator, desc=message, disable=(not progress))]
        self.checkpoint("gather", where="gather", msg={"data":data}, save=data)
        return self.map_reduce(data, fields=fields, returnType=returnType, **context)

    def _init_iterator(self, args: Arguments, context: Context,
                    iterateArgs: List[_KT]=list(), iterateProduct: List[_KT]=list(),
                    pagination: Pagination=False, index=True, count=False) -> Tuple[List[Context],Context]:
        iterate_params = dict(iterateArgs=iterateArgs, iterateProduct=iterateProduct, pagination=pagination)
        iterator, context = self.set_iterator(*args, **iterate_params, index=index, **context)
        self.checkpoint("iterator"+("_count" if count else str()), where="_init_iterator", msg={"iterator":iterator})
        return iterator, context

    def _init_count(self, *args, count: Pagination, progress=True, **context) -> Tuple[List[Context],Context]:
        if not_na(context.get("size")): pass
        elif context.get("interval"):
            return self._gather_count_by_date(*args, progress=progress, **context)
        elif isinstance(count, str):
            size = self._gather_count(*args, progress=progress, **context)
            return self._init_iterator(args+(size,), context, self.iterateArgs+[count], self.iterateProduct, pagination=count)
        else: context["size"] = cast_int(self.fetch(*args, **self.get_count_context(**context)))
        return self._init_iterator(args, context, self.iterateArgs, self.iterateProduct, pagination=True)

    def _gather_count(self, *args, progress=True, **context) -> List[int]:
        count_context = self.get_count_context(**context)
        iterator, context = self._init_iterator(args, count_context, self.iterateArgs, index=False, count=True)
        message = self.get_count_message(*args, **context)
        size = [self.fetch(*__i, **context) for __i in tqdm(iterator, desc=message, disable=(not progress))]
        self.checkpoint("gather_count", where="gather_count", msg={"size":size})
        return list(map(cast_int, size))

    def _gather_count_by_date(self, *args, progress=True, size: _PASS=None, **context) -> Tuple[List[Context],Context]:
        size = self._gather_count(*args, progress=progress, **context)
        arguments, context = self._init_iterator(args, context, self.iterateArgs, index=False, count=True)
        args_pages, context = self._init_iterator(
            (arguments, size), context, ["args","size"], pagination="size", index=False, count=True)
        map_iterator = lambda args, **pages: chain_dict([args, pages], keep="first")
        ranges, context = self._from_context(iterateProduct=self.iterateProduct, **context)
        iterator = self._product_iterator([map_iterator(**__i) for __i in args_pages], ranges)
        self.checkpoint("iterator", where="gather_count_by_date", msg={"iterator":iterator})
        return iterator, context

    def get_gather_message(self, *args, **context) -> str:
        return GATHER_MSG(self.which, self.where, self.by)

    def get_count_context(self, **context) -> Context:
        return context

    def get_count_message(self, *args, which=str(), by=str(), **context) -> str:
        return GATHER_MSG(which, self.where, by)

    def map_reduce(self, data: List[Data], fields: IndexLabel=list(),
                    returnType: Optional[TypeHint]=None, **context) -> Data:
        return filter_data(chain_exists(data), fields=fields, if_null="pass", return_type=returnType)

    ###################################################################
    ########################## Fetch Request ##########################
    ###################################################################

    @abstractmethod
    @BaseSession.catch_exception
    @requests_limit
    def fetch(self, *args, **context) -> Data:
        ...

    def encode_messages(func):
        @functools.wraps(func)
        def wrapper(self: Spider, method: str, url: str, session: Optional[requests.Session]=None,
                    messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                    data=None, json=None, headers=None, cookies=str(), *args, index=0, **context):
            session = session if session else requests
            url, params = self.encode_params(url, params, encode=encode)
            if headers and cookies: headers["Cookie"] = str(cookies)
            messages = messages if messages else dict(params=params, data=data, json=json, headers=headers)
            suffix = f"_{index}" if index else str()
            self.checkpoint("request"+suffix, where=func.__name__, msg=dict(url=url, **exists_dict(messages)))
            self.logger.debug(log_messages(**messages, dump=self.logJson))
            response = func(self, method=method, url=url, session=session, messages=messages, **context)
            self.checkpoint("response"+suffix, where=func.__name__, msg={"response":response}, save=response, ext="response")
            return response
        return wrapper

    @encode_messages
    def request(self, method: str, url: str, session: requests.Session, messages: Dict=dict(),
                params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                valid: Optional[Status]=None, invalid: Optional[Status]=None, close=True, **context) -> requests.Response:
        response = session.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl)
        self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
        if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
        return response.close() if close else response

    @encode_messages
    def request_status(self, method: str, url: str, session: requests.Session, messages: Dict=dict(),
                        params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                        allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                        valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> int:
        with session.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.status_code

    @encode_messages
    def request_content(self, method: str, url: str, session: requests.Session, messages: Dict=dict(),
                        params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                        allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                        valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> bytes:
        with session.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.content

    @encode_messages
    def request_text(self, method: str, url: str, session: requests.Session, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                    allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                    valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> str:
        with session.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.text

    @encode_messages
    def request_json(self, method: str, url: str, session: requests.Session, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                    allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                    valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> JsonData:
        with session.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.json()

    @encode_messages
    def request_headers(self, method: str, url: str, session: requests.Session, messages: Dict=dict(),
                        params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                        allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                        valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> Dict:
        with session.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.headers

    @encode_messages
    def request_table(self, method: str, url: str, session: requests.Session, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                    allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                    valid: Optional[Status]=None, invalid: Optional[Status]=None, html=True, table_header=0, table_idx=0,
                    engine: Optional[Literal["xlrd","openpyxl","odf","pyxlsb"]]=None, **context) -> pd.DataFrame:
        with session.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            if html: return pd.read_html(response.text, header=table_header)[table_idx]
            else: return pd.read_excel(response.content, engine=engine)

    def encode_params(self, url: str, params: Optional[Dict]=None, encode: Optional[bool]=None) -> Tuple[str,Dict]:
        if not params: return url, None
        elif not isinstance(encode, bool): return url, params
        else: return encode_params(url, params, encode=encode), None

    def validate_status(self, response: requests.Response, how: Literal["error","interrupt"]="interrupt",
                        valid: Optional[Status]=None, invalid: Optional[Status]=None):
        status = response.status_code
        if (valid and (status not in cast_tuple(valid))) or (invalid and (status in cast_tuple(invalid))):
            if how == "interrupt": raise KeyboardInterrupt(INVALID_STATUS_MSG(self.where))
            else: raise requests.ConnectionError(INVALID_STATUS_MSG(self.where))

    ###################################################################
    ############################ Google API ###########################
    ###################################################################

    def gcloud_authorized(func):
        @functools.wraps(func)
        def wrapper(self: Spider, *args, redirectUrl=str(), authorization=str(), account: Account=dict(), **context):
            if not authorization:
                authorization = fetch_gcloud_authorization(redirectUrl, account)
            return func(
                self, *args, redirectUrl=redirectUrl, authorization=authorization, account=account, **context)
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
        data = read_gspread(key, sheet, account, fields=cast_tuple(fields), if_null="drop",
                            numericise_ignore=str_cols, return_type="dataframe", rename=self.get_rename_map(**context))
        self.logger.info(log_table(data, dump=self.logJson))
        for field in cast_tuple(fields):
            values = list(data[field]) if field in data else None
            if (len(values) == 1) and (field not in arr_cols): values = values[0]
            self.update(field=values)

    @BaseSession.catch_exception
    def upload_gspread(self, key: str, sheet: str, data: pd.DataFrame, mode: Literal["fail","replace","append"]="append",
                        base_sheet=str(), cell=str(), account: Account=dict(), clear: _PASS=None, **context):
        data = self.map_gs_data(data, **context)
        if base_sheet:
            data = self.map_gs_base(data, self.read_gs_base(key, base_sheet, account=account), **context)
        self.checkpoint(sheet, where="upload_gspread", msg={"key":key, "sheet":sheet, "mode":mode, "data":data}, save=data)
        self.logger.info(log_table(data, key=key, sheet=sheet, dump=self.logJson))
        cell, clear = ("A2" if mode == "replace" else (cell if cell else str())), (True if mode == "replace" else clear)
        update_gspread(key, sheet, data, account=account, cell=cell, clear=clear)

    def map_gs_data(self, data: pd.DataFrame, **context) -> pd.DataFrame:
        return data

    def read_gs_base(self, key: str, sheet: str, str_cols: NumericiseIgnore=list(),
                    account: Account=dict(), **context) -> pd.DataFrame:
        data = read_gspread(key, sheet, account=account, numericise_ignore=str_cols, rename=self.get_rename_map(**context))
        self.logger.info(log_table(data, key=key, sheet=sheet, dump=self.logJson))
        return data

    def map_gs_base(self, data: pd.DataFrame, base: pd.DataFrame, **context) -> pd.DataFrame:
        return cloc(data, base.columns, if_null="drop")

    ###################################################################
    ######################### Google Bigquery #########################
    ###################################################################

    @BaseSession.catch_exception
    def upload_gbq(self, table: str, project_id: str, data: pd.DataFrame,
                    mode: Literal["fail","replace","append","upsert"]="append",
                    schema: Optional[BigQuerySchema]=None, progress=True, partition=str(),
                    partition_by: Literal["auto","second","minute","hour","day","date"]="auto",
                    reauth=False, account: Account=dict(), credentials: Optional[IDTokenCredentials]=None, **context):
        schema = schema if schema and is_records(schema) else self.get_gbq_schema(mode=mode, schema=schema, **context)
        data = self.map_gbq_data(data, schema=schema, **context)
        context = dict(project_id=project_id, reauth=reauth, account=account, credentials=credentials)
        if mode == "upsert":
            data = self.map_gbq_base(data, self.read_gbq_base(query=table, **context), schema=schema, **context)
        self.checkpoint(table, where="upload_gbq", msg={"table":table, "pid":project_id, "mode":mode, "data":data}, save=data)
        self.logger.info(log_table(data, table=table, pid=project_id, mode=mode, schema=schema, dump=self.logJson))
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
    by = str()
    fields = list()
    iterateArgs = list()
    redirectArgs = None
    iterateProduct = list()
    redirectProduct = list()
    iterateUnit = 1
    redirectUnit = 1
    pagination = False
    pageUnit = 0
    pageLimit = 0
    interval = str()
    fromNow = None
    tzinfo = None
    datetimeUnit = "second"
    returnType = "records"
    renameMap = dict()
    ssl = None
    maxLimit = MAX_ASYNC_TASK_LIMIT
    redirectLimit = MAX_REDIRECT_LIMIT

    def __init__(self, fields: IndexLabel=list(), contextFields: IndexLabel=list(),
                iterateUnit: Unit=0, interval: Timedelta=str(), fromNow: Optional[Unit]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Literal["second","minute","hour","day"]="second",
                returnType: Optional[TypeHint]=None, logName=str(), logLevel: LogLevel="WARN", logFile=str(),
                debug: List[str]=list(), extraSave: List[str]=list(), interrupt=str(), localSave=False,
                renameMap: RenameMap=dict(), schemaInfo: SchemaInfo=dict(), delay: Union[float,int,Tuple[int]]=1.,
                progress=True, message=str(), cookies=str(), numTasks=100, apiRedirect=False, redirectUnit: Unit=0,
                queryInfo: GspreadReadInfo=dict(), **context):
        Spider.__init__(
            self, fields=fields, iterateUnit=iterateUnit, interval=interval, fromNow=fromNow, tzinfo=tzinfo,
            datetimeUnit=datetimeUnit, returnType=returnType, logName=logName, logLevel=logLevel, logFile=logFile,
            debug=debug, extraSave=extraSave, interrupt=interrupt, localSave=localSave, renameMap=renameMap,
            schemaInfo=schemaInfo, delay=delay, progress=progress, message=message, cookies=cookies)
        self.numTasks = cast_int(numTasks, default=MIN_ASYNC_TASK_LIMIT)
        self.apiRedirect = apiRedirect
        if not is_array(self.redirectArgs): self.redirectArgs = self.iterateArgs
        self.redirectUnit = exists_one(redirectUnit, self.redirectUnit, self.iterateUnit, strict=False)
        if context: self.update(kloc(UNIQUE_CONTEXT(**context), contextFields, if_null="pass"))
        self.set_query(queryInfo, **context)

    ###################################################################
    ########################## Async Managers #########################
    ###################################################################

    def asyncio_task(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSpider, *args, self_var=True, uploadInfo: Optional[UploadInfo]=dict(), **context):
            context = TASK_CONTEXT(**(dict(self.__dict__, **context) if self_var else context))
            self.checkpoint("context", where=func.__name__, msg={"context":context})
            semaphore = self.asyncio_semaphore(**context)
            data = await func(self, *args, semaphore=semaphore, **context)
            self.checkpoint("crawl", where=func.__name__, msg={"data":data}, save=data)
            self._with_data(data, uploadInfo, **context)
            return data
        return wrapper

    def asyncio_session(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSpider, *args, self_var=True, uploadInfo: Optional[UploadInfo]=dict(), **context):
            context = REQUEST_CONTEXT(**(dict(self.__dict__, **context) if self_var else context))
            self.checkpoint("context", where=func.__name__, msg={"context":context})
            semaphore = self.asyncio_semaphore(**context)
            async with aiohttp.ClientSession() as session:
                data = await func(self, *args, session=session, semaphore=semaphore, **context)
            await asyncio.sleep(.25)
            self.checkpoint("crawl", where=func.__name__, msg={"data":data}, save=data)
            self._with_data(data, uploadInfo, **context)
            return data
        return wrapper

    def asyncio_semaphore(self, numTasks: Optional[int]=None, apiRedirect=False, **context) -> asyncio.Semaphore:
        numTasks = min(self.numTasks, (self.redirectLimit if apiRedirect else self.maxLimit), self.maxLimit)
        return asyncio.Semaphore(numTasks)

    def catch_exception(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSpider, *args, **context):
            try: return await func(self, *args, **context)
            except KeyboardInterrupt as interrupt:
                raise interrupt
            except Exception as exception:
                if "exception" in self.debug:
                    self.checkpoint("exception", where=func.__name__, msg={"args":args, "context":context})
                    raise exception
                self.log_errors(*args, **context)
                name = f"{func.__name__}({self.__class__.__name__})"
                self.logger.error(log_exception(name, *args, json=self.logJson, **REQUEST_CONTEXT(**context)))
                return init_origin(func)
        return wrapper

    def asyncio_redirect(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSpider, *args, apiRedirect=False, **context):
            if apiRedirect: return await self.redirect(self, *args, **context)
            else: return await func(self, *args, **context)
        return wrapper

    def asyncio_limit(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSpider, *args, semaphore: Optional[asyncio.Semaphore]=None, **context):
            if self.delay:
                await asyncio.sleep(self.delay)
            if semaphore:
                async with semaphore:
                    return await func(self, *args, **context)
            return await func(self, *args, **context)
        return wrapper

    ###################################################################
    ########################### Async Gather ##########################
    ###################################################################

    @abstractmethod
    @asyncio_session
    async def crawl(self, *args, **context) -> Data:
        args, context = self.validate_params(locals())
        return await self.gather(*args, **context)

    async def gather(self, *args, count: Optional[Pagination]=None, progress=True,
                    fields: IndexLabel=list(), returnType: Optional[TypeHint]=None, **context) -> Data:
        self.checkpoint("params", where="gather", msg=dict(zip(["args","context"], self.local_params(locals()))))
        if count: iterator, context = await self._init_count(*args, count=count, progress=progress, **context)
        else: iterator, context = self._init_iterator(args, context, self.iterateArgs, self.iterateProduct, self.pagination)
        message = self.get_gather_message(*args, **context)
        data = await tqdm.gather(*[
                self.fetch(**__i, fields=fields, **context) for __i in iterator], desc=message, disable=(not progress))
        self.checkpoint("gather", where="gather", msg={"data":data}, save=data)
        return self.map_reduce(data, fields=fields, returnType=returnType, **context)

    async def _init_count(self, *args, count: Pagination, progress=True,
                        **context) -> Tuple[List[Context],Context]:
        if not_na(context.get("size")): pass
        elif context.get("interval"):
            return await self._gather_count_by_date(*args, progress=progress, **context)
        elif isinstance(count, str):
            size = await self._gather_count(*args, progress=progress, **context)
            return self._init_iterator(args+(size,), context, self.iterateArgs+[count], self.iterateProduct, pagination=count)
        else: context["size"] = cast_int(await self.fetch(*args, **self.get_count_context(**context)))
        return self._init_iterator(args, context, self.iterateArgs, self.iterateProduct, pagination=True)

    async def _gather_count(self, *args, progress=True, **context) -> List[int]:
        count_context = self.get_count_context(**context)
        iterator, context = self._init_iterator(args, count_context, self.iterateArgs, index=False, count=True)
        message = self.get_count_message(*args, **context)
        size = await tqdm.gather(*[
                self.fetch(**__i, **context) for __i in iterator], desc=message, disable=(not progress))
        self.checkpoint("gather_count", where="gather_count", msg={"size":size})
        return list(map(cast_int, size))

    async def _gather_count_by_date(self, *args, progress=True, size: _PASS=None, **context) -> Tuple[List[Context],Context]:
        size = await self._gather_count(*args, progress=progress, **context)
        arguments, context = self._init_iterator(args, context, self.iterateArgs, index=False, count=True)
        args_pages, context = self._init_iterator(
            (arguments, size), context, ["args","size"], pagination="size", index=False, count=True)
        map_iterator = lambda args, **pages: chain_dict([args, pages], keep="first")
        ranges, context = self._from_context(iterateProduct=self.iterateProduct, **context)
        iterator = self._product_iterator([map_iterator(**__i) for __i in args_pages], ranges)
        self.checkpoint("iterator", where="gather_count_by_date", msg={"iterator":iterator})
        return iterator, context


    ###################################################################
    ########################## Async Requests #########################
    ###################################################################

    @abstractmethod
    @catch_exception
    @asyncio_limit
    async def fetch(self, *args, **context) -> Data:
        ...

    def encode_messages(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSpider, method: str, url: str, session: Optional[aiohttp.ClientSession]=None,
                        messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                        data=None, json=None, headers=None, cookies=str(), *args, index=0, **context):
            session = session if session else aiohttp
            url, params = self.encode_params(url, params, encode=encode)
            if headers and cookies: headers["Cookie"] = str(cookies)
            messages = messages if messages else dict(params=params, data=data, json=json, headers=headers)
            suffix = f"_{index}" if index else str()
            self.checkpoint("request"+suffix, where=func.__name__, msg=dict(url=url, **exists_dict(messages)))
            self.logger.debug(log_messages(**messages, dump=self.logJson))
            response = await func(self, method=method, url=url, session=session, messages=messages, **context)
            self.checkpoint("response"+suffix, where=func.__name__, msg={"response":response}, save=response, ext="response")
            return response
        return wrapper

    @encode_messages
    async def request(self, method: str, url: str, session: aiohttp.ClientSession, messages: Dict=dict(),
                            params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                            allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                            valid: Optional[Status]=None, invalid: Optional[Status]=None, **context):
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, ssl=self.ssl) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)

    @encode_messages
    async def request_status(self, method: str, url: str, session: aiohttp.ClientSession, messages: Dict=dict(),
                            params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                            allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                            valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> int:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, ssl=self.ssl) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.status

    @encode_messages
    async def request_content(self, method: str, url: str, session: aiohttp.ClientSession, messages: Dict=dict(),
                            params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                            allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt", 
                            valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> bytes:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, ssl=self.ssl) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return await response.read()

    @encode_messages
    async def request_text(self, method: str, url: str, session: aiohttp.ClientSession, messages: Dict=dict(),
                            params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                            allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                            valid: Optional[Status]=None, invalid: Optional[Status]=None, encoding=None, **context) -> str:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, ssl=self.ssl) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return await response.text(encoding=encoding)

    @encode_messages
    async def request_json(self, method: str, url: str, session: aiohttp.ClientSession, messages: Dict=dict(),
                            params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                            allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                            valid: Optional[Status]=None, invalid: Optional[Status]=None, encoding=None, **context) -> JsonData:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, ssl=self.ssl) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return await response.json(encoding=encoding)

    @encode_messages
    async def request_headers(self, method: str, url: str, session: aiohttp.ClientSession, messages: Dict=dict(),
                            params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                            allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                            valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> Dict:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, ssl=self.ssl) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.headers

    @encode_messages
    async def request_table(self, method: str, url: str, session: aiohttp.ClientSession, messages: Dict=dict(),
                            params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                            allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                            valid: Optional[Status]=None, invalid: Optional[Status]=None, html=False, table_header=0, table_idx=0,
                            engine: Optional[Literal["xlrd","openpyxl","odf","pyxlsb"]]=None, **context) -> pd.DataFrame:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, ssl=self.ssl) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            if html: return pd.read_html(await response.text(), header=table_header)[table_idx]
            else: return pd.read_excel(await response.read(), engine=engine)

    def validate_status(self, response: aiohttp.ClientResponse, how: Literal["error","interrupt"]="interrupt",
                        valid: Optional[Status]=None, invalid: Optional[Status]=None):
        status = response.status
        if (valid and (status not in cast_tuple(valid))) or (invalid and (status in cast_tuple(invalid))):
            if how == "interrupt": raise KeyboardInterrupt(INVALID_STATUS_MSG(self.where))
            else: raise aiohttp.ServerConnectionError(INVALID_STATUS_MSG(self.where))

    ###################################################################
    ########################## Async Redirect #########################
    ###################################################################

    def gcloud_authorized(func):
        @functools.wraps(func)
        async def wrapper(self: Spider, *args, redirectUrl=str(), authorization=str(), account: Account=dict(), **context):
            if not authorization:
                authorization = fetch_gcloud_authorization(redirectUrl, account)
            return await func(
                self, *args, redirectUrl=redirectUrl, authorization=authorization, account=account, **context)
        return wrapper

    @gcloud_authorized
    async def redirect(self, *args, message=str(), redirectUnit: Unit=1, progress=True,
                        fields: IndexLabel=list(), returnType: Optional[TypeHint]=None, **context) -> Data:
        self.checkpoint("params", where="gather", msg=dict(zip(["args","context"], self.local_params(locals()))))
        context["iterateUnit"] = redirectUnit
        iterator, context = self._init_iterator(args, context, self.redirectArgs, self.redirectProduct, self.pagination)
        message = self.get_redirect_message(*args, **context)
        data = await tqdm.gather(*[
                self.fetch_redirect(**__i, fields=fields, **context) for __i in iterator], desc=message, disable=(not progress))
        self.checkpoint("gather", where="gather", msg={"data":data}, save=data)
        return self.map_reduce(data, fields=fields, returnType=returnType, **context)

    def get_redirect_message(self, *args, **context) -> str:
        return REDIRECT_MSG(self.operation)

    @catch_exception
    @asyncio_limit
    @gcloud_authorized
    async def fetch_redirect(self, redirectUrl: str, authorization: str, session: Optional[aiohttp.ClientSession]=None,
                            account: Account=dict(), **context) -> Records:
        data = self._filter_redirect_data(redirectUrl, authorization, account, **context)
        response = self.request_text(POST, redirectUrl, session, json=data, headers=dict(Authorization=authorization))
        self.checkpoint("redirect", where="fetch_redirect", msg={"response":response}, save=response, ext="json")
        return self._parse_redirect(json.loads(response), **context)

    def _filter_redirect_data(self, redirectUrl: str, authorization: str, account: Account=dict(), **context) -> Context:
        return dict(
            redirectUrl=redirectUrl,
            authorization=authorization,
            account=account,
            cookies=self.cookies,
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
    ssl = None

    @abstractmethod
    def __init__(self, logName=str(), logLevel: LogLevel="WARN", logFile=str(),
                debug: List[str]=list(), extraSave: List[str]=list(), interrupt=str(), cookies=str(), **context):
        requests.Session.__init__(self)
        if cookies: self.cookies.update(decode_cookies(cookies))
        self.set_logger(logName, logLevel, logFile, debug, extraSave, interrupt)
        self.disable_warnings()

    @abstractmethod
    def login(self):
        ...

    def get_cookies(self, returnType: Literal["str","dict"]="str") -> Union[str,Dict]:
        if returnType == "str": return parse_cookies(self.cookies)
        elif returnType == "dict": return dict(self.cookies.items())
        else: return self.cookies

    def update_cookies(self, cookies: Union[str,Dict]=str(), if_exists: Literal["ignore","replace"]="ignore", **context):
        cookies = decode_cookies(cookies, **context) if isinstance(cookies, str) else dict(cookies, **context)
        for __key, __value in cookies.items():
            if (if_exists == "replace") or (__key not in self.cookies):
                self.cookies.set(__key, __value)

    ###################################################################
    ########################## Fetch Request ##########################
    ###################################################################

    def encode_messages(func):
        @functools.wraps(func)
        def wrapper(self: LoginSpider, method: str, url: str, origin: str,
                    messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                    data=None, json=None, headers=None, cookies=str(), *args, **context):
            url, params = self.encode_params(url, params, encode=encode)
            if headers and cookies: headers["Cookie"] = str(cookies)
            messages = messages if messages else dict(params=params, data=data, json=json, headers=headers)
            self.checkpoint(origin+"_request", where=func.__name__, msg=dict(url=url, **exists_dict(messages)))
            self.logger.debug(log_messages(**messages, dump=self.logJson))
            response = func(self, method=method, url=url, origin=origin, messages=messages, **context)
            self.checkpoint(origin+"_response", where=func.__name__, msg={"response":response}, save=response, ext="response")
            return response
        return wrapper

    @encode_messages
    def request_url(self, method: str, url: str, origin: str, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None,
                    headers=None, cookies=str(), allow_redirects=True, **context):
        with self.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, origin=origin))
            self.logger.debug(log_messages(cookies=dict(self.cookies), origin=origin, dump=self.logJson))
            self.log_response_text(response, origin)

    @encode_messages
    def request_status(self, method: str, url: str, origin: str, messages: Dict=dict(),
                        params=None, encode: Optional[bool]=None, data=None, json=None,
                        headers=None, cookies=str(), allow_redirects=True, **context) -> int:
        with self.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, origin=origin))
            self.logger.debug(log_messages(cookies=dict(self.cookies), origin=origin, dump=self.logJson))
            self.log_response_text(response, origin)
            return response.status_code

    @encode_messages
    def request_content(self, method: str, url: str, origin: str, messages: Dict=dict(),
                        params=None, encode: Optional[bool]=None, data=None, json=None,
                        headers=None, cookies=str(), allow_redirects=True, **context) -> bytes:
        with self.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, origin=origin))
            self.logger.debug(log_messages(cookies=dict(self.cookies), origin=origin, dump=self.logJson))
            self.log_response_text(response, origin)
            return response.content

    @encode_messages
    def request_text(self, method: str, url: str, origin: str, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None,
                    headers=None, cookies=str(), allow_redirects=True, **context) -> str:
        with self.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, origin=origin))
            self.logger.debug(log_messages(cookies=dict(self.cookies), origin=origin, dump=self.logJson))
            return response.text

    @encode_messages
    def request_json(self, method: str, url: str, origin: str, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None,
                    headers=None, cookies=str(), allow_redirects=True, **context) -> JsonData:
        with self.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, origin=origin))
            self.logger.debug(log_messages(cookies=dict(self.cookies), origin=origin, dump=self.logJson))
            self.log_response_text(response, origin)
            return response.json()

    @encode_messages
    def request_headers(self, method: str, url: str, origin: str, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None,
                    headers=None, cookies=str(), allow_redirects=True, **context) -> Dict:
        with self.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, origin=origin))
            self.logger.debug(log_messages(cookies=dict(self.cookies), origin=origin, dump=self.logJson))
            self.log_response_text(response, origin)
            return response.headers

    def log_response_text(self, response: requests.Response, origin: str):
        self.checkpoint(origin+"_text", where=origin, msg={"response":response.text}, save=response, ext="response")


class BaseLogin(LoginSpider):
    operation = "login"

    def __init__(self, cookies=str()):
        super().__init__(cookies=cookies)

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
    by = str()
    fields = list()
    iterateArgs = list()
    iterateProduct = list()
    iterateUnit = 1
    pagination = False
    pageUnit = 0
    pageLimit = 0
    interval = str()
    fromNow = None
    tzinfo = None
    datetimeUnit = "second"
    returnType = "records"
    renameMap = dict()
    ssl = None
    auth = LoginSpider
    decryptedKey = dict()

    def __init__(self, fields: IndexLabel=list(), contextFields: IndexLabel=list(),
                iterateUnit: Optional[Unit]=0, interval: Optional[Timedelta]=str(), fromNow: Optional[Unit]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Literal["second","minute","hour","day"]="second",
                returnType: Optional[TypeHint]=None, logName=str(), logLevel: LogLevel="WARN", logFile=str(),
                debug: List[str]=list(), extraSave: List[str]=list(), interrupt=str(), localSave=False,
                renameMap: RenameMap=dict(), schemaInfo: SchemaInfo=dict(), delay: Union[float,int,Tuple[int]]=1.,
                progress=True, message=str(), cookies=str(), queryInfo: GspreadReadInfo=dict(),
                encryptedKey: EncryptedKey=str(), decryptedKey: Union[str,Dict]=str(), **context):
        Spider.__init__(
            self, fields=fields, iterateUnit=iterateUnit, interval=interval, fromNow=fromNow, tzinfo=tzinfo,
            datetimeUnit=datetimeUnit, returnType=returnType, logName=logName, logLevel=logLevel, logFile=logFile,
            debug=debug, extraSave=extraSave, interrupt=interrupt, localSave=localSave, renameMap=renameMap,
            schemaInfo=schemaInfo, delay=delay, progress=progress, message=message, cookies=cookies,
            contextFields=contextFields, queryInfo=queryInfo, **context)
        if not self.cookies:
            self.set_secrets(encryptedKey, decryptedKey)

    def set_secrets(self, encryptedKey=str(), decryptedKey=str()):
        if not self.decryptedKey and not isinstance(decryptedKey, Dict):
            try: decryptedKey = json.loads(decryptedKey if decryptedKey else decrypt(encryptedKey,1))
            except JSONDecodeError: raise ValueError(INVALID_USER_INFO_MSG(self.where))
        self.decryptedKey = decryptedKey if isinstance(decryptedKey, Dict) else self.decryptedKey
        self.logger.info(log_encrypt(**self.decryptedKey, show=3))

    ###################################################################
    ########################## Login Session ##########################
    ###################################################################

    def login_session(func):
        @functools.wraps(func)
        def wrapper(self: EncryptedSpider, *args, self_var=True, uploadInfo: Optional[UploadInfo]=dict(), **context):
            context = LOGIN_CONTEXT(**(dict(self.__dict__, **context) if self_var else context))
            self.checkpoint("context", where=func.__name__, msg={"context":context})
            with (BaseLogin(self.cookies) if self.cookies else self.auth(**dict(context, **self.decryptedKey))) as session:
                self.login(session, **context)
                data = func(self, *args, session=session, **context)
            time.sleep(.25)
            self.checkpoint("crawl", where=func.__name__, msg={"data":data}, save=data)
            self._with_data(data, uploadInfo, **context)
            return data
        return wrapper

    def login(self, auth: LoginSpider, **context):
        auth.login()
        auth.update_cookies(self.set_cookies(**context), if_exists="replace")
        self.checkpoint("login", where="login", msg={"cookies":dict(auth.cookies)})
        self.cookies = auth.get_cookies()

    def set_cookies(self, **context) -> Dict:
        return dict()

    ###################################################################
    ########################### API Session ###########################
    ###################################################################

    def api_session(func):
        @functools.wraps(func)
        def wrapper(self: EncryptedSpider, *args, self_var=True, uploadInfo: Optional[UploadInfo]=dict(), **context):
            context = API_CONTEXT(**(dict(self.__dict__, **context) if self_var else context))
            self.checkpoint("context", where=func.__name__, msg={"context":context})
            with requests.Session() as session:
                data = func(self, *args, session=session, **self.validate_secret(**dict(context, **self.decryptedKey)))
            time.sleep(.25)
            self.checkpoint("crawl", where=func.__name__, msg={"data":data}, save=data)
            self._with_data(data, uploadInfo, **context)
            return data
        return wrapper

    def validate_secret(self, clientId: str, clientSecret: str, **context) -> Context:
        headers = self.set_headers(clientId=clientId, clientSecret=clientSecret, **context)
        self.checkpoint("api", where="set_headers", msg={"headers":headers})
        if headers: context["headers"] = headers
        return context

    def set_headers(self, **context) -> Dict:
        return dict()


class EncryptedAsyncSpider(AsyncSpider, EncryptedSpider):
    __metaclass__ = ABCMeta
    asyncio = True
    operation = "spider"
    where = WHERE
    which = WHICH
    by = str()
    fields = list()
    iterateArgs = list()
    redirectArgs = None
    iterateProduct = list()
    redirectProduct = None
    iterateUnit = 1
    redirectUnit = 1
    pagination = False
    pageUnit = 0
    pageLimit = 0
    interval = str()
    fromNow = None
    tzinfo = None
    datetimeUnit = "second"
    returnType = "records"
    rename = dict()
    ssl = None
    maxLimit = MAX_ASYNC_TASK_LIMIT
    redirectLimit = MAX_REDIRECT_LIMIT
    auth = LoginSpider
    decryptedKey = dict()

    def __init__(self, fields: IndexLabel=list(), contextFields: IndexLabel=list(),
                iterateUnit: Optional[Unit]=0, interval: Optional[Timedelta]=str(), fromNow: Optional[Unit]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Literal["second","minute","hour","day"]="second",
                returnType: Optional[TypeHint]=None, logName=str(), logLevel: LogLevel="WARN", logFile=str(),
                debug: List[str]=list(), extraSave: List[str]=list(), interrupt=str(), localSave=False,
                renameMap: RenameMap=dict(), schemaInfo: SchemaInfo=dict(), delay: Union[float,int,Tuple[int]]=1.,
                progress=True, message=str(), cookies=str(), numTasks=100, queryInfo: GspreadReadInfo=dict(),
                apiRedirect=False, redirectUnit: Unit=0, encryptedKey: EncryptedKey=str(), decryptedKey=str(), **context):
        AsyncSpider.__init__(
            self, fields=fields, iterateUnit=iterateUnit, interval=interval, fromNow=fromNow, tzinfo=tzinfo,
            datetimeUnit=datetimeUnit, returnType=returnType, logName=logName, logLevel=logLevel, logFile=logFile,
            debug=debug, extraSave=extraSave, interrupt=interrupt, localSave=localSave, renameMap=renameMap,
            schemaInfo=schemaInfo, delay=delay, progress=progress, message=message, cookies=cookies,
            contextFields=contextFields, queryInfo=queryInfo,
            numTasks=numTasks, apiRedirect=apiRedirect, redirectUnit=redirectUnit, **context)
        if not self.cookies:
            self.set_secrets(encryptedKey, decryptedKey)

    def login_session(func):
        @functools.wraps(func)
        async def wrapper(self: EncryptedAsyncSpider, *args, self_var=True, uploadInfo: Optional[UploadInfo]=dict(), **context):
            context = LOGIN_CONTEXT(**(dict(self.__dict__, **context) if self_var else context))
            self.checkpoint("context", where=func.__name__, msg={"context":context})
            semaphore = self.asyncio_semaphore(**context)
            ssl = dict(connector=aiohttp.TCPConnector(ssl=False)) if self.ssl == False else dict()
            with (BaseLogin(self.cookies) if self.cookies else self.auth(**dict(context, **self.decryptedKey))) as auth:
                self.login(auth, **context)
                async with aiohttp.ClientSession(**ssl, cookies=dict(auth.cookies)) as session:
                    data = await func(self, *args, session=session, semaphore=semaphore, **context)
            await asyncio.sleep(.25)
            self.checkpoint("crawl", where=func.__name__, msg={"data":data}, save=data)
            self._with_data(data, uploadInfo, **context)
            return data
        return wrapper

    def api_session(func):
        @functools.wraps(func)
        async def wrapper(self: EncryptedSpider, *args, self_var=True, uploadInfo: Optional[UploadInfo]=dict(), **context):
            context = API_CONTEXT(**(dict(self.__dict__, **context) if self_var else context))
            self.checkpoint("context", where=func.__name__, msg={"context":context})
            ssl = dict(connector=aiohttp.TCPConnector(ssl=False)) if self.ssl == False else dict()
            async with aiohttp.ClientSession(**ssl) as session:
                data = await func(self, *args, session=session, **self.validate_secret(**dict(context, **self.decryptedKey)))
            await asyncio.sleep(.25)
            self.checkpoint("crawl", where=func.__name__, msg={"data":data}, save=data)
            self._with_data(data, uploadInfo, **context)
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
                debug: List[str]=list(), root: _KT=list(), renameMap: RenameMap=dict(), schemaInfo: SchemaInfo=dict(), **context):
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
                    fields: IndexLabel=list(), updateTime=True, **context) -> Data:
        data = parse_schema(data, schemaInfo, self.responseType, root, self.get_rename_map(**context), discard, **context)
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
    iterateUnit = 1
    tzinfo = None
    datetimeUnit = "second"
    returnType = "dataframe"
    renameMap = dict()

    @abstractmethod
    @Spider.requests_task
    def crawl(self, *args, **context) -> Data:
        args, context = self.validate_params(locals())
        return self.gather(*args, **context)

    @abstractmethod
    def gather(self, **context) -> Data:
        args = list()
        for cralwer in self.dependencies:
            data = self.crawl_proxy(cralwer, **context)
            args.append(data)
        return self.map_reduce(*args, **context)

    def crawl_proxy(self, init: Type[Spider], returnType: Optional[TypeHint]=None, **context) -> Data:
        crawler = init(**PROXY_CONTEXT(**context), returnType=returnType)
        data = crawler.crawl()
        self.checkpoint(crawler.operation, where=crawler.operation, msg={"data":data}, save=data)
        return data

    def map_reduce(self, *data: Data, fields: IndexLabel=list(), **context) -> Data:
        return filter_data(chain_exists(data), fields=fields, if_null="pass")


class AsyncPipeline(AsyncSpider, Pipeline):
    __metaclass__ = ABCMeta
    operation = "pipeline"
    dependencies = (AsyncSpider, AsyncSpider)
    fields = list()
    iterateUnit = 1
    tzinfo = None
    datetimeUnit = "second"
    returnType = "dataframe"
    renameMap = dict()

    @abstractmethod
    @AsyncSpider.asyncio_task
    async def crawl(self, *args, **context) -> Data:
        args, context = self.validate_params(locals())
        return await self.gather(*args, **context)

    @abstractmethod
    async def gather(self, **context) -> Data:
        args = list()
        for cralwer in self.dependencies:
            if cralwer.asyncio: data = await self.async_proxy(cralwer, **context)
            else: data = self.crawl_proxy(cralwer, **context)
            args.append(data)
        return self.map_reduce(*args, **context)

    async def async_proxy(self, init: Type[AsyncSpider], returnType: Optional[TypeHint]=None, **context) -> Data:
        crawler = init(**PROXY_CONTEXT(**context), returnType=returnType)
        data = await crawler.crawl()
        self.checkpoint(crawler.operation, where=crawler.operation, msg={"data":data}, save=data)
        return data
