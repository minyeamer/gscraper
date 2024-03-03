from __future__ import annotations
from gscraper.base.abstract import CustomDict, OptionalDict, TypedRecords, Query, INVALID_OBJECT_TYPE_MSG
from gscraper.base.abstract import UNIQUE_CONTEXT, TASK_CONTEXT, REQUEST_CONTEXT, RESPONSE_CONTEXT
from gscraper.base.abstract import SESSION_CONTEXT, PROXY_CONTEXT, REDIRECT_CONTEXT

from gscraper.base.session import BaseSession, Iterator, Parser, Info, Schema, Field, Flow, Process
from gscraper.base.session import ITER_INDEX, ITER_SUFFIX, ITER_MSG, PAGE_ITERATOR
from gscraper.base.gcloud import GoogleQueryReader, GoogleUploader, GoogleQueryList, GoogleUploadList
from gscraper.base.gcloud import fetch_gcloud_authorization, read_gcloud

from gscraper.base.types import _KT, _PASS, Arguments, Context, LogLevel, TypeHint, EncryptedKey, DecryptedKey
from gscraper.base.types import IndexLabel, Keyword, Pagination, Status, Unit, Range, DateFormat, Timedelta, Timezone
from gscraper.base.types import Records, Data, MappedData, JsonData, RedirectData, Account
from gscraper.base.types import is_datetime_type, is_date_type, is_array, is_int_array, init_origin

from gscraper.utils import notna
from gscraper.utils.cast import cast_list, cast_tuple, cast_int, cast_datetime_format
from gscraper.utils.date import get_date_pair, get_datetime_pair
from gscraper.utils.logs import log_encrypt, log_messages, log_response, log_client, log_data
from gscraper.utils.map import to_array, align_array, transpose_array, unit_array, get_scala, union, inter, diff
from gscraper.utils.map import kloc, exists_dict, drop_dict, split_dict
from gscraper.utils.map import vloc, apply_records, to_dataframe, convert_data, rename_data, filter_data
from gscraper.utils.map import exists_one, unique, chain_exists, between_data, re_get
from gscraper.utils.map import convert_dtypes as _convert_dtypes

from abc import ABCMeta, abstractmethod
from http.cookies import SimpleCookie
from requests.cookies import RequestsCookieJar
from urllib.parse import quote, urlencode, urlparse
import asyncio
import aiohttp
import copy
import functools
import inspect
import requests
import time

from typing import Callable, Dict, Iterable, List, Literal, Optional, Sequence, Tuple, Union
from numbers import Real
from ast import literal_eval
from bs4 import BeautifulSoup, Tag
from json import JSONDecodeError
import datetime as dt
import json
import pandas as pd

from tqdm.auto import tqdm
import base64
import inspect
import re


GET = "GET"
POST = "POST"
OPTIONS = "OPTIONS"
HEAD = "HEAD"
PUT = "PUT"
PATCH = "PATCH"
DELETE = "DELETE"
API = "API"

MIN_ASYNC_TASK_LIMIT = 1
MAX_ASYNC_TASK_LIMIT = 100
MAX_REDIRECT_LIMIT = 10

TIME_UNIQUE = ["tzinfo", "datetimeUnit"]
LOG_UNIQUE = ["logName", "logLevel", "logFile", "localSave", "debug", "extraSave", "interrupt"]

ITERATOR_UNIQUE = ["iterateUnit", "interval", "fromNow"]
GCLOUD_UNIQUE = ["queryList", "uploadList", "account"]

FILTER_UNIQUE = ["fields", "ranges", "returnType"]
REQUEST_UNIQUE = ["numRetries", "delay", "cookies"]

SPIDER_UNIQUE = ["discard", "progress", "numTasks"]
REDIRECT_UNIQUE = ["apiRedirect", "redirectUnit", "redirectUrl", "authorization", "account"]

ENCRYPTED_UNIQUE = ["encryptedKey", "decryptedKey"]

PROXY_LOG = ["logLevel", "logFile", "debug", "extraSave", "interrupt"]
PIPELINE_UNIQUE = FILTER_UNIQUE + TIME_UNIQUE + PROXY_LOG + REQUEST_UNIQUE + ENCRYPTED_UNIQUE
WORKER_UNIQUE = PIPELINE_UNIQUE + SPIDER_UNIQUE + REDIRECT_UNIQUE
WORKER_EXTRA = ["message", "where", "which", "by"]

NAME, OPERATOR, TASK, DATANAME, DATATYPE = "name", "operator", "task", "dataName", "dataType"
FIELDS, ALLOWED, PARAMS, DERIV, CONTEXT = "fields", "allowedFields", "params", "derivData", "context"

SUCCESS_RESULT = [{"response": "completed"}]


###################################################################
############################# Messages ############################
###################################################################

INVALID_VALUE_MSG = lambda name, value: f"'{value}' value is not valid {name}."

GATHER_MSG = lambda which, where=str(), by=str(): \
    f"Collecting {which}{(' '+by) if by else str()}{(' from '+where) if where else str()}"
INVALID_STATUS_MSG = lambda where: f"Response from {where} is not valid."

REDIRECT_MSG = lambda operation: f"{operation} operation is redirecting"
INVALID_REDIRECT_RESPONSE_MSG = "Please verify the redirect response."

INVALID_USER_INFO_MSG = lambda where=str(): f"{where} user information is not valid.".strip()
INVALID_API_INFO_MSG = lambda where=str(): f"{re.sub(' API$','',where)} API information is not valid.".strip()

DEPENDENCY_HAS_NO_NAME_MSG = "Dependency has no operation name. Please define operation name."

WHERE, WHICH = "urls", "data"
FIRST_PAGE, NEXT_PAGE = "of first page", "of next pages"

AUTH_KEY = "Auth Key"
SAVE_SHEET = "Data"


###################################################################
############################# Headers #############################
###################################################################

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


def get_content_type(content_type=str(), urlencoded=False, utf8=False) -> str:
    if urlencoded or content_type == "urlencoded":
        __type = "application/x-www-form-urlencoded"
    elif content_type == "json": __type = "application/json"
    elif content_type == "text": __type = "text/plain"
    else: return str()
    return __type+(("; charset=UTF-8") if utf8 else str())


def get_headers(authority=str(), referer=str(), cookies=str(), host=str(),
                origin: Union[bool,str]=False, secure=False,
                content_type=str(), urlencoded=False, utf8=False, xml=False, **kwargs) -> Dict[str,str]:
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
############################### Json ##############################
###################################################################

class LazyDecoder(json.JSONDecoder):
    def decode(s, **kwargs):
        regex_replacements = [
            (re.compile(r'([^\\])\\([^\\])'), r'\1\\\\\2'),
            (re.compile(r',(\s*])'), r'\1'),
        ]
        for regex, replacement in regex_replacements:
            s = regex.sub(replacement, s)
        return super().decode(s, **kwargs)


def validate_json(data: JsonData, __path: IndexLabel, default=dict()) -> JsonData:
    __m = data.copy()
    try:
        for key in __path:
            __m = __m[key]
            if isinstance(__m, str):
                try: __m = json.loads(__m)
                except json.JSONDecodeError: return json.loads(__m, cls=LazyDecoder)
        return __m
    except: return default


def parse_invalid_json(raw_json: str, key: str, value_type: Literal["any","dict"]="dict") -> JsonData:
    rep_bool = lambda s: str(s).replace("null","None").replace("true","True").replace("false","False")
    try:
        if value_type == "dict" and re.search("\""+key+"\":\{[^\}]*\}+",raw_json):
            return literal_eval(rep_bool("{"+re.search("\""+key+"\":\{[^\}]*\}+",raw_json).group()+"}"))
        elif value_type == "any" and re.search(f"(?<=\"{key}\":)"+"([^,}])+(?=[,}])",raw_json):
            return literal_eval(rep_bool(re.search(f"(?<=\"{key}\":)"+"([^,}])+(?=[,}])",raw_json).group()))
        else: return
    except: return dict() if value_type == "dict" else None


###################################################################
############################## Urllib #############################
###################################################################

def encrypt(s: str, count=1) -> str:
    return encrypt(base64.b64encode(str(s).encode("utf-8")).decode("utf-8"), count-1) if count else s


def decrypt(s: str, count=1) -> str:
    return decrypt(base64.b64decode(str(s).encode("utf-8")).decode("utf-8"), count-1) if count else s


def get_cookies(session, encode=True, raw=False, url=None) -> Union[str,Dict,RequestsCookieJar,SimpleCookie]:
    if isinstance(session, aiohttp.ClientSession):
        cookies = session.cookie_jar.filter_cookies(url)
    elif isinstance(session, requests.Session):
        cookies = session.cookies
    else: return str() if encode else dict()
    if raw: return cookies
    elif encode: return parse_cookies(cookies)
    else: return {str(key): str(value) for key, value in cookies.items()}


def parse_parth(url: str) -> str:
    return re.sub(urlparse(url).path+'$','',url)


def parse_origin(url: str) -> str:
    return re_get(f"(.*)(?={urlparse(url).path})", url) if urlparse(url).path else url


def parse_cookies(cookies: Union[RequestsCookieJar,SimpleCookie]) -> str:
    if isinstance(cookies, str): return cookies
    else: return '; '.join([str(key)+"="+str(value) for key, value in cookies.items()])


def encode_cookies(cookies: Union[str,Dict], *args, **kwargs) -> str:
    cookies = parse_cookies(cookies)
    if args: cookies = '; '.join([cookies]+[parse_cookies(arg) for arg in args])
    if kwargs: cookies = '; '.join([cookies, parse_cookies(kwargs)])
    return cookies


def decode_cookies(cookies: Union[str,Dict], **kwargs) -> Dict:
    if not cookies: return kwargs
    elif isinstance(cookies, str):
        cookies = dict(map(lambda x: re_get("([^=]*)=(.*)", x, index=None)[0], cookies.split('; ')))
    return dict(cookies, **kwargs)


def encode_params(url=str(), params: Dict=dict(), encode=True) -> str:
    if encode: params = urlencode(params)
    else: params = '&'.join([f"{key}={value}" for key, value in params.items()])
    return url+'?'+params if url else params


def encode_object(__object: str) -> str:
    return quote(str(__object).replace('\'','\"'))


###################################################################
########################### Range Filter ##########################
###################################################################

class RangeContext(OptionalDict):
    def __init__(self, field: _KT, left: Optional[Real]=None, right: Optional[Real]=None, valueType: Optional[TypeHint]=None,
                inclusive: Literal["both","neither","left","right"]="both", null=False):
        left, right = self.validate_value(left, right, valueType)
        super().__init__(field=field,
            optional=dict(left=left, right=right, inclusive=inclusive, null=null))

    def validate_value(self, left: Optional[Real]=None, right: Optional[Real]=None,
                        valueType: Optional[TypeHint]=None) -> Tuple[Real,Real]:
        if not valueType: return left, right
        elif is_datetime_type(valueType): return get_datetime_pair(left, right, if_null=(None, None))
        elif is_date_type(valueType): return get_date_pair(left, right, if_null=(None, None))
        else: return left, right


class RangeFilter(TypedRecords):
    dtype = RangeContext
    typeCheck = True

    def __init__(self, *args: RangeContext):
        super().__init__(*args)


###################################################################
######################### Request Session #########################
###################################################################

class UploadSession(GoogleQueryReader, GoogleUploader):
    __metaclass__ = ABCMeta
    operation = "session"

    def __init__(self, queryList: GoogleQueryList=list(), uploadList: GoogleUploadList=list(), account: Optional[Account]=None, **context):
        CustomDict.__init__(self, UNIQUE_CONTEXT(**context))
        self.set_query(queryList, account)
        self.set_upload_info(uploadList, account)

    def set_upload_info(self, uploadList: GoogleUploadList=list(), account: Optional[Account]=None):
        self.update_exists(uploadList=uploadList, account=account)


class RequestSession(UploadSession):
    __metaclass__ = ABCMeta
    asyncio = False
    operation = "session"
    fields = list()
    ranges = list()
    returnType = None
    mappedReturn = False
    info = Info()

    def __init__(self, fields: Optional[IndexLabel]=None, ranges: Optional[RangeFilter]=None, returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debug: Optional[Keyword]=None, extraSave: Optional[Keyword]=None, interrupt: Optional[Keyword]=None,
                numRetries: Optional[int]=None, delay: Range=1., cookies: Optional[str]=None,
                queryList: GoogleQueryList=list(), uploadList: GoogleUploadList=list(), account: Optional[Account]=None, **context):
        self.set_filter_variables(fields, ranges, returnType)
        self.set_init_time(tzinfo, datetimeUnit)
        self.set_logger(logName, logLevel, logFile, localSave, debug, extraSave, interrupt)
        self.set_request_variables(numRetries, delay, cookies)
        UploadSession.__init__(self, queryList, uploadList, account, **context)

    def set_filter_variables(self, fields: Optional[IndexLabel]=None, ranges: Optional[RangeFilter]=None,
                            returnType: Optional[TypeHint]=None):
        self.fields = fields if fields else self.fields
        self.ranges = RangeFilter(*ranges) if isinstance(ranges, Sequence) and ranges else RangeFilter()
        self.returnType = returnType if returnType else self.returnType

    def set_request_variables(self, numRetries: Optional[int]=None, delay: Range=1., cookies: Optional[str]=None):
        self.set_retries(numRetries, delay)
        self.cookies = cookies

    ###################################################################
    ######################### Request Managers ########################
    ###################################################################

    def init_context(self, args: Arguments, context: Context, self_var=True,
                    mapper: Optional[Callable]=None) -> Tuple[Arguments,Context]:
        if self_var: context = dict(self, **context)
        context = mapper(**context) if isinstance(mapper, Callable) else UNIQUE_CONTEXT(**context)
        self.checkpoint("context", where="init_context", msg={"args":args, "context":context})
        return args, context

    def init_task(func):
        @functools.wraps(func)
        def wrapper(self: RequestSession, *args, self_var=True, **context):
            args, context = self.init_context(args, context, self_var=self_var)
            data = func(self, *args, **TASK_CONTEXT(**context))
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper

    def init_session(func):
        @functools.wraps(func)
        def wrapper(self: RequestSession, *args, self_var=True, **context):
            args, context = self.init_context(args, context, self_var=self_var)
            with requests.Session() as session:
                data = func(self, *args, session=session, **SESSION_CONTEXT(**context))
            time.sleep(.25)
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper

    def limit_request(func):
        @functools.wraps(func)
        def wrapper(self: RequestSession, *args, **context):
            response = func(self, *args, **context)
            self.sleep()
            return response
        return wrapper

    ###################################################################
    ########################## Validate Data ##########################
    ###################################################################

    def validate_data(func):
        @functools.wraps(func)
        def wrapper(self: RequestSession, *args, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None, **params):
            data = func(self, *args, fields=fields, returnType=returnType, **params)
            if self.mappedReturn and isinstance(data, Dict):
                return self.filter_mapped_data(data, fields=fields, returnType=returnType)
            else: return self.filter_data(data, fields=fields, returnType=returnType)
        return wrapper

    def filter_data(self, data: Data, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None,
                    default=None, if_null: Literal["drop","pass"]="pass") -> Data:
        data = convert_data(data, return_type=returnType)
        data = filter_data(data, fields=fields, default=default, if_null=if_null)
        return data

    def filter_mapped_data(self, data: Data, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None) -> MappedData:
        if isinstance(fields, Dict):
            return {__key: self.filter_data(data[__key], __fields, returnType)
                    for __key, __fields, in fields.items() if __key in data}
        else: return {__key: self.filter_data(__data, fields, returnType) for __key, __data, in data.items()}

    ###################################################################
    ########################## Validate Range #########################
    ###################################################################

    def validate_range(func):
        @functools.wraps(func)
        def wrapper(self: RequestSession, *args, ranges: RangeFilter=list(), **params):
            data = func(self, *args, **params)
            if self.mappedReturn and isinstance(data, Dict):
                return self.filter_mapped_range(data, ranges=ranges)
            else: return self.filter_range(data, ranges=ranges)
        return wrapper

    def filter_range(self, data: Data, ranges: RangeFilter=list()) -> Data:
        if isinstance(ranges, Sequence):
            for range in ranges:
                data = between_data(data, **range)
        return data

    def filter_mapped_range(self, data: Data, ranges: RangeFilter=list()) -> MappedData:
        if isinstance(ranges, Dict):
            return {__key: (self.filter_range(__data, ranges[__key]) if __key in ranges else __data)
                    for __key, __data in data.items()}
        else: return {__key: self.filter_range(__data, ranges) for __key, __data, in data.items()}

    ###################################################################
    ########################### Arrange Data ##########################
    ###################################################################

    def arrange_data(func):
        @functools.wraps(func)
        def wrapper(self: RequestSession, *args, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None,
                    convert_dtypes=False, sortby=str(), reset_index=False, **context):
            data = func(self, *args, fields=fields, returnType=returnType, **context)
            arrange_context = dict(convert_dtypes=convert_dtypes, sortby=sortby, reset_index=reset_index)
            if self.mappedReturn and isinstance(data, Dict):
                return self.arrange_mapped_data(data, fields=fields, returnType=returnType, **arrange_context)
            else: return self.arrange_single_data(data, fields=fields, returnType=returnType, **arrange_context)
        return wrapper

    def arrange_mapped_data(self, data: Data, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None,
                            convert_dtypes=False, sortby=str(), reset_index=False) -> MappedData:
        context = dict(returnType=returnType, convert_dtypes=convert_dtypes, sortby=sortby, reset_index=reset_index)
        if isinstance(fields, Dict):
            return {__key: self.arrange_single_data(data[__key], __fields, **context)
                    for __key, __fields, in fields.items() if __key in data}
        else: return {__key: self.arrange_single_data(__data, fields, **context) for __key, __data, in data.items()}

    def arrange_single_data(self, data: Data, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None,
                            convert_dtypes=False, sortby=str(), reset_index=False) -> Data:
        data = self.filter_data(data, fields=fields, returnType=returnType)
        if not isinstance(data, pd.DataFrame): return data
        else: return self.arrange_dataframe(data, convert_dtypes, sortby, reset_index)

    def arrange_dataframe(self, data: pd.DataFrame, convert_dtypes=False, sortby=str(), reset_index=False) -> pd.DataFrame:
        if convert_dtypes: data = _convert_dtypes(data)
        if sortby: data = data.sort_values(sortby)
        if reset_index: data = data.reset_index(drop=True)
        return data

    ###################################################################
    ############################ With Data ############################
    ###################################################################

    def with_data(self, data: Data, uploadList: Optional[GoogleUploadList]=list(), func=str(), **context):
        self.checkpoint("crawl", where=func, msg={"data":data}, save=data)
        self.save_result(data)
        self.upload_result(data, uploadList, **context)

    @BaseSession.catch_exception
    def save_result(self, data: Data):
        if not self.localSave: return
        file_name = self.get_save_name()+'_'+self.now("%Y%m%d%H%M%S")+".xlsx"
        if self.mappedReturn and isinstance(data, Dict):
            self.save_mapped_result(file_name, **data)
        else: self.save_dataframe(data, file_name, self.get_save_sheet())

    def save_mapped_result(self, file_name: str, **data):
        renameMap = self.get_rename_map(to="desc")
        with pd.ExcelWriter(file_name, engine="openpyxl") as writer:
            for __key, __data in data.items():
                sheet_name = self.get_save_sheet(__key)
                to_dataframe(__data).rename(columns=renameMap).to_excel(writer, sheet_name=sheet_name, index=False)

    @BaseSession.catch_exception
    def upload_result(self, data: Data, uploadList: GoogleUploadList=list(), **context):
        if not uploadList: return
        elif self.mappedReturn and isinstance(data, Dict):
            for uploadContext in uploadList:
                if not isinstance(uploadContext, Dict): continue
                elif uploadContext.get(NAME) not in data: continue
                else: self.upload_data(data[uploadContext[NAME]], [uploadContext], **context)
        else: self.upload_data(data, uploadList, **context)

    def get_save_name(self, prefix=str()) -> str:
        return prefix if prefix else self.operation

    def get_save_sheet(self, sheet_name=str()) -> str:
        return sheet_name if sheet_name else SAVE_SHEET


###################################################################
############################## Spider #############################
###################################################################

class Spider(RequestSession, Iterator, Parser):
    __metaclass__ = ABCMeta
    asyncio = False
    operation = "spider"
    host = str()
    where = WHERE
    which = WHICH
    by = str()
    fields = list()
    ranges = list()
    iterateArgs = list()
    iterateCount = dict()
    iterateProduct = list()
    iterateUnit = 1
    redirectUnit = 1
    pagination = False
    pageFrom = 1
    offsetFrom = 1
    pageUnit = 0
    pageLimit = 0
    interval = str()
    fromNow = None
    ssl = None
    responseType = "records"
    returnType = "records"
    mappedReturn = False
    root = list()
    groupby = list()
    groupSize = dict()
    countby = str()
    info = Info()
    flow = Flow()

    def __init__(self, fields: Optional[IndexLabel]=None, ranges: Optional[RangeFilter]=None, returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debug: Optional[Keyword]=None, extraSave: Optional[Keyword]=None, interrupt: Optional[Keyword]=None,
                numRetries: Optional[int]=None, delay: Range=1., cookies: Optional[str]=None,
                fromNow: Optional[Unit]=None, discard=True, progress=True, where=str(), which=str(), by=str(), message=str(),
                iterateUnit: Optional[int]=None, interval: Timedelta=str(), apiRedirect=False, redirectUnit: Optional[int]=None,
                queryList: GoogleQueryList=list(), uploadList: GoogleUploadList=list(), account: Optional[Account]=None, **context):
        self.set_filter_variables(fields, ranges, returnType)
        self.set_init_time(tzinfo, datetimeUnit)
        self.set_logger(logName, logLevel, logFile, localSave, debug, extraSave, interrupt)
        self.set_request_variables(numRetries, delay, cookies)
        self.set_spider_variables(fromNow, discard, progress)
        self.set_gather_message(where, which, by, message)
        self.set_iterator_unit(iterateUnit, interval)
        self.set_redirect_variables(apiRedirect, redirectUnit)
        UploadSession.__init__(self, queryList, uploadList, account, **context)
        self._disable_warnings()

    def set_spider_variables(self, fromNow: Optional[Unit]=None, discard=True, progress=True):
        if fromNow is not None:
            self.fromNow = fromNow
        self.discard = discard
        self.progress = progress

    def set_gather_message(self, where=str(), which=str(), by=str(), message=str()):
        if where: self.where = where
        if which: self.which = which
        if by: self.by = by
        if message: self.message = message

    def set_redirect_variables(self, apiRedirect=False, redirectUnit: Optional[int]=None):
        self.apiRedirect = apiRedirect
        self.redirectUnit = exists_one(redirectUnit, self.redirectUnit, self.iterateUnit)

    def _disable_warnings(self):
        if self.ssl == False:
            from urllib3.exceptions import InsecureRequestWarning
            import urllib3
            urllib3.disable_warnings(InsecureRequestWarning)

    ###################################################################
    ############################# Override ############################
    ###################################################################

    def log_errors(self, func: Callable, msg: Dict):
        iterator = self.get_iterator(**msg.get("context", dict()))
        super().log_errors(func=func, msg=(iterator if iterator else msg))

    def get_date(self, date: Optional[DateFormat]=None, if_null: Optional[Unit]=None, index=0, busdate=False) -> dt.date:
        if_null = if_null if if_null is not None else self.fromNow
        return super().get_date(date, if_null=get_scala(if_null, index), busdate=busdate)

    def get_date_pair(self, startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                        if_null: Optional[Unit]=None, busdate=False) -> Tuple[dt.date,dt.date]:
        if_null = if_null if if_null is not None else self.fromNow
        return super().get_date_pair(startDate, endDate, if_null=if_null, busdate=busdate)

    ###################################################################
    ######################### Local Variables #########################
    ###################################################################

    def local_args(self, locals: Dict=dict(), drop: _KT=list(), **context) -> Arguments:
        context = self.from_locals(locals, drop, **context)
        return kloc(context, self.iterateArgs, if_null="drop", values_only=True)

    def local_context(self, locals: Dict=dict(), drop: _KT=list(), **context) -> Context:
        context = self.from_locals(locals, drop, **context)
        return drop_dict(context, self.iterateArgs, inplace=False)

    def local_params(self, locals: Dict=dict(), drop: _KT=list(), **context) -> Tuple[Arguments,Context]:
        context = self.from_locals(locals, drop, **context)
        return self.local_args(**context), self.local_context(**context)

    def local_request(self, locals: Dict=dict(), drop: _KT=list(), **context) -> Context:
        context = self.from_locals(locals, drop, **context)
        keys = unique(ITER_INDEX, *self.get_iterator(keys_only=True), *inspect.getfullargspec(REQUEST_CONTEXT)[0])
        return kloc(context, keys, if_null="drop")

    def local_response(self, locals: Dict=dict(), drop: _KT=list(), **context) -> Context:
        context = self.from_locals(locals, drop, **context)
        return RESPONSE_CONTEXT(**REQUEST_CONTEXT(**context))

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
        if locals:
            context = self.local_context(locals, **context, drop=drop)
        queryMap = self.get_query_map(key="name", value=["type","iterable"])
        for __key in list(context.keys()):
            if __key not in queryMap: pass
            elif is_date_type(queryMap[__key]["type"]):
                context[__key] = self.get_date(context[__key], index=int(str(__key).lower().endswith("enddate")))
            elif queryMap[__key].get("iterable") and notna(context[__key]):
                context[__key] = to_array(context[__key], default=default, dropna=dropna, strict=strict, unique=unique)
            if __key in rename:
                context[__key] = rename_data(context[__key], rename=rename[__key])
        return context

    ###################################################################
    ########################## Gather Request #########################
    ###################################################################

    @abstractmethod
    @RequestSession.init_session
    def crawl(self, *args, **context) -> Data:
        args, context = self.validate_params(locals())
        return self.gather(*args, **context)

    def redirect_available(func):
        @functools.wraps(func)
        def wrapper(self: Spider, *args, apiRedirect=False, **context):
            if apiRedirect: return self.redirect(*args, **context)
            else: return func(self, *args, **context)
        return wrapper

    @redirect_available
    def gather(self, *args, iterator: List[Context]=list(), message=str(), progress=True,
                fields: IndexLabel=list(), ranges: RangeFilter=list(), returnType: Optional[TypeHint]=None, **context) -> Data:
        message = message if message else self.get_gather_message(**context)
        if not iterator:
            iterator, context = self._init_iterator(args, context, self.iterateArgs, self.iterateProduct, self.pagination)
        data = self._gather_data(iterator, message=message, progress=progress, fields=fields, **context)
        self.checkpoint("gather", where="gather", msg={"data":data}, save=data)
        return self.reduce(data, fields=fields, ranges=ranges, returnType=returnType, **context)

    def get_gather_message(self, which=str(), where=str(), by=str(), **context) -> str:
        which = which if which else self.which
        where = where if where else self.where
        by = by if by else self.by
        return GATHER_MSG(which, where, by)

    @RequestSession.arrange_data
    @RequestSession.validate_range
    def reduce(self, data: List[Data], fields: IndexLabel=list(), ranges: RangeFilter=list(),
                returnType: Optional[TypeHint]=None, **context) -> Data:
        return chain_exists(data) if is_array(data) else data

    def _init_iterator(self, args: Arguments, context: Context, iterateArgs: List[_KT]=list(), iterateProduct: List[_KT]=list(),
                        pagination: Pagination=False, indexing=True) -> Tuple[List[Context],Context]:
        self.checkpoint("params", where="init_iterator", msg=dict(zip(["args","context"], self.local_params(locals()))))
        iterate_params = dict(iterateArgs=iterateArgs, iterateProduct=iterateProduct, pagination=pagination)
        iterator, context = self.set_iterator(*args, **iterate_params, indexing=indexing, **context)
        self.checkpoint("iterator", where="init_iterator", msg={"iterator":iterator})
        return iterator, context

    def _gather_data(self, iterator: List[Context], message=str(), progress=True, fields: IndexLabel=list(), **context) -> List[Data]:
        fields = fields if isinstance(fields, Sequence) else list()
        return [self.fetch(**__i, fields=fields, **context) for __i in tqdm(iterator, desc=message, disable=(not progress))]

    ###################################################################
    ########################### Gather Count ##########################
    ###################################################################

    def gather_count(self, *args, countPath: _KT=list(), message=str(), progress=True,
                    fields: IndexLabel=list(), ranges: RangeFilter=list(), returnType: Optional[TypeHint]=None,
                    size: Optional[int]=None, pageSize=0, **context) -> Data:
        hasSize = (size is not None) and (isinstance(size, int) or is_int_array(size, how="all", empty=False))
        context = dict(context, size=(size if hasSize else pageSize), pageSize=pageSize)
        iterator, context = self._init_iterator(args, context, self.iterateArgs, self.iterateProduct, pagination=hasSize)
        if not (hasSize or iterator):
            iterator = [{ITER_INDEX:0}]
        data = self._gather_first(iterator, countPath, hasSize, progress=progress, fields=fields, **context)
        if iterator and (not hasSize):
            data += self._gather_next(iterator, progress=progress, fields=fields, **context)
        return self.reduce(data, fields=fields, ranges=ranges, returnType=returnType, **context)

    def _gather_first(self, iterator: List[Context], countPath: _KT=list(), hasSize=False,
                    message=str(), progress=True, fields: IndexLabel=list(), **context) -> List[Data]:
        message = message if message else self.get_gather_message(by=(FIRST_PAGE if not hasSize else self.by), **context)
        data = self._gather_data(iterator, countPath=countPath, message=message, progress=progress, fields=fields, **context)
        self.checkpoint("gather", where="gather_first", msg={"data":data}, save=data)
        return data

    def _gather_next(self, iterator: List[Context], message=str(), progress=True, fields: IndexLabel=list(),
                    pageSize=0, pageStart=1, offset=1, interval: _PASS=None, **context) -> List[Data]:
        context = dict(context, pageSize=pageSize, pageStart=pageStart+1, offset=offset+pageSize)
        message = message if message else self.get_gather_message(by=NEXT_PAGE, **context)
        iterator, context = self._init_count_iterator(iterator, context, pageSize)
        data = self._gather_data(iterator, message=message, progress=progress, fields=fields, **context)
        self.checkpoint("gather_count", where="gather_next", msg={"data":data}, save=data)
        return data

    def _init_count_iterator(self, iterator: List[Context], context: Context, pageSize=0, indexing=True) -> Tuple[List[Context],Context]:
        iterateArgs = list(drop_dict(iterator[0], [ITER_INDEX]+PAGE_ITERATOR, inplace=False).keys())
        args = transpose_array([kloc(__i, iterateArgs, values_only=True) for __i in iterator])
        context["size"] = [max(self.iterateCount.get(__i[ITER_INDEX], 0)-pageSize, 0) for __i in iterator]
        iterator, context = self.set_iterator(*args, iterateArgs=iterateArgs, pagination=True, indexing=indexing, **context)
        self.checkpoint("iterator_count", where="init_count_iterator", msg={"iterator":iterator})
        return iterator, context

    ###################################################################
    ########################## Fetch Request ##########################
    ###################################################################

    @abstractmethod
    @RequestSession.catch_exception
    @RequestSession.limit_request
    def fetch(self, *args, **context) -> Data:
        ...

    def encode_messages(func):
        @functools.wraps(func)
        def wrapper(self: Spider, method: str, url: str, session: Optional[requests.Session]=None,
                    messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                    data=None, json=None, headers=None, cookies=str(), *args, **context):
            session = session if session is not None else requests
            url, params = self.encode_params(url, params, encode=encode)
            if headers and cookies: headers["Cookie"] = str(cookies)
            messages = messages if messages else dict(params=params, data=data, json=json, headers=headers)
            self.checkpoint("request"+ITER_SUFFIX(context), where=func.__name__, msg=dict(url=url, **exists_dict(messages)))
            self.logger.debug(log_messages(**ITER_MSG(context), **messages, dump=self.logJson))
            response = func(self, method=method, url=url, session=session, messages=messages, **context)
            self.checkpoint("response"+ITER_SUFFIX(context), where=func.__name__, msg={"response":response}, save=response)
            return response
        return wrapper

    @encode_messages
    def request(self, method: str, url: str, session: requests.Session, messages: Dict=dict(),
                params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                valid: Optional[Status]=None, invalid: Optional[Status]=None, close=True, **context) -> requests.Response:
        response = session.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl)
        self.logger.info(log_response(response, url=url, **self.get_iterator(**context, _index=True)))
        if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
        return response.close() if close else response

    @encode_messages
    def request_status(self, method: str, url: str, session: requests.Session, messages: Dict=dict(),
                        params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                        allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                        valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> int:
        with session.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.status_code

    @encode_messages
    def request_content(self, method: str, url: str, session: requests.Session, messages: Dict=dict(),
                        params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                        allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                        valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> bytes:
        with session.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.content

    @encode_messages
    def request_text(self, method: str, url: str, session: requests.Session, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                    allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                    valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> str:
        with session.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.text

    @encode_messages
    def request_json(self, method: str, url: str, session: requests.Session, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                    allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                    valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> JsonData:
        with session.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.json()

    @encode_messages
    def request_headers(self, method: str, url: str, session: requests.Session, messages: Dict=dict(),
                        params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                        allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                        valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> Dict:
        with session.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.headers

    @encode_messages
    def request_source(self, method: str, url: str, session: requests.Session, messages: Dict=dict(),
                        params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                        allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                        valid: Optional[Status]=None, invalid: Optional[Status]=None, features="html.parser", **context) -> Tag:
        with session.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return BeautifulSoup(response.text, features)

    @encode_messages
    def request_table(self, method: str, url: str, session: requests.Session, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                    allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                    valid: Optional[Status]=None, invalid: Optional[Status]=None, html=True, table_header=0, table_idx=0,
                    engine: Optional[Literal["xlrd","openpyxl","odf","pyxlsb"]]=None, **context) -> pd.DataFrame:
        with session.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context, _index=True)))
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

    def log_results(self, data: Data, **context):
        self.logger.info(log_data(data, **self.get_iterator(**context)))

    ###################################################################
    ######################### Redirect Request ########################
    ###################################################################

    def run_iterator(self, iterator: List[Context], self_var=True, **context) -> Data:
        context = UNIQUE_CONTEXT(**(dict(self, **context) if self_var else context))
        self.checkpoint("context", where="run_iterator", msg={"iterator":iterator, "context":context})
        with requests.Session() as session:
            data = self.gather(iterator=iterator, session=session, **SESSION_CONTEXT(**context))
        time.sleep(.25)
        return data

    def gcloud_authorized(func):
        @functools.wraps(func)
        def wrapper(self: Spider, *args, redirectUrl=str(), authorization=str(), account: Account=dict(), **context):
            return func(self, *args, **self._validate_redirect_info(redirectUrl, authorization, account), **context)
        return wrapper

    @gcloud_authorized
    def redirect(self, *args, redirectUnit: Optional[int]=None, progress=True, fields: IndexLabel=list(),
                ranges: RangeFilter=list(), returnType: Optional[TypeHint]=None, **context) -> Data:
        message = self.get_redirect_message(**context)
        iterator, context = self._init_iterator(args, context, self.iterateArgs, self.iterateProduct, self.pagination)
        data = self._redirect_data(iterator, redirectUnit, message=message, progress=progress, fields=fields, **context)
        self.checkpoint("gather", where="redirect", msg={"data":data}, save=data)
        return self.reduce(data, fields=fields, ranges=ranges, returnType=returnType, **context)

    def get_redirect_message(self, **context) -> str:
        return REDIRECT_MSG(self.operation)

    @RequestSession.catch_exception
    @RequestSession.limit_request
    @gcloud_authorized
    def fetch_redirect(self, iterator: List[Context], redirectUrl: str, authorization: str,
                        account: Account=dict(), cookies=str(), **context) -> Data:
        data = self._get_redirect_data(iterator, redirectUrl, authorization, account, cookies=cookies, **context).encode("utf-8")
        response = self.request_json(POST, redirectUrl, data=data, headers=dict(Authorization=authorization), **context)
        return self._parse_redirect(response, **context)

    def _validate_redirect_info(self, redirectUrl=str(), authorization=str(), account: Account=dict()) -> Context:
        if not redirectUrl:
            account = account if account and isinstance(account, dict) else read_gcloud(account)
            redirectUrl = account["audience"]
        if not authorization:
            authorization = fetch_gcloud_authorization(redirectUrl, account)
        return dict(redirectUrl=redirectUrl, authorization=authorization, account=account)

    def _redirect_data(self, iterator: List[Context], redirectUnit: Optional[int]=None, message=str(), progress=True,
                        fields: IndexLabel=list(), **context) -> List[Data]:
        fields = fields if isinstance(fields, Sequence) else list()
        iterator = unit_array(iterator, max(cast_int(redirectUnit), 1))
        return [self.fetch_redirect(__i, fields=fields, **context) for __i in tqdm(iterator, desc=message, disable=(not progress))]

    def _get_redirect_data(self, iterator: List[Context], redirectUrl: str, authorization: str,
                            account: Account=dict(), **context) -> str:
        return json.dumps(dict(
            operation = self.operation,
            iterator = iterator,
            redirectUrl = redirectUrl,
            authorization = authorization,
            account = account,
            **REDIRECT_CONTEXT(**context)), ensure_ascii=False, default=str)

    def _parse_redirect(self, response: RedirectData, **context) -> Data:
        data = self._map_redirect(response)
        self.log_results(data, **context)
        return data

    def _map_redirect(self, data: Records) -> Records:
        if not isinstance(data, (Dict,List)): return list()
        cast_datetime_or_keep = lambda x: cast_datetime_format(x, default=x)
        if isinstance(data, Dict):
            return {__key: self._map_redirect(__data) for __key, __data in data.items()}
        else: return apply_records(data, apply=cast_datetime_or_keep, all_keys=True)


###################################################################
########################## Async Session ##########################
###################################################################

class AsyncSession(RequestSession):
    __metaclass__ = ABCMeta
    asyncio = True
    operation = "session"
    fields = list()
    ranges = list()
    maxLimit = MAX_ASYNC_TASK_LIMIT
    returnType = None
    mappedReturn = False
    info = Info()

    def __init__(self, fields: Optional[IndexLabel]=None, ranges: Optional[RangeFilter]=None, returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debug: Optional[Keyword]=None, extraSave: Optional[Keyword]=None, interrupt: Optional[Keyword]=None,
                numRetries: Optional[int]=None, delay: Range=1., cookies: Optional[str]=None, numTasks=100,
                queryList: GoogleQueryList=list(), uploadList: GoogleUploadList=list(), account: Optional[Account]=None, **context):
        self.set_filter_variables(fields, ranges, returnType)
        self.set_init_time(tzinfo, datetimeUnit)
        self.set_logger(logName, logLevel, logFile, localSave, debug, extraSave, interrupt)
        self.set_async_variables(numRetries, delay, cookies, numTasks)
        UploadSession.__init__(self, queryList, uploadList, account, **context)

    def set_async_variables(self, numRetries: Optional[int]=None, delay: Range=1., cookies: Optional[str]=None, numTasks=100):
        self.set_request_variables(numRetries, delay, cookies)
        self.numTasks = cast_int(numTasks, default=MIN_ASYNC_TASK_LIMIT)

    ###################################################################
    ########################## Async Managers #########################
    ###################################################################

    def init_task(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSession, *args, self_var=True, **context):
            args, context = self.init_context(args, context, self_var=self_var)
            semaphore = self.asyncio_semaphore(**context)
            data = await func(self, *args, semaphore=semaphore, **TASK_CONTEXT(**context))
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper

    def init_session(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSession, *args, self_var=True, **context):
            args, context = self.init_context(args, context, self_var=self_var)
            semaphore = self.asyncio_semaphore(**context)
            async with aiohttp.ClientSession() as session:
                data = await func(self, *args, session=session, semaphore=semaphore, **SESSION_CONTEXT(**context))
            await asyncio.sleep(.25)
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper

    def asyncio_semaphore(self, numTasks: Optional[int]=None, **context) -> asyncio.Semaphore:
        numTasks = numTasks if isinstance(numTasks, int) and numTasks > 0 else self.numTasks
        return asyncio.Semaphore(min(numTasks, self.maxLimit))

    def limit_request(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSession, *args, semaphore: Optional[asyncio.Semaphore]=None, **context):
            if semaphore:
                async with semaphore:
                    response = await func(self, *args, **context)
            else: response = await func(self, *args, **context)
            await self.async_sleep()
            return response
        return wrapper

    async def async_sleep(self, delay: Optional[Range]=None):
        delay = delay if delay is not None else self.get_delay(self.delay)
        if delay: await asyncio.sleep(delay)

    ###################################################################
    ########################## Async Override #########################
    ###################################################################

    def catch_exception(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSession, *args, **context):
            for retry in range(self.numRetries+1):
                try: return await func(self, *args, **context)
                except KeyboardInterrupt as interrupt:
                    raise interrupt
                except Exception as exception:
                    if retry+1 < self.numRetries: await self.async_sleep()
                    return self.pass_exception(exception, func=func, msg={"args":args, "context":context})
        return wrapper

    def ignore_exception(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSession, *args, **context):
            try: return await func(self, *args, **context)
            except: return init_origin(func)
        return wrapper

    def validate_data(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSession, *args, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None, **params):
            data = await func(self, *args, fields=fields, returnType=returnType, **params)
            if self.mappedReturn and isinstance(data, Dict):
                return self.filter_mapped_data(data, fields=fields, returnType=returnType)
            else: return self.filter_data(data, fields=fields, returnType=returnType)
        return wrapper

    def validate_range(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSession, *args, ranges: RangeFilter=list(), **params):
            data = await func(self, *args, **params)
            if self.mappedReturn and isinstance(data, Dict):
                return self.filter_mapped_range(data, ranges=ranges)
            else: return self.filter_range(data, ranges=ranges)
        return wrapper



###################################################################
########################### Async Spider ##########################
###################################################################

class AsyncSpider(Spider, AsyncSession):
    __metaclass__ = ABCMeta
    asyncio = True
    operation = "spider"
    host = str()
    where = WHERE
    which = WHICH
    by = str()
    fields = list()
    ranges = list()
    iterateArgs = list()
    iterateCount = dict()
    iterateProduct = list()
    iterateUnit = 1
    maxLimit = MAX_ASYNC_TASK_LIMIT
    redirectUnit = 1
    redirectLimit = MAX_REDIRECT_LIMIT
    pagination = False
    pageFrom = 1
    offsetFrom = 1
    pageUnit = 0
    pageLimit = 0
    interval = str()
    fromNow = None
    ssl = None
    responseType = "records"
    returnType = "records"
    mappedReturn = False
    root = list()
    groupby = list()
    groupSize = dict()
    countby = str()
    info = Info()
    flow = Flow()

    def __init__(self, fields: Optional[IndexLabel]=None, ranges: Optional[RangeFilter]=None, returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debug: Optional[Keyword]=None, extraSave: Optional[Keyword]=None, interrupt: Optional[Keyword]=None,
                numRetries: Optional[int]=None, delay: Range=1., cookies: Optional[str]=None, numTasks=100,
                fromNow: Optional[Unit]=None, discard=True, progress=True, where=str(), which=str(), by=str(), message=str(),
                iterateUnit: Optional[int]=None, interval: Timedelta=str(), apiRedirect=False, redirectUnit: Optional[int]=None,
                queryList: GoogleQueryList=list(), uploadList: GoogleUploadList=list(), account: Optional[Account]=None, **context):
        self.set_filter_variables(fields, ranges, returnType)
        self.set_init_time(tzinfo, datetimeUnit)
        self.set_logger(logName, logLevel, logFile, localSave, debug, extraSave, interrupt)
        self.set_async_variables(numRetries, delay, cookies, numTasks)
        self.set_max_limit(apiRedirect)
        self.set_spider_variables(fromNow, discard, progress)
        self.set_gather_message(where, which, by, message)
        self.set_iterator_unit(iterateUnit, interval)
        self.set_redirect_variables(apiRedirect, redirectUnit)
        UploadSession.__init__(self, queryList, uploadList, account, **context)
        self._disable_warnings()

    def set_max_limit(self, apiRedirect=False):
        self.maxLimit = min(self.maxLimit, self.redirectLimit) if apiRedirect else self.maxLimit

    ###################################################################
    ########################### Async Gather ##########################
    ###################################################################

    @abstractmethod
    @AsyncSession.init_session
    async def crawl(self, *args, **context) -> Data:
        args, context = self.validate_params(locals())
        return await self.gather(*args, **context)

    def redirect_available(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSpider, *args, apiRedirect=False, **context):
            if apiRedirect: return await self.redirect(*args, **context)
            else: return await func(self, *args, **context)
        return wrapper

    @redirect_available
    async def gather(self, *args, iterator: List[Context]=list(), message=str(), progress=True,
                    fields: IndexLabel=list(), ranges: RangeFilter=list(), returnType: Optional[TypeHint]=None, **context) -> Data:
        message = message if message else self.get_gather_message(**context)
        if not iterator:
            iterator, context = self._init_iterator(args, context, self.iterateArgs, self.iterateProduct, self.pagination)
        data = await self._gather_data(iterator, message=message, progress=progress, fields=fields, **context)
        self.checkpoint("gather", where="gather", msg={"data":data}, save=data)
        return self.reduce(data, fields=fields, ranges=ranges, returnType=returnType, **context)

    async def _gather_data(self, iterator: List[Context], message=str(), progress=True, fields: IndexLabel=list(),
                            **context) -> List[Data]:
        fields = fields if isinstance(fields, Sequence) else list()
        return await tqdm.gather(*[self.fetch(**__i, fields=fields, **context) for __i in iterator], desc=message, disable=(not progress))

    ###################################################################
    ########################### Gather Count ##########################
    ###################################################################

    async def gather_count(self, *args, countPath: _KT=list(), message=str(), progress=True,
                            fields: IndexLabel=list(), ranges: RangeFilter=list(), returnType: Optional[TypeHint]=None,
                            size: Optional[int]=None, pageSize=0, **context) -> Data:
        hasSize = (size is not None) and (isinstance(size, int) or is_int_array(size, how="all", empty=False))
        context = dict(context, size=(size if hasSize else pageSize), pageSize=pageSize)
        iterator, context = self._init_iterator(args, context, self.iterateArgs, self.iterateProduct, pagination=hasSize)
        if not (hasSize or iterator):
            iterator = [{ITER_INDEX:0}]
        data = await self._gather_first(iterator, countPath, hasSize, progress=progress, fields=fields, **context)
        if iterator and (not hasSize):
            data += await self._gather_next(iterator, progress=progress, fields=fields, **context)
        return self.reduce(data, fields=fields, ranges=ranges, returnType=returnType, **context)

    async def _gather_first(self, iterator: List[Context], countPath: _KT=list(), hasSize=False,
                            message=str(), progress=True, fields: IndexLabel=list(), **context) -> List[Data]:
        message = message if message else self.get_gather_message(by=(FIRST_PAGE if not hasSize else self.by), **context)
        data = await self._gather_data(iterator, countPath=countPath, message=message, progress=progress, fields=fields, **context)
        self.checkpoint("gather", where="gather_first", msg={"data":data}, save=data)
        return data

    async def _gather_next(self, iterator: List[Context], message=str(), progress=True, fields: IndexLabel=list(),
                            pageSize=0, pageStart=1, offset=1, interval: _PASS=None, **context) -> List[Data]:
        context = dict(context, pageSize=pageSize, pageStart=pageStart+1, offset=offset+pageSize)
        message = message if message else self.get_gather_message(by=NEXT_PAGE, **context)
        iterator, context = self._init_count_iterator(iterator, context, pageSize)
        data = await self._gather_data(iterator, message=message, progress=progress, fields=fields, **context)
        self.checkpoint("gather_count", where="gather_next", msg={"data":data}, save=data)
        return data

    ###################################################################
    ########################## Async Requests #########################
    ###################################################################

    @abstractmethod
    @AsyncSession.catch_exception
    @AsyncSession.limit_request
    async def fetch(self, *args, **context) -> Data:
        ...

    def encode_messages(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSpider, method: str, url: str, session: Optional[aiohttp.ClientSession]=None,
                        messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                        data=None, json=None, headers=None, cookies=str(), *args, **context):
            session = session if session is not None else aiohttp
            url, params = self.encode_params(url, params, encode=encode)
            if headers and cookies: headers["Cookie"] = str(cookies)
            messages = messages if messages else dict(params=params, data=data, json=json, headers=headers)
            self.checkpoint("request"+ITER_SUFFIX(context), where=func.__name__, msg=dict(url=url, **exists_dict(messages)))
            self.logger.debug(log_messages(**ITER_MSG(context), **messages, dump=self.logJson))
            response = await func(self, method=method, url=url, session=session, messages=messages, **context)
            self.checkpoint("response"+ITER_SUFFIX(context), where=func.__name__, msg={"response":response}, save=response)
            return response
        return wrapper

    @encode_messages
    async def request(self, method: str, url: str, session: aiohttp.ClientSession, messages: Dict=dict(),
                            params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                            allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                            valid: Optional[Status]=None, invalid: Optional[Status]=None, **context):
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, ssl=self.ssl) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)

    @encode_messages
    async def request_status(self, method: str, url: str, session: aiohttp.ClientSession, messages: Dict=dict(),
                            params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                            allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                            valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> int:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, ssl=self.ssl) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.status

    @encode_messages
    async def request_content(self, method: str, url: str, session: aiohttp.ClientSession, messages: Dict=dict(),
                            params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                            allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt", 
                            valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> bytes:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, ssl=self.ssl) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return await response.read()

    @encode_messages
    async def request_text(self, method: str, url: str, session: aiohttp.ClientSession, messages: Dict=dict(),
                            params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                            allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                            valid: Optional[Status]=None, invalid: Optional[Status]=None, encoding=None, **context) -> str:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, ssl=self.ssl) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return await response.text(encoding=encoding)

    @encode_messages
    async def request_json(self, method: str, url: str, session: aiohttp.ClientSession, messages: Dict=dict(),
                            params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                            allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                            valid: Optional[Status]=None, invalid: Optional[Status]=None, encoding=None, **context) -> JsonData:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, ssl=self.ssl) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return await response.json(encoding=encoding, content_type=None)

    @encode_messages
    async def request_headers(self, method: str, url: str, session: aiohttp.ClientSession, messages: Dict=dict(),
                            params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                            allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                            valid: Optional[Status]=None, invalid: Optional[Status]=None, **context) -> Dict:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, ssl=self.ssl) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.headers

    @encode_messages
    async def request_source(self, method: str, url: str, session: aiohttp.ClientSession, messages: Dict=dict(),
                            params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                            allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                            valid: Optional[Status]=None, invalid: Optional[Status]=None, encoding=None,
                            features="html.parser", **context) -> Tag:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, ssl=self.ssl) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return BeautifulSoup(await response.text(encoding=encoding), features)

    @encode_messages
    async def request_table(self, method: str, url: str, session: aiohttp.ClientSession, messages: Dict=dict(),
                            params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                            allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                            valid: Optional[Status]=None, invalid: Optional[Status]=None, html=False, table_header=0, table_idx=0,
                            engine: Optional[Literal["xlrd","openpyxl","odf","pyxlsb"]]=None, **context) -> pd.DataFrame:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, ssl=self.ssl) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context, _index=True)))
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

    async def run_iterator(self, iterator: List[Context], self_var=True, **context) -> Data:
        context = UNIQUE_CONTEXT(**(dict(self, **context) if self_var else context))
        self.checkpoint("context", where="run_iterator", msg={"iterator":iterator, "context":context})
        semaphore = self.asyncio_semaphore(**context)
        async with aiohttp.ClientSession() as session:
            data = await self.gather(iterator=iterator, session=session, semaphore=semaphore, **SESSION_CONTEXT(**context))
        await asyncio.sleep(.25)
        return data

    def gcloud_authorized(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSpider, *args, redirectUrl=str(), authorization=str(), account: Account=dict(), **context):
            return await func(self, *args, **self._validate_redirect_info(redirectUrl, authorization, account), **context)
        return wrapper

    @gcloud_authorized
    async def redirect(self, *args, redirectUnit: Optional[int]=None, progress=True,
                        fields: IndexLabel=list(), ranges: RangeFilter=list(), returnType: Optional[TypeHint]=None, **context) -> Data:
        message = self.get_redirect_message(**context)
        iterator, context = self._init_iterator(args, context, self.iterateArgs, self.iterateProduct, self.pagination)
        data = await self._redirect_data(iterator, redirectUnit, message=message, progress=progress, fields=fields, **context)
        self.checkpoint("gather", where="redirect", msg={"data":data}, save=data)
        return self.reduce(data, fields=fields, ranges=ranges, returnType=returnType, **context)

    @AsyncSession.catch_exception
    @AsyncSession.limit_request
    @gcloud_authorized
    async def fetch_redirect(self, iterator: List[Context], redirectUrl: str, authorization: str,
                            account: Account=dict(), cookies=str(), **context) -> Records:
        data = self._get_redirect_data(iterator, redirectUrl, authorization, account, cookies=cookies, **context).encode("utf-8")
        response = await self.request_json(POST, url=redirectUrl, data=data, headers=dict(Authorization=authorization), **context)
        return self._parse_redirect(response, **context)

    async def _redirect_data(self, iterator: List[Context], redirectUnit: Optional[int]=None, message=str(), progress=True,
                            fields: IndexLabel=list(), **context) -> List[Data]:
        fields = fields if isinstance(fields, Sequence) else list()
        iterator = unit_array(iterator, max(cast_int(redirectUnit), 1))
        return await tqdm.gather(*[self.fetch_redirect(__i, fields=fields, **context) for __i in iterator], desc=message, disable=(not progress))


###################################################################
########################### Login Spider ##########################
###################################################################

class LoginSpider(requests.Session, Spider):
    __metaclass__ = ABCMeta
    asyncio = False
    operation = "login"
    host = str()
    where = WHERE
    ssl = None
    cookie = str()

    @abstractmethod
    def __init__(self, logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None,
                debug: Optional[Keyword]=None, extraSave: Optional[Keyword]=None, interrupt: Optional[Keyword]=None,
                cookies: Optional[Union[str,Dict]]=None, **context):
        requests.Session.__init__(self)
        self.set_cookies(cookies)
        self.set_logger(logName, logLevel, logFile, debug=debug, extraSave=extraSave, interrupt=interrupt)
        self._disable_warnings()

    @abstractmethod
    def login(self):
        ...

    def set_cookies(self, cookies: Optional[Union[str,Dict]]=None):
        if not cookies: return
        elif isinstance(cookies, str): self.cookie = cookies
        elif isinstance(cookies, Dict): self.cookies.update(cookies)
        else: return

    def get_cookies(self, encode=True, raw=False, url=None) -> Union[str,Dict,RequestsCookieJar,SimpleCookie]:
        return get_cookies(self, encode=encode, raw=raw, url=url)

    def update_cookies(self, cookies: Union[str,Dict]=str(), if_exists: Literal["ignore","replace"]="ignore"):
        if not cookies: return
        for __key, __value in decode_cookies(cookies).items():
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
            self.checkpoint(origin+"_response", where=func.__name__, msg={"response":response}, save=response)
            return response
        return wrapper

    @encode_messages
    def request_url(self, method: str, url: str, origin: str, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None,
                    headers=None, cookies=str(), allow_redirects=True, **context):
        with self.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, origin=origin))
            self.logger.debug(log_messages(cookies=self.get_cookies(encode=False), origin=origin, dump=self.logJson))
            self.log_response_text(response, origin)

    @encode_messages
    def request_status(self, method: str, url: str, origin: str, messages: Dict=dict(),
                        params=None, encode: Optional[bool]=None, data=None, json=None,
                        headers=None, cookies=str(), allow_redirects=True, **context) -> int:
        with self.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, origin=origin))
            self.logger.debug(log_messages(cookies=self.get_cookies(encode=False), origin=origin, dump=self.logJson))
            self.log_response_text(response, origin)
            return response.status_code

    @encode_messages
    def request_content(self, method: str, url: str, origin: str, messages: Dict=dict(),
                        params=None, encode: Optional[bool]=None, data=None, json=None,
                        headers=None, cookies=str(), allow_redirects=True, **context) -> bytes:
        with self.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, origin=origin))
            self.logger.debug(log_messages(cookies=self.get_cookies(encode=False), origin=origin, dump=self.logJson))
            self.log_response_text(response, origin)
            return response.content

    @encode_messages
    def request_text(self, method: str, url: str, origin: str, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None,
                    headers=None, cookies=str(), allow_redirects=True, **context) -> str:
        with self.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, origin=origin))
            self.logger.debug(log_messages(cookies=self.get_cookies(encode=False), origin=origin, dump=self.logJson))
            return response.text

    @encode_messages
    def request_json(self, method: str, url: str, origin: str, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None,
                    headers=None, cookies=str(), allow_redirects=True, **context) -> JsonData:
        with self.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, origin=origin))
            self.logger.debug(log_messages(cookies=self.get_cookies(encode=False), origin=origin, dump=self.logJson))
            self.log_response_text(response, origin)
            return response.json()

    @encode_messages
    def request_headers(self, method: str, url: str, origin: str, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None,
                    headers=None, cookies=str(), allow_redirects=True, **context) -> Dict:
        with self.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, origin=origin))
            self.logger.debug(log_messages(cookies=self.get_cookies(encode=False), origin=origin, dump=self.logJson))
            self.log_response_text(response, origin)
            return response.headers

    @encode_messages
    def request_source(self, method: str, url: str, origin: str, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None,
                    headers=None, cookies=str(), allow_redirects=True, features="html.parser", **context) -> Tag:
        with self.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, origin=origin))
            self.logger.debug(log_messages(cookies=self.get_cookies(encode=False), origin=origin, dump=self.logJson))
            return BeautifulSoup(response.text, features)

    def log_response_text(self, response: requests.Response, origin: str):
        self.checkpoint(origin+"_text", where=origin, msg={"response":response.text}, save=response)


class LoginCookie(LoginSpider):
    operation = "login"

    def __init__(self, cookies=str(), **context):
        requests.Session.__init__(self)
        self.update_cookies(cookies)

    def login(self):
        return


###################################################################
######################## Encrypted Session ########################
###################################################################

class EncryptedSession(RequestSession):
    __metaclass__ = ABCMeta
    asyncio = False
    operation = "session"
    where = WHERE
    fields = list()
    ranges = list()
    returnType = None
    mappedReturn = False
    auth = LoginSpider
    authKey = list()
    decryptedKey = dict()
    sessionCookies = True
    info = Info()

    def __init__(self, fields: Optional[IndexLabel]=None, ranges: Optional[RangeFilter]=None, returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debug: Optional[Keyword]=None, extraSave: Optional[Keyword]=None, interrupt: Optional[Keyword]=None,
                numRetries: Optional[int]=None, delay: Range=1., cookies: Optional[str]=None,
                queryList: GoogleQueryList=list(), uploadList: GoogleUploadList=list(), account: Optional[Account]=None,
                encryptedKey: Optional[EncryptedKey]=None, decryptedKey: Optional[DecryptedKey]=None, **context):
        RequestSession.__init__(self, **self.from_locals(locals(), drop=ENCRYPTED_UNIQUE))
        self.set_secret(encryptedKey, decryptedKey)

    def set_secret(self, encryptedKey: Optional[EncryptedKey]=None, decryptedKey: Optional[DecryptedKey]=None):
        if self.cookies or ((encryptedKey is None) and (decryptedKey is None)): return
        elif isinstance(decryptedKey, Dict): pass
        elif isinstance(encryptedKey, str) or isinstance(decryptedKey, str):
            try: decryptedKey = json.loads((decryptedKey if decryptedKey else decrypt(encryptedKey)).replace('\'','\"'))
            except JSONDecodeError: raise ValueError(INVALID_USER_INFO_MSG(self.where))
        decryptedKey = decryptedKey if isinstance(decryptedKey, Dict) else self.decryptedKey
        if decryptedKey:
            self.update(encryptedKey=encrypt(decryptedKey,1), decryptedKey=decryptedKey)

    ###################################################################
    ########################## Login Managers #########################
    ###################################################################

    def login_task(func):
        @functools.wraps(func)
        def wrapper(self: EncryptedSession, *args, self_var=True, **context):
            args, context = self.init_context(args, context, self_var=self_var)
            with self.init_auth(**context) as session:
                login_context = TASK_CONTEXT(**self.validate_account(session, sessionCookies=False, **context))
                data = func(self, *args, **login_context)
            time.sleep(.25)
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper

    def login_session(func):
        @functools.wraps(func)
        def wrapper(self: EncryptedSession, *args, self_var=True, **context):
            args, context = self.init_context(args, context, self_var=self_var)
            with self.init_auth(**context) as session:
                login_context = SESSION_CONTEXT(**self.validate_account(session, **context))
                data = func(self, *args, session=session, **login_context)
            time.sleep(.25)
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper

    def init_auth(self, cookies=str(), **context) -> LoginSpider:
        cookies = cookies if cookies else self.cookies
        if cookies: return LoginCookie(cookies=cookies)
        else: return self.auth(**self.get_auth_info(update=True, **context))

    def get_auth_info(self, update=False, **context) -> Context:
        if not self.authKey: return dict()
        auth_info = self._from_auth_key(**context)
        if not (auth_info and isinstance(auth_info, Dict) and all(auth_info.values())):
            raise ValueError(INVALID_USER_INFO_MSG(self.where))
        self.logger.info(log_encrypt(**auth_info, show=3))
        return dict(context, **auth_info) if update else auth_info

    def _from_auth_key(self, **context) -> Context:
        auth_info = self.decryptedKey if self.decryptedKey else context
        if isinstance(self.authKey, Sequence): return kloc(auth_info, self.authKey, if_null="pass")
        elif isinstance(self.authKey, Callable): return self.authKey(**auth_info)
        else: raise ValueError(INVALID_OBJECT_TYPE_MSG(self.authKey, AUTH_KEY))

    def validate_account(self, auth: LoginSpider, sessionCookies=False, **context) -> Context:
        try: self.login(auth, **context)
        except: raise ValueError(INVALID_USER_INFO_MSG(self.where))
        if isinstance(auth, LoginCookie) or not (sessionCookies and self.sessionCookies):
            context["cookies"] = self.cookies
        return context

    def login(self, auth: LoginSpider, **context):
        auth.login()
        auth.update_cookies(self.set_cookies(**context), if_exists="replace")
        self.checkpoint("login", where="login", msg={"cookies":auth.get_cookies(encode=False)})
        self.update(cookies=auth.get_cookies(encode=True))

    def set_cookies(self, **context) -> Dict:
        return dict()

    ###################################################################
    ########################### API Managers ##########################
    ###################################################################

    def api_task(func):
        @functools.wraps(func)
        def wrapper(self: EncryptedSession, *args, self_var=True, **context):
            args, context = self.init_context(args, context, self_var=self_var)
            api_context = TASK_CONTEXT(**self.get_auth_info(update=True, **context))
            data = func(self, *args, **api_context)
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper

    def api_session(func):
        @functools.wraps(func)
        def wrapper(self: EncryptedSession, *args, self_var=True, **context):
            args, context = self.init_context(args, context, self_var=self_var)
            with requests.Session() as session:
                api_context = SESSION_CONTEXT(**self.get_auth_info(update=True, **context))
                data = func(self, *args, session=session, **api_context)
            time.sleep(.25)
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper


###################################################################
######################### Encrypted Spider ########################
###################################################################

class EncryptedSpider(Spider, EncryptedSession):
    __metaclass__ = ABCMeta
    asyncio = False
    operation = "spider"
    host = str()
    where = WHERE
    which = WHICH
    by = str()
    fields = list()
    ranges = list()
    iterateArgs = list()
    iterateCount = dict()
    iterateProduct = list()
    iterateUnit = 1
    redirectUnit = 1
    pagination = False
    pageFrom = 1
    offsetFrom = 1
    pageUnit = 0
    pageLimit = 0
    interval = str()
    fromNow = None
    ssl = None
    responseType = "records"
    returnType = "records"
    mappedReturn = False
    root = list()
    groupby = list()
    groupSize = dict()
    countby = str()
    auth = LoginSpider
    decryptedKey = dict()
    sessionCookies = True
    info = Info()
    flow = Flow()

    def __init__(self, fields: Optional[IndexLabel]=None, ranges: Optional[RangeFilter]=None, returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debug: Optional[Keyword]=None, extraSave: Optional[Keyword]=None, interrupt: Optional[Keyword]=None,
                numRetries: Optional[int]=None, delay: Range=1., cookies: Optional[str]=None,
                fromNow: Optional[Unit]=None, discard=True, progress=True, where=str(), which=str(), by=str(), message=str(),
                iterateUnit: Optional[int]=None, interval: Timedelta=str(), apiRedirect=False, redirectUnit: Optional[int]=None,
                queryList: GoogleQueryList=list(), uploadList: GoogleUploadList=list(), account: Optional[Account]=None,
                encryptedKey: Optional[EncryptedKey]=None, decryptedKey: Optional[DecryptedKey]=None, **context):
        Spider.__init__(self, **self.from_locals(locals(), drop=ENCRYPTED_UNIQUE))
        self.set_secret(encryptedKey, decryptedKey)


###################################################################
##################### Encrypted Async Session #####################
###################################################################

class EncryptedAsyncSession(AsyncSession, EncryptedSession):
    __metaclass__ = ABCMeta
    asyncio = True
    operation = "session"
    fields = list()
    ranges = list()
    maxLimit = MAX_ASYNC_TASK_LIMIT
    returnType = None
    mappedReturn = False
    auth = LoginSpider
    decryptedKey = dict()
    sessionCookies = True
    info = Info()

    def __init__(self, fields: Optional[IndexLabel]=None, ranges: Optional[RangeFilter]=None, returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debug: Optional[Keyword]=None, extraSave: Optional[Keyword]=None, interrupt: Optional[Keyword]=None,
                numRetries: Optional[int]=None, delay: Range=1., cookies: Optional[str]=None, numTasks=100,
                queryList: GoogleQueryList=list(), uploadList: GoogleUploadList=list(), account: Optional[Account]=None,
                encryptedKey: Optional[EncryptedKey]=None, decryptedKey: Optional[DecryptedKey]=None, **context):
        AsyncSession.__init__(self, **self.from_locals(locals(), drop=ENCRYPTED_UNIQUE))
        self.set_secret(encryptedKey, decryptedKey)

    ###################################################################
    ########################## Login Managers #########################
    ###################################################################

    def login_task(func):
        @functools.wraps(func)
        async def wrapper(self: EncryptedAsyncSession, *args, self_var=True, **context):
            args, context = self.init_context(args, context, self_var=self_var)
            semaphore = self.asyncio_semaphore(**context)
            with self.init_auth(**context) as auth:
                login_context = TASK_CONTEXT(**self.validate_account(auth, sessionCookies=False, **context))
                data = await func(self, *args, semaphore=semaphore, **login_context)
            await asyncio.sleep(.25)
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper

    def login_session(func):
        @functools.wraps(func)
        async def wrapper(self: EncryptedAsyncSession, *args, self_var=True, **context):
            args, context = self.init_context(args, context, self_var=self_var)
            semaphore = self.asyncio_semaphore(**context)
            with self.init_auth(**context) as auth:
                login_context = SESSION_CONTEXT(**self.validate_account(auth, **context))
                cookies = dict(cookies=auth.get_cookies(encode=False)) if self.sessionCookies else dict()
                async with aiohttp.ClientSession(**cookies) as session:
                    data = await func(self, *args, session=session, semaphore=semaphore, **login_context)
            await asyncio.sleep(.25)
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper

    ###################################################################
    ########################### API Managers ##########################
    ###################################################################

    def api_task(func):
        @functools.wraps(func)
        async def wrapper(self: EncryptedAsyncSession, *args, self_var=True, **context):
            args, context = self.init_context(args, context, self_var=self_var)
            semaphore = self.asyncio_semaphore(**context)
            api_context = TASK_CONTEXT(**self.get_auth_info(update=True, **context))
            data = await func(self, *args, semaphore=semaphore, **api_context)
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper

    def api_session(func):
        @functools.wraps(func)
        async def wrapper(self: EncryptedAsyncSession, *args, self_var=True, **context):
            args, context = self.init_context(args, context, self_var=self_var)
            semaphore = self.asyncio_semaphore(**context)
            async with aiohttp.ClientSession() as session:
                api_context = SESSION_CONTEXT(**self.get_auth_info(update=True, **context))
                data = await func(self, *args, session=session, semaphore=semaphore, **api_context)
            await asyncio.sleep(.25)
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper


###################################################################
###################### Encrypted Async Spider #####################
###################################################################

class EncryptedAsyncSpider(AsyncSpider, EncryptedAsyncSession):
    __metaclass__ = ABCMeta
    asyncio = True
    operation = "spider"
    host = str()
    where = WHERE
    which = WHICH
    by = str()
    fields = list()
    ranges = list()
    iterateArgs = list()
    iterateCount = dict()
    iterateProduct = list()
    iterateUnit = 1
    maxLimit = MAX_ASYNC_TASK_LIMIT
    redirectUnit = 1
    redirectLimit = MAX_REDIRECT_LIMIT
    pagination = False
    pageFrom = 1
    offsetFrom = 1
    pageUnit = 0
    pageLimit = 0
    interval = str()
    fromNow = None
    ssl = None
    responseType = "records"
    returnType = "records"
    mappedReturn = False
    root = list()
    groupby = list()
    groupSize = dict()
    countby = str()
    auth = LoginSpider
    decryptedKey = dict()
    sessionCookies = True
    info = Info()
    flow = Flow()

    def __init__(self, fields: Optional[IndexLabel]=None, ranges: Optional[RangeFilter]=None, returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debug: Optional[Keyword]=None, extraSave: Optional[Keyword]=None, interrupt: Optional[Keyword]=None,
                numRetries: Optional[int]=None, delay: Range=1., cookies: Optional[str]=None, numTasks=100,
                fromNow: Optional[Unit]=None, discard=True, progress=True, where=str(), which=str(), by=str(), message=str(),
                iterateUnit: Optional[int]=None, interval: Timedelta=str(), apiRedirect=False, redirectUnit: Optional[int]=None,
                queryList: GoogleQueryList=list(), uploadList: GoogleUploadList=list(), account: Optional[Account]=None,
                encryptedKey: Optional[EncryptedKey]=None, decryptedKey: Optional[DecryptedKey]=None, **context):
        AsyncSpider.__init__(self, **self.from_locals(locals(), drop=ENCRYPTED_UNIQUE))
        self.set_secret(encryptedKey, decryptedKey)


###################################################################
########################## Pipeline Task ##########################
###################################################################

class Task(OptionalDict):
    def __init__(self, operator: Spider, task: str, dataName: str, dataType: Optional[TypeHint]=None,
                name=str(), fields: IndexLabel=list(), allowedFields: Optional[IndexLabel]=None,
                params: Optional[_KT]=None, derivData: Optional[_KT]=None, **context):
        name = name if name else operator.operation
        super().__init__(name=name, operator=operator, task=task, fields=fields, dataName=dataName, dataType=dataType,
            optional=dict(allowedFields=allowedFields, params=params, derivData=derivData, context=context),
            null_if=dict(context=dict()))


class Dag(TypedRecords):
    dtype = Task
    typeCheck = True

    def __init__(self, *args: Union[Task,Sequence[Task]]):
        super().__init__(*args)

    def validate_dtype(self, __object: Dict) -> Union[Task,Sequence[Task]]:
        if isinstance(__object, self.dtype): return __object
        elif isinstance(__object, Sequence): return tuple(map(self.validate_dtype, __object))
        elif isinstance(__object, Dict): return self.dtype(**__object)
        else: self.raise_dtype_error(__object)


###################################################################
########################## Pipeline Info ##########################
###################################################################

class PipelineQuery(Query):
    ...


class PipelineField(Field):
    typeCast = True

    def __init__(self, name: _KT, type: TypeHint, desc: Optional[str]=None, **kwargs):
        super().__init__(name=name, type=type, desc=desc)


class PipelineSchema(Schema):
    dtype = PipelineField
    typeCheck = True

    def copy(self) -> PipelineSchema:
        return copy.deepcopy(self)

    def update(self, __iterable: Iterable[Field], inplace=True) -> PipelineSchema:
        return super().update(__iterable, inplace=inplace)


class PipelineInfo(Info):
    dtype = (PipelineQuery, PipelineSchema)
    typeCheck = True

    def __init__(self, **kwargs: Union[PipelineQuery,PipelineSchema]):
        super().__init__(**kwargs)

    def validate_dtype(self, __object: Union[Iterable,Dict]) -> Union[PipelineQuery,PipelineSchema]:
        if isinstance(__object, self.dtype): return __object
        elif isinstance(__object, Query): return PipelineQuery(*__object)
        elif isinstance(__object, Iterable): return PipelineSchema(*__object)
        else: self.raise_dtype_error(__object, self.dtype[1])

    def union(self, *schema: Schema) -> PipelineSchema:
        return PipelineSchema(*union(*[__schema for __schema in schema if isinstance(__schema, Schema)]))


###################################################################
############################# Pipeline ############################
###################################################################

class Pipeline(EncryptedSession):
    __metaclass__ = ABCMeta
    asyncio = False
    operation = "pipeline"
    fields = list()
    derivFields = list()
    ranges = list()
    returnType = "dataframe"
    mappedReturn = False
    info = PipelineInfo()
    dags = Dag()

    @abstractmethod
    @EncryptedSession.init_task
    def crawl(self, **context) -> Data:
        return self.gather(**context)

    def validate_context(self, locals: Dict=dict(), drop: _KT=list(), rename: Dict[_KT,Dict]=dict(), **context) -> Context:
        if locals:
            context = self.from_locals(locals, **context, drop=drop)
        for __key in list(context.keys()):
            if __key in rename:
                context[__key] = rename_data(context[__key], rename=rename[__key])
        return context

    ###################################################################
    ########################### Gather Task ###########################
    ###################################################################

    def gather(self, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None, **context) -> Data:
        data = dict()
        for task in self.dags:
            data[task[DATANAME]] = self.run_task(task, fields=fields, data=data, **context)
        return self.map_reduce(fields=fields, returnType=returnType, **dict(context, **data))

    def run_task(self, task: Task, fields: IndexLabel=list(), data: Dict[_KT,Data]=dict(), **context) -> Data:
        fields = self._get_fields(fields, allowed=task.get(ALLOWED), name=task[DATANAME])
        method, worker, params = self._from_task(task, fields=fields, data=data, **context)
        response = method(**params)
        self.errors.append({task[NAME]:worker.errors})
        self.checkpoint(task[NAME], where=method.__name__, msg={"data":response}, save=response)
        return response

    def _get_fields(self, fields: IndexLabel=list(), allowed: Optional[IndexLabel]=None, name=str()) -> IndexLabel:
        if self.mappedReturn and isinstance(fields, Dict):
            fields = fields.get(name, list())
            if isinstance(fields, Dict):
                return {__key: self._set_fields(__fields, allowed) for __key, __fields in fields.items()}
        return self._set_fields(fields, allowed)

    def _set_fields(self, fields: IndexLabel, allowed: Optional[IndexLabel]=None) -> IndexLabel:
        fields = cast_list(fields)
        if self.derivFields:
            fields = diff(fields, cast_list(self.derivFields))
        if allowed:
            fields = inter(fields, cast_list(allowed))
        return fields

    @EncryptedSession.limit_request
    def request_crawl(self, worker: Spider, **params) -> Data:
        return worker.crawl(**params)

    @abstractmethod
    @EncryptedSession.arrange_data
    def map_reduce(self, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None, **context) -> Data:
        ...

    ###################################################################
    ############################ From Task ############################
    ###################################################################

    def _from_task(self, task: Task, fields: IndexLabel=list(), data: Dict[_KT,Data]=dict(),
                    **context) -> Tuple[Callable,Spider,Context]:
        method = getattr(self, task[TASK])
        task_filter = dict(fields=self._get_task_fields(task[FIELDS], fields), returnType=task[DATATYPE])
        configs, params = self._get_task_params(task, **context)
        worker = task[OPERATOR](**task_filter, **configs)
        data = kloc(data, cast_list(task[DERIV]), if_null="drop") if (DERIV in task) and data else dict()
        params = dict(params, worker=worker, taskname=task[NAME], **task_filter, **data)
        self.checkpoint("params", where=method.__name__, msg=dict(zip(["task","configs","params"],[task[NAME],configs,params])))
        return method, worker, params

    def _get_task_fields(self, task_fields: IndexLabel=list(), fields: IndexLabel=list()) -> IndexLabel:
        if isinstance(task_fields, Dict):
            if isinstance(fields, Dict):
                return {__key: self._set_task_fields(task_fields[__key], __fields)
                        for __key, __fields in fields.items() if __key in task_fields}
            else: return {__key: self._set_task_fields(__fields, fields)
                        for __key, __fields in task_fields.items()}
        else: return self._set_task_fields(task_fields, fields)

    def _set_task_fields(self, task_fields: IndexLabel=list(), fields: IndexLabel=list()) -> IndexLabel:
        if isinstance(task_fields, Tuple): return task_fields
        else: return unique(*cast_list(task_fields), *fields)

    def _get_task_params(self, task: Task, **context) -> Tuple[Context,Context]:
        if PARAMS in task:
            return self._select_task_params(task, **context)
        else: return self._split_task_params(task, **context)

    def _select_task_params(self, task: Task, **context) -> Tuple[Context,Context]:
        params = kloc(context, task[PARAMS], if_null="drop")
        context = kloc(context, WORKER_UNIQUE, if_null="drop")
        return self._update_extra_params(task, context, PROXY_CONTEXT(**params))

    def _split_task_params(self, task: Task, **context) -> Tuple[Context,Context]:
        context, params = split_dict(context, WORKER_UNIQUE)
        return self._update_extra_params(task, context, PROXY_CONTEXT(**params))

    def _update_extra_params(self, task: Task, context: Context, params: Context) -> Tuple[Context,Context]:
        if CONTEXT in task:
            context = dict(context, **kloc(task[CONTEXT], WORKER_EXTRA, if_null="drop"))
            params = dict(params, **drop_dict(task[CONTEXT], WORKER_EXTRA, inplace=False))
        return context, params


class AsyncPipeline(EncryptedAsyncSession, Pipeline):
    __metaclass__ = ABCMeta
    operation = "pipeline"
    fields = list()
    derivFields = list()
    ranges = list()
    returnType = "dataframe"
    mappedReturn = False
    info = PipelineInfo()
    dags = Dag()

    @abstractmethod
    @EncryptedAsyncSession.init_task
    async def crawl(self, **context) -> Data:
        return await self.gather(**context)

    async def gather(self, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None, **context) -> Data:
        data = dict()
        for task in self.dags:
            if isinstance(task, Sequence):
                response = await asyncio.gather(*[
                    self.run_task(subtask, fields=fields, data=data, **context) for subtask in task])
                data = dict(data, **dict(zip(vloc(task, DATANAME), response)))
            else: data[task[DATANAME]] = await self.run_task(task, fields=fields, data=data, **context)
        return self.map_reduce(fields=fields, returnType=returnType, **dict(context, **data))

    async def run_task(self, task: Task, fields: IndexLabel=list(), data: Dict[_KT,Data]=dict(), **context) -> Data:
        fields = self._get_fields(fields, allowed=task.get(ALLOWED), name=task[DATANAME])
        method, worker, params = self._from_task(task, fields=fields, data=data, **context)
        response = (await method(**params)) if inspect.iscoroutinefunction(method) else method(**params)
        self.errors.append({task[NAME]:worker.errors})
        self.checkpoint(task[NAME], where=task, msg={"data":response}, save=response)
        return response

    @EncryptedAsyncSession.limit_request
    async def async_crawl(self, worker: AsyncSpider, **params) -> Data:
        return await worker.crawl(**params)
