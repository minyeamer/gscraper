from __future__ import annotations
from gscraper.base.abstract import TypedDict, TypedList
from gscraper.base.abstract import UNIQUE_CONTEXT, TASK_CONTEXT, REQUEST_CONTEXT, RESPONSE_CONTEXT
from gscraper.base.abstract import SESSION_CONTEXT, LOGIN_CONTEXT, PROXY_CONTEXT, REDIRECT_CONTEXT

from gscraper.base.session import CustomDict, Iterator, Parser, SchemaInfo
from gscraper.base.session import ITER_INDEX, ITER_SUFFIX, ITER_MSG, PAGE_ITERATOR, pretty_print
from gscraper.base.gcloud import GoogleQueryReader, GoogleUploader, GoogleQueryInfo, GoogleUploadInfo
from gscraper.base.gcloud import fetch_gcloud_authorization

from gscraper.base.types import _KT, _PASS, Arguments, Context, LogLevel, TypeHint, EncryptedKey, DecryptedKey
from gscraper.base.types import IndexLabel, Keyword, Pagination, Status, Unit, DateFormat, Timedelta, Timezone
from gscraper.base.types import Records, RenameMap, Data, MappedData, JsonData, RedirectData, Account
from gscraper.base.types import is_array, is_int_array, init_origin

from gscraper.utils import notna
from gscraper.utils.cast import cast_list, cast_tuple, cast_int, cast_datetime_format
from gscraper.utils.logs import log_encrypt, log_messages, log_response, log_client, log_data
from gscraper.utils.map import to_array, align_array, transpose_array, get_scala, inter, diff
from gscraper.utils.map import kloc, notna_dict, exists_dict, drop_dict, split_dict
from gscraper.utils.map import vloc, apply_records, convert_data, rename_data, filter_data
from gscraper.utils.map import exists_one, unique, chain_exists, between_data, re_get
from gscraper.utils.map import convert_dtypes as _convert_dtypes

from abc import ABCMeta, abstractmethod
from http.cookies import SimpleCookie
from requests.cookies import RequestsCookieJar
from urllib.parse import quote, urlencode, urlparse
import asyncio
import aiohttp
import functools
import inspect
import requests
import time

from typing import Callable, Dict, List, Literal, Optional, Sequence, Tuple, Union
from ast import literal_eval
from bs4 import BeautifulSoup, Tag
from json import JSONDecodeError
import datetime as dt
import json
import pandas as pd

from tqdm.auto import tqdm
import base64
import inspect
import random
import re


MIN_ASYNC_TASK_LIMIT = 1
MAX_ASYNC_TASK_LIMIT = 100
MAX_REDIRECT_LIMIT = 10

TIME_UNIQUE = ["tzinfo", "datetimeUnit"]
LOG_UNIQUE = ["logName", "logLevel", "logFile", "localSave", "debug", "extraSave", "interrupt"]

ITERATOR_UNIQUE = ["iterateUnit", "interval", "fromNow"]

GCLOUD_AUTH_UNIQUE = ["account"]
GCLOUD_QUERY_UNIQUE = ["queryInfo"]
GCLOUD_UPLOAD_UNIQUE = ["uploadInfo", "reauth", "audience", "credentials"]
GCLOUD_UNIQUE = GCLOUD_AUTH_UNIQUE + GCLOUD_QUERY_UNIQUE + GCLOUD_UPLOAD_UNIQUE

FILTER_UNIQUE = ["fields", "returnType"]
REQUEST_UNIQUE = ["delay", "cookies"]

SPIDER_UNIQUE = ["discard", "progress"]
GATHER_UNIQUE = ["message", "where", "which", "by"]
DATE_UNIQUE = ["byDate", "fromDate", "toDate"]

ASYNC_UNIQUE = ["numTasks"]
REDIRECT_UNIQUE = ["apiRedirect", "redirectUnit", "redirectUrl", "authorization", "account"]

ENCRYPTED_UNIQUE = ["encryptedKey", "decryptedKey"]

PROXY_LOG = ["logLevel", "logFile", "debug", "extraSave", "interrupt"]
PIPELINE_UNIQUE = FILTER_UNIQUE + TIME_UNIQUE + PROXY_LOG + REQUEST_UNIQUE + ENCRYPTED_UNIQUE
WORKER_UNIQUE = PIPELINE_UNIQUE + SPIDER_UNIQUE + ASYNC_UNIQUE + REDIRECT_UNIQUE
WORKER_EXTRA = GATHER_UNIQUE + DATE_UNIQUE

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
INVALID_REDIRECT_LOG_MSG = "Please verify that the both results and errors are in redierct returns."

INVALID_USER_INFO_MSG = lambda where=str(): f"{where} user information is not valid.".strip()
INVALID_API_INFO_MSG = lambda where=str(): f"{re.sub(' API$','',where)} API information is not valid.".strip()

DEPENDENCY_HAS_NO_NAME_MSG = "Dependency has no operation name. Please define operation name."

WHERE, WHICH = "urls", "data"
FIRST_PAGE, NEXT_PAGE = "of first page", "of next pages"


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
    return '; '.join([str(key)+"="+str(value) for key, value in cookies.items()])


def encode_cookies(cookies: Union[Dict,str], **kwargs) -> str:
    return '; '.join(
    [(parse_cookies(data) if isinstance(data, Dict) else str(data)) for data in [cookies,kwargs] if data])


def decode_cookies(cookies: str, **kwargs) -> Dict:
    if not cookies: return kwargs
    return dict({__key: __value for cookie in cookies.split('; ') for __key, __value in (cookie.split('=')[:2],)}, **kwargs)


def encode_params(url=str(), params: Dict=dict(), encode=True) -> str:
    if encode: params = urlencode(params)
    else: params = '&'.join([f"{key}={value}" for key, value in params.items()])
    return url+'?'+params if url else params


def encode_object(__object: str) -> str:
    return quote(str(__object).replace('\'','\"'))


###################################################################
######################### Request Session #########################
###################################################################

class UploadSession(GoogleQueryReader, GoogleUploader):
    __metaclass__ = ABCMeta
    operation = "session"
    schemaInfo = SchemaInfo()

    def __init__(self, queryInfo: GoogleQueryInfo=dict(), uploadInfo: GoogleUploadInfo=dict(),
                reauth=False, audience=str(), account: Account=dict(), credentials=None, **context):
        CustomDict.__init__(self, UNIQUE_CONTEXT(**context))
        self.set_query(queryInfo, account)
        self.set_upload_info(uploadInfo, reauth, audience, account, credentials)

    def set_upload_info(self, uploadInfo: GoogleUploadInfo=dict(), reauth=False, audience=str(),
                        account: Account=dict(), credentials=None):
        self.update_exists(uploadInfo=uploadInfo, reauth=reauth, audience=audience, account=account, credentials=credentials)

    def get_rename_map(self, to: Optional[Literal["desc","name"]]="desc", schema_names: _KT=list(),
                        keep: Literal["fist","last",False]="first") -> RenameMap:
        if to in ("desc", "name"):
            return self.schemaInfo.get_rename_map(to=to, schema_names=schema_names, keep=keep)
        else: return dict()

    def print(self, *__object, path: Optional[_KT]=None, drop: Optional[_KT]=None, indent=2, step=2, sep=' '):
        pretty_print(*__object, path=path, drop=drop, indent=indent, step=step, sep=sep)

    def print_log(self, log_string: str, func="checkpoint", path: Optional[_KT]=None, drop: Optional[_KT]=None,
                    indent=2, step=2, sep=' '):
        log_object = self.eval_log(log_string, func=func)
        self.print(log_object, path=path, drop=drop, indent=indent, step=step, sep=sep)


class RequestSession(UploadSession):
    __metaclass__ = ABCMeta
    asyncio = False
    operation = "session"
    fields = list()
    tzinfo = None
    datetimeUnit = "second"
    returnType = None
    mappedReturn = False
    schemaInfo = SchemaInfo()

    def __init__(self, fields: Optional[IndexLabel]=None, returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debug: Optional[Keyword]=None, extraSave: Optional[Keyword]=None, interrupt: Optional[str]=None,
                delay: Union[float,int,Tuple[int]]=1., cookies: Optional[str]=None,
                byDate: Optional[IndexLabel]=None, fromDate: Optional[DateFormat]=None, toDate: Optional[DateFormat]=None,
                queryInfo: GoogleQueryInfo=dict(), uploadInfo: GoogleUploadInfo=dict(),
                reauth=False, audience=str(), account: Account=dict(), credentials=None, **context):
        self.set_filter_variables(fields, returnType)
        self.set_init_time(tzinfo, datetimeUnit)
        self.set_logger(logName, logLevel, logFile, localSave, debug, extraSave, interrupt)
        self.set_request_variables(delay, cookies)
        self.set_date_filter(byDate, fromDate=fromDate, toDate=toDate)
        UploadSession.__init__(self, queryInfo, uploadInfo, reauth, audience, account, credentials, **context)

    def set_filter_variables(self, fields: Optional[IndexLabel]=None, returnType: Optional[TypeHint]=None):
        self.fields = fields if fields else self.fields
        self.returnType = returnType if returnType else self.returnType

    def set_request_variables(self, delay: Union[float,int,Tuple[int]]=1., cookies: Optional[str]=None):
        self.delay = delay
        self.cookies = cookies

    def set_date_filter(self, byDate: IndexLabel, fromDate: Optional[DateFormat]=None, toDate: Optional[DateFormat]=None):
        if byDate:
            self.byDate = cast_list(byDate)
            self.fromDate, self.toDate = self.get_date_pair(fromDate, toDate, if_null=(None, None))

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

    def sleep(self, tsUnit: Literal["ms","s"]="ms"):
        delay = self.get_delay(tsUnit)
        if delay: time.sleep(delay)

    def get_delay(self, tsUnit: Literal["ms","s"]="ms") -> Union[float,int]:
        if isinstance(self.delay, (float,int)):
            return self.delay
        elif isinstance(self.delay, Tuple):
            random.randrange(*self.delay[:2])/(1000 if tsUnit == "ms" else 1)
        else: return 0.

    ###################################################################
    ########################## Validate Data ##########################
    ###################################################################

    def validate_data(func):
        @functools.wraps(func)
        def wrapper(self: Pipeline, *args, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None, **params):
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
    ########################## Validate Date ##########################
    ###################################################################

    def validate_date(func):
        @functools.wraps(func)
        def wrapper(self: Pipeline, *args, byDate: IndexLabel=list(),
                    fromDate: Optional[dt.date]=None, toDate: Optional[dt.date]=None, **params):
            data = func(self, *args, byDate=byDate, fromDate=fromDate, toDate=toDate, **params)
            if self.mappedReturn: return data
            else: return self.filter_date(data, byDate=byDate, fromDate=fromDate, toDate=toDate)
        return wrapper

    def filter_date(self, data: Data, byDate: IndexLabel=list(),
                    fromDate: Optional[dt.date]=None, toDate: Optional[dt.date]=None) -> Data:
        if byDate and (fromDate or toDate):
            between_context = {field: (fromDate, toDate) for field in cast_tuple(byDate)}
            return between_data(data, **between_context)
        else: return data

    ###################################################################
    ########################### Arrange Data ##########################
    ###################################################################

    def arrange_data(func):
        @functools.wraps(func)
        def wrapper(self: Pipeline, *args, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None,
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

    def with_data(self, data: Data, prefix=str(), uploadInfo: Optional[GoogleUploadInfo]=dict(),
                    func=str(), **context):
        self.checkpoint("crawl", where=func, msg={"data":data}, save=data)
        if self.mappedReturn and isinstance(data, Dict):
            self.with_mapped_data(data, uploadInfo=uploadInfo, **context)
        else: self.with_single_data(data, prefix=prefix, uploadInfo=uploadInfo, **context)

    def with_mapped_data(self, data: Data, uploadInfo: Optional[GoogleUploadInfo]=dict(), **context):
        for __key, __data in data.items():
            self.with_single_data(__data, prefix=__key, uploadInfo=kloc(uploadInfo, [__key], if_null="drop"), **context)

    def with_single_data(self, data: Data, prefix=str(), uploadInfo: Optional[GoogleUploadInfo]=dict(), **context):
        if self.localSave:
            self.save_data(data, prefix=self.get_save_name(prefix), ext="dataframe")
        if uploadInfo:
            self.upload_data(data, uploadInfo, **context)

    def get_save_name(self, prefix=str()) -> str:
        return prefix if prefix else self.operation


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
    iterateArgs = list()
    iterateCount = dict()
    iterateProduct = list()
    iterateUnit = 1
    pagination = False
    pageFrom = 1
    offsetFrom = 1
    pageUnit = 0
    pageLimit = 0
    interval = str()
    fromNow = None
    tzinfo = None
    datetimeUnit = "second"
    ssl = None
    responseType = "records"
    returnType = "records"
    mappedReturn = False
    root = list()
    groupby = list()
    groupSize = dict()
    countby = str()
    schemaInfo = SchemaInfo()

    def __init__(self, fields: Optional[IndexLabel]=None, returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debug: Optional[Keyword]=None, extraSave: Optional[Keyword]=None, interrupt: Optional[str]=None,
                iterateUnit: Unit=0, interval: Timedelta=str(), delay: Union[float,int,Tuple[int]]=1., cookies: Optional[str]=None,
                fromNow: Optional[Unit]=None, discard=True, progress=True, where=str(), which=str(), by=str(), message=str(),
                byDate: Optional[IndexLabel]=None, fromDate: Optional[DateFormat]=None, toDate: Optional[DateFormat]=None,
                queryInfo: GoogleQueryInfo=dict(), uploadInfo: GoogleUploadInfo=dict(),
                reauth=False, audience=str(), account: Account=dict(), credentials=None, **context):
        self.set_filter_variables(fields, returnType)
        self.set_init_time(tzinfo, datetimeUnit)
        self.set_logger(logName, logLevel, logFile, localSave, debug, extraSave, interrupt)
        self.set_iterator_unit(iterateUnit, interval)
        self.set_request_variables(delay, cookies)
        self.set_spider_variables(fromNow, discard, progress)
        self.set_gather_message(where, which, by, message)
        self.set_date_filter(byDate, fromDate=fromDate, toDate=toDate)
        UploadSession.__init__(self, queryInfo, uploadInfo, reauth, audience, account, credentials, **context)
        self._disable_warnings()

    def set_spider_variables(self, fromNow: Optional[Unit]=None, discard=True, progress=True):
        self.fromNow = fromNow
        self.discard = discard
        self.progress = progress

    def set_gather_message(self, where=str(), which=str(), by=str(), message=str()):
        if where: self.where = where
        if which: self.which = which
        if by: self.by = by
        if message: self.message = message

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
        if_null = if_null if if_null else self.fromNow
        return super().get_date(date, if_null=get_scala(if_null, index), busdate=busdate)

    def get_date_pair(self, startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                        if_null: Optional[Unit]=None, busdate=False) -> Tuple[dt.date,dt.date]:
        if_null = if_null if if_null else self.fromNow
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
        dateformat_keys = self.inspect("crawl", annotation="DateFormat").keys()
        sequence_keys = self.inspect("crawl", "iterable").keys()
        for __key in list(context.keys()):
            if __key in dateformat_keys:
                context[__key] = self.get_date(context[__key], index=int(str(__key).lower().endswith("enddate")))
            elif (__key in sequence_keys) and notna(context[__key]):
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

    def gather(self, *args, message=str(), progress=True, fields: IndexLabel=list(),
                byDate: IndexLabel=list(), fromDate: Optional[dt.date]=None, toDate: Optional[dt.date]=None,
                returnType: Optional[TypeHint]=None, **context) -> Data:
        message = message if message else self.get_gather_message(**context)
        iterator, context = self._init_iterator(args, context, self.iterateArgs, self.iterateProduct, self.pagination)
        data = self._gather_data(iterator, message=message, progress=progress, fields=fields, **context)
        self.checkpoint("gather", where="gather", msg={"data":data}, save=data)
        return self.reduce(data, fields=fields, byDate=byDate, fromDate=fromDate, toDate=toDate, returnType=returnType, **context)

    def get_gather_message(self, which=str(), where=str(), by=str(), **context) -> str:
        which = which if which else self.which
        where = where if where else self.where
        by = by if by else self.by
        return GATHER_MSG(which, where, by)

    @RequestSession.arrange_data
    @RequestSession.validate_date
    def reduce(self, data: List[Data], fields: IndexLabel=list(), byDate: IndexLabel=list(),
                fromDate: Optional[dt.date]=None, toDate: Optional[dt.date]=None,
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

    def gather_count(self, *args, countPath: _KT=list(), message=str(), progress=True, fields: IndexLabel=list(),
                    byDate: IndexLabel=list(), fromDate: Optional[dt.date]=None, toDate: Optional[dt.date]=None,
                    returnType: Optional[TypeHint]=None, size: Optional[int]=None, pageSize=0, **context) -> Data:
        hasSize = (size is not None) and (isinstance(size, int) or is_int_array(size, how="all", empty=False))
        context = dict(context, size=(size if hasSize else pageSize), pageSize=pageSize)
        iterator, context = self._init_iterator(args, context, self.iterateArgs, self.iterateProduct, pagination=hasSize)
        if not (hasSize or iterator):
            iterator = [{ITER_INDEX:0}]
        data = self._gather_first(iterator, countPath, hasSize, progress=progress, fields=fields, **context)
        if iterator and (not hasSize):
            data += self._gather_next(iterator, progress=progress, fields=fields, **context)
        return self.reduce(data, fields=fields, byDate=byDate, fromDate=fromDate, toDate=toDate, returnType=returnType, **context)

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
########################## Async Session ##########################
###################################################################

class AsyncSession(RequestSession):
    __metaclass__ = ABCMeta
    asyncio = True
    operation = "session"
    fields = list()
    tzinfo = None
    datetimeUnit = "second"
    maxLimit = MAX_ASYNC_TASK_LIMIT
    returnType = None
    mappedReturn = False
    schemaInfo = SchemaInfo()

    def __init__(self, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debug: Optional[Keyword]=None, extraSave: Optional[Keyword]=None, interrupt: Optional[str]=None,
                delay: Union[float,int,Tuple[int]]=1., numTasks=100, cookies: Optional[str]=None,
                byDate: Optional[IndexLabel]=None, fromDate: Optional[DateFormat]=None, toDate: Optional[DateFormat]=None,
                queryInfo: GoogleQueryInfo=dict(), uploadInfo: GoogleUploadInfo=dict(),
                reauth=False, audience=str(), account: Account=dict(), credentials=None, **context):
        self.set_filter_variables(fields, returnType)
        self.set_init_time(tzinfo, datetimeUnit)
        self.set_logger(logName, logLevel, logFile, localSave, debug, extraSave, interrupt)
        self.set_async_variables(delay, numTasks, cookies)
        self.set_date_filter(byDate, fromDate=fromDate, toDate=toDate)
        UploadSession.__init__(self, queryInfo, uploadInfo, reauth, audience, account, credentials, **context)

    def set_async_variables(self, delay: Union[float,int,Tuple[int]]=1., numTasks=100, cookies: Optional[str]=None):
        self.delay = delay
        self.numTasks = cast_int(numTasks, default=MIN_ASYNC_TASK_LIMIT)
        self.cookies = cookies

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

    async def async_sleep(self, tsUnit: Literal["ms","s"]="ms"):
        delay = self.get_delay(tsUnit)
        if delay: await asyncio.sleep(delay)

    ###################################################################
    ########################## Async Override #########################
    ###################################################################

    def catch_exception(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSession, *args, **context):
            try: return await func(self, *args, **context)
            except KeyboardInterrupt as interrupt:
                raise interrupt
            except Exception as exception:
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
        async def wrapper(self: Pipeline, *args, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None, **params):
            data = await func(self, *args, fields=fields, returnType=returnType, **params)
            if self.mappedReturn and isinstance(data, Dict):
                return self.filter_mapped_data(data, fields=fields, returnType=returnType)
            else: return self.filter_data(data, fields=fields, returnType=returnType)
        return wrapper

    def validate_date(func):
        @functools.wraps(func)
        async def wrapper(self: Pipeline, *args, byDate: IndexLabel=list(),
                    fromDate: Optional[dt.date]=None, toDate: Optional[dt.date]=None, **params):
            data = await func(self, *args, byDate=byDate, fromDate=fromDate, toDate=toDate, **params)
            if self.mappedReturn: return data
            else: return self.filter_date(data, byDate=byDate, fromDate=fromDate, toDate=toDate)
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
    iterateArgs = list()
    redirectArgs = None
    iterateCount = dict()
    iterateProduct = list()
    redirectProduct = list()
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
    tzinfo = None
    datetimeUnit = "second"
    ssl = None
    responseType = "records"
    returnType = "records"
    mappedReturn = False
    root = list()
    groupby = list()
    groupSize = dict()
    countby = str()
    schemaInfo = SchemaInfo()

    def __init__(self, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debug: Optional[Keyword]=None, extraSave: Optional[Keyword]=None, interrupt: Optional[str]=None,
                iterateUnit: Unit=0, interval: Timedelta=str(), delay: Union[float,int,Tuple[int]]=1., cookies: Optional[str]=None,
                fromNow: Optional[Unit]=None, discard=True, progress=True, where=str(), which=str(), by=str(), message=str(),
                byDate: IndexLabel=list(), fromDate: Optional[DateFormat]=None, toDate: Optional[DateFormat]=None,
                numTasks=100, apiRedirect=False, redirectUnit: Optional[Unit]=None,
                queryInfo: GoogleQueryInfo=dict(), uploadInfo: GoogleUploadInfo=dict(),
                reauth=False, audience=str(), account: Account=dict(), credentials=None, **context):
        self.set_filter_variables(fields, returnType)
        self.set_init_time(tzinfo, datetimeUnit)
        self.set_logger(logName, logLevel, logFile, localSave, debug, extraSave, interrupt)
        self.set_iterator_unit(iterateUnit, interval)
        self.set_async_variables(delay, numTasks, cookies)
        self.set_spider_variables(fromNow, discard, progress)
        self.set_gather_message(where, which, by, message)
        self.set_date_filter(byDate, fromDate=fromDate, toDate=toDate)
        self.set_redirect_variables(apiRedirect, redirectUnit)
        UploadSession.__init__(self, queryInfo, uploadInfo, reauth, audience, account, credentials, **context)
        self._disable_warnings()

    def set_redirect_variables(self, apiRedirect=False, redirectUnit: Optional[Unit]=None):
        self.apiRedirect = apiRedirect
        self.redirectUnit = exists_one(redirectUnit, self.redirectUnit, self.iterateUnit)

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
            if apiRedirect: return await self.redirect(self, *args, **context)
            else: return await func(self, *args, **context)
        return wrapper

    @redirect_available
    async def gather(self, *args, message=str(), progress=True, fields: IndexLabel=list(),
                    byDate: IndexLabel=list(), fromDate: Optional[dt.date]=None, toDate: Optional[dt.date]=None,
                    returnType: Optional[TypeHint]=None, **context) -> Data:
        message = message if message else self.get_gather_message(**context)
        iterator, context = self._init_iterator(args, context, self.iterateArgs, self.iterateProduct, self.pagination)
        data = await self._gather_data(iterator, message=message, progress=progress, fields=fields, **context)
        self.checkpoint("gather", where="gather", msg={"data":data}, save=data)
        return self.reduce(data, fields=fields, returnType=returnType, byDate=byDate, fromDate=fromDate, toDate=toDate, **context)

    async def _gather_data(self, iterator: List[Context], message=str(), progress=True, fields: IndexLabel=list(),
                            **context) -> List[Data]:
        fields = fields if isinstance(fields, Sequence) else list()
        return await tqdm.gather(*[self.fetch(**__i, fields=fields, **context) for __i in iterator], desc=message, disable=(not progress))

    ###################################################################
    ########################### Gather Count ##########################
    ###################################################################

    @redirect_available
    async def gather_count(self, *args, countPath: _KT=list(), message=str(), progress=True, fields: IndexLabel=list(),
                            byDate: IndexLabel=list(), fromDate: Optional[dt.date]=None, toDate: Optional[dt.date]=None,
                            returnType: Optional[TypeHint]=None, size: Optional[int]=None, pageSize=0, **context) -> Data:
        hasSize = (size is not None) and (isinstance(size, int) or is_int_array(size, how="all", empty=False))
        context = dict(context, size=(size if hasSize else pageSize), pageSize=pageSize)
        iterator, context = self._init_iterator(args, context, self.iterateArgs, self.iterateProduct, pagination=hasSize)
        if not (hasSize or iterator):
            iterator = [{ITER_INDEX:0}]
        data = await self._gather_first(iterator, countPath, hasSize, progress=progress, fields=fields, **context)
        if iterator and (not hasSize):
            data += await self._gather_next(iterator, progress=progress, fields=fields, **context)
        return self.reduce(data, fields=fields, byDate=byDate, fromDate=fromDate, toDate=toDate, returnType=returnType, **context)

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

    def gcloud_authorized(func):
        @functools.wraps(func)
        async def wrapper(self: Spider, *args, redirectUrl=str(), authorization=str(), account: Account=dict(), **context):
            if not authorization:
                authorization = fetch_gcloud_authorization(redirectUrl, account)
            return await func(
                self, *args, redirectUrl=redirectUrl, authorization=authorization, account=account, **context)
        return wrapper

    @gcloud_authorized
    async def redirect(self, *args, redirectUnit: Unit=1, progress=True, fields: IndexLabel=list(),
                        byDate: IndexLabel=list(), fromDate: Optional[dt.date]=None, toDate: Optional[dt.date]=None,
                        returnType: Optional[TypeHint]=None, **context) -> Data:
        redirectArgs = redirectArgs if is_array(self.redirectArgs) else self.iterateArgs
        context["iterateUnit"] = redirectUnit
        message = self.get_redirect_message(**context)
        iterator, context = self._init_iterator(args, context, redirectArgs, self.redirectProduct, self.pagination)
        data = await self._redirect_data(iterator, message=message, progress=progress, fields=fields, **context)
        self.checkpoint("gather", where="redirect", msg={"data":data}, save=data)
        return self.reduce(data, fields=fields, returnType=returnType, byDate=byDate, fromDate=fromDate, toDate=toDate, **context)

    def get_redirect_message(self, **context) -> str:
        return REDIRECT_MSG(self.operation)

    async def _redirect_data(self, iterator: List[Context], message=str(), progress=True, fields: IndexLabel=list(),
                            **context) -> List[Data]:
        fields = fields if isinstance(fields, Sequence) else list()
        return await tqdm.gather(*[
                self.fetch_redirect(**__i, fields=fields, **context) for __i in iterator], desc=message, disable=(not progress))

    @AsyncSession.catch_exception
    @AsyncSession.limit_request
    @gcloud_authorized
    async def fetch_redirect(self, redirectUrl: str, authorization: str, session: Optional[aiohttp.ClientSession]=None,
                            account: Account=dict(), **context) -> Records:
        data = self._filter_redirect_data(redirectUrl, authorization, account, **context)
        messages = dict(json=data, headers=dict(Authorization=authorization))
        self.logger.debug(log_messages(**ITER_MSG(context), **messages, dump=self.logJson))
        async with session.post(redirectUrl, **messages) as response:
            self.checkpoint("redirect"+ITER_SUFFIX(context), where="fetch_redirect", msg={"response":response}, save=response, ext="json")
            self.logger.info(await log_client(response, url=redirectUrl, **self.get_iterator(**context, _index=True)))
            return self._parse_redirect(response.json(), **context)

    def _filter_redirect_data(self, redirectUrl: str, authorization: str, account: Account=dict(), **context) -> Context:
        return dict(
            operation = self.operation,
            redirectUrl=redirectUrl,
            authorization=authorization,
            account=account,
            **REDIRECT_CONTEXT(**context))

    def _parse_redirect(self, data: RedirectData, **context) -> Records:
        data = self._log_redirect_errors(data)
        results = self._map_redirect(data, **context)
        self.logger.debug(log_data(results, **context))
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
        return apply_records(data, apply=cast_datetime_or_keep, all_keys=True)


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

    @abstractmethod
    def __init__(self, logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None,
                debug: Optional[Keyword]=None, extraSave: Optional[Keyword]=None, interrupt: Optional[str]=None,
                cookies: Optional[str]=None, **context):
        requests.Session.__init__(self)
        if cookies: self.cookies.update(decode_cookies(cookies))
        self.set_logger(logName, logLevel, logFile, debug=debug, extraSave=extraSave, interrupt=interrupt)
        self._disable_warnings()

    @abstractmethod
    def login(self):
        ...

    def get_cookies(self, encode=True, raw=False, url=None) -> Union[str,Dict,RequestsCookieJar,SimpleCookie]:
        return get_cookies(self, encode=encode, raw=raw, url=url)

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


class BaseLogin(LoginSpider):
    operation = "login"

    def __init__(self, cookies=str()):
        super().__init__(cookies=cookies)

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
    tzinfo = None
    datetimeUnit = "second"
    returnType = None
    mappedReturn = False
    schemaInfo = SchemaInfo()
    auth = LoginSpider
    authKey = list()
    decryptedKey = dict()
    sessionCookies = True

    def __init__(self, fields: Optional[IndexLabel]=None, returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debug: Optional[Keyword]=None, extraSave: Optional[Keyword]=None, interrupt: Optional[str]=None,
                delay: Union[float,int,Tuple[int]]=1., cookies: Optional[str]=None,
                byDate: Optional[IndexLabel]=None, fromDate: Optional[DateFormat]=None, toDate: Optional[DateFormat]=None,
                queryInfo: GoogleQueryInfo=dict(), uploadInfo: GoogleUploadInfo=dict(),
                reauth=False, audience=str(), account: Account=dict(), credentials=None,
                encryptedKey: Optional[EncryptedKey]=None, decryptedKey: Optional[DecryptedKey]=None, **context):
        RequestSession.__init__(self, **self.from_locals(locals(), drop=ENCRYPTED_UNIQUE))
        self.set_secret(encryptedKey, decryptedKey)

    def set_secret(self, encryptedKey: Optional[EncryptedKey]=None, decryptedKey: Optional[DecryptedKey]=None):
        if self.cookies or ((encryptedKey is None) and (decryptedKey is None)): return
        elif isinstance(decryptedKey, Dict): pass
        elif isinstance(encryptedKey, str) or isinstance(decryptedKey, str):
            try: decryptedKey = json.loads(decryptedKey if decryptedKey else decrypt(encryptedKey,1))
            except JSONDecodeError: raise ValueError(INVALID_USER_INFO_MSG(self.where))
        decryptedKey = decryptedKey if isinstance(decryptedKey, Dict) else self.decryptedKey
        if decryptedKey:
            self.update(encryptedKey=encrypt(decryptedKey,1), decryptedKey=decryptedKey)
            self.logger.info(log_encrypt(**self.decryptedKey, show=3))

    ###################################################################
    ########################## Login Managers #########################
    ###################################################################

    def login_task(func):
        @functools.wraps(func)
        def wrapper(self: EncryptedSpider, *args, self_var=True, **context):
            args, context = self.init_context(args, context, self_var=self_var)
            with (BaseLogin(cookies=self.cookies) if self.cookies else self.auth(**self.get_auth_params(**context))) as session:
                login_context = TASK_CONTEXT(**self.validate_account(session, sessionCookies=False, **context))
                data = func(self, *args, **login_context)
            time.sleep(.25)
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper

    def login_session(func):
        @functools.wraps(func)
        def wrapper(self: EncryptedSpider, *args, self_var=True, **context):
            args, context = self.init_context(args, context, self_var=self_var)
            with (BaseLogin(cookies=self.cookies) if self.cookies else self.auth(**self.get_auth_params(**context))) as session:
                login_context = LOGIN_CONTEXT(**self.validate_account(session, **context))
                data = func(self, *args, session=session, **login_context)
            time.sleep(.25)
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper

    def get_auth_params(self, **context) -> Context:
        auth_info = kloc((self.decryptedKey if self.decryptedKey else context), self.authKey, if_null="drop")
        if self.authKey and not (auth_info and all(auth_info.values())):
            raise ValueError(INVALID_USER_INFO_MSG(self.where))
        else: return dict(auth_info, **notna_dict(kloc(context, PROXY_LOG, if_null="drop")))

    def validate_account(self, auth: LoginSpider, sessionCookies=True, **context) -> Context:
        if self.cookies:
            self.sessionCookies = False
            return dict(context, cookies=encode_cookies(self.cookies, **self.set_cookies(**context)))
        self.login(auth, **context)
        if not (sessionCookies and self.sessionCookies):
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
        def wrapper(self: EncryptedSpider, *args, self_var=True, **context):
            args, context = self.init_context(args, context, self_var=self_var)
            api_context = TASK_CONTEXT(**self.validate_secret(**context))
            data = func(self, *args, **api_context)
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper

    def api_session(func):
        @functools.wraps(func)
        def wrapper(self: EncryptedSpider, *args, self_var=True, **context):
            args, context = self.init_context(args, context, self_var=self_var)
            with requests.Session() as session:
                api_context = SESSION_CONTEXT(**self.validate_secret(**context))
                data = func(self, *args, session=session, **api_context)
            time.sleep(.25)
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper

    def validate_secret(self, **context) -> Context:
        api_info = self.get_api_info(**context)
        self.checkpoint("api", where="set_headers", msg={"api_info":api_info})
        return dict(context, **api_info)

    def get_api_info(self, **context) -> Context:
        auth_info = kloc((self.decryptedKey if self.decryptedKey else context), self.authKey, if_null="drop")
        if self.authKey and not (auth_info and all(auth_info.values())):
            raise ValueError(INVALID_API_INFO_MSG(self.where))
        else: return auth_info


###################################################################
######################## Encrypted Spiders ########################
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
    iterateArgs = list()
    iterateCount = dict()
    iterateProduct = list()
    iterateUnit = 1
    pagination = False
    pageFrom = 1
    offsetFrom = 1
    pageUnit = 0
    pageLimit = 0
    interval = str()
    fromNow = None
    tzinfo = None
    datetimeUnit = "second"
    ssl = None
    responseType = "records"
    returnType = "records"
    mappedReturn = False
    root = list()
    groupby = list()
    groupSize = dict()
    countby = str()
    schemaInfo = SchemaInfo()
    auth = LoginSpider
    decryptedKey = dict()
    sessionCookies = True

    def __init__(self, fields: Optional[IndexLabel]=None, returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debug: Optional[Keyword]=None, extraSave: Optional[Keyword]=None, interrupt: Optional[str]=None,
                iterateUnit: Unit=0, interval: Timedelta=str(), delay: Union[float,int,Tuple[int]]=1., cookies: Optional[str]=None,
                fromNow: Optional[Unit]=None, discard=True, progress=True, where=str(), which=str(), by=str(), message=str(),
                byDate: Optional[IndexLabel]=None, fromDate: Optional[DateFormat]=None, toDate: Optional[DateFormat]=None,
                queryInfo: GoogleQueryInfo=dict(), uploadInfo: GoogleUploadInfo=dict(),
                reauth=False, audience=str(), account: Account=dict(), credentials=None,
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
    tzinfo = None
    datetimeUnit = "second"
    maxLimit = MAX_ASYNC_TASK_LIMIT
    returnType = None
    mappedReturn = False
    schemaInfo = SchemaInfo()
    auth = LoginSpider
    decryptedKey = dict()
    sessionCookies = True

    def __init__(self, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debug: Optional[Keyword]=None, extraSave: Optional[Keyword]=None, interrupt: Optional[str]=None,
                delay: Union[float,int,Tuple[int]]=1., numTasks=100, cookies: Optional[str]=None,
                byDate: Optional[IndexLabel]=None, fromDate: Optional[DateFormat]=None, toDate: Optional[DateFormat]=None,
                queryInfo: GoogleQueryInfo=dict(), uploadInfo: GoogleUploadInfo=dict(),
                reauth=False, audience=str(), account: Account=dict(), credentials=None,
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
            with (BaseLogin(cookies=self.cookies) if self.cookies else self.auth(**self.get_auth_params(**context))) as auth:
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
            with (BaseLogin(cookies=self.cookies) if self.cookies else self.auth(**self.get_auth_params(**context))) as auth:
                cookies = dict(cookies=auth.get_cookies(encode=False)) if self.sessionCookies else dict()
                async with aiohttp.ClientSession(**cookies) as session:
                    login_context = LOGIN_CONTEXT(**self.validate_account(auth, **context))
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
            api_context = TASK_CONTEXT(**self.validate_secret(**context))
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
                api_context = SESSION_CONTEXT(**self.validate_secret(**context))
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
    iterateArgs = list()
    redirectArgs = None
    iterateCount = dict()
    iterateProduct = list()
    redirectProduct = None
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
    tzinfo = None
    datetimeUnit = "second"
    ssl = None
    responseType = "records"
    returnType = "records"
    mappedReturn = False
    root = list()
    groupby = list()
    groupSize = dict()
    countby = str()
    schemaInfo = SchemaInfo()
    auth = LoginSpider
    decryptedKey = dict()
    sessionCookies = True

    def __init__(self, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debug: Optional[Keyword]=None, extraSave: Optional[Keyword]=None, interrupt: Optional[str]=None,
                iterateUnit: Unit=0, interval: Timedelta=str(), delay: Union[float,int,Tuple[int]]=1., cookies: Optional[str]=None,
                fromNow: Optional[Unit]=None, discard=True, progress=True, where=str(), which=str(), by=str(), message=str(),
                byDate: IndexLabel=list(), fromDate: Optional[DateFormat]=None, toDate: Optional[DateFormat]=None,
                numTasks=100, apiRedirect=False, redirectUnit: Optional[Unit]=None,
                queryInfo: GoogleQueryInfo=dict(), uploadInfo: GoogleUploadInfo=dict(),
                reauth=False, audience=str(), account: Account=dict(), credentials=None,
                encryptedKey: Optional[EncryptedKey]=None, decryptedKey: Optional[DecryptedKey]=None, **context):
        AsyncSpider.__init__(self, **self.from_locals(locals(), drop=ENCRYPTED_UNIQUE))
        self.set_secret(encryptedKey, decryptedKey)


###################################################################
############################# Pipeline ############################
###################################################################

class Task(TypedDict):
    def __init__(self, operator: Spider, task: str, dataName: str, dataType: Optional[TypeHint]=None,
                name=str(), fields: IndexLabel=list(), allowedFields: IndexLabel=list(), params: Optional[_KT]=None,
                derivData: Optional[_KT]=None, **context):
        name = name if name else operator.operation
        super().__init__(name=name, operator=operator, task=task, fields=fields, dataName=dataName, dataType=dataType)
        self.update_exists(allowedFields=allowedFields, params=params, derivData=derivData, context=context)


class Dag(TypedList):
    def __init__(self, *args: Union[Task,Sequence[Task]]):
        super().__init__(*args)


class Pipeline(EncryptedSession):
    __metaclass__ = ABCMeta
    asyncio = False
    operation = "pipeline"
    fields = list()
    derivFields = list()
    tzinfo = None
    datetimeUnit = "second"
    errors = dict()
    returnType = "dataframe"
    mappedReturn = False
    schemaInfo = SchemaInfo()
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
        self.errors[task[NAME]] = worker.errors
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
    tzinfo = None
    datetimeUnit = "second"
    errors = dict()
    returnType = "dataframe"
    mappedReturn = False
    schemaInfo = SchemaInfo()
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
        self.errors[task[NAME]] = worker.errors
        self.checkpoint(task[NAME], where=task, msg={"data":response}, save=response)
        return response

    @EncryptedAsyncSession.limit_request
    async def async_crawl(self, worker: AsyncSpider, **params) -> Data:
        return await worker.crawl(**params)
