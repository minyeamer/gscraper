from __future__ import annotations
from gscraper.base.context import UNIQUE_CONTEXT, TASK_CONTEXT, REQUEST_CONTEXT, LOGIN_CONTEXT, API_CONTEXT
from gscraper.base.context import RESPONSE_CONTEXT, UPLOAD_CONTEXT, PROXY_CONTEXT, REDIRECT_CONTEXT
from gscraper.base.session import Iterator
from gscraper.base.parser import Parser, SchemaInfo

from gscraper.base.types import _KT, _PASS, Arguments, Context, TypeHint, LogLevel
from gscraper.base.types import IndexLabel, EncryptedKey, Pagination, Status, Unit, Timedelta, Timezone
from gscraper.base.types import Records, TabularData, Data, JsonData, RedirectData
from gscraper.base.types import Account, NumericiseIgnore, BigQuerySchema, GspreadReadInfo, UploadInfo
from gscraper.base.types import not_na, is_array, is_records, init_origin, inspect_function

from gscraper.utils.cast import cast_tuple, cast_int, cast_datetime_format
from gscraper.utils.gcloud import fetch_gcloud_authorization, update_gspread, read_gspread
from gscraper.utils.gcloud import IDTokenCredentials, read_gbq, to_gbq, validate_upsert_key
from gscraper.utils.logs import log_encrypt, log_messages, log_response, log_client, log_data, log_exception, log_table
from gscraper.utils.map import re_get, unique, to_array, align_array
from gscraper.utils.map import kloc, chain_dict, drop_dict, exists_dict, cloc, apply_df
from gscraper.utils.map import exists_one, rename_data, filter_data, chain_exists, data_empty

from abc import ABCMeta, abstractmethod
from http.cookies import SimpleCookie
from requests.cookies import RequestsCookieJar
from urllib.parse import quote, urlencode, urlparse
import asyncio
import aiohttp
import functools
import requests
import time

from typing import Dict, List, Literal, Optional, Sequence, Tuple, Type, Union
from bs4 import BeautifulSoup, Tag
from json import JSONDecodeError
import json
import pandas as pd

from tqdm.auto import tqdm
import base64
import inspect
import random
import re


GET = "GET"
POST = "POST"
OPTIONS = "OPTIONS"
HEAD = "HEAD"
PUT = "PUT"
PATCH = "PATCH"
DELETE = "DELETE"

MIN_ASYNC_TASK_LIMIT = 1
MAX_ASYNC_TASK_LIMIT = 100
MAX_REDIRECT_LIMIT = 10

ARGS, PAGES = 0, 1
KEY, SHEET, FIELDS = "key", "sheet", "fields"
TABLE, PID = "table", "project_id"


###################################################################
############################# Messages ############################
###################################################################

INVALID_VALUE_MSG = lambda name, value: f"'{value}' value is not valid {name}."

GATHER_MSG = lambda which, where=str(), by=str(): \
    f"Collecting {which}{(' by '+by) if by else str()}{(' from '+where) if where else str()}"
INVALID_STATUS_MSG = lambda where: f"Response from {where} is not valid."

REDIRECT_MSG = lambda operation: f"{operation} operation is redirecting"
INVALID_REDIRECT_LOG_MSG = "Please verify that the both results and errors are in redierct returns."

INVALID_USER_INFO_MSG = lambda where=str(): f"{where} user information is not valid.".strip()

DEPENDENCY_HAS_NO_NAME_MSG = "Dependency has no operation name. Please define operation name."

WHERE, WHICH = "urls", "data"


###################################################################
############################## Urllib #############################
###################################################################

encrypt = lambda s=str(), count=0, *args: encrypt(
    base64.b64encode(str(s).encode("utf-8")).decode("utf-8"), count-1) if count else s
decrypt = lambda s=str(), count=0, *args: decrypt(
    base64.b64decode(str(s).encode("utf-8")).decode("utf-8"), count-1) if count else s


def get_cookies(session, url=None) -> str:
    if isinstance(session, aiohttp.ClientSession):
        return "; ".join([str(key)+"="+str(value) for key,value in session.cookie_jar.filter_cookies(url).items()])
    elif isinstance(session, requests.Session):
        return "; ".join([str(key)+"="+str(value) for key,value in dict(session.cookies).items()])


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
############################## Spider #############################
###################################################################

class Spider(Parser, Iterator):
    __metaclass__ = ABCMeta
    asyncio = False
    operation = "spider"
    host = str()
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
    ssl = None
    returnType = "records"
    root = list()
    groupby = list()
    groupSize = dict()
    rankby = str()
    schemaInfo = SchemaInfo()

    def __init__(self, fields: IndexLabel=list(), contextFields: IndexLabel=list(),
                iterateUnit: Unit=0, interval: Timedelta=str(), fromNow: Optional[Unit]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Literal["second","minute","hour","day"]="second",
                returnType: Optional[TypeHint]=None, logName=str(), logLevel: LogLevel="WARN", logFile=str(),
                debug: List[str]=list(), extraSave: List[str]=list(), interrupt=str(), localSave=False,
                delay: Union[float,int,Tuple[int]]=1., progress=True, cookies=str(),
                queryInfo: GspreadReadInfo=dict(), **context):
        Parser.__init__(
            self, fields=fields, tzinfo=tzinfo, datetimeUnit=datetimeUnit, returnType=returnType,
            logName=logName, logLevel=logLevel, logFile=logFile,
            debug=debug, extraSave=extraSave, interrupt=interrupt, localSave=localSave)
        Iterator.__init__(self, iterateUnit=iterateUnit, interval=interval, fromNow=fromNow)
        self.delay = delay
        self.progress = progress
        self.cookies = cookies
        if context: self.update(kloc(UNIQUE_CONTEXT(**context), contextFields, if_null="pass"))
        self.set_query(queryInfo, **context)
        self._disable_warnings()

    def _disable_warnings(self):
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
    ########################## Local Variable #########################
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
        self.alert_message(data, **context)

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
        return self.reduce(data, fields=fields, returnType=returnType, **context)

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

    def reduce(self, data: List[Data], fields: IndexLabel=list(),
                    returnType: Optional[TypeHint]=None, **context) -> Data:
        return filter_data(chain_exists(data), fields=fields, if_null="pass", return_type=returnType)

    ###################################################################
    ########################## Fetch Request ##########################
    ###################################################################

    @abstractmethod
    @Parser.catch_exception
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
    def request_source(self, method: str, url: str, session: requests.Session, messages: Dict=dict(),
                        params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                        allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                        valid: Optional[Status]=None, invalid: Optional[Status]=None, features="html.parser", **context) -> Tag:
        with session.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return BeautifulSoup(response.text, features)

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

    @Parser.catch_exception
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

    @Parser.catch_exception
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
########################### Async Spider ##########################
###################################################################

class AsyncSpider(Spider):
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
    iterateProduct = list()
    redirectProduct = list()
    iterateUnit = 1
    maxLimit = MAX_ASYNC_TASK_LIMIT
    redirectUnit = 1
    redirectLimit = MAX_REDIRECT_LIMIT
    pagination = False
    pageUnit = 0
    pageLimit = 0
    interval = str()
    fromNow = None
    tzinfo = None
    datetimeUnit = "second"
    ssl = None
    returnType = "records"
    root = list()
    groupby = list()
    groupSize = dict()
    rankby = str()
    schemaInfo = SchemaInfo()

    def __init__(self, fields: IndexLabel=list(), contextFields: IndexLabel=list(),
                iterateUnit: Unit=0, interval: Timedelta=str(), fromNow: Optional[Unit]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Literal["second","minute","hour","day"]="second",
                returnType: Optional[TypeHint]=None, logName=str(), logLevel: LogLevel="WARN", logFile=str(),
                debug: List[str]=list(), extraSave: List[str]=list(), interrupt=str(), localSave=False,
                delay: Union[float,int,Tuple[int]]=1., progress=True, message=str(), cookies=str(),
                numTasks=100, apiRedirect=False, redirectUnit: Unit=0, queryInfo: GspreadReadInfo=dict(), **context):
        Spider.__init__(
            self, fields=fields, tzinfo=tzinfo, datetimeUnit=datetimeUnit, returnType=returnType,
            iterateUnit=iterateUnit, interval=interval, fromNow=fromNow,
            logName=logName, logLevel=logLevel, logFile=logFile,
            debug=debug, extraSave=extraSave, interrupt=interrupt, localSave=localSave,
            delay=delay, progress=progress, message=message, cookies=cookies)
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
        async def wrapper(self: AsyncSpider, *args, self_var=True, uploadInfo: Optional[UploadInfo]=dict(),
                        alertMessage=False, alertName=str(), nateonUrl=str(), **context):
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
        async def wrapper(self: AsyncSpider, *args, self_var=True, uploadInfo: Optional[UploadInfo]=dict(),
                        alertMessage=False, alertName=str(), nateonUrl=str(), **context):
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
        return self.reduce(data, fields=fields, returnType=returnType, **context)

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
            return await response.json(encoding=encoding, content_type=None)

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
    async def request_source(self, method: str, url: str, session: aiohttp.ClientSession, messages: Dict=dict(),
                            params=None, encode: Optional[bool]=None, data=None, json=None, headers=None, cookies=str(),
                            allow_redirects=True, validate=False, exception: Literal["error","interrupt"]="interrupt",
                            valid: Optional[Status]=None, invalid: Optional[Status]=None, encoding=None,
                            features="html.parser", **context) -> Tag:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, ssl=self.ssl) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return BeautifulSoup(await response.text(encoding=encoding), features)

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
        return self.reduce(data, fields=fields, returnType=returnType, **context)

    def get_redirect_message(self, *args, **context) -> str:
        return REDIRECT_MSG(self.operation)

    @catch_exception
    @asyncio_limit
    @gcloud_authorized
    async def fetch_redirect(self, redirectUrl: str, authorization: str, session: Optional[aiohttp.ClientSession]=None,
                            account: Account=dict(), **context) -> Records:
        data = self._filter_redirect_data(redirectUrl, authorization, account, **context)
        response = self.request_json(POST, redirectUrl, session, json=data, headers=dict(Authorization=authorization))
        self.checkpoint("redirect", where="fetch_redirect", msg={"response":response}, save=response, ext="json")
        return self._parse_redirect(response, **context)

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
    host = str()
    where = str()
    ssl = None

    @abstractmethod
    def __init__(self, logName=str(), logLevel: LogLevel="WARN", logFile=str(),
                debug: List[str]=list(), extraSave: List[str]=list(), interrupt=str(), cookies=str(), **context):
        requests.Session.__init__(self)
        if cookies: self.cookies.update(decode_cookies(cookies))
        self.set_logger(logName, logLevel, logFile, debug, extraSave, interrupt)
        self._disable_warnings()

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

    @encode_messages
    def request_source(self, method: str, url: str, origin: str, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None,
                    headers=None, cookies=str(), allow_redirects=True, features="html.parser", **context) -> Tag:
        with self.request(method, url, **messages, allow_redirects=allow_redirects, verify=self.ssl) as response:
            self.logger.info(log_response(response, url=url, origin=origin))
            self.logger.debug(log_messages(cookies=dict(self.cookies), origin=origin, dump=self.logJson))
            return BeautifulSoup(response.text, features)

    def log_response_text(self, response: requests.Response, origin: str):
        self.checkpoint(origin+"_text", where=origin, msg={"response":response.text}, save=response, ext="response")


class BaseLogin(LoginSpider):
    operation = "login"

    def __init__(self, cookies=str()):
        super().__init__(cookies=(cookies if isinstance(cookies, str) else str()))

    def login(self):
        return


###################################################################
######################## Encrypted Spiders ########################
###################################################################

class EncryptedSpider(Spider):
    __metaclass__ = ABCMeta
    asyncio = False
    operation = "spider"
    host = str()
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
    ssl = None
    returnType = "records"
    returnType = "records"
    root = list()
    groupby = list()
    groupSize = dict()
    rankby = str()
    schemaInfo = SchemaInfo()
    auth = LoginSpider
    decryptedKey = dict()

    def __init__(self, fields: IndexLabel=list(), contextFields: IndexLabel=list(),
                iterateUnit: Optional[Unit]=0, interval: Optional[Timedelta]=str(), fromNow: Optional[Unit]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Literal["second","minute","hour","day"]="second",
                returnType: Optional[TypeHint]=None, logName=str(), logLevel: LogLevel="WARN", logFile=str(),
                debug: List[str]=list(), extraSave: List[str]=list(), interrupt=str(), localSave=False,
                delay: Union[float,int,Tuple[int]]=1., progress=True, message=str(), cookies=str(),
                queryInfo: GspreadReadInfo=dict(), encryptedKey: EncryptedKey=str(), decryptedKey=str(), **context):
        Spider.__init__(
            self, fields=fields, tzinfo=tzinfo, datetimeUnit=datetimeUnit, returnType=returnType,
            iterateUnit=iterateUnit, interval=interval, fromNow=fromNow,
            logName=logName, logLevel=logLevel, logFile=logFile,
            debug=debug, extraSave=extraSave, interrupt=interrupt, localSave=localSave,
            delay=delay, progress=progress, message=message, cookies=cookies,
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
    host = str()
    where = WHERE
    which = WHICH
    by = str()
    fields = list()
    iterateArgs = list()
    redirectArgs = None
    iterateProduct = list()
    redirectProduct = None
    iterateUnit = 1
    maxLimit = MAX_ASYNC_TASK_LIMIT
    redirectUnit = 1
    redirectLimit = MAX_REDIRECT_LIMIT
    pagination = False
    pageUnit = 0
    pageLimit = 0
    interval = str()
    fromNow = None
    tzinfo = None
    datetimeUnit = "second"
    ssl = None
    returnType = "records"
    root = list()
    groupby = list()
    groupSize = dict()
    rankby = str()
    schemaInfo = SchemaInfo()
    auth = LoginSpider
    decryptedKey = dict()
    sessionCookies = True

    def __init__(self, fields: IndexLabel=list(), contextFields: IndexLabel=list(),
                iterateUnit: Optional[Unit]=0, interval: Optional[Timedelta]=str(), fromNow: Optional[Unit]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Literal["second","minute","hour","day"]="second",
                returnType: Optional[TypeHint]=None, logName=str(), logLevel: LogLevel="WARN", logFile=str(),
                debug: List[str]=list(), extraSave: List[str]=list(), interrupt=str(), localSave=False,
                delay: Union[float,int,Tuple[int]]=1., progress=True, message=str(), cookies=str(),
                numTasks=100, apiRedirect=False, redirectUnit: Unit=0, queryInfo: GspreadReadInfo=dict(),
                encryptedKey: EncryptedKey=str(), decryptedKey=str(), **context):
        AsyncSpider.__init__(
            self, fields=fields, tzinfo=tzinfo, datetimeUnit=datetimeUnit, returnType=returnType,
            iterateUnit=iterateUnit, interval=interval, fromNow=fromNow,
            logName=logName, logLevel=logLevel, logFile=logFile,
            debug=debug, extraSave=extraSave, interrupt=interrupt, localSave=localSave,
            delay=delay, progress=progress, message=message, cookies=cookies,
            contextFields=contextFields, queryInfo=queryInfo,
            numTasks=numTasks, apiRedirect=apiRedirect, redirectUnit=redirectUnit, **context)
        if not self.cookies:
            self.set_secrets(encryptedKey, decryptedKey)

    def login_session(func):
        @functools.wraps(func)
        async def wrapper(self: EncryptedAsyncSpider, *args, self_var=True, uploadInfo: Optional[UploadInfo]=dict(),
                            alertMessage=False, alertName=str(), nateonUrl=str(), **context):
            context = LOGIN_CONTEXT(**(dict(self.__dict__, **context) if self_var else context))
            self.checkpoint("context", where=func.__name__, msg={"context":context})
            semaphore = self.asyncio_semaphore(**context)
            ssl = dict(connector=aiohttp.TCPConnector(ssl=False)) if self.ssl == False else dict()
            with (BaseLogin(self.cookies) if self.cookies else self.auth(**dict(context, **self.decryptedKey))) as auth:
                self.login(auth, **context)
                cookies = dict(cookies=dict(auth.cookies)) if self.sessionCookies else dict()
                async with aiohttp.ClientSession(**ssl, **cookies) as session:
                    if not self.sessionCookies: context["cookies"] = self.cookies
                    data = await func(self, *args, session=session, semaphore=semaphore, **context)
            await asyncio.sleep(.25)
            self.checkpoint("crawl", where=func.__name__, msg={"data":data}, save=data)
            self._with_data(data, uploadInfo, **context)
            return data
        return wrapper

    def api_session(func):
        @functools.wraps(func)
        async def wrapper(self: EncryptedSpider, *args, self_var=True, uploadInfo: Optional[UploadInfo]=dict(),
                        alertMessage=False, alertName=str(), nateonUrl=str(), **context):
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
