from __future__ import annotations
from gscraper.base.abstract import UNIQUE_CONTEXT, TASK_CONTEXT, REQUEST_CONTEXT, LOGIN_CONTEXT, API_CONTEXT
from gscraper.base.abstract import RESPONSE_CONTEXT, PROXY_CONTEXT, REDIRECT_CONTEXT
from gscraper.base.session import Iterator, Parser, SchemaInfo, ITER_INDEX, ITER_SUFFIX, ITER_MSG
from gscraper.base.gcloud import GoogleQueryReader, GoogleUploader, GoogleQueryInfo, GoogleUploadInfo
from gscraper.base.gcloud import fetch_gcloud_authorization

from gscraper.base.types import _KT, _PASS, Arguments, Context, LogLevel, TypeHint, EncryptedKey, DecryptedKey
from gscraper.base.types import IndexLabel, Pagination, Status, Unit, DateFormat, Timedelta, Timezone
from gscraper.base.types import Records, Data, JsonData, RedirectData
from gscraper.base.types import Account
from gscraper.base.types import is_array, init_origin

from gscraper.utils import notna
from gscraper.utils.cast import cast_list, cast_tuple, cast_int, cast_datetime_format
from gscraper.utils.logs import log_encrypt, log_messages, log_response, log_client, log_data, log_exception
from gscraper.utils.map import to_array, align_array, kloc, exists_dict, chain_dict, apply_records, drop_dict
from gscraper.utils.map import exists_one, unique, rename_data, filter_data, chain_exists, between_data, re_get

from abc import ABCMeta, abstractmethod
from http.cookies import SimpleCookie
from requests.cookies import RequestsCookieJar
from urllib.parse import quote, urlencode, urlparse
import asyncio
import aiohttp
import functools
import requests
import time

from typing import Dict, List, Literal, Optional, Tuple, Type, Union
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

CONTEXT_UNIQUE = ["context", "contextFields"]
ITERATOR_UNIQUE = ["iterateUnit", "interval", "fromNow"]

GCLOUD_AUTH_UNIQUE = ["account"]
GCLOUD_QUERY_UNIQUE = ["queryInfo"]
GCLOUD_UPLOAD_UNIQUE = ["uploadInfo", "reauth", "audience", "credentials"]
GLOUD_UNIQUE = GCLOUD_AUTH_UNIQUE + GCLOUD_QUERY_UNIQUE + GCLOUD_UPLOAD_UNIQUE

SPIDER_UNIQUE = ["delay", "progress", "cookies"]
ASYNC_UNIQUE = ["numTasks", "maxLimit", "apiRedirect", "redirectUnit"]
ENCRYPTED_UNIQUE = ["encryptedKey", "decryptedKey"]

SUCCESS_RESULT = [{"response": "completed"}]


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

encrypt = lambda s=str(), count=0, *args: encrypt(
    base64.b64encode(str(s).encode("utf-8")).decode("utf-8"), count-1) if count else s
decrypt = lambda s=str(), count=0, *args: decrypt(
    base64.b64decode(str(s).encode("utf-8")).decode("utf-8"), count-1) if count else s


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
############################## Spider #############################
###################################################################

class Spider(Parser, Iterator, GoogleQueryReader, GoogleUploader):
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
    responseType = "records"
    returnType = "records"
    root = list()
    groupby = list()
    groupSize = dict()
    countby = str()
    schemaInfo = SchemaInfo()

    def __init__(self, fields: IndexLabel=list(), contextFields: IndexLabel=list(),
                iterateUnit: Unit=0, interval: Timedelta=str(), fromNow: Optional[Unit]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Literal["second","minute","hour","day"]="second",
                returnType: Optional[TypeHint]=None, logName=str(), logLevel: LogLevel="WARN", logFile=str(),
                debug: List[str]=list(), extraSave: List[str]=list(), interrupt=str(), localSave=False,
                delay: Union[float,int,Tuple[int]]=1., progress=True, cookies=str(),
                byDate: IndexLabel=list(), fromDate: Optional[DateFormat]=None, toDate: Optional[DateFormat]=None,
                reauth=False, audience=str(), account: Account=dict(), credentials=None,
                queryInfo: GoogleQueryInfo=dict(), uploadInfo: GoogleUploadInfo=dict(), **context):
        Parser.__init__(self, **self.from_locals(locals(), drop=CONTEXT_UNIQUE+ITERATOR_UNIQUE+SPIDER_UNIQUE+GLOUD_UNIQUE))
        self.iterateUnit = iterateUnit if iterateUnit else self.iterateUnit
        self.interval = interval if interval else self.interval
        self.fromNow = fromNow if notna(fromNow) else self.fromNow
        self.delay = delay
        self.progress = progress
        self.cookies = cookies
        if byDate:
            self.set_date_filter(byDate, fromDate=fromDate, toDate=toDate)
        self.update(kloc(UNIQUE_CONTEXT(**context), contextFields, if_null="pass"), self_var=True)
        if queryInfo:
            self.set_query(queryInfo, account)
        self.update_exists(uploadInfo=uploadInfo, reauth=reauth, audience=audience, account=account, credentials=credentials)
        self._disable_warnings()

    def set_date_filter(self, byDate: IndexLabel, fromDate: Optional[DateFormat]=None, toDate: Optional[DateFormat]=None):
        self.byDate = cast_list(byDate)
        self.fromDate, self.toDate = self.get_date_pair(fromDate, toDate, fromNow=(None, None))

    def _disable_warnings(self):
        if self.ssl == False:
            from urllib3.exceptions import InsecureRequestWarning
            import urllib3
            urllib3.disable_warnings(InsecureRequestWarning)

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
        return RESPONSE_CONTEXT(**context)

    ###################################################################
    ######################### Session Managers ########################
    ###################################################################

    def requests_task(func):
        @functools.wraps(func)
        def wrapper(self: Spider, *args, self_var=True, **context):
            context = TASK_CONTEXT(**(dict(self, **context) if self_var else context))
            self.checkpoint("context", where=func.__name__, msg={"args":args, "context":context})
            data = func(self, *args, **PROXY_CONTEXT(**context))
            self.checkpoint("crawl", where=func.__name__, msg={"data":data}, save=data)
            self._with_data(data, **context)
            return data
        return wrapper

    def requests_session(func):
        @functools.wraps(func)
        def wrapper(self: Spider, *args, self_var=True, **context):
            context = REQUEST_CONTEXT(**(dict(self, **context) if self_var else context))
            self.checkpoint("context", where=func.__name__, msg={"args":args, "context":context})
            with requests.Session() as session:
                data = func(self, *args, session=session, **PROXY_CONTEXT(**context))
            time.sleep(.25)
            self.checkpoint("crawl", where=func.__name__, msg={"data":data}, save=data)
            self._with_data(data, **context)
            return data
        return wrapper

    def _with_data(self, data: Data, uploadInfo: Optional[GoogleUploadInfo]=dict(),
                    alertMessage=False, alertName=str(), nateonUrl=str(), **context):
        if self.localSave: self.save_data(data, ext="dataframe")
        self.upload_data(data, uploadInfo, **context)
        self.alert_message(data, alertMessage, alertName, nateonUrl, **context)

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
    ######################### Gather Requests #########################
    ###################################################################

    @abstractmethod
    @requests_session
    def crawl(self, *args, **context) -> Data:
        args, context = self.validate_params(locals())
        return self.gather(*args, **context)

    def gather(self, *args, count: Optional[Pagination]=None, progress=True, fields: IndexLabel=list(),
                byDate: IndexLabel=list(), fromDate: Optional[dt.date]=None, toDate: Optional[dt.date]=None,
                returnType: Optional[TypeHint]=None, **context) -> Data:
        self.checkpoint("params", where="gather", msg=dict(zip(["args","context"], self.local_params(locals()))))
        if count: iterator, context = self._init_count(*args, count=count, progress=progress, **context)
        else: iterator, context = self._init_iterator(args, context, self.iterateArgs, self.iterateProduct, self.pagination)
        message = self.get_gather_message(*args, **context)
        data = [self.fetch(**__i, fields=fields, **context) for __i in tqdm(iterator, desc=message, disable=(not progress))]
        self.checkpoint("gather", where="gather", msg={"data":data}, save=data)
        return self.reduce(data, fields=fields, returnType=returnType, byDate=byDate, fromDate=fromDate, toDate=toDate, **context)

    def _init_iterator(self, args: Arguments, context: Context,
                    iterateArgs: List[_KT]=list(), iterateProduct: List[_KT]=list(),
                    pagination: Pagination=False, indexing=True, count=False) -> Tuple[List[Context],Context]:
        iterate_params = dict(iterateArgs=iterateArgs, iterateProduct=iterateProduct, pagination=pagination)
        iterator, context = self.set_iterator(*args, **iterate_params, indexing=indexing, **context)
        self.checkpoint("iterator"+("_count" if count else str()), where="init_iterator", msg={"iterator":iterator})
        return iterator, context

    def _init_count(self, *args, count: Pagination, progress=True, **context) -> Tuple[List[Context],Context]:
        if notna(context.get("size")): pass
        elif context.get("interval"):
            return self._gather_count_by_date(*args, progress=progress, **context)
        elif isinstance(count, str):
            size = self._gather_count(*args, progress=progress, **context)
            return self._init_iterator(args+(size,), context, self.iterateArgs+[count], self.iterateProduct, pagination=count)
        else: context["size"] = cast_int(self.fetch(*args, **self.get_count_context(**context)))
        return self._init_iterator(args, context, self.iterateArgs, self.iterateProduct, pagination=True)

    def _gather_count(self, *args, progress=True, **context) -> List[int]:
        count_context = self.get_count_context(**context)
        iterator, context = self._init_iterator(args, count_context, self.iterateArgs, indexing=False, count=True)
        message = self.get_count_message(*args, **context)
        size = [self.fetch(*__i, **context) for __i in tqdm(iterator, desc=message, disable=(not progress))]
        self.checkpoint("gather_count", where="gather_count", msg={"size":size})
        return list(map(cast_int, size))

    def _gather_count_by_date(self, *args, progress=True, size: _PASS=None, **context) -> Tuple[List[Context],Context]:
        size = self._gather_count(*args, progress=progress, **context)
        arguments, context = self._init_iterator(args, context, self.iterateArgs, indexing=False, count=True)
        args_pages, context = self._init_iterator(
            (arguments, size), context, ["args","size"], pagination="size", indexing=False, count=True)
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

    def reduce(self, data: List[Data], fields: IndexLabel=list(), byDate: IndexLabel=list(),
                fromDate: Optional[dt.date]=None, toDate: Optional[dt.date]=None,
                returnType: Optional[TypeHint]=None, **context) -> Data:
        data = chain_exists(data)
        if byDate: data = self.filter_date(data, byDate, fromDate, toDate)
        return filter_data(data, fields=fields, if_null="pass", return_type=returnType)

    def filter_date(self, data: Data, byDate: IndexLabel,
                    fromDate: Optional[dt.date]=None, toDate: Optional[dt.date]=None) -> Data:
        between_context = {field: (fromDate, toDate) for field in cast_tuple(byDate)}
        return between_data(data, **between_context)

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
    responseType = "records"
    returnType = "records"
    root = list()
    groupby = list()
    groupSize = dict()
    countby = str()
    schemaInfo = SchemaInfo()

    def __init__(self, fields: IndexLabel=list(), contextFields: IndexLabel=list(),
                iterateUnit: Unit=0, interval: Timedelta=str(), fromNow: Optional[Unit]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Literal["second","minute","hour","day"]="second",
                returnType: Optional[TypeHint]=None, logName=str(), logLevel: LogLevel="WARN", logFile=str(),
                debug: List[str]=list(), extraSave: List[str]=list(), interrupt=str(), localSave=False,
                delay: Union[float,int,Tuple[int]]=1., progress=True, cookies=str(),
                byDate: IndexLabel=list(), fromDate: Optional[DateFormat]=None, toDate: Optional[DateFormat]=None,
                numTasks=100, apiRedirect=False, redirectUnit: Unit=0,
                reauth=False, audience=str(), account: Account=dict(), credentials=None,
                queryInfo: GoogleQueryInfo=dict(), uploadInfo: GoogleUploadInfo=dict(), **context):
        Spider.__init__(self, **self.from_locals(locals(), drop=CONTEXT_UNIQUE+ASYNC_UNIQUE+GLOUD_UNIQUE))
        self.numTasks = cast_int(numTasks, default=MIN_ASYNC_TASK_LIMIT)
        self.apiRedirect = apiRedirect
        self.redirectUnit = exists_one(redirectUnit, self.redirectUnit, self.iterateUnit)
        self.update(kloc(UNIQUE_CONTEXT(**context), contextFields, if_null="pass"), self_var=True)
        if queryInfo:
            self.set_query(queryInfo, account)
        self.update_exists(uploadInfo=uploadInfo, reauth=reauth, audience=audience, account=account, credentials=credentials)

    ###################################################################
    ########################## Async Managers #########################
    ###################################################################

    def asyncio_task(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSpider, *args, self_var=True, **context):
            context = TASK_CONTEXT(**(dict(self, **context) if self_var else context))
            self.checkpoint("context", where=func.__name__, msg={"args":args, "context":context})
            semaphore = self.asyncio_semaphore(**context)
            data = await func(self, *args, semaphore=semaphore, **PROXY_CONTEXT(**context))
            self.checkpoint("crawl", where=func.__name__, msg={"data":data}, save=data)
            self._with_data(data, **context)
            return data
        return wrapper

    def asyncio_session(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSpider, *args, self_var=True, **context):
            context = REQUEST_CONTEXT(**(dict(self, **context) if self_var else context))
            self.checkpoint("context", where=func.__name__, msg={"args":args, "context":context})
            semaphore = self.asyncio_semaphore(**context)
            async with aiohttp.ClientSession() as session:
                data = await func(self, *args, session=session, semaphore=semaphore, **PROXY_CONTEXT(**context))
            await asyncio.sleep(.25)
            self.checkpoint("crawl", where=func.__name__, msg={"data":data}, save=data)
            self._with_data(data, **context)
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
                if ("exception" in self.debug) or ("all" in self.debug):
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

    async def gather(self, *args, count: Optional[Pagination]=None, progress=True, fields: IndexLabel=list(),
                    byDate: IndexLabel=list(), fromDate: Optional[dt.date]=None, toDate: Optional[dt.date]=None,
                    returnType: Optional[TypeHint]=None, **context) -> Data:
        self.checkpoint("params", where="gather", msg=dict(zip(["args","context"], self.local_params(locals()))))
        if count: iterator, context = await self._init_count(*args, count=count, progress=progress, **context)
        else: iterator, context = self._init_iterator(args, context, self.iterateArgs, self.iterateProduct, self.pagination)
        message = self.get_gather_message(*args, **context)
        data = await tqdm.gather(*[
                self.fetch(**__i, fields=fields, **context) for __i in iterator], desc=message, disable=(not progress))
        self.checkpoint("gather", where="gather", msg={"data":data}, save=data)
        return self.reduce(data, fields=fields, returnType=returnType, byDate=byDate, fromDate=fromDate, toDate=toDate, **context)

    async def _init_count(self, *args, count: Pagination, progress=True,
                        **context) -> Tuple[List[Context],Context]:
        if notna(context.get("size")): pass
        elif context.get("interval"):
            return await self._gather_count_by_date(*args, progress=progress, **context)
        elif isinstance(count, str):
            size = await self._gather_count(*args, progress=progress, **context)
            return self._init_iterator(args+(size,), context, self.iterateArgs+[count], self.iterateProduct, pagination=count)
        else: context["size"] = cast_int(await self.fetch(*args, **self.get_count_context(**context)))
        return self._init_iterator(args, context, self.iterateArgs, self.iterateProduct, pagination=True)

    async def _gather_count(self, *args, progress=True, **context) -> List[int]:
        count_context = self.get_count_context(**context)
        iterator, context = self._init_iterator(args, count_context, self.iterateArgs, indexing=False, count=True)
        message = self.get_count_message(*args, **context)
        size = await tqdm.gather(*[
                self.fetch(**__i, **context) for __i in iterator], desc=message, disable=(not progress))
        self.checkpoint("gather_count", where="gather_count", msg={"size":size})
        return list(map(cast_int, size))

    async def _gather_count_by_date(self, *args, progress=True, size: _PASS=None, **context) -> Tuple[List[Context],Context]:
        size = await self._gather_count(*args, progress=progress, **context)
        arguments, context = self._init_iterator(args, context, self.iterateArgs, indexing=False, count=True)
        args_pages, context = self._init_iterator(
            (arguments, size), context, ["args","size"], pagination="size", indexing=False, count=True)
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
        self.checkpoint("params", where="gather", msg=dict(zip(["args","context"], self.local_params(locals()))))
        redirectArgs = redirectArgs if is_array(self.redirectArgs) else self.iterateArgs
        context["iterateUnit"] = redirectUnit
        iterator, context = self._init_iterator(args, context, redirectArgs, self.redirectProduct, self.pagination)
        message = self.get_redirect_message(*args, **context)
        data = await tqdm.gather(*[
                self.fetch_redirect(**__i, fields=fields, **context) for __i in iterator], desc=message, disable=(not progress))
        self.checkpoint("gather", where="gather", msg={"data":data}, save=data)
        return self.reduce(data, fields=fields, returnType=returnType, byDate=byDate, fromDate=fromDate, toDate=toDate, **context)

    def get_redirect_message(self, *args, **context) -> str:
        return REDIRECT_MSG(self.operation)

    @catch_exception
    @asyncio_limit
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
            redirectUrl=redirectUrl,
            authorization=authorization,
            account=account,
            cookies=self.cookies,
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
########################## Login Spiders ##########################
###################################################################

class LoginSpider(requests.Session, Spider):
    __metaclass__ = ABCMeta
    asyncio = False
    operation = "login"
    host = str()
    where = WHERE
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
    responseType = "records"
    returnType = "records"
    root = list()
    groupby = list()
    groupSize = dict()
    countby = str()
    schemaInfo = SchemaInfo()
    auth = LoginSpider
    decryptedKey = dict()
    sessionCookies = True

    def __init__(self, fields: IndexLabel=list(), contextFields: IndexLabel=list(),
                iterateUnit: Optional[Unit]=0, interval: Optional[Timedelta]=str(), fromNow: Optional[Unit]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Literal["second","minute","hour","day"]="second",
                returnType: Optional[TypeHint]=None, logName=str(), logLevel: LogLevel="WARN", logFile=str(),
                debug: List[str]=list(), extraSave: List[str]=list(), interrupt=str(), localSave=False,
                delay: Union[float,int,Tuple[int]]=1., progress=True, cookies=str(),
                byDate: IndexLabel=list(), fromDate: Optional[DateFormat]=None, toDate: Optional[DateFormat]=None,
                reauth=False, audience=str(), account: Account=dict(), credentials=None,
                queryInfo: GoogleQueryInfo=dict(), uploadInfo: GoogleUploadInfo=dict(),
                encryptedKey: Optional[EncryptedKey]=None, decryptedKey: Optional[DecryptedKey]=None, **context):
        Spider.__init__(self, **self.from_locals(locals(), drop=ENCRYPTED_UNIQUE))
        if not self.cookies:
            self.set_secrets(encryptedKey, decryptedKey)

    def set_secrets(self, encryptedKey: Optional[EncryptedKey]=None, decryptedKey: Optional[DecryptedKey]=None):
        if isinstance(encryptedKey, str) or isinstance(decryptedKey, str):
            try: decryptedKey = json.loads(decryptedKey if decryptedKey else decrypt(encryptedKey,1))
            except JSONDecodeError: raise ValueError(INVALID_USER_INFO_MSG(self.where))
        self.update(decryptedKey=(decryptedKey if isinstance(decryptedKey, Dict) else self.decryptedKey))
        self.logger.info(log_encrypt(**self.decryptedKey, show=3))

    ###################################################################
    ########################## Login Session ##########################
    ###################################################################

    def login_session(func):
        @functools.wraps(func)
        def wrapper(self: EncryptedSpider, *args, self_var=True, **context):
            context = LOGIN_CONTEXT(**(dict(self, **context) if self_var else context))
            self.checkpoint("context", where=func.__name__, msg={"args":args, "context":context})
            with (BaseLogin(self.cookies) if self.cookies else self.auth(**dict(context, **self.decryptedKey))) as session:
                self.login(session, **context)
                if not self.sessionCookies: context["cookies"] = self.cookies
                data = func(self, *args, session=session, **PROXY_CONTEXT(**context))
            time.sleep(.25)
            self.checkpoint("crawl", where=func.__name__, msg={"data":data}, save=data)
            self._with_data(data, **context)
            return data
        return wrapper

    def login(self, auth: LoginSpider, **context):
        auth.login()
        auth.update_cookies(self.set_cookies(**context), if_exists="replace")
        self.checkpoint("login", where="login", msg={"cookies":auth.get_cookies(encode=False)})
        self.update(cookies=auth.get_cookies())

    def set_cookies(self, **context) -> Dict:
        return dict()

    ###################################################################
    ########################### API Session ###########################
    ###################################################################

    def api_session(func):
        @functools.wraps(func)
        def wrapper(self: EncryptedSpider, *args, self_var=True, **context):
            context = API_CONTEXT(**(dict(self, **context) if self_var else context))
            self.checkpoint("context", where=func.__name__, msg={"args":args, "context":context})
            with requests.Session() as session:
                data = func(self, *args, session=session,
                    **self.validate_secret(**dict(PROXY_CONTEXT(**context), **self.decryptedKey)))
            time.sleep(.25)
            self.checkpoint("crawl", where=func.__name__, msg={"data":data}, save=data)
            self._with_data(data, **context)
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
    responseType = "records"
    returnType = "records"
    root = list()
    groupby = list()
    groupSize = dict()
    countby = str()
    schemaInfo = SchemaInfo()
    auth = LoginSpider
    decryptedKey = dict()
    sessionCookies = True

    def __init__(self, fields: IndexLabel=list(), contextFields: IndexLabel=list(),
                iterateUnit: Optional[Unit]=0, interval: Optional[Timedelta]=str(), fromNow: Optional[Unit]=None,
                tzinfo: Optional[Timezone]=None, datetimeUnit: Literal["second","minute","hour","day"]="second",
                returnType: Optional[TypeHint]=None, logName=str(), logLevel: LogLevel="WARN", logFile=str(),
                debug: List[str]=list(), extraSave: List[str]=list(), interrupt=str(), localSave=False,
                delay: Union[float,int,Tuple[int]]=1., progress=True, cookies=str(),
                byDate: IndexLabel=list(), fromDate: Optional[DateFormat]=None, toDate: Optional[DateFormat]=None,
                numTasks=100, apiRedirect=False, redirectUnit: Unit=0,
                reauth=False, audience=str(), account: Account=dict(), credentials=None,
                queryInfo: GoogleQueryInfo=dict(), uploadInfo: GoogleUploadInfo=dict(),
                encryptedKey: Optional[EncryptedKey]=None, decryptedKey: Optional[DecryptedKey]=None, **context):
        AsyncSpider.__init__(self, **self.from_locals(locals(), drop=ENCRYPTED_UNIQUE))
        if not self.cookies:
            self.set_secrets(encryptedKey, decryptedKey)

    def login_session(func):
        @functools.wraps(func)
        async def wrapper(self: EncryptedAsyncSpider, *args, self_var=True, **context):
            context = LOGIN_CONTEXT(**(dict(self, **context) if self_var else context))
            self.checkpoint("context", where=func.__name__, msg={"args":args, "context":context})
            semaphore = self.asyncio_semaphore(**context)
            ssl = dict(connector=aiohttp.TCPConnector(ssl=False)) if self.ssl == False else dict()
            with (BaseLogin(self.cookies) if self.cookies else self.auth(**dict(context, **self.decryptedKey))) as auth:
                self.login(auth, **context)
                cookies = dict(cookies=auth.get_cookies(encode=False)) if self.sessionCookies else dict()
                async with aiohttp.ClientSession(**ssl, **cookies) as session:
                    if not self.sessionCookies: context["cookies"] = self.cookies
                    data = await func(self, *args, session=session, semaphore=semaphore, **PROXY_CONTEXT(**context))
            await asyncio.sleep(.25)
            self.checkpoint("crawl", where=func.__name__, msg={"data":data}, save=data)
            self._with_data(data, **context)
            return data
        return wrapper

    def api_session(func):
        @functools.wraps(func)
        async def wrapper(self: EncryptedSpider, *args, self_var=True, **context):
            context = API_CONTEXT(**(dict(self, **context) if self_var else context))
            self.checkpoint("context", where=func.__name__, msg={"args":args, "context":context})
            ssl = dict(connector=aiohttp.TCPConnector(ssl=False)) if self.ssl == False else dict()
            async with aiohttp.ClientSession(**ssl) as session:
                data = await func(self, *args, session=session,
                    **self.validate_secret(**dict(PROXY_CONTEXT(**context), **self.decryptedKey)))
            await asyncio.sleep(.25)
            self.checkpoint("crawl", where=func.__name__, msg={"data":data}, save=data)
            self._with_data(data, **context)
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
