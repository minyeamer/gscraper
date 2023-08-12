from __future__ import annotations
from .cast import cast_str, cast_int, cast_datetime, cast_date
from .date import now, get_date, get_datetime, date_range
from .excel import update_gspread, read_gspread, clear_gspead, to_excel_date
from .logs import CustomLogger, dumps_map, unraw
from .map import chain_exists, filter_data, re_get, df_exist
from .map import exists_one, cast_get, list_get
from .parse import parse_cookies

from abc import ABCMeta, abstractmethod
from urllib.parse import unquote, urlparse
import asyncio
import aiohttp
import requests

from typing import Callable, Dict, List, Optional, Union
from tqdm.auto import tqdm
from collections import defaultdict
import datetime as dt
import functools
import json
import logging
import pandas as pd
import re
import sys
import time
import traceback

from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
try: import pandas_gbq
except: pass

from typing import Any, Dict, List, Optional, TypeVar, Union
import base64
import json
import logging
import pandas as pd
import sys
import os

_KT = TypeVar("_KT")
_VT = TypeVar("_VT")

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "sec-ch-ua": '"Chromium";v="114", "Google Chrome";v="114", "Not;A=Brand";v="8"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
}

DEFAULT_LIMIT = 100
REDIRECT_LIMIT = 10

REDIRECT_MSG = lambda operation: f"{operation} operation is redirecting"

GCLOUD_ACCOUNT = "env/gcloud.json"

parse_path = lambda url: re_get(f"(.*)(?={urlparse(url).path})", url) if urlparse(url).path else url

encrypt = lambda s=str(), count=0, *args: encrypt(
    base64.b64encode(str(s).encode("utf-8")).decode("utf-8"), count-1) if count else s
decrypt = lambda s=str(), count=0, *args: decrypt(
    base64.b64decode(str(s).encode("utf-8")).decode("utf-8"), count-1) if count else s


def get_cookies(session, url=None) -> str:
    if isinstance(session, aiohttp.ClientSession):
        return "; ".join([str(key)+"="+str(value) for key,value in session.cookie_jar.filter_cookies(url).items()])
    elif isinstance(session, requests.Session):
        return "; ".join([str(key)+"="+str(value) for key,value in dict(session.cookies).items()])


def get_headers(authority=str(), referer=str(), cookies=str(), host=str(),
                origin: Optional[Union[bool,str]]=False, **kwargs) -> Dict:
    headers = HEADERS.copy()
    if authority: headers["Authority"] = urlparse(authority).hostname
    if referer: headers["referer"] = referer
    if host: headers["Host"] = urlparse(host).hostname
    if origin: headers["Origin"] = parse_path(origin if isinstance(origin, str) else (authority if authority else host))
    if cookies: headers["Cookie"] = cookies
    return dict(headers, **kwargs)


def read_json(file: str) -> Dict[str,str]:
    if os.path.exists(file):
        with open(file, "r", encoding="utf-8") as f:
            return json.loads(f.read())
    else: return dict()


def fetch_gcloud_authorization(audience=str(), account: Optional[Dict]=dict(), file=str(), **kwargs) -> str:
    account = account if account else read_json(file if file else GCLOUD_ACCOUNT)
    audience = audience if audience else account.get("audience")
    credentials = service_account.IDTokenCredentials.from_service_account_info(account, target_audience=audience)
    auth_session = AuthorizedSession(credentials)
    auth_session.get(audience)
    return "Bearer "+credentials.token


def request_gcloud(url=str(), data: Optional[Dict]=dict(), account: Optional[Dict]=dict(),
                file=str(), operation=str(), **kwargs) -> requests.Response:
    url = url if url else account.get("audience")
    authorization = fetch_gcloud_authorization(url)
    data = data if data else read_json(file if file else GCLOUD_ACCOUNT).get(operation, dict())
    headers = get_headers(url, Authorization=authorization)
    return requests.post(url, json=data, headers=headers)


###################################################################
############################### Base ##############################
###################################################################

class CustomDict(dict):
    def __init__(self, **kwargs):
        super().__init__(kwargs)

    def copy(self, instance: Optional[object]=None) -> Any:
        if instance: return instance.__class__(**self.__dict__)
        else: return self.__class__(**self.__dict__)

    def get(self, __key: Union[_KT,List[_KT]], default=None, cast=False) -> Union[Any,Dict,List,str]:
        __m, __type = self.__dict__, default.__class__
        if isinstance(__key, List): return list_get(__m, __key, default, cast)
        else: return cast_get(__m, __key, __type, default) if cast else __m.get(__key, default)

    def update(self, __m: Optional[Dict]=dict(), inplace=True, return_true=False, **kwargs) -> Union[bool,Dict]:
        if not inplace: self = self.copy()
        for key, value in dict(kwargs, **__m).items():
            setattr(self, key, value)
        super().update(self.__dict__)
        return exists_one(return_true, inplace, self)

    def __getitem__(self, __key: _KT) -> _VT:
        if isinstance(__key, List):
            return [super().__getitem__(key) for key in __key]
        else:
            return super().__getitem__(__key)

    def __setitem__(self, __key: _KT, __value: _VT):
        setattr(self, __key, __value)
        super().__setitem__(__key, __value)


###################################################################
############################# Spiders #############################
###################################################################

class Spider(CustomDict):
    __metaclass__ = ABCMeta
    asyncio = False
    operation = str()
    errors = defaultdict(list)
    returnType = "records"

    def __init__(self, operation=str(), filter: Optional[List[str]]=list(), filterArgs: Callable[[],Dict]=None,
                logName=str(), logLevel="WARN", logFile=str(), logErrors=False, errorArgs=list(), errorKwargs=list(),
                delay=1., numTasks=100, maxLoops=1, progress=tqdm, debug=False,
                queryKey=str(), querySheet=str(), queryFields: Optional[List[str]]=list(),
                apiRedirect=False, redirectUnit=1, redirectErrors=False, localSave=True, extraSave=False, **kwargs):
        self.operation = self.operation
        self.initTime = now()
        self.filter = filter
        self.logName = logName if logName else "spider"
        self.logLevel = int(logLevel) if str(logLevel).isdigit() else logging.getLevelName(str(logLevel).upper())
        self.logFile = logFile
        self.logJson = bool(logFile)
        self.logger = CustomLogger(name=self.logName, level=self.logLevel, file=self.logFile)
        self.logErrors = logErrors
        self.errorArgs = errorArgs
        self.errorKwargs = errorKwargs
        self.delay = delay
        self.numTasks = numTasks
        self.maxLoops = maxLoops
        self.tqdm = progress
        self.debug = debug
        self.apiRedirect = apiRedirect
        self.redirectUnit = redirectUnit
        self.redirectErrors = redirectErrors
        self.localSave = localSave
        self.extraSave = extraSave
        self.update(filterArgs(**kwargs) if filterArgs else kwargs)
        if queryKey and querySheet:
            self.read_query(queryKey, querySheet, queryFields)

    ###################################################################
    ######################### Session Managers ########################
    ###################################################################

    def requests_session(func):
        @functools.wraps(func)
        def wrapper(self: Spider, *args, **kwargs):
            if not (args and kwargs): kwargs = self.__dict__.copy()
            with requests.Session() as session:
                results = func(self, *args, session=session, **kwargs)
            time.sleep(.25)
            self.upload_data(results, **kwargs)
            return results
        return wrapper

    def response_filter(func):
        @functools.wraps(func)
        def wrapper(self: Spider, *args, filter: Optional[List]=list(), **kwargs):
            filter = filter if filter else self.filter
            results = func(self, *args, filter=filter, **kwargs)
            return filter_data(results, filter, return_type=self.returnType)
        return wrapper

    def log_errors(func):
        @functools.wraps(func)
        def wrapper(self: Spider, *args, **kwargs):
            try: return func(self, *args, **kwargs)
            except KeyboardInterrupt as interrupt:
                raise interrupt
            except Exception as exception:
                name = f"{func.__name__}({self.__class__.__name__})"
                if self.logErrors: self.log_arguments(args, kwargs)
                if self.debug: raise exception
                else: self.logger.error(log_exception(name, *args, json=self.logJson, **kwargs))
        return wrapper

    def log_arguments(self, args, kwargs):
        for idx, key in enumerate(self.errorArgs):
            if idx < len(args) and key: self.errors[key].append(args[idx])
        for key in self.errorKwargs:
            if key in kwargs: self.errors[key].append(kwargs[key])

    def requests_limit(func):
        @functools.wraps(func)
        def wrapper(self: Spider, *args, **kwargs):
            if self.delay: time.sleep(self.delay)
            return func(self, *args, **kwargs)
        return wrapper

    ###################################################################
    ############################ Interfaces ###########################
    ###################################################################

    @abstractmethod
    @requests_session
    def crawl(self, query: List[str], **kwargs) -> Union[List[Dict],pd.DataFrame]:
        results = self.gather(query, **kwargs)
        self.upload_data(results, **kwargs)
        return results

    @response_filter
    def gather(self, query: List[str], message=str(), **kwargs) -> List[Dict]:
        message = message if message else "example tqdm message"
        return [self.fetch(value, **kwargs) for value in self.tqdm(query, desc=message)]

    @log_errors
    @requests_limit
    def fetch(self, query: str, session: Optional[requests.Session]=None, **kwargs) -> Union[List[Dict],Dict]:
        url = f"https://example.com?query={query}"
        headers = get_headers(url)
        self.logger.debug(log_messages(headers=headers, json=self.logJson))
        with session.get(url, headers=headers) as response:
            self.logger.info(log_response(response, url=url, query=query))
            results = json.loads(response.text)
        self.logger.info(log_results(results, query=query))
        return results

    ###################################################################
    ############################# Methods #############################
    ###################################################################

    def read_query(self, queryKey: str, querySheet: str, queryFields: Union[List,str], **kwargs):
        data = read_gspread(queryKey, querySheet, numericise=False)
        self.logger.info(log_table(data, json=self.logJson))
        if isinstance(queryFields, str): queryFields = queryFields.split(',')
        for queryName in queryFields:
            if queryName in data:
                self.update(**{queryName:data[queryName].tolist()})

    def upload_data(self, data: Union[List[Dict],pd.DataFrame], gsKey=str(), gsSheet=str(), gsMode="append", gsHistory=str(),
                    gsIndex=str(), gsIndexKey=str(), gsRange=str(), gbqTable=str(), gbqPid=str(), gbqMode="append",
                    gbqSchema=None, gbqIndex=str(), gbqIndexMode="replace", gbqIndexSchema=None, account=dict(), **kwargs):
        if (gsKey and gsSheet) or (gbqTable and gbqPid) or (gbqIndex and gbqPid):
            data = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
            if data.empty: return
        if gsKey and gsSheet:
            self.upload_gspread(gsKey, gsSheet, data.copy(), gsMode, gsHistory, gsIndex, gsIndexKey, gsRange, account, **kwargs)
        if gbqTable and gbqPid:
            self.upload_gbq(gbqTable, gbqPid, data.copy(), gbqMode, gbqSchema, **kwargs)
        if gbqIndex and gbqPid:
            self.index_gbq(gbqIndex, gbqPid, data.copy(), gbqIndexMode, gbqIndexSchema, **kwargs)

    def extra_save(self, results: pd.DataFrame, prefix=str(), extraSave=False, rename=dict(), **kwargs):
        if not extraSave: return
        file = f"{str(prefix)}_{now('%Y%m%d%H%M%S')}.xlsx"
        results.rename(columns=rename).to_excel(file, index=False)

    ###################################################################
    ############################ Google API ###########################
    ###################################################################

    def gcloud_authorized(func):
        @functools.wraps(func)
        async def wrapper(self: Spider, *args, redirectUrl=str(), authorization=str(), account=dict(), **kwargs):
            if not authorization:
                authorization = fetch_gcloud_authorization(redirectUrl, account, **kwargs)
            return await func(
                self, *args, redirectUrl=redirectUrl, authorization=authorization, account=account, **kwargs)
        return wrapper

    ###################################################################
    ########################## Google Spread ##########################
    ###################################################################

    @log_errors
    def upload_gspread(self, key: str, sheet: str, data: pd.DataFrame, mode="append", history=str(),
                        index=str(), index_key=str(), cell=str(), account: Optional[Dict]=dict(), **kwargs):
        history = self.read_gs_history(key, history, account)
        if df_exist(history):
            data = self.map_gs_history(data, history)
        data = self.map_gs_data(data)
        self.logger.info(log_table(data, key=key, sheet=sheet, json=self.logJson))
        if mode == "replace":
            clear_gspead(key, sheet, account, header=False)
        cell = cell if cell else self.set_gs_range(key, index, index_key, mode, account, add=2)
        update_gspread(data, key, sheet, account, cell=cell)

    def read_gs_history(self, key: str, sheet: str, rename: Dict[str,str]=dict(),
                        date_cols: Optional[List[str]]=list(), datetime_cols: Optional[List[str]]=list(),
                        str_cols: Optional[List[str]]=list(), account: Optional[Dict]=dict(), **kwargs) -> pd.DataFrame:
        if not (key and sheet): return pd.DataFrame()
        history = read_gspread(key, sheet, account).rename(columns=rename)
        self.logger.info(log_table(history, key=key, sheet=sheet, json=self.logJson))
        for column in date_cols:
            if column in history: history[column] = history[column].apply(cast_date)
        for column in datetime_cols:
            if column in history: history[column] = history[column].apply(cast_datetime)
        for column in str_cols:
            if column in history: history[column] = history[column].apply(cast_str)
        return history

    def map_gs_history(self, data: pd.DataFrame, history: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return data[[column for column in history.columns if column in data.columns]]

    def map_gs_data(self, data: pd.DataFrame, date_cols: List[str]=list(), **kwargs) -> pd.DataFrame:
        for column in date_cols:
            if column in data: data[column] = data[column].apply(to_excel_date)
        return data

    def set_gs_range(self, key: str, sheet: str, index_key: str, mode: str,
                    account: Optional[Dict]=dict(), add=0, **kwargs) -> str:
        if mode == "replace": return "A2"
        elif key and sheet and index_key: return str()
        rows = read_gspread(key, sheet, account).loc[0,index_key]
        self.logger.info({"key":key, "sheet":sheet, "index":index_key, "rows":rows})
        return 'A'+cast_int(rows)+add

    ###################################################################
    ######################### Google Bigquery #########################
    ###################################################################

    @log_errors
    def upload_gbq(self, table: str, project_id: str, data: pd.DataFrame, mode="append",
                    schema: Optional[List[Dict[str,str]]]=None, **kwargs):
        schema = schema if schema else self.get_gbq_schema(**kwargs)
        data = self.map_gbq_data(data, filter=[field.get("name") for field in schema])
        self.logger.info(log_table(data, table=table, projectId=project_id, mode=mode, schema=schema, json=self.logJson))
        data.to_gbq(table, project_id=project_id, reauth=False, if_exists=mode, table_schema=schema, progress_bar=True)

    def get_gbq_schema(self, **kwargs) -> List[Dict[str,str]]:
        return self.get("gbqSchema", list())

    def map_gbq_data(self, data: pd.DataFrame, filter: Optional[List[str]]=list(), **kwargs) -> pd.DataFrame:
        return data[[column for column in filter if column in data.columns]]

    @log_errors
    def index_gbq(self, table: str, project_id: str, data: pd.DataFrame, mode="replace",
                    schema: Optional[List[Dict[str,str]]]=None, **kwargs):
        schema = schema if schema else self.get_gbq_index(**kwargs)
        key = [field.get("name") for field in schema if field.get("mode") == "REQUIRED"][0]
        index = self.read_gbq_index(table, project_id)
        data = self.map_gbq_index(data, key, schema)
        index = self.merge_gbq_index(data, index, key, schema)
        self.logger.info(log_table(index, table=table, projectId=project_id, mode=mode, schema=schema, json=self.logJson))
        index.to_gbq(table, project_id=project_id, reauth=False, if_exists=mode, table_schema=schema, progress_bar=True)

    def read_gbq_index(self, table: str, project_id: str, **kwargs) -> pd.DataFrame:
        return pd.read_gbq(table, project_id)

    def get_gbq_index(self, **kwargs) -> List[Dict[str,str]]:
        return self.get("gbqIndexSchema", list())

    def map_gbq_index(self, data: pd.DataFrame, key: str,
                        schema: Optional[List[Dict[str,str]]]=None, **kwargs) -> pd.DataFrame:
        schema = schema if schema else self.get_gbq_index()
        fields = [field.get("name") for field in schema]
        data = data[[column for column in data.columns if column in fields]].copy()
        for column in data.columns.tolist():
            data[column] = data[str(column)].apply(lambda x: None if x == str() else x)
        return data[data[key].notna()].drop_duplicates(key)

    def merge_gbq_index(self, data: pd.DataFrame, index: pd.DataFrame, key: str,
                        schema: Optional[List[Dict[str,str]]]=None, **kwargs) -> pd.DataFrame:
        fields = [field.get("name") for field in schema]
        index = data.set_index(key).combine_first(index.set_index(key)).reset_index()
        return index[[column for column in fields if column in index.columns]]


class AsyncSpider(Spider):
    __metaclass__ = ABCMeta
    asyncio = True
    operation = str()
    errors = defaultdict(list)
    returnType = "records"

    ###################################################################
    ########################## Async Managers #########################
    ###################################################################

    def asyncio_session(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSpider, *args, **kwargs):
            if not (args and kwargs): kwargs = self.__dict__.copy()
            semaphore = self.asyncio_semaphore(**kwargs)
            async with aiohttp.ClientSession() as session:
                results = await func(self, *args, session=session, semaphore=semaphore, **kwargs)
            await asyncio.sleep(.25)
            self.upload_data(results, **kwargs)
            return results
        return wrapper

    def asyncio_semaphore(self, numTasks: Optional[int]=None, apiRedirect=False, **kwargs) -> asyncio.Semaphore:
        numTasks = cast_int(numTasks, (REDIRECT_LIMIT if apiRedirect else DEFAULT_LIMIT))
        return asyncio.Semaphore(numTasks)

    def asyncio_filter(func):
        @functools.wraps(func)
        async def wrapper(self: Spider, *args, filter: Optional[List]=list(), **kwargs):
            filter = filter if filter else self.filter
            results = await func(self, *args, filter=filter, **kwargs)
            return filter_data(results, filter, return_type=self.returnType)
        return wrapper

    def asyncio_errors(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSpider, *args, maxLoops: Optional[int]=1, **kwargs):
            for i in range(max(1,maxLoops)):
                try: return await func(self, *args, **kwargs)
                except KeyboardInterrupt as interrupt:
                    raise interrupt
                except Exception as exception:
                    if (i+1) == maxLoops:
                        name = f"{func.__name__}({self.__class__.__name__})"
                        if self.logErrors: self.log_arguments(args, kwargs)
                        if self.debug: raise exception
                        else: self.logger.error(log_exception(name, *args, json=self.logJson, **kwargs))
                    elif self.delay: await asyncio.sleep(self.delay)
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
    ############################ Interfaces ###########################
    ###################################################################

    @abstractmethod
    @asyncio_session
    async def crawl(self, startDate: dt.date, endDate: dt.date, dateRange=True,
                    apiRedirect=False, **kwargs) -> Union[List[Dict],pd.DataFrame]:
        startDate, endDate = get_date(startDate, default=1), get_date(endDate, default=1)
        context = dict(startDate=startDate, endDate=endDate, dateRange=dateRange, **kwargs)
        results = await (self.redirect(**context) if apiRedirect else self.gather(**context))
        return results

    @asyncio_filter
    async def gather(self, startDate: dt.date, endDate: dt.date, dateRange=True, message=str(), **kwargs) -> List[Dict]:
        if dateRange: return chain_exists(await self.tqdm.gather(
            *[self.fetch(startDate=date, endDate=date, **kwargs) for date in date_range(startDate, endDate)], desc=message))
        else: return await self.fetch(str(startDate), str(endDate), **kwargs)

    @Spider.gcloud_authorized
    async def redirect(self, startDate: dt.date, endDate: dt.date, message=str(), **kwargs) -> List[Dict]:
        message = message if message else REDIRECT_MSG(self.operation)
        return chain_exists(await self.tqdm.gather(
            *[self.fetch_redirect(startDate=date, endDate=date, **kwargs)
                for date in date_range(startDate, endDate)], desc=message))

    def redirect_range(self, *args: List, **kwargs) -> List[List]:
        if len(args) == 1: return [args[0][i:i+self.redirectUnit] for i in range(0, len(args[0]), self.redirectUnit)]
        if (len(args) > 1) and (len(set(map(len,args))) == 1):
            return zip(*[[arg[i:i+self.redirectUnit] for i in range(0, len(arg), self.redirectUnit)] for arg in args])
        return list()

    @asyncio_errors
    @asyncio_limit
    async def fetch(self, startDate: str, endDate: str,
                    session: Optional[aiohttp.ClientSession]=None, **kwargs) -> Union[List[Dict],Dict]:
        url = f"https://example.com?startDate={startDate}&endDate={endDate}"
        headers = get_headers(url)
        self.logger.debug(log_messages(headers=headers, json=self.logJson))
        async with session.get(url, headers=headers) as response:
            self.logger.info(await log_client(response, url=url, startDate=startDate, endDate=endDate))
            results = json.loads(await response.text())
        self.logger.info(log_results(results, startDate=startDate, endDate=endDate))
        return results

    ###################################################################
    ############################# Methods #############################
    ###################################################################

    @asyncio_errors
    @asyncio_limit
    @Spider.gcloud_authorized
    async def fetch_redirect(self, redirectUrl: str, authorization: str, account: Optional[Dict]=dict(),
                            redirectError=False, session: Optional[aiohttp.ClientSession]=None,
                            **context) -> List[Dict]:
        data = self.filter_redirect(redirectUrl, authorization, account, **context)
        headers = get_headers(redirectUrl, Authorization=authorization)
        self.logger.debug(log_messages(data=data, headers=headers, json=self.logJson))
        async with session.post(redirectUrl, json=data, headers=headers) as response:
            self.logger.info(await log_client(response, url=redirectUrl))
            results = json.loads(await response.text())
        results = self.parse_redirect(results, **context)
        self.logger.debug(log_results(results))
        if redirectError and not len(results): raise ValueError("Redirect function returns empty list")
        return results

    def filter_redirect(self, redirectUrl: str, authorization: str, account: Optional[Dict]=dict(),
                        filterRedirect: Callable[[],Dict]=None, **kwargs):
        return dict(
            redirectUrl=redirectUrl,
            authorization=authorization,
            account=account,
            **(filterRedirect(**kwargs) if filterRedirect else dict()))

    def parse_redirect(self, data: List[Dict], **kwargs) -> List[Dict]:
        if self.redirectErrors:
            if not isinstance(data, dict): return list()
            errors = data.get("errors", dict())
            for key, values in errors.items():
                self.errors[key] += values
            data = data.get("results", list())
        return self.parse_redirect_results(data, **kwargs)

    def parse_redirect_results(self, data: List[Dict], date_cols: List[str]=list(),
                                datetime_cols: List[str]=list(), **kwargs) -> List[Dict]:
        if not isinstance(data, list): return list()
        if not (date_cols or datetime_cols): return data
        for idx, record in enumerate(data):
            for key, value in record.items():
                if key in datetime_cols: data[idx][key] = get_datetime(value)
                elif key in date_cols: data[idx][key] = get_date(value)
        return data

###################################################################
######################## Encrypted Spiders ########################
###################################################################

class LoginSpider(requests.Session, Spider):
    __metaclass__ = ABCMeta
    asyncio = False
    operation = str()
    errors = defaultdict(list)
    where = str()

    def __init__(self, logName=str(), logLevel="WARN", logFile=str(), **kwargs):
        super().__init__()
        self.operation = self.operation
        self.logName = logName if logName else "login"
        self.logLevel = int(logLevel) if str(logLevel).isdigit() else logging.getLevelName(str(logLevel).upper())
        self.logFile = logFile
        self.logJson = bool(logFile)
        self.logger = CustomLogger(name=self.logName, level=self.logLevel, file=self.logFile)

    @abstractmethod
    def login(self):
        pass

    def login_required(func):
        @functools.wraps(func)
        def wrapper(self: LoginSpider, *args, **kwargs):
            if not self.get_cookies():
                self.login()
            if not self.get_cookies():
                raise KeyboardInterrupt(f"{self.where} login information is required".strip())
            return func(self, *args, **kwargs)
        return wrapper

    def get_cookies(self, **kwargs) -> str:
        return parse_cookies(self.cookies)


class EncryptedSpider(Spider):
    __metaclass__ = ABCMeta
    authType = LoginSpider
    idKey = "userid"
    pwKey = "passwd"
    extraKeys = list()
    where = str()

    def __init__(self, operation=str(), select: Optional[List[str]]=list(), extraSelect: Optional[List[str]]=list(),
                configs: Optional[List[Dict]]=list(), logFile=str(), logLevel="WARN", logErrors=False,
                errorArgs=list(), errorKwargs=list(), delay=1., numTasks=100, maxLoops=1, progress=tqdm,
                debug=False, confirm=True, queryKey=str(), querySheet=str(), queryFields: Optional[List[str]]=list(),
                apiRedirect=False, redirectUnit=1, redirectErrors=False, localSave=True, extraSave=False,
                cookies=str(), encryptedKey=str(), **kwargs):
        super().__init__(operation, select, extraSelect, configs, logFile, logLevel, logErrors, errorArgs, errorKwargs,
                        delay, numTasks, maxLoops, progress, debug, confirm, queryKey, querySheet, queryFields, apiRedirect,
                        redirectUnit, redirectErrors, localSave, extraSave, **kwargs)
        self.cookies = cookies
        self.read_secrets(encryptedKey)

    def read_secrets(self, encryptedKey: str):
        self.encryptedKey = encryptedKey
        decryptedKey = {key:self.get(key,str()) for key in [self.idKey, self.pwKey]+self.extraKeys}
        self.decryptedKey = json.loads(decrypt(encryptedKey, 1)) if encryptedKey else decryptedKey
        if not encryptedKey and sum(list(map(len,decryptedKey.values()))):
            self.update(encryptedKey=encrypt(json.dumps(decryptedKey, ensure_ascii=False, default=1),1))
        self.logger.info(log_encrypt(**self.decryptedKey))

    def login_required(func):
        @functools.wraps(func)
        def wrapper(self: EncryptedSpider, *args, cookies=str(), **kwargs):
            if not self.cookies:
                if cookies: self.update(cookies=cookies)
                else: self.login()
            if not self.cookies:
                raise KeyboardInterrupt(f"{self.where} login information is required".strip())
            return func(self, *args, cookies=self.cookies, **kwargs)
        return wrapper

    def login(self, update=True) -> str:
        auth = self.authType(logFile=self.logFile, logLevel=self.logLevel, **self.decryptedKey)
        auth.login()
        cookies = auth.get_cookies()
        auth.close()
        return self.update(cookies=cookies) if update else cookies

    def validate_response(self, response: requests.Response):
        if response.status_code == 401:
            raise KeyboardInterrupt(f"{self.where} login information is not valid".strip())


class EncryptedAsyncSpider(AsyncSpider, EncryptedSpider):
    __metaclass__ = ABCMeta
    authType = LoginSpider
    idKey = "userid"
    pwKey = "passwd"
    extraKeys = list()
    where = str()

    def __init__(self, operation=str(), select: Optional[List[str]]=list(), extraSelect: Optional[List[str]]=list(),
                configs: Optional[List[Dict]]=list(), logFile=str(), logLevel="WARN", logErrors=False,
                errorArgs=list(), errorKwargs=list(), delay=1., numTasks=100, maxLoops=1, progress=tqdm,
                debug=False, confirm=True, queryKey=str(), querySheet=str(), queryFields: Optional[List[str]]=list(),
                apiRedirect=False, redirectUnit=1, redirectErrors=False, localSave=True, extraSave=False,
                cookies=str(), encryptedKey=str(), **kwargs):
        super().__init__(operation, select, extraSelect, configs, logFile, logLevel, logErrors, errorArgs, errorKwargs,
                        delay, numTasks, maxLoops, progress, debug, confirm, queryKey, querySheet, queryFields, apiRedirect,
                        redirectUnit, redirectErrors, localSave, extraSave, **kwargs)
        self.cookies = cookies
        self.read_secrets(encryptedKey)

    def login_required(func):
        @functools.wraps(func)
        async def wrapper(self: EncryptedAsyncSpider, *args, cookies=str(), **kwargs):
            if not self.cookies:
                if cookies: self.update(cookies=cookies)
                else: self.login()
            if not self.cookies:
                raise KeyboardInterrupt(f"{self.where} login information is required".strip())
            return await func(self, *args, cookies=self.cookies, **kwargs)
        return wrapper

    def validate_response(self, response: aiohttp.ClientResponse):
        if response.status == 401:
            raise KeyboardInterrupt(f"{self.where} login information is not valid".strip())


###################################################################
############################# Parsers #############################
###################################################################

class Parser(CustomDict):
    __metaclass__ = ABCMeta
    operation = str()
    errors = defaultdict(list)

    def __init__(self, logName=str(), logLevel="WARN", logFile=str(), **kwargs):
        super().__init__()
        self.logName = logName if logName else "parser"
        self.logLevel = int(logLevel) if str(logLevel).isdigit() else logging.getLevelName(str(logLevel).upper())
        self.logFile = logFile
        self.logJson = bool(logFile)
        self.logger = CustomLogger(name=self.logName, level=self.logLevel, file=self.logFile)

    @abstractmethod
    def parse(self, response: str, **kwargs) -> List[Dict]:
        return response


###################################################################
############################ Pipelines ############################
###################################################################

class Pipeline(Spider):
    __metaclass__ = ABCMeta
    dependencies = list()
    returnType = "dataframe"

    @abstractmethod
    def crawl(self, query: List[str], **kwargs) -> Union[List[Dict],pd.DataFrame]:
        results = self.gather(query, **kwargs)
        results = self.map_reduce(results, **kwargs)
        self.upload_data(results, **kwargs)
        return results

    @Spider.response_filter
    def gather(self, query: List[str], **kwargs) -> Union[List[Dict],pd.DataFrame]:
        crawler1 = Spider(query=query, **kwargs)
        results1 = crawler1.crawl(**crawler1.__dict__)
        crawler2 = Spider(query=query, **kwargs)
        results2 = crawler2.crawl(**crawler2.__dict__)
        return results1 + results2

    def map_reduce(self, data: Union[List[Dict],pd.DataFrame], **kwargs) -> Union[List[Dict],pd.DataFrame]:
        return data


class AsyncPipeline(AsyncSpider):
    __metaclass__ = ABCMeta
    dependencies = list()
    returnType = "dataframe"

    @abstractmethod
    async def crawl(self, query: List[str], **kwargs) -> Union[List[Dict],pd.DataFrame]:
        results = await self.gather(query, **kwargs)
        results = self.map_reduce(results, **kwargs)
        self.upload_data(results, **kwargs)
        return results

    @abstractmethod
    @AsyncSpider.asyncio_filter
    async def gather(self, query: List[str], **kwargs) -> Union[List[Dict],pd.DataFrame]:
        crawler1 = AsyncSpider(query=query, **kwargs)
        results1 = await crawler1.crawl(**crawler1.__dict__)
        crawler2 = AsyncSpider(query=query, **kwargs)
        results2 = await crawler2.crawl(**crawler2.__dict__)
        return results1 + results2

    def map_reduce(self, data: Union[List[Dict],pd.DataFrame], **kwargs) -> Union[List[Dict],pd.DataFrame]:
        return data


###################################################################
########################### Log Managers ##########################
###################################################################

def log_encrypt(**kwargs: Dict[str,str]) -> Dict[str,str]:
    encrypt = lambda string, show=3: str(string[:show]).ljust(len(string),'*')
    return dict(**{key:encrypt(value) for key, value in kwargs.items()})


def log_messages(data: Optional[Dict]=None, params: Optional[Dict]=None,
                headers: Optional[Dict]=None, cookies=str(), json=True, **kwargs) -> Dict[str,Dict]:
    dumps = lambda struct: dumps_map(struct) if json else str(struct)
    data = {"data":dumps(data)} if data else dict()
    params = {"params":dumps(params)} if params else dict()
    headers = {"headers":dumps(headers)} if headers else dict()
    cookies = {"cookies":dumps(cookies)} if cookies else dict()
    kwargs = {key:dumps(values) for key,values in kwargs.items()}
    return dict(**kwargs, **data, **params, **headers, **cookies)


def log_response(response: requests.Response, url: str, **kwargs) -> Dict[str,str]:
    try: length = len(response.text)
    except: length = None
    return dict(**kwargs, **{"url":unquote(url), "status":response.status_code, "contents-length":length})


async def log_client(response: aiohttp.ClientResponse, url: str, **kwargs) -> Dict[str,str]:
    try: length = len(await response.text())
    except: length = None
    return dict(**kwargs, **{"url":unquote(url), "status":response.status, "contents-length":length})


def log_results(results: List, **kwargs) -> Dict[str,str]:
    try: length = len(results)
    except: length = None
    return dict(**kwargs, **{"data-length":length})


def log_exception(func: str, *args, json=True, **kwargs) -> Dict[str,str]:
    dumps = lambda struct: (dumps_map(struct) if json else str(struct))[:1000]
    error = ''.join(traceback.format_exception(*sys.exc_info()))
    error = unraw(error) if json else error
    return dict(func=func, args=dumps(args), kwargs=dumps(kwargs), error=error)


def log_table(data: pd.DataFrame, schema: Optional[Dict]=None, json=True, **kwargs) -> Dict[str,str]:
    dumps = lambda struct: dumps_map(struct) if json else str(struct)
    schema = {"schema":dumps(schema)} if schema else dict()
    try: shape = data.shape
    except: shape = (0,0)
    return dict(**kwargs, **{"table-shape":str(shape)}, **schema)
