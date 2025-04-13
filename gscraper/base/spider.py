from __future__ import annotations
from gscraper.base.abstract import CustomDict, OptionalDict, TypedRecords, Query, INVALID_OBJECT_TYPE_MSG
from gscraper.base.abstract import UNIQUE_CONTEXT, TASK_CONTEXT, REQUEST_CONTEXT
from gscraper.base.abstract import SESSION_CONTEXT, PROXY_CONTEXT, REDIRECT_CONTEXT

from gscraper.base.session import BaseSession, Iterator, Parser
from gscraper.base.session import UserInterrupt, ForbiddenError, ParseError
from gscraper.base.session import Info, Schema, Field, Flow, Process
from gscraper.base.session import ITER_INDEX, PAGE_ITERATOR, iter_task
from gscraper.base.gcloud import GoogleUploader, GoogleQueryList, GoogleUploadList
from gscraper.base.gcloud import Account, fetch_gcloud_authorization, read_gcloud

from gscraper.base.types import _KT, _VT, _PASS, Arguments, Context, LogLevel, TypeHint, EncryptedKey, DecryptedKey
from gscraper.base.types import IndexLabel, Keyword, Logic, Status, Unit, Range, DateFormat, Timedelta, Timezone
from gscraper.base.types import Records, Data, MappedData, JsonData, RedirectData, Pagination
from gscraper.base.types import is_datetime_type, is_date_type, is_array, is_int_array

from gscraper.utils.cast import cast_list, cast_tuple, cast_int, cast_datetime_format
from gscraper.utils.date import get_random_seconds, get_date_pair, get_datetime_pair
from gscraper.utils.logs import log_object, log_encrypt, log_messages, log_response, log_client, log_data
from gscraper.utils.map import to_array, align_array, transpose_array, unit_array, get_scala, union, inter, diff
from gscraper.utils.map import kloc, hier_get, notna_dict, drop_dict, split_dict, traversal_dict
from gscraper.utils.map import vloc, apply_records, to_dataframe, write_df, convert_data, filter_data, regex_get
from gscraper.utils.map import exists_one, unique, chain_exists, between_data, concat, encrypt, decrypt, read_table
from gscraper.utils.map import convert_dtypes as _convert_dtypes

from gscraper.utils.alert import AlertInfo, NaverBot, format_map
from gscraper.utils.alert import format_date as format_alert_date
from gscraper.utils.alert import format_date_range as format_alert_date_range
from gscraper.utils.alert import format_value as format_alert_value
from gscraper.utils.request import get_cookies, decode_cookies, encode_params
from gscraper.utils.request import apply_nest_asyncio, close_async_loop, asyncio_run

from http.cookies import SimpleCookie
from requests.cookies import RequestsCookieJar
from tqdm.auto import tqdm
import abc
import asyncio
import aiohttp
import copy
import functools
import inspect
import requests
import time

from typing import Any, Callable, Coroutine, Dict, Iterable, List, Literal
from typing import Optional, Sequence, Tuple, Type, Union
from numbers import Real
from bs4 import BeautifulSoup, Tag
from json import JSONDecodeError
import datetime as dt
import json
import pandas as pd
import re


RETRY, RETURN = 0, 1

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

TIME_UNIQUE = ["tzinfo", "countryCode", "datetimeUnit"]
LOG_UNIQUE = ["logName", "logLevel", "logFile", "localSave", "debugPoint", "killPoint", "extraSave"]

ITERATOR_UNIQUE = ["iterateUnit", "interval", "fromNow"]
UPLOAD_UNIQUE = ["queryList", "uploadList", "alertInfo", "account"]

FILTER_UNIQUE = ["fields", "ranges", "returnType"]
REQUEST_UNIQUE = ["numRetries", "delay", "cookies"]

SPIDER_UNIQUE = ["discard", "progress", "numTasks"]
REDIRECT_UNIQUE = ["apiRedirect", "redirectUnit", "redirectUrl", "authorization", "account"]

ENCRYPTED_UNIQUE = ["encryptedKey", "decryptedKey"]
PIPELINE_UNIQUE = ["globalProgress", "asyncProgress", "taskProgress"]

PROXY_LOG = ["logLevel", "logFile", "debugPoint", "killPoint", "extraSave"]

WORKER_CONFIG = (
    FILTER_UNIQUE + TIME_UNIQUE + PROXY_LOG + REQUEST_UNIQUE +
    SPIDER_UNIQUE + REDIRECT_UNIQUE + ENCRYPTED_UNIQUE + PIPELINE_UNIQUE)
WORKER_EXTRA = ["message", "where", "which", "by"]

NAME, OPERATOR, TASK, DATANAME, DATATYPE = "name", "operator", "task", "dataName", "dataType"
FIELDS, ALLOWED, PARAMS, DERIV, CONTEXT = "fields", "allowedFields", "params", "derivData", "context"

SUCCESS_RESULT = [{"response": "completed"}]

class RequestInterrupt(requests.ConnectionError):
    ...

class AuthenticationError(ValueError):
    ...

class ParameterError(ValueError):
    ...

SPIDER_KILL = (UserInterrupt, RequestInterrupt, ParameterError)
SPIDER_ERROR = (ForbiddenError, ParseError)

ENCRYPTED_SESSION_KILL = (UserInterrupt, AuthenticationError, ParameterError)
ENCRYPTED_SPIDER_KILL = (UserInterrupt, RequestInterrupt, AuthenticationError, ParameterError)


###################################################################
############################# Messages ############################
###################################################################

INVALID_VALUE_MSG = lambda name, value: f"'{value}' value is not valid {name}."

GATHER_MSG = lambda action, which, by=str(), where=str(): \
    f"{action} {which}{(' '+by) if by else str()}{(' from '+where) if where else str()}"
INVALID_STATUS_MSG = lambda where: f"Response from {where} is not valid."
MISSING_CLIENT_SESSION_MSG = "Async client session does not exist."

REDIRECT_MSG = lambda operation: f"{operation} operation is redirecting"

INVALID_USER_INFO_MSG = lambda where=str(): f"{where} user information is not valid.".strip()

PIPELINE_DEFAULT_GLOBAL_MSG = "Processing pipeline works"
PIPELINE_DEFAULT_ASYNC_MSG = "Processing pipeline works asynchronously"

ACTION, WHERE, WHICH = "Collecting", "urls", "data"
FIRST_PAGE, NEXT_PAGE = "of first page", "of next pages"

AUTH_KEY = "Auth Key"
SAVE_SHEET = "Data"


###################################################################
########################### Range Filter ##########################
###################################################################

class RangeContext(OptionalDict):
    def __init__(self, field: _KT, type: Optional[TypeHint]=None, left: Optional[Real]=None, right: Optional[Real]=None,
                inclusive: Literal["both","neither","left","right"]="both", null=False, tzinfo=None):
        left, right = self.validate_value(left, right, type, tzinfo)
        super().__init__(field=field,
            optional=dict(left=left, right=right, inclusive=inclusive, null=null))

    def validate_value(self, left: Optional[Real]=None, right: Optional[Real]=None,
                        type: Optional[TypeHint]=None, tzinfo=None) -> Tuple[Real,Real]:
        if not type: return left, right
        elif is_datetime_type(type): return get_datetime_pair(left, right, if_null=(None, None), tzinfo=tzinfo)
        elif is_date_type(type): return get_date_pair(left, right, if_null=(None, None), tzinfo=tzinfo)
        else: return left, right


class RangeFilter(TypedRecords):
    dtype = RangeContext
    typeCheck = True

    def __init__(self, *args: RangeContext, tzinfo=None):
        list.__init__(self, [self.validate_dtype(__i, tzinfo) for __i in args])

    def validate_dtype(self, __object, tzinfo=None) -> Dict:
        if (not self.dtype) or isinstance(__object, self.dtype): return __object
        elif isinstance(__object, Dict):
            if tzinfo is not None: __object["tzinfo"] = tzinfo
            return self.dtype(**__object)
        else: self.raise_dtype_error(__object)


###################################################################
########################## Upload Session #########################
###################################################################

class UploadSession(GoogleUploader):
    __metaclass__ = abc.ABCMeta
    operation = "session"
    alertStatus = None

    def __init__(self, queryList: GoogleQueryList=list(), uploadList: GoogleUploadList=list(), alertInfo: AlertInfo=dict(),
                account: Optional[Account]=None, **context):
        CustomDict.__init__(self, UNIQUE_CONTEXT(**context))
        self.set_query(queryList, account)
        self.set_upload_info(uploadList, alertInfo, account)

    def set_upload_info(self, uploadList: GoogleUploadList=list(), alertInfo: AlertInfo=dict(), account: Optional[Account]=None):
        self.update_exists(uploadList=uploadList, alertInfo=alertInfo, account=account)

    ###################################################################
    ############################ Alert Text ###########################
    ###################################################################

    def send_alert_text(self, text: str, alertInfo: AlertInfo):
        self.checkpoint("alert", where="alert_message", msg={"text":text})
        with NaverBot(**alertInfo["authInfo"]) as bot:
            self.alertStatus = bot.send_text(text, **alertInfo["botInfo"])
        self.log_alert(self.alertStatus, text)

    def map_alert_data(self, data: Data, **context) -> Data:
        return data

    def format_alert_text(self, data: Data, textFormat: str, textHeader: Optional[str]=None, textFooter: Optional[str]=None,
                        dateFormat: Optional[DateFormat]=None, rangeFormat: Optional[str]=None,
                        name: Optional[str]=None, **context) -> str:
        textInfo = dict(dateFormat=dateFormat, rangeFormat=rangeFormat, name=name)
        if "{header}" in textFormat:
            header = self.format_alert_header(data, textHeader=textHeader, **textInfo, **context)
            textFormat = textFormat.replace("{header}", header)
        if "{footer}" in textFormat:
            footer = self.format_alert_footer(data, textFooter=textFooter, **textInfo, **context)
            textFormat = textFormat.replace("{footer}", footer)
        mapping = dict(filter(None,
            [self.format_alert_key_value(data, __key, **textInfo, **context)
                for __key in set(regex_get(r"\{([^}]+)\}", textFormat, indices=[]))]))
        return format_map(textFormat, mapping)

    def format_alert_header(self, data: Data, textHeader: Optional[str]=None, **context) -> str:
        return textHeader if isinstance(textHeader, str) else str()

    def format_alert_footer(self, data: Data, textFooter: Optional[str]=None, **context) -> str:
        if isinstance(textFooter, str): return textFooter
        elif self.uploadStatus: return '\n\n'+self.format_upload_status(**context)
        else: return str()

    @BaseSession.ignore_exception
    def format_alert_key_value(self, data: Data, __key: str, name=str(), **context) -> Tuple[_KT,_VT]:
        if __key == "name":
            return __key, (name if isinstance(name, str) else self.operation)
        elif re.match(r"^(date|datetime)\(.+\)$", __key):
            dateFunc, path = regex_get(r"^(\w+)\((.+)\)$", __key, groups=[0,1])
            return __key, self.format_alert_date(context.get(path), dateFunc, **context)
        elif re.match(r"^(daterange|timerange)\([^,]+,[^,]+\)$", __key):
            dateFunc, start, end = regex_get(r"^(\w+)\(([^,]+),([^,]+)\)$", __key, groups=[0,1,2])
            return __key, self.format_alert_date_range(context.get(start), context.get(end), dateFunc, **context)
        else: return __key.rsplit(':', maxsplit=1)[0], self.format_alert_value(data, __key, **context)

    def format_alert_value(self, data: Data, __key: str, **context) -> _VT:
        return format_alert_value(data, __key, **context)

    def format_alert_date(self, __object, __func: str, dateFormat: Optional[str]=None, busdate=False, **context) -> str:
        if isinstance(__object, (dt.date,dt.datetime)): __datetime = __object
        elif __func == "date": __datetime = self.get_date(__object, if_null=None, busdate=busdate)
        elif __func == "datetime": __datetime = self.get_datetime(__object, if_null=None)
        else: __datetime = None
        return format_alert_date(__datetime, (dateFormat if dateFormat else __func), tzinfo=self.tzinfo)

    def format_alert_date_range(self, __start, __end, __func: str, dateFormat: Optional[Union[str,Tuple[str,str]]]=None,
                                rangeFormat: Optional[str]=None, busdate=False, **context) -> str:
        if __func == "daterange": __start, __end = self.get_date_pair(__start, __end, if_null=None, busdate=busdate)
        elif __func == "timerange": __start, __end = self.get_datetime_pair(__start, __end, if_null=None)
        else: __start, __end = None, None
        return format_alert_date_range(__start, __end, (dateFormat if dateFormat else __func), rangeFormat, tzinfo=self.tzinfo)

    def log_alert(self, status: Status, text: str, limit=100):
        if isinstance(status, List):
            status = ', '.join([f"({__i}) {__s}" for __i, __s in enumerate(status, start=1)])
        else: status = str(status)
        preview = text[:limit]+"..." if len(text) > limit else text
        self.logger.info(log_object({"status":status, "text":preview}))


###################################################################
######################### Request Session #########################
###################################################################

class RequestSession(UploadSession):
    __metaclass__ = abc.ABCMeta
    asyncio = False
    operation = "session"
    fields = list()
    ranges = list()
    numRetries = 0
    delay = 1.
    cookies = str()
    interruptType = tuple()
    killType = tuple()
    errorType = tuple()
    returnType = None
    mappedReturn = False
    info = Info()

    def __init__(self, fields: Optional[IndexLabel]=None, ranges: Optional[RangeFilter]=None, returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, countryCode=str(), datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debugPoint: Optional[Keyword]=None, killPoint: Optional[Keyword]=None, extraSave: Optional[Keyword]=None,
                numRetries: Optional[int]=None, delay: Optional[Range]=None, cookies: Optional[str]=None,
                queryList: GoogleQueryList=list(), uploadList: GoogleUploadList=list(), alertInfo: AlertInfo=dict(), **context):
        self.set_filter_variables(fields, ranges, returnType)
        self.set_init_time(tzinfo, countryCode, datetimeUnit)
        self.set_logger(logName, logLevel, logFile, localSave, debugPoint, killPoint, extraSave)
        self.set_request_variables(numRetries, delay, cookies)
        UploadSession.__init__(self, queryList, uploadList, alertInfo, **context)

    def set_filter_variables(self, fields: Optional[IndexLabel]=None, ranges: Optional[RangeFilter]=None,
                            returnType: Optional[TypeHint]=None):
        self.fields = fields if fields is not None else self.fields
        self.ranges = RangeFilter(*ranges, tzinfo=self.tzinfo) if isinstance(ranges, Sequence) and ranges else RangeFilter()
        self.returnType = returnType if returnType is not None else self.returnType

    def set_request_variables(self, numRetries: Optional[int]=None, delay: Optional[Range]=None, cookies: Optional[str]=None):
        self.numRetries = numRetries if isinstance(numRetries, int) else self.numRetries
        self.delay = delay if delay is not None else self.delay
        self.cookies = cookies if cookies is not None else self.cookies

    ###################################################################
    ######################## Context Validator ########################
    ###################################################################

    def validate_context(self, locals: Dict=dict(), drop: _KT=list(), **context) -> Context:
        if locals:
            context = self.from_locals(locals, drop=drop, **context)
        queryMap = self.get_query_map(key="name", value=[], if_null="pass")
        for __key, __map in queryMap.items():
            if __key in context:
                context[__key] = self.validate_variable(context[__key], **__map)
        return context

    def validate_variable(self, __object: Any, type: Optional[Type]=None, default=None,
                        iterable=False, arr_options=dict(), enum=None, **context) -> Any:
        if is_date_type(type):
            __object = [self.get_date(__e) for __e in __object] if is_array(__object) else self.get_date(__object)
        if isinstance(enum, Dict):
            __object = [enum.get(__e,__e) for __e in __object] if is_array(__object) else enum.get(__object,__object)
            enum = tuple(enum.values())
        if isinstance(enum, Tuple):
            if is_array(__object): __object = [(__e if __e in enum else None) for __e in __object]
            else: __object = __object if __object in enum else default
        if iterable:
            __object = to_array(__object, **arr_options)
            if not __object: __object = default
        return __object

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

    def with_session(func):
        @functools.wraps(func)
        def wrapper(self: RequestSession, *args, **context):
            with requests.Session() as session:
                return func(self, *args, session=session, **context)
        return wrapper

    def limit_request(func):
        @functools.wraps(func)
        def wrapper(self: RequestSession, *args, **context):
            try: return func(self, *args, **context)
            finally: self.sleep()
        return wrapper

    ###################################################################
    ########################## Async Managers #########################
    ###################################################################

    def with_async_loop(func):
        @functools.wraps(func)
        def wrapper(self: RequestSession, *args, loop: _PASS=None, **context):
            loop, close = self.run_async_loop()
            try: return func(self, *args, loop=loop, **context)
            finally:
                if close: close_async_loop(loop)
        return wrapper

    def run_async_loop(self) -> Tuple[asyncio.AbstractEventLoop,bool]:
        try:
            loop, close = asyncio.get_running_loop(), False
            apply_nest_asyncio()
        except RuntimeError:
            loop, close = asyncio.new_event_loop(), True
            asyncio.set_event_loop(loop)
        return loop, close

    def close_async_loop(loop: asyncio.AbstractEventLoop):
        close_async_loop(loop)

    def asyncio_run(self, func: Coroutine, loop: asyncio.AbstractEventLoop, args: Arguments=tuple(), kwargs: Context=dict()) -> Any:
        return asyncio_run(func, loop, args, kwargs)

    ###################################################################
    ########################### Log Managers ##########################
    ###################################################################

    def retry_request(func):
        @functools.wraps(func)
        def wrapper(self: RequestSession, *args, numRetries: Optional[int]=None, retryCount: _PASS=None, **context):
            numRetries = numRetries if isinstance(numRetries, int) else self.numRetries
            for count in reversed(range(numRetries+1)):
                try: return func(self, *args, numRetries=numRetries, retryCount=count, **context)
                except Exception as exception:
                    exitCode, value = self.catch_exception(exception, func, args, context, numRetries, retryCount=count)
                    if exitCode == RETURN: return value
                    elif exitCode == RETRY: continue
                    else: raise exception
            return func(self, *args, **context)
        return wrapper

    def catch_exception(self, exception: Exception, func: Callable, args: Arguments, context: Context,
                        numRetries=0, retryCount=0) -> Tuple[int,Any]:
        if self.is_interrupt(exception):
            return self.interrupt(*args, exception=exception, func=func, numRetries=numRetries, retryCount=retryCount, **context)
        elif self.is_kill(exception):
            raise exception
        elif (retryCount == 0) or self.is_error(exception):
            return RETURN, self.pass_exception(exception, func, msg={"args":args, "context":context})
        else: return RETRY, None

    def interrupt(self, *args, exception: Exception, func: Callable, retryCount=0, **context) -> Tuple[int,Any]:
        if retryCount == 0:
            return RETURN, self.pass_exception(exception, func, msg={"args":args, "context":context})
        else: return RETRY, None

    def is_interrupt(self, exception: Exception) -> bool:
        return isinstance(exception, self.interruptType)

    def is_kill(self, exception: Exception) -> bool:
        return isinstance(exception, UserInterrupt) or isinstance(exception, self.killType)

    def is_error(self, exception: Exception) -> bool:
        return isinstance(exception, ForbiddenError) or isinstance(exception, self.errorType)

    def sleep(self, delay: Optional[Range]=None, minimum=0., maximum=None):
        delay = self.get_delay(delay)
        if isinstance(minimum, (float,int)): delay = max(delay, minimum)
        if isinstance(maximum, (float,int)): delay = min(delay, maximum)
        if delay: time.sleep(delay)

    def get_delay(self, delay: Optional[Range]=None) -> Union[float,int]:
        delay = delay if delay is not None else self.delay
        if isinstance(delay, (float,int)): return delay
        elif isinstance(delay, Sequence):
            if len(delay) > 1: return get_random_seconds(*delay[:2])
            else: return self.get_delay(delay[0] if delay else None)
        else: return 0.

    def get_retry_delay(self, delay: Optional[Range]=None, numRetries=0, retryCount=0, increment=1., **context) -> Union[float,int]:
        count = numRetries - retryCount
        return self.get_delay(delay) + (count * increment)

    ###################################################################
    ########################## Validate Data ##########################
    ###################################################################

    def validate_data(func):
        @functools.wraps(func)
        def wrapper(self: RequestSession, *args, fields: Union[List,Dict]=list(), returnType: Optional[TypeHint]=None, **context):
            data = func(self, *args, fields=fields, returnType=returnType, **context)
            if self.mappedReturn:
                return traversal_dict(data, fields, apply=self.filter_data, dropna=True, returnType=returnType)
            else: return self.filter_data(data, fields, returnType=returnType)
        return wrapper

    def filter_data(self, data: Data, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None,
                    default=None, if_null: Literal["drop","pass"]="pass") -> Data:
        data = convert_data(data, return_type=returnType)
        data = filter_data(data, fields=cast_list(fields), default=default, if_null=if_null)
        return data

    ###################################################################
    ########################## Validate Range #########################
    ###################################################################

    def validate_range(func):
        @functools.wraps(func)
        def wrapper(self: RequestSession, *args, ranges: RangeFilter=list(), **context):
            data = func(self, *args, **context)
            if self.mappedReturn:
                return traversal_dict(data, ranges, apply=self.filter_range, dropna=False)
            else: return self.filter_range(data, ranges)
        return wrapper

    def filter_range(self, data: Data, ranges: RangeFilter=list()) -> Data:
        if isinstance(ranges, Sequence):
            for range in ranges:
                data = between_data(data, **range)
        return data

    ###################################################################
    ########################### Arrange Data ##########################
    ###################################################################

    def arrange_data(func):
        @functools.wraps(func)
        def wrapper(self: RequestSession, *args, fields: Union[List,Dict]=list(), ranges: RangeFilter=list(),
                    returnType: Optional[TypeHint]=None, convert_dtypes=False, sortby=str(), reset_index=False, **context):
            data = func(self, *args, fields=fields, ranges=ranges, returnType=returnType, **context)
            reset_index = reset_index or (len(sortby) > 0) or (len(ranges) > 0)
            params = dict(returnType=returnType, convert_dtypes=convert_dtypes, sortby=sortby, reset_index=reset_index)
            if self.mappedReturn:
                return traversal_dict(data, fields, apply=self.filter_data_plus, dropna=True, **params)
            else: return self.filter_data_plus(data, fields, **params)
        return wrapper

    def filter_data_plus(self, data: Data, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None,
                        convert_dtypes=False, sortby=str(), reset_index=False) -> Data:
        data = self.filter_data(data, fields=fields, returnType=returnType)
        if not isinstance(data, pd.DataFrame): return data
        if convert_dtypes: data = _convert_dtypes(data)
        if sortby: data = data.sort_values(sortby)
        if reset_index: data = data.reset_index(drop=True)
        return data

    ###################################################################
    ############################ With Data ############################
    ###################################################################

    def with_data(self, data: Data, uploadList: GoogleUploadList=list(), alertInfo: AlertInfo=dict(), func=str(), **context):
        self.checkpoint("crawl", where=func, msg={"data":data}, save=data)
        self.save_result(data, **context)
        self.upload_result(data, uploadList, **context)
        self.alert_result(data, alertInfo, **context)

    @BaseSession.ignore_exception
    def save_result(self, data: Data, **context):
        if not self.localSave: return
        file_name = self.get_save_name()+'_'+self.now("%Y%m%d%H%M%S")+".xlsx"
        if self.mappedReturn and isinstance(data, Dict):
            self.save_mapped_result(data, file_name, **context)
        else: self.save_dataframe(data, file_name, self.get_save_sheet())

    def save_mapped_result(self, data: MappedData, file_name: str, key: _PASS=None, **context):
        with pd.ExcelWriter(file_name, engine="openpyxl") as writer:
            for __key, __data in data.items():
                sheet_name = self.get_save_sheet(__key)
                __data = self.rename_save_data(to_dataframe(__data), key=__key, **context)
                try: write_df(__data, writer, sheet_name=sheet_name, index=False)
                except: continue

    @BaseSession.ignore_exception
    def upload_result(self, data: Data, uploadList: GoogleUploadList=list(), **context):
        if not uploadList: return
        self.uploadStatus = dict()
        if self.mappedReturn and isinstance(data, Dict):
            for uploadContext in uploadList:
                if not isinstance(uploadContext, Dict): continue
                elif uploadContext.get(NAME) not in data: continue
                else: self.upload_data(data[uploadContext[NAME]], [uploadContext], **context)
        else: self.upload_data(data, uploadList, **context)

    @BaseSession.ignore_exception
    def alert_result(self, data: Data, alertInfo: AlertInfo=dict(), **context) -> Status:
        if not alertInfo: return
        elif not isinstance(alertInfo, AlertInfo): alertInfo = AlertInfo(**alertInfo)
        context.update(initTime=self.initTime, curTime=self.now(unit="second"), curDate=self.today(), **alertInfo["textInfo"])
        data = self.map_alert_data(data, **context)
        text = self.format_alert_text(data, **context)
        return self.send_alert_text(text, alertInfo)

    def get_save_name(self, prefix=str()) -> str:
        return prefix if prefix else self.operation

    def get_save_sheet(self, sheet_name=str()) -> str:
        return sheet_name if sheet_name else SAVE_SHEET


###################################################################
############################## Spider #############################
###################################################################

class Spider(RequestSession, Iterator, Parser):
    __metaclass__ = abc.ABCMeta
    asyncio = False
    operation = "spider"
    host = str()
    where = WHERE
    which = WHICH
    verb = ACTION
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
    interval = None
    fromNow = None
    timeout = None
    ssl = None
    numRetries = 0
    delay = 1.
    cookies = str()
    interruptType = tuple()
    killType = tuple()
    errorType = tuple()
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
                tzinfo: Optional[Timezone]=None, countryCode=str(), datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debugPoint: Optional[Keyword]=None, killPoint: Optional[Keyword]=None, extraSave: Optional[Keyword]=None,
                numRetries: Optional[int]=None, delay: Optional[Range]=None, cookies: Optional[str]=None,
                fromNow: Optional[Unit]=None, discard=True, progress=True, where=str(), which=str(), by=str(), message=str(),
                iterateUnit: Optional[int]=None, interval: Optional[Timedelta]=None, apiRedirect=False, redirectUnit: Optional[int]=None,
                queryList: GoogleQueryList=list(), uploadList: GoogleUploadList=list(), alertInfo: AlertInfo=dict(), **context):
        self.set_filter_variables(fields, ranges, returnType)
        self.set_init_time(tzinfo, countryCode, datetimeUnit)
        self.set_logger(logName, logLevel, logFile, localSave, debugPoint, killPoint, extraSave)
        self.set_request_variables(numRetries, delay, cookies)
        self.set_spider_variables(fromNow, discard, progress)
        self.set_gather_message(where, which, by, message)
        self.set_iterator_unit(iterateUnit, interval)
        self.set_redirect_variables(apiRedirect, redirectUnit)
        UploadSession.__init__(self, queryList, uploadList, alertInfo, **context)
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

    def is_kill(self, exception: Exception) -> bool:
        return isinstance(exception, SPIDER_KILL) or isinstance(exception, self.killType)

    def is_error(self, exception: Exception) -> bool:
        return isinstance(exception, SPIDER_ERROR) or isinstance(exception, self.errorType)

    def log_error(self, func: Callable, msg: Dict):
        iterator = self.get_iterator(**msg.get("context", dict()))
        super().log_error(func=func, msg=(iterator if iterator else msg))

    def get_date(self, date: Optional[DateFormat]=None, if_null: Optional[Unit]=None, index=0, busdate=False) -> dt.date:
        if_null = if_null if if_null is not None else self.fromNow
        return super().get_date(date, if_null=get_scala(if_null, index), busdate=busdate)

    def get_date_pair(self, startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                        if_null: Optional[Unit]=None, busdate: Logic=False) -> Tuple[dt.date,dt.date]:
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
        args = kloc(context, self.iterateArgs, if_null="drop", values_only=True)
        context = drop_dict(context, self.iterateArgs, inplace=False)
        return args, context

    ###################################################################
    ####################### Parameter Validator #######################
    ###################################################################

    def validate_params(self, locals: Dict=dict(), drop: _KT=list(), args_options=dict(), **context) -> Tuple[Arguments,Context]:
        args, context = self.local_params(locals, drop=drop, **context)
        args = self.validate_args(*args, args_options=args_options, **context)
        context = self.validate_context(**context)
        return args, context

    def validate_args(self, *args, args_options=dict(), **context) -> Arguments:
        if not args: return tuple()
        elif len(self.iterateArgs) == 1:
            queryMap = self.get_query_map(key="name", value=[], if_null="pass")
            return (self.validate_variable(args[0], **queryMap.get(self.iterateArgs[0], dict(iterable=True))),)
        else: return self.align_args(args, self.iterateArgs, **args_options)

    def align_args(self, args: Arguments, keys: _KT, alignment: Literal["min","max","first"]="min", default=None,
                    dropna=False, drop_empty=False, unique=False) -> Arguments:
        arrays, arrayDefault = list(), list()
        queryMap = self.get_query_map(key="name", value=[], if_null="pass")
        for __i, __key in enumerate(cast_list(keys)):
            __map = queryMap[__key] if __key in queryMap else dict(iterable=True)
            arrays.append(self.validate_variable(args[__i], **__map))
            arrayDefault.append(hier_get(__map, ["arr_options","default"]))
        default = default if isinstance(default, Tuple) else tuple(arrayDefault)
        return align_array(*arrays, alignment=alignment, default=default, dropna=dropna, drop_empty=drop_empty, unique=unique)

    ###################################################################
    ########################### Gather Data ###########################
    ###################################################################

    @abc.abstractmethod
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

    def get_gather_message(self, where=str(), which=str(), verb=str(), by=str(), **context) -> str:
        action = verb if verb else self.verb
        which = which if which else self.which
        by = by if by else self.by
        where = where if where else self.where
        return GATHER_MSG(action, which, by, where)

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
        return [self.fetch(**__i, progress=progress, fields=fields, **context) for __i in tqdm(iterator, desc=message, disable=(not progress))]

    ###################################################################
    ########################### Gather Count ##########################
    ###################################################################

    def gather_count(self, *args, iterator: List[Context]=list(), message=str(), progress=True,
                    fields: IndexLabel=list(), ranges: RangeFilter=list(), returnType: Optional[TypeHint]=None,
                    countPath: _KT=list(), size: Optional[int]=None, pageSize=0, **context) -> Data:
        hasSize = (size is not None) and (isinstance(size, int) or is_int_array(size, how="all", empty=False))
        context = dict(context, size=(size if hasSize else pageSize), pageSize=pageSize)
        if not iterator:
            iterator, context = self._init_first_iterator(args, context, self.iterateArgs, self.iterateProduct, hasSize)
        data = self._gather_first(iterator, countPath, hasSize, progress=progress, fields=fields, **context)
        iterator, context = self._init_next_iterator(iterator, context)
        if iterator and (not hasSize):
            data += self._gather_next(iterator, progress=progress, **context)
        return self.reduce(data, fields=fields, ranges=ranges, returnType=returnType, **context)

    def _init_first_iterator(self, args: Arguments, context: Context, iterateArgs: List[_KT]=list(), iterateProduct: List[_KT]=list(),
                            hasSize=False, indexing=True) -> Tuple[List[Context],Context]:
        if iterateArgs or iterateProduct or hasSize:
            return self._init_iterator(args, context, iterateArgs, iterateProduct, pagination=hasSize, indexing=indexing)
        else: return [{ITER_INDEX:0}], context

    def _gather_first(self, iterator: List[Context], countPath: _KT=list(), hasSize=False,
                    message=str(), progress=True, **context) -> List[Data]:
        message = message if message else self.get_gather_message(by=(FIRST_PAGE if not hasSize else self.by), **context)
        data = self._gather_data(iterator, countPath=countPath, message=message, progress=progress, **context)
        self.checkpoint("gather", where="gather_first", msg={"data":data}, save=data)
        return data

    def _init_next_iterator(self, iterator: List[Context], context: Context, indexing=True) -> Tuple[List[Context],Context]:
        iterateArgs = list(drop_dict(iterator[0], [ITER_INDEX]+PAGE_ITERATOR, inplace=False).keys())
        args = transpose_array([kloc(__i, iterateArgs, values_only=True) for __i in iterator])
        context = self._set_count_size(iterator, **context)
        iterator, context = self.set_iterator(*args, iterateArgs=iterateArgs, pagination=True, indexing=indexing, **context)
        self.checkpoint("iterator_next", where="init_next_iterator", msg={"iterator":iterator})
        return iterator, context

    def _gather_next(self, iterator: List[Context], message=str(), progress=True, **context) -> List[Data]:
        message = message if message else self.get_gather_message(by=NEXT_PAGE, **context)
        data = self._gather_data(iterator, message=message, progress=progress, **context)
        self.checkpoint("gather_next", where="gather_next", msg={"data":data}, save=data)
        return data

    def _set_count_size(self, iterator: List[Context], pageSize=0, pageStart=1, offset=1, **context) -> Context:
        context["size"] = [max(self.iterateCount.get(__i[ITER_INDEX], 0)-pageSize, 0) for __i in iterator]
        context.update(pageSize=pageSize, pageStart=pageStart+1, offset=offset+pageSize, interval=None)
        return context

    ###################################################################
    ########################### Request Data ##########################
    ###################################################################

    @abc.abstractmethod
    def fetch(self, *args, **context) -> Data:
        ...

    def validate_request(func):
        @functools.wraps(func)
        def wrapper(self: Spider, *args, locals: Dict=dict(), drop: _KT=list(), **context):
            context = self._encode_messages(*args, **self.from_locals(locals, drop, **context))
            self.checkpoint(iter_task(context, "request"), where=func.__name__, msg=dict(url=context["url"], **context["messages"]))
            response = func(self, **self._validate_session(context))
            self.checkpoint(iter_task(context, "response"), where=func.__name__, msg={"response":response}, save=response)
            return response
        return wrapper

    def _encode_messages(self, method: str, url: str, messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                        data=None, json=None, headers=None, cookies=str(), *args, **context) -> Context:
        url, params = self.encode_params(url, params, encode=encode)
        if headers and cookies: headers["Cookie"] = str(cookies)
        messages = messages if messages else notna_dict(params=params, data=data, json=json, headers=headers)
        self.logger.debug(log_messages(**notna_dict({ITER_INDEX: context.get(ITER_INDEX)}), **messages))
        return dict(context, method=method, url=url, messages=messages)

    def _validate_session(self, context: Context) -> Context:
        if not (("session" in context) and isinstance(context["session"], requests.Session)):
            context["session"] = requests
        return context

    @validate_request
    def request(self, method: str, url: str, messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                data=None, json=None, headers=None, cookies=str(), *args, locals: Dict=dict(), drop: _KT=list(),
                session: Optional[requests.Session]=None, allow_redirects=True, validate=False,
                exception: Literal["error","interrupt"]="interrupt", valid: Optional[Status]=None, invalid: Optional[Status]=None,
                close=True, **context) -> requests.Response:
        response = session.request(method, url, **messages, allow_redirects=allow_redirects, **self.get_request_options())
        self.logger.info(log_response(response, url=url, **self.get_iterator(**context, _index=True)))
        if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
        return response.close() if close else response

    @validate_request
    def request_status(self, method: str, url: str, messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                        data=None, json=None, headers=None, cookies=str(), *args, locals: Dict=dict(), drop: _KT=list(),
                        session: Optional[requests.Session]=None, allow_redirects=True, validate=False,
                        exception: Literal["error","interrupt"]="interrupt", valid: Optional[Status]=None, invalid: Optional[Status]=None,
                        **context) -> int:
        with session.request(method, url, **messages, allow_redirects=allow_redirects, **self.get_request_options()) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.status_code

    @validate_request
    def request_content(self, method: str, url: str, messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                        data=None, json=None, headers=None, cookies=str(), *args, locals: Dict=dict(), drop: _KT=list(),
                        session: Optional[requests.Session]=None, allow_redirects=True, validate=False,
                        exception: Literal["error","interrupt"]="interrupt", valid: Optional[Status]=None, invalid: Optional[Status]=None,
                        **context) -> bytes:
        with session.request(method, url, **messages, allow_redirects=allow_redirects, **self.get_request_options()) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.content

    @validate_request
    def request_text(self, method: str, url: str, messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                    data=None, json=None, headers=None, cookies=str(), *args, locals: Dict=dict(), drop: _KT=list(),
                    session: Optional[requests.Session]=None, allow_redirects=True, validate=False,
                    exception: Literal["error","interrupt"]="interrupt", valid: Optional[Status]=None, invalid: Optional[Status]=None,
                    **context) -> str:
        with session.request(method, url, **messages, allow_redirects=allow_redirects, **self.get_request_options()) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.text

    @validate_request
    def request_json(self, method: str, url: str, messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                    data=None, json=None, headers=None, cookies=str(), *args, locals: Dict=dict(), drop: _KT=list(),
                    session: Optional[requests.Session]=None, allow_redirects=True, validate=False,
                    exception: Literal["error","interrupt"]="interrupt", valid: Optional[Status]=None, invalid: Optional[Status]=None,
                    **context) -> JsonData:
        with session.request(method, url, **messages, allow_redirects=allow_redirects, **self.get_request_options()) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.json()

    @validate_request
    def request_headers(self, method: str, url: str, messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                        data=None, json=None, headers=None, cookies=str(), *args, locals: Dict=dict(), drop: _KT=list(),
                        session: Optional[requests.Session]=None, allow_redirects=True, validate=False,
                        exception: Literal["error","interrupt"]="interrupt", valid: Optional[Status]=None, invalid: Optional[Status]=None,
                        **context) -> Dict:
        with session.request(method, url, **messages, allow_redirects=allow_redirects, **self.get_request_options()) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.headers

    @validate_request
    def request_source(self, method: str, url: str, messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                        data=None, json=None, headers=None, cookies=str(), *args, locals: Dict=dict(), drop: _KT=list(),
                        session: Optional[requests.Session]=None, allow_redirects=True, validate=False,
                        exception: Literal["error","interrupt"]="interrupt", valid: Optional[Status]=None, invalid: Optional[Status]=None,
                        features="html.parser", **context) -> Tag:
        with session.request(method, url, **messages, allow_redirects=allow_redirects, **self.get_request_options()) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return BeautifulSoup(response.text, features)

    @validate_request
    def request_table(self, method: str, url: str, messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                    data=None, json=None, headers=None, cookies=str(), *args, locals: Dict=dict(), drop: _KT=list(),
                    session: Optional[requests.Session]=None, allow_redirects=True, validate=False,
                    exception: Literal["error","interrupt"]="interrupt", valid: Optional[Status]=None, invalid: Optional[Status]=None,
                    table_type="auto", table_idx=0, table_options: Dict=dict(), **context) -> pd.DataFrame:
        with session.request(method, url, **messages, allow_redirects=allow_redirects, **self.get_request_options()) as response:
            self.logger.info(log_response(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return read_table(response.content, file_type=table_type, sheet_name=table_idx, **table_options)

    def get_request_options(self) -> Dict:
        return dict(timeout=self.timeout, verify=self.ssl)

    def encode_params(self, url: str, params: Optional[Dict]=None, encode: Optional[bool]=None) -> Tuple[str,Dict]:
        if not params: return url, None
        elif not isinstance(encode, bool): return url, params
        else: return encode_params(url, params, encode=encode), None

    def validate_status(self, response: requests.Response, how: Literal["error","interrupt"]="interrupt",
                        valid: Optional[Status]=None, invalid: Optional[Status]=None):
        status = response.status_code
        if (valid and (status not in cast_tuple(valid))) or (invalid and (status in cast_tuple(invalid))):
            if how == "interrupt": raise RequestInterrupt(INVALID_STATUS_MSG(self.where))
            else: raise requests.ConnectionError(INVALID_STATUS_MSG(self.where))

    def log_results(self, data: Data, **context):
        self.logger.info(log_data(data, query=self.get_iterator(**context)))

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
    def redirect(self, *args, iterator: List[Context]=list(), redirectUnit: Optional[int]=None, progress=True,
                fields: IndexLabel=list(), ranges: RangeFilter=list(), returnType: Optional[TypeHint]=None, **context) -> Data:
        message = self.get_redirect_message(**context)
        if not iterator:
            iterator, context = self._init_iterator(args, context, self.iterateArgs, self.iterateProduct, self.pagination)
        data = self._redirect_data(iterator, redirectUnit, message=message, progress=progress, fields=fields, **context)
        self.checkpoint("gather", where="redirect", msg={"data":data}, save=data)
        return self.reduce(data, fields=fields, ranges=ranges, returnType=returnType, **context)

    def get_redirect_message(self, **context) -> str:
        return REDIRECT_MSG(self.operation)

    @RequestSession.retry_request
    @RequestSession.limit_request
    @gcloud_authorized
    def fetch_redirect(self, iterator: List[Context], redirectUrl: str, authorization: str,
                        account: Account=dict(), cookies=str(), **context) -> Data:
        body = self._make_redirect_body(iterator, redirectUrl, authorization, account, cookies=cookies, **context).encode("utf-8")
        response = self.request_json(POST, redirectUrl, data=body, headers=dict(Authorization=authorization), **context)
        return self._parse_redirect(response, **context)

    def _validate_redirect_info(self, redirectUrl=str(), authorization=str(), account: Account=dict()) -> Context:
        if not redirectUrl:
            account = account if account and isinstance(account, dict) else read_gcloud(account)
            redirectUrl = account["audience"]
        if not authorization:
            authorization = fetch_gcloud_authorization(redirectUrl, account)
        return dict(redirectUrl=redirectUrl, authorization=authorization, account=account)

    def _redirect_data(self, iterator: List[Context], redirectUnit: Optional[int]=None,
                        message=str(), progress=True, fields: IndexLabel=list(), **context) -> List[Data]:
        iterator = unit_array(iterator, max(cast_int(redirectUnit), 1))
        fields = fields if isinstance(fields, Sequence) else list()
        return [self.fetch_redirect(__i, progress=progress, fields=fields, **context) for __i in tqdm(iterator, desc=message, disable=(not progress))]

    def _make_redirect_body(self, iterator: List[Context], redirectUrl: str, authorization: str, account: Account=dict(), **context) -> str:
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
    __metaclass__ = abc.ABCMeta
    asyncio = True
    operation = "session"
    fields = list()
    ranges = list()
    maxLimit = MAX_ASYNC_TASK_LIMIT
    numRetries = 0
    delay = 1.
    cookies = str()
    interruptType = tuple()
    killType = tuple()
    errorType = tuple()
    returnType = None
    mappedReturn = False
    info = Info()

    def __init__(self, fields: Optional[IndexLabel]=None, ranges: Optional[RangeFilter]=None, returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, countryCode=str(), datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debugPoint: Optional[Keyword]=None, killPoint: Optional[Keyword]=None, extraSave: Optional[Keyword]=None,
                numRetries: Optional[int]=None, delay: Optional[Range]=None, cookies: Optional[str]=None, numTasks=100,
                queryList: GoogleQueryList=list(), uploadList: GoogleUploadList=list(), alertInfo: AlertInfo=dict(), **context):
        self.set_filter_variables(fields, ranges, returnType)
        self.set_init_time(tzinfo, countryCode, datetimeUnit)
        self.set_logger(logName, logLevel, logFile, localSave, debugPoint, killPoint, extraSave)
        self.set_async_variables(numRetries, delay, cookies, numTasks)
        UploadSession.__init__(self, queryList, uploadList, alertInfo, **context)

    def set_async_variables(self, numRetries: Optional[int]=None, delay: Optional[Range]=None, cookies: Optional[str]=None, numTasks=100):
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
            async with aiohttp.ClientSession(**self.set_client_session(**context)) as session:
                data = await func(self, *args, session=session, semaphore=semaphore, **SESSION_CONTEXT(**context))
            await asyncio.sleep(.25)
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper

    def with_requests_session(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSession, *args, **context):
            with requests.Session() as session:
                return await func(self, *args, session=session, **context)
        return wrapper

    def with_async_session(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSession, *args, semaphore: Optional[asyncio.Semaphore]=None, **context):
            if semaphore is None:
                semaphore = self.asyncio_semaphore(**context)
            async with aiohttp.ClientSession(**self.set_client_session(**context)) as session:
                return await func(self, *args, session=session, semaphore=semaphore, **context)
        return wrapper

    def asyncio_semaphore(self, numTasks: Optional[int]=None, **context) -> asyncio.Semaphore:
        numTasks = numTasks if isinstance(numTasks, int) and numTasks > 0 else self.numTasks
        return asyncio.Semaphore(min(numTasks, self.maxLimit))

    def set_client_session(self, **context) -> Dict:
        return dict()

    def limit_request(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSession, *args, semaphore: Optional[asyncio.Semaphore]=None, **context):
            try:
                if semaphore is not None:
                    async with semaphore:
                        return await func(self, *args, **context)
                else: return await func(self, *args, **context)
            finally: await self.async_sleep()
        return wrapper

    async def async_sleep(self, delay: Optional[Range]=None, minimum=0., maximum=None):
        delay = self.get_delay(delay)
        if isinstance(minimum, (float,int)): delay = max(delay, minimum)
        if isinstance(maximum, (float,int)): delay = min(delay, maximum)
        if delay: await asyncio.sleep(delay)

    ###################################################################
    ########################## Async Override #########################
    ###################################################################

    def retry_request(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSession, *args, numRetries: Optional[int]=None, retryCount: _PASS=None, **context):
            numRetries = numRetries if isinstance(numRetries, int) else self.numRetries
            for count in reversed(range(numRetries+1)):
                try: return await func(self, *args, numRetries=numRetries, retryCount=count, **context)
                except Exception as exception:
                    exitCode, value = await self.catch_exception(exception, func, args, context, numRetries, retryCount=count)
                    if exitCode == RETURN: return value
                    elif exitCode == RETRY: continue
                    else: raise exception
            return await func(self, *args, **context)
        return wrapper

    async def catch_exception(self, exception: Exception, func: Callable, args: Arguments, context: Context,
                            numRetries=0, retryCount=0) -> Tuple[int,Any]:
        if self.is_interrupt(exception):
            return await self.interrupt(*args, exception=exception, func=func, numRetries=numRetries, retryCount=retryCount, **context)
        elif self.is_kill(exception):
            raise exception
        elif (retryCount == 0) or self.is_error(exception):
            return RETURN, self.pass_exception(exception, func, msg={"args":args, "context":context})
        else: return RETRY, None

    async def interrupt(self, *args, exception: Exception, func: Callable, retryCount=0, **context) -> Tuple[int,Any]:
        if retryCount == 0:
            return RETURN, self.pass_exception(exception, func, msg={"args":args, "context":context})
        else: return RETRY, None

    def validate_data(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSession, *args, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None, **params):
            data = await func(self, *args, fields=fields, returnType=returnType, **params)
            if self.mappedReturn:
                return traversal_dict(data, fields, apply=self.filter_data, dropna=True, returnType=returnType)
            else: return self.filter_data(data, fields, returnType=returnType)
        return wrapper


###################################################################
########################### Async Spider ##########################
###################################################################

class AsyncSpider(AsyncSession, Spider):
    __metaclass__ = abc.ABCMeta
    asyncio = True
    operation = "spider"
    host = str()
    where = WHERE
    which = WHICH
    verb = ACTION
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
    interval = None
    fromNow = None
    timeout = None
    ssl = None
    numRetries = 0
    delay = 1.
    cookies = str()
    interruptType = tuple()
    killType = tuple()
    errorType = tuple()
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
                tzinfo: Optional[Timezone]=None, countryCode=str(), datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debugPoint: Optional[Keyword]=None, killPoint: Optional[Keyword]=None, extraSave: Optional[Keyword]=None,
                numRetries: Optional[int]=None, delay: Optional[Range]=None, cookies: Optional[str]=None, numTasks=100,
                fromNow: Optional[Unit]=None, discard=True, progress=True, where=str(), which=str(), by=str(), message=str(),
                iterateUnit: Optional[int]=None, interval: Optional[Timedelta]=None, apiRedirect=False, redirectUnit: Optional[int]=None,
                queryList: GoogleQueryList=list(), uploadList: GoogleUploadList=list(), alertInfo: AlertInfo=dict(), **context):
        self.set_filter_variables(fields, ranges, returnType)
        self.set_init_time(tzinfo, countryCode, datetimeUnit)
        self.set_logger(logName, logLevel, logFile, localSave, debugPoint, killPoint, extraSave)
        self.set_async_variables(numRetries, delay, cookies, numTasks)
        self.set_max_limit(apiRedirect)
        self.set_spider_variables(fromNow, discard, progress)
        self.set_gather_message(where, which, by, message)
        self.set_iterator_unit(iterateUnit, interval)
        self.set_redirect_variables(apiRedirect, redirectUnit)
        UploadSession.__init__(self, queryList, uploadList, alertInfo, **context)
        self._disable_warnings()

    def set_max_limit(self, apiRedirect=False):
        self.maxLimit = min(self.maxLimit, self.redirectLimit) if apiRedirect else self.maxLimit

    ###################################################################
    ########################### Async Gather ##########################
    ###################################################################

    @abc.abstractmethod
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
    async def gather(self, *args, iterator: List[Context]=list(), asynchronous=True, message=str(), progress=True,
                    fields: IndexLabel=list(), ranges: RangeFilter=list(), returnType: Optional[TypeHint]=None, **context) -> Data:
        message = message if message else self.get_gather_message(**context)
        if not iterator:
            iterator, context = self._init_iterator(args, context, self.iterateArgs, self.iterateProduct, self.pagination)
        data = await self._gather_data(iterator, asynchronous, message=message, progress=progress, fields=fields, **context)
        self.checkpoint("gather", where="gather", msg={"data":data}, save=data)
        return self.reduce(data, fields=fields, ranges=ranges, returnType=returnType, **context)

    async def _gather_data(self, iterator: List[Context], asynchronous=True,
                            message=str(), progress=True, fields: IndexLabel=list(), **context) -> List[Data]:
        fields = fields if isinstance(fields, Sequence) else list()
        options = dict(desc=message, disable=(not progress))
        if asynchronous:
            return await tqdm.gather(*[self.fetch(**__i, progress=progress, fields=fields, **context) for __i in iterator], **options)
        else: return [(await self.fetch(**__i, progress=progress, fields=fields, **context)) for __i in tqdm(iterator, **options)]

    ###################################################################
    ########################### Async Count ###########################
    ###################################################################

    async def gather_count(self, *args, iterator: List[Context]=list(), asynchronous=True, message=str(), progress=True,
                            fields: IndexLabel=list(), ranges: RangeFilter=list(), returnType: Optional[TypeHint]=None,
                            countPath: _KT=list(), size: Optional[int]=None, pageSize=0, **context) -> Data:
        hasSize = (size is not None) and (isinstance(size, int) or is_int_array(size, how="all", empty=False))
        context = dict(context, size=(size if hasSize else pageSize), pageSize=pageSize)
        if not iterator:
            iterator, context = self._init_first_iterator(args, context, self.iterateArgs, self.iterateProduct, hasSize)
        data = await self._gather_first(iterator, asynchronous, countPath, hasSize, progress=progress, fields=fields, **context)
        iterator, context = self._init_next_iterator(iterator, context)
        if iterator and (not hasSize):
            data += await self._gather_next(iterator, asynchronous, progress=progress, fields=fields, **context)
        return self.reduce(data, fields=fields, ranges=ranges, returnType=returnType, **context)

    async def _gather_first(self, iterator: List[Context], asynchronous=True, countPath: _KT=list(), hasSize=False,
                            message=str(), progress=True, **context) -> List[Data]:
        message = message if message else self.get_gather_message(by=(FIRST_PAGE if not hasSize else self.by), **context)
        data = await self._gather_data(iterator, asynchronous, countPath=countPath, message=message, progress=progress, **context)
        self.checkpoint("gather", where="gather_first", msg={"data":data}, save=data)
        return data

    async def _gather_next(self, iterator: List[Context], asynchronous=True, message=str(), progress=True, **context) -> List[Data]:
        message = message if message else self.get_gather_message(by=NEXT_PAGE, **context)
        data = await self._gather_data(iterator, asynchronous, message=message, progress=progress, **context)
        self.checkpoint("gather_count", where="gather_next", msg={"data":data}, save=data)
        return data

    ###################################################################
    ########################## Async Request ##########################
    ###################################################################

    @abc.abstractmethod
    async def fetch(self, *args, **context) -> Data:
        ...

    def validate_request(func):
        @functools.wraps(func)
        async def wrapper(self: AsyncSpider, *args, locals: Dict=dict(), drop: _KT=list(), **context):
            context = self._encode_messages(*args, **self.from_locals(locals, drop, **context))
            self.checkpoint(iter_task(context, "request"), where=func.__name__, msg=dict(url=context["url"], **context["messages"]))
            response = await func(self, **self._validate_session(context))
            self.checkpoint(iter_task(context, "response"), where=func.__name__, msg={"response":response}, save=response)
            return response
        return wrapper

    def _validate_session(self, context: Context) -> Context:
        if not (("session" in context) and isinstance(context["session"], aiohttp.ClientSession)):
            raise RequestInterrupt(MISSING_CLIENT_SESSION_MSG)
        return context

    @validate_request
    async def request(self, method: str, url: str, messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                        data=None, json=None, headers=None, cookies=str(), *args, locals: Dict=dict(), drop: _KT=list(),
                        session: Optional[aiohttp.ClientSession]=None, allow_redirects=True, validate=False,
                        exception: Literal["error","interrupt"]="interrupt", valid: Optional[Status]=None, invalid: Optional[Status]=None,
                        **context):
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, **self.get_request_options()) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)

    @validate_request
    async def request_status(self, method: str, url: str, messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                            data=None, json=None, headers=None, cookies=str(), *args, locals: Dict=dict(), drop: _KT=list(),
                            session: Optional[aiohttp.ClientSession]=None, allow_redirects=True, validate=False,
                            exception: Literal["error","interrupt"]="interrupt", valid: Optional[Status]=None, invalid: Optional[Status]=None,
                            **context) -> int:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, **self.get_request_options()) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.status

    @validate_request
    async def request_content(self, method: str, url: str, messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                            data=None, json=None, headers=None, cookies=str(), *args, locals: Dict=dict(), drop: _KT=list(),
                            session: Optional[aiohttp.ClientSession]=None, allow_redirects=True, validate=False,
                            exception: Literal["error","interrupt"]="interrupt",  valid: Optional[Status]=None, invalid: Optional[Status]=None,
                            **context) -> bytes:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, **self.get_request_options()) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return await response.read()

    @validate_request
    async def request_text(self, method: str, url: str, messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                            data=None, json=None, headers=None, cookies=str(), *args, locals: Dict=dict(), drop: _KT=list(),
                            session: Optional[aiohttp.ClientSession]=None, allow_redirects=True, validate=False,
                            exception: Literal["error","interrupt"]="interrupt", valid: Optional[Status]=None, invalid: Optional[Status]=None,
                            encoding=None, **context) -> str:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, **self.get_request_options()) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return await response.text(encoding=encoding)

    @validate_request
    async def request_json(self, method: str, url: str, messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                            data=None, json=None, headers=None, cookies=str(), *args, locals: Dict=dict(), drop: _KT=list(),
                            session: Optional[aiohttp.ClientSession]=None, allow_redirects=True, validate=False,
                            exception: Literal["error","interrupt"]="interrupt", valid: Optional[Status]=None, invalid: Optional[Status]=None,
                            encoding=None, **context) -> JsonData:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, **self.get_request_options()) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return await response.json(encoding=encoding, content_type=None)

    @validate_request
    async def request_headers(self, method: str, url: str, messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                            data=None, json=None, headers=None, cookies=str(), *args, locals: Dict=dict(), drop: _KT=list(),
                            session: Optional[aiohttp.ClientSession]=None, allow_redirects=True, validate=False,
                            exception: Literal["error","interrupt"]="interrupt", valid: Optional[Status]=None, invalid: Optional[Status]=None,
                            **context) -> Dict:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, **self.get_request_options()) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return response.headers

    @validate_request
    async def request_source(self, method: str, url: str, messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                            data=None, json=None, headers=None, cookies=str(), *args, locals: Dict=dict(), drop: _KT=list(),
                            session: Optional[aiohttp.ClientSession]=None, allow_redirects=True, validate=False,
                            exception: Literal["error","interrupt"]="interrupt", valid: Optional[Status]=None, invalid: Optional[Status]=None,
                            encoding=None, features="html.parser", **context) -> Tag:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, **self.get_request_options()) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return BeautifulSoup(await response.text(encoding=encoding), features)

    @validate_request
    async def request_table(self, method: str, url: str, messages: Dict=dict(), params=None, encode: Optional[bool]=None,
                            data=None, json=None, headers=None, cookies=str(), *args, locals: Dict=dict(), drop: _KT=list(),
                            session: Optional[aiohttp.ClientSession]=None, allow_redirects=True, validate=False,
                            exception: Literal["error","interrupt"]="interrupt", valid: Optional[Status]=None, invalid: Optional[Status]=None,
                            table_type="auto", table_idx=0, table_options: Dict=dict(), **context) -> pd.DataFrame:
        async with session.request(method, url, **messages, allow_redirects=allow_redirects, **self.get_request_options()) as response:
            self.logger.info(await log_client(response, url=url, **self.get_iterator(**context, _index=True)))
            if validate: self.validate_status(response, how=exception, valid=valid, invalid=invalid)
            return read_table(await response.read(), file_type=table_type, sheet_name=table_idx, **table_options)

    def get_request_options(self) -> Dict:
        timeout = aiohttp.ClientTimeout(total=self.timeout) if self.timeout is not None else None
        return dict(timeout=timeout, ssl=self.ssl)

    def validate_status(self, response: aiohttp.ClientResponse, how: Literal["error","interrupt"]="interrupt",
                        valid: Optional[Status]=None, invalid: Optional[Status]=None):
        status = response.status
        if (valid and (status not in cast_tuple(valid))) or (invalid and (status in cast_tuple(invalid))):
            if how == "interrupt": raise RequestInterrupt(INVALID_STATUS_MSG(self.where))
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
    async def redirect(self, *args, iterator: List[Context], asynchronous=True, redirectUnit: Optional[int]=None, progress=True,
                        fields: IndexLabel=list(), ranges: RangeFilter=list(), returnType: Optional[TypeHint]=None, **context) -> Data:
        message = self.get_redirect_message(**context)
        if not iterator:
            iterator, context = self._init_iterator(args, context, self.iterateArgs, self.iterateProduct, self.pagination)
        data = await self._redirect_data(iterator, asynchronous, redirectUnit, message=message, progress=progress, fields=fields, **context)
        self.checkpoint("gather", where="redirect", msg={"data":data}, save=data)
        return self.reduce(data, fields=fields, ranges=ranges, returnType=returnType, **context)

    @AsyncSession.retry_request
    @AsyncSession.limit_request
    @gcloud_authorized
    async def fetch_redirect(self, iterator: List[Context], redirectUrl: str, authorization: str,
                            account: Account=dict(), cookies=str(), **context) -> Records:
        body = self._make_redirect_body(iterator, redirectUrl, authorization, account, cookies=cookies, **context).encode("utf-8")
        response = await self.request_json(POST, redirectUrl, data=body, headers=dict(Authorization=authorization), **context)
        return self._parse_redirect(response, **context)

    async def _redirect_data(self, iterator: List[Context], asynchronous=True, redirectUnit: Optional[int]=None,
                            message=str(), progress=True, fields: IndexLabel=list(), **context) -> List[Data]:
        iterator = unit_array(iterator, max(cast_int(redirectUnit), 1))
        fields = fields if isinstance(fields, Sequence) else list()
        options = dict(desc=message, disable=(not progress))
        if asynchronous:
            return await tqdm.gather(*[self.fetch_redirect(__i, progress=progress, fields=fields, **context) for __i in iterator], **options)
        else: return [(await self.fetch_redirect(**__i, progress=progress, fields=fields, **context)) for __i in tqdm(iterator, **options)]


###################################################################
########################### Login Spider ##########################
###################################################################

class LoginSpider(requests.Session, Spider):
    __metaclass__ = abc.ABCMeta
    asyncio = False
    operation = "login"
    host = str()
    where = WHERE
    timeout = None
    ssl = None
    cookie = str()

    @abc.abstractmethod
    def __init__(self, logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None,
                debugPoint: Optional[Keyword]=None, killPoint: Optional[Keyword]=None, extraSave: Optional[Keyword]=None,
                cookies: Optional[Union[str,Dict]]=None, **context):
        requests.Session.__init__(self)
        self.set_cookies(cookies)
        self.set_logger(logName, logLevel, logFile, False, debugPoint, killPoint, extraSave)
        self._disable_warnings()

    @abc.abstractmethod
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
    ########################### Request Data ##########################
    ###################################################################

    def validate_request(func):
        @functools.wraps(func)
        def wrapper(self: Spider, *args, locals: Dict=dict(), drop: _KT=list(), origin=str(), **context):
            context = self._encode_messages(*args, **self.from_locals(locals, drop, **context))
            self.checkpoint(concat(origin, "request", sep='_'), where=func.__name__, msg=dict(url=context["url"], **context["messages"]))
            response = func(self, origin=origin, **context)
            self.checkpoint(concat(origin, "response", sep='_'), where=func.__name__, msg={"response":response}, save=response)
            return response
        return wrapper

    @validate_request
    def request_url(self, method: str, url: str, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None,
                    headers=None, cookies=str(), *args, origin=str(), allow_redirects=True, **context):
        with self.request(method, url, **messages, allow_redirects=allow_redirects, **self.get_request_options()) as response:
            self.logger.info(log_response(response, url=url, origin=origin))
            self.logger.debug(log_messages(cookies=self.get_cookies(encode=False), origin=origin))
            self.log_response_text(response, origin)

    @validate_request
    def request_status(self, method: str, url: str, messages: Dict=dict(),
                        params=None, encode: Optional[bool]=None, data=None, json=None,
                        headers=None, cookies=str(), *args, origin=str(), allow_redirects=True, **context) -> int:
        with self.request(method, url, **messages, allow_redirects=allow_redirects, **self.get_request_options()) as response:
            self.logger.info(log_response(response, url=url, origin=origin))
            self.logger.debug(log_messages(cookies=self.get_cookies(encode=False), origin=origin))
            self.log_response_text(response, origin)
            return response.status_code

    @validate_request
    def request_content(self, method: str, url: str, messages: Dict=dict(),
                        params=None, encode: Optional[bool]=None, data=None, json=None,
                        headers=None, cookies=str(), *args, origin=str(), allow_redirects=True, **context) -> bytes:
        with self.request(method, url, **messages, allow_redirects=allow_redirects, **self.get_request_options()) as response:
            self.logger.info(log_response(response, url=url, origin=origin))
            self.logger.debug(log_messages(cookies=self.get_cookies(encode=False), origin=origin))
            self.log_response_text(response, origin)
            return response.content

    @validate_request
    def request_text(self, method: str, url: str, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None,
                    headers=None, cookies=str(), *args, origin=str(), allow_redirects=True, **context) -> str:
        with self.request(method, url, **messages, allow_redirects=allow_redirects, **self.get_request_options()) as response:
            self.logger.info(log_response(response, url=url, origin=origin))
            self.logger.debug(log_messages(cookies=self.get_cookies(encode=False), origin=origin))
            return response.text

    @validate_request
    def request_json(self, method: str, url: str, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None,
                    headers=None, cookies=str(), *args, origin=str(), allow_redirects=True, **context) -> JsonData:
        with self.request(method, url, **messages, allow_redirects=allow_redirects, **self.get_request_options()) as response:
            self.logger.info(log_response(response, url=url, origin=origin))
            self.logger.debug(log_messages(cookies=self.get_cookies(encode=False), origin=origin))
            self.log_response_text(response, origin)
            return response.json()

    @validate_request
    def request_headers(self, method: str, url: str, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None,
                    headers=None, cookies=str(), *args, origin=str(), allow_redirects=True, **context) -> Dict:
        with self.request(method, url, **messages, allow_redirects=allow_redirects, **self.get_request_options()) as response:
            self.logger.info(log_response(response, url=url, origin=origin))
            self.logger.debug(log_messages(cookies=self.get_cookies(encode=False), origin=origin))
            self.log_response_text(response, origin)
            return response.headers

    @validate_request
    def request_source(self, method: str, url: str, messages: Dict=dict(),
                    params=None, encode: Optional[bool]=None, data=None, json=None,
                    headers=None, cookies=str(), *args, origin=str(), allow_redirects=True, features="html.parser", **context) -> Tag:
        with self.request(method, url, **messages, allow_redirects=allow_redirects, **self.get_request_options()) as response:
            self.logger.info(log_response(response, url=url, origin=origin))
            self.logger.debug(log_messages(cookies=self.get_cookies(encode=False), origin=origin))
            return BeautifulSoup(response.text, features)

    def log_response_text(self, response: requests.Response, origin=str()):
        self.checkpoint(concat(origin, "text", sep='_'), where=origin, msg={"response":response.text}, save=response)


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
    __metaclass__ = abc.ABCMeta
    asyncio = False
    operation = "session"
    where = WHERE
    fields = list()
    ranges = list()
    numRetries = 0
    delay = 1.
    cookies = str()
    interruptType = tuple()
    killType = tuple()
    errorType = tuple()
    returnType = None
    mappedReturn = False
    auth = LoginSpider
    authKey = list()
    decryptedKey = dict()
    info = Info()

    def __init__(self, fields: Optional[IndexLabel]=None, ranges: Optional[RangeFilter]=None, returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, countryCode=str(), datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debugPoint: Optional[Keyword]=None, killPoint: Optional[Keyword]=None, extraSave: Optional[Keyword]=None,
                numRetries: Optional[int]=None, delay: Optional[Range]=None, cookies: Optional[str]=None,
                queryList: GoogleQueryList=list(), uploadList: GoogleUploadList=list(), alertInfo: AlertInfo=dict(),
                encryptedKey: Optional[EncryptedKey]=None, decryptedKey: Optional[DecryptedKey]=None, **context):
        RequestSession.__init__(self, **self.from_locals(locals(), drop=ENCRYPTED_UNIQUE))
        self.set_secret(encryptedKey, decryptedKey)

    def set_secret(self, encryptedKey: Optional[EncryptedKey]=None, decryptedKey: Optional[DecryptedKey]=None):
        if (encryptedKey is None) and (decryptedKey is None): return
        elif isinstance(decryptedKey, Dict): pass
        elif isinstance(encryptedKey, str) or isinstance(decryptedKey, str):
            try: decryptedKey = json.loads((decryptedKey if decryptedKey else decrypt(encryptedKey)).replace('\'','\"'))
            except JSONDecodeError: raise AuthenticationError(INVALID_USER_INFO_MSG(self.where))
        decryptedKey = decryptedKey if isinstance(decryptedKey, Dict) else self.decryptedKey
        if decryptedKey:
            encryptedKey = encrypt(json.dumps(decryptedKey, ensure_ascii=False, default=str))
            self.update(encryptedKey=encryptedKey, decryptedKey=decryptedKey)

    def is_kill(self, exception: Exception) -> bool:
        return isinstance(exception, ENCRYPTED_SESSION_KILL) or isinstance(exception, self.killType)

    ###################################################################
    ########################## Login Managers #########################
    ###################################################################

    def login_task(func):
        @functools.wraps(func)
        def wrapper(self: EncryptedSession, *args, self_var=True, **context):
            args, context = self.init_context(args, context, self_var=self_var)
            with self.init_auth(**context) as session:
                self.login(session, **context)
                data = func(self, *args, **TASK_CONTEXT(**self.set_auth_info(session, includeCookies=True, **context)))
            time.sleep(.25)
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper

    def login_session(func):
        @functools.wraps(func)
        def wrapper(self: EncryptedSession, *args, self_var=True, **context):
            args, context = self.init_context(args, context, self_var=self_var)
            with self.init_auth(**context) as session:
                self.login(session, **context)
                data = func(self, *args, session=session, **SESSION_CONTEXT(**self.set_auth_info(session, **context)))
            time.sleep(.25)
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper

    def init_auth(self, cookies=str(), **context) -> LoginSpider:
        cookies = cookies if cookies else self.cookies
        if cookies: return LoginCookie(cookies=cookies)
        else: return self.auth(**self.get_auth_key(update=True, **context))

    def get_auth_key(self, update=False, **context) -> Dict:
        if not self.authKey: return dict()
        authKey = self._filter_auth_key(**context)
        if not (authKey and isinstance(authKey, Dict) and all(authKey.values())):
            raise AuthenticationError(INVALID_USER_INFO_MSG(self.where))
        self.logger.info(log_encrypt(**authKey, show=3))
        return dict(context, **authKey) if update else authKey

    def _filter_auth_key(self, **context) -> Dict:
        authKey = self.decryptedKey if self.decryptedKey else context
        if isinstance(self.authKey, Sequence): return kloc(authKey, self.authKey, if_null="pass")
        elif isinstance(self.authKey, Callable): return self.authKey(**authKey)
        else: raise AuthenticationError(INVALID_OBJECT_TYPE_MSG(self.authKey, AUTH_KEY))

    def login(self, auth: LoginSpider, **context):
        try: auth.login()
        except: raise AuthenticationError(INVALID_USER_INFO_MSG(self.where))

    def set_auth_info(self, auth: LoginSpider, cookies=str(), includeCookies=False, **context) -> Context:
        cookies = auth.get_cookies(encode=True)
        includeCookies = includeCookies or isinstance(auth, LoginCookie)
        authInfo = self.get_auth_info(auth=auth, cookies=cookies, includeCookies=includeCookies, **context)
        self.checkpoint("login", where="set_auth_info", msg=dict(authInfo, cookies=auth.get_cookies(encode=False)))
        self.update(authInfo, cookies=cookies)
        return dict(context, **authInfo)

    def get_auth_info(self, auth: LoginSpider, cookies=str(), includeCookies=False, **context) -> Dict:
        return dict(cookies=cookies) if includeCookies else dict()

    ###################################################################
    ########################### API Managers ##########################
    ###################################################################

    def api_task(func):
        @functools.wraps(func)
        def wrapper(self: EncryptedSession, *args, self_var=True, **context):
            args, context = self.init_context(args, context, self_var=self_var)
            data = func(self, *args, **TASK_CONTEXT(**self.get_auth_key(update=True, **context)))
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper

    def api_session(func):
        @functools.wraps(func)
        def wrapper(self: EncryptedSession, *args, self_var=True, **context):
            args, context = self.init_context(args, context, self_var=self_var)
            with requests.Session() as session:
                data = func(self, *args, session=session, **SESSION_CONTEXT(**self.get_auth_key(update=True, **context)))
            time.sleep(.25)
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper


###################################################################
######################### Encrypted Spider ########################
###################################################################

class EncryptedSpider(Spider, EncryptedSession):
    __metaclass__ = abc.ABCMeta
    asyncio = False
    operation = "spider"
    host = str()
    where = WHERE
    which = WHICH
    verb = ACTION
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
    interval = None
    fromNow = None
    timeout = None
    ssl = None
    numRetries = 0
    delay = 1.
    cookies = str()
    interruptType = tuple()
    killType = tuple()
    errorType = tuple()
    responseType = "records"
    returnType = "records"
    mappedReturn = False
    root = list()
    groupby = list()
    groupSize = dict()
    countby = str()
    auth = LoginSpider
    authKey = list()
    decryptedKey = dict()
    info = Info()
    flow = Flow()

    def __init__(self, fields: Optional[IndexLabel]=None, ranges: Optional[RangeFilter]=None, returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, countryCode=str(), datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debugPoint: Optional[Keyword]=None, killPoint: Optional[Keyword]=None, extraSave: Optional[Keyword]=None,
                numRetries: Optional[int]=None, delay: Optional[Range]=None, cookies: Optional[str]=None,
                fromNow: Optional[Unit]=None, discard=True, progress=True, where=str(), which=str(), by=str(), message=str(),
                iterateUnit: Optional[int]=None, interval: Optional[Timedelta]=None, apiRedirect=False, redirectUnit: Optional[int]=None,
                queryList: GoogleQueryList=list(), uploadList: GoogleUploadList=list(), alertInfo: AlertInfo=dict(),
                encryptedKey: Optional[EncryptedKey]=None, decryptedKey: Optional[DecryptedKey]=None, **context):
        Spider.__init__(self, **self.from_locals(locals(), drop=ENCRYPTED_UNIQUE))
        self.set_secret(encryptedKey, decryptedKey)

    def is_kill(self, exception: Exception) -> bool:
        return isinstance(exception, ENCRYPTED_SPIDER_KILL) or isinstance(exception, self.killType)


###################################################################
##################### Encrypted Async Session #####################
###################################################################

class EncryptedAsyncSession(AsyncSession, EncryptedSession):
    __metaclass__ = abc.ABCMeta
    asyncio = True
    operation = "session"
    fields = list()
    ranges = list()
    maxLimit = MAX_ASYNC_TASK_LIMIT
    numRetries = 0
    delay = 1.
    cookies = str()
    interruptType = tuple()
    killType = tuple()
    errorType = tuple()
    returnType = None
    mappedReturn = False
    auth = LoginSpider
    authKey = list()
    decryptedKey = dict()
    info = Info()

    def __init__(self, fields: Optional[IndexLabel]=None, ranges: Optional[RangeFilter]=None, returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, countryCode=str(), datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debugPoint: Optional[Keyword]=None, killPoint: Optional[Keyword]=None, extraSave: Optional[Keyword]=None,
                numRetries: Optional[int]=None, delay: Optional[Range]=None, cookies: Optional[str]=None, numTasks=100,
                queryList: GoogleQueryList=list(), uploadList: GoogleUploadList=list(), alertInfo: AlertInfo=dict(),
                encryptedKey: Optional[EncryptedKey]=None, decryptedKey: Optional[DecryptedKey]=None, **context):
        AsyncSession.__init__(self, **self.from_locals(locals(), drop=ENCRYPTED_UNIQUE))
        self.set_secret(encryptedKey, decryptedKey)

    def is_kill(self, exception: Exception) -> bool:
        return isinstance(exception, ENCRYPTED_SESSION_KILL) or isinstance(exception, self.killType)

    ###################################################################
    ########################## Login Managers #########################
    ###################################################################

    def login_task(func):
        @functools.wraps(func)
        async def wrapper(self: EncryptedAsyncSession, *args, self_var=True, **context):
            args, context = self.init_context(args, context, self_var=self_var)
            semaphore = self.asyncio_semaphore(**context)
            with self.init_auth(**context) as auth:
                self.login(auth, **context)
                authInfo = self.set_auth_info(auth, includeCookies=True, **context)
                data = await func(self, *args, semaphore=semaphore, **TASK_CONTEXT(**authInfo))
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
                self.login(auth, **context)
                authInfo = self.set_auth_info(auth, **context)
                async with aiohttp.ClientSession(**self.set_client_session(auth=auth, **authInfo)) as session:
                    data = await func(self, *args, auth=auth, session=session, semaphore=semaphore, **SESSION_CONTEXT(**authInfo))
            await asyncio.sleep(.25)
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper

    def set_client_session(self, auth: Optional[LoginSpider]=None, cookies=str(), **context) -> Dict:
        if auth is not None:
            return dict() if cookies else dict(cookies=auth.get_cookies(encode=False))
        else: return dict()

    ###################################################################
    ########################### API Managers ##########################
    ###################################################################

    def api_task(func):
        @functools.wraps(func)
        async def wrapper(self: EncryptedAsyncSession, *args, self_var=True, **context):
            args, context = self.init_context(args, context, self_var=self_var)
            semaphore = self.asyncio_semaphore(**context)
            authKey = TASK_CONTEXT(**self.get_auth_key(update=True, **context))
            data = await func(self, *args, semaphore=semaphore, **authKey)
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper

    def api_session(func):
        @functools.wraps(func)
        async def wrapper(self: EncryptedAsyncSession, *args, self_var=True, **context):
            args, context = self.init_context(args, context, self_var=self_var)
            semaphore = self.asyncio_semaphore(**context)
            async with aiohttp.ClientSession() as session:
                authKey = SESSION_CONTEXT(**self.get_auth_key(update=True, **context))
                data = await func(self, *args, session=session, semaphore=semaphore, **authKey)
            await asyncio.sleep(.25)
            self.with_data(data, func=func.__name__, **context)
            return data
        return wrapper


###################################################################
###################### Encrypted Async Spider #####################
###################################################################

class EncryptedAsyncSpider(AsyncSpider, EncryptedAsyncSession):
    __metaclass__ = abc.ABCMeta
    asyncio = True
    operation = "spider"
    host = str()
    where = WHERE
    which = WHICH
    verb = ACTION
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
    interval = None
    fromNow = None
    timeout = None
    ssl = None
    numRetries = 0
    delay = 1.
    cookies = str()
    interruptType = tuple()
    killType = tuple()
    errorType = tuple()
    responseType = "records"
    returnType = "records"
    mappedReturn = False
    root = list()
    groupby = list()
    groupSize = dict()
    countby = str()
    auth = LoginSpider
    authKey = list()
    decryptedKey = dict()
    info = Info()
    flow = Flow()

    def __init__(self, fields: Optional[IndexLabel]=None, ranges: Optional[RangeFilter]=None, returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, countryCode=str(), datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debugPoint: Optional[Keyword]=None, killPoint: Optional[Keyword]=None, extraSave: Optional[Keyword]=None,
                numRetries: Optional[int]=None, delay: Optional[Range]=None, cookies: Optional[str]=None, numTasks=100,
                fromNow: Optional[Unit]=None, discard=True, progress=True, where=str(), which=str(), by=str(), message=str(),
                iterateUnit: Optional[int]=None, interval: Optional[Timedelta]=None, apiRedirect=False, redirectUnit: Optional[int]=None,
                queryList: GoogleQueryList=list(), uploadList: GoogleUploadList=list(), alertInfo: AlertInfo=dict(),
                encryptedKey: Optional[EncryptedKey]=None, decryptedKey: Optional[DecryptedKey]=None, **context):
        AsyncSpider.__init__(self, **self.from_locals(locals(), drop=ENCRYPTED_UNIQUE))
        self.set_secret(encryptedKey, decryptedKey)

    def is_kill(self, exception: Exception) -> bool:
        return isinstance(exception, ENCRYPTED_SPIDER_KILL) or isinstance(exception, self.killType)


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
    __metaclass__ = abc.ABCMeta
    asyncio = False
    operation = "pipeline"
    fields = list()
    derivFields = list()
    ranges = list()
    numRetries = 0
    delay = 1.
    cookies = str()
    interruptType = tuple()
    killType = tuple()
    errorType = tuple()
    returnType = "dataframe"
    mappedReturn = False
    globalMessage = str()
    globalProgress = False
    taskProgress = True
    taskErrors = dict()
    info = PipelineInfo()
    dags = Dag()

    def __init__(self, fields: Optional[IndexLabel]=None, ranges: Optional[RangeFilter]=None, returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, countryCode=str(), datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debugPoint: Optional[Keyword]=None, killPoint: Optional[Keyword]=None, extraSave: Optional[Keyword]=None,
                numRetries: Optional[int]=None, delay: Optional[Range]=None, cookies: Optional[str]=None,
                queryList: GoogleQueryList=list(), uploadList: GoogleUploadList=list(), alertInfo: AlertInfo=dict(),
                encryptedKey: Optional[EncryptedKey]=None, decryptedKey: Optional[DecryptedKey]=None,
                globalMessage=str(), globalProgress: Optional[bool]=None, taskProgress: Optional[bool]=None, **context):
        EncryptedSession.__init__(self, **self.from_locals(locals(), drop=PIPELINE_UNIQUE))
        self.set_global_progress(globalProgress, globalMessage, taskProgress)

    def set_global_progress(self, globalMessage=str(), globalProgress: Optional[bool]=None, taskProgress: Optional[bool]=None):
        self.globalMessage = exists_one(globalMessage, self.globalMessage, PIPELINE_DEFAULT_GLOBAL_MSG)
        self.globalProgress = globalProgress if isinstance(globalProgress, bool) else self.globalProgress
        self.taskProgress = taskProgress if isinstance(taskProgress, bool) else self.taskProgress

    @abc.abstractmethod
    def crawl(self, **context) -> Data:
        return self.gather(**context)

    ###################################################################
    ########################### Gather Task ###########################
    ###################################################################

    def gather(self, progress=True, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None, **context) -> Data:
        data = dict()
        for task in tqdm(self.dags, desc=self.globalMessage, disable=(not (self.globalProgress and progress))):
            data[task[DATANAME]] = self.run_task(task, fields=fields, data=data, progress=progress, **context)
        return self.map_reduce(fields=fields, returnType=returnType, **dict(context, **data))

    def run_task(self, task: Task, fields: IndexLabel=list(), data: Dict[_KT,Data]=dict(), **context) -> Data:
        fields = self.get_task_fields(fields, allowed=task.get(ALLOWED), name=task[DATANAME])
        method, worker, params = self._from_task(task, fields=fields, data=data, **context)
        response = method(**params)
        self.taskErrors[task[NAME]] = worker.errors
        self.checkpoint(task[NAME], where=method.__name__, msg={"data":response}, save=response)
        return response

    def get_task_fields(self, fields: Union[List,Dict], allowed: Optional[IndexLabel]=None, name=str()) -> IndexLabel:
        if self.mappedReturn:
            if name and isinstance(fields, Dict):
                fields = fields.get(name, list())
            return traversal_dict(fields, allowed, apply=self.set_allowed_fields, dropna=False)
        else: return self.set_allowed_fields(fields, allowed)

    def set_allowed_fields(self, fields: IndexLabel, allowed: Optional[IndexLabel]=None) -> IndexLabel:
        fields = cast_list(fields)
        if self.derivFields:
            fields = diff(fields, cast_list(self.derivFields))
        if allowed:
            fields = inter(fields, cast_list(allowed))
        return fields

    @EncryptedSession.limit_request
    def request_crawl(self, worker: Spider, **params) -> Data:
        return worker.crawl(**params)

    @abc.abstractmethod
    def map_reduce(self, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None, **context) -> Data:
        ...

    ###################################################################
    ############################ From Task ############################
    ###################################################################

    def _from_task(self, task: Task, fields: IndexLabel=list(), data: Dict[_KT,Data]=dict(),
                    **context) -> Tuple[Callable,Spider,Context]:
        method = getattr(self, task[TASK])
        task_filter = dict(fields=self.validate_task_fields(task[FIELDS], fields), returnType=task[DATATYPE])
        configs, params = self.set_task_params(task, **context)
        worker = task[OPERATOR](**task_filter, **configs)
        data = kloc(data, cast_list(task[DERIV]), if_null="drop") if (DERIV in task) and data else dict()
        params = dict(params, worker=worker, taskname=task[NAME], **task_filter, **data)
        self.checkpoint("params", where=method.__name__, msg=dict(zip(["task","configs","params"],[task[NAME],configs,params])))
        return method, worker, params

    def validate_task_fields(self, task_fields: Union[List,Dict,Tuple], fields: Union[List,Dict]) -> IndexLabel:
        if self.mappedReturn:
            return traversal_dict(task_fields, fields, apply=self.concat_task_fields, dropna=True)
        else: return self.concat_task_fields(task_fields, fields)

    def concat_task_fields(self, task_fields: IndexLabel, fields: IndexLabel) -> IndexLabel:
        if isinstance(task_fields, Tuple): return task_fields
        else: return unique(*cast_list(task_fields), *fields)

    def set_task_params(self, task: Task, **context) -> Tuple[Context,Context]:
        if PARAMS in task:
            params, configs = split_dict(context, task[PARAMS])
            configs = kloc(configs, WORKER_CONFIG+WORKER_EXTRA, if_null="drop")
        else:
            configs, params = split_dict(context, WORKER_CONFIG)
            params = PROXY_CONTEXT(**params)
        if CONTEXT in task:
            configs = dict(configs, **kloc(task[CONTEXT], WORKER_EXTRA, if_null="drop"))
            params = dict(params, **drop_dict(task[CONTEXT], WORKER_EXTRA, inplace=False))
        configs["progress"] = configs.get("progress", True) and self.taskProgress
        return configs, params


class AsyncPipeline(EncryptedAsyncSession, Pipeline):
    __metaclass__ = abc.ABCMeta
    operation = "pipeline"
    fields = list()
    derivFields = list()
    ranges = list()
    numRetries = 0
    delay = 1.
    cookies = str()
    interruptType = tuple()
    killType = tuple()
    errorType = tuple()
    returnType = "dataframe"
    mappedReturn = False
    globalMessage = str()
    globalProgress = False
    asyncMessage = str()
    asyncProgress = False
    taskProgress = True
    message = str()
    taskErrors = dict()
    info = PipelineInfo()
    dags = Dag()

    def __init__(self, fields: Optional[IndexLabel]=None, ranges: Optional[RangeFilter]=None, returnType: Optional[TypeHint]=None,
                tzinfo: Optional[Timezone]=None, countryCode=str(), datetimeUnit: Optional[Literal["second","minute","hour","day"]]=None,
                logName: Optional[str]=None, logLevel: LogLevel="WARN", logFile: Optional[str]=None, localSave=False,
                debugPoint: Optional[Keyword]=None, killPoint: Optional[Keyword]=None, extraSave: Optional[Keyword]=None,
                numRetries: Optional[int]=None, delay: Optional[Range]=None, cookies: Optional[str]=None, numTasks=100,
                queryList: GoogleQueryList=list(), uploadList: GoogleUploadList=list(), alertInfo: AlertInfo=dict(),
                encryptedKey: Optional[EncryptedKey]=None, decryptedKey: Optional[DecryptedKey]=None,
                globalMessage=str(), globalProgress: Optional[bool]=None, taskProgress: Optional[bool]=None,
                asyncMessage=str(), asyncProgress: Optional[bool]=None, **context):
        EncryptedAsyncSession.__init__(self, **self.from_locals(locals(), drop=PIPELINE_UNIQUE))
        self.set_global_progress(globalMessage, globalProgress, taskProgress)
        self.set_async_progress(asyncMessage, asyncProgress)

    def set_async_progress(self, asyncMessage=str(), asyncProgress: Optional[bool]=None):
        self.asyncMessage = exists_one(asyncMessage, self.asyncMessage, PIPELINE_DEFAULT_ASYNC_MSG)
        self.asyncProgress = asyncProgress if isinstance(asyncProgress, bool) else self.asyncProgress

    @abc.abstractmethod
    async def crawl(self, **context) -> Data:
        return await self.gather(**context)

    async def gather(self, progress=True, fields: IndexLabel=list(), returnType: Optional[TypeHint]=None, **context) -> Data:
        data = dict()
        for task in tqdm(self.dags, desc=self.globalMessage, disable=(not (self.globalProgress and progress))):
            if isinstance(task, Sequence):
                response = await tqdm.gather(*[
                    self.run_task(subtask, fields=fields, data=data, progress=progress, **context) for subtask in task],
                        desc=self.asyncMessage, disable=(self.globalProgress or (not (self.asyncProgress and progress))))
                data.update(dict(zip(vloc(task, DATANAME), response)))
            else: data[task[DATANAME]] = await self.run_task(task, fields=fields, data=data, progress=progress, **context)
        return self.map_reduce(fields=fields, returnType=returnType, **dict(context, **data))

    async def run_task(self, task: Task, fields: IndexLabel=list(), data: Dict[_KT,Data]=dict(), **context) -> Data:
        fields = self.get_task_fields(fields, allowed=task.get(ALLOWED), name=task[DATANAME])
        method, worker, params = self._from_task(task, fields=fields, data=data, **context)
        response = (await method(**params)) if inspect.iscoroutinefunction(method) else method(**params)
        self.taskErrors[task[NAME]] = worker.errors
        self.checkpoint(task[NAME], where=task, msg={"data":response}, save=response)
        return response

    @EncryptedAsyncSession.limit_request
    async def async_crawl(self, worker: AsyncSpider, **params) -> Data:
        return await worker.crawl(**params)
