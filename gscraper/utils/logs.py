from gscraper.base.types import LogLevel, TabularData, Data

from typing import Any, Dict, List, Optional, Sequence, Union
from aiohttp import ClientResponse
from requests import Response
import logging

from bs4 import Tag
from pandas import DataFrame, Series
import json
import re
import sys
import traceback


LOG_FILE = "log"
CRITICAL = 50
FATAL = CRITICAL
ERROR = 40
WARNING = 30
WARN = WARNING
INFO = 20
DEBUG = 10
NOTSET = 0

LOG_FORMAT = "%(asctime)s [%(levelname)s] <%(name)s> %(module)s:%(lineno)d %(funcName)s() | %(message)s"
LOG_FORMAT2 = "[%(levelname)s] <%(name)s> %(module)s:%(lineno)d %(funcName)s() | %(message)s"

JSON_LOG_FORMAT = '{"asctime": "%(asctime)s", "level": "%(levelname)s", "name": "%(name)s", \
"module": "%(module)s", "line": %(lineno)d, "func": "%(funcName)s", "message": %(message)s},'


def get_default_log_format(file=str()) -> str:
    if not (isinstance(file, str) and file): return LOG_FORMAT2
    elif file.endswith(".json"): return JSON_LOG_FORMAT
    else: return LOG_FORMAT


###################################################################
############################## Logger #############################
###################################################################

class CustomLogger(logging.Logger):
    def __init__(self, name=__name__, level: LogLevel=WARN, file=str(), fmt=str(), datefmt="%Y-%m-%d %H:%M:%S"):
        super().__init__(name, level)
        formatter = logging.Formatter(fmt=(fmt if fmt else get_default_log_format(file)), datefmt=datefmt)
        handler = logging.FileHandler(file, encoding="utf-8") if file else logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(formatter)
        self.addHandler(handler)


def get_log_level(level: Union[int,str]) -> int:
    if isinstance(level, str):
        if not level: return None
        elif level.isdigit(): return int(level)
        else: return attr if isinstance(attr := getattr(logging, level.upper(), None), int) else None
    else: return int(level) if isinstance(level, (float,int)) else None


def format_json_log(path: str):
    with open(path, "r", encoding="utf-8") as file:
        logs = file.read().rstrip()
        logs = re.sub(r",*\n]\n", ",\n", logs)
        if logs[0] != '[':
            logs = '[\n' + logs
        if logs[-1] != ']':
            logs = (logs[:-1] if logs[-1] == ',' else logs) + '\n]\n'
    with open(path, "w", encoding="utf-8") as file:
        file.write(logs)


###################################################################
########################### Log Message ###########################
###################################################################

def _limit(__object: Union[Data,str], limit=1000) -> Union[Data,str]:
    if (not isinstance(limit, int)) or (len(str(__object)) < limit):
        return __object
    elif isinstance(__object, str):
        return __object[:limit] + "..."
    elif isinstance(__object, List):
        if len(str(__object[:5])) < limit: return __object[:5] + ["..."]
        else: return [_limit(__object[0], limit), "..."]
    elif isinstance(__object, Dict):
        if len(str(__object)) < limit: return __object
        __object = {__key: "..." for __key in __object.keys()}
        return __object if len(str(__object)) < limit else {"...": "..."}
    else: return str(__object)[:limit]


def limit_data(__object, limit=1000, depth=2, data_only=True) -> Union[Any,Dict]:
    if isinstance(__object, str):
        return _limit(__object, limit) if data_only else {"data-type":"str", "data-length":len(__object), "data":_limit(__object, limit)}
    elif isinstance(__object, Dict):
        __data = _limit((__object if depth < 1 else
            {__key: limit_data(__value, limit=limit, depth=(depth-1)) for __key, __value in __object.items()}), limit)
        return __data if data_only else {"data-type":"Dict", "dict-keys":len(__object), "data":__data}
    elif isinstance(__object, Sequence):
        __data = _limit((__object if depth < 1 else
            [(limit_data(__e, limit=limit, depth=(depth-1)) if isinstance(__e, Dict) else __e) for __e in __object]), limit)
        __length = [len(__object), len(__data[0])] if __data and isinstance(__data[0], Dict) else len(__object)
        return __data if data_only else {"data-type":"List", "data-length":__length, "data":__data}
    elif isinstance(__object, DataFrame):
        if depth > 0: __data = limit_data(__object.to_dict("records"), limit=limit, depth=(depth-1))
        else: __data = _limit(__object.to_dict("records"), limit)
        return __data if data_only else {"data-type":"DataFrame", "data-shape":list(__object.shape), "data":__data}
    elif isinstance(__object, Series):
        if depth > 0: __data = limit_data(__object.tolist(), limit=limit, depth=(depth-1))
        else: __data = _limit(__object.tolist(), limit)
        return __data if data_only else {"data-type":"Series", "data-length":len(__object), "data":__data}
    elif isinstance(__object, Tag):
        __object = __object.prettify()
        return _limit(__object, limit) if data_only else {"data-type":"Tag", "data-length":len(__object), "data":_limit(__object, limit)}
    else: return _limit(__object, limit) if data_only else {"data-type":str(type(__object)), "data":_limit(__object, limit)}


def log_object(object: Dict, indent=None, ensure_ascii=False, default=str, **options) -> str:
    return json.dumps(object, indent=indent, ensure_ascii=ensure_ascii, default=default, **options)


def log_encrypt(show=3, **kwargs) -> str:
    encrypt = lambda string, show=3: str(string)[:show].ljust(len(str(string)),'*')
    return log_object(dict(**{key:encrypt(value, show=show) for key, value in kwargs.items()}))


def log_messages(params: Optional[Dict]=None, data: Optional[Dict]=None, json: Optional[Dict]=None,
                headers: Optional[Dict]=None, cookies=None, **kwargs) -> str:
    params = dict(params=limit_data(params, limit=3000)) if params else dict()
    data = dict(data=limit_data((data if data else json), limit=3000)) if data or json else dict()
    headers = dict(headers=limit_data(headers, limit=3000)) if headers else dict()
    cookies = dict(cookies=limit_data(cookies, limit=3000)) if cookies else dict()
    return log_object(dict(**limit_data(kwargs), **data, **params, **headers, **cookies))


def log_response(response: Response, url: str, **kwargs) -> str:
    try: length = len(response.text)
    except: length = None
    return log_object(dict(**kwargs, **{"url":url, "status":response.status_code, "contents-length":length}))


async def log_client(response: ClientResponse, url: str, **kwargs) -> str:
    try: length = len(await response.text())
    except: length = None
    return log_object(dict(**kwargs, **{"url":url, "status":response.status, "contents-length":length}))


def log_data(data: Data, **kwargs) -> str:
    return log_object(dict(**limit_data(kwargs), data=limit_data(data, data_only=False)))


def log_error(func: str, **kwargs) -> str:
    error = ''.join(traceback.format_exception(*sys.exc_info()))
    return log_object(dict(func=func, kwargs=limit_data(kwargs), error=error))


def log_table(data: TabularData, schema: Optional[List]=None, **kwargs) -> str:
    schema = dict(schema=limit_data(schema, limit=3000)) if schema else dict()
    if not isinstance(data, (DataFrame,Series)):
        try: shape = (len(data),)
        except: shape = (0,)
    else: shape = data.shape
    return log_object(dict(**kwargs, **{"table-shape":shape}, **schema))
