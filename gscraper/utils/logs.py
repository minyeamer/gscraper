from gscraper.base.types import LogLevel, LogMessage, Shape, TabularData, Data
from gscraper.base.types import is_records, is_dfarray, is_tag_array
from gscraper.utils.map import chain_dict, regex_get

from typing import Any, Dict, List, Optional, Tuple, Union
from ast import literal_eval
from aiohttp import ClientResponse
from requests import Response
import logging

from bs4 import Tag
from pandas import DataFrame, Series
import json
import os
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

GENERAL_LOG = "[%(levelname)s] %(name)s(%(module)s) line %(lineno)d: %(funcName)s - %(message)s | %(asctime)s"
JSON_LOG = '{"levelname":"%(levelname)s", "name":"%(name)s", "module":"%(module)s", \
"lineno":"%(lineno)d", "funcName":"%(funcName)s", "message":"%(message)s", "asctime":"%(asctime)s"},'


unquote = lambda s: str(s).replace('\"','').replace('\'','')
unmap = lambda s: str(s).replace('{','(').replace('}',')').replace('\"','`').replace('\'','`')
unraw = lambda s: str(s).replace('\\','/').replace(r'\n','\n').replace('\"','`').replace('\'','`')


###################################################################
############################## Logger #############################
###################################################################

class CustomLogger(logging.Logger):
    def __init__(self, name=__name__, level: LogLevel=WARN, file=str()):
        super().__init__(name, level)
        format = JSON_LOG if file else GENERAL_LOG
        formatter = logging.Formatter(fmt=format, datefmt="%Y-%m-%d %H:%M:%S")
        handler = logging.FileHandler(file, encoding="utf-8") if file else logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(formatter)
        self.addHandler(handler)


def fit_json_log(log_file=str()):
    if not os.path.exists(log_file): return
    with open(log_file, 'r', encoding="utf-8") as __input:
        logs = __input.read().strip().split('\n')
        if logs:
            with open(log_file, 'w', encoding="utf-8") as __output:
                if logs[0] == '[': __output.write(parse_history(logs, indent=1)+'\n')
                else: __output.write('[\n' + parse_logs(logs, indent=1) + '\n]\n')


def parse_history(logs: List[str], indent=1) -> str:
    symbols = [__i for __i, log in enumerate(logs) if log == ']']
    if not symbols: history, new = logs[1:], list()
    elif len(symbols) == 1: history, new = logs[1:symbols[0]], logs[symbols[0]+1:]
    else: history, new = logs[1:symbols[0]], [log for log in logs[symbols[0]+1:] if log != ']']
    history, new = re.sub(',$', '', '\n'.join(history)), parse_logs(new, indent)
    return '[\n' + history + ((',\n' + new) if new else str()) + '\n]'


def parse_logs(logs: List[str], indent=1) -> str:
    return re.sub(',$', '', '\n'.join([('\t'*indent)+parse_message(re.sub(',$', '', log).strip())+',' for log in logs]))


def parse_message(message: str) -> str:
    msg_part = regex_get(r'"message":"\{.*\}",', message, indices=0)
    if msg_part:
        msg = {__k: (list(__v) if isinstance(__v, Tuple) else __v) for __k, __v in load_dict(msg_part[11:-2]).items()}
        info = chain_dict([load_dict(message.replace(msg_part, '')), msg], keep="first")
    else: info = load_dict(message)
    message = json.dumps(info, ensure_ascii=False, default=str)
    return message.replace('\'','\"').replace('`','\'').replace("\": ","\":")


def load_dict(__s: str) -> Dict:
    __s = re.sub(r"datetime.date\((\d+), (\d+), (\d+)\)", convert_date, __s)
    try: return literal_eval(__s)
    except ValueError:
        __m = dict()
        for kv in __s[1:-1].split(', '):
            try: __m.update(literal_eval('{' + kv + '}'))
            except:
                key, value = kv.split(': ', maxsplit=1)
                __m.update(literal_eval('{' + key + ': ' + '\''+value.replace('\'','\\\'').replace('\"','\\\'')+'\'' + '}'))
        return __m


def convert_date(match: re.Match) -> str:
    year = match.group(1)
    month = str(match.group(2)).zfill(2)
    day = str(match.group(3)).zfill(2)
    return f'"{year}-{month}-{day}"'


###################################################################
########################### Log Message ###########################
###################################################################

def dumps_map(__object) -> str:
    return unmap(json.dumps(__object, ensure_ascii=False, default=unquote))

def dumps_exc() -> str:
    unraw('\\n'.join(traceback.format_exception(*sys.exc_info())))

def _limit(__object: Union[Data,str], limit=3000) -> Union[Data,str]:
    if isinstance(__object, str):
        return __object[:limit]
    elif isinstance(__object, List):
        if len(str(__object)) < limit: return __object
        else: return _limit(__object[0], limit=limit)
    elif isinstance(__object, Dict):
        if len(str(__object)) < limit: return __object
        key_limit = 5 if len(__object) > 5 else (1 if len(__object) > 1 else 0)
        if key_limit == 0: return _limit(str(__object), limit=limit)
        else: return _limit(dict(list(__object.items())[:key_limit]), limit=limit)
    else: return str(__object)[:limit]

def _info_data(__object, limit=3000, depth=3) -> Union[Tuple[Shape,Any],Any]:
    if isinstance(__object, Dict):
        __object = dumps_key_value(__object, limit=limit, depth=depth-1)
    if isinstance(__object, DataFrame):
        shape = __object.shape
        __object = __object.to_dict("records")
    if isinstance(__object, Series):
        shape = __object.shape
        __object = __object.tolist()
    elif isinstance(__object, Tag):
        __object = str(__object).replace('\n', ' ')
        shape = (len(__object),)
    elif is_records(__object, empty=False):
        shape = (len(__object), len(__object[0]))
    elif is_dfarray(__object, empty=False) or is_tag_array(__object, empty=False):
        return [_info_data(__e, limit=max(100,(limit//len(__object)))) for __e in __object]
    elif (not limit) or (__object == None) or ((limit > 0) and (len(str(__object)) < limit)):
        return __object
    else: shape = (len(__object),) if isinstance(__object, Dict) else len(str(__object))
    return (shape, (_limit(__object, limit) if limit > 0 else __object))

def dumps_key_value(__m: Dict, limit=3000, depth=3) -> Dict:
    _dumps = lambda kwargs: (kwargs[0],
        _info_data(kwargs[1], limit=limit, depth=depth) if depth > 0 else kwargs[1])
    return dict(map(_dumps, __m.items()))

def dumps_data(__object, limit=3000, depth=3) -> Any:
    if isinstance(__object, Dict): return dumps_key_value(__object, limit=limit, depth=depth)
    else: return _info_data(__object, limit=limit)

def dumps(__object, dump=False, limit=3000, depth=3) -> Any:
    __object = dumps_data(__object, limit=limit, depth=depth)
    return dumps_map(__object) if dump else __object


def log_encrypt(show=3, **kwargs) -> LogMessage:
    encrypt = lambda string, show=3: str(string)[:show].ljust(len(str(string)),'*')
    return dict(**{key:encrypt(value, show=show) for key, value in kwargs.items()})


def log_messages(params: Optional[Dict]=None, data: Optional[Dict]=None, json: Optional[Dict]=None,
                headers: Optional[Dict]=None, cookies=None, dump=False, **kwargs) -> LogMessage:
    params = dict(params=dumps(params, dump=dump)) if params else dict()
    data = dict(data=dumps(data if data else json, dump=dump)) if data or json else dict()
    headers = dict(headers=dumps(headers, dump=dump)) if headers else dict()
    cookies = dict(cookies=dumps(cookies, dump=dump)) if cookies else dict()
    kwargs = {key: dumps(values, dump=dump) for key, values in kwargs.items()}
    return dict(**kwargs, **data, **params, **headers, **cookies)


def log_response(response: Response, url: str, **kwargs) -> LogMessage:
    try: length = len(response.text)
    except: length = None
    return dict(**kwargs, **{"url":unquote(url), "status":response.status_code, "contents-length":length})


async def log_client(response: ClientResponse, url: str, **kwargs) -> LogMessage:
    try: length = len(await response.text())
    except: length = None
    return dict(**kwargs, **{"url":unquote(url), "status":response.status, "contents-length":length})


def log_data(data: Data, **kwargs) -> LogMessage:
    try: length = int(bool(data)) if isinstance(data, Dict) else len(data)
    except: length = None
    return dict(**kwargs, **{"data-length":length})


def log_exception(func: str, dump=False, **kwargs) -> LogMessage:
    error = '\\n'.join(traceback.format_exception(*sys.exc_info()))
    error = unraw(error) if dump else error
    return dict(func=func, kwargs=dumps(kwargs, dump=dump), error=error)


def log_table(data: TabularData, schema: Optional[List]=None, dump=False, **kwargs) -> LogMessage:
    schema = dict(schema=dumps(schema, dump=dump)) if schema else dict()
    if not isinstance(data, (DataFrame,Series)):
        try: shape = (len(data),)
        except: shape = (0,)
    else: shape = data.shape
    return dict(**kwargs, **{"table-shape":shape}, **schema)
