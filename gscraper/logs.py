from .types import LogLevel, LogMessage, Data, BigQuerySchema

from typing import Dict, Optional
from ast import literal_eval
from aiohttp import ClientResponse
from pandas import DataFrame
from requests import Response
import json
import logging
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
dumps_map = lambda struct: unmap(json.dumps(struct, ensure_ascii=False, default=unquote))
dumps_exc = lambda: unraw(''.join(traceback.format_exception(*sys.exc_info())))

class CustomLogger(logging.Logger):
    def __init__(self, name=__name__, level: LogLevel=WARN, file=str()):
        super().__init__(name, level)
        format = JSON_LOG if file else GENERAL_LOG
        formatter = logging.Formatter(fmt=format, datefmt="%Y-%m-%d %H:%M:%S")
        handler = logging.FileHandler(file, encoding="utf-8") if file else logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(formatter)
        self.addHandler(handler)


def parse_message(message: str):
    msg_part = re.search('"message":"\{.*\}",', message)
    if msg_part:
        dict_msg = str(literal_eval(re.search('(?<="message":")(\{.*\})(?=",)', message).group()))
        dict_msg = dict_msg.replace('\'','\"').replace('`','\'').replace("\": ","\":")[1:-1]+','
        message = message.replace(msg_part.group(), dict_msg)
    return message.replace(":True",":true").replace(":False",":false").replace(":None",":null")


def fit_json_log(log_file=str()):
    if not os.path.exists(log_file): return
    with open(log_file, "r", encoding="utf-8") as f:
        log = f.readlines()
        if '}\n' in log:
            new_index = log.index('}\n')+1
            if len(log) == new_index: return
            new_log = [' '*4+line for line in log[new_index:]]
            new_log = [parse_message(line) for line in new_log]
            new_log[-1] = re.sub(",$", "", new_log[-1])
            log[new_index-3] = log[new_index-3].replace("\n", ",\n").replace("[,","[")
            log = log[:new_index-2]+new_log+log[new_index-2:new_index]
        else:
            log = [parse_message(line) for line in log]
            log = ["{\n", '  "log": [\n']+[' '*4+line for line in log]+["  ]\n","}\n"]
            log[-3] = re.sub(",$", "", log[-3])
    with open(log_file, "w", encoding="utf-8") as f:
        f.writelines(log)


def log_encrypt(show=3, **kwargs) -> LogMessage:
    encrypt = lambda string, show=3: str(string[:show]).ljust(len(string),'*')
    return dict(**{key:encrypt(value, show=show) for key, value in kwargs.items()})


def log_messages(params: Optional[Dict]=None, data: Optional[Dict]=None, json: Optional[Dict]=None,
                headers: Optional[Dict]=None, cookies=None, logJson=False, **kwargs) -> LogMessage:
    dumps = lambda struct: dumps_map(struct) if logJson else str(struct)
    params = {"params":dumps(params)} if params else dict()
    data = {"data":dumps(data if data else json)} if data or json else dict()
    headers = {"headers":dumps(headers)} if headers else dict()
    cookies = {"cookies":dumps(cookies)} if cookies else dict()
    kwargs = {key:dumps(values) for key,values in kwargs.items()}
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


def log_exception(func: str, logJson=False, **kwargs) -> LogMessage:
    dumps = lambda struct: (dumps_map(struct) if logJson else str(struct))[:1000]
    error = ''.join(traceback.format_exception(*sys.exc_info()))
    error = unraw(error) if logJson else error
    return dict(func=func, kwargs=dumps(kwargs), error=error)


def log_table(data: DataFrame, schema: Optional[BigQuerySchema]=None, logJson=False, **kwargs) -> LogMessage:
    dumps = lambda struct: dumps_map(struct) if logJson else str(struct)
    schema = {"schema":dumps(schema)} if schema else dict()
    try: shape = data.shape
    except: shape = (0,0)
    return dict(**kwargs, **{"table-shape":str(shape)}, **schema)
