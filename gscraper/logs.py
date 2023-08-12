from ast import literal_eval
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
    def __init__(self, name=__name__, level=WARN, file=str()):
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
