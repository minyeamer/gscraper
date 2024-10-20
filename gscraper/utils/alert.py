from gscraper.base.abstract import CustomDict
from gscraper.base.types import _KT, _VT, Status, Datetime, Data

from gscraper.utils.cast import cast_numeric, cast_str
from gscraper.utils.date import get_datetime, get_datetime_pair, get_date, get_date_pair, get_weekday
from gscraper.utils.map import notna_dict, filter_data, regex_get, safe_len
from gscraper.utils.map import aggregate_data as agg

from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple, Union
import datetime as dt
import os
import re
import sys

import jwt
import requests
import time


DateFormat = Union[str,Tuple[str,str]]

HEADER = "header"
FOOTER = "footer"
NULL = "#N/A"

ENV_PATH = "env/"
PRIVATE_KEY = ENV_PATH+"private.key"

NAVER_WORKS_LIMIT = 2000
NATEON_LIMIT = 2000

DEFAULT_DATE_FORMAT = "%y-%m-%d({d})"
DEFAULT_DATE_END_FORMAT = "%m-%d-({d})"

DEFAULT_DATETIME_FORMAT = "%y-%m-%d({d}) %H:%M:%S"
DEFAULT_DATETIME_END_FORMAT = "%H:%M:%S"

DEFAULT_RANGE_FORMAT = "{start} to {end}"

DEFAULT_TEXT_BODY_FORMAT = "▶ [{name}] {length():,} rows collected"


###################################################################
########################### Format Text ###########################
###################################################################

class Mapping(dict):
    def __missing__(self, key: str) -> Any:
        return str() if key in (HEADER,FOOTER) else NULL


def format_map(__s: str, mapping: Dict) -> str:
    return __s.format_map(Mapping(mapping))


def get_format_map(data: Data, textFormat: str, textHeader: Optional[str]=None, textFooter: Optional[str]=None,
                dateFormat: Optional[DateFormat]=None, rangeFormat: Optional[str]=None,
                tzinfo=None, busdate=False, **kwargs) -> Dict:
    mapping = dict()
    if ("{header}" in textFormat) and isinstance(textHeader, str):
        textFormat = textFormat.replace("{header}", textHeader)
    if ("{footer}" in textFormat) and isinstance(textFooter, str):
        textFormat = textFormat.replace("{footer}", textFooter)
    for __key in set(regex_get(r"\{([^}]+)\}", textFormat, indices=[])):
        try:
            __key, __value = format_key_value(data, __key, dateFormat, rangeFormat, tzinfo, busdate, **kwargs)
            mapping[__key] = __value
        except: pass
    return mapping


def format_key_value(data: Data, __key: str, dateFormat: Optional[DateFormat]=None, rangeFormat: Optional[str]=None,
                    tzinfo=None, busdate=False, **kwargs) -> Tuple[_KT,_VT]:
    if re.match(r"^(date|datetime)\(.+\)$", __key):
        dateFunc, path = regex_get(r"^(\w+)\((.+)\)$", __key, groups=[0,1])
        __datetime = _get_date_object(kwargs.get(path), dateFunc, tzinfo=tzinfo, busdate=busdate)
        return __key, format_date(__datetime, (dateFormat if dateFormat else dateFunc), tzinfo=tzinfo)
    elif re.match(r"^(daterange|timerange)\([^,]+,[^,]+\)$", __key):
        dateFunc, start, end = regex_get(r"^(\w+)\(([^,]+),([^,]+)\)$", __key, groups=[0,1,2])
        __start, __end = _get_date_range_object(kwargs.get(start), kwargs.get(end), dateFunc, tzinfo=tzinfo, busdate=busdate)
        return __key, format_date_range(__start, __end, (dateFormat if dateFormat else dateFunc), rangeFormat, tzinfo=tzinfo)
    else: return __key.rsplit(':', maxsplit=1)[0], format_value(data, __key, **kwargs)


def format_value(data: Data, __key: str, **kwargs) -> _VT:
    __key, isNumber = (__key.rsplit(':', maxsplit=1)[0], True) if ':' in __key else (__key, False)
    if re.match(r"^\w+\(.*\)$", __key):
        aggFunc, path = regex_get(r"^(\w+)\((.*)\)$", __key, groups=[0,1])
        return aggregate_data(data, aggFunc, path)
    elif __key in kwargs:
        return cast_numeric(kwargs[__key]) if isNumber else cast_str(kwargs[__key], default=NULL)
    else: return 0 if isNumber else NULL


def aggregate_data(data: Data, aggFunc: str, path: str) -> Union[float,int]:
    if '>' in path:
        name, column = path.split('>', maxsplit=1)
        return aggregate_data(data[name], aggFunc, column)
    elif aggFunc == "length":
        return safe_len(data[path]) if path else safe_len(data)
    else:
        values = filter_data(data, path, if_null="drop") if path else data
        return cast_numeric(agg(values, aggFunc))


def split_lines(__s: str, maxLength: int, sep='\n', strip=True) -> List[str]:
    array, lines, length = list(), str(), 0
    for line in __s.split(sep):
        length += len(line)+len(sep)
        if length >= maxLength:
            array.append(lines.strip() if strip else lines)
            lines, length = line, len(line)
        else: lines = sep.join([lines, line]) if lines else line
    if lines: array.append(lines.strip() if strip else lines)
    return array


###################################################################
########################### Format Date ###########################
###################################################################

def format_date(__datetime: Datetime, dateFormat: str, tzinfo=None) -> str:
    dateFormat = get_date_format(dateFormat)
    if not (isinstance(__datetime, (dt.date,dt.datetime)) and dateFormat):
        return NULL
    elif ("{d}" in dateFormat) or ("{ddd}" in dateFormat):
        mapping = {__k: get_weekday(__datetime, __f, tzinfo) for __k, __f in zip(["d","ddd"],["short","long"])}
        dateFormat = format_map(dateFormat, mapping)
    return __datetime.strftime(dateFormat.encode("unicode-escape").decode()).encode().decode("unicode-escape")


def format_date_range(__start: Datetime, __end: Datetime, dateFormat: DateFormat, rangeFormat=str(), tzinfo=None) -> str:
    (startFormat, endFormat), rangeFormat = get_date_range_format(dateFormat, rangeFormat)
    if __start == __end:
        return format_date(__start, dateFormat=startFormat, tzinfo=tzinfo)
    elif not (isinstance(__start, (dt.date,dt.datetime)) and (type(__start) == type(__end)) and startFormat and endFormat):
        return format_map(rangeFormat, dict(start=NULL, end=NULL))
    if isinstance(__start, dt.datetime) and re.search(r"(%[^\s]*H|%[^\s]*M|%[^\s]*S)", startFormat):
        endFormat = endFormat if __start.date() == __end.date() else startFormat
    else: endFormat = endFormat if __start.year == __end.year else startFormat
    __start = format_date(__start, dateFormat=startFormat, tzinfo=tzinfo)
    __end = format_date(__end, dateFormat=endFormat, tzinfo=tzinfo)
    return rangeFormat.format_map(dict(start=__start, end=__end))


def get_date_format(dateFormat=str()) -> str:
    if dateFormat == "date": return DEFAULT_DATE_FORMAT
    elif dateFormat == "datetime": return DEFAULT_DATETIME_FORMAT
    else: return dateFormat if isinstance(dateFormat, str) else str()


def get_date_end_format(dateFormat=str()) -> str:
    if dateFormat in "date": return DEFAULT_DATE_END_FORMAT
    elif dateFormat == "datetime": return DEFAULT_DATETIME_END_FORMAT
    else: return dateFormat if isinstance(dateFormat, str) else str()


def get_date_range_format(dateFormat: Optional[DateFormat]=None, rangeFormat=str()) -> Tuple[Tuple[str,str],str]:
    rangeFormat = rangeFormat if rangeFormat else DEFAULT_RANGE_FORMAT
    if dateFormat == "daterange": return (DEFAULT_DATE_FORMAT, DEFAULT_DATE_END_FORMAT), rangeFormat
    elif dateFormat == "timerange": return (DEFAULT_DATETIME_FORMAT, DEFAULT_DATETIME_END_FORMAT), rangeFormat
    elif isinstance(dateFormat, str):
        return (get_date_format(dateFormat), get_date_end_format(dateFormat)), rangeFormat
    elif isinstance(dateFormat, Sequence) and (len(dateFormat) == 2):
        return (get_date_format(dateFormat[0]), get_date_end_format(dateFormat[1])), rangeFormat
    else: return (str(), str()), rangeFormat


def _get_date_object(__object, dateFunc: Literal["date","datetime"]=str(), tzinfo=None, busdate=False) -> Datetime:
    if isinstance(__object, (dt.date,dt.datetime)): return __object
    elif dateFunc == "date": return get_date(__object, if_null=None, tzinfo=tzinfo, busdate=busdate)
    elif dateFunc == "datetime": return get_datetime(__object, if_null=None, tzinfo=tzinfo)
    else: return None


def _get_date_range_object(__start, __end, dateFunc: Literal["daterange","timerange"]=str(),
                            tzinfo=None, busdate=False) -> Tuple[Datetime,Datetime]:
    if dateFunc == "daterange": return get_date_pair(__start, __end, if_null=None, tzinfo=tzinfo, busdate=busdate)
    elif dateFunc == "timerange": return get_datetime_pair(__start, __end, if_null=None, tzinfo=tzinfo)
    else: return (None, None)


###################################################################
########################### Private Key ###########################
###################################################################

def get_sys_file(file: str) -> str:
    base_path = getattr(sys, '_MEIPASS', os.path.abspath(os.path.dirname(__file__)))
    return os.path.join(base_path, file)


def read_file(file: str) -> Union[str,bytes]:
    if os.path.exists(file):
        with open(file, "rb") as f:
            return f.read()
    else: return file


def read_private_key(file=str()) -> Union[str,bytes]:
    if isinstance(file, str) and file.endswith(".key") and os.path.exists(file): pass
    elif os.path.exists(PRIVATE_KEY): file = PRIVATE_KEY
    else: file = get_sys_file(PRIVATE_KEY.split('/')[-1])
    return read_file(file)


###################################################################
######################### NAVER WORKS API #########################
###################################################################

class AlertInfo(CustomDict):
    def __init__(self, clientId: str, clientSecret: str, serviceAccount: str, botId: str,
                channelId=str(), userId=str(), limit=2000, strip=True, privateKey: Union[str,bytes]=str(),
                textFormat: Optional[str]=None, textHeader: Optional[str]=None, textBody: Optional[str]=None, textFooter: Optional[str]=None,
                dateFormat: Optional[DateFormat]=None, rangeFormat: Optional[str]=None, name: Optional[str]=None):
        authInfo = dict(clientId=clientId, clientSecret=clientSecret, serviceAccount=serviceAccount, privateKey=privateKey)
        botInfo = dict(botId=botId, channelId=channelId, userId=userId, limit=min(limit,NAVER_WORKS_LIMIT), strip=strip)
        textInfo = notna_dict(name=name, dateFormat=dateFormat, rangeFormat=rangeFormat)
        textInfo.update(self.set_text_format(textFormat, textHeader, textBody, textFooter))
        super().__init__(authInfo=authInfo, botInfo=botInfo, textInfo=textInfo)

    def set_text_format(self, textFormat=None, textHeader=None, textBody=None, textFooter=None) -> Dict[str,str]:
        textFormat = textFormat if textFormat else ("{header}"+(textBody if textBody else DEFAULT_TEXT_BODY_FORMAT)+"{footer}")
        return notna_dict(textFormat=textFormat, textHeader=textHeader, textFooter=textFooter)


class NaverBot(requests.Session):
    accessToken = str()

    def __init__(self, clientId: str, clientSecret: str, serviceAccount: str, privateKey: Union[str,bytes]=str()):
        super().__init__()
        privateKey = privateKey if isinstance(privateKey, bytes) else read_private_key(privateKey)
        self.accessToken = self.authorize(clientId, clientSecret, serviceAccount, privateKey)

    def authorize(self, clientId: str, clientSecret: str, serviceAccount: str, privateKey: Union[str,bytes], expires=60*5) -> str:
        url = "https://auth.worksmobile.com/oauth2/v2.0/token"
        params = dict(
            assertion=self.encode_jwt(clientId, serviceAccount, privateKey, expires),
            grant_type="urn:ietf:params:oauth:grant-type:jwt-bearer",
            client_id=clientId,
            client_secret=clientSecret,
            scope="bot"
        )
        response = self.post(url, params=params)
        return response.json()["access_token"]

    def encode_jwt(self, clientId: str, serviceAccount: str, privateKey: Union[str,bytes], expires=60*5) -> str:
        headers = {"alg":"RS256", "typ":"JWT"}
        payload = {"iss":clientId, "sub":serviceAccount, "iat":int(time.time()), "exp":int(time.time())+expires}
        return jwt.encode(payload, privateKey, algorithm="HS256", headers=headers)

    def send_text(self, text: str, botId: str, channelId=str(), userId=str(), limit=2000, strip=True) -> Status:
        __id, __type = (channelId if channelId else userId), ("channels" if channelId else "users")
        url = f"https://www.worksapis.com/v1.0/bots/{botId}/{__type}/{__id}/messages"
        headers = {"Authorization":f"Bearer {self.accessToken}", "Content-Type":"application/json"}
        status = list()
        for lines in split_lines((text.strip() if strip else text), maxLength=limit, strip=strip):
            data = {"content":{"type":"text", "text":lines}}
            response = self.post(url, headers=headers, json=data)
            status.append(response.status_code)
        return status[0] if len(status) == 1 else status


###################################################################
############################ NateOn API ###########################
###################################################################

def alert_nateon(nateonUrl: str, content: str, limit=(2000-100)) -> Status:
    status, headers = list(), {"Content-Type":"application/x-www-form-urlencoded"}
    for lines in split_lines(content, maxLength=limit):
        data = {"content":lines}
        status.append(requests.post(nateonUrl, headers=headers, data=data).status_code)
    return status[0] if len(status) == 1 else status
