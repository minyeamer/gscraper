from gscraper.base.types import IndexLabel, JsonData

from gscraper.utils.cast import cast_datetime, cast_date
from gscraper.utils.map import re_get

from typing import Dict, List, Literal, Union
from ast import literal_eval
from bs4 import BeautifulSoup
from bs4.element import Tag
from http.cookies import SimpleCookie
from requests.cookies import RequestsCookieJar
from urllib.parse import quote, urlencode, urlparse

import datetime as dt
import json
import re

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')


###################################################################
############################# Requests ############################
###################################################################

clean_html = lambda html: BeautifulSoup(str(html), "lxml").text
clean_tag = lambda source: re.sub("<[^>]*>", "", str(source))


def parse_cookies(cookies: Union[RequestsCookieJar,SimpleCookie]) -> str:
    return '; '.join([str(key)+"="+str(value) for key, value in cookies.items()])


def parse_parth(url: str) -> str:
    return re.sub(urlparse(url).path+'$','',url)


def parse_origin(url: str) -> str:
    return re_get(f"(.*)(?={urlparse(url).path})", url) if urlparse(url).path else url


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
############################## Source #############################
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


def select_text(source: Tag, selector: str, pattern='\n', sub=' ', multiple=False) -> Union[str,List[str]]:
    try:
        if multiple: return [re.sub(pattern, sub, select.text).strip() for select in source.select(selector)]
        else: return re.sub(pattern, sub, source.select_one(selector).text).strip()
    except (AttributeError, IndexError, TypeError):
        return list() if multiple else str()


def select_attr(source: Tag, selector: str, key: str, default=None, multiple=False) -> Union[str,List[str]]:
    try:
        if multiple: return [select.attrs.get(key,default).strip() for select in source.select(selector)]
        else: return source.select_one(selector).attrs.get(key,default).strip()
    except (AttributeError, IndexError, TypeError):
        return list() if multiple else str()


def select_datetime(source: Tag, selector: str, default=None, multiple=False) -> dt.datetime:
    if multiple:
        return [cast_datetime(text, default) for text in select_text(source, selector, multiple=True)]
    else:
        return cast_datetime(select_text(source, selector, multiple=False), default)


def select_date(source: Tag, selector: str, default=None, multiple=False) -> dt.datetime:
    if multiple:
        return [cast_date(text, default) for text in select_text(source, selector, multiple=True)]
    else:
        return cast_date(select_text(source, selector, multiple=False), default)


def match_class(source: Tag, class_name: str) -> bool:
    try:
        class_list = source.attrs.get("class")
        if isinstance(class_list, List):
            return class_name in class_list
        return class_name == class_list
    except (AttributeError, TypeError):
        return False
