from gscraper.base.abstract import CustomDict, INVALID_OBJECT_TYPE_MSG
from gscraper.base.types import _KT, _VT, Arguments, Context, TypeHint, IndexLabel, JsonData
from gscraper.utils.map import regex_get, replace_map

from http.cookies import SimpleCookie
from requests.cookies import RequestsCookieJar
from urllib.parse import quote, urlencode, urlparse
import asyncio
import aiohttp
import requests

from typing import Any, Coroutine, Dict, List, Literal, Optional, Union
from ast import literal_eval
import json
import re


###################################################################
############################# Headers #############################
###################################################################

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
}


def get_content_type(content_type=str(), urlencoded=False, utf8=False) -> str:
    if urlencoded or (content_type == "urlencoded"):
        __type = "application/x-www-form-urlencoded"
    elif content_type == "javascript": __type = "javascript"
    elif content_type == "json": __type = "application/json"
    elif content_type == "text": __type = "text/plain"
    elif "WebKitFormBoundary" in content_type:
        return f"multipart/form-data; boundary={content_type}"
    else: __type = content_type if isinstance(content_type, str) else str()
    return __type+(("; charset=UTF-8") if utf8 else str())


def get_headers(authority=str(), referer=str(), cookies=str(), host=str(),
                origin: Union[bool,str]=False, secure=False,
                content_type=str(), urlencoded=False, utf8=False, xml=False, **kwargs) -> Dict[str,str]:
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
############################# Asyncio #############################
###################################################################

def apply_nest_asyncio():
    import nest_asyncio
    nest_asyncio.apply()


def run_async_loop() -> asyncio.AbstractEventLoop:
    try: return asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def close_async_loop(loop: asyncio.AbstractEventLoop):
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()


def asyncio_run(func: Coroutine, loop: asyncio.AbstractEventLoop, args: Arguments=tuple(), kwargs: Context=dict()) -> Any:
    task = asyncio.create_task(func(*args, **kwargs))
    return loop.run_until_complete(task)


###################################################################
############################# GraphQL #############################
###################################################################

class GraphQLObject(CustomDict):
    def format(self, __object: Union[Dict,List,str], indent=0, step=2, linebreak=True, colons=False) -> str:
        indent, seq = (indent if linebreak else 0), ('\n' if linebreak else ', ')
        if isinstance(__object, Dict):
            return seq.join([self._format_kv(__k, __v, indent, step, linebreak, colons) for __k,__v in __object.items()])
        elif isinstance(__object, List):
            return seq.join([self.format(__e, indent, step, linebreak, colons) for __e in __object])
        elif isinstance(__object, str): return (' '*indent)+__object
        else: raise TypeError(INVALID_OBJECT_TYPE_MSG(__object, GraphQLObject))

    def _format_kv(self, __key: _KT, __value: _VT, indent=0, step=2, linebreak=True, colons=False) -> str:
        indent, seq = (indent if linebreak else 0), ('\n' if linebreak else '')
        __body = '{'+seq+self.format(__value, indent+step, step, linebreak, colons)+seq+(' '*indent)+'}'
        return f"{(' '*indent)}{__key}{(':' if colons else '')} {__body}"


class GraphQLVariables(GraphQLObject):
    variables = None

    def __init__(self, variables: Union[Dict,List]):
        self.variables = self.validate_variables(variables)
        CustomDict.__init__(self)

    def get_variables(self) -> Union[Dict,List]:
        return self.variables

    def validate_variables(self, variables: Union[Dict,List]) -> Union[Dict,List]:
        if isinstance(variables, (Dict,List)):
            return variables
        else: raise TypeError(INVALID_OBJECT_TYPE_MSG(variables, GraphQLVariables))

    def generate_variables(self, indent=4, step=2, linebreak=True, colons=True, prefix=str(), suffix=str(), replace=dict(), **kwargs) -> str:
        if isinstance(self.variables, Dict):
            __formatted = self.format_variables_dict(self.variables, indent, step, linebreak, colons)
        else: __formatted = self.format_variables(self.variables, indent, step, linebreak, bracket=True)
        return prefix+(replace_map(__formatted, **replace) if replace else __formatted)+suffix

    def format_variables(self, variables: List[str], indent=4, step=2, linebreak=True, bracket=True) -> str:
        if linebreak:
            variables = ('\n'+' '*indent).join([(__name+': $'+__name) for __name in variables])
            return ('('*bracket)+'\n'+(' '*indent)+variables+'\n'+(' '*max(indent-step,0))+(')'*bracket)
        else: return ('('*bracket)+", ".join([(__name+': $'+__name) for __name in variables])+(')'*bracket)

    def format_variables_dict(self, variables: Dict, indent=4, step=2, linebreak=True, colons=True) -> str:
        variables = {__k: self.format_variables(__v, linebreak=False, bracket=False) for __k, __v in self.variables.items()}
        __formatted = self.format(variables, indent, step, linebreak=False, colons=colons)
        return '('+(('\n'+' '*indent) if linebreak else '')+__formatted+(('\n'+' '*max(indent-step,0)) if linebreak else '')+')'


class GraphQLFields(GraphQLObject):
    fields = list()

    def __init__(self, fields: Union[Dict,List], typename=True):
        self.fields = self.validate_fields(fields, typename)
        CustomDict.__init__(self)

    def get_fields(self) -> Union[Dict,List]:
        return self.fields

    def validate_fields(self, fields: Union[Dict,List,str], typename=True) -> Union[Dict,List]:
        if isinstance(fields, GraphQLFragment):
            __appendix = ["__typename"] if typename else []
            return [fields, *__appendix]
        elif isinstance(fields, Dict):
            return {__k: self.validate_fields(__v, typename) for __k, __v in fields.items()}
        elif isinstance(fields, List):
            __appendix = ["__typename"] if typename else []
            return [self.validate_fields(__field, typename) for __field in fields] + __appendix
        elif isinstance(fields, str):
            return fields
        else: raise TypeError(INVALID_OBJECT_TYPE_MSG(fields, GraphQLFields))

    def generate_fields(self, indent=4, step=2, linebreak=True, colons=False, prefix=str(), suffix=str(), replace=dict(), **kwargs) -> str:
        __formatted = '{\n'+self.format(self.fields, indent, step, linebreak, colons)+'\n'+(' '*max(indent-step,0))+'}'
        return prefix+(replace_map(__formatted, **replace) if replace else __formatted)+suffix

    def format(self, __object: Union[Dict,List,str], indent=0, step=2, linebreak=True, colons=False) -> str:
        __object = "..."+__object.name if isinstance(__object, GraphQLFragment) else __object
        return super().format(__object, indent, step, linebreak, colons)


class GraphQLSelection(GraphQLObject):
    name = str()
    alias = str()
    variables = None
    fields = None

    def __init__(self, name: str, variables: Union[Dict,List], fields: Optional[Union[Dict,List]]=None, alias=str(), typename=True):
        self.name = name
        self.alias = alias
        self.variables = variables if isinstance(variables, GraphQLVariables) else GraphQLVariables(variables)
        self.fields = (fields if isinstance(fields, GraphQLFields) else GraphQLFields(fields, typename)) if fields is not None else None
        CustomDict.__init__(self)

    def get_variables(self) -> Union[Dict,List]:
        return self.variables.get_variables()

    def get_fields(self) -> Union[Dict,List]:
        return self.fields.get_fields() if self.fields is not None else list()

    def generate_selection(self, indent=2, step=2, variables=dict(), fields=dict(), **kwargs) -> str:
        name = f"{self.name}: {self.alias}" if self.alias else self.name
        variables = self.variables.generate_variables(indent+step, step, **variables)
        fields = (' '+self.fields.generate_fields(indent+step, step, **fields)) if self.fields is not None else str()
        return '{\n'+(' '*indent)+f"{name}{variables}{fields}"+'\n'+(' '*max(indent-step,0))+'}'


class GraphQLFragment(GraphQLObject):
    name = str()
    type = str()
    fields = list()

    def __init__(self, name: str, type: str, fields: Union[Dict,List], typename=True):
        self.name = name
        self.type = type
        self.fields = fields if isinstance(fields, GraphQLFields) else GraphQLFields(fields, typename)
        CustomDict.__init__(self)

    def get_fields(self) -> Union[Dict,List]:
        return self.fields.get_fields()

    def generate_fragment(self, indent=0, step=2, linebreak=True, colons=False, prefix=str(), suffix=str(), replace=dict(), **kwargs) -> str:
        fields = self.fields.get_fields()
        __formatted = self.format({f"fragment {self.name} on {self.type}": fields}, indent, step, linebreak, colons)
        return prefix+(replace_map(__formatted, **replace) if replace else __formatted)+suffix


class GraphQLOperation(GraphQLObject):
    operation = str()
    variables = dict()
    types = dict()
    selection = list()
    fragments = list()

    def __init__(self, operation: str, variables: Dict, types: Dict[str,TypeHint], selection: Dict):
        self.operation = operation
        self.variables = variables
        self.types = types
        self.selection = self.validate_selection(selection)
        self.fragments = self.validate_fragments(self.selection.get_fields())
        CustomDict.__init__(self)

    def validate_selection(self, selection: Dict) -> GraphQLSelection:
        if isinstance(selection, GraphQLSelection): return selection
        elif isinstance(selection, Dict): return GraphQLSelection(**selection)
        else: raise TypeError(INVALID_OBJECT_TYPE_MSG(selection, GraphQLSelection))

    def validate_fragments(self, fields: Union[Dict,List]) -> List[GraphQLFragment]:
        return get_json_values(fields, GraphQLFragment)

    def generate_data(self, query_options=dict()) -> Dict:
        data = dict(operationName=self.operation) if self.operation else dict()
        data["variables"] = self.variables
        data["query"] = self.generate_query(**query_options)
        return data

    def generate_query(self, command="query", selection=dict(), fragment=dict(), prefix=str(), suffix=str(), **kwargs) -> str:
        signature = self.generate_signature()
        selection = self.selection.generate_selection(**selection)
        fragments = self.generate_fragments(**fragment)
        return prefix+f"{command} {signature} {selection}{fragments}"+suffix

    def generate_signature(self) -> str:
        return self.operation+'('+', '.join([f"${__name}: {__type}" for __name, __type in self.types.items()])+')'

    def generate_fragments(self, indent=0, step=2, **kwargs) -> str:
        fragments = '\n\n'.join([__frag.generate_fragment(indent, step) for __frag in self.fragments])
        return '\n\n'+fragments if fragments else str()


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


def get_json_values(data: JsonData, __type: type) -> List[_VT]:
    values = list()
    if isinstance(data, __type):
        values.append(data)
    elif isinstance(data, Dict):
        for __value in data.values():
            values.extend(get_json_values(__value, __type))
    elif isinstance(data, List):
        for __value in data:
            values.extend(get_json_values(__value, __type))
    return values


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
    return regex_get(f"(.*)(?={urlparse(url).path})", url, groups=0) if urlparse(url).path else url


def parse_cookies(cookies: Union[RequestsCookieJar,SimpleCookie]) -> str:
    if isinstance(cookies, str): return cookies
    else: return '; '.join([str(key)+"="+str(value) for key, value in cookies.items()])


def encode_cookies(cookies: Union[str,Dict], *args, **kwargs) -> str:
    cookies = parse_cookies(cookies)
    if args: cookies = '; '.join([cookies]+[parse_cookies(arg) for arg in args])
    if kwargs: cookies = '; '.join([cookies, parse_cookies(kwargs)])
    return cookies


def decode_cookies(cookies: Union[str,Dict], **kwargs) -> Dict:
    if not cookies: return kwargs
    elif isinstance(cookies, str):
        cookies = dict([__cookie.split('=', maxsplit=1) for __cookie in cookies.split('; ') if '=' in __cookie])
    return dict(cookies, **kwargs)


def encode_params(url=str(), params: Dict=dict(), encode=True) -> str:
    if encode: params = urlencode(params)
    else: params = '&'.join([f"{key}={value}" for key, value in params.items()])
    return url+'?'+params if url else params


def encode_object(__object: str) -> str:
    return quote(str(__object).replace('\'','\"'))
