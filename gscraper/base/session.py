from __future__ import annotations
from gscraper.base.context import REQUEST_CONTEXT

from gscraper.base.types import _KT, _VT, _PASS, ClassInstance, Context, TypeHint, LogLevel, RenameMap, IndexLabel
from gscraper.base.types import Pagination, Pages, Unit, DateFormat, DateQuery, Timedelta, Timezone, Data, ResponseData
from gscraper.base.types import is_na, not_na, is_dataframe_type, allin_instance, is_array, is_str_array, init_origin

from gscraper.utils.cast import cast_list, cast_tuple, cast_int1
from gscraper.utils.date import now, get_date, get_busdate, get_date_range, is_daily_frequency
from gscraper.utils.logs import CustomLogger, dumps_data, log_exception
from gscraper.utils.map import data_exists, unique, get_scala, diff
from gscraper.utils.map import iloc, fill_array, is_same_length, unit_array, concat_array, transpose_array
from gscraper.utils.map import kloc, apply_dict, chain_dict, drop_dict, exists_one, convert_data

from abc import ABCMeta
from itertools import product
import functools
import logging
import os

from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple, Union
from bs4 import BeautifulSoup
from bs4.element import Tag
import datetime as dt
import json
import pandas as pd


PAGE_ITERATOR = ["page", "start", "dataSize"]
PAGE_PARAMS = ["size", "pageSize", "pageStart", "offset"]

DATE_ITERATOR = ["startDate", "endDate"]
DATE_PARAMS = ["startDate", "endDate", "interval"]

CHECKPOINT = [
    "all", "context", "crawl", "params", "iterator", "iterator_count", "gather", "gather_count", "redirect",
    "request", "response", "parse", "login", "api", "exception"]
CHECKPOINT_PATH = "saved/"

USER_INTERRUPT_MSG = lambda where: f"Interrupt occurred on {where} by user."


###################################################################
############################### Base ##############################
###################################################################

class CustomDict(dict):
    def __init__(self, **kwargs):
        super().__init__(kwargs)
        self.update(self.__dict__)

    def copy(self, __instance: Optional[ClassInstance]=None) -> Any:
        if __instance: return __instance.__class__(**self.__dict__)
        else: return self.__class__(**self.__dict__)

    def get(self, __key: _KT, default=None, if_null: Literal["drop","pass"]="drop",
            reorder=True, values_only=False) -> Union[Any,Dict,List,str]:
        return kloc(self.__dict__, __key, default=default, if_null=if_null, reorder=reorder, values_only=values_only)

    def update(self, __m: Optional[Dict]=dict(), inplace=True, **kwargs) -> Union[bool,Dict]:
        if not inplace: self = self.copy()
        for __key, __value in dict(kwargs, **__m).items():
            setattr(self, __key, __value)
        super().update(self.__dict__)
        return exists_one(inplace, self)

    def __getitem__(self, __keys: _KT) -> _VT:
        if is_array(__keys):
            return [super().__getitem__(__key) for __key in __keys]
        else:
            return super().__getitem__(__keys)

    def __setitem__(self, __key: _KT, __value: _VT):
        setattr(self, __key, __value)
        super().__setitem__(__key, __value)


###################################################################
############################# Session #############################
###################################################################

class BaseSession(CustomDict):
    __metaclass__ = ABCMeta
    operation = "session"
    by = str()
    fields = list()
    tzinfo = None
    datetimeUnit = "second"
    returnType = None
    errors = list()

    def __init__(self, fields: IndexLabel=list(),
                tzinfo: Optional[Timezone]=None, datetimeUnit: Literal["second","minute","hour","day"]="second",
                returnType: Optional[TypeHint]=None, logName=str(), logLevel: LogLevel="WARN", logFile=str(),
                debug: List[str]=list(), extraSave: List[str]=list(), interrupt=str(), localSave=False,
                **context):
        super().__init__(context)
        self.operation = self.operation
        self.fields = fields if fields else self.fields
        self.tzinfo = tzinfo if tzinfo else self.tzinfo
        self.datetimeUnit = datetimeUnit if datetimeUnit else self.datetimeUnit
        self.initTime = now(tzinfo=self.tzinfo, droptz=True, unit=self.datetimeUnit)
        self.returnType = returnType if returnType else returnType
        self.set_logger(logName, logLevel, logFile, debug, extraSave, interrupt, localSave)

    def set_logger(self, logName=str(), logLevel: LogLevel="WARN", logFile=str(),
                    debug: List[str]=list(), extraSave: List[str]=list(), interrupt=str(), localSave=False):
        logName = logName if logName else self.operation
        self.logLevel = int(logLevel) if str(logLevel).isdigit() else logging.getLevelName(str(logLevel).upper())
        self.logFile = logFile
        self.logJson = bool(logFile)
        self.logger = CustomLogger(name=logName, level=self.logLevel, file=self.logFile)
        self.debug = cast_list(debug)
        self.extraSave = cast_list(extraSave)
        self.interrupt = interrupt
        self.localSave = localSave

    def now(self, __format=str(), days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0,
            hours=0, weeks=0, droptz=True) -> dt.datetime:
        return now(__format, days, seconds, microseconds, milliseconds, minutes, hours, weeks,
                    tzinfo=self.tzinfo, droptz=droptz, unit=self.datetimeUnit)

    def today(self, __format=str(), days=0, weeks=0, droptz=True) -> dt.date:
        unit = self.datetimeUnit if self.datetimeUnit in ["month","year"] else None
        return now(__format, days, weeks, tzinfo=self.tzinfo, droptz=droptz, droptime=True, unit=unit)

    def get_rename_map(self, **context) -> RenameMap:
        return dict()

    def rename(self, string: str, **context) -> str:
        renameMap = self.get_rename_map(**context)
        return renameMap[string] if renameMap and (string in renameMap) else string

    def match(self, data: ResponseData, **context) -> bool:
        return True

    ###################################################################
    ############################ Checkpoint ###########################
    ###################################################################

    def checkpoint(self, point: str, where: str, msg: Dict,
                    save: Optional[Data]=None, ext: Optional[TypeHint]=None):
        if (point in self.debug) or ("all" in self.debug):
            self.logger.warning(dict(point=f"[{str(point).upper()}]", **dumps_data(msg, limit=0)))
        if ((point in self.extraSave) or ("all" in self.extraSave)) and data_exists(save):
            if (ext == "response"): save, ext = self._check_response(save)
            self._validate_dir(CHECKPOINT_PATH)
            self.save_data(save, prefix=CHECKPOINT_PATH+str(point).replace('.','_'), ext=ext)
        if self.interrupt == point:
            raise KeyboardInterrupt(USER_INTERRUPT_MSG(where))

    def save_data(self, data: Data, prefix=str(), ext: Optional[TypeHint]=None):
        prefix = self.rename(prefix if prefix else self.operation)
        file = prefix+'_'+self.now("%Y%m%d%H%M%S")
        ext = ext if ext else type(data)
        if is_dataframe_type(ext):
            self.save_dataframe(data, file+".xlsx")
        elif ext == "html" and isinstance(data, str):
            self.save_source(data, file+".html")
        else: self.save_json(data, file+".json")

    def save_json(self, data: Data, file: str):
        file = self._validate_file(file)
        if isinstance(data, pd.DataFrame): data = data.to_dict("records")
        with open(file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)

    def save_dataframe(self, data: Data, file: str):
        file = self._validate_file(file)
        data = convert_data(data, return_type="dataframe")
        data.rename(columns=self.get_rename_map()).to_excel(file, index=False)

    def save_source(self, data: Union[str,Tag], file: str):
        file = self._validate_file(file)
        if not isinstance(data, Tag):
            data = BeautifulSoup(data, "html.parser")
        with open(file, "w", encoding="utf-8") as f:
            f.write(str(data))

    def _validate_dir(self, dir: str):
        if not os.path.exists(dir):
            os.mkdir(dir)

    def _validate_file(self, file: str):
        file, ext = os.path.splitext(file)
        __i, suffix = 1, str()
        while True:
            if os.path.exists(file+suffix+ext):
                suffix, __i = f"_{__i}", __i+1
            else: break
        return file+suffix+ext

    def _check_response(self, response: Data) -> Tuple[Data, TypeHint]:
        if isinstance(response, str) and response:
            try: return json.loads(response), "json"
            except: return response, "html"
        elif isinstance(response, pd.DataFrame):
            return response, "dataframe"
        else: return response, "json"

    ###################################################################
    ########################### Log Managers ##########################
    ###################################################################

    def catch_exception(func):
        @functools.wraps(func)
        def wrapper(self: BaseSession, *args, **context):
            try: return func(self, *args, **context)
            except KeyboardInterrupt as interrupt:
                raise interrupt
            except Exception as exception:
                if "exception" in self.debug:
                    self.checkpoint("exception", where=func.__name__, msg={"args":args, "context":context})
                    raise exception
                self.log_errors(*args, **context)
                func_name = f"{func.__name__}({self.__class__.__name__})"
                self.logger.error(log_exception(func_name, json=self.logJson, **REQUEST_CONTEXT(**context)))
                return init_origin(func)
        return wrapper

    def log_errors(self, *args, **context):
        self.errors.append(dict({"args":args}, **context))


###################################################################
############################# Iterator ############################
###################################################################

class Iterator(CustomDict):
    iterateArgs = list()
    iterateProduct = list()
    iterateUnit = 1
    pagination = False
    pageUnit = 0
    pageLimit = 0
    interval = str()
    fromNow = None

    def __init__(self, iterateUnit: Optional[Unit]=0, interval: Optional[Timedelta]=str(), fromNow: Optional[Unit]=None):
        super().__init__()
        self.iterateUnit = iterateUnit if iterateUnit else self.iterateUnit
        self.interval = interval if interval else self.interval
        self.fromNow = fromNow if not_na(fromNow, strict=True) else self.fromNow

    def get_date(self, date: Optional[DateFormat]=None, fromNow: Optional[Unit]=None, index=0, busdate=False) -> dt.date:
        fromNow = fromNow if fromNow else self.fromNow
        if busdate: return get_busdate(date, if_null=get_scala(fromNow, index=index))
        else: return get_date(date, if_null=get_scala(fromNow, index=index))

    def get_date_pair(self, startDate: Optional[DateFormat]=None, endDate: Optional[DateFormat]=None,
                        fromNow: Optional[Unit]=None, busdate=False) -> Tuple[dt.date,dt.date]:
        startDate = self.get_date(startDate, fromNow=fromNow, index=0, busdate=busdate)
        endDate = self.get_date(endDate, fromNow=fromNow, index=1, busdate=busdate)
        if startDate: startDate = min(startDate, endDate) if endDate else startDate
        if endDate: endDate = max(startDate, endDate) if startDate else endDate
        return startDate, endDate

    ###################################################################
    ########################### Set Iterator ##########################
    ###################################################################

    def get_iterator(self, _args=True, _page=True, _date=True, _product=True, _params=False,
                    keys_only=False, values_only=False, if_null: Literal["drop","pass"]="drop",
                    **context) -> Union[Context,_KT,_VT]:
        iterateArgs = self.iterateArgs * int(_args)
        iteratePage = (PAGE_PARAMS if _params else PAGE_ITERATOR) * int(bool(self.pagination)) * int(_page)
        iterateDate = (DATE_PARAMS if _params else DATE_ITERATOR) * int(bool(self.interval)) * int(_date)
        iterateProduct = self.iterateProduct * int(_product)
        query = unique(*iterateArgs, *iteratePage, *iterateDate, *iterateProduct)
        if keys_only: return query
        else: return kloc(context, query, if_null=if_null, values_only=values_only)

    def set_iterator(self, *args: Sequence, iterateArgs: List[_KT]=list(), iterateProduct: List[_KT]=list(),
                    iterateUnit: Unit=1, pagination: Pagination=False, interval: Timedelta=str(), index=True,
                    **context) -> Tuple[List[Context],Context]:
        arguments, periods, ranges, iterateUnit = list(), list(), list(), cast_list(iterateUnit)
        args_context = self._check_args(*args, iterateArgs=iterateArgs, pagination=pagination)
        if args_context:
            arguments, context = self._from_args(*args, **args_context, iterateUnit=iterateUnit, **context)
            iterateProduct = diff(iterateProduct, iterateArgs, PAGE_ITERATOR)
            if len(iterateProduct) > 1: iterateUnit = iterateUnit[1:]
        if interval:
            periods, context = self._from_date(interval=interval, **context)
            iterateProduct = diff(iterateProduct, DATE_ITERATOR+["date"])
        ranges, context = self._from_context(iterateProduct=iterateProduct, iterateUnit=iterateUnit, **context)
        iterator = self._product_iterator(arguments, periods, ranges, index=index)
        return iterator, context

    ###################################################################
    ########################## From Arguments #########################
    ###################################################################

    def _check_args(self, *args, iterateArgs: List[_KT], pagination: Pagination=False) -> Context:
        match_query = is_same_length(args, iterateArgs)
        match_args = is_same_length(*args)
        valid_args = (match_query and match_args) or ((not iterateArgs) and pagination)
        valid_pages = isinstance(pagination, bool) or (isinstance(pagination, str) and (pagination in iterateArgs))
        return dict(iterateArgs=iterateArgs, pagination=pagination) if valid_args and valid_pages else dict()

    def _from_args(self, *args: Sequence, iterateArgs: List[_KT], iterateUnit: Unit=1,
                pagination: Pagination=False, **context) -> Tuple[List[Context],Context]:
        if not is_same_length(*args): return list(), context
        argnames, pagenames = self._split_argnames(*args, iterateArgs=iterateArgs, pagination=pagination, **context)
        if pagination:
            how = "numeric" if pagenames == PAGE_ITERATOR else "categorical"
            args, context = self._from_pages(*args, how=how, iterateArgs=iterateArgs, pagination=pagination, **context)
        args = self._product_args(*args)
        if get_scala(iterateUnit) > 1:
            args = list(map(lambda __s: unit_array(__s, unit=get_scala(iterateUnit)), args))
        args = [dict(zip(argnames, values)) for values in zip(*args)]
        return (self._map_pages(*args, keys=pagenames) if pagenames else args), context

    def _split_argnames(self, *args, iterateArgs: List[_KT], pagination: Pagination=False,
                        size: Optional[Unit]=None, **context) -> Tuple[List[_KT],List[_KT]]:
        argnames = iterateArgs.copy()
        if isinstance(pagination, str) and is_str_array(args[iterateArgs.index(pagination)]):
            argnames.pop(iterateArgs.index(pagination))
            return argnames+["pages"], [pagination]+PAGE_ITERATOR+PAGE_PARAMS
        elif pagination:
            return argnames+["pages"], PAGE_ITERATOR
        else: return argnames, list()

    def _product_args(self, *args: Sequence) -> List[List]:
        tuple_idx = list(map(lambda x: allin_instance(x, Tuple, empty=False), args))
        if not any(tuple_idx): return args
        __args = list()
        for __arg in zip(*args):
            tuples, others = iloc(__arg, tuple_idx), iloc(__arg, list(map(lambda x: (not x), tuple_idx)))
            __product = list(product((others,), *tuples))
            __args += [concat_array(__s[1:], __s[0], tuple_idx) for __s in __product]
        return transpose_array(__args, count=len(args))

    ###################################################################
    ############################ From Pages ###########################
    ###################################################################

    def _from_pages(self, *args: Sequence, how: Literal["numeric","categorical"], iterateArgs: List[_KT],
                    pagination: Pagination=False, size: Optional[Unit]=None, **context) -> Tuple[List[List],Context]:
        if how == "numeric":
            if is_na(size) and isinstance(pagination, str):
                size = args[iterateArgs.index(pagination)]
            return self._from_numeric_pages(*args, size=size, **context)
        base = list(args)
        labels = cast_list(base.pop(iterateArgs.index(pagination)))
        return self._from_categorical_pages(*base, labels=labels, pagination=pagination, size=size, **context)

    def _from_numeric_pages(self, *args, size: Unit, pageSize=0, pageStart=1, offset=1, **context) -> Tuple[List[List],Context]:
        pageSize = self.validate_page_size(pageSize, self.pageUnit, self.pageLimit)
        pages = self.get_pages(size, pageSize, pageStart, offset, how="all")
        if isinstance(size, int):
            pages = [pages] * len(get_scala(args, default=[0]))
        pages = list(map(lambda __s: tuple(map(tuple, __s)), map(transpose_array, pages)))
        return args+(pages,), dict(context, pageSize=pageSize)

    def _from_categorical_pages(self, *args, labels: List, pagination: str, size: Optional[int]=None,
                                pageSize=0, pageStart=1, offset=1, **context) -> Tuple[List[List],Context]:
        pages = list()
        for label in labels:
            size = self.get_size_by_label(label, size=size, **context)
            pageSize = self.get_page_size_by_label(label, pageSize=pageSize, **context)
            pageStart = self.get_page_start_by_label(label, pageStart=pageStart, **context)
            offset = self.get_offset_by_label(label, offset=offset, **context)
            iterator = self.get_pages(size, pageSize, pageStart, offset, how="all")
            num_pages = len(iterator[0])
            params = ((size,)*num_pages, (pageSize,)*num_pages, (pageStart,)*num_pages, (offset,)*num_pages)
            pages.append(tuple(map(tuple, transpose_array(((label,)*num_pages,)+iterator+params))))
        return args+(pages,), context

    def validate_page_size(self, pageSize: int, pageUnit=0, pageLimit=0, **context) -> int:
        if (pageUnit > 0) and (pageSize & pageUnit != 0):
            pageSize = round(pageSize / pageUnit) * pageUnit
        if pageLimit > 0:
            pageSize = min(pageSize, pageLimit)
        return pageSize

    def get_pages(self, size: Unit, pageSize: int, pageStart=1, offset=1, pageUnit=0, pageLimit=0,
                    how: Literal["all","page","start"]="all", **context) -> Union[Pages,List[Pages]]:
        pageSize = self.validate_page_size(pageSize, pageUnit, pageLimit)
        if isinstance(size, int):
            return self.calc_pages(cast_int1(size), pageSize, pageStart, offset, how)
        elif is_array(size):
            return [self.calc_pages(cast_int1(__sz), pageSize, pageStart, offset, how) for __sz in size]
        else: return tuple()

    def calc_pages(self, size: int, pageSize: int, pageStart=1, offset=1,
                    how: Literal["all","page","start"]="all", **context) -> Pages:
        pages = tuple(range(pageStart, (((size-1)//pageSize)+1)+pageStart))
        if how == "page": return pages
        starts = tuple(range(offset, size+offset, pageSize))
        if how == "start": return starts
        dataSize = tuple(min(size-start+1, pageSize) for start in starts)
        return (pages, starts, dataSize)

    def get_size_by_label(self, label: Any, size: Optional[int]=None, **context) -> int:
        return size

    def get_page_size_by_label(self, label: Any, pageSize=0, **context) -> int:
        return pageSize

    def get_page_start_by_label(self, label: Any, pageStart=1, **context) -> int:
        return pageStart

    def get_offset_by_label(self, label: Any, offset=1, **context) -> int:
        return offset

    def _map_pages(self, *args: Context, keys: List[_KT]) -> List[Context]:
        base = list()
        for __i in range(len(args)):
            pages = args[__i].pop("pages")
            base.append(dict(args[__i], **dict(zip(keys, pages))))
        return base

    ###################################################################
    ############################ From Date ############################
    ###################################################################

    def _from_date(self, startDate: Optional[dt.date]=None, endDate: Optional[dt.date]=None,
                    interval: Timedelta="D", date: _PASS=None, **context) -> Tuple[List[DateQuery],Context]:
        date_range = get_date_range(*self.get_date_pair(startDate, endDate), interval=interval)
        if is_daily_frequency(interval):
            period = [dict(startDate=date, endDate=date, date=date) for date in date_range]
        elif len(date_range) > 1:
            period = [dict(startDate=start, endDate=(end-dt.timedelta(days=1)), date=(end-dt.timedelta(days=1)))
                        for start, end in zip(date_range, date_range[1:]+[endDate+dt.timedelta(days=1)])]
        else: period = [dict(startDate=startDate, endDate=endDate, date=endDate)]
        return period, dict(context, interval=interval)

    ###################################################################
    ########################### From Context ##########################
    ###################################################################

    def _from_context(self, iterateProduct: List[_KT], iterateUnit: Unit=1, **context) -> Tuple[List[Context],Context]:
        if not iterateProduct: return list(), context
        query, context = kloc(context, iterateProduct, if_null="drop"), drop_dict(context, iterateProduct, inplace=False)
        if any(map(lambda x: x>1, iterateUnit)): query = self._group_context(iterateProduct, iterateUnit, **query)
        else: query = [dict(zip(query.keys(), values)) for values in product(*map(cast_tuple, query.values()))]
        return query, context

    def _group_context(self, iterateProduct: List[_KT], iterateUnit: Unit=1, **context) -> List[Context]:
        query = apply_dict(context, apply=cast_list, all_keys=True)
        keys, unit = query.keys(), fill_array(iterateUnit, count=len(iterateProduct), value=1)
        combinations = product(*[range(0, len(query[key]), unit[i]) for i, key in enumerate(keys)])
        return [{key: query[key][index:index+unit[i]] for i, (key, index) in enumerate(zip(keys, indices))}
                for indices in combinations]

    def _product_iterator(self, *ranges: Sequence[Context], index=True) -> List[Context]:
        if sum(map(len, ranges)) == 0: return list()
        ranges_array = map((lambda x: x if x else [{}]), ranges)
        indexing = lambda __i: dict(index=__i) if index else dict()
        return [dict(**indexing(__i), **chain_dict(query)) for __i, query in enumerate(product(*ranges_array))]
