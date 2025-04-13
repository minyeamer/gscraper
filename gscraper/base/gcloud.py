from __future__ import annotations
from gscraper.base.abstract import OptionalDict, TypedRecords, Value, ValueSet
from gscraper.base.abstract import GCLOUD_CONTEXT, INVALID_OBJECT_MSG, INVALID_MSG, INVALID_OBJECT_TYPE_MSG
from gscraper.base.session import BaseSession

from gscraper.base.types import _KT, Context, TypeHint, IndexLabel, DateFormat, RenameMap, TypeMap
from gscraper.base.types import TabularData, PostData, from_literal

from gscraper.utils.cast import cast_float, cast_list, cast_date, cast_datetime, cast_datetime_format
from gscraper.utils.date import get_datetime, DATE_UNIT
from gscraper.utils.logs import log_table
from gscraper.utils.map import isna, df_empty, to_array, kloc, to_dict, to_records, read_table
from gscraper.utils.map import cloc, to_dataframe, convert_data, rename_data, filter_data, apply_data

from google.oauth2 import service_account
from google.oauth2.service_account import IDTokenCredentials
from google.auth.transport.requests import AuthorizedSession

from gspread.worksheet import Worksheet
import gspread

from google.cloud import bigquery
from google.cloud.bigquery.table import RowIterator
from google.cloud.bigquery.job import LoadJobConfig

from tqdm import tqdm
import abc
import copy
import functools
import os
import requests
import sys

from typing import Any, Dict, Iterable, List, Literal, Optional, Sequence, Tuple, Union
import datetime as dt
import json
import re
import pandas as pd


Account = Union[Dict[str,str], str]

ENV_PATH = "env/"
GCLOUD_ACCOUNT = ENV_PATH+"gcloud.json"
GCLOUD_DATA = ENV_PATH+"data.json"

NAME = "name"
KEY, SHEET, FIELDS = "key", "sheet", "fields"
TABLE, QUERY, PID = "table", "query", "project_id"
MODE, DATA = "mode", "data"

FILEPATH, SHEETNAME = "file_path", "sheet_name"

FROM_GS, TO_GBQ = ["from_key", "from_sheet"], ["to_table", "to_pid"]
FROM_GBQ, TO_GS = ["from_query", "from_pid"], ["to_key", "to_sheet"]

READ = lambda name=str(): f"read_{name}" if name else "read"
UPLOAD = lambda name=str(): f"upload_{name}" if name else "upload"


###################################################################
############################# Messages ############################
###################################################################

UPLOAD_FAILED_MSG = "There is no upload data."
UPLOAD_GBQ_MSG = lambda table: f"Uploading data to '{table}' table"

UPLOAD_STATUS_TITLE = "▷ Upload Status"
UPLOAD_STATUS_MSG = lambda idx, name, type, status=True: f'({idx}) "{name}" {type} {"✅" if status else "❌"}'
GOOGL_SHEETS = "sheets"
BIGQUERY_TABLE = "table"

GOOGLE_READ_CONTEXT = "Google cloud read context"
GOOGLE_QUERY_CONTEXT = "Google cloud query context"
GOOGLE_UPLOAD_CONTEXT = "Google cloud upload context"

BIGQUERY_TYPE = "BigQueryType"
BIGQUERY_MODE = "BigQueryMode"
BIGQUERY_SCHEMA = "BigQuerySchema"

PARTITION_FILTER = "Partition filter"


###################################################################
###################### Google Authorization #######################
###################################################################

def get_sys_file(file: str) -> str:
    base_path = getattr(sys, '_MEIPASS', os.path.abspath(os.path.dirname(__file__)))
    return os.path.join(base_path, file)


def read_json(file: str) -> Dict:
    if os.path.exists(file):
        with open(file, "r", encoding="utf-8") as f:
            return json.loads(f.read())
    else: return dict()


def read_gcloud(file=str()) -> Dict:
    if isinstance(file, str) and file.endswith(".json") and os.path.exists(file): pass
    elif os.path.exists(GCLOUD_ACCOUNT): file = GCLOUD_ACCOUNT
    else: file = get_sys_file(GCLOUD_ACCOUNT.split('/')[-1])
    return read_json(file)


def read_data(operation: str, file=str()) -> PostData:
    return read_json(str(file) if str(file).endswith(".json") else GCLOUD_DATA).get(operation, dict())


def fetch_gcloud_credentials(audience=str(), account: Account=dict()) -> IDTokenCredentials:
    account = account if account and isinstance(account, dict) else read_gcloud(account)
    audience = audience if audience else account.get("audience", str())
    return service_account.IDTokenCredentials.from_service_account_info(account, target_audience=audience)


def fetch_gcloud_authorization(audience=str(), account: Account=dict()) -> str:
    credentials = fetch_gcloud_credentials(audience, account)
    auth_session = AuthorizedSession(credentials)
    auth_session.get(audience)
    return "Bearer "+credentials.token


def request_gcloud(audience: str, data: Optional[PostData]=dict(), authorization=str(),
                    account: Account=dict(), file=str(), operation=str()) -> requests.Response:
    if not authorization:
        authorization = fetch_gcloud_authorization(audience, account)
    data = data if data and isinstance(data, dict) else read_data(file).get(operation, dict())
    return requests.post(audience, json=data, headers={"Authorization":authorization})


###################################################################
######################### BigQuery Schema #########################
###################################################################

BigQueryNumericType = Literal["INTEGER", "FLOAT", "NUMERIC", "BIGNUMERIC", "BOOLEAN"]
BigQUeryDatetimeType = Literal["TIMESTAMP", "DATE", "TIME", "DATETIME"]
BigQueryDataType = Literal["GEOGRAPHY", "RECORD", "JSON"]
BigQueryType = Union[Literal["STRING", "BYTES"], BigQueryNumericType, BigQUeryDatetimeType, BigQueryDataType]

BigQueryMode = Literal["NULLABLE", "REQUIRED", "REPEATED"]

class BigQueryField(Value):
    typeCast = False

    def __init__(self, name: str, type: BigQueryType, mode: Optional[BigQueryMode]=None,
                description: Optional[str]=None, maxLength: Optional[int]=None):
        super().__init__(name=name, **self.validate_type(type),
            optional=dict(**self.validate_mode(mode), description=description, maxLength=maxLength))

    def validate_type(self, type: BigQueryType) -> Context:
        if type not in from_literal(BigQueryType):
            raise ValueError(INVALID_OBJECT_MSG(type, BIGQUERY_TYPE))
        else: return dict(type=type)

    def validate_mode(self, mode: BigQueryMode) -> Context:
        if mode and (mode not in from_literal(BigQueryMode)):
            raise ValueError(INVALID_OBJECT_MSG(mode, BIGQUERY_MODE))
        else: return dict(mode=mode)

    def copy(self) -> BigQueryField:
        return copy.deepcopy(self)

    def update(self, __m: Dict=dict(), inplace=True, self_var=False, **kwargs) -> BigQueryField:
        return super().update(__m, inplace=inplace, self_var=self_var, **kwargs)


class BigQuerySchema(ValueSet):
    dtype = BigQueryField
    typeCheck = True

    def __init__(self, *fields: BigQueryField):
        super().__init__(*fields)

    def copy(self) -> BigQuerySchema:
        return copy.deepcopy(self)

    def update(self, __iterable: Iterable[BigQueryField], inplace=True) -> BigQuerySchema:
        return super().update(__iterable, inplace=inplace)

    def map(self, key: str, value: str) -> Dict:
        key, value = re.sub(r"^desc$", "description", key), re.sub(r"^desc$", "description", value)
        return super().map(key, value)

    def get_primary_key(self) -> IndexLabel:
        return [field.get("name") for field in self if isinstance(field, Dict) and field.get("mode") == "REQUIRED"]


def validate_gbq_schema(schema: Any, optional=False) -> BigQuerySchema:
    if optional and (not schema): return
    elif isinstance(schema, BigQuerySchema): return schema
    elif isinstance(schema, List): return BigQuerySchema(*schema)
    else: raise TypeError(INVALID_OBJECT_TYPE_MSG(schema, BIGQUERY_SCHEMA))


###################################################################
######################## Google Cloud Query #######################
###################################################################

NumericiseIgnore = Union[Sequence[int], bool]

class GspreadReadContext(OptionalDict):
    def __init__(self, key: str, sheet: str, fields: Optional[IndexLabel]=None, default: Optional[Any]=None,
                if_null: Literal["drop","pass"]="pass", head=1, headers: Optional[IndexLabel]=None,
                str_cols: Optional[NumericiseIgnore]=None, return_type: Optional[TypeHint]="dataframe",
                rename: Optional[RenameMap]=None, to: Optional[Literal["desc","name"]]="name",
                convert_dtypes=True, size: Optional[int]=None, name=str(), **kwargs):
        super().__init__(key=key, sheet=sheet,
            optional=dict(
                fields=fields, default=default, if_null=if_null, head=head, headers=headers,
                str_cols=str_cols, return_type=return_type, rename=rename, to=to, convert_dtypes=convert_dtypes,
                size=size, name=name, **kwargs),
            null_if=dict(if_null="pass", head=1, to="name", return_type="dataframe", convert_dtypes=True, name=str()))


class GspreadQueryContext(GspreadReadContext):
    def __init__(self, key: str, sheet: str, fields: Optional[IndexLabel]=None, default: Optional[Any]=None,
                if_null: Literal["drop","pass"]="drop", axis=0, dropna=True, drop_empty=False, unique=False,
                head=1, headers: Optional[IndexLabel]=None, str_cols: Optional[NumericiseIgnore]=None,
                arr_cols: Optional[IndexLabel]=None, as_records=False, as_frame=False,
                rename: Optional[RenameMap]=None, to: Optional[Literal["desc","name"]]="name",
                convert_dtypes=True, size: Optional[int]=None, name=str(), **kwargs):
        return_type = "records" if as_records else "dataframe"
        super().__init__(
            key, sheet, fields, default, if_null, head, headers, str_cols, return_type, rename, to, convert_dtypes, size, name)
        self.update_notna(
                axis=axis, dropna=dropna, drop_empty=drop_empty, unique=unique, arr_cols=arr_cols,
                as_records=as_records, as_frame=as_frame, **kwargs,
            null_if=dict(axis=0, dropna=True, drop_empty=False, unique=False, as_records=False, as_frame=False))


class BigQueryReadContext(OptionalDict):
    def __init__(self, query: str, project_id: str, return_type: Optional[TypeHint]="dataframe",
                size: Optional[int]=None, name=str(), **kwargs):
        super().__init__(query=query, project_id=project_id,
            optional=dict(return_type=return_type, size=size, name=name, **kwargs),
            null_if=dict(return_type="dataframe", name=str()))


class BigQueryContext(BigQueryReadContext):
    def __init__(self, query: str, project_id: str, axis=0, dropna=True, drop_empty=False, unique=False,
                arr_cols: Optional[IndexLabel]=None, as_records=False, as_frame=False,
                size: Optional[int]=None, name=str(), **kwargs):
        return_type = "records" if as_records else "dataframe"
        super().__init__(query, project_id, return_type, size, name)
        self.update_notna(
                axis=axis, dropna=dropna, drop_empty=drop_empty, unique=unique, arr_cols=arr_cols,
                as_records=as_records, as_frame=as_frame, **kwargs,
            null_if=dict(axis=0, dropna=True, drop_empty=False, unique=False, as_records=False, as_frame=False))


class ExcelReadContext(OptionalDict):
    def __init__(self, file_path: str, sheet_name: Union[str,int]=0, fields: Optional[IndexLabel]=None,
                default: Optional[Any]=None, if_null: Literal["drop","pass"]="pass",
                dtype: Optional[Union[TypeMap,Literal["str"]]]=None, return_type: Optional[TypeHint]="dataframe",
                rename: Optional[RenameMap]=None, to: Optional[Literal["desc","name"]]="name",
                regex_path=False, reverse_path=False, size: Optional[int]=None, name=str(), **kwargs):
        super().__init__(file_path=file_path, sheet_name=sheet_name,
            optional=dict(
                fields=fields, default=default, if_null=if_null, dtype=(str if dtype == "str" else dtype), return_type=return_type,
                rename=rename, to=to, regex_path=regex_path, reverse_path=reverse_path, size=size, name=name, **kwargs),
            null_if=dict(if_null="pass", return_type="dataframe", to="name", regex_path=False, reverse_path=False, name=str()))


class ExcelQueryContext(ExcelReadContext):
    def __init__(self, file_path: str, sheet_name: Union[str,int]=0, fields: Optional[IndexLabel]=None,
                default: Optional[Any]=None, if_null: Literal["drop","pass"]="pass", axis=0,
                dropna=True, drop_empty=False, unique=False, dtype: Optional[Union[TypeMap,Literal["str"]]]=None,
                arr_cols: Optional[IndexLabel]=None, as_records=False, as_frame=False,
                rename: Optional[RenameMap]=None, to: Optional[Literal["desc","name"]]="name",
                regex_path=False, reverse_path=False, size: Optional[int]=None, name=str(), **kwargs):
        return_type = "records" if as_records else "dataframe"
        super().__init__(
            file_path, sheet_name, fields, default, if_null, dtype, return_type, rename, to, regex_path, reverse_path, size, name)
        self.update_notna(
                axis=axis, dropna=dropna, drop_empty=drop_empty, unique=unique, arr_cols=arr_cols,
                as_records=as_records, as_frame=as_frame, **kwargs,
            null_if=dict(axis=0, dropna=True, drop_empty=False, unique=False, as_records=False, as_frame=False))


class GoogleReadContext(GspreadReadContext, BigQueryReadContext, ExcelReadContext):
    def __init__(self, **kwargs):
        if len(kloc(kwargs, [KEY, SHEET], if_null="drop")) == 2: self.__class__ = GspreadReadContext
        elif len(kloc(kwargs, [QUERY, PID], if_null="drop")) == 2: self.__class__ = BigQueryReadContext
        elif FILEPATH in kwargs: self.__class__ = ExcelReadContext
        else: raise ValueError(INVALID_MSG(GOOGLE_READ_CONTEXT))
        self.__class__.__init__(self, **kwargs)


class GoogleQueryContext(GspreadQueryContext, BigQueryContext, ExcelQueryContext):
    def __init__(self, **kwargs):
        if len(kloc(kwargs, [KEY, SHEET], if_null="drop")) == 2: self.__class__ = GspreadQueryContext
        elif len(kloc(kwargs, [QUERY, PID], if_null="drop")) == 2: self.__class__ = BigQueryContext
        elif FILEPATH in kwargs: self.__class__ = ExcelQueryContext
        else: raise ValueError(INVALID_MSG(GOOGLE_QUERY_CONTEXT))
        self.__class__.__init__(self, **kwargs)


class GoogleQueryList(TypedRecords):
    dtype = GoogleQueryContext
    typeCheck = True

    def __init__(self, *args: GoogleQueryContext):
        super().__init__(*args)

    def validate_dtype(self, __object) -> Dict:
        if isinstance(__object, (GspreadQueryContext, BigQueryContext, ExcelQueryContext)): return __object
        elif isinstance(__object, Dict): return GoogleQueryContext(**__object)
        else: self.raise_dtype_error(__object)


class GoogleQueryReader(BaseSession):
    __metaclass__ = abc.ABCMeta
    operation = "googleQueryReader"

    def read_data(self, context: Dict, account: Account=dict()) -> TabularData:
        if isinstance(context, GspreadReadContext): return self.read_gspread(account=account, **context)
        elif isinstance(context, BigQueryReadContext): return self.read_gbq(account=account, **context)
        elif isinstance(context, ExcelReadContext): return self.read_excel(account=account, **context)
        else: return self.read_data(GoogleReadContext(**context), account)

    def read_gspread(self, key: str, sheet: str, fields: IndexLabel=list(), default=None,
                    if_null: Literal["drop","pass"]="pass", head=1, headers=None,
                    str_cols: NumericiseIgnore=list(), return_type: Optional[TypeHint]="dataframe",
                    rename: Optional[RenameMap]=None, to: Optional[Literal["desc","name"]]="name",
                    convert_dtypes=True, size: Optional[int]=None, name=str(), account: Account=dict(), **context) -> TabularData:
        rename_map = rename if rename else self.get_rename_map(to=to, query=True)
        kwargs = dict(numericise_ignore=str_cols, return_type=return_type, rename=rename_map, convert_dtypes=convert_dtypes, account=account)
        data = read_gspread(key, sheet, fields, default, if_null, head, headers, **kwargs)
        if isinstance(size, int): data = data[:size]
        self.checkpoint(READ(name), where="read_gspread", msg={KEY:key, SHEET:sheet, DATA:data}, save=data)
        self.logger.info(log_table(data, name=name, key=key, sheet=sheet))
        return data

    def read_gbq(self, query: str, project_id: str, return_type: Literal["records","dataframe"]="dataframe",
                size: Optional[int]=None, name=str(), account: Account=dict(), **context) -> TabularData:
        data = read_gbq(query, project_id, return_type=return_type, account=account)
        if isinstance(size, int): data = data[:size]
        self.checkpoint(READ(name), where="read_gspread", msg={QUERY:query, PID:project_id, DATA:data}, save=data)
        self.logger.info(log_table(data, name=name, query=query, project_id=project_id))
        return data

    def read_excel(self, file_path: str, sheet_name: Union[str,int]=0,
                    fields: IndexLabel=list(), default=None, if_null: Literal["drop","pass"]="pass",
                    dtype: TypeMap=None, return_type: Literal["records","dataframe"]="dataframe",
                    rename: Optional[RenameMap]=None, to: Optional[Literal["desc","name"]]="name",
                    regex_path=False, reverse_path=False, size: Optional[int]=None, name=str(), **context) -> TabularData:
        rename_map = rename if rename else self.get_rename_map(to=to, query=True)
        kwargs = dict(dtype=dtype, return_type=return_type, rename=rename_map, regex_path=regex_path, reverse_path=reverse_path)
        data = read_table(file_path, sheet_name=sheet_name, columns=fields, default=default, if_null=if_null, **kwargs)
        if isinstance(size, int): data = data[:size]
        self.checkpoint(READ(name), where="read_excel", msg={FILEPATH:file_path, SHEETNAME:sheet_name, DATA:data}, save=data)
        self.logger.info(log_table(data, name=name, file_path=file_path, sheet_name=sheet_name))
        return data

    def set_query(self, queryList: GoogleQueryList=list(), account: Account=dict()):
        for queryContext in GoogleQueryList(*queryList):
            data = self.read_data(queryContext, account)
            self.update(self.map_query_data(data, **queryContext), inplace=True)

    def map_query_data(self, data: pd.DataFrame, axis=0, dropna=True, drop_empty=False, unique=False,
                        arr_cols: IndexLabel=list(), as_records=False, as_frame=False,
                        name=str(), **context) -> Dict[_KT,Union[List,Any]]:
        if as_records or as_frame: return {name: data}
        arr_cols = cast_list(arr_cols)
        __m = to_dict((data.T if axis == 1 else data), "list", depth=2)
        for __key, __values in __m.copy().items():
            values = to_array(__values, dropna=dropna, drop_empty=drop_empty, unique=unique)
            if (len(values) < 2) and (__key not in arr_cols):
                __m[__key] = values[0] if len(values) == 1 else None
            else: __m[__key] = values
        return __m


###################################################################
###################### Google Cloud Uploader ######################
###################################################################

class GspreadUpdateContext(OptionalDict):
    def __init__(self, key: str, sheet: str, columns: IndexLabel=list(),
                mode: Literal["append","replace","ignore","upsert"]="append", primary_key: Optional[_KT]=None,
                set_date: Optional[Dict]=None, cell: Optional[str]=None, read: Optional[Dict]=None, name=str(), **kwargs):
        super().__init__(key=key, sheet=sheet,
            optional=dict(
                columns=columns, mode=mode, primary_key=cast_list(primary_key), set_date=set_date,
                cell=cell, read=read, name=name, **kwargs),
            null_if=dict(mode="append", head=1, to="name", name=str()))


class BigQueryPartition(OptionalDict):
    def __init__(self, field: str, type: Literal["date","datetime","number"]="date",
                unit: Literal["auto","hour","day","month","year"]="auto",
                left=None, right=None, value=None, limited=True, drop_partition=False, filter_partition=True):
        has_value = (left is not None) or (right is not None) or (value is not None)
        super().__init__(field=field, type=type, unit=unit.lower(),
            optional=dict(
                left=left, right=right, value=value, limited=limited,
                drop_partition=drop_partition, filter_partition=filter_partition, has_value=has_value))


class BigQueryUploadContext(OptionalDict):
    def __init__(self, table: str, project_id: str, columns: IndexLabel=list(),
                mode: Literal["append","replace","ignore","upsert"]="append", primary_key: Optional[_KT]=None,
                partition: Optional[Dict]=None, set_date: Optional[Dict]=None, read: Optional[Dict]=None, name=str(), **kwargs):
        super().__init__(table=table, project_id=project_id,
            optional=dict(
                columns=columns, mode=mode, primary_key=cast_list(primary_key),
                partition=partition, set_date=set_date, read=read, name=name, **kwargs),
            null_if=dict(mode="append", partition_by="auto", name=str()))


class GoogleUploadContext(GspreadUpdateContext, BigQueryUploadContext):
    def __init__(self, **kwargs):
        if len(kloc(kwargs, [KEY, SHEET], if_null="drop")) == 2: self.__class__ = GspreadUpdateContext
        elif len(kloc(kwargs, [TABLE, PID], if_null="drop")) == 2: self.__class__ = BigQueryUploadContext
        else: raise ValueError(INVALID_MSG(GOOGLE_UPLOAD_CONTEXT))
        self.__class__.__init__(self, **kwargs)


class GoogleCopyContext(OptionalDict):
    def __init__(self, read: Dict, upload: Dict, name=str(), **kwargs):
        if not isinstance(read, (GspreadReadContext, BigQueryReadContext, ExcelReadContext)):
            read = GoogleReadContext(**(read if isinstance(read, Dict) else dict()))
            if "name" not in read: read["name"] = name
        if not isinstance(upload, (GspreadUpdateContext, BigQueryUploadContext)):
            upload = GoogleUploadContext(**(upload if isinstance(upload, Dict) else dict()))
            if "name" not in upload: upload["name"] = name
        super().__init__(read=read, upload=upload)


class GoogleUploadList(TypedRecords):
    dtype = GoogleUploadContext
    typeCheck = True

    def __init__(self, *args: GoogleUploadContext):
        super().__init__(*args)

    def validate_dtype(self, __object) -> Dict:
        if isinstance(__object, (GspreadUpdateContext, BigQueryUploadContext, GoogleCopyContext)): return __object
        elif isinstance(__object, Dict):
            if ("read" in __object) and ("upload" in __object): return GoogleCopyContext(**__object)
            else: return GoogleUploadContext(**__object)
        else: self.raise_dtype_error(__object)


class GoogleUploader(GoogleQueryReader):
    __metaclass__ = abc.ABCMeta
    operation = "googleUploader"
    uploadStatus = dict()

    def upload_data(self, data: TabularData, uploadList: GoogleUploadList=list(), account: Account=dict(), **context):
        data = to_dataframe(data)
        context = GCLOUD_CONTEXT(**context)
        for uploadContext in GoogleUploadList(*uploadList):
            target = self._get_upload_target(uploadContext)
            if (not data.empty) and isinstance(uploadContext, GspreadUpdateContext):
                self.uploadStatus[target] = self.upload_gspread(data=data.copy(), **uploadContext, account=account, **context)
            elif (not data.empty) and isinstance(uploadContext, BigQueryUploadContext):
                self.uploadStatus[target] = self.upload_gbq(data=data.copy(), **uploadContext, account=account, **context)
            elif isinstance(uploadContext, GoogleCopyContext):
                self.uploadStatus[target] = self.copy_data(**uploadContext, account=account, **context)
            else: self.uploadStatus[target] = False

    def _get_upload_target(self, uploadContext) -> str:
        if isinstance(uploadContext, GspreadUpdateContext): return uploadContext["sheet"]
        elif isinstance(uploadContext, BigQueryUploadContext): return uploadContext["table"]
        elif isinstance(uploadContext, GoogleCopyContext): return self._get_upload_target(uploadContext["upload"])
        else: return uploadContext.get(NAME, "unnamed") if isinstance(uploadContext, Dict) else "unnamed"

    @BaseSession.ignore_exception
    def copy_data(self, read: Dict, upload: Dict, account: Account=dict(), **context) -> bool:
        read["rename"] = read.get("rename", self.get_rename_map(to=context.get("to", "name")))
        data = self.read_data(read, account)
        self.upload_data(data, [upload], account, **context)
        return True

    def format_upload_status(self, **context) -> str:
        formatted = list()
        for __idx, (__name, __status) in enumerate(self.uploadStatus.items(), start=1):
            __type = BIGQUERY_TABLE if re.match(r"^[\w\d]+\.[\w\d]+$", __name) else GOOGL_SHEETS
            formatted.append(UPLOAD_STATUS_MSG(__idx, __name, __type, __status))
        return '\n'.join([UPLOAD_STATUS_TITLE]+formatted) if formatted else str()

    ###################################################################
    ###################### Google Spread Sheets #######################
    ###################################################################

    @BaseSession.ignore_exception
    def upload_gspread(self, key: str, sheet: str, data: pd.DataFrame, columns: IndexLabel=list(),
                        mode: Literal["append","replace","ignore","upsert"]="append", primary_key: List[str]=list(),
                        set_date=None, cell=str(), read=None, name=str(), account: Account=dict(), **context) -> bool:
        params = dict(key=key, sheet=sheet, mode=mode, primary_key=primary_key, set_date=set_date, name=name, account=account)
        if mode in ("ignore","upsert"):
            read = self.set_base_sheet(read=read, **params, **context)
            mode = {"ignore":"append", "upsert":"replace"}.get(mode)
        data = self.from_base_data(data, read, **params, **context)
        data = self.map_upload_data(data, columns=columns, **params, **context)
        self.checkpoint(UPLOAD(name), where="upload_gspread", msg={KEY:key, SHEET:sheet, MODE:mode, DATA:data}, save=data)
        self.logger.info(log_table(data, name=name, key=key, sheet=sheet, mode=mode))
        cell = "A2" if mode == "replace" else (cell if cell else str())
        update_gspread(key, sheet, data, cell=cell, clear=(mode == "replace"), account=account)
        return True

    def set_base_sheet(self, key: str, sheet: str, read=None, name=str(), **context) -> GoogleReadContext:
        if isinstance(read, (BigQueryReadContext, ExcelReadContext)): return read
        read = read if isinstance(read, Dict) else dict()
        for __key, __value in zip([KEY, SHEET, NAME], [key, sheet, name]):
            if __key not in read: read[__key] = __value
        return read if isinstance(read, GspreadReadContext) else GspreadReadContext(**read)

    ###################################################################
    ######################### Google BigQuery #########################
    ###################################################################

    @BaseSession.ignore_exception
    def upload_gbq(self, table: str, project_id: str, data: pd.DataFrame, columns: IndexLabel=list(),
                    mode: Literal["append","replace","ignore","upsert"]="append", primary_key: List[str]=list(),
                    partition=None, set_date=None, read=None, progress=True, name=str(), account: Account=dict(), **context) -> bool:
        partition = self.validate_partition(partition, **context)
        params = dict(table=table, project_id=project_id, mode=mode,
            primary_key=primary_key, partition=partition, set_date=set_date, name=name, account=account)
        if mode in ("ignore","upsert"):
            read = self.set_base_query(read=read, **params, **context)
            mode = {"ignore":"append", "upsert":"replace"}.get(mode)
        data = self.from_base_data(data, read, **params, **context)
        data = self.map_upload_data(data, columns=columns, **params, **context)
        self.checkpoint(UPLOAD(name), where="upload_gbq", msg={TABLE:table, PID:project_id, MODE:mode, DATA:data}, save=data)
        self.logger.info(log_table(data, name=name, table=table, pid=project_id, mode=mode))
        upload_gbq(table, project_id, data, mode, partition, progress, account)
        return True

    def validate_partition(self, partition=None, startDate=None, endDate=None, busdate=False, **context) -> BigQueryPartition:
        if not (isinstance(partition, Dict) and partition.get("field")):
            return dict()
        elif (partition.get("type") in ("date","datetime")) and (partition.get("unit") != "hour"):
            if ("value" not in partition):
                left, right = self.get_date_pair(partition.get("left", startDate), partition.get("right", endDate), busdate=busdate)
                partition["left"], partition["right"] = left, right
            else: partition["value"] = self.get_date(partition["value"])
        return partition if isinstance(partition, BigQueryPartition) else BigQueryPartition(**partition)

    def set_base_query(self, table: str, project_id: str, read=None, partition=dict(), name=str(), **context) -> GoogleReadContext:
        if isinstance(read, (GspreadReadContext, ExcelReadContext)): return read
        read = read if isinstance(read, Dict) else dict()
        read = self.set_partition_query(read, table, partition=partition, name=name, **context)
        for __key, __value in zip([PID, NAME], [project_id, name]):
            if __key not in read: read[__key] = __value
        return read if isinstance(read, BigQueryReadContext) else BigQueryReadContext(**read)

    def set_partition_query(self, read: Dict, table: str, partition=dict(), **context) -> Dict:
        if QUERY in read: return read
        else: read[QUERY] = make_select_query(table, **partition)
        return read

    ###################################################################
    ########################## From Base Data #########################
    ###################################################################

    def from_base_data(self, data: pd.DataFrame, read: Optional[Dict]=None, mode=str(),
                        primary_key: List[str]=list(), name=str(), account: Account=dict(), **context) -> pd.DataFrame:
        if not isinstance(read, Dict): return data
        base = to_dataframe(self.read_data(read, account))
        if mode == "ignore":
            return self.map_ignore_data(data, base, primary_key=primary_key, name=name, **context)
        elif mode == "upsert":
            return self.map_upsert_data(data, base, primary_key=primary_key, name=name, **context)
        else: return self.map_upload_base(data, base, primary_key=primary_key, name=name, **context)

    def map_upload_base(self, data: pd.DataFrame, base: pd.DataFrame, name=str(), **context) -> pd.DataFrame:
        return data

    def map_ignore_data(self, data: pd.DataFrame, base: pd.DataFrame, primary_key: List[str], name=str(), **context) -> pd.DataFrame:
        if (not primary_key) or base.empty: return data
        data = data.merge(base[primary_key].drop_duplicates(primary_key), how="left", on=primary_key, indicator=True)
        return data[data["_merge"] == "left_only"].drop(columns=["_merge"])

    def map_upsert_data(self, data: pd.DataFrame, base: pd.DataFrame, primary_key: List[str],
                        agg_func: Dict=dict(), name=str(), **context) -> pd.DataFrame:
        if (not primary_key) or base.empty: return data
        agg_func = agg_func if agg_func else {__column:"first" for __column in base.columns if __column not in primary_key}
        return pd.concat([data, base]).groupby(primary_key).agg(agg_func).reset_index()

    ###################################################################
    ######################### Map Upload Data #########################
    ###################################################################

    def map_upload_data(self, data: pd.DataFrame, columns: IndexLabel=list(), primary_key: List[str]=list(),
                        set_date=None, name=str(), **context) -> pd.DataFrame:
        columns = cast_list(columns) if columns else self.get_upload_columns(name=name, **context)
        if columns:
            data = cloc(data, columns, if_null="pass", reorder=True)
        if primary_key:
            data = data.dropna(subset=primary_key, how="any").drop_duplicates(primary_key)
        if isinstance(set_date, Dict) and set_date:
            data = self.set_upload_date(data, set_date=set_date, name=name, **context)
        if data.empty:
            raise ValueError(UPLOAD_FAILED_MSG)
        return data

    def get_upload_columns(self, name=str(), **context) -> IndexLabel:
        return list()

    def set_upload_date(self, data: pd.DataFrame, set_date: Dict[str,DateFormat], name=str(), **context) -> pd.DataFrame:
        data = data.copy()
        for __key, __value in set_date.items():
            __date = self.get_date(__value)
            if (__key in data) and isinstance(__date, dt.date):
                data[__key] = __date
        return data


###################################################################
###################### Google Spread Sheets #######################
###################################################################

def load_gspread(key: str, sheet: str, account: Account=dict()) -> Worksheet:
    account = account if account and isinstance(account, dict) else read_gcloud(account)
    gs_acc = gspread.service_account_from_dict(account)
    gs = gs_acc.open_by_key(key)
    return gs.worksheet(sheet)


def gs_loaded(func):
    @functools.wraps(func)
    def wrapper(key=str(), sheet=str(), *args, account: Account=dict(), gs: Optional[Worksheet]=None, **kwargs):
        if not gs:
            gs = load_gspread(key, sheet, account)
        return func(key, sheet, *args, account=account, gs=gs, **kwargs)
    return wrapper


@gs_loaded
def read_gspread(key: str, sheet: str, fields: Optional[IndexLabel]=list(), default=None,
                if_null: Literal["drop","pass"]="pass", head=1, headers=None, numericise_ignore: NumericiseIgnore=list(),
                reorder=True, return_type: Optional[TypeHint]="dataframe", rename: RenameMap=dict(), convert_dtypes=True,
                account: Account=dict(), gs: Optional[Worksheet]=None) -> TabularData:
    if isinstance(numericise_ignore, bool):
        numericise_ignore = ["all"] if numericise_ignore else list()
    data = gs.get_all_records(head=head, default_blank=default, numericise_ignore=numericise_ignore, expected_headers=headers)
    data = convert_data(data, return_type)
    data = rename_data(data, rename)
    data = filter_data(data, cast_list(fields), default=default, if_null=if_null, reorder=reorder)
    return apply_data(data, apply=_convert_dtype, all_keys=True) if convert_dtypes else data


@gs_loaded
def clear_gspead(key: str, sheet: str, include_header=False, account: Account=dict(), gs: Optional[Worksheet]=None):
    if include_header: return gs.clear()
    last_row = len(gs.get_all_records())+1
    gs.insert_row([], 2)
    gs.delete_rows(3, last_row+2)


@gs_loaded
def update_gspread(key: str, sheet: str, data: TabularData, col='A', row=0, cell=str(), clear=False,
                    clear_header=False, account: Account=dict(), gs: Optional[Worksheet]=None):
    records = to_records(data)
    if not records: return
    values = [[_to_excel_date(__value) for __value in __m.values()] for __m in records]
    if clear:
        clear_gspead(gs=gs, include_header=clear_header)
    if not cell:
        cell = col+str(row if row else len(gs.get_all_records())+2)
        gs.add_rows(len(values))
    gs.update(cell, values)


def _convert_dtype(__value) -> Any:
    if isinstance(__value, str):
        if __value == "TRUE": return True
        elif __value == "FALSE": return False
        elif re.match(r"^\d[\d.]*%$", __value): return cast_float(__value) / 100
        else: return cast_datetime_format(__value, default=__value)
    else: return __value


def _to_excel_date(__value) -> Union[int,Any]:
    if isna(__value): return None
    elif not isinstance(__value, dt.date): return __value
    offset = 693594
    days = __value.toordinal() - offset
    if isinstance(__value, dt.datetime):
        seconds = (__value.hour*60*60 + __value.minute*60 + __value.second)/(24*60*60)
        return days + seconds
    return days


###################################################################
######################### Google BigQuery #########################
###################################################################

BIGQUERY_JOB = {"append":"WRITE_APPEND", "replace":"WRITE_TRUNCATE"}

class PartitionedIterator:
    data = None
    field = str()
    index = 0
    partition = list()
    drop_partition = False

    def __init__(self, data: pd.DataFrame, field: str, type: Literal["date","datetime","number"]="date",
                unit: Literal["auto","hour","day","month","year"]="auto", left=None, right=None, value=None,
                drop_partition=False, filter_partition=True, has_value=False, **kwargs):
        data, isCreated = self.create_partition(data, field, type, unit)
        self.data = data
        self.field = "_PARTITIONTIME" if isCreated else field
        self.index = 0
        self.partition = self.set_partition(data, self.field, type, left, right, value, (filter_partition and has_value))
        self.drop_partition = True if isCreated else drop_partition

    def create_partition(self, data: pd.DataFrame, field: str, type: Literal["date","datetime","number"]="date",
                        unit: Literal["auto","hour","day","month","year"]="auto") -> Tuple[pd.DataFrame,bool]:
        if type in ("date","datetime"):
            data = data.copy()
            if (type == "datetime") or (unit != "auto"):
                data["_PARTITIONTIME"] = data[field].apply(lambda x: get_datetime(x, unit=(unit if unit in DATE_UNIT else "day")))
                return data[data["_PARTITIONTIME"].notna()], True
            else: data[field] = data[field].apply(cast_date)
        return data[data[field].notna()], False

    def set_partition(self, data: pd.DataFrame, field: str, type: Literal["date","datetime","number"]="date",
                    left=None, right=None, value=None, filter_partition=True) -> List:
        if filter_partition:
            if field == "_PARTITIONTIME": left, right, value = cast_datetime(left), cast_datetime(right), cast_datetime(value)
            elif type == "date": left, right, value = cast_date(left), cast_date(right), cast_date(value)
            data = self.filter_partition(data, field, left, right, value)
        return sorted(data[field].unique())

    def filter_partition(self, data: pd.DataFrame, field: str, left=None, right=None, value=None) -> pd.DataFrame:
        __left = (data[field] >= left) if left is not None else data[field].notna()
        __right = (data[field] <= right) if right is not None else data[field].notna()
        __value = (data[field] == value) if value is not None else data[field].notna()
        return data[__left&__right&__value]

    def __iter__(self):
        return self

    def __next__(self):
        if self.index < len(self.partition):
            partition = self.data[self.data[self.field]==self.partition[self.index]]
            self.index += 1
            return partition.drop(columns=[self.field]) if self.drop_partition else partition
        else: raise StopIteration

    def __len__(self):
        return len(self.partition)


def create_connection(project_id: str, account: Account=dict()) -> bigquery.Client:
    account = account if account and isinstance(account, dict) else read_gcloud(account)
    return bigquery.Client.from_service_account_info(account, project=project_id)


def execute_query(query: str, project_id=str(), account: Account=dict()) -> RowIterator:
    client = create_connection(project_id, account)
    job = client.query(query)
    return job.result()


def make_select_query(table: str, columns: IndexLabel=list(), field=str(), type: Literal["date","datetime","number"]="date",
                    left=None, right=None, value=None, limited=True, has_value=True, **kwargs) -> str:
    query = f"SELECT {', '.join(columns) if columns else '*'} FROM `{table}`"
    if not field: return query + ';'
    elif (not limited) or has_value:
        return query + ' ' + get_partition_filter(field, type, left, right, value) + ';'
    else: raise ValueError(INVALID_MSG(PARTITION_FILTER))


def make_delete_query(table: str, field=str(), type: Literal["date","datetime","number"]="date",
                    left=None, right=None, value=None, limited=True, has_value=True, **kwargs) -> str:
    query = f"DELETE FROM `{table}`"
    if not field: return query + ' ' + "WHERE TRUE" + ';'
    elif (not limited) or has_value:
        return query + ' ' + get_partition_filter(field, type, left, right, value) + ';'
    else: raise ValueError(INVALID_MSG(PARTITION_FILTER))


def get_partition_filter(field=str(), type: Literal["date","datetime","number"]="date", left=None, right=None, value=None, **kwargs) -> str:
    if type in ("date","datetime"):
        return _get_datetime_partition_filter(field, type, left, right, value)
    elif value is not None:
        return f"WHERE {field} = {value}"
    elif (left is not None) and (right is not None):
        if left == right: return f"WHERE {field} = {left}"
        else: return f"WHERE {field} BETWEEN {left} AND {right}"
    elif left is not None:
        return f"WHERE {field} >= {left}"
    elif right is not None:
        return f"WHERE {field} <= {right}"
    else: return "WHERE TRUE"


def _get_datetime_partition_filter(field=str(), dtype: Literal["date","datetime"]="date", left=None, right=None, value=None) -> str:
    def set_filed(field: str, dtype: Literal["date","datetime"], value: dt.date) -> str:
        return f"DATE({field})" if (dtype == "datetime") and not isinstance(value, dt.datetime) else field
    if isinstance(value, dt.date):
        return f"WHERE {set_filed(field, dtype, value)} = '{value}'"
    elif isinstance(left, dt.date) and isinstance(right, dt.date) and (type(left) == type(right)):
        field = set_filed(field, dtype, left)
        if left == right: return f"WHERE {field} = '{left}'"
        else: return f"WHERE {field} BETWEEN '{left}' AND '{right}'"
    elif isinstance(left, dt.date):
        return f"WHERE {set_filed(field, dtype, left)} >= '{left}'"
    elif isinstance(right, dt.date):
        return f"WHERE {set_filed(field, dtype, right)} <= '{right}'"
    else: return "WHERE TRUE"


def read_gbq(query: str, project_id: str, return_type: Literal["records","dataframe"]="dataframe",
            account: Account=dict()) -> TabularData:
    client = create_connection(project_id, account)
    query_job = client.query(query if query.upper().startswith("SELECT") else f"SELECT * FROM `{query}`;")
    if return_type == "dataframe": return query_job.to_dataframe()
    else: return [dict(row.items()) for row in query_job.result()]


def upload_gbq(table: str, project_id: str, data: pd.DataFrame, if_exists: Literal["replace","append"]="append",
                partition: BigQueryPartition=dict(), progress=True, account: Account=dict()):
    client = create_connection(project_id, account)
    if if_exists == "replace":
        client.query(make_delete_query(table, **partition))
    job_config = LoadJobConfig(write_disposition="WRITE_APPEND")
    iterator = PartitionedIterator(data, **partition) if _is_partitioned_data(data, partition) else [data]
    for __data in tqdm(iterator, desc=UPLOAD_GBQ_MSG(table), disable=(not progress)):
        client.load_table_from_dataframe(__data, f"{project_id}.{table}", job_config=job_config)


def _is_partitioned_data(data: pd.DataFrame, partition: BigQueryPartition=dict()) -> bool:
    return isinstance(partition, Dict) and partition.get("field") and (partition["field"] in data)


def upsert_gbq(table: str, project_id: str, data: pd.DataFrame, primary_key: List[str],
                base: Optional[pd.DataFrame]=None, account: Account=dict()):
    if df_empty(base):
        base = read_gbq(table, project_id, return_type="dataframe", account=account)
    data = data.set_index(primary_key).combine_first(base.set_index(primary_key)).reset_index()
    client = create_connection(project_id, account)
    client.query(f"DELETE FROM `{table}` WHERE TRUE;")
    job_config = LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(data, f"{project_id}.{table}", job_config=job_config)
