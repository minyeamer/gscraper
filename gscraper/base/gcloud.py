from __future__ import annotations
from gscraper.base.abstract import OptionalDict, TypedRecords, Value, ValueSet, GCLOUD_CONTEXT, INVALID_OBJECT_MSG, INVALID_OBJECT_TYPE_MSG
from gscraper.base.session import BaseSession

from gscraper.base.types import _KT, Context, TypeHint, IndexLabel, RenameMap
from gscraper.base.types import TabularData, PostData, from_literal

from gscraper.utils.cast import cast_list, cast_datetime_format
from gscraper.utils.date import get_datetime, get_date, DATE_UNIT
from gscraper.utils.logs import log_table
from gscraper.utils.map import isna, df_empty, to_array, kloc, to_dict, to_records, read_excel
from gscraper.utils.map import cloc, to_dataframe, convert_data, rename_data, filter_data, apply_data

from google.oauth2 import service_account
from google.oauth2.service_account import IDTokenCredentials
from google.auth.transport.requests import AuthorizedSession

from gspread.worksheet import Worksheet
import gspread

from google.cloud import bigquery
from google.cloud.bigquery.table import RowIterator
from google.cloud.bigquery.job import LoadJobConfig

from abc import ABCMeta
from tqdm import tqdm
import copy
import functools
import os
import requests
import sys

from typing import Any, Dict, Iterable, List, Literal, Optional, Sequence, Union
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

INVALID_QUERY_MSG = "To update data, parameters for source and destination are required."
UPLOAD_GBQ_MSG = lambda table, partitioned=False: f"Uploading data to '{table}'" + (" by partition" if partitioned else str())

BIGQUERY_TYPE = "BigQueryType"
BIGQUERY_MODE = "BigQueryMode"
BIGQUERY_SCHEMA = "BigQuerySchema"


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
                size: Optional[int]=None, name=str(), **kwargs):
        super().__init__(key=key, sheet=sheet,
            optional=dict(
                fields=fields, default=default, if_null=if_null, head=head, headers=headers,
                str_cols=str_cols, return_type=return_type, rename=rename, to=to, size=size, name=name, **kwargs),
            null_if=dict(if_null="pass", head=1, to="name", return_type="dataframe", name=str()))


class GspreadQueryContext(GspreadReadContext):
    def __init__(self, key: str, sheet: str, fields: Optional[IndexLabel]=None, default: Optional[Any]=None,
                if_null: Literal["drop","pass"]="drop", axis=0, dropna=True, strict=True, unique=False,
                head=1, headers: Optional[IndexLabel]=None, str_cols: Optional[NumericiseIgnore]=None,
                arr_cols: Optional[IndexLabel]=None, as_records=False, as_frame=False,
                rename: Optional[RenameMap]=None, to: Optional[Literal["desc","name"]]="name",
                size: Optional[int]=None, name=str(), **kwargs):
        return_type = "records" if as_records else "dataframe"
        super().__init__(
            key, sheet, fields, default, if_null, head, headers, str_cols, return_type, rename, to, size, name)
        self.update_notna(
                axis=axis, dropna=dropna, strict=strict, unique=unique, arr_cols=arr_cols,
                as_records=as_records, as_frame=as_frame, **kwargs,
            null_if=dict(axis=0, dropna=True, strict=True, unique=False, as_records=False, as_frame=False))


class BigQueryReadContext(OptionalDict):
    def __init__(self, query: str, project_id: str, return_type: Optional[TypeHint]="dataframe",
                size: Optional[int]=None, name=str(), **kwargs):
        super().__init__(query=query, project_id=project_id,
            optional=dict(return_type=return_type, size=size, name=name, **kwargs),
            null_if=dict(return_type="dataframe", name=str()))


class BigQueryContext(BigQueryReadContext):
    def __init__(self, query: str, project_id: str, axis=0, dropna=True, strict=True, unique=False,
                arr_cols: Optional[IndexLabel]=None, as_records=False, as_frame=False,
                size: Optional[int]=None, name=str(), **kwargs):
        return_type = "records" if as_records else "dataframe"
        super().__init__(query, project_id, return_type, size, name)
        self.update_notna(
                axis=axis, dropna=dropna, strict=strict, unique=unique, arr_cols=arr_cols,
                as_records=as_records, as_frame=as_frame, **kwargs,
            null_if=dict(axis=0, dropna=True, strict=True, unique=False, as_records=False, as_frame=False))


class ExcelReadContext(OptionalDict):
    def __init__(self, file_path: str, sheet_name: Union[str,int]=0, fields: Optional[IndexLabel]=None,
                default: Optional[Any]=None, if_null: Literal["drop","pass"]="pass",
                str_cols: Optional[NumericiseIgnore]=None, return_type: Optional[TypeHint]="dataframe",
                rename: Optional[RenameMap]=None, to: Optional[Literal["desc","name"]]="name",
                file_pattern=False, reverse=False, size: Optional[int]=None, name=str(), **kwargs):
        super().__init__(file_path=file_path, sheet_name=sheet_name,
            optional=dict(
                fields=fields, default=default, if_null=if_null, str_cols=str_cols, return_type=return_type,
                rename=rename, to=to, file_pattern=file_pattern, reverse=reverse, size=size, name=name, **kwargs),
            null_if=dict(if_null="pass", return_type="dataframe", to="name", file_pattern=False, reverse=False, name=str()))


class ExcelQueryContext(ExcelReadContext):
    def __init__(self, file_path: str, sheet_name: Union[str,int]=0, fields: Optional[IndexLabel]=None,
                default: Optional[Any]=None, if_null: Literal["drop","pass"]="pass", axis=0,
                dropna=True, strict=True, unique=False, str_cols: Optional[NumericiseIgnore]=None,
                arr_cols: Optional[IndexLabel]=None, as_records=False, as_frame=False,
                rename: Optional[RenameMap]=None, to: Optional[Literal["desc","name"]]="name",
                file_pattern=False, reverse=False, size: Optional[int]=None, name=str(), **kwargs):
        return_type = "records" if as_records else "dataframe"
        super().__init__(
            file_path, sheet_name, fields, default, if_null, str_cols, return_type, rename, to, file_pattern, reverse, size, name)
        self.update_notna(
                axis=axis, dropna=dropna, strict=strict, unique=unique, arr_cols=arr_cols,
                as_records=as_records, as_frame=as_frame, **kwargs,
            null_if=dict(axis=0, dropna=True, strict=True, unique=False, as_records=False, as_frame=False))


GoogleReadContext = Union[GspreadReadContext, BigQueryReadContext, ExcelReadContext]
GoogleQueryContext = Union[GspreadQueryContext, BigQueryContext, ExcelQueryContext]

class GoogleQueryList(TypedRecords):
    dtype = (GspreadQueryContext, BigQueryContext, ExcelQueryContext)
    typeCheck = True

    def __init__(self, *args: GoogleQueryContext):
        super().__init__(*args)

    def validate_dtype(self, __object) -> Dict:
        if (not self.dtype) or isinstance(__object, self.dtype): return __object
        elif isinstance(__object, Dict):
            if len(kloc(__object, [KEY, SHEET], if_null="drop")) == 2: return GspreadQueryContext(**__object)
            elif len(kloc(__object, [QUERY, PID], if_null="drop")) == 2: return BigQueryContext(**__object)
            elif FILEPATH in __object: return ExcelQueryContext(**__object)
            else: self.raise_dtype_error(__object, "QueryContext")
        else: self.raise_dtype_error(__object, dict.__name__)


class GoogleQueryReader(BaseSession):
    __metaclass__ = ABCMeta
    operation = "googleQueryReader"

    def read_gspread(self, key: str, sheet: str, fields: IndexLabel=list(), default=None,
                    if_null: Literal["drop","pass"]="pass", head=1, headers=None,
                    str_cols: NumericiseIgnore=list(), return_type: Optional[TypeHint]="dataframe",
                    rename: Optional[RenameMap]=None, to: Optional[Literal["desc","name"]]="name",
                    size: Optional[int]=None, name=str(), account: Account=dict(), **context) -> TabularData:
        rename_map = rename if rename else self.get_rename_map(to=to, query=True)
        kwargs = dict(numericise_ignore=str_cols, return_type=return_type, rename=rename_map, account=account)
        data = read_gspread(key, sheet, fields, default, if_null, head, headers, **kwargs)
        if isinstance(size, int): data = data[:size]
        self.checkpoint(READ(name), where="read_gspread", msg={KEY:key, SHEET:sheet, DATA:data}, save=data)
        self.logger.info(log_table(data, name=name, key=key, sheet=sheet, dump=self.logJson))
        return data

    def read_gbq(self, query: str, project_id: str, return_type: Literal["records","dataframe"]="dataframe",
                size: Optional[int]=None, name=str(), account: Account=dict(), **context) -> TabularData:
        data = read_gbq(query, project_id, return_type=return_type, account=account)
        if isinstance(size, int): data = data[:size]
        self.checkpoint(READ(name), where="read_gspread", msg={QUERY:query, PID:project_id, DATA:data}, save=data)
        self.logger.info(log_table(data, name=name, query=query, project_id=project_id, dump=self.logJson))
        return data

    def read_excel(self, file_path: str, sheet_name: Union[str,int]=0,
                    fields: IndexLabel=list(), default=None, if_null: Literal["drop","pass"]="pass",
                    str_cols: NumericiseIgnore=list(), return_type: Literal["records","dataframe"]="dataframe",
                    rename: Optional[RenameMap]=None, to: Optional[Literal["desc","name"]]="name",
                    file_pattern=False, reverse=False, size: Optional[int]=None, name=str(), **context) -> TabularData:
        rename_map = rename if rename else self.get_rename_map(to=to, query=True)
        kwargs = dict(str_cols=str_cols, return_type=return_type, rename=rename_map, file_pattern=file_pattern, reverse=reverse)
        data = read_excel(file_path, sheet_name, fields, default, if_null, **kwargs)
        if isinstance(size, int): data = data[:size]
        self.checkpoint(READ(name), where="read_excel", msg={FILEPATH:file_path, SHEETNAME:sheet_name, DATA:data}, save=data)
        self.logger.info(log_table(data, name=name, file_path=file_path, sheet_name=sheet_name, dump=self.logJson))
        return data

    def set_query(self, queryList: GoogleQueryList=list(), account: Account=dict()):
        for queryContext in GoogleQueryList(*queryList):
            if isinstance(queryContext, GspreadQueryContext):
                data = self.read_gspread(**queryContext, account=account)
            elif isinstance(queryContext, BigQueryContext):
                data = self.read_gbq(**queryContext, account=account)
            elif isinstance(queryContext, ExcelQueryContext):
                data = self.read_excel(**queryContext, account=account)
            else: continue
            self.update(self.map_query_data(data, **queryContext), inplace=True)

    def map_query_data(self, data: pd.DataFrame, axis=0, dropna=True, strict=True, unique=False,
                        arr_cols: IndexLabel=list(), as_records=False, as_frame=False,
                        name=str(), **context) -> Dict[_KT,Union[List,Any]]:
        if as_records or as_frame: return {name: data}
        arr_cols = cast_list(arr_cols)
        __m = to_dict((data.T if axis == 1 else data), "list", depth=2)
        for __key, __values in __m.copy().items():
            values = to_array(__values, dropna=dropna, strict=strict, unique=unique)
            if (len(values) < 2) and (__key not in arr_cols):
                __m[__key] = values[0] if len(values) == 1 else None
            else: __m[__key] = values
        return __m


###################################################################
###################### Google Cloud Uploader ######################
###################################################################

class GspreadUpdateContext(OptionalDict):
    def __init__(self, key: str, sheet: str, columns: IndexLabel=list(), mode: Literal["append","replace","upsert"]="append",
                cell: Optional[str]=None, base_sheet: Optional[str]=None, primary_key: Optional[_KT]=None,
                default: Optional[Any]=None, head=1, headers: Optional[IndexLabel]=None,
                str_cols: Optional[NumericiseIgnore]=None, rename: Optional[RenameMap]=None,
                to: Optional[Literal["desc","name"]]="name", name=str(), **kwargs):
        super().__init__(key=key, sheet=sheet,
            optional=dict(
                columns=columns, mode=mode, cell=cell, base_sheet=base_sheet, primary_key=primary_key, default=default,
                head=head, headers=headers, str_cols=str_cols, rename=rename, to=to, name=name, **kwargs),
            null_if=dict(mode="append", head=1, to="name", name=str()))


class BigQueryUploadContext(OptionalDict):
    def __init__(self, table: str, project_id: str, columns: IndexLabel=list(),
                mode: Literal["append","replace","upsert"]="append", partition: Optional[str]=None,
                partition_by: Literal["auto","second","minute","hour","day","month","year","date"]="auto",
                base_query: Optional[str]=None, primary_key: Optional[_KT]=None, name=str(), **kwargs):
        super().__init__(table=table, project_id=project_id,
            optional=dict(
                columns=columns, mode=mode, partition=partition, partition_by=partition_by,
                base_query=base_query, primary_key=primary_key, name=name, **kwargs),
            null_if=dict(mode="append", partition_by="auto", name=str()))


class GoogleUpdateContext(OptionalDict):
    def __init__(self, from_key=str(), from_sheet=str(), from_query=str(), from_pid=str(),
                to_key=str(), to_sheet=str(), to_table=str(), to_pid=str(), columns: IndexLabel=list(),
                mode: Literal["append","replace"]="append", primary_key: Optional[_KT]=None, cell: Optional[str]=None,
                default: Optional[Any]=None, head=1, headers: Optional[IndexLabel]=None, str_cols: Optional[NumericiseIgnore]=None,
                rename: Optional[RenameMap]=None, to: Optional[Literal["desc","name"]]="name", partition: Optional[str]=None,
                partition_by: Optional[Literal["auto","second","minute","hour","day","month","year","date"]]="auto", name=str(), **kwargs):
        super().__init__(**self.validate_key(from_key, from_sheet, from_query, from_pid, to_key, to_sheet, to_table, to_pid),
            optional=dict(
                columns=columns, mode=mode, primary_key=primary_key, cell=cell, default=default, head=head, headers=headers,
                str_cols=str_cols, rename=rename, to=to, partition=partition, partition_by=partition_by, name=name, **kwargs),
            null_if=dict(mode="append", head=1, to="name", name=str()))

    def validate_key(self, from_key=str(), from_sheet=str(), from_query=str(), from_pid=str(),
                    to_key=str(), to_sheet=str(), to_table=str(), to_pid=str()) -> Context:
        if not (((from_key and from_sheet) or (from_query or from_pid)) and ((to_key and to_sheet) or (to_table and to_pid))):
            raise ValueError(INVALID_QUERY_MSG)
        else: return kloc(locals(), FROM_GS + FROM_GBQ + TO_GS + TO_GBQ, if_null="drop")


GoogleUploadMode = Literal["append","replace","upsert"]
GoogleUploadContext = Union[GspreadUpdateContext, BigQueryUploadContext, GoogleUpdateContext]

class GoogleUploadList(TypedRecords):
    dtype = (GspreadUpdateContext, BigQueryUploadContext, GoogleUpdateContext)
    typeCheck = True

    def __init__(self, *args: GoogleUploadContext):
        super().__init__(*args)

    def validate_dtype(self, __object) -> Dict:
        if (not self.dtype) or isinstance(__object, self.dtype): return __object
        elif isinstance(__object, Dict):
            if len(kloc(__object, [KEY, SHEET], if_null="drop")) == 2: return GspreadUpdateContext(**__object)
            elif len(kloc(__object, [TABLE, PID], if_null="drop")) == 2: return BigQueryUploadContext(**__object)
            elif (((len(kloc(__object, FROM_GS)) == 2) or (len(kloc(__object, FROM_GBQ)) == 2)) and
                    ((len(kloc(__object, TO_GS)) == 2) or (len(kloc(__object, TO_GBQ)) == 2))):
                return GoogleUpdateContext(**__object)
            else: self.raise_dtype_error(__object, "UploadContext")
        else: self.raise_dtype_error(__object, dict.__name__)


class GoogleUploader(BaseSession):
    __metaclass__ = ABCMeta
    operation = "googleUploader"
    uploadStatus = dict()

    def upload_data(self, data: TabularData, uploadList: GoogleUploadList=list(), account: Account=dict(), **context):
        data = to_dataframe(data)
        context = GCLOUD_CONTEXT(**context)
        for uploadContext in GoogleUploadList(*uploadList):
            if (not data.empty) and isinstance(uploadContext, GspreadUpdateContext):
                status = self.upload_gspread(data=data.copy(), **uploadContext, account=account, **context)
            elif (not data.empty) and isinstance(uploadContext, BigQueryUploadContext):
                status = self.upload_gbq(data=data.copy(), **uploadContext, account=account, **context)
            elif isinstance(uploadContext, GoogleUpdateContext):
                status = self.update_data(**uploadContext, account=account, **context)
            else: status = False
            self.uploadStatus[uploadContext.get(NAME, str())] = status

    def get_upload_columns(self, name: str, **context) -> IndexLabel:
        return list()

    def _validate_upload_columns(self, name: str, data: pd.DataFrame, columns: IndexLabel=list(), **context) -> IndexLabel:
        columns = columns if columns else self.get_upload_columns(name=name, **context)
        if not columns: return data
        else: return cloc(data, columns, if_null="pass", reorder=True)

    def _validate_primary_key(self, data: pd.DataFrame, primary_key: _KT=list()) -> pd.DataFrame:
        if not primary_key: return data
        primary_key = cast_list(primary_key)
        for __key in primary_key:
            data = data[data[__key].notna()]
        return data.drop_duplicates(primary_key)

    def map_upload_base(self, data: pd.DataFrame, base: pd.DataFrame, name=str(), **context) -> pd.DataFrame:
        return cloc(data, base.columns, if_null="pass")

    def map_upload_data(self, data: pd.DataFrame, name=str(), **context) -> pd.DataFrame:
        return data

    ###################################################################
    ###################### Google Spread Sheets #######################
    ###################################################################

    @BaseSession.catch_exception
    def upload_gspread(self, key: str, sheet: str, data: pd.DataFrame, columns: IndexLabel=list(),
                        mode: Literal["append","replace","upsert"]="append", primary_key: _KT=list(), cell=str(), base_sheet=str(),
                        default=None, head=1, headers=None, str_cols: NumericiseIgnore=list(), rename: Optional[RenameMap]=None,
                        to: Optional[Literal["desc","name"]]="name", name=str(), account: Account=dict(), **context) -> bool:
        data = self._validate_upload_columns(name, data, columns, key=key, sheet=sheet, **context)
        if base_sheet or (mode == "upsert"):
            data = self.from_base_sheet(**self.from_locals(locals()))
            if mode == "upsert": mode = "replace"
        data = self.map_upload_data(data,
            key=key, sheet=sheet, mode=mode, primary_key=primary_key, name=name, account=account, **context)
        data = self._validate_primary_key(data, primary_key)
        self.checkpoint(UPLOAD(name), where="upload_gspread", msg={KEY:key, SHEET:sheet, MODE:mode, DATA:data}, save=data)
        self.logger.info(log_table(data, name=name, key=key, sheet=sheet, mode=mode, dump=self.logJson))
        cell = "A2" if mode == "replace" else (cell if cell else str())
        update_gspread(key, sheet, data, cell=cell, clear=(mode == "replace"), account=account)
        return True

    def from_base_sheet(self, key: str, sheet: str, data: pd.DataFrame, mode: Literal["append","replace","upsert"]="append",
                        base_sheet=str(), primary_key: _KT=list(), name=str(), **context) -> pd.DataFrame:
        base_sheet = base_sheet if base_sheet else sheet
        base = self.read_gs_base(key, base_sheet, name, **context)
        if (mode == "upsert") and primary_key:
            data = data.set_index(primary_key).combine_first(base.set_index(primary_key)).reset_index()
        return self.map_upload_base(data, base, primary_key=primary_key, name=name, **context)

    def read_gs_base(self, key: str, sheet: str, name=str(), default=None, head=1, headers=None,
                    str_cols: NumericiseIgnore=list(), rename: Optional[RenameMap]=None,
                    to: Optional[Literal["desc","name"]]="name", account: Account=dict(), **context) -> pd.DataFrame:
        data = read_gspread(key, sheet, default=default, head=head, headers=headers, numericise_ignore=str_cols,
                            rename=(rename if rename else self.get_rename_map(to=to)), account=account)
        self.checkpoint(READ(name), where="read_gs_base", msg={KEY:key, SHEET:sheet, DATA:data}, save=data)
        self.logger.info(log_table(data, name=name, key=key, sheet=sheet, dump=self.logJson))
        return data

    ###################################################################
    ######################### Google BigQuery #########################
    ###################################################################

    @BaseSession.catch_exception
    def upload_gbq(self, table: str, project_id: str, data: pd.DataFrame, columns: IndexLabel=list(),
                    mode: Literal["append","replace","upsert"]="append", primary_key: _KT=list(), partition=str(),
                    partition_by: Literal["auto","second","minute","hour","day","date"]="auto",
                    progress=True, base_query=str(), name=str(), account: Account=dict(), **context) -> bool:
        data = self._validate_upload_columns(name, data, columns, table=table, project_id=project_id, **context)
        if base_query or (mode == "upsert"):
            data = self.from_base_query(**self.from_locals(locals()))
            if mode == "upsert": mode = "replace"
        data = self.map_upload_data(data,
            table=table, project_id=project_id, mode=mode, primary_key=primary_key, name=name, account=account, **context)
        data = self._validate_primary_key(data, primary_key)
        self.checkpoint(UPLOAD(name), where="upload_gbq", msg={TABLE:table, PID:project_id, MODE:mode, DATA:data}, save=data)
        self.logger.info(log_table(data, name=name, table=table, pid=project_id, mode=mode, dump=self.logJson))
        upload_gbq(table, project_id, data, mode, partition, partition_by, progress, account)
        return True

    def from_base_query(self, table: str, project_id: str, data: pd.DataFrame, mode: Literal["append","replace","upsert"]="append",
                        base_query=str(), primary_key: _KT=list(), name=str(), account: Account=dict(), **context) -> pd.DataFrame:
        base_query = base_query if base_query else table
        base = self.read_gbq_base(base_query, project_id, name, account)
        if (mode == "upsert") and primary_key:
            data = data.set_index(primary_key).combine_first(base.set_index(primary_key)).reset_index()
        return self.map_upload_base(data, base, primary_key=primary_key, name=name, **context)

    def read_gbq_base(self, query: str, project_id: str, name=str(), account: Account=dict()) -> pd.DataFrame:
        data = read_gbq(query, project_id, return_type="dataframe", account=account)
        self.checkpoint(READ(name), where="read_gbq_base", msg={QUERY:query, PID:project_id}, save=data)
        self.logger.info(log_table(data, name=name, query=query, pid=project_id, dump=self.logJson))
        return data

    ###################################################################
    ########################### Update Data ###########################
    ###################################################################

    @BaseSession.catch_exception
    def update_data(self, from_key=str(), from_sheet=str(), from_query=str(), from_pid=str(),
                    to_key=str(), to_sheet=str(), to_table=str(), to_pid=str(), columns: IndexLabel=list(),
                    mode: Literal["append","replace"]="append", primary_key: _KT=list(),
                    cell=str(), default=None, head=1, headers=None, str_cols: NumericiseIgnore=list(),
                    rename: Optional[RenameMap]=None, to: Optional[Literal["desc","name"]]="desc", 
                    partition=str(), partition_by: Literal["auto","second","minute","hour","day","date"]="auto",
                    progress=True, name=str(), account: Account=dict(), **context) -> bool:
        if from_key and from_sheet:
            data = self.read_gs_base(from_key, from_sheet, name, default, head, headers, str_cols, rename, to, account)
        elif from_query and from_pid:
            data = self.read_gbq_base(from_query, from_pid, name, account)
        else: return False
        if to_key and to_sheet:
            return self.upload_gspread(
                to_key, to_sheet, data, columns, mode, primary_key, cell, name=name, account=account, **context)
        elif to_table and to_pid:
            return self.upload_gbq(
                to_table, to_pid, data, columns, mode, primary_key, partition, partition_by, progress, name=name, account=account, **context)
        else: return False


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


def _cast_boolean(__object) -> Union[bool,Any]:
    return {"TRUE":True, "FALSE":False}.get(__object, __object)


def _to_excel_date(__object) -> Union[int,Any]:
    if isna(__object): return None
    elif not isinstance(__object, dt.date): return __object
    offset = 693594
    days = __object.toordinal() - offset
    if isinstance(__object, dt.datetime):
        seconds = (__object.hour*60*60 + __object.minute*60 + __object.second)/(24*60*60)
        return days + seconds
    return days


@gs_loaded
def read_gspread(key: str, sheet: str, fields: Optional[IndexLabel]=list(), default=None,
                if_null: Literal["drop","pass"]="pass", head=1, headers=None, numericise_ignore: NumericiseIgnore=list(),
                reorder=True, return_type: Optional[TypeHint]="dataframe", rename: RenameMap=dict(),
                account: Account=dict(), gs: Optional[Worksheet]=None) -> TabularData:
    if isinstance(numericise_ignore, bool): numericise_ignore = ["all"] if numericise_ignore else list()
    data = gs.get_all_records(head=head, default_blank=default, numericise_ignore=numericise_ignore, expected_headers=headers)
    data = convert_data(data, return_type)
    data = rename_data(data, rename)
    data = filter_data(data, fields, default=default, if_null=if_null, reorder=reorder)
    return apply_data(data, apply=(lambda x: cast_datetime_format(x, default=_cast_boolean(x))), all_keys=True)


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


###################################################################
######################### Google BigQuery #########################
###################################################################

BIGQUERY_JOB = {"append":"WRITE_APPEND", "replace":"WRITE_TRUNCATE"}


def create_connection(project_id: str, account: Account=dict()) -> bigquery.Client:
    account = account if account and isinstance(account, dict) else read_gcloud(account)
    return bigquery.Client.from_service_account_info(account, project=project_id)


def execute_query(query: str, project_id=str(), account: Account=dict()) -> RowIterator:
    client = create_connection(project_id, account)
    job = client.query(query)
    return job.result()


def read_gbq(query: str, project_id: str, return_type: Literal["records","dataframe"]="dataframe",
            account: Account=dict()) -> TabularData:
    client = create_connection(project_id, account)
    query_job = client.query(query if query.upper().startswith("SELECT") else f"SELECT * FROM `{query}`;")
    if return_type == "dataframe": return query_job.to_dataframe()
    else: return [dict(row.items()) for row in query_job.result()]


def upload_gbq(table: str, project_id: str, data: pd.DataFrame, if_exists: Literal["replace","append"]="append",
            partition=str(), partition_by: Literal["auto","second","minute","hour","day","month","year","date"]="auto",
            progress=True, account: Account=dict()):
    client = create_connection(project_id, account)
    if if_exists == "replace":
        client.query(f"DELETE FROM `{table}` WHERE TRUE;")
    job_config = LoadJobConfig(write_disposition="WRITE_APPEND")
    iterator = _partition_by(data, partition, partition_by)
    for __data in tqdm(iterator, desc=UPLOAD_GBQ_MSG(table, partitioned=(len(iterator) > 1)), disable=(not progress)):
        client.load_table_from_dataframe(__data, f"{project_id}.{table}", job_config=job_config)


def _partition_by(data: pd.DataFrame, partition=str(),
                partition_by: Literal["auto","second","minute","hour","day","month","year","date"]="auto") -> Sequence[pd.DataFrame]:
    if (not partition) or (partition not in data): return [data]
    elif partition_by.upper() in DATE_UNIT+["date"]:
        set_partition = (lambda x: get_datetime(x, datetimePart=partition_by)) if partition_by in DATE_UNIT else get_date
        data["_PARTITIONTIME"] = data[partition].apply(set_partition)
        return [data[data["_PARTITIONTIME"]==date].drop(columns="_PARTITIONTIME") for date in sorted(data["_PARTITIONTIME"].unique())]
    else: return [data[data[partition]==part] for part in sorted(data[partition].unique())]


def upsert_gbq(table: str, project_id: str, data: pd.DataFrame, primary_key: _KT,
                base: Optional[pd.DataFrame]=None, account: Account=dict()):
    if df_empty(base):
        base = read_gbq(table, project_id, return_type="dataframe", account=account)
    data = data.set_index(primary_key).combine_first(base.set_index(primary_key)).reset_index()
    client = create_connection(project_id, account)
    client.query(f"DELETE FROM `{table}` WHERE TRUE;")
    job_config = LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(data, f"{project_id}.{table}", job_config=job_config)
