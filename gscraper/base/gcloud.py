from gscraper.base.abstract import TypedDict, TypedRecords, GCLOUD_CONTEXT
from gscraper.base.session import BaseSession, Schema, Field, INVALID_OBJECT_MSG, INVALID_OBJECT_TYPE_MSG

from gscraper.base.types import _KT, TypeHint, IndexLabel, RenameMap
from gscraper.base.types import TabularData, Account, PostData, is_records, from_literal

from gscraper.utils.cast import cast_str, cast_list, cast_float, cast_int, cast_datetime_format
from gscraper.utils.date import get_datetime, get_timestamp, get_time, get_date, DATE_UNIT
from gscraper.utils.logs import log_table
from gscraper.utils.map import isna, df_exists, df_empty, to_array, get_scala, kloc, to_dict, to_records
from gscraper.utils.map import cloc, to_dataframe, apply_df, convert_data, rename_data, filter_data, apply_data

from google.oauth2 import service_account
from google.oauth2.service_account import IDTokenCredentials
from google.auth.transport.requests import AuthorizedSession

from gspread.worksheet import Worksheet
import gspread

from pandas_gbq.gbq import InvalidSchema
import pandas_gbq

from abc import ABCMeta
from tqdm.auto import tqdm
import functools
import os
import requests

from typing import Any, Dict, List, Literal, Optional, Sequence, Union
from collections import defaultdict
import datetime as dt
import json
import pandas as pd


ENV_PATH = "env/"
GCLOUD_ACCOUNT = ENV_PATH+"gcloud.json"
GCLOUD_DATA = ENV_PATH+"data.json"

KEY, SHEET, FIELDS = "key", "sheet", "fields"
TABLE, QUERY, PID = "table", "query", "project_id"
MODE, DATA = "mode", "data"

FROM_GS, TO_GBQ = ["from_key", "from_sheet"], ["to_table", "to_pid"]
FROM_GBQ, TO_GS = ["from_query", "from_pid"], ["to_key", "to_sheet"]

READ = lambda name=str(): f"read_{name}" if name else "read"
UPLOAD = lambda name=str(): f"upload_{name}" if name else "upload"


###################################################################
############################# Messages ############################
###################################################################

INVALID_QUERY_MSG = "To update data, parameters for source and destination are required."

INVALID_AXIS_MSG = lambda axis: f"'{axis}' is not valid axis. Only allowed in table(-1), each column(0), each row(1)."

INVALID_GS_ACTION_MSG = lambda action: f"'{action}' is not valid action for gspread task."

SCHEMA_MISMATCH_MSG = "Please verify that the structure and data types in the DataFrame match the schema of the destination table."
INVALID_PRIMARY_KEY_MSG = "Please verify that a primary key exists and is in both DataFrame objects."

BIGQUERY_PARTITION_MSG = "Uploading partitioned data to Google BigQuery"

BIGQUERY_FIELD = "BigQueryField"
BIGQUERY_TYPE = "BigQueryType"
BIGQUERY_MODE = "BigQueryMode"
BIGQUERY_SCHEMA = "BigQuerySchema"


###################################################################
###################### Google Authorization #######################
###################################################################

def read_json(file: str) -> Dict:
    if os.path.exists(file):
        with open(file, "r", encoding="utf-8") as f:
            return json.loads(f.read())
    else: return dict()


def read_gcloud(file=str()) -> Account:
    return read_json(str(file) if str(file).endswith(".json") else GCLOUD_ACCOUNT)


def read_data(file=str()) -> PostData:
    return read_json(str(file) if str(file).endswith(".json") else GCLOUD_DATA)


def fetch_gcloud_credentials(audience=str(), account: Account=dict()) -> IDTokenCredentials:
    account = account if account and isinstance(account, dict) else read_gcloud(account)
    audience = audience if audience else account.get("audience", str())
    return service_account.IDTokenCredentials.from_service_account_info(account, target_audience=audience)


def fetch_gcloud_authorization(audience=str(), account: Account=dict()) -> str:
    credentials = fetch_gcloud_credentials(audience, account)
    auth_session = AuthorizedSession(credentials)
    auth_session.get(audience)
    return "Bearer "+credentials.token


def gcloud_authorized(func):
    @functools.wraps(func)
    def wrapper(*args, audience=str(), authorization=str(), account: Account=dict(), **kwargs):
        if not authorization:
            authorization = fetch_gcloud_authorization(audience, account)
        return func(*args, audience=audience, authorization=authorization, account=account, **kwargs)
    return wrapper


@gcloud_authorized
def request_gcloud(audience: str, data: Optional[PostData]=dict(), authorization=str(),
                    file=str(), operation=str()) -> requests.Response:
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

class BigQueryField(Field):
    def __init__(self, name: str, type: BigQueryType, mode: Optional[BigQueryMode]=None,
                desc: Optional[str]=None, maxLength: Optional[int]=None, description: Optional[str]=None):
        self.validate(type, mode)
        TypedDict.__init__(self, name=name, type=type)
        self.update_notna(mode=mode, description=(desc if desc else description), maxLength=maxLength)

    def validate(self, type: BigQueryType, mode: BigQueryMode):
        if type not in from_literal(BigQueryType):
            raise ValueError(INVALID_OBJECT_MSG(type, BIGQUERY_TYPE))
        if mode and (mode not in from_literal(BigQueryMode)):
            raise ValueError(INVALID_OBJECT_MSG(type, BIGQUERY_TYPE))


def validate_gbq_field(field: Any) -> BigQueryField:
    if isinstance(field, BigQueryField): return field
    elif isinstance(field, Dict): return BigQueryField(**field)
    else: raise TypeError(INVALID_OBJECT_TYPE_MSG(field, BIGQUERY_FIELD))


class BigQuerySchema(Schema):
    def __init__(self, *args: BigQueryField):
        TypedRecords.__init__(self, *[validate_gbq_field(field) for field in args])

    def get_primary_key(self, index=0) -> str:
        keys = [field.get("name") for field in self if isinstance(field, Dict) and field.get("mode") == "REQUIRED"]
        return get_scala(keys, index=index)


def validate_gbq_schema(schema: Any, optional=False) -> BigQuerySchema:
    if (not schema) and optional: return
    elif isinstance(schema, BigQuerySchema): return schema
    elif isinstance(schema, List): return BigQuerySchema(*schema)
    else: raise TypeError(INVALID_OBJECT_TYPE_MSG(schema, BIGQUERY_SCHEMA))


###################################################################
######################## Google Cloud Query #######################
###################################################################

NumericiseIgnore = Union[Sequence[int], bool]

class GspreadReadContext(TypedDict):
    def __init__(self, key: str, sheet: str, fields: Optional[IndexLabel]=None, default: Optional[Any]=None,
                if_null: Literal["drop","pass"]="pass", head=1, headers: Optional[IndexLabel]=None,
                str_cols: Optional[NumericiseIgnore]=None, to: Optional[Literal["desc","name"]]="name",
                return_type: Optional[TypeHint]="dataframe", rename: Optional[RenameMap]=None,
                size: Optional[int]=None):
        super().__init__(key=key, sheet=sheet, fields=fields)
        self.update_default(dict(if_null="pass", head=1, to="name", return_type="dataframe"),
            default=default, if_null=if_null, head=head, headers=headers,
            str_cols=str_cols, to=to, return_type=return_type, rename=rename, size=size)


class GoogleQueryContext(GspreadReadContext):
    def __init__(self, key: str, sheet: str, fields: IndexLabel, default: Optional[Any]=None,
                if_null: Literal["drop","pass"]="drop", axis=0, dropna=True, strict=True, unique=False,
                head=1, headers: Optional[IndexLabel]=None, str_cols: Optional[NumericiseIgnore]=None,
                arr_cols: Optional[IndexLabel]=None, to: Optional[Literal["desc","name"]]="name",
                return_type: Optional[TypeHint]="dataframe", rename: Optional[RenameMap]=None,
                size: Optional[int]=None):
        super().__init__(key, sheet, fields, default, if_null, head, headers, str_cols, to, return_type, rename, size)
        self.update_default(dict(axis=0, dropna=True, strict=True, unique=False),
            axis=axis, dropna=dropna, strict=strict, unique=unique, arr_cols=arr_cols)


class GoogleQueryInfo(TypedDict):
    def __init__(self, **context: GoogleQueryContext):
        super().__init__(context)


class GoogleQueryReader(BaseSession):
    __metaclass__ = ABCMeta
    operation = "googleQueryReader"

    def set_query(self, queryInfo: GoogleQueryInfo=dict(), account: Account=dict()):
        for name, queryContext in queryInfo.items():
            if not isinstance(queryInfo, Dict): continue
            elif len(kloc(queryContext, [KEY, SHEET, FIELDS], if_null="drop")) != 3: continue
            elif "if_null" not in queryContext: queryContext["if_null"] = "drop"
            data = self.read_gspread(**queryContext, name=name, account=account)
            self.update(self.get_values_by_axis(to_dataframe(data), **queryContext))

    def read_gspread(self, key: str, sheet: str, fields: IndexLabel=list(), default=None,
                    if_null: Literal["drop","pass"]="pass", head=1, headers=None,
                    str_cols: NumericiseIgnore=list(), to: Optional[Literal["desc","name"]]="name",
                    return_type: Optional[TypeHint]="dataframe", rename: Optional[RenameMap]=None,
                    size: Optional[int]=None, name=str(), account: Account=dict()) -> TabularData:
        context = dict(default=default, if_null=if_null, head=head, headers=headers, numericise_ignore=str_cols,
                        return_type=return_type, rename=(rename if rename else self.get_rename_map(to=to)))
        data = read_gspread(key, sheet, fields=fields, account=account, **context)
        if isinstance(size, int): data = data[:size]
        self.checkpoint(READ(name), where="read_gspread", msg={KEY:key, SHEET:sheet, DATA:data}, save=data)
        self.logger.info(log_table(data, name=name, key=key, sheet=sheet, dump=self.logJson))
        return data

    def get_values_by_axis(self, df: pd.DataFrame, axis=0, dropna=True, strict=True, unique=False,
                            arr_cols: IndexLabel=list(), **context) -> Dict[_KT,Union[List,Any]]:
        if axis not in (-1,0,1): raise ValueError(INVALID_AXIS_MSG(axis))
        elif axis == -1: return to_dict(df, "list", depth=2)
        arr_cols = cast_list(arr_cols)
        data = to_dict((df.T if axis == 1 else df), "list", depth=2)
        for __key, __values in data.copy().items():
            values = to_array(__values, dropna=dropna, strict=strict, unique=unique)
            if (len(values) < 2) and (__key not in arr_cols):
                data[__key] = values[0] if len(values) == 1 else None
            else: data[__key] = values
        return data


###################################################################
###################### Google Cloud Uploader ######################
###################################################################

class GspreadUpdateContext(TypedDict):
    def __init__(self, key: str, sheet: str, mode: Literal["replace","append","upsert"]="append",
                cell: Optional[str]=None, base_sheet: Optional[str]=None, default: Optional[Any]=None,
                head=1, headers: Optional[IndexLabel]=None, str_cols: Optional[NumericiseIgnore]=None,
                to: Optional[Literal["desc","name"]]="desc", rename: Optional[RenameMap]=None):
        super().__init__(key=key, sheet=sheet)
        self.update_default(dict(mode="append", head=1, to="desc"),
            mode=mode, cell=cell, base_sheet=base_sheet, default=default,
            head=head, headers=headers, str_cols=str_cols, to=to, rename=rename)


class BigQueryContext(TypedDict):
    def __init__(self, table: str, project_id: str, mode: Literal["fail","replace","append","upsert"]="append",
                base_query: Optional[str]=None, schema: Optional[BigQuerySchema]=None,
                progress=True, partition: Optional[str]=None,
                partition_by: Optional[Literal["auto","second","minute","hour","day","month","year","date"]]="auto"):
        super().__init__(table=table, project_id=project_id)
        self.update_default(dict(mode="append", progress=True, partition_by="auto"),
            mode=mode, base_query=base_query, schema=validate_gbq_schema(schema, optional=True), progress=progress,
            partition=partition, partition_by=partition_by)


class GoogleUpdateContext(TypedDict):
    def __init__(self, from_key=str(), from_sheet=str(), from_query=str(), from_pid=str(),
                to_key=str(), to_sheet=str(), to_table=str(), to_pid=str(),
                mode: Literal["fail","replace","append","upsert"]="append", default: Optional[Any]=None,
                head=1, headers: Optional[IndexLabel]=None, str_cols: Optional[NumericiseIgnore]=None,
                to: Optional[Literal["desc","name"]]="desc", rename: Optional[RenameMap]=None, cell: Optional[str]=None,
                schema: Optional[BigQuerySchema]=None, progress=True, partition: Optional[str]=None,
                partition_by: Optional[Literal["auto","second","minute","hour","day","month","year","date"]]="auto"):
        query = kloc(locals(), FROM_GS + FROM_GBQ + TO_GS + TO_GBQ, if_null="drop")
        if len(query) != 4: raise ValueError(INVALID_QUERY_MSG)
        super().__init__(**query)
        self.update_default(dict(mode="append", progress=True, head=1, to="desc"),
            mode=mode, default=default, head=head, headers=headers, str_cols=str_cols, to=to, rename=rename,
            cell=cell, schema=validate_gbq_schema(schema, optional=True), progress=progress,
            partition=partition, partition_by=partition_by)


GoogleUploadMode = Literal["fail","replace","append","upsert"]
GoogleUploadContext = Union[GspreadUpdateContext, BigQueryContext, GoogleUpdateContext]

class GoogleUploadInfo(TypedDict):
    def __init__(self, **context: GoogleUploadContext):
        super().__init__(context)


class GoogleUploader(BaseSession):
    __metaclass__ = ABCMeta
    operation = "googleUploader"
    uploadStatus = defaultdict(bool)

    def upload_data(self, data: TabularData, uploadInfo: GoogleUploadInfo=dict(), reauth=False,
                    audience=str(), account: Account=dict(), credentials: Optional[IDTokenCredentials]=None, **context):
        data = to_dataframe(data)
        context = GCLOUD_CONTEXT(account=account, **context)
        gbq_auth = dict(reauth=reauth, audience=audience, credentials=credentials)
        for name, uploadContext in uploadInfo.items():
            if not isinstance(uploadContext, Dict): status = False
            elif (not data.empty) and (len(kloc(uploadContext, [KEY, SHEET], if_null="drop")) == 2):
                status = self.upload_gspread(data=data.copy(), **uploadContext, name=name, **context)
            elif (not data.empty) and (len(kloc(uploadContext, [TABLE, PID], if_null="drop")) == 2):
                status = self.upload_gbq(data=data.copy(), **uploadContext, name=name, **gbq_auth, **context)
            elif len(kloc(uploadContext, FROM_GS + FROM_GBQ + TO_GS + TO_GBQ, if_null="drop")) == 4:
                status = self.update_data(**uploadContext, name=name, **gbq_auth, **context)
            else: status = False
            self.uploadStatus[name] = status

    ###################################################################
    ###################### Google Spread Sheets #######################
    ###################################################################

    @BaseSession.catch_exception
    def upload_gspread(self, key: str, sheet: str, data: pd.DataFrame,
                        mode: Literal["replace","append","upsert"]="append", cell=str(), base_sheet=str(), default=None,
                        head=1, headers=None, str_cols: NumericiseIgnore=list(), to: Optional[Literal["desc","name"]]="desc",
                        rename: Optional[RenameMap]=None, name=str(), account: Account=dict(), **context) -> bool:
        if base_sheet or (mode == "upsert"):
            base_sheet = sheet if mode == "upsert" else base_sheet
            base = self.read_gs_base(key, base_sheet, name, account, default, head, headers, str_cols, rename=rename)
            data = self.map_gs_base(data, base, name=name, **context)
        data = self.map_gs_data(data, name=name, **context)
        self.checkpoint(UPLOAD(name), where="upload_gspread", msg={KEY:key, SHEET:sheet, MODE:mode, DATA:data}, save=data)
        self.logger.info(log_table(data, name=name, key=key, sheet=sheet, mode=mode, dump=self.logJson))
        cell, clear = ("A2" if mode == "replace" else (cell if cell else str())), (True if mode == "replace" else clear)
        update_gspread(key, sheet, data, cell=cell, clear=clear, account=account)
        return True

    def read_gs_base(self, key: str, sheet: str, name=str(), account: Account=dict(), default=None,
                    head=1, headers=None, str_cols: NumericiseIgnore=list(),
                    to: Optional[Literal["desc","name"]]="name", rename: Optional[RenameMap]=None) -> pd.DataFrame:
        data = read_gspread(key, sheet, default=default, head=head, headers=headers, numericise_ignore=str_cols,
                            rename=(rename if rename else self.get_rename_map(to=to)), account=account)
        self.checkpoint(READ(name), where="read_gs_base", msg={KEY:key, SHEET:sheet, DATA:data}, save=data)
        self.logger.info(log_table(data, name=name, key=key, sheet=sheet, dump=self.logJson))
        return data

    def map_gs_base(self, data: pd.DataFrame, base: pd.DataFrame, name=str(), **context) -> pd.DataFrame:
        return cloc(data, base.columns, if_null="pass")

    def map_gs_data(self, data: pd.DataFrame, name=str(), **context) -> pd.DataFrame:
        return data

    ###################################################################
    ######################### Google BigQuery #########################
    ###################################################################

    @BaseSession.catch_exception
    def upload_gbq(self, table: str, project_id: str, data: pd.DataFrame,
                    mode: Literal["fail","replace","append","upsert"]="append",
                    schema: Optional[BigQuerySchema]=None, base_query=str(), progress=True, partition=str(),
                    partition_by: Literal["auto","second","minute","hour","day","date"]="auto",
                    name=str(), reauth=False, audience=str(), account: Account=dict(),
                    credentials: Optional[IDTokenCredentials]=None, **context) -> bool:
        schema = validate_gbq_schema(schema if schema else self.get_gbq_schema(name=name, schema=schema, **context))
        gbq_auth = dict(reauth=reauth, audience=audience, account=account, credentials=credentials)
        if base_query or (mode == "upsert"):
            base_query = table if mode == "upsert" else base_query
            base = self.read_gbq_base(base_query, project_id, name=name, **gbq_auth)
            data = self.map_gbq_base(data, base, table=table, project_id=project_id, schema=schema, name=name, **gbq_auth, **context)
        data = self.map_gbq_data(data, table=table, project_id=project_id, schema=schema, name=name, **gbq_auth, **context)
        self.checkpoint(UPLOAD(name), where="upload_gbq", msg={TABLE:table, PID:project_id, MODE:mode, DATA:data}, save=data)
        self.logger.info(log_table(data, name=name, table=table, pid=project_id, mode=mode, schema=schema, dump=self.logJson))
        to_gbq(table, project_id, data, if_exists=("replace" if mode == "upsert" else mode), schema=schema, progress=progress,
                partition=partition, partition_by=partition_by, **gbq_auth)
        return True

    def get_gbq_schema(self, name=str(), **context) -> BigQuerySchema:
        ...

    def read_gbq_base(self, query: str, project_id: str, name=str(), reauth=False, audience=str(),
                        account: Account=dict(), credentials: Optional[IDTokenCredentials]=None) -> pd.DataFrame:
        data = read_gbq(query, project_id, reauth=reauth, audience=audience, account=account, credentials=credentials)
        self.checkpoint(READ(name), where="read_gbq_base", msg={QUERY:query, PID:project_id}, save=data)
        self.logger.info(log_table(data, name=name, query=query, pid=project_id, dump=self.logJson))
        return data

    def map_gbq_base(self, data: pd.DataFrame, base: pd.DataFrame, schema: BigQuerySchema, **context) -> pd.DataFrame:
        key = validate_primary_key(data, base, schema=schema)
        data = data.set_index(key).combine_first(base.set_index(key)).reset_index()
        return data[data[key].notna()].drop_duplicates(key)

    def map_gbq_data(self, data: pd.DataFrame, schema: Optional[BigQuerySchema]=None, name=str(), **context) -> pd.DataFrame:
        columns = [field["name"] for field in schema if field["name"] in data]
        data = cloc(data, columns, if_null="drop", reorder=True)
        if len(data.columns) != len(columns): raise ValueError(SCHEMA_MISMATCH_MSG)
        else: return data

    ###################################################################
    ########################### Update Data ###########################
    ###################################################################

    @BaseSession.catch_exception
    def update_data(self, from_key=str(), from_sheet=str(), from_query=str(), from_pid=str(),
                    to_key=str(), to_sheet=str(), to_table=str(), to_pid=str(),
                    mode: Literal["fail","replace","append","upsert"]="append", default=None, head=1, headers=None,
                    str_cols: NumericiseIgnore=list(), to: Optional[Literal["desc","name"]]="desc",
                    rename: Optional[RenameMap]=None, cell=str(), schema: Optional[BigQuerySchema]=None, progress=True,
                    partition=str(), partition_by: Literal["auto","second","minute","hour","day","date"]="auto",name=str(),
                    reauth=False, audience=str(), account: Account=dict(), credentials: Optional[IDTokenCredentials]=None,
                    **context) -> bool:
        if from_key and from_sheet:
            data = self.read_gs_base(from_key, from_sheet, name, account, default, head, headers, str_cols, to, rename)
        elif from_query and from_pid:
            data = self.read_gbq_base(from_query, from_pid, name, reauth, audience, account, credentials)
        else: return False
        if to_key and to_sheet:
            return self.upload_gspread(to_key, to_sheet, data, mode, cell, name=name, account=account, **context)
        elif to_table and to_pid:
            return self.upload_gbq(
                to_table, to_pid, data, mode, schema, str(), progress, partition, partition_by,
                name, reauth, audience, account, credentials, **context)
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
    if last_row > 2: gs.delete_rows(3, last_row)


@gs_loaded
def update_gspread(key: str, sheet: str, data: TabularData, col='A', row=0, cell=str(), clear=False,
                    clear_header=False, account: Account=dict(), gs: Optional[Worksheet]=None):
    records = to_records(data)
    if not records: return
    values = [[_to_excel_date(__value) for __value in __m.values()] for __m in records]
    if clear: clear_gspead(gs=gs, include_header=clear_header)
    cell = cell if cell else (col+str(row if row else len(gs.get_all_records())+2))
    gs.update(cell, values)


###################################################################
######################### Google BigQuery #########################
###################################################################

BIGQUERY_TYPE_CAST = lambda __type, fillna=False: {
    "STRING": lambda x: x if isinstance(x, str) else cast_str(x, default=(str() if fillna else None)),
    "BYTES": lambda x: x if isinstance(x, bytes) else None,
    "INTEGER": lambda x: x if isinstance(x, int) else cast_int(x, default=(0 if fillna else None)),
    "FLOAT": lambda x: x if isinstance(x, float) else cast_float(x, default=(0. if fillna else None)),
    "NUMERIC": lambda x: x if isinstance(x, int) else cast_int(x, default=(0 if fillna else None)),
    "BIGNUMERIC": lambda x: x if isinstance(x, int) else cast_int(x, default=(0 if fillna else None)),
    "BOOLEAN": lambda x: x if isinstance(x, bool) else bool(x),
    "TIMESTAMP": lambda x: x if isinstance(x, int) else get_timestamp(x, if_null=(0 if fillna else None), tsUnit="ms"),
    "DATE": lambda x: x if isinstance(x, dt.date) else get_date(x, if_null=(0 if fillna else None)),
    "TIME": lambda x: x if isinstance(x, dt.time) else get_time(x, if_null=(0 if fillna else None)),
    "DATETIME": lambda x: x if isinstance(x, dt.datetime) else get_datetime(x, if_null=(0 if fillna else None)),
}.get(__type, lambda x: x)


def gbq_authorized(func):
    @functools.wraps(func)
    def wrapper(*args, reauth=False, audience=str(), account: Account=dict(),
                credentials: Optional[IDTokenCredentials]=None, **kwargs):
        if reauth and not credentials:
            credentials = fetch_gcloud_credentials(audience, account)
        return func(*args, audience=audience, credentials=credentials, account=account, reauth=reauth, **kwargs)
    return wrapper


def _validate_schema(data: pd.DataFrame, schema: Optional[BigQuerySchema]=None, fillna=False) -> pd.DataFrame:
    if not (schema and is_records(schema)): return data
    context = {field["name"]:BIGQUERY_TYPE_CAST(field["type"], fillna) for field in schema}
    data = apply_df(cloc(data, list(context.keys()), if_null="pass"), **context)
    if df_exists(data, drop_na=True): return data
    else: raise InvalidSchema(SCHEMA_MISMATCH_MSG, local_schema=data.dtypes.to_frame().to_dict()[0], remote_schema=schema)


@gbq_authorized
def read_gbq(query: str, project_id: str, reauth=False, audience=str(), account: Account=dict(),
            credentials: Optional[IDTokenCredentials]=None) -> pd.DataFrame:
    return pd.read_gbq(query, project_id, reauth=reauth, credentials=credentials)


@gbq_authorized
def to_gbq(table: str, project_id: str, data: pd.DataFrame, reauth=False,
            if_exists: Literal["fail","replace","append"]="append", schema: Optional[BigQuerySchema]=None,
            progress=True, validate=True, fillna=False, partition=str(),
            partition_by: Literal["auto","second","minute","hour","day","month","year","date"]="auto",
            audience=str(), account: Account=dict(), credentials: Optional[IDTokenCredentials]=None):
    if validate: data = _validate_schema(data, schema, fillna=fillna)
    context = dict(reauth=reauth, if_exists=if_exists, credentials=credentials)
    if partition:
        return to_gbq_partition(table, project_id, data, schema=schema, progress=progress, validate=False,
                                partition=partition, partition_by=partition_by, **context)
    else: data.to_gbq(table, project_id, table_schema=schema, progress_bar=progress, **context)


@gbq_authorized
def to_gbq_partition(table: str, project_id: str, data: pd.DataFrame, reauth=False,
                    if_exists: Literal["fail","replace","append"]="append", schema: Optional[BigQuerySchema]=None,
                    progress=True, validate=True, fillna=False, partition=str(),
                    partition_by: Literal["auto","second","minute","hour","day","month","year","date"]="auto",
                    audience=str(), account: Account=dict(), credentials: Optional[IDTokenCredentials]=None):
    if validate: data = _validate_schema(data, schema, fillna=fillna)
    context = dict(destination_table=table, project_id=project_id, reauth=reauth, if_exists=if_exists,
                    table_schema=schema, progress_bar=progress, credentials=credentials)
    if partition not in data: data.to_gbq(**context)
    elif partition_by.upper() in DATE_UNIT+["date"]:
        set_partition = (lambda x: get_datetime(x, datetimePart=partition_by)) if partition_by in DATE_UNIT else get_date
        data["_PARTITIONTIME"] = data[partition].apply(set_partition)
        for date in tqdm(sorted(data["_PARTITIONTIME"].unique()), desc=BIGQUERY_PARTITION_MSG):
            data[data["_PARTITIONTIME"]==date].drop(columns="_PARTITIONTIME").to_gbq(**context)
    else:
        for part in tqdm(sorted(data[partition].unique()), desc=BIGQUERY_PARTITION_MSG):
            data[data[partition]==part].to_gbq(**context)


def validate_primary_key(data: pd.DataFrame, base: pd.DataFrame, key=str(),
                        schema: Optional[BigQuerySchema]=None, index=0) -> str:
    if not key:
        key = validate_gbq_schema(schema).get_primary_key(index=index)
    if key and (key in data) and (key in base): return key
    else: raise ValueError(INVALID_PRIMARY_KEY_MSG)


@gbq_authorized
def upsert_gbq(table: str, project_id: str, data: pd.DataFrame, base: Optional[pd.DataFrame]=None, key=str(),
                reauth=False, if_exists: Literal["fail","replace","append"]="replace",
                schema: Optional[BigQuerySchema]=None, progress=True, validate=True, fillna=False,
                audience=str(), account: Account=dict(), credentials: Optional[IDTokenCredentials]=None):
    if validate: data = _validate_schema(data, schema, fillna=fillna)
    context = dict(reauth=reauth, credentials=credentials)
    if df_empty(base): base = read_gbq(table, project_id, **context)
    key = validate_primary_key(data, base, key, schema)
    data = data.set_index(key).combine_first(base.set_index(key)).reset_index()
    data.to_gbq(table, project_id, if_exists=if_exists, table_schema=schema, progress_bar=progress, **context)
