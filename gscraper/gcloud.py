from .cast import cast_str, cast_float, cast_int, to_datetime
from .date import is_datetime_format, get_datetime, get_timestamp, get_time, get_date, DATEPART
from .map import iloc, cloc, apply_df, apply_data, df_exists, df_empty
from .types import RenameDict, TabularData, TypeHint, is_records

from google.oauth2 import service_account
from google.oauth2.service_account import IDTokenCredentials
from google.auth.transport.requests import AuthorizedSession

from gspread.worksheet import Worksheet
import gspread

from pandas_gbq.gbq import InvalidSchema
import pandas_gbq

from typing import Any, Dict, List, Optional, Sequence, Union
from tqdm.auto import tqdm
import datetime as dt
import functools
import json
import os
import pandas as pd
import requests

Account = Union[Dict[str,str], str]
PostData = Union[Dict[str,Any],str]
Datetime = Union[dt.datetime, dt.date]

NumericiseIgnore = Union[bool, Sequence[int]]

BigQuerySchema = List[Dict[str,str]]
SchemaSequence = Union[BigQuerySchema, Sequence[BigQuerySchema]]

ENV_PATH = "env/"
GCLOUD_ACCOUNT = ENV_PATH+"gcloud.json"
GCLOUD_DATA = ENV_PATH+"data.json"


INVALID_SCHEMA_MSG = "Please verify that the structure and data types in the DataFrame match the schema of the destination table."
INVALID_UPSERT_KEY_MSG = "Please verify that the key exists and is in both DataFrame objects."
BIGQUERY_PARTITION_MSG = "Uploading partitioned data to Google BigQuery."


###################################################################
###################### Google Authorization #######################
###################################################################

def read_json(file: str) -> Dict:
    if os.path.exists(file):
        with open(file, "r", encoding="utf-8") as f:
            return json.loads(f.read())
    else: return dict()


def read_gcloud(file=str(), **kwargs) -> Account:
    return read_json(str(file) if str(file).endswith(".json") else GCLOUD_ACCOUNT)


def read_data(file=str(), **kwargs) -> PostData:
    return read_json(str(file) if str(file).endswith(".json") else GCLOUD_DATA)


def fetch_gcloud_credentials(audience=str(), account: Account=dict(), **kwargs) -> IDTokenCredentials:
    account = account if account and isinstance(account, dict) else read_gcloud(account)
    audience = audience if audience else account.get("audience", str())
    return service_account.IDTokenCredentials.from_service_account_info(account, target_audience=audience)


def fetch_gcloud_authorization(audience=str(), account: Account=dict(), **kwargs) -> str:
    credentials = fetch_gcloud_credentials(audience, account)
    auth_session = AuthorizedSession(credentials)
    auth_session.get(audience)
    return "Bearer "+credentials.token


def gcloud_authorized(func):
    @functools.wraps(func)
    def wrapper(*args, audience=str(), authorization=str(), account: Account=dict(), **kwargs):
        if not authorization:
            authorization = fetch_gcloud_authorization(audience, account, **kwargs)
        return func(*args, audience=audience, authorization=authorization, account=account, **kwargs)
    return wrapper


@gcloud_authorized
def request_gcloud(audience: str, authorization: str, data: Optional[PostData]=dict(),
                    operation=str(), **kwargs) -> requests.Response:
    data = data if data and isinstance(data, dict) else read_data(str(data)).get(operation, dict())
    return requests.post(audience, json=data, headers={"Authorization":authorization})


###################################################################
###################### Google Spread Sheets #######################
###################################################################

def to_excel_date(date: Datetime) -> int:
        offset = 693594
        days = date.toordinal() - offset
        if isinstance(date, dt.datetime):
            seconds = (date.hour*60*60 + date.minute*60 + date.second)/(24*60*60)
            return days + seconds
        return days


def validate_excel_format(data: TabularData, action="download", returnType: Optional[TypeHint]=None, **kwargs) -> TabularData:
    if action == "download":
        cast_boolean = lambda x: {"TRUE":True, "FALSE":False}.get(x, x)
        applyFunc = lambda x: to_datetime(x, __type="auto") if is_datetime_format(x) else cast_boolean(x)
    elif action == "upload":
        applyFunc = lambda x: to_excel_date(x) if isinstance(dt.date) else x
    return apply_data(data, __applyFunc=applyFunc, all_keys=True, returnType=returnType)


def load_gspread(key: str, sheet: str, account: Account=dict(), **kwargs) -> Worksheet:
    account = account if account and isinstance(account, dict) else read_gcloud(account)
    gs_acc = gspread.service_account_from_dict(account)
    gs = gs_acc.open_by_key(key)
    return gs.worksheet(sheet)


def read_gspread(key: str, sheet: str, account: Account=dict(), gs: Optional[Worksheet]=None,
                head=1, headers=None, if_null=str(), numericise_ignore: NumericiseIgnore=list(),
                rename: RenameDict=dict(), **kwargs) -> pd.DataFrame:
    gs = gs if gs else load_gspread(key, sheet, account)
    if isinstance(numericise_ignore, bool): numericise_ignore = ["all"] if numericise_ignore else list()
    params = dict(head=head, default_blank=if_null, expected_headers=headers, numericise_ignore=numericise_ignore)
    data = pd.DataFrame(gs.get_all_records(**params)).rename(columns=rename)
    return validate_excel_format(data, action="download", returnType="dataframe")


def clear_gspead(key: str, sheet: str, account: Account=dict(),
                gs: Optional[Worksheet]=None, include_header=False, **kwargs):
    gs = gs if gs else load_gspread(key, sheet, account)
    if include_header: return gs.clear()
    last_row = len(gs.get_all_records())+1
    if last_row > 2: gs.delete_rows(3, last_row)


def update_gspread(key: str, sheet: str, data: TabularData, account: Account=dict(),
                    gs: Optional[Worksheet]=None, col='A', row=0, cell=str(), clear=False, clear_header=False, **kwargs):
    gs = gs if gs else load_gspread(key, sheet, account)
    records = validate_excel_format(data, action="upload", returnType="records")
    if not records: return
    values = [[value if pd.notna(value) else None for value in record.values()] for record in records]
    if clear: clear_gspead(gs=gs, include_header=clear_header)
    cell = cell if cell else (col+str(row if row else len(gs.get_all_records())+2))
    gs.update(cell, values)


###################################################################
######################### Google Bigquery #########################
###################################################################

BIGQUERY_TYPES = lambda __type, fillna=False: {
    "STRING": lambda x: x if isinstance(x, str) else cast_str(x, default=(str() if fillna else None), match=pd.notna),
    "BYTES": lambda x: x if isinstance(x, bytes) else None,
    "INTEGER": lambda x: x if isinstance(x, int) else cast_int(x, default=(0 if fillna else None)),
    "FLOAT": lambda x: x if isinstance(x, float) else cast_float(x, default=(0. if fillna else None)),
    "NUMERIC": lambda x: x if isinstance(x, int) else cast_int(x, default=(0 if fillna else None)),
    "BIGNUMERIC": lambda x: x if isinstance(x, int) else cast_int(x, default=(0 if fillna else None)),
    "BOOLEAN": lambda x: x if isinstance(x, bool) else bool(x),
    "TIMESTAMP": lambda x: x if isinstance(x, int) else get_timestamp(x, default=(0 if fillna else None), tsUnit="ms"),
    "DATE": lambda x: x if isinstance(x, dt.date) else get_date(x, default=(0 if fillna else None)),
    "TIME": lambda x: x if isinstance(x, dt.time) else get_time(x, default=(0 if fillna else None)),
    "DATETIME": lambda x: x if isinstance(x, dt.datetime) else get_datetime(x, default=(0 if fillna else None)),
}.get(__type, lambda x: x)


def gbq_authorized(func):
    @functools.wraps(func)
    def wrapper(*args, audience=str(), credentials: Optional[IDTokenCredentials]=None, account=dict(), reauth=False, **kwargs):
        if reauth and not credentials:
            credentials = fetch_gcloud_credentials(audience, account, **kwargs)
        return func(*args, audience=audience, credentials=credentials, account=account, reauth=reauth, **kwargs)
    return wrapper


def validate_schema(data: pd.DataFrame, schema: Optional[BigQuerySchema]=None, fillna=False, **kwargs) -> pd.DataFrame:
    if not (schema and is_records(schema)): return data
    context = {field["name"]:BIGQUERY_TYPES(field["type"], fillna) for field in schema}
    data = apply_df(cloc(data, list(context.keys()), default="pass"), **context)
    if df_exists(data, null=False): return data
    else: raise InvalidSchema(INVALID_SCHEMA_MSG, local_schema=data.dtypes.to_frame().to_dict()[0], remote_schema=schema)


@gbq_authorized
def read_gbq(query: str, project_id: str, reauth=False, credentials: Optional[IDTokenCredentials]=None, **kwargs) -> pd.DataFrame:
    return pd.read_gbq(query, project_id, reauth=reauth, credentials=credentials)


@gbq_authorized
def to_gbq(table: str, project_id: str, data: pd.DataFrame, reauth=False, if_exists="append",
            schema: Optional[BigQuerySchema]=None, progress=True, validate=True, fillna=False,
            partition=str(), partition_by="auto", credentials: Optional[IDTokenCredentials]=None, **kwargs):
    if validate: data = validate_schema(data, schema, fillna=fillna)
    context = dict(reauth=reauth, if_exists=if_exists, credentials=credentials)
    if partition:
        return to_gbq_partition(table, project_id, data, schema=schema, progress=progress, validate=False,
                                partition=partition, partition_by=partition_by, **context)
    else: data.to_gbq(table, project_id, table_schema=schema, progress_bar=progress, **context)


@gbq_authorized
def to_gbq_partition(table: str, project_id: str, data: pd.DataFrame, reauth=False, if_exists="append",
                    schema: Optional[BigQuerySchema]=None, progress=True, validate=True, fillna=False,
                    partition=str(), partition_by="auto", credentials: Optional[IDTokenCredentials]=None, **kwargs):
    if validate: data = validate_schema(data, schema, fillna=fillna)
    context = dict(destination_table=table, project_id=project_id, reauth=reauth, if_exists=if_exists,
                    table_schema=schema, progress_bar=progress, credentials=credentials)
    if partition not in data: data.to_gbq(**context)
    elif partition_by in DATEPART+["DATE"]:
        set_partition = (lambda x: get_datetime(x, datetimePart=partition_by)) if partition_by in DATEPART else get_date
        data["_PARTITIONTIME"] = data[partition].apply(set_partition)
        for date in tqdm(sorted(data["_PARTITIONTIME"].unique()), desc=BIGQUERY_PARTITION_MSG):
            data[data["_PARTITIONTIME"]==date].drop(columns="_PARTITIONTIME").to_gbq(**context)
    else:
        for part in tqdm(sorted(data[partition].unique()), desc=BIGQUERY_PARTITION_MSG):
            data[data[partition]==part].to_gbq(**context)


def validate_upsert_key(data: pd.DataFrame, base: pd.DataFrame, key=str(),
                        schema: Optional[BigQuerySchema]=None, index=0, **kwargs) -> str:
    if not (key and isinstance(key, str)) and (schema and is_records(schema)):
        keys = [field.get("name") for field in schema if field.get("mode") == "REQUIRED"]
        key = iloc(keys, index, default=str())
    if key and key in data and key in base: return key
    else: raise ValueError(INVALID_UPSERT_KEY_MSG)


@gbq_authorized
def upsert_gbq(table: str, project_id: str, data: pd.DataFrame, base: Optional[pd.DataFrame]=None, key=str(),
                reauth=False, if_exists="replace", schema: Optional[BigQuerySchema]=None, progress=True,
                validate=True, fillna=False, credentials: Optional[IDTokenCredentials]=None, **kwargs):
    if validate: data = validate_schema(data, schema, fillna=fillna)
    context = dict(reauth=reauth, credentials=credentials)
    if df_empty(base): base = read_gbq(table, project_id, **context)
    key = validate_upsert_key(data, base, key, schema)
    data = data.set_index(key).combine_first(base.set_index(key)).reset_index()
    data.to_gbq(table, project_id, if_exists=if_exists, table_schema=schema, progress_bar=progress, **context)
