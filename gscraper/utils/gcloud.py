from gscraper.base.types import TypeHint, IndexLabel, RenameMap, TabularData, is_records
from gscraper.base.types import Account, PostData, Datetime, NumericiseIgnore, BigQuerySchema

from gscraper.utils.cast import cast_str, cast_float, cast_int, cast_datetime_format
from gscraper.utils.date import get_datetime, get_timestamp, get_time, get_date, DATE_UNIT
from gscraper.utils.map import df_exists, df_empty, iloc, drop_dict, cloc, apply_df, apply_data

from google.oauth2 import service_account
from google.oauth2.service_account import IDTokenCredentials
from google.auth.transport.requests import AuthorizedSession

from gspread.worksheet import Worksheet
import gspread

from pandas_gbq.gbq import InvalidSchema
import pandas_gbq

from typing import Dict, Literal, Optional
from tqdm.auto import tqdm
import datetime as dt
import functools
import json
import os
import pandas as pd
import requests


ENV_PATH = "env/"
GCLOUD_ACCOUNT = ENV_PATH+"gcloud.json"
GCLOUD_DATA = ENV_PATH+"data.json"

INVALID_GS_ACTION_MSG = lambda action: f"'{action}' is not valid action for gspread task."
INVALID_SCHEMA_MSG = "Please verify that the structure and data types in the DataFrame match the schema of the destination table."
INVALID_UPSERT_KEY_MSG = "Please verify that a primary key exists and is in both DataFrame objects."
BIGQUERY_PARTITION_MSG = "Uploading partitioned data to Google BigQuery"


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
###################### Google Spread Sheets #######################
###################################################################

def load_gspread(key: str, sheet: str, account: Account=dict()) -> Worksheet:
    account = account if account and isinstance(account, dict) else read_gcloud(account)
    gs_acc = gspread.service_account_from_dict(account)
    gs = gs_acc.open_by_key(key)
    return gs.worksheet(sheet)


def gs_loaded(func):
    @functools.wraps(func)
    def wrapper(key=str(), sheet=str(), account: Account=dict(), *args, gs: Optional[Worksheet]=None, **kwargs):
        if not gs:
            gs = load_gspread(key, sheet, account)
        return func(*args, key=key, sheet=sheet, account=account, gs=gs, **kwargs)
    return wrapper


def to_excel_date(date: Datetime, default) -> int:
        if not isinstance(date, dt.date): return default
        offset = 693594
        days = date.toordinal() - offset
        if isinstance(date, dt.datetime):
            seconds = (date.hour*60*60 + date.minute*60 + date.second)/(24*60*60)
            return days + seconds
        return days


def validate_gs_format(data: TabularData, action: Literal["read","update"]="read",
                        fields: Optional[IndexLabel]=list(), default=None, if_null: Literal["drop","pass"]="drop",
                        reorder=True, return_type: Optional[TypeHint]=None, rename: RenameMap=dict(),
                        convert_first=False, rename_first=False, filter_first=False) -> TabularData:
    if action == "read":
        cast_boolean = lambda x: {"TRUE":True, "FALSE":False}.get(x, x)
        applyFunc = lambda x: cast_datetime_format(x, default=cast_boolean(x))
    elif action == "update": applyFunc = lambda x: to_excel_date(x, default=x)
    else: raise ValueError(INVALID_GS_ACTION_MSG(action))
    return apply_data(apply=applyFunc, all_keys=True, **drop_dict(locals(), "action", inplace=False))


@gs_loaded
def read_gspread(key: str, sheet: str, account: Account=dict(), gs: Optional[Worksheet]=None,
                fields: Optional[IndexLabel]=list(), default=None, if_null: Literal["drop","pass"]="drop",
                head=1, headers=None, numericise_ignore: NumericiseIgnore=list(), reorder=True,
                return_type: Optional[TypeHint]="dataframe", rename: RenameMap=dict(),
                convert_first=True, rename_first=True, filter_first=True, **kwargs) -> TabularData:
    if isinstance(numericise_ignore, bool): numericise_ignore = ["all"] if numericise_ignore else list()
    data = gs.get_all_records(head=head, default_blank=default, numericise_ignore=numericise_ignore, expected_headers=headers)
    return validate_gs_format(data, action="read", fields=fields, default=default, if_null=if_null, reorder=reorder,
                                return_type=return_type, rename=rename,
                                convert_first=convert_first, rename_first=rename_first, filter_first=filter_first)


@gs_loaded
def clear_gspead(key: str, sheet: str, account: Account=dict(), gs: Optional[Worksheet]=None, include_header=False, **kwargs):
    if include_header: return gs.clear()
    last_row = len(gs.get_all_records())+1
    if last_row > 2: gs.delete_rows(3, last_row)


@gs_loaded
def update_gspread(key: str, sheet: str, data: TabularData, account: Account=dict(),
                    gs: Optional[Worksheet]=None, col='A', row=0, cell=str(), clear=False, clear_header=False, **kwargs):
    records = validate_gs_format(data, action="update", return_type="records")
    if not records: return
    values = [[value if pd.notna(value) else None for value in record.values()] for record in records]
    if clear: clear_gspead(gs=gs, include_header=clear_header)
    cell = cell if cell else (col+str(row if row else len(gs.get_all_records())+2))
    gs.update(cell, values)


###################################################################
######################### Google Bigquery #########################
###################################################################

BIGQUERY_TYPE_CAST = lambda __type, fillna=False: {
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
            credentials = fetch_gcloud_credentials(audience, account)
        return func(*args, audience=audience, credentials=credentials, account=account, reauth=reauth, **kwargs)
    return wrapper


def validate_schema(data: pd.DataFrame, schema: Optional[BigQuerySchema]=None, fillna=False) -> pd.DataFrame:
    if not (schema and is_records(schema)): return data
    context = {field["name"]:BIGQUERY_TYPE_CAST(field["type"], fillna) for field in schema}
    data = apply_df(cloc(data, list(context.keys()), if_null="pass"), **context)
    if df_exists(data, allow_na=False): return data
    else: raise InvalidSchema(INVALID_SCHEMA_MSG, local_schema=data.dtypes.to_frame().to_dict()[0], remote_schema=schema)


@gbq_authorized
def read_gbq(query: str, project_id: str, reauth=False, credentials: Optional[IDTokenCredentials]=None, **kwargs) -> pd.DataFrame:
    return pd.read_gbq(query, project_id, reauth=reauth, credentials=credentials)


@gbq_authorized
def to_gbq(table: str, project_id: str, data: pd.DataFrame, reauth=False,
            if_exists: Literal["fail","replace","append"]="append", schema: Optional[BigQuerySchema]=None,
            progress=True, validate=True, fillna=False, partition=str(),
            partition_by: Literal["auto","second","minute","hour","day","month","year","date"]="auto",
            credentials: Optional[IDTokenCredentials]=None, **kwargs):
    if validate: data = validate_schema(data, schema, fillna=fillna)
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
                    credentials: Optional[IDTokenCredentials]=None, **kwargs):
    if validate: data = validate_schema(data, schema, fillna=fillna)
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


def validate_upsert_key(data: pd.DataFrame, base: pd.DataFrame, key=str(),
                        schema: Optional[BigQuerySchema]=None, index=0) -> str:
    if not (key and isinstance(key, str)) and (schema and is_records(schema)):
        keys = [field.get("name") for field in schema if field.get("mode") == "REQUIRED"]
        key = iloc(keys, index, default=str())
    if key and key in data and key in base: return key
    else: raise ValueError(INVALID_UPSERT_KEY_MSG)


@gbq_authorized
def upsert_gbq(table: str, project_id: str, data: pd.DataFrame, base: Optional[pd.DataFrame]=None, key=str(),
                reauth=False, if_exists: Literal["fail","replace","append"]="replace",
                schema: Optional[BigQuerySchema]=None, progress=True, validate=True, fillna=False,
                credentials: Optional[IDTokenCredentials]=None, **kwargs):
    if validate: data = validate_schema(data, schema, fillna=fillna)
    context = dict(reauth=reauth, credentials=credentials)
    if df_empty(base): base = read_gbq(table, project_id, **context)
    key = validate_upsert_key(data, base, key, schema)
    data = data.set_index(key).combine_first(base.set_index(key)).reset_index()
    data.to_gbq(table, project_id, if_exists=if_exists, table_schema=schema, progress_bar=progress, **context)
