from .map import astype_str

from gspread.worksheet import Worksheet
import gspread

from typing import Dict, List, Optional, Union
import datetime as dt
import pandas as pd


def to_excel_date(date: Union[dt.date,dt.datetime]):
        offset = 693594
        days = date.toordinal() - offset
        if isinstance(date, dt.datetime):
            seconds = (date.hour*60*60 + date.minute*60 + date.second)/(24*60*60)
            return days + seconds
        return days


###################################################################
###################### Google Spread Sheets #######################
###################################################################

def load_gspread(key: str, sheet: str, account: Optional[Dict]=dict(), **kwargs) -> Worksheet:
    gs_acc = gspread.service_account_from_dict(account)
    gs = gs_acc.open_by_key(key)
    return gs.worksheet(sheet)


def read_gspread(key=str(), sheet=str(), account: Optional[Dict]=dict(), gs: Optional[Worksheet]=None,
                head=1, headers=None, numericise=True, str_cols: Optional[List[str]]=list(),
                rename: Optional[Dict[str,str]]=dict(), **kwargs) -> pd.DataFrame:
    gs = gs if gs else load_gspread(key, sheet, account)
    params = dict(head=head, expected_headers=headers, numericise_ignore=(list() if numericise else ["all"]))
    df = pd.DataFrame(gs.get_all_records(**params)).rename(columns=rename)
    return astype_str(df) if str_cols else df


def update_gspread(data: Union[pd.DataFrame,List[Dict]], key=str(), sheet=str(),
                    account: Optional[Dict]=dict(), gs: Optional[Worksheet]=None,
                    col='A', row=0, cell=str(), **kwargs):
    gs = gs if gs else load_gspread(key, sheet, account)
    records = data.to_dict("records") if isinstance(data, pd.DataFrame) else data
    if not records: return
    values = [[value if pd.notna(value) else None for value in record.values()] for record in records]
    cell = cell if cell else (col+str(row if row else len(gs.get_all_records())+2))
    gs.update(cell, values)


def clear_gspead(key: str, sheet: str, account=dict(), header=False, **kwargs):
    gs = load_gspread(key, sheet, account)
    if header: return gs.clear()
    last_row = len(gs.get_all_records())+1
    if last_row > 2: gs.delete_rows(3, last_row)
