from typing import List, Tuple
import pandas as pd
import re

OBJECT_SEQUENCE = (List, Tuple)


def isna(__object) -> bool:
    _isna = pd.isna(__object)
    return _isna if isinstance(_isna, bool) else False

def isna_plus(__object) -> bool:
    if isinstance(__object, OBJECT_SEQUENCE):
        return (not __object) or all(pd.isna(__object))
    elif isinstance(__object, pd.Series):
        return __object.empty or __object.isna().all()
    elif isinstance(__object, pd.DataFrame):
        return __object.empty or __object.isna().all().all()
    else: return isna(__object)


def notna(__object) -> bool:
    _notna = pd.notna(__object)
    return _notna if isinstance(_notna, bool) else True

def notna_plus(__object) -> bool:
    if isinstance(__object, OBJECT_SEQUENCE):
        return __object and any(pd.notna(__object))
    elif isinstance(__object, pd.Series):
        return (not __object.empty) and __object.notna().any()
    elif isinstance(__object, pd.DataFrame):
        return (not __object.empty) and __object.notna().any().any()
    else: return isna(__object)


def is_empty(__object) -> bool:
    if isinstance(__object, float): # np.NaN
        return pd.isna(__object)
    try: return (not __object)
    except: return isna(__object)

def is_empty_plus(__object) -> bool:
    if isinstance(__object, OBJECT_SEQUENCE):
        return all([is_empty(__e) for __e in __object])
    elif isinstance(__object, pd.Series):
        return __object.empty or __object.apply(is_empty).all()
    elif isinstance(__object, pd.DataFrame):
        return __object.empty or __object.apply({__column: is_empty for __column in __object.columns}).all().all()
    else: return is_empty(__object)


def exists(__object) -> bool:
    if isinstance(__object, float): # np.NaN
        return pd.notna(__object) and bool(__object)
    try: return bool(__object)
    except: return notna(__object)

def exists_plus(__object) -> bool:
    if isinstance(__object, OBJECT_SEQUENCE):
        return any([exists(__e) for __e in __object])
    elif isinstance(__object, pd.Series):
        return (not __object.empty) and __object.apply(exists).any()
    elif isinstance(__object, pd.DataFrame):
        return (not __object.empty) and __object.apply({__column: exists for __column in __object.columns}).any().any()
    else: return exists(__object)


def to_snake_case(__s: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", '_', str(__s)).lower()

def to_camel_case(__s: str) -> str:
    return ''.join([s.capitalize() if __i > 0 else s for __i, s in enumerate(str(__s).split('_'))])
