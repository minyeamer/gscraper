from typing import Iterable
from pandas import isna as is_na
from pandas import notna as not_na
import re


to_snake_case = lambda __s=str(): re.sub(r"(?<!^)(?=[A-Z])", '_', str(__s)).lower()
to_camel_case = lambda __s=str(): ''.join([s.capitalize() if __i > 0 else s for __i, s in enumerate(str(__s).split('_'))])


def isna(__object, strict=True) -> bool:
    if not strict:
        try: return not __object
        except: return is_na(__object)
    _isna = is_na(__object)
    return False if isinstance(_isna, Iterable) else _isna


def notna(__object, strict=True) -> bool:
    if not strict:
        try: return bool(__object)
        except: return not_na(__object)
    _notna = not_na(__object)
    return True if isinstance(_notna, Iterable) else _notna


def empty(__object, strict=False) -> bool:
    return isna(__object, strict=strict)


def exists(__object, strict=False) -> bool:
    return notna(__object, strict=strict)
