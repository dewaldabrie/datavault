import datetime
import hashlib
from typing import Dict

HASH_LENGTH = 32
HASH_KEY_FUNCTION = 'md5'
HASH_DIFF_FUNCTION = 'md5'


def md5_calc(input: str | Dict) -> str:
    return hashlib.md5(input.encode('utf-8')).hexdigest()
    

if HASH_KEY_FUNCTION == 'md5':
    hash_key_calc = md5_calc
else:
    raise NotImplementedError(f'Hash function {HASH_KEY_FUNCTION} has not been implemented in code.')


if HASH_DIFF_FUNCTION == 'md5':
    hash_diff_calc = md5_calc
else:
    raise NotImplementedError(f'Hash function {HASH_KEY_FUNCTION} has not been implemented in code.')


def stringify(payload: str | Dict) -> str:
    if isinstance(payload, dict):
        s = '|'.join(
            map(
                lambda x: str(x[1] or ''),
                sorted(
                    payload.items(), 
                    key=lambda o: o[0]
                )
            )
        )

    else:
        s = payload
    return s

def calculate_hash_key(payload: str | Dict, create_ts: datetime.datetime = None) -> str:
    hash_input = stringify(payload)
    if create_ts:
        hash_input += str(create_ts)
    return hash_key_calc(hash_input)


def calculate_hash_diff(payload: str | Dict) -> str:
    hash_input = stringify(payload)
    return hash_diff_calc(hash_input)