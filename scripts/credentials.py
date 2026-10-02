"""Atomic, private local demo credentials; never changes an existing valid identity silently."""
import json
import os
import secrets
import tempfile
from pathlib import Path

def read(path):
    try:
        value=json.loads(Path(path).read_text())
        if not isinstance(value,dict) or set(value)!={'demo','operator'} or any(not isinstance(k,str) or len(k)!=64 or any(c not in '0123456789abcdef' for c in k) for k in value.values()):
            raise ValueError('Invalid key set')
        return value
    except (ValueError,TypeError) as error:
        raise RuntimeError('Saved demo credentials are invalid. Restore the private credentials file from a backup; existing database identities are preserved and will not be silently rotated.') from error

def load_or_create(path):
    path=Path(path)
    if path.exists():return read(path)
    keys={name:secrets.token_hex(32) for name in ['demo','operator']}
    descriptor,temporary=tempfile.mkstemp(prefix='local-secrets-',suffix='.tmp',dir=path.parent)
    try:
        with os.fdopen(descriptor,'w') as file:
            json.dump(keys,file)
            file.flush()
            os.fsync(file.fileno())
        try:
            os.link(temporary,path)  # Atomic publication; never replaces a competing complete file.
        except FileExistsError:
            pass
        directory=os.open(path.parent,os.O_RDONLY)
        try:os.fsync(directory)
        finally:os.close(directory)
    finally:
        Path(temporary).unlink(missing_ok=True)
    return read(path)
