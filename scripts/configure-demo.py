#!/usr/bin/env python3
"""Generate local-only container passwords. Never prints secrets."""
import os
import secrets
from pathlib import Path

path = Path(__file__).resolve().parents[1] / '.env'
if path.exists():
    raise SystemExit('.env already exists; preserve existing credentials')
flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
with os.fdopen(os.open(path, flags, 0o600), 'w') as file:
    file.write('POSTGRES_PASSWORD=' + secrets.token_hex(24) + '\n')
    file.write('MSSQL_SA_PASSWORD=Relay!' + secrets.token_hex(24) + '\n')
    file.write('ACCEPT_EULA=Y\n')
print('Created private .env with generated development passwords.')
