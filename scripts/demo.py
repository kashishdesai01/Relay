#!/usr/bin/env python3
"""Start/stop the containerized demo and provision private session credentials."""
import json
import os
import sys
from common import ARTIFACTS, compose
from credentials import load_or_create

command=sys.argv[1] if len(sys.argv)>1 else 'up'
if command=='up':
    compose('up','-d','--build',observability=True,apps=True,capture=False)
    secrets_file=ARTIFACTS/'local-secrets.json'
    credentials=load_or_create(secrets_file)
    for name,scopes in [('demo','events.write,events.read'),('operator','relay.admin')]:
        result=compose('run','--rm','-e','RELAY_PROVISION_KEY','migrator','provision',name,scopes,observability=True,apps=True,
                       extra_env={'RELAY_PROVISION_KEY':credentials[name]})
        if result.stdout.strip()!=credentials[name]:
            raise RuntimeError('Provisioning did not confirm the saved key; inspect the migrator invocation.')
    print('Relay console: http://127.0.0.1:5080')
    print('Producer/admin keys: artifacts/local-secrets.json (private; paste into the console).')
elif command=='down':
    compose('down',observability=True,apps=True,capture=False)
else:
    raise SystemExit('Usage: python3 scripts/demo.py [up|down]')
