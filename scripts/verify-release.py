#!/usr/bin/env python3
"""Verify production assemblies cannot activate test-only controls."""
from common import ROOT
assembly=ROOT/'src/Relay.Infrastructure/bin/Release/net10.0/Relay.Infrastructure.dll'
content=assembly.read_bytes()
for marker in ['RELAY_FAILPOINTS','RELAY_PAUSE_MARKER','consumer.skip_dedupe','consumer.pause','injected_handler_failure']:
    if marker.encode('utf-16le') in content or marker.encode() in content:
        raise SystemExit('Test control leaked into Release assembly: '+marker)
print('Release artifact: test-only controls absent.')
