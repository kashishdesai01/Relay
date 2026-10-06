#!/usr/bin/env python3
"""Check causal parent spans in actual collector output, not only equal trace IDs."""
import json
import sys
from pathlib import Path
from common import ARTIFACTS

trace_id=sys.argv[1] if len(sys.argv)>1 else '1234567890abcdef1234567890abcdef'
spans=[]
for line in (ARTIFACTS/'traces.jsonl').read_text().splitlines():
    for resource in json.loads(line).get('resourceSpans',[]):
        for scope in resource.get('scopeSpans',[]):
            for span in scope.get('spans',[]):
                if span.get('traceId')==trace_id:spans.append(span)
by_name={}
for span in spans:by_name.setdefault(span['name'],[]).append(span)
chains=[]
for ingest in by_name.get('ingest.commit',[]):
    for dispatch in by_name.get('dispatch.enqueue',[]):
        if dispatch.get('parentSpanId')!=ingest['spanId']:continue
        for consumer in by_name.get('consumer.apply',[]):
            if consumer.get('parentSpanId')==dispatch['spanId']:
                chains.append({'ingest':ingest['spanId'],'dispatch':dispatch['spanId'],'consumer':consumer['spanId']})
if not chains:raise SystemExit('No exported API-dispatch-consumer parent chain found')
report={'traceId':trace_id,'parentChains':chains,'verifiedAgainst':'actual OTLP collector file export'}
(ARTIFACTS/'trace-results.json').write_text(json.dumps(report,indent=2)+'\n')
print(f'PASS trace continuity: {len(chains)} exported causal chain(s)')
