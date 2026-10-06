#!/usr/bin/env python3
"""Verify the published Release containers, fresh traces, metrics, and all event contracts."""
import json
import subprocess
import time
import uuid
from urllib.parse import quote
from common import ARTIFACTS, ROOT, compose, docker_environment, event, request, sql, wait_for
from credentials import read
from evidence import prometheus_target_up, fresh_sdk_sources

keys=read(ARTIFACTS/'local-secrets.json')
api='http://127.0.0.1:5080'
worker='http://127.0.0.1:5081'
checks=[]
started_ns=time.time_ns()
def check(name,condition):
    assert condition,name
    checks.append(name)
    print('PASS '+name,flush=True)

wait_for(lambda:request(api,'/health/ready')[0]==200,'container API readiness')
wait_for(lambda:request(worker,'/health/ready')[0]==200,'container worker readiness')
check('api_and_worker_ready',True)
page=request(api,'/')
check('console_static_content_and_csp',page[0]==200 and 'Relay' in page[1] and 'Content-Security-Policy' in page[2])
check('missing_key_401',request(api,'/v1/events',body=event('unauth'))[0]==401)
check('producer_cannot_admin_403',request(api,'/v1/admin/incidents',keys['demo'])[0]==403)
check('operator_cannot_ingest_403',request(api,'/v1/events',keys['operator'],event('wrong-scope'))[0]==403)

suffix=uuid.uuid4().hex[:12]
trace_id=uuid.uuid4().hex
trace_parent=f'00-{trace_id}-{uuid.uuid4().hex[:16]}-01'
body=event('verified-'+suffix)
check('durable_202',request(api,'/v1/events',keys['demo'],body,{'traceparent':trace_parent})[0]==202)
retry=request(api,'/v1/events',keys['demo'],body)
check('identical_retry_202',retry[0]==202 and retry[2].get('Relay-Idempotent-Replay')=='true')
changed=dict(body,data=dict(body['data'],value=99))
check('changed_content_409',request(api,'/v1/events',keys['demo'],changed)[0]==409)
wait_for(lambda:request(api,'/v1/events/'+body['eventId'],keys['demo'])[1]['state']=='processed','container metric effect')
check('one_database_effect',sql('relay',f"SELECT count(*) FROM event_records WHERE producer_id='demo' AND event_id='{body['eventId']}'")=='1')

other_bodies=[
    dict(event('v1-'+suffix),schemaVersion=1,data={'name':'v1.compatibility','value':5}),
    {'eventId':'deployment-'+suffix,'type':'deployment.completed','schemaVersion':1,'occurredAt':body['occurredAt'],
     'data':{'service':'demo-service','version':'1.0.0','success':True}},
    {'eventId':'logs-'+suffix,'type':'log.batch','schemaVersion':1,'occurredAt':body['occurredAt'],
     'data':{'severity':'info','entries':['synthetic demo entry']}},
]
for candidate in other_bodies:
    assert request(api,'/v1/events',keys['demo'],candidate)[0]==202
    wait_for(lambda:request(api,'/v1/events/'+candidate['eventId'],keys['demo'])[1]['state']=='processed',candidate['type'])
check('v1_upcast_deployment_and_log_contracts',True)
check('v1_unit_unspecified',sql('relay',"SELECT unit FROM rollups_minute WHERE producer_id='demo' AND metric_name='v1.compatibility'")=='unspecified')
check('admin_incident_listing',request(api,'/v1/admin/incidents',keys['operator'])[0]==200)
check('invalid_admin_action_409',request(api,'/v1/admin/incidents/1/invalid-action',keys['operator'],{})[0]==409)

def scrape():
    response=request('http://127.0.0.1:9464','/metrics')
    return response[0]==200 and 'relay_events_accepted_total' in response[1] and 'relay_consume_total' in response[1]
wait_for(scrape,'OTLP metrics exported to Prometheus endpoint',timeout=30)
check('live_application_metrics',True)
query='/api/v1/query?query='+quote('up{job="relay"}')
wait_for(lambda:prometheus_target_up(request('http://127.0.0.1:9090',query)[1]),
         'Prometheus target scrape',timeout=20)
check('prometheus_scrape_target_up',True)

time.sleep(6)
container=compose('ps','-q','collector',observability=True).stdout.strip()
assert container,'Collector must be running'
subprocess.run(['docker','cp',container+':/data/traces.jsonl',str(ARTIFACTS/'traces.jsonl')],
               env=docker_environment(),check=True,capture_output=True)
subprocess.run(['python3',str(ROOT/'scripts/verify-traces.py'),trace_id],check=True)
check('fresh_exported_causal_trace',True)
sources=fresh_sdk_sources((ARTIFACTS/'traces.jsonl').read_text(),started_ns)
check('azure_sdk_spans_exported',any(name.startswith('Azure.Messaging.ServiceBus') for name in sources))

for service in ['api','worker']:
    container=compose('ps','-q',service,observability=True,apps=True).stdout.strip()
    result=subprocess.run(['docker','inspect',container,'--format','{{.Config.User}}'],env=docker_environment(),
                          text=True,capture_output=True,check=True)
    check(service+'_runs_non_root',result.stdout.strip() not in ['','0','root','0:0'])

report={'checks':checks,'eventId':body['eventId'],'traceId':trace_id,'containers':'Release',
        'dateUtc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())}
(ARTIFACTS/'demo-results.json').write_text(json.dumps(report,indent=2)+'\n')
