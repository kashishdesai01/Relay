#!/usr/bin/env python3
"""Isolated live recovery demo; fault injection exists only in FaultTests artifacts."""
import argparse
import json
import time
import uuid
from common import ARTIFACTS, Service, event, migrate, provision, request, sql, wait_for
from credentials import load_or_create

parser=argparse.ArgumentParser()
parser.add_argument('--verify',action='store_true',help='Exercise replay and discard automatically, then clean up')
args=parser.parse_args()
database='relay_test_recovery_'+uuid.uuid4().hex[:8]
services=[]
created=False
def start(project,**kwargs):
    service=Service(project,database,**kwargs);services.append(service);return service

try:
    sql('postgres',f'CREATE DATABASE {database}');created=True
    migrate(database,'FaultTests')
    sql(database,f"SELECT setval('outbox_id_seq',{time.time_ns()//100})")
    key_path=ARTIFACTS/'local-secrets-recovery.json'
    keys=load_or_create(key_path)
    for producer,scopes in [('demo',['events.write','events.read']),('operator',['relay.admin'])]:
        assert provision(database,producer,scopes,'FaultTests',keys[producer])==keys[producer]
    api=start('Relay.Api')
    dispatcher=start('Relay.Worker',roles='dispatcher,dlq-capture')
    time.sleep(1)

    def quarantine(event_id):
        body=event(event_id)
        assert request(api.url,'/v1/events',keys['demo'],body)[0]==202
        poison=start('Relay.Worker',roles='consumer',failpoint='consumer.permanent_failure')
        try:
            wait_for(lambda:request(api.url,'/v1/events/'+event_id,keys['demo'])[1]['state']=='consumer_quarantined',
                     'synthetic handler failure captured')
        finally:poison.stop()
        rows=request(api.url,'/v1/admin/incidents',keys['operator'])[1]
        return next(row for row in rows if row['eventId']==event_id)

    incident=quarantine('recovery-demo')
    healthy=start('Relay.Worker',roles='consumer')
    print('Recovery console: '+api.url,flush=True)
    print('Session keys: artifacts/local-secrets-recovery.json (private)',flush=True)
    print('Refresh incidents, inspect recovery-demo, then click replay. The faulty worker has stopped.',flush=True)
    print('The healthy worker will apply the retained original event once. Press Ctrl+C to stop and clean up.',flush=True)
    if args.verify:
        detail=request(api.url,f"/v1/admin/incidents/{incident['id']}",keys['operator'])
        assert detail[0]==200 and 'SequenceNumber' in detail[1]['metadataJson']
        replay=request(api.url,f"/v1/admin/incidents/{incident['id']}/replay",keys['operator'],{})
        assert replay[0]==200 and replay[1]['state']=='replay_pending'
        wait_for(lambda:request(api.url,'/v1/events/recovery-demo',keys['demo'])[1]['state']=='processed','replay effect')
        assert sql(database,"SELECT count(*) FROM event_records WHERE event_id='recovery-demo'")=='1'
        assert request(api.url,f"/v1/admin/incidents/{incident['id']}/replay",keys['operator'],{})[0]==409
        healthy.stop()
        discarded=quarantine('discard-demo')
        resolution=request(api.url,f"/v1/admin/incidents/{discarded['id']}/discard",keys['operator'],{})
        assert resolution[0]==200 and resolution[1]['state']=='discarded'
        assert request(api.url,'/v1/events/discard-demo',keys['demo'])[1]['state']=='discarded'
        assert sql(database,"SELECT count(*) FROM event_records WHERE event_id='discard-demo'")=='0'
        assert sql(database,'SELECT count(*) FROM incident_actions')=='2'
        report={'status':'passed','checks':['real_handler_failure_to_dlq','authenticated_diagnostics','operator_replay',
            'one_committed_effect','repeat_action_conflict','operator_discard','discard_has_no_effect','durable_actor_audit'],
            'injection':'FaultTests-only; normal recovery endpoints','dateUtc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())}
        (ARTIFACTS/'recovery-demo-results.json').write_text(json.dumps(report,indent=2)+'\n')
        print('PASS isolated recovery demo: replay, discard, and audit verified',flush=True)
    else:
        try:
            while True:time.sleep(1)
        except KeyboardInterrupt:
            pass
finally:
    for service in reversed(services):service.stop()
    if created:sql('postgres',f'DROP DATABASE IF EXISTS {database} WITH (FORCE)')
