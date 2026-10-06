#!/usr/bin/env python3
"""Crash the actual API/worker processes and reconcile durable admissions with exact effects."""
import json
import time
import uuid
from common import ARTIFACTS, ROOT, Service, compose, event, migrate, provision, request, sql, wait_for

database = 'relay_test_fault_' + uuid.uuid4().hex[:8]
services = []
results = []
started = time.monotonic()

def start(project, **kwargs):
    service = Service(project, database, **kwargs)
    services.append(service)
    return service

def passed(name):
    results.append({'test':name,'status':'passed'})
    print('PASS ' + name, flush=True)

def accepted(api, key, body):
    wait_for(lambda: request(api.url,'/v1/events',key,body)[0] == 202,'event admission')

def processed(event_id):
    return sql(database, f"SELECT count(*) FROM processed_events WHERE event_id='{event_id}'") == '1'

try:
    sql('postgres', f'CREATE DATABASE {database}')
    migrate(database, 'FaultTests')
    # Isolate transport identity from other test databases sharing the broker queue.
    sql(database, f"SELECT setval('outbox_id_seq',{time.time_ns() // 100})")
    key = provision(database,'p',['events.write','events.read'],'FaultTests')
    admin_key = provision(database,'operator',['relay.admin'],'FaultTests')
    other_key = provision(database,'other',['events.read'],'FaultTests')

    for point, stored in [('api.before_commit',False),('api.after_commit',True)]:
        api = start('Relay.Api',failpoint=point)
        body = event(point.replace('.','-'))
        time.sleep(1)
        try:
            request(api.url,'/v1/events',key,body)
        except OSError:
            pass
        wait_for(lambda: api.process.poll() is not None,'API failpoint kill')
        assert (sql(database,f"SELECT count(*) FROM events WHERE event_id='{body['eventId']}'")=='1') == stored
        api.stop()
        healthy = start('Relay.Api')
        accepted(healthy,key,body)
        response = request(healthy.url,'/v1/events',key,body)
        assert response[0] == 202 and response[2].get('Relay-Idempotent-Replay') == 'true'
        healthy.stop()
        passed(point)

    api = start('Relay.Api')
    assert request(api.url,'/v1/events',body=event('unauth'))[0] == 401
    assert request(api.url,'/v1/admin/incidents',key)[0] == 403
    assert request(api.url,'/v1/events/api-after_commit',other_key)[0] == 404
    assert request(api.url,'/v1/events',key,event('api-after_commit',99))[0] == 409
    assert request(api.url,'/v1/events',key,event('invalid',1e300))[0] == 422
    oversized = event('oversized'); oversized['padding'] = 'x' * 210000
    assert request(api.url,'/v1/events',key,oversized)[0] == 413
    passed('api.authorization_conflict_validation_and_body_limit')

    dispatch = start('Relay.Worker',roles='dispatcher',failpoint='dispatcher.after_publish')
    wait_for(lambda: dispatch.process.poll() is not None,'dispatcher after-publish kill')
    assert int(sql(database,"SELECT count(*) FROM outbox WHERE status='pending'")) >= 2
    dispatch.stop()
    dispatch = start('Relay.Worker',roles='dispatcher')
    consumer = start('Relay.Worker',roles='consumer,dlq-capture')
    wait_for(lambda: sql(database,'SELECT count(*) FROM processed_events')=='2','drain after dispatcher crash')
    passed('dispatcher.after_publish')

    for point in ['consumer.before_commit','consumer.after_commit']:
        consumer.stop()
        body=event(point.replace('.','-'))
        accepted(api,key,body)
        doomed=start('Relay.Worker',roles='consumer',failpoint=point)
        wait_for(lambda: doomed.process.poll() is not None,'consumer failpoint kill')
        assert processed(body['eventId']) == (point=='consumer.after_commit')
        doomed.stop()
        consumer=start('Relay.Worker',roles='consumer,dlq-capture')
        wait_for(lambda: processed(body['eventId']),'consumer crash recovery',timeout=30)
        passed(point)

    consumer.stop()
    body=event('replay-race')
    accepted(api,key,body)
    poison=start('Relay.Worker',roles='consumer',failpoint='consumer.permanent_failure')
    capture=start('Relay.Worker',roles='dlq-capture',failpoint='capture.after_insert')
    wait_for(lambda: capture.process.poll() is not None,'capture commit crash')
    poison.stop(); capture.stop()
    incidents=request(api.url,'/v1/admin/incidents',admin_key)[1]
    incident=next(i for i in incidents if i['eventId']=='replay-race')
    details=request(api.url,f"/v1/admin/incidents/{incident['id']}",admin_key)
    assert details[0]==200 and 'SequenceNumber' in details[1]['metadataJson']
    assert request(api.url,f"/v1/admin/incidents/{incident['id']}",key)[0]==403
    before_replay=int(sql(database,'SELECT count(*) FROM outbox'))
    interrupted=start('Relay.Api',failpoint='replay.before_commit')
    try:
        request(interrupted.url,f"/v1/admin/incidents/{incident['id']}/replay",admin_key,{})
    except OSError:
        pass
    wait_for(lambda:interrupted.process.poll() is not None,'replay transaction crash')
    interrupted.stop()
    assert int(sql(database,'SELECT count(*) FROM outbox'))==before_replay
    assert sql(database,'SELECT count(*) FROM incident_actions')=='0'
    assert request(api.url,f"/v1/admin/incidents/{incident['id']}",admin_key)[1]['incident']['state']=='captured'
    passed('replay.before_commit_atomic_rollback')
    response=request(api.url,f"/v1/admin/incidents/{incident['id']}/replay",admin_key,{})
    assert response[0]==200 and response[1]['state']=='replay_pending'
    consumer=start('Relay.Worker',roles='consumer,dlq-capture')
    wait_for(lambda: processed('replay-race'),'immediate replay after capture crash')
    # Five-second test queue lock expires, so original DLQ delivery must be recaptured without a new incident.
    time.sleep(6)
    assert sql(database,"SELECT count(*) FROM incidents WHERE event_id='replay-race'")=='1'
    assert request(api.url,f"/v1/admin/incidents/{incident['id']}/replay",admin_key,{})[0]==409
    passed('capture.after_insert_then_immediate_replay_and_original_redelivery')

    consumer.stop(); accepted(api,key,event('shutdown-drain'))
    marker=ARTIFACTS/'shutdown-entered.txt'
    marker.unlink(missing_ok=True)
    consumer=start('Relay.Worker',roles='consumer,dlq-capture',failpoint='consumer.pause',overrides={'RELAY_PAUSE_MARKER':str(marker)})
    wait_for(lambda:marker.exists(),'handler entered shutdown barrier')
    shutdown_started=time.monotonic()
    consumer.stop()  # Popen.terminate sends a real SIGTERM on Unix while handler is active.
    assert time.monotonic()-shutdown_started<10
    assert consumer.process.returncode==0
    consumer=start('Relay.Worker',roles='consumer,dlq-capture')
    wait_for(lambda: processed('shutdown-drain'),'SIGTERM recovery')
    passed('worker.sigterm')

    consumer.stop()
    accepted(api,key,event('db-outage'))
    wait_for(lambda:sql(database,"SELECT count(*) FROM outbox WHERE event_id='db-outage' AND status='sent'")=='1','outage event published')
    compose('pause','postgres')
    try:
        consumer=start('Relay.Worker',roles='consumer, dlq-capture')
        assert request(api.url,'/health/live','a'*64)[0]==200
        assert request(api.url,'/unmatched','a'*64)[0]==404
        assert request(consumer.url,'/health/ready')[0]==503
        time.sleep(2)
    finally:
        compose('unpause','postgres')
    wait_for(lambda:request(consumer.url,'/health/ready')[0]==200,'supervisor startup after DB recovery')
    wait_for(lambda:processed('db-outage'),'database outage recovery')
    passed('supervisor_database_outage_startup_and_recovery')

    consumer.stop()
    sql(database,"INSERT INTO outbox(producer_id,event_id) VALUES('p','shutdown-drain')")
    consumer=start('Relay.Worker',roles='consumer',failpoint='consumer.skip_dedupe')
    wait_for(lambda:int(sql(database,'SELECT sum(count) FROM rollups_minute'))>int(sql(database,'SELECT count(*) FROM events')),'dedupe mutation produces duplicate effect')
    consumer.stop()
    assert int(sql(database,'SELECT sum(count) FROM rollups_minute'))!=int(sql(database,'SELECT count(*) FROM events'))
    sql(database,'UPDATE rollups_minute SET count=count-1,sum=sum-12')
    consumer=start('Relay.Worker',roles='consumer,dlq-capture')
    passed('disabled_dedupe_is_detected_by_full_effect_oracle')

    accepted_count=int(sql(database,'SELECT count(*) FROM events'))
    wait_for(lambda:int(sql(database,'SELECT count(*) FROM processed_events'))==accepted_count,'complete reconciliation')
    assert int(sql(database,'SELECT count(*) FROM event_records'))==accepted_count
    assert sql(database,'SELECT count FROM rollups_minute')==str(accepted_count)
    assert float(sql(database,'SELECT sum FROM rollups_minute'))==accepted_count*12
    assert float(sql(database,'SELECT min FROM rollups_minute'))==12
    assert float(sql(database,'SELECT max FROM rollups_minute'))==12
    passed('exact_effect_reconciliation')

    logs='\n'.join(service.logpath.read_text() for service in services)
    assert key not in logs and admin_key not in logs and other_key not in logs
    passed('api_key_log_redaction')
    (ARTIFACTS/'fault-results.json').write_text(json.dumps({'results':results,'durationSeconds':time.monotonic()-started,
        'acceptedEvents':accepted_count,'configuration':'FaultTests','broker':'Service Bus emulator 2.0.1','database':'PostgreSQL container'},indent=2)+'\n')
finally:
    for service in reversed(services):
        service.stop()
    sql('postgres',f'DROP DATABASE IF EXISTS {database} WITH (FORCE)')
