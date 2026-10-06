#!/usr/bin/env python3
"""Seeded worker-crash scenario with durable admission/effect/incident reconciliation."""
import collections
import json
import random
import time
import uuid
from common import ARTIFACTS, Service, event, migrate, provision, request, sql, wait_for

database='relay_test_chaos_'+uuid.uuid4().hex[:8]
rng=random.Random(42)
services=[]
schedule=[]
expected={}
seed=42

def start(project,**kwargs):
    service=Service(project,database,**kwargs); services.append(service); return service

try:
    sql('postgres',f'CREATE DATABASE {database}')
    migrate(database,'FaultTests')
    sql(database,f"SELECT setval('outbox_id_seq',{time.time_ns()//100})")
    keys={name:provision(database,name,['events.write','events.read'],'FaultTests') for name in ['producer0','producer1','producer2']}
    api=start('Relay.Api',overrides={'Relay__ProducerRequestsPerSecond':'10000'})
    dispatcher=start('Relay.Worker',roles='dispatcher,dlq-capture')
    consumers=[start('Relay.Worker',roles='consumer') for _ in range(2)]
    time.sleep(1)
    started=time.monotonic()
    for index in range(200):
        producer=rng.choice(list(keys))
        body=event(f'chaos-{index}',rng.randint(-20,20))
        identity=(producer,body['eventId']); expected[identity]=body['data']['value']
        response=request(api.url,'/v1/events',keys[producer],body)
        assert response[0]==202
        schedule.append({'seconds':time.monotonic()-started,'operation':'admit','producer':producer,'eventId':body['eventId']})
        if rng.random()<.45:
            assert request(api.url,'/v1/events',keys[producer],body)[0]==202
            schedule.append({'seconds':time.monotonic()-started,'operation':'duplicate','producer':producer,'eventId':body['eventId']})
        if index and index%35==0:
            victim=rng.randrange(2)
            consumers[victim].stop(kill=True)
            schedule.append({'seconds':time.monotonic()-started,'operation':'kill_consumer','replica':victim})
            consumers[victim]=start('Relay.Worker',roles='consumer')
        time.sleep(.01)

    def reconciled():
        return int(sql(database,"""SELECT count(*) FROM events e WHERE
          (EXISTS(SELECT 1 FROM processed_events p WHERE p.producer_id=e.producer_id AND p.event_id=e.event_id)
           AND EXISTS(SELECT 1 FROM event_records r WHERE r.producer_id=e.producer_id AND r.event_id=e.event_id))
          OR EXISTS(SELECT 1 FROM incidents i WHERE i.producer_id=e.producer_id AND i.event_id=e.event_id AND i.state='captured')"""))==len(expected)
    wait_for(reconciled,'durable admission/effect/current-incident reconciliation',timeout=60)
    processed_rows=json.loads(sql(database,"SELECT COALESCE(json_agg(json_build_array(producer_id,event_id)),'[]') FROM processed_events"))
    committed=collections.defaultdict(list)
    for producer,event_id in processed_rows:committed[producer].append(expected[(producer,event_id)])
    rollups=json.loads(sql(database,"SELECT COALESCE(json_agg(json_build_object('producer',producer_id,'count',count,'sum',sum,'min',min,'max',max)),'[]') FROM rollups_minute"))
    for row in rollups:
        values=committed[row['producer']]
        assert row['count']==len(values) and row['sum']==sum(values) and row['min']==min(values) and row['max']==max(values)
    assert len(rollups)==len(committed)
    records=int(sql(database,'SELECT count(*) FROM event_records'))
    assert records==len(processed_rows)
    quarantined=int(sql(database,"SELECT count(*) FROM events e WHERE e.processed_at IS NULL AND EXISTS(SELECT 1 FROM incidents i WHERE i.producer_id=e.producer_id AND i.event_id=e.event_id AND i.state='captured')"))
    assert len(processed_rows)+quarantined==len(expected)
    report={'seed':seed,'acceptedEvents':len(expected),'committedEvents':len(processed_rows),'recoverableEvents':quarantined,
      'effectReconciliation':True,'consumerReplicas':2,'durationSeconds':time.monotonic()-started,'schedule':schedule,
      'scope':'real local emulator; seed fixes input choices, recorded schedule captures actual timing'}
    (ARTIFACTS/'chaos-results.json').write_text(json.dumps(report,indent=2)+'\n')
    print(f"PASS chaos: {len(expected)} admissions, {records} exact effects, {quarantined} recoverable events")
finally:
    for service in reversed(services):service.stop()
    sql('postgres',f'DROP DATABASE IF EXISTS {database} WITH (FORCE)')
