#!/usr/bin/env python3
"""Measured local load and dispatch-outage recovery. Uses real HTTP, Postgres, and Service Bus."""
import concurrent.futures
import argparse
import json
import platform
import statistics
import time
import uuid
from common import ARTIFACTS, Service, event, migrate, provision, request, sql, wait_for

database='relay_test_load_'+uuid.uuid4().hex[:8]
services=[]
samples=[]
counts={}
parser=argparse.ArgumentParser()
parser.add_argument('--events',type=int,default=1000)
parser.add_argument('--workers',type=int,default=1)
parser.add_argument('--rate',type=float,default=0,help='Target unique events/sec; zero means closed-loop')
args=parser.parse_args()
unique=args.events
concurrency=16
workers=[]
worker=None

def start(project,**kwargs):
    service=Service(project,database,configuration='Release',**kwargs); services.append(service); return service

def percentile(values, fraction):
    values=sorted(values)
    return values[min(len(values)-1,int((len(values)-1)*fraction))]

try:
    sql('postgres',f'CREATE DATABASE {database}')
    migrate(database,'Release')
    sql(database,f"SELECT setval('outbox_id_seq',{time.time_ns()//100})")
    key=provision(database,'load',['events.read','events.write'],'Release')
    api=start('Relay.Api',overrides={'Relay__ProducerRequestsPerSecond':'10000','Relay__OldestPendingLimitSeconds':'300'})
    workers=[start('Relay.Worker') for _ in range(args.workers)]
    wait_for(lambda:request(api.url,'/health/ready')[0]==200,'API readiness')
    time.sleep(1)
    # Warmup is excluded from measured admissions and uses a separate metric key/id set.
    for i in range(20):
        body=event(f'warmup-{i}',i%10); body['data']['name']='warmup'
        assert request(api.url,'/v1/events',key,body)[0]==202
    wait_for(lambda:int(sql(database,"SELECT count(*) FROM processed_events WHERE event_id LIKE 'warmup-%'"))==20,'warmup drain')

    def send(index):
        if args.rate:
            delay=started+index/args.rate-time.perf_counter()
            if delay>0:time.sleep(delay)
        body=event(f'load-{index}',index%10)
        request_started=time.perf_counter(); response=request(api.url,'/v1/events',key,body)
        return response[0],(time.perf_counter()-request_started)*1000,len(json.dumps(body).encode())
    started=time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        for status,latency,size in pool.map(send,range(unique)):
            counts[str(status)]=counts.get(str(status),0)+1; samples.append(latency)
    admission_seconds=time.perf_counter()-started
    assert counts=={'202':unique},counts
    drain_started=time.perf_counter()
    wait_for(lambda:int(sql(database,"SELECT count(*) FROM processed_events WHERE event_id LIKE 'load-%'"))==unique,'measured load drain',timeout=60)
    load_drain=time.perf_counter()-drain_started
    e2e=json.loads(sql(database,"""SELECT json_build_object('p50',percentile_cont(0.5) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM processed_at-received_at)*1000),
        'p95',percentile_cont(0.95) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM processed_at-received_at)*1000),
        'p99',percentile_cont(0.99) WITHIN GROUP(ORDER BY EXTRACT(EPOCH FROM processed_at-received_at)*1000))
        FROM events WHERE event_id LIKE 'load-%'"""))
    assert int(sql(database,"SELECT count FROM rollups_minute WHERE metric_name='harness.latency'"))==unique
    assert float(sql(database,"SELECT sum FROM rollups_minute WHERE metric_name='harness.latency'"))==sum(i%10 for i in range(unique))
    assert float(sql(database,"SELECT min FROM rollups_minute WHERE metric_name='harness.latency'"))==0
    assert float(sql(database,"SELECT max FROM rollups_minute WHERE metric_name='harness.latency'"))==9

    # Explicit dispatcher pause demonstrates API-side backpressure without restarting a non-durable emulator.
    for worker in workers:worker.stop()
    paused_api=start('Relay.Api',overrides={'Relay__BacklogLimit':'10','Relay__ProducerRequestsPerSecond':'10000'})
    time.sleep(1)
    outage_started=time.perf_counter()
    for i in range(10):
        assert request(paused_api.url,'/v1/events',key,event(f'recovery-{i}',1))[0]==202
    time.sleep(1.2)
    shedding=request(paused_api.url,'/v1/events',key,event('shed-attempt',1))
    assert shedding[0]==503 and shedding[2].get('Retry-After')=='1'
    pending=int(sql(database,"SELECT count(*) FROM outbox WHERE status='pending'"))
    resumed=time.perf_counter(); workers=[start('Relay.Worker') for _ in range(args.workers)]
    wait_for(lambda:int(sql(database,"SELECT count(*) FROM processed_events WHERE event_id LIKE 'recovery-%'"))==10,'outage backlog drain',timeout=30)
    recovery_seconds=time.perf_counter()-resumed
    report={'scope':'local API and workers; real emulator, not production capacity','dateUtc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),
        'host':{'system':platform.system(),'machine':platform.machine(),'processor':platform.processor()},
        'containerBudget':{'cpus':4,'memoryGiB':6,'sqlServer':'amd64 under Rosetta'},
        'databaseVersion':sql(database,'SHOW server_version'),'brokerVersion':'Service Bus emulator 2.0.1',
        'configuration':'Release','workerReplicas':args.workers,'targetEventsPerSecond':args.rate or None,
        'producerRateLimit':10000,'uniqueEvents':unique,'concurrency':concurrency,'warmupEvents':20,
        'payloadBytesExample':len(json.dumps(event('load-999',9)).encode()),'httpStatusCounts':counts,
        'admissionSeconds':admission_seconds,'uniqueAdmissionsPerSecond':unique/admission_seconds,
        'httpAcceptLatencyMs':{'p50':percentile(samples,.5),'p95':percentile(samples,.95),'p99':percentile(samples,.99)},
        'endToEndLatencyMs':e2e,'postAdmissionDrainSeconds':load_drain,
        'recovery':{'fault':'dispatcher/consumer process stopped; broker stayed alive','outageSeconds':resumed-outage_started,
            'pendingEvents':pending,'shedStatus':shedding[0],'resumeToDrainSeconds':recovery_seconds},
        'exactMetricReconciliation':True,'repetitions':1,'limitations':['finite local run','not a saturation sweep','HTTP client opens new connections','not high availability evidence']}
    report_file='sustained-load-results.json' if args.rate else 'load-results.json'
    (ARTIFACTS/report_file).write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
finally:
    for service in reversed(services):service.stop()
    sql('postgres',f'DROP DATABASE IF EXISTS {database} WITH (FORCE)')
