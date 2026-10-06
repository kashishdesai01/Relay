#!/usr/bin/env python3
"""Run final source checks sequentially against the existing, approved local infrastructure."""
import hashlib
import json
import os
import subprocess
import time
import uuid
import xml.etree.ElementTree as ET
from common import ARTIFACTS, ROOT, Service, compose, database_env, migrate, request, sql, wait_for

checks=[]
def run(name,command,env=None):
    print('VERIFY '+name,flush=True)
    subprocess.run(command,cwd=ROOT,env=env,check=True)
    checks.append(name)

database='relay_test_verify_'+uuid.uuid4().hex[:8]
started=time.monotonic()
created=False
try:
    sql('postgres',f'CREATE DATABASE {database}')
    created=True
    env=database_env(database)
    env['RELAY_TEST_DATABASE']=env['Relay__Database']
    env['RELAY_TEST_SERVICEBUS']=env['Relay__ServiceBusConnectionString']
    run('python_credentials',['python3','scripts/test-harness.py'])
    run('locked_restore',['bash','scripts/dotnet.sh','restore','Relay.sln','--locked-mode'])
    run('format',['bash','scripts/dotnet.sh','format','Relay.sln','--verify-no-changes','--no-restore'])
    for configuration in ['Release','FaultTests']:
        run('build_'+configuration,['bash','scripts/dotnet.sh','build','Relay.sln','-c',configuration,
                                    '--no-restore','-m:1','-nr:false','-p:UseSharedCompilation=false'])
    migrate(database,'Release')
    probe=Service('Relay.Worker',database,configuration='Release',roles='dispatcher')
    try:
        wait_for(lambda:request(probe.url,'/health/ready')[0]==200,'native AMQP readiness',timeout=90)
        checks.append('native_amqp_readiness')
    finally:probe.stop()
    run('all_xunit_postgres17_and_broker',['bash','scripts/dotnet.sh','test','tests/Relay.Tests','-c','Release',
        '--no-build','--no-restore','--logger','trx;LogFileName=final.trx','--results-directory','artifacts/test-results'],env)
    run('release_failpoints_absent',['python3','scripts/verify-release.py'])
    run('actual_process_faults',['python3','scripts/fault-tests.py'])
    run('recovery_flow',['python3','scripts/demo-recovery.py','--verify'])
    run('seeded_chaos',['python3','scripts/chaos-test.py'])
    run('sustained_two_workers',['python3','scripts/load-test.py','--events','3000','--workers','2','--rate','50'])
    run('rebuild_demo',['python3','scripts/demo.py','up'])
    run('container_flow_observability',['python3','scripts/smoke-demo.py'])
    # Provisioning is deliberately retried against existing identities.
    before=(ARTIFACTS/'local-secrets.json').read_bytes()
    run('demo_restart_idempotence',['python3','scripts/demo.py','up'])
    assert before==(ARTIFACTS/'local-secrets.json').read_bytes()
    with (ARTIFACTS/'dependency-audit.json').open('w') as output:
        subprocess.run(['bash','scripts/dotnet.sh','list','Relay.sln','package','--vulnerable','--include-transitive','--format','json'],
                       cwd=ROOT,stdout=output,check=True)
    run('dependency_audit',['python3','scripts/check-audit.py','artifacts/dependency-audit.json'])
    compose('config','--quiet',observability=True,apps=True)
    checks.append('compose_config')
    subprocess.run(['docker','run','--rm','-v',str(ROOT/'deploy/prometheus/rules.yml')+':/rules.yml:ro',
        '--entrypoint','promtool','prom/prometheus:v3.7.3','check','rules','/rules.yml'],
        env=__import__('common').docker_environment(),check=True)
    checks.append('prometheus_rules')
    ns={'t':'http://microsoft.com/schemas/VisualStudio/TeamTest/2010'}
    counters=ET.parse(ARTIFACTS/'test-results/final.trx').find('.//t:Counters',ns).attrib
    fingerprint=hashlib.sha256()
    files=[]
    for folder in ['src','tests','scripts','deploy','.github']:
        for path in sorted((ROOT/folder).rglob('*')):
            if path.is_file() and not any(part in ['bin','obj','__pycache__'] for part in path.parts):files.append(path)
    files += [ROOT/name for name in ['Dockerfile','global.json','Directory.Build.props','Directory.Packages.props','NuGet.Config','Relay.sln']]
    for path in files:
        fingerprint.update(str(path.relative_to(ROOT)).encode());fingerprint.update(path.read_bytes())
    report={'status':'passed','checks':checks,'xunitCounters':counters,'sourceSha256':fingerprint.hexdigest(),
        'dateUtc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'durationSeconds':time.monotonic()-started,
        'scope':'local Release/FaultTests, PostgreSQL 17.11, real Service Bus emulator 2.0.1; no cloud deployment'}
    (ARTIFACTS/'verification-results.json').write_text(json.dumps(report,indent=2)+'\n')
    print('PASS final verification',flush=True)
finally:
    if created:sql('postgres',f'DROP DATABASE IF EXISTS {database} WITH (FORCE)')
