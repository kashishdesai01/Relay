#!/usr/bin/env python3
"""Wait for native SDK AMQP readiness; the emulator HTTP endpoint alone is insufficient."""
import argparse
import uuid
from common import Service, migrate, request, sql, wait_for

parser=argparse.ArgumentParser()
parser.add_argument('--configuration',default='Release')
args=parser.parse_args()
database='relay_test_brokerprobe_'+uuid.uuid4().hex[:8]
created=False
service=None
try:
    sql('postgres','CREATE DATABASE '+database);created=True
    migrate(database,args.configuration)
    service=Service('Relay.Worker',database,configuration=args.configuration,roles='dispatcher')
    wait_for(lambda:request(service.url,'/health/ready')[0]==200,'native SDK AMQP readiness',timeout=90)
    print('PASS native SDK AMQP readiness')
finally:
    if service is not None:service.stop()
    if created:sql('postgres','DROP DATABASE '+database+' WITH (FORCE)')
