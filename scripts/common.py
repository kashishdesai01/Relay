"""Local harness utilities. All secrets live in environment/private files, not result artifacts."""
import json
import os
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / 'artifacts'
ARTIFACTS.mkdir(exist_ok=True)

def settings():
    path = ROOT / '.env'
    if not path.exists():
        raise RuntimeError('Missing .env; run scripts/configure-demo.py after accepting licenses.')
    return dict(line.split('=', 1) for line in path.read_text().splitlines() if line and not line.startswith('#'))

def docker_environment():
    env = dict(os.environ)
    dedicated = Path.home() / '.colima/relay/docker.sock'
    if dedicated.exists() and 'DOCKER_HOST' not in env:
        env['DOCKER_HOST'] = f'unix://{dedicated}'
        env['DOCKER_CONFIG'] = str(ROOT / '.review-tools/docker-config')
    return env

def compose(*args, observability=False, apps=False, capture=True, extra_env=None):
    command = ['docker-compose'] if shutil.which('docker-compose') else ['docker', 'compose']
    command += ['--env-file', str(ROOT / '.env'), '-f', str(ROOT / 'deploy/compose.infrastructure.yml')]
    if observability:
        command += ['-f', str(ROOT / 'deploy/compose.observability.yml')]
    if apps:
        command += ['-f', str(ROOT / 'deploy/compose.apps.yml')]
    env = docker_environment()
    env.update(extra_env or {})
    return subprocess.run(command + list(args), env=env, cwd=ROOT, check=True,
                          text=True, capture_output=capture)

def database_env(name):
    config = settings()
    if not name.replace('_', '').isalnum():
        raise ValueError('Invalid harness database name')
    env = dict(os.environ)
    env['Relay__Database'] = f'Host=127.0.0.1;Port=55440;Database={name};Username=relay;Password={config["POSTGRES_PASSWORD"]}'
    env['Relay__ServiceBusConnectionString'] = 'Endpoint=sb://127.0.0.1;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;'
    env['Relay__Queue'] = 'relay-tests' if name.startswith('relay_test') else 'relay'
    env['DOTNET_CLI_HOME'] = str(ROOT / '.review-tools/cli-home')
    env['DOTNET_CLI_TELEMETRY_OPTOUT'] = '1'
    env['OTEL_EXPORTER_OTLP_ENDPOINT'] = 'http://127.0.0.1:4317'
    return env

def sql(name, query):
    # PostgreSQL credentials are already inside the private container environment.
    result = compose('exec', '-T', 'postgres', 'psql', '-v', 'ON_ERROR_STOP=1', '-U', 'relay', '-d', name, '-At', '-c', query)
    return result.stdout.strip()

def dotnet():
    local = ROOT / '.review-tools/dotnet/dotnet'
    return str(local) if local.exists() else shutil.which('dotnet') or 'dotnet'

def migrate(name, configuration='Debug'):
    subprocess.run([dotnet(), str(ROOT / f'src/Relay.Migrator/bin/{configuration}/net10.0/Relay.Migrator.dll')],
                   env=database_env(name), cwd=ROOT, check=True, capture_output=True)

def provision(name, producer, scopes, configuration='Debug', key=None):
    env=database_env(name)
    if key is not None:env['RELAY_PROVISION_KEY']=key
    result = subprocess.run([dotnet(), str(ROOT / f'src/Relay.Migrator/bin/{configuration}/net10.0/Relay.Migrator.dll'),
                             'provision', producer, ','.join(scopes)], env=env, cwd=ROOT,
                            check=True, capture_output=True, text=True)
    return result.stdout.strip().splitlines()[-1]

def port():
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        return listener.getsockname()[1]

class Service:
    def __init__(self, project, database, *, configuration='FaultTests', failpoint=None, roles=None, overrides=None):
        self.port = port()
        env = database_env(database)
        env['ASPNETCORE_URLS'] = f'http://127.0.0.1:{self.port}'
        if failpoint:
            env['RELAY_FAILPOINTS'] = failpoint
        else:
            env.pop('RELAY_FAILPOINTS', None)
        if roles:
            env['Relay__Roles'] = roles
        env.update(overrides or {})
        self.logpath = ARTIFACTS / f'{project}-{self.port}.log'
        self.log = self.logpath.open('w')
        self.process = subprocess.Popen([dotnet(), str(ROOT / f'src/{project}/bin/{configuration}/net10.0/{project}.dll')],
                                        env=env, cwd=ROOT / f'src/{project}', stdout=self.log, stderr=subprocess.STDOUT)
        self.url = f'http://127.0.0.1:{self.port}'
        self.wait_live()
    def wait_live(self):
        for _ in range(100):
            if self.process.poll() is not None:
                raise RuntimeError(f'{self.process.args[-1]} exited; inspect {self.logpath.name}')
            try:
                if request(self.url, '/health/live')[0] == 200:
                    time.sleep(0.1)
                    return
            except OSError:
                pass
            time.sleep(0.1)
        raise RuntimeError('Service startup timed out')
    def stop(self, kill=False):
        if self.process.poll() is None:
            self.process.kill() if kill else self.process.terminate()
            try:
                self.process.wait(timeout=65)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
                raise RuntimeError('Graceful shutdown exceeded 65 seconds')
        self.log.close()

def request(base, path, key=None, body=None, headers=None):
    all_headers = dict(headers or {})
    if key:
        all_headers['X-Relay-Key'] = key
    if body is not None:
        all_headers['Content-Type'] = 'application/json'
    data = json.dumps(body).encode() if body is not None else None
    request_object = urllib.request.Request(base + path, data=data, headers=all_headers)
    try:
        response = urllib.request.urlopen(request_object, timeout=15)
    except urllib.error.HTTPError as error:
        response = error
    raw = response.read()
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        value = raw.decode()
    return response.status, value, dict(response.headers)

def event(event_id, value=12):
    return {'eventId':event_id, 'type':'metric.reported', 'schemaVersion':2,
            'occurredAt':'2026-10-13T09:00:00Z', 'data':{'name':'harness.latency','value':value,'unit':'ms'}}

def wait_for(predicate, description, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.15)
    raise AssertionError('Timed out: ' + description)
