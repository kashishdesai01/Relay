'use strict';
const byId = id => document.getElementById(id);
let currentEvent = null;
let currentOccurredAt = null;
let pollGeneration = 0;
function newId() { byId('event-id').value = `demo-${crypto.randomUUID()}`; currentEvent = null; currentOccurredAt = null; }
function note(message) { const log = byId('activity'); log.textContent = `${new Date().toLocaleTimeString()}  ${message}\n${log.textContent}`.slice(0,8000); }
async function api(path, admin = false, body) {
  const key = byId(admin ? 'admin-key' : 'producer-key').value;
  if (!key) throw new Error(`Enter a ${admin ? 'admin' : 'producer'} key first.`);
  const response = await fetch(path, { method: body === undefined ? 'GET' : 'POST', headers: { 'X-Relay-Key': key, 'Content-Type': 'application/json' }, body: body === undefined ? undefined : JSON.stringify(body) });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(`${response.status}: ${result.title || 'Request failed'}`);
  return { result, status: response.status, replay: response.headers.get('Relay-Idempotent-Replay') === 'true' };
}
async function lookup(id, generation) {
  const { result } = await api(`/v1/events/${encodeURIComponent(id)}`);
  if (generation !== pollGeneration) return;
  byId('status').textContent = JSON.stringify(result, null, 2);
  if (['accepted','dispatched','replay_pending'].includes(result.state)) setTimeout(() => lookup(id,generation).catch(e => note(e.message)),1000);
}
byId('ingest').addEventListener('submit', async event => {
  event.preventDefault(); const button = event.submitter; button.disabled = true;
  try {
    const id = byId('event-id').value;
    if (currentEvent !== id) { currentEvent = id; currentOccurredAt = new Date().toISOString(); }
    const body = { eventId:id,type:'metric.reported',schemaVersion:2,occurredAt:currentOccurredAt,data:{name:'demo.latency',value:Number(byId('metric-value').value),unit:'ms'} };
    const response = await api('/v1/events',false,body); note(`${response.status} ${id}${response.replay ? ' (idempotent retry)' : ' (new admission)'}`);
    byId('lookup-id').value = id; await lookup(id,++pollGeneration);
  } catch (error) { note(error.message); } finally { button.disabled = false; }
});
byId('lookup').addEventListener('submit', event => { event.preventDefault(); lookup(byId('lookup-id').value,++pollGeneration).catch(e=>note(e.message)); });
byId('new-id').addEventListener('click',newId);
async function refresh() {
  const {result: incidents} = await api('/v1/admin/incidents',true);
  const table = byId('incidents'); table.replaceChildren();
  for (const incident of incidents) {
    const row = document.createElement('tr');
    for (const value of [incident.id,incident.eventId || 'Unrecognized transport',`${incident.source} / ${incident.reason}`,incident.state]) { const cell=document.createElement('td'); cell.textContent=value; row.append(cell); }
    const actions=document.createElement('td');
    const inspect=document.createElement('button');inspect.textContent='Inspect';inspect.className='secondary';
    inspect.addEventListener('click',async()=>{try{const {result}=await api(`/v1/admin/incidents/${incident.id}`,true);const details=byId('incident-details');details.hidden=false;details.textContent=JSON.stringify({incident:result.incident,description:result.description,metadata:JSON.parse(result.metadataJson)},null,2);}catch(error){note(error.message);}});actions.append(inspect);
    if(incident.state==='captured') for(const action of ['replay','discard']) {
      const button=document.createElement('button'); button.textContent=action; button.className='secondary';
      button.addEventListener('click',async()=>{ if(action==='discard' && !confirm('Discard this event? This suppresses future processing for its identity.')) return; button.disabled=true; try {const response=await api(`/v1/admin/incidents/${incident.id}/${action}`,true,{}); note(`Incident ${incident.id}: ${response.result.state}`);await refresh();} catch(error){note(error.message);button.disabled=false;} }); actions.append(button);
    }
    row.append(actions);table.append(row);
  }
  if(!incidents.length){const row=document.createElement('tr');const cell=document.createElement('td');cell.colSpan=5;cell.textContent='No delivery incidents.';row.append(cell);table.append(row);}
}
byId('refresh').addEventListener('click',()=>refresh().catch(e=>note(e.message)));
fetch('/health/ready').then(r=>{byId('health').textContent=r.ok?'API ready':'API unavailable';}).catch(()=>{byId('health').textContent='API unavailable';});
newId();
