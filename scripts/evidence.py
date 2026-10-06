"""Pure predicates for current-run observability evidence."""
import json

def prometheus_target_up(body):
    if not isinstance(body,dict) or body.get('status')!='success':return False
    return any(series.get('value',[None,None])[1]=='1'
               for series in body.get('data',{}).get('result',[]))

def fresh_sdk_sources(lines,started_ns):
    sources=set()
    for line in lines.splitlines():
        for resource in json.loads(line).get('resourceSpans',[]):
            for scope in resource.get('scopeSpans',[]):
                if any(int(span.get('startTimeUnixNano',0))>=started_ns for span in scope.get('spans',[])):
                    sources.add(scope.get('scope',{}).get('name',''))
    return sources
