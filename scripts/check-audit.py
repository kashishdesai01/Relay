#!/usr/bin/env python3
"""Fail CI on vulnerable dependencies or incomplete audit output."""
import json
import sys
report=json.load(open(sys.argv[1]))
if report.get('problems'):
    raise SystemExit('Dependency audit returned problems')
if not isinstance(report.get('projects'),list) or not report['projects']:
    raise SystemExit('Dependency audit did not cover any projects')
vulnerable=[]
for project in report.get('projects',[]):
    for framework in project.get('frameworks',[]):
        for package in framework.get('topLevelPackages',[])+framework.get('transitivePackages',[]):
            if package.get('vulnerabilities'):vulnerable.append(package['id'])
if vulnerable:raise SystemExit('Vulnerable dependencies: '+', '.join(sorted(set(vulnerable))))
print('Dependency audit: no known vulnerable packages reported.')
