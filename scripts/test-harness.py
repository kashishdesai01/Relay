#!/usr/bin/env python3
import json
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from credentials import load_or_create
from evidence import prometheus_target_up, fresh_sdk_sources

class CredentialTests(unittest.TestCase):
    def test_atomic_private_and_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'local-secrets.json'
            first=load_or_create(path)
            self.assertEqual(first,load_or_create(path))
            self.assertEqual(0o600,stat.S_IMODE(path.stat().st_mode))
    def test_interrupted_write_does_not_publish_partial_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'local-secrets.json'
            def interrupt(keys,file):
                file.write('{')
                raise RuntimeError('Simulated interruption')
            with patch('credentials.json.dump',side_effect=interrupt):
                with self.assertRaises(RuntimeError):load_or_create(path)
            self.assertFalse(path.exists())
            self.assertEqual([],list(Path(directory).iterdir()))
            self.assertEqual(2,len(load_or_create(path)))
    def test_invalid_existing_file_is_preserved_with_recovery_error(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'local-secrets.json';path.write_text('{')
            with self.assertRaisesRegex(RuntimeError,'Restore'):load_or_create(path)
            self.assertEqual('{',path.read_text())
    def test_non_object_json_has_an_explicit_recovery_error(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'local-secrets.json';path.write_text('["demo","operator"]')
            with self.assertRaisesRegex(RuntimeError,'Restore'):load_or_create(path)

class EvidenceTests(unittest.TestCase):
    def test_empty_scrape_waits_and_live_target_passes(self):
        self.assertFalse(prometheus_target_up({'status':'success','data':{'result':[]}}))
        self.assertTrue(prometheus_target_up({'status':'success','data':{'result':[{'value':[0,'1']}]}}))
    def test_historical_sdk_spans_do_not_prove_current_export(self):
        def record(timestamp):
            return json.dumps({'resourceSpans':[{'scopeSpans':[{'scope':{'name':'Azure.Messaging.ServiceBus'},
                'spans':[{'startTimeUnixNano':str(timestamp)}]}]}]})
        self.assertEqual(set(),fresh_sdk_sources(record(9),10))
        self.assertEqual({'Azure.Messaging.ServiceBus'},fresh_sdk_sources(record(10),10))

unittest.main()
