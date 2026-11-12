#!/usr/bin/env python3
import json
import contextlib
import io
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch

import bench_matrix as matrix
from bench_matrix_report import aggregate


class BenchmarkValidation(unittest.TestCase):
    def reply(self, wire):
        listener=socket.socket()
        listener.bind(('127.0.0.1',0));listener.listen()
        port=listener.getsockname()[1]
        def serve():
            try:
                conn,_=listener.accept()
                with conn:
                    conn.recv(4096)
                    for byte in wire:conn.sendall(bytes([byte]))
            finally:listener.close()
        thread=threading.Thread(target=serve);thread.start()
        self.addCleanup(thread.join,3)
        return port

    def test_nested_fragmented_binary_responses(self):
        port=self.reply(b'*3\r\n$4\r\na\x00\r\n\r\n:7\r\n*-1\r\n')
        self.assertEqual(matrix.command(port,'MGET','a','b'),[b'a\x00\r\n',7,None])

    def test_truncated_bulk_is_rejected(self):
        port=self.reply(b'$5\r\nx')
        with self.assertRaisesRegex(RuntimeError,'truncated RESP payload'):
            matrix.command(port,'GET','key')

    def test_inner_runs_are_aggregated_by_work_and_time(self):
        row={'data':[{'throughput':[{'label':'SET','ops':100,'elapsed_ns':10_000_000,'ops_per_sec':10000}]},{'throughput':[{'label':'SET','ops':900,'elapsed_ns':100_000_000,'ops_per_sec':9000}]}]}
        item=aggregate(row)['SET']
        self.assertEqual(item['ops'],1000)
        self.assertAlmostEqual(item['ops_per_sec'],1000/0.11)

    def exercise_failure(self, corrupt):
        state={'State':{'Running':True,'OOMKilled':False,'ExitCode':0}}
        def docker(*args,**kwargs):
            return subprocess.CompletedProcess(args,0,json.dumps([state]) if args[0]=='inspect' else '', '')
        def client(argv,**kwargs):
            if corrupt:
                Path(argv[argv.index('-json')+1]).write_text(json.dumps({'throughput':[]}))
            return subprocess.CompletedProcess(argv,0 if corrupt else 2)
        with tempfile.TemporaryDirectory() as d:
            records=[]
            with patch.object(matrix,'start',return_value=(12345,1,['run'])),patch.object(matrix,'docker',side_effect=docker) as called,patch.object(matrix,'wait_idle',return_value={}),patch.object(matrix,'pressure',return_value={}),patch.object(matrix,'rss',return_value=1),patch.object(matrix,'cpu_seconds',return_value=0),patch.object(matrix.subprocess,'run',side_effect=client),patch.object(matrix,'command',side_effect=[10_000_000,[b'wrong']*1002]):
                with contextlib.redirect_stdout(io.StringIO()):
                    matrix.run_case(Path(d),records,{'current':'image'},'current','single',[0],[1],'default',{},dict(name='key',args=['-m','key']),0,Path('/client'))
            self.assertEqual(records[0]['status'],'failed')
            self.assertTrue(any(call.args[:3]==('rm','-f','-v') for call in called.call_args_list))
            self.assertTrue(records[0]['state']['Running'])
            return records[0]

    def test_failed_client_is_not_a_server_pass(self):
        self.assertIn('benchmark client failed',self.exercise_failure(False)['error'])

    def test_corrupt_values_fail_even_when_client_exits_zero(self):
        self.assertIn('KV value verification failed',self.exercise_failure(True)['error'])


if __name__=='__main__':unittest.main()
