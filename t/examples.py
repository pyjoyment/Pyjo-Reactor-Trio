from Pyjo.Test import *

import http.client
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time


# The examples, on the port 3000 replaced by a free one, each in a process of its own
EXAMPLES = os.path.join(os.path.dirname(__file__), '..', 'examples')


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def get(port, deadline):
    while True:
        try:
            conn = http.client.HTTPConnection('127.0.0.1', port, timeout=5)
            try:
                conn.request('GET', '/')
                res = conn.getresponse()
                return res.status, res.read()
            finally:
                conn.close()
        except OSError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.1)


with subtest('hello_world_trio.py'), tempfile.TemporaryDirectory() as dir:
    with open(os.path.join(EXAMPLES, 'hello_world_trio.py')) as file:
        source = file.read()
    port = free_port()
    script = os.path.join(dir, 'hello_world_trio.py')
    with open(script, 'w') as file:
        file.write(source.replace('3000', str(port)))

    process = subprocess.Popen([sys.executable, script], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        status, body = get(port, time.monotonic() + 30)
        is_ok(status, 200, 'right status')
        is_ok(body, b'Your Pyjo is working!', 'right content')
    finally:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        output = process.stdout.read()
        process.stdout.close()
    is_ok(process.returncode, 0, 'stopped by INT')
    like_ok(output, rf'Web application available at http://127\.0\.0\.1:{port}', 'listening')

with subtest('trio_nursery.py'):
    result = subprocess.run(
        [sys.executable, os.path.join(EXAMPLES, 'trio_nursery.py')], capture_output=True, text=True, timeout=60
    )
    is_ok(
        result.stdout,
        'trio: HTTP/1.1 200 OK\nPyjo.UserAgent: 200 Your Pyjo is working!\n',
        'a stream of Trio and the user agent of Pyjoyment in the run of Trio of the program',
    )
    is_ok(result.stderr, '', 'nothing on the standard error')
    is_ok(result.returncode, 0, 'the server and the user agent closed at the end')

# As the workflow Build runs it with the wheel: its server listens on a port that the system picks, so it needs no free
# port of the tests
with subtest('loopback_trio.py'):
    result = subprocess.run(
        [sys.executable, os.path.join(EXAMPLES, 'loopback_trio.py')], capture_output=True, text=True, timeout=60
    )
    is_ok(result.stdout, 'Pyjo_Reactor_Trio trio\n200 Your Pyjo is working!\n', 'the response on Trio')
    is_ok(result.stderr, '', 'nothing on the standard error')
    is_ok(result.returncode, 0, 'the server and the user agent closed at the end')


# The lines of each task, by the name before the colon, as the tasks and the callbacks of Pyjoyment run concurrently, in
# an order of their lines that differs between the runs
def lines_of(output):
    lines = {}
    for line in output.splitlines():
        lines.setdefault(line.split(':')[0], []).append(line)
    return lines


with subtest('echo_client_trio.py'):
    result = subprocess.run(
        [sys.executable, os.path.join(EXAMPLES, 'echo_client_trio.py')], capture_output=True, text=True, timeout=60
    )
    lines = lines_of(result.stdout)
    is_ok(sorted(lines), ['echo_server 0', 'parent', 'receiver', 'sender'], 'the lines of the tasks')
    like_ok(lines['parent'][0], r'^parent: connecting to 127\.0\.0\.1:\d+$', 'the client connects')
    is_ok(lines['parent'][1:], ['parent: spawning sender...', 'parent: spawning receiver...'], 'the client spawns')
    is_ok(
        lines['sender'],
        [
            'sender: started!',
            "sender: sending b'async can sometimes be confusing,'",
            "sender: sending b'but I believe in you!'",
        ],
        'the sender of Trio',
    )
    is_ok(
        lines['receiver'],
        [
            'receiver: started!',
            "receiver: got data b'async can sometimes be confusing,'",
            "receiver: got data b'but I believe in you!'",
            'receiver: connection closed',
        ],
        'the receiver of Trio',
    )
    is_ok(
        lines['echo_server 0'],
        [
            'echo_server 0: started',
            "echo_server 0: received data b'async can sometimes be confusing,'",
            "echo_server 0: received data b'but I believe in you!'",
            'echo_server 0: connection closed',
        ],
        'the echo server of Pyjoyment',
    )
    is_ok(result.stderr, '', 'nothing on the standard error')
    is_ok(result.returncode, 0, 'the server and the client closed at the end')

with subtest('echo_server_trio.py'):
    result = subprocess.run(
        [sys.executable, os.path.join(EXAMPLES, 'echo_server_trio.py')], capture_output=True, text=True, timeout=60
    )
    lines = lines_of(result.stdout)
    is_ok(sorted(lines), ['client', 'echo_server 0'], 'the lines of the tasks')
    is_ok(
        lines['client'],
        [
            "client: sending b'async can sometimes be confusing,'",
            "client: got data b'async can sometimes be confusing,'",
            "client: sending b'but I believe in you!'",
            "client: got data b'but I believe in you!'",
            'client: connection closed',
        ],
        'the echo client of Pyjoyment',
    )
    is_ok(
        lines['echo_server 0'],
        [
            'echo_server 0: started',
            "echo_server 0: received data b'async can sometimes be confusing,'",
            "echo_server 0: received data b'but I believe in you!'",
            'echo_server 0: connection closed',
        ],
        'the echo server of Trio',
    )
    is_ok(result.stderr, '', 'nothing on the standard error')
    is_ok(result.returncode, 0, 'the server and the client closed at the end')

done_testing()
