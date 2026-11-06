[![Home](https://img.shields.io/badge/%F0%9F%8F%A0-Pyjoyment-02a7a0)](https://pyjoyment.dev/)
[![GitHub](https://img.shields.io/github/stars/pyjoyment/Pyjo-Reactor-Trio?style=flat&label=GitHub%20%E2%AD%90&logo=github)](https://github.com/pyjoyment/Pyjo-Reactor-Trio)
[![Documentation](https://img.shields.io/badge/docs-Read_the_Docs-blue?logo=readthedocs&logoColor=white)](https://pyjoyment.readthedocs.io/projects/pyjo-reactor-trio/en/stable/)
[![License](https://img.shields.io/github/license/pyjoyment/Pyjo-Reactor-Trio.svg)](https://github.com/pyjoyment/Pyjo-Reactor-Trio/blob/main/LICENSE)
[![PyPI](https://img.shields.io/pypi/v/Pyjo-Reactor-Trio?logo=pypi&logoColor=white)](https://pypi.org/project/Pyjo-Reactor-Trio/)
[![Tests](https://github.com/pyjoyment/Pyjo-Reactor-Trio/actions/workflows/tests.yaml/badge.svg?branch=main)](https://github.com/pyjoyment/Pyjo-Reactor-Trio/actions/workflows/tests.yaml)
[![Trunk Check](https://github.com/pyjoyment/Pyjo-Reactor-Trio/actions/workflows/trunk-check.yaml/badge.svg?branch=main)](https://github.com/pyjoyment/Pyjo-Reactor-Trio/actions/workflows/trunk-check.yaml)

# Pyjo-Reactor-Trio

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/pyjoyment/Pyjoyment/main/Pyjoyment/resources/public/pyjo/logo-white-4x.png">
    <img src="https://raw.githubusercontent.com/pyjoyment/Pyjoyment/main/Pyjoyment/resources/public/pyjo/logo-black-4x.png" alt="Pyjoyment" width="322">
  </picture>
</p>

Low level event reactor with [Trio](https://trio.readthedocs.io) support for
[Pyjoyment](https://pyjoyment.dev/).

`Pyjo.Reactor.Trio` is a reactor of Pyjoyment on Trio, the async library of
structured concurrency, so that the servers of Pyjoyment, `daemon`, `prefork`
and `prethread`, and its user agent run along with the tasks of a program of
Trio.

Inside a run of Trio, as of `trio.run`, the reactor watches its handles and
timers with tasks of that run, so a task of Trio awaits a promise of
Pyjoyment, and a coroutine of Pyjoyment awaits anything of Trio. Outside of
one, `Pyjo.IOLoop.start()` runs a run of Trio of the reactor itself, in the
guest mode of Trio, for as long as it runs.

## Installation

From [PyPI](https://pypi.org/project/Pyjo-Reactor-Trio/), with
[Pyjoyment](https://pypi.org/project/Pyjoyment/) and
[Trio](https://pypi.org/project/trio/), which it depends on:

```sh
pip install Pyjo-Reactor-Trio
```

or, in a project managed with [uv](https://docs.astral.sh/uv/):

```sh
uv add Pyjo-Reactor-Trio
```

The development version is installed from the Git repository:

```sh
pip install git+https://github.com/pyjoyment/Pyjo-Reactor-Trio
```

## Usage

Select the reactor with `PYJO_REACTOR` before `Pyjo.IOLoop` is loaded:

```sh
PYJO_REACTOR=Pyjo.Reactor.Trio python myapp.py daemon
PYJO_REACTOR=Pyjo.Reactor.Trio python myapp.py prefork
```

## Examples

`examples/hello_world_trio.py` runs `Pyjo.HelloWorld` on the server of
Pyjoyment on Trio until the signal `INT` or `TERM`, in a `with` block, which
closes the listen sockets of the server at its end:

```python
import os


# The reactor of Pyjoyment on Trio, selected before Pyjo.IOLoop is loaded
os.environ['PYJO_REACTOR'] = 'Pyjo.Reactor.Trio'

import Pyjo.HelloWorld  # noqa: E402
import Pyjo.Server.Daemon  # noqa: E402


# The server of Pyjoyment on Trio, until the signal INT or TERM, which closes its listen sockets at the end of the block
with Pyjo.Server.Daemon.new(app=Pyjo.HelloWorld.new(), listen=['http://*:3000']) as daemon:
    daemon.run()
```

```console
$ python examples/hello_world_trio.py
Web application available at http://127.0.0.1:3000
```

```console
$ curl http://127.0.0.1:3000/
Your Pyjo is working!
```

`examples/trio_nursery.py` is a program of Trio, whose run has the server of
Pyjoyment and two tasks of a nursery that request it, with a stream of Trio
and with the user agent of Pyjoyment, whose promise a task of Trio awaits:

```python
import os


# The reactor of Pyjoyment on Trio, selected before Pyjo.IOLoop is loaded
os.environ['PYJO_REACTOR'] = 'Pyjo.Reactor.Trio'

import Pyjo.HelloWorld  # noqa: E402
import Pyjo.Server.Daemon  # noqa: E402
import Pyjo.UserAgent  # noqa: E402

import trio  # noqa: E402


# A task with a stream of Trio, which reads the status line of the response
async def stream(port, results):
    async with await trio.open_tcp_stream('127.0.0.1', port) as stream:
        await stream.send_all(b'GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n')
        data = b''
        while b'\r\n' not in data:
            data += await stream.receive_some()
    results['trio'] = data.split(b'\r\n')[0].decode()


# A task with the user agent of Pyjoyment, which awaits the promise of its request
async def user_agent(port, results):
    with Pyjo.UserAgent.new() as ua:
        tx = await ua.get_p(f'http://127.0.0.1:{port}/')
    results['Pyjo.UserAgent'] = f'{tx.res.code} {tx.res.text}'


# The server of Pyjoyment in the run of Trio of the program, on a port that the system picks, with two tasks of a nursery
# that request it, and its listen sockets closed at the end of the block
async def main():
    results = {}
    with Pyjo.Server.Daemon.new(app=Pyjo.HelloWorld.new(), listen=['http://127.0.0.1'], silent=True) as daemon:
        port = daemon.start().ports()[0]
        async with trio.open_nursery() as nursery:
            nursery.start_soon(stream, port, results)
            nursery.start_soon(user_agent, port, results)
    for name in ('trio', 'Pyjo.UserAgent'):
        print(f'{name}: {results[name]}')


trio.run(main)
```

```console
$ python examples/trio_nursery.py
trio: HTTP/1.1 200 OK
Pyjo.UserAgent: 200 Your Pyjo is working!
```

`examples/loopback_trio.py` is a server of Pyjoyment and its user agent on
Trio, a blocking request, which runs the event loop until the response, and
the class of the reactor and the async library that its callbacks run in,
which Build checks with the wheel:

```python
import os


# The reactor of Pyjoyment on Trio, selected before Pyjo.IOLoop is loaded
os.environ['PYJO_REACTOR'] = 'Pyjo.Reactor.Trio'

import Pyjo.HelloWorld  # noqa: E402
import Pyjo.IOLoop  # noqa: E402
import Pyjo.Server.Daemon  # noqa: E402
import Pyjo.UserAgent  # noqa: E402

import sniffio  # noqa: E402


# The async library that a callback of the reactor runs in, as a library of Trio would detect it
libraries = []


def detect(loop):
    try:
        libraries.append(sniffio.current_async_library())
    except sniffio.AsyncLibraryNotFoundError:
        libraries.append('none')


# The server, on a port that the system picks, and the user agent, in the same event loop, which close their sockets at
# the end of the block
with (
    Pyjo.Server.Daemon.new(app=Pyjo.HelloWorld.new(), listen=['http://127.0.0.1'], silent=True) as daemon,
    Pyjo.UserAgent.new(ioloop=Pyjo.IOLoop.singleton) as ua,
):
    # A blocking request, which runs the event loop of the server on Trio until the response
    Pyjo.IOLoop.next_tick(detect)
    tx = ua.get(f'http://127.0.0.1:{daemon.start().ports()[0]}/')
    print(type(Pyjo.IOLoop.singleton.reactor).__name__, *libraries)
    print(tx.result.code, tx.result.text)
```

```console
$ python examples/loopback_trio.py
Pyjo_Reactor_Trio trio
200 Your Pyjo is working!
```

`examples/echo_client_trio.py` is the echo client of the
[tutorial of Trio](https://trio.readthedocs.io/en/stable/tutorial.html#an-echo-client),
with its sender and receiver in a nursery, and an echo server of
`Pyjo.IOLoop` in the same run of Trio. The sender waits for the echo of each
message, and closes its side of the connection after the last one, which the
server closes the connection at:

```python
import os


# The reactor of Pyjoyment on Trio, selected before Pyjo.IOLoop is loaded
os.environ['PYJO_REACTOR'] = 'Pyjo.Reactor.Trio'

import Pyjo.IOLoop  # noqa: E402
import Pyjo.Promise  # noqa: E402

from itertools import count  # noqa: E402

import trio  # noqa: E402


MESSAGES = [b'async can sometimes be confusing,', b'but I believe in you!']

CONNECTION_COUNTER = count()


# The echo server of Pyjoyment, which writes back what it reads, until the client closes the connection
def echo_server(closed):
    def accept(loop, stream, id):
        ident = next(CONNECTION_COUNTER)
        print(f'echo_server {ident}: started')

        @stream.on
        def read(stream, data):
            print(f'echo_server {ident}: received data {data!r}')
            stream.write(data)

        @stream.on
        def close(stream):
            print(f'echo_server {ident}: connection closed')
            closed.resolve()

    return Pyjo.IOLoop.server(accept, address='127.0.0.1')


# The echo client of the tutorial of Trio, whose sender waits for the echo of each message before the next one and
# closes its side of the connection after the last one, as the stream of Pyjoyment closes at the end of the data
async def sender(client_stream, echoed):
    print('sender: started!')
    for data in MESSAGES:
        print(f'sender: sending {data!r}')
        await client_stream.send_all(data)
        await echoed.receive()
    await client_stream.send_eof()


async def receiver(client_stream, echoed):
    print('receiver: started!')
    async for data in client_stream:
        print(f'receiver: got data {data!r}')
        await echoed.send(data)
    print('receiver: connection closed')


# The server of Pyjoyment in the run of Trio of the program, on a port that the system picks, and the client in a
# nursery, until the server closed the connection, and the listen socket of the server removed at the end
async def parent():
    closed = Pyjo.Promise.new()
    id = echo_server(closed)
    port = Pyjo.IOLoop.acceptor(id).port
    print(f'parent: connecting to 127.0.0.1:{port}')
    client_stream = await trio.open_tcp_stream('127.0.0.1', port)
    send_echoed, receive_echoed = trio.open_memory_channel(0)
    async with client_stream, send_echoed, receive_echoed:
        async with trio.open_nursery() as nursery:
            print('parent: spawning sender...')
            nursery.start_soon(sender, client_stream, receive_echoed)

            print('parent: spawning receiver...')
            nursery.start_soon(receiver, client_stream, send_echoed)
    await closed
    Pyjo.IOLoop.remove(id)


trio.run(parent)
```

The tasks run concurrently, so the order of their lines differs between the
runs:

```console
$ python examples/echo_client_trio.py
parent: connecting to 127.0.0.1:60341
parent: spawning sender...
parent: spawning receiver...
echo_server 0: started
receiver: started!
sender: started!
sender: sending b'async can sometimes be confusing,'
echo_server 0: received data b'async can sometimes be confusing,'
receiver: got data b'async can sometimes be confusing,'
sender: sending b'but I believe in you!'
echo_server 0: received data b'but I believe in you!'
receiver: got data b'but I believe in you!'
echo_server 0: connection closed
receiver: connection closed
```

`examples/echo_server_trio.py` is the echo server of the
[tutorial of Trio](https://trio.readthedocs.io/en/stable/tutorial.html#an-echo-server),
with `trio.serve_tcp` in a nursery, and an echo client of `Pyjo.IOLoop`, whose
promise a task of Trio awaits:

```python
import os


# The reactor of Pyjoyment on Trio, selected before Pyjo.IOLoop is loaded
os.environ['PYJO_REACTOR'] = 'Pyjo.Reactor.Trio'

import Pyjo.IOLoop  # noqa: E402
import Pyjo.Promise  # noqa: E402

from functools import partial  # noqa: E402
from itertools import count  # noqa: E402

import trio  # noqa: E402


MESSAGES = [b'async can sometimes be confusing,', b'but I believe in you!']

CONNECTION_COUNTER = count()


# The echo server of the tutorial of Trio, until the client closes the connection
async def echo_server(server_stream):
    ident = next(CONNECTION_COUNTER)
    print(f'echo_server {ident}: started')
    try:
        async for data in server_stream:
            print(f'echo_server {ident}: received data {data!r}')
            await server_stream.send_all(data)
        print(f'echo_server {ident}: connection closed')
    except Exception as exc:
        print(f'echo_server {ident}: crashed: {exc!r}')


# The echo client of Pyjoyment, which writes each message after the echo of the one before and closes the connection
# after the last one, and its promise, which a task of Trio awaits
def echo_client(port):
    promise = Pyjo.Promise.new()
    messages = list(MESSAGES)
    echo = b''

    @Pyjo.IOLoop.client(address='127.0.0.1', port=port)
    def client(loop, err, stream):
        if err is not None:
            return promise.reject(err)

        def send():
            print(f'client: sending {messages[0]!r}')
            stream.write(messages[0])

        @stream.on
        def read(stream, data):
            nonlocal echo
            echo += data
            if len(echo) < len(messages[0]):
                return
            print(f'client: got data {echo!r}')
            echo = b''
            messages.pop(0)
            if messages:
                send()
            else:
                stream.close_gracefully()

        @stream.on
        def close(stream):
            print('client: connection closed')
            promise.resolve()

        send()

    return promise


# The server of Trio in a nursery, on a port that the system picks, with its connections in a nursery of their own, so
# that its listen socket closes after the client and the echo server ends with the connection
async def main():
    async with trio.open_nursery() as handler_nursery:
        async with trio.open_nursery() as nursery:
            listeners = await nursery.start(
                partial(trio.serve_tcp, echo_server, 0, host='127.0.0.1', handler_nursery=handler_nursery)
            )
            await echo_client(listeners[0].socket.getsockname()[1])
            nursery.cancel_scope.cancel()


trio.run(main)
```

```console
$ python examples/echo_server_trio.py
echo_server 0: started
client: sending b'async can sometimes be confusing,'
echo_server 0: received data b'async can sometimes be confusing,'
client: got data b'async can sometimes be confusing,'
client: sending b'but I believe in you!'
echo_server 0: received data b'but I believe in you!'
client: got data b'but I believe in you!'
client: connection closed
echo_server 0: connection closed
```

## Documentation

The API of the module and the examples are on
[Read the Docs](https://pyjoyment.readthedocs.io/projects/pyjo-reactor-trio/en/stable/).

## Status

A reactor of Pyjoyment in a distribution of its own, as
[Pyjo-Reactor-Twisted](https://github.com/pyjoyment/Pyjo-Reactor-Twisted),
[Pyjo-Reactor-Uvloop](https://github.com/pyjoyment/Pyjo-Reactor-Uvloop) and
[Pyjo-Reactor-Gevent](https://github.com/pyjoyment/Pyjo-Reactor-Gevent).
