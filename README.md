[![Home](https://img.shields.io/badge/%F0%9F%8F%A0-Pyjoyment-02a7a0)](https://github.com/pyjoyment/Pyjoyment)
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
[Pyjoyment](https://github.com/pyjoyment/Pyjoyment).

`Pyjo.Reactor.Trio` is a reactor of Pyjoyment on Trio, the async library of
structured concurrency, so that the servers of Pyjoyment, `daemon`, `prefork`
and `prethread`, and its user agent run along with the tasks of a program of
Trio.

The reactor is not implemented yet: this is the template of the distribution.

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

## Documentation

The API of the module and the examples are on
[Read the Docs](https://pyjoyment.readthedocs.io/projects/pyjo-reactor-trio/en/stable/).

## Status

A reactor of Pyjoyment in a distribution of its own, as
[Pyjo-Reactor-Twisted](https://github.com/pyjoyment/Pyjo-Reactor-Twisted),
[Pyjo-Reactor-Uvloop](https://github.com/pyjoyment/Pyjo-Reactor-Uvloop) and
[Pyjo-Reactor-Gevent](https://github.com/pyjoyment/Pyjo-Reactor-Gevent).
