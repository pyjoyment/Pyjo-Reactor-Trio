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
