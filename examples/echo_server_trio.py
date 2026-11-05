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
