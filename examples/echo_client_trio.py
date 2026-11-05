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
