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
