import os


# Unlike Mojolicious: the reactor of the test is selected before Pyjo.IOLoop is loaded, as Mojo::Reactor->detect prefers
# Mojo::Reactor::EV where it is installed, and Pyjo.Reactor.detect() prefers Pyjo.Reactor.Asyncio
os.environ['PYJO_REACTOR'] = 'Pyjo.Reactor.Trio'

import Pyjo.Reactor  # noqa: E402
import Pyjo.Reactor.Trio  # noqa: E402

from Pyjo.Loader import load_class  # noqa: E402
from Pyjo.Test import *  # noqa: E402

import gc  # noqa: E402
import signal  # noqa: E402
import socket  # noqa: E402
import subprocess  # noqa: E402
import sys  # noqa: E402
import threading  # noqa: E402
import time  # noqa: E402

import sniffio  # noqa: E402
import trio  # noqa: E402


def eval_(cb):
    try:
        cb()
    except Exception as e:
        return e


# Shared between subtests, since I/O watchers and timers keep firing across them
readable = writable = 0


def rw_cb(reactor, is_writable):
    global readable, writable
    if is_writable:
        writable += 1
    else:
        readable += 1


with subtest('Instantiation'):
    reactor = Pyjo.Reactor.Trio.new()
    is_ok(type(reactor), Pyjo.Reactor.Trio.object, 'right object')
    is_ok(type(Pyjo.Reactor.Trio.new()), Pyjo.Reactor.Trio.object, 'right object')
    del reactor
    is_ok(type(Pyjo.Reactor.Trio.new()), Pyjo.Reactor.Trio.object, 'right object')
    ok(not load_class('Pyjo.IOLoop'), 'use Pyjo.IOLoop')
    reactor = Pyjo.IOLoop.singleton.reactor
    is_ok(type(reactor), Pyjo.Reactor.Trio.object, 'right object')

with subtest('Make sure it stops automatically when not watching for events'):
    triggered = 0

    def triggered_cb(loop):
        global triggered
        triggered += 1

    Pyjo.IOLoop.next_tick(triggered_cb)
    Pyjo.IOLoop.start()
    ok(triggered, 'reactor waited for one event')
    started = time.time()
    Pyjo.IOLoop.start()
    Pyjo.IOLoop.one_tick()
    ok(time.time() < (started + 10), 'stopped automatically')

with subtest('Listen'):
    listen = socket.create_server(('127.0.0.1', 0), backlog=5)
    port = listen.getsockname()[1]
    reactor.io(listen, rw_cb).watch(listen, False, False).watch(listen, True, True)
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(not readable, 'handle is not readable')
    ok(not writable, 'handle is not writable')

with subtest('Connect'):
    client = socket.create_connection(('127.0.0.1', port))
    reactor.timer(1, lambda reactor: reactor.stop())
    reactor.start()
    ok(readable, 'handle is readable')
    ok(not writable, 'handle is not writable')

with subtest('Accept'):
    server = listen.accept()[0]
    ok(reactor.remove(listen), 'removed')
    ok(not reactor.remove(listen), 'not removed again')
    readable = writable = 0
    reactor.io(client, rw_cb)
    reactor.again(reactor.timer(0.025, lambda reactor: reactor.stop()))
    reactor.start()
    ok(not readable, 'handle is not readable')
    ok(writable, 'handle is writable')
    client.sendall(b'hello!\n')
    time.sleep(1)
    ok(reactor.remove(client), 'removed')
    readable = writable = 0
    reactor.io(server, rw_cb)
    reactor.watch(server, True, False)
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(readable, 'handle is readable')
    ok(not writable, 'handle is not writable')
    readable = writable = 0
    reactor.watch(server, True, True)
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(readable, 'handle is readable')
    ok(writable, 'handle is writable')
    readable = writable = 0
    reactor.watch(server, False, False)
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(not readable, 'handle is not readable')
    ok(not writable, 'handle is not writable')
    readable = writable = 0
    reactor.watch(server, True, False)
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(readable, 'handle is readable')
    ok(not writable, 'handle is not writable')
    readable = writable = 0
    reactor.io(server, rw_cb)
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(readable, 'handle is readable')
    ok(writable, 'handle is writable')

with subtest('Timers'):
    timer = recurring = 0

    def timer_cb(reactor):
        global timer
        timer += 1

    def recurring_cb(reactor):
        global recurring
        recurring += 1

    reactor.timer(0, timer_cb)
    ok(reactor.remove(reactor.timer(0, timer_cb)), 'removed')
    id = reactor.recurring(0, recurring_cb)
    readable = writable = 0
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(readable, 'handle is readable again')
    ok(writable, 'handle is writable again')
    ok(timer, 'timer was triggered')
    ok(recurring, 'recurring was triggered')
    done = False
    readable = writable = timer = recurring = 0

    def done_cb(reactor):
        global done
        done = reactor.is_running

    reactor.timer(0.025, done_cb)
    while not done:
        reactor.one_tick()
    ok(readable, 'handle is readable again')
    ok(writable, 'handle is writable again')
    ok(not timer, 'timer was not triggered')
    ok(recurring, 'recurring was triggered again')
    readable = writable = timer = recurring = 0
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(readable, 'handle is readable again')
    ok(writable, 'handle is writable again')
    ok(not timer, 'timer was not triggered')
    ok(recurring, 'recurring was triggered again')
    ok(reactor.remove(id), 'removed')
    ok(not reactor.remove(id), 'not removed again')
    readable = writable = timer = recurring = 0
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(readable, 'handle is readable again')
    ok(writable, 'handle is writable again')
    ok(not timer, 'timer was not triggered')
    ok(not recurring, 'recurring was not triggered again')
    readable = writable = timer = recurring = 0
    next_tick = 0

    def next_tick_cb(reactor):
        global next_tick
        next_tick += 1

    is_ok(reactor.next_tick(next_tick_cb), None, 'returned None')
    id = reactor.recurring(0, recurring_cb)
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(readable, 'handle is readable again')
    ok(writable, 'handle is writable again')
    ok(not timer, 'timer was not triggered')
    ok(recurring, 'recurring was triggered again')
    ok(next_tick, 'next tick was triggered')


with subtest('Reset'):

    def reset_failed_cb(reactor):
        raise Exception('Reset failed')

    reactor.next_tick(reset_failed_cb)
    reactor.reset()
    readable = writable = recurring = 0
    reactor.next_tick(lambda reactor: reactor.stop())
    reactor.start()
    ok(not readable, 'io event was not triggered again')
    ok(not writable, 'io event was not triggered again')
    ok(not recurring, 'recurring was not triggered again')
    reactor2 = Pyjo.Reactor.Trio.new()
    is_ok(type(reactor2), Pyjo.Reactor.Trio.object, 'right object')

with subtest('Ordered next_tick'):
    result = []
    for i in range(1, 11):
        reactor.next_tick(lambda reactor, i=i: result.append(i))
    reactor.start()
    is_deeply_ok(result, list(range(1, 11)), 'right result')

with subtest('Reset while watchers are active'):
    writable = 0

    def reset_cb(reactor, is_writable):
        global writable
        writable += 1
        reactor.reset()

    for handle in (client, server):
        reactor.io(handle, reset_cb).watch(handle, False, True)
    reactor.start()
    is_ok(writable, 1, 'only one handle was writable')
    client.close()
    server.close()

with subtest('Concurrent reactors'):
    timer = 0
    reactor.recurring(0, timer_cb)
    timer2 = 0

    def timer2_cb(reactor):
        global timer2
        timer2 += 1

    reactor2.recurring(0, timer2_cb)
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(timer, 'timer was triggered')
    ok(not timer2, 'timer was not triggered')
    timer = timer2 = 0
    reactor2.timer(0.025, lambda reactor: reactor.stop())
    reactor2.start()
    ok(not timer, 'timer was not triggered')
    ok(timer2, 'timer was triggered')
    timer = timer2 = 0
    reactor.timer(0.025, lambda reactor: reactor.stop())
    reactor.start()
    ok(timer, 'timer was triggered')
    ok(not timer2, 'timer was not triggered')
    timer = timer2 = 0
    reactor2.timer(0.025, lambda reactor: reactor.stop())
    reactor2.start()
    ok(not timer, 'timer was not triggered')
    ok(timer2, 'timer was triggered')
    reactor.reset()

with subtest('Restart timer'):
    single = pair = last = 0

    def single_cb(reactor):
        global single
        single += 1

    def one_cb(reactor):
        global pair, last
        if single and pair:
            last += 1
        pair += 1
        reactor.stop() if pair > 1 else reactor.again(two)

    def two_cb(reactor):
        global pair, last
        if single and pair:
            last += 1
        pair += 1
        reactor.stop() if pair > 1 else reactor.again(one)

    reactor.timer(0.025, single_cb)
    one = reactor.timer(0.025, one_cb)
    two = reactor.timer(0.025, two_cb)
    reactor.start()
    is_ok(pair, 2, 'timer pair was triggered')
    ok(single, 'single timer was triggered')
    ok(last, 'timers were triggered in the right order')

with subtest('Reset timer'):
    before = time.monotonic()
    after = None
    again = 0

    def after_cb(reactor):
        global after
        after = time.monotonic()

    def again_cb(reactor):
        global again
        again += 1
        if again > 3:
            reactor.remove(two)

    def restart_cb(reactor):
        reactor.again(one, 0.025)
        reactor.again(two, 0.025)

    one = reactor.timer(300, after_cb)
    two = reactor.recurring(300, again_cb)
    reactor.timer(0.025, restart_cb)
    reactor.start()
    ok(after, 'timer was triggered')
    ok((after - before) < 200, 'less than 200 seconds')
    is_ok(again, 4, 'recurring timer triggered four times')

with subtest('Restart inactive timer'):
    id = reactor.timer(0, lambda reactor: None)
    ok(reactor.remove(id), 'removed')
    err = eval_(lambda: reactor.again(id))
    like_ok(str(err), r'Timer not active', 'right error')

with subtest('Change inactive I/O watcher'):
    ok(not reactor.remove(listen), 'not removed again')
    err = eval_(lambda: reactor.watch(listen, True, True))
    like_ok(str(err), r'I/O watcher not active', 'right error')
    listen.close()

with subtest('Error'):
    err = None

    def error_cb(reactor, e):
        global err
        reactor.stop()
        err = e

    def die_cb(reactor):
        raise Exception('works!\n')

    reactor.unsubscribe('error').on('error', error_cb)
    reactor.timer(0, die_cb)
    reactor.start()
    like_ok(str(err), r'works!', 'right error')

    # Not in Mojolicious: the arguments of a callback, which Perl does not check
    reactor.timer(0, lambda: None)
    reactor.start()
    like_ok(
        str(err),
        r'takes 0 positional arguments but 1 was given: the callbacks of a reactor get the reactor first',
        'hint',
    )

with subtest('Reset events'):
    reactor.on('error', lambda reactor, err: None)
    ok(reactor.has_subscribers('error'), 'has subscribers')
    reactor.reset()
    ok(not reactor.has_subscribers('error'), 'no subscribers')

with subtest('Recursion'):
    timer = 0

    def recursion_cb(reactor):
        global timer
        timer += 1
        reactor.one_tick()

    reactor = Pyjo.Reactor.Trio.new()
    reactor.timer(0, recursion_cb)
    reactor.one_tick()
    is_ok(timer, 1, 'timer was triggered once')

with subtest('Detection'):
    is_ok(Pyjo.Reactor.object.detect(), 'Pyjo.Reactor.Trio', 'right class')

with subtest('Reactor in control'):
    is_ok(type(Pyjo.IOLoop.singleton.reactor), Pyjo.Reactor.Trio.object, 'right object')
    ok(not Pyjo.IOLoop.is_running(), 'loop is not running')
    buffer = b''
    server_err = server_running = client_err = client_running = None

    def server_cb(loop, stream, cid):
        global server_err, server_running
        stream.write(b'test', lambda stream: stream.write(b'321'))
        server_running = Pyjo.IOLoop.is_running()
        server_err = eval_(lambda: Pyjo.IOLoop.start())

    id = Pyjo.IOLoop.server(server_cb, address='127.0.0.1')
    port = Pyjo.IOLoop.acceptor(id).port

    def client_cb(loop, err, stream):
        global client_err, client_running

        def read_cb(stream, chunk):
            global buffer
            buffer += chunk
            if buffer != b'test321':
                return
            Pyjo.IOLoop.singleton.reactor.stop()

        stream.on('read', read_cb)
        client_running = Pyjo.IOLoop.is_running()
        client_err = eval_(lambda: Pyjo.IOLoop.start())

    Pyjo.IOLoop.client(client_cb, port=port)
    Pyjo.IOLoop.singleton.reactor.start()
    ok(not Pyjo.IOLoop.is_running(), 'loop is not running')
    like_ok(str(server_err), r'^Pyjo.IOLoop already running', 'right error')
    like_ok(str(client_err), r'^Pyjo.IOLoop already running', 'right error')
    ok(server_running, 'loop is running')
    ok(client_running, 'loop is running')

# Left out from Mojolicious: the abstract methods of Mojo::Reactor, as reactor_ev.t does not have them

# Not in Mojolicious: a callback of next_tick from a signal handler, which Python runs in the middle of what the reactor
# does, as the servers stop gracefully, with enough timers that the handler interrupts the reactor that looks for the
# first of them
with subtest('next_tick from a signal handler'):
    if not hasattr(signal, 'setitimer'):
        plan(skip_all='signal.setitimer is required')
    reactor = Pyjo.Reactor.Trio.new()
    for _ in range(100000):
        reactor.timer(10, lambda reactor: None)
    ticks = []
    handler = signal.signal(signal.SIGALRM, lambda signum, frame: reactor.next_tick(lambda reactor: ticks.append(1)))
    signal.setitimer(signal.ITIMER_REAL, 0.001, 0.001)
    reactor.timer(0.3, lambda reactor: reactor.stop())
    try:
        err = eval_(lambda: reactor.start())
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, handler)
    is_ok(err, None, 'no error')
    ok(ticks, 'callbacks of the signal handler')
    reactor.reset()

# Not in Mojolicious: a signal handler in the middle of the start of the own run, which Trio binds to the thread before it
# has its system nursery, where the reactor would take it for the run of the application, as PyPy runs the handler of
# the test above there; a trace function runs next_tick there, as the handler would
with subtest('next_tick in the start of the own run'):
    reactor = Pyjo.Reactor.Trio.new()
    ticks = []

    def trace(frame, event, arg):
        if event == 'call' and frame.f_code.co_name == 'init' and 'trio' in frame.f_code.co_filename and not ticks:
            ticks.append('init')
            reactor.next_tick(lambda reactor: ticks.append('next_tick'))

    reactor.timer(0.01, lambda reactor: None)
    sys.settrace(trace)
    try:
        err = eval_(lambda: reactor.start())
    finally:
        sys.settrace(None)
    is_ok(err, None, 'no error')
    is_deeply_ok(ticks, ['init', 'next_tick'], 'callback of next_tick')
    reactor.close()

# A finalizer that the garbage collector runs while the run starts, as PyPy does, removes a handle
with subtest('Handle removed in the start of the own run'):
    reactor = Pyjo.Reactor.Trio.new()
    first, first_peer = socket.socketpair()
    second, second_peer = socket.socketpair()
    second_peer.sendall(b'x')
    got = []

    def trace(frame, event, arg):
        if event == 'call' and frame.f_code.co_name == '_watch' and not got:
            got.append('remove')
            reactor.remove(second)

    reactor.io(first, lambda reactor, writable: got.append('first')).watch(first, True, False)
    reactor.io(second, lambda reactor, writable: got.append('second')).watch(second, True, False)
    reactor.timer(0.05, lambda reactor: reactor.stop())
    sys.settrace(trace)
    try:
        err = eval_(lambda: reactor.start())
    finally:
        sys.settrace(None)
    is_ok(err, None, 'no error')
    is_deeply_ok(got, ['remove'], 'removed handle is not watched')
    reactor.reset()
    reactor.close()
    for handle in (first, first_peer, second, second_peer):
        handle.close()

# Not in Mojolicious: the reactor in a run of Trio of the application, which watches its handles and timers with tasks of
# its own, where start returns at once, and a callback runs in Trio
with subtest('In a run of Trio'):
    got = []
    first, peer = socket.socketpair()

    async def application():
        reactor = Pyjo.Reactor.Trio.new()
        got.append(reactor.is_running)
        reactor.timer(0.01, lambda reactor: got.append(('timer', sniffio.current_async_library())))
        reactor.io(first, lambda reactor, writable: got.append('readable') or first.recv(1)).watch(first, True, False)
        reactor.start()
        got.append('start returned')
        peer.sendall(b'x')
        await trio.sleep(0.1)
        reactor.remove(first)
        return reactor

    reactor = trio.run(application)
    is_deeply_ok(got, [True, 'start returned', 'readable', ('timer', 'trio')], 'events in the run of Trio')
    ok(not reactor.is_running, 'not running after the run')
    first.close()
    peer.close()

with subtest('Coroutines in a run of Trio'):
    got = []

    async def coroutine():
        await trio.sleep(0.01)
        return 'value'

    async def application():
        reactor = Pyjo.Reactor.Trio.new()
        reactor.run_coroutine(coroutine(), lambda value: got.append(value), lambda reason: got.append(reason))
        await trio.sleep(0.1)

    trio.run(application)
    is_deeply_ok(got, ['value'], 'coroutine is a task of the run')

# Not in Mojolicious: a run of Trio of the reactor itself, in the guest mode of Trio, which ends when start and one_tick
# return, unless coroutines that it runs are not done, which close cancels
with subtest('Coroutines of the own run'):
    got = []
    reactor = Pyjo.Reactor.Trio.new()

    async def coroutine():
        await trio.sleep(0.01)
        got.append(sniffio.current_async_library())
        return 'value'

    reactor.run_coroutine(coroutine(), lambda value: got.append(value), lambda reason: got.append(reason))
    reactor.start()
    is_deeply_ok(got, ['trio', 'value'], 'start waits for the coroutine')

    reactor.run_coroutine(trio.sleep_forever(), lambda value: got.append(value), lambda reason: got.append(reason))
    reactor.timer(0.01, lambda reactor: None)
    reactor.one_tick()
    ok(reactor._run is not None, 'run kept for the coroutine')
    reactor.close()
    isa_ok(got[-1], trio.Cancelled, 'coroutine cancelled by close')
    ok(reactor._run is None, 'run ended')
    is_ok(trio.run(trio.sleep, 0), None, 'thread can run Trio again')

# Not in Mojolicious: the cancellation of a coroutine where it waits, with the promise that it waits for, in a run of
# Trio and in the own run of the reactor
with subtest('Cancellation of a coroutine'):
    import Pyjo.Promise

    got = []

    def waiting():
        return Pyjo.Promise.new(cancel=lambda: got.append('promise cancelled'))

    async def coroutine(promise):
        try:
            await promise
        finally:
            got.append('finally')

    async def application():
        reactor = Pyjo.Reactor.Trio.new()
        cancel = reactor.run_coroutine(coroutine(waiting()), got.append, lambda reason: got.append(type(reason)))
        await trio.sleep(0.01)
        cancel()
        await trio.sleep(0.01)
        cancel()

    trio.run(application)
    is_deeply_ok(got, ['promise cancelled', 'finally', trio.Cancelled], 'in a run of Trio')

    got.clear()
    reactor = Pyjo.Reactor.Trio.new()
    cancel = reactor.run_coroutine(coroutine(waiting()), got.append, lambda reason: got.append(type(reason)))
    reactor.timer(0.01, lambda reactor: cancel())
    reactor.start()
    is_deeply_ok(got, ['promise cancelled', 'finally', trio.Cancelled], 'in the own run, between its guest ticks')
    reactor.close()

with subtest('Order of the events'):
    orders = []
    for i in range(20):
        reactor = Pyjo.Reactor.Trio.new()
        a, a_peer = socket.socketpair()
        b, b_peer = socket.socketpair()
        a_peer.sendall(b'x')
        b_peer.sendall(b'x')
        got = []

        def cb(reactor, writable, name, got=got):
            got.append(name)
            if name == 'a':
                reactor.next_tick(lambda reactor, got=got: got.append('next_tick'))

        handles = [('a', a), ('b', b)] if i % 2 else [('b', b), ('a', a)]
        for name, handle in handles:
            reactor.io(handle, lambda reactor, writable, name=name: cb(reactor, writable, name))
            reactor.watch(handle, True, False)
        reactor.one_tick()
        orders.append(got == [name for name, handle in handles] + ['next_tick'])
        reactor.remove(a)
        reactor.remove(b)
        reactor.close()
        for handle in (a, a_peer, b, b_peer):
            handle.close()
    is_deeply_ok(orders, [True] * 20, 'handles in the order in which they were added, then the timers')

# Not in Mojolicious: a guest tick of Trio runs its batches until it waits for I/O, where no round comes after stop or
# after the event of one_tick
with subtest('One round after stop'):
    count = []
    reactor = Pyjo.Reactor.Trio.new()
    reactor.recurring(0, lambda reactor: count.append(1) or reactor.stop())
    reactor.start()
    is_ok(len(count), 1, 'one event before stop')
    reactor.one_tick()
    is_ok(len(count), 2, 'one event of one_tick')
    reactor.reset()
    reactor.close()

with subtest('Watcher removed before it started'):
    first, peer = socket.socketpair()
    reactor = Pyjo.Reactor.Trio.new()
    readable = 0

    def readable_cb(reactor, writable):
        global readable
        readable += 1
        first.recv(1)
        reactor.stop()

    def again_cb(reactor):
        for _ in range(20):
            reactor.remove(first)
            reactor.io(first, readable_cb).watch(first, True, False)
        peer.sendall(b'x')

    reactor.io(first, readable_cb).watch(first, True, False)
    reactor.timer(0, again_cb)
    reactor.timer(5, lambda reactor: reactor.stop())
    err = eval_(lambda: reactor.start())
    is_ok(err, None, 'no error')
    is_ok(readable, 1, 'handle is readable')
    reactor.reset()
    reactor.close()
    first.close()
    peer.close()

with subtest('Watcher removed between the guest ticks'):
    first, peer = socket.socketpair()
    reactor = Pyjo.Reactor.Trio.new()
    readable = 0

    def readable_cb(reactor, writable):
        global readable
        readable += 1

    reactor.run_coroutine(trio.sleep_forever(), lambda value: None, lambda reason: None)
    reactor.io(first, readable_cb).watch(first, True, False)
    reactor.timer(0.01, lambda reactor: None)
    reactor.one_tick()

    # The thread of I/O of Trio has the event, which the next guest tick takes from the queue
    peer.sendall(b'x')
    deadline = time.monotonic() + 5
    while reactor._run.queue.empty() and time.monotonic() < deadline:
        time.sleep(0.001)
    ok(not reactor._run.queue.empty(), 'event is waiting')
    reactor.remove(first)
    reactor.timer(0.01, lambda reactor: None)
    err = eval_(lambda: reactor.one_tick())
    is_ok(err, None, 'no error')
    is_ok(readable, 0, 'removed handle is not readable')
    reactor.close()
    first.close()
    peer.close()

with subtest('Handle added while the own run ends'):
    first, peer = socket.socketpair()
    reactor = Pyjo.Reactor.Trio.new()
    errors = []
    readable = []

    def readable_cb(reactor, writable):
        readable.append(writable)
        first.recv(1)
        reactor.stop()

    # A cleanup that the end of the run runs with run_sync_soon, after the system tasks, as _drain does
    def add(hops=0):
        if not trio._core._run.GLOBAL_RUN_CONTEXT.runner.system_nursery._closed and hops < 100:
            trio.lowlevel.current_trio_token().run_sync_soon(add, hops + 1)
            return
        errors.append(eval_(lambda: reactor.io(first, readable_cb).watch(first, True, False)))

    def rejected(reason):
        trio.lowlevel.current_trio_token().run_sync_soon(add)

    reactor.run_coroutine(trio.sleep_forever(), lambda value: None, rejected)
    reactor.timer(0.01, lambda reactor: None)
    reactor.one_tick()
    reactor.close()
    is_deeply_ok(errors, [None], 'handle added')
    peer.sendall(b'x')
    reactor.timer(5, lambda reactor: reactor.stop())
    reactor.start()
    is_deeply_ok(readable, [False], 'handle watched in the next run')
    reactor.reset()
    reactor.close()
    first.close()
    peer.close()

with subtest('Finalizer in the frames of Trio'):
    got = []

    async def main():
        reactor = Pyjo.Reactor.Trio.new()
        reactor.timer(10, lambda reactor: None)
        Pyjo.Reactor._finalize(lambda: got.append('cleanup'), reactor)
        got.append('finalizer')
        for _ in range(10):
            await trio.sleep(0)
        reactor.reset()

    trio.run(main)
    is_deeply_ok(got, ['finalizer', 'cleanup'], 'cleanup at the next tick')

with subtest('Reactor gone in a run of Trio'):
    first, peer = socket.socketpair()
    got = []

    async def application():
        gone = Pyjo.Reactor.Trio.new()
        gone.io(first, lambda reactor, writable: None).watch(first, True, False)
        del gone
        gc.collect()
        await trio.sleep(0.01)
        reactor = Pyjo.Reactor.Trio.new()
        reactor.io(first, lambda reactor, writable: got.append('readable')).watch(first, True, False)
        await trio.sleep(0.01)
        peer.sendall(b'x')
        await trio.sleep(0.1)
        reactor.remove(first)

    err = eval_(lambda: trio.run(application))
    is_ok(err, None, 'no error')
    is_deeply_ok(got[:1], ['readable'], 'handle of the reactor that is gone watched again')
    first.close()
    peer.close()

with subtest('Exception in a run of Trio'):

    async def application():
        reactor = Pyjo.Reactor.Trio.new()
        reactor.timer(0, lambda reactor: 1 / 0)
        await trio.sleep(0.1)

    err = eval_(lambda: trio.run(application))
    isa_ok(err, trio.TrioInternalError, 'run of Trio ends')
    cause = err.__cause__
    while isinstance(cause, BaseExceptionGroup):
        cause = cause.exceptions[0]
    like_ok(str(cause), r'Timer failed: .*division by zero', 'right error')

with subtest('Threads'):
    got = []

    def thread():
        reactor = Pyjo.Reactor.Trio.new()
        reactor.timer(0.05, lambda reactor: got.append(sniffio.current_async_library()))
        reactor.start()
        reactor.close()

    threads = [threading.Thread(target=thread) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    is_deeply_ok(got, ['trio'] * 4, 'a run of each thread')

with subtest('Wakeup fd of the signals'):
    if threading.current_thread() is not threading.main_thread():
        plan(skip_all='the main thread is required')
    first, peer = socket.socketpair()
    first.setblocking(False)
    previous = signal.set_wakeup_fd(first.fileno())
    reactor = Pyjo.Reactor.Trio.new()
    reactor.timer(0.01, lambda reactor: None)
    reactor.start()
    is_ok(signal.set_wakeup_fd(previous), first.fileno(), 'left to the program')
    reactor.close()
    first.close()
    peer.close()

# Not in Mojolicious: the child of a fork leaves the run of the parent, also in the middle of it, and the idle threads of
# Trio are stopped before, as Python warns about a fork of a process with threads
with subtest('Fork'):
    if not hasattr(os, 'fork'):
        plan(skip_all='os.fork is required')
    code = """
import os, sys
import Pyjo.Reactor.Trio

def child():
    reactor = Pyjo.Reactor.Trio.new()
    reactor.timer(0.01, lambda reactor: print('child', flush=True))
    reactor.start()
    os._exit(0)

reactor = Pyjo.Reactor.Trio.new()
reactor.timer(0.01, lambda reactor: None)
reactor.start()
if not os.fork():
    child()
os.wait()

def fork(reactor):
    if not os.fork():
        child()
    os.wait()

reactor.timer(0.01, fork)
reactor.start()
print('parent', flush=True)
"""
    env = dict(os.environ, PYTHONWARNINGS='error::DeprecationWarning')
    result = subprocess.run([sys.executable, '-c', code], env=env, capture_output=True, text=True, timeout=60)
    is_ok(result.stdout, 'child\nchild\nparent\n', 'children run Trio')
    is_ok(result.stderr, '', 'no warning of a fork with threads')

done_testing()
