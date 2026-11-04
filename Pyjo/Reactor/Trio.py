"""
Pyjo.Reactor.Trio - Low-level event reactor on Trio
===================================================
::

    from Pyjo.Reactor.Trio import Trio

    # Watch if handle becomes readable or writable
    reactor = Trio()
    reactor.io(first, lambda reactor, writable: print(
        'First handle is writable' if writable else 'First handle is readable'
    ))

    # Or import the module, as in Google Python Style Guide

    import Pyjo.Reactor.Trio

    # Watch if handle becomes readable or writable
    reactor = Pyjo.Reactor.Trio.new()
    reactor.io(first, lambda reactor, writable: print(
        'First handle is writable' if writable else 'First handle is readable'
    ))

    # Change to watching only if handle becomes writable
    reactor.watch(first, False, True)

    # Watch a file descriptor if it becomes readable
    reactor.io(fd, lambda reactor, writable: print(
        'Second handle is writable' if writable else 'Second handle is readable'
    )).watch(fd, True, False)

    # Add a timer
    def timeout(reactor):
        reactor.remove(first)
        reactor.remove(fd)
        print('Timeout!')

    reactor.timer(15, timeout)

    # Start reactor if necessary
    if not reactor.is_running:
        reactor.start()

:mod:`Pyjo.Reactor.Trio` is a low-level event reactor based on
`Trio <https://trio.readthedocs.io>`_, the async library of structured
concurrency. It watches handles and runs timers in a run of Trio, so the
tasks of Trio run along with it. ``PYJO_REACTOR`` selects it for
:mod:`Pyjo.IOLoop`::

    PYJO_REACTOR=Pyjo.Reactor.Trio

Inside a run of Trio that runs already, as of :func:`trio.run`, the reactor
watches its handles and timers in that run, with tasks of its own, and
:attr:`~Pyjo_Reactor_Trio.is_running` is true there:
:meth:`~Pyjo_Reactor_Trio.start` and :meth:`~Pyjo_Reactor_Trio.one_tick`
return right away, as Trio does not nest its runs, a promise of Pyjoyment is
awaited in a task of Trio, :meth:`Pyjo.Promise.Pyjo_Promise.wait` returns at
once, and a blocking request of :mod:`Pyjo.UserAgent` raises
:class:`RuntimeError`. A coroutine of Pyjoyment, as an ``async def`` action,
is a task of that run, where it can await anything of Trio. An exception of a
callback that no ``error`` event handles ends that run with
:class:`trio.TrioInternalError`, as Trio does for a crashed system task.

Outside of a run of Trio, :meth:`~Pyjo_Reactor_Trio.start` and
:meth:`~Pyjo_Reactor_Trio.one_tick` run a run of Trio of the reactor itself,
in the guest mode of Trio, which ends when they return, unless coroutines that
it runs are not done yet, which it keeps until they are, or until
:meth:`~Pyjo_Reactor_Trio.close` cancels them, as the end of a ``with`` block
does. While it lives, the thread cannot start another run of Trio, as of
:func:`trio.run`. Each thread has runs of its own, as the workers of
:mod:`Pyjo.Server.Prethread`, and the child of a fork, as a worker of
:mod:`Pyjo.Server.Prefork`, leaves the run of its parent. The run of the
reactor leaves the signals and :func:`signal.set_wakeup_fd` to the program, as
the servers of Pyjoyment set them.

The events come in the order of :mod:`Pyjo.Reactor.Poll`: the handles that are
ready, in the order in which they were added, and then the timers that are
due, with the callbacks of :meth:`~Pyjo.Reactor.Poll.Pyjo_Reactor_Poll.next_tick`
that the handles added. On Windows, Trio watches sockets only.

Events
------

:mod:`Pyjo.Reactor.Trio` inherits all events from :mod:`Pyjo.Reactor.Poll`.

See also
--------

:mod:`Pyjo.Reactor.Poll`, :mod:`Pyjoyment`, https://github.com/pyjoyment/Pyjoyment.

Classes
-------
"""

import Pyjo.Exception
import Pyjo.Reactor
import Pyjo.Reactor.Poll

from Pyjo.Reactor import Handle, IOCallback, TimerCallback

import importlib.metadata
import itertools
import math
import os
import queue
import threading
import time
import weakref

from collections.abc import Awaitable, Callable, Coroutine, Generator
from types import TracebackType
from typing import Any, Self

import outcome
import trio
import trio.lowlevel


__version__ = importlib.metadata.version('Pyjo-Reactor-Trio')
"""The version of the distribution, of its metadata, which the workflow Automated version sets in pyproject.toml."""

_READABLE = 1
_WRITABLE = 2

# The coroutines that a reactor runs, by a number of their own
_TASKS = itertools.count()


class _Run:
    # A run of Trio that the tasks of a reactor are in: its own guest run, which the queue of its host delivers the
    # ticks of, or the run of the application
    def __init__(self, token: trio.lowlevel.TrioToken, own: bool) -> None:
        self.token = token
        self.own = own
        self.done = False
        self.queue: queue.SimpleQueue[Callable[[], Any]] = queue.SimpleQueue()
        self.main: trio.CancelScope | None = None
        self.outcome: outcome.Outcome[None] | None = None


class Pyjo_Reactor_Trio(Pyjo.Reactor.Poll.object):
    """
    :mod:`Pyjo.Reactor.Trio` inherits all methods from
    :mod:`Pyjo.Reactor.Poll` and implements the following new ones.
    """

    def __init__(self) -> None:
        """::

            reactor = Pyjo.Reactor.Trio.new()

        Construct a new :mod:`Pyjo.Reactor.Trio` object.
        """
        super().__init__()

        # The run of Trio of the reactor, and the pump of its own one
        self._run: _Run | None = None
        self._pumping = 0
        self._until: Callable[[], bool] | None = None
        self._error: BaseException | None = None
        self._finalizer: weakref.finalize[..., Any] | None = None

        # The cancel scopes of the watchers of the handles, readable and writable, the order in which the handles were
        # added, and the handles that the watchers found ready, 1 readable and 2 writable, for the next round
        self._scopes: dict[int, list[trio.CancelScope | None]] = {}
        self._order: dict[int, int] = {}
        self._sequence = 0
        self._ready: dict[int, int] = {}

        # The ticker, which runs the rounds: its cancel scope, the event that wakes it, the scope of its sleep until the
        # first timer, the event of the end of the round that the watchers wait for, and the gate of the own run
        self._ticker: trio.CancelScope | None = None
        self._signal: trio.Event | None = None
        self._sleep: trio.CancelScope | None = None
        self._sleep_until = math.inf
        self._round: trio.Event | None = None
        self._gate: trio.Event | None = None

        # The rounds with events and the steps of the coroutines, which end one_tick, and the coroutines that run
        self._ticks = 0
        self._tasks: set[int] = set()

        _REACTORS.add(self)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self, type: type[BaseException] | None, value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        self.close()

    def again(self, id: str, after: float | None = None) -> None:
        """::

            reactor.again(id)
            reactor.again(id, 0.5)

        Restart timer and optionally change the invocation time. Note that this
        method requires an active timer.
        """
        super().again(id, after)
        self._bind()
        self._rearm(self._timers[id]['time'])

    def awaitable(self, promise: Any) -> Awaitable[None]:
        """::

            awaitable = reactor.awaitable(promise)

        Return what ``await promise`` waits on in a task of Trio, which
        completes when the promise is fulfilled or rejected.
        """
        return _settled(promise)

    def close(self) -> None:
        """::

            reactor.close()

        End the run of Trio of the reactor itself, after the coroutines that
        are left in it are cancelled, as the end of a ``with`` block does; a
        run of the application is left to it.
        """
        self._end(True)

    def io(self, handle: Handle, cb: IOCallback) -> Self:
        """::

            reactor = reactor.io(handle, cb)

        Watch handle for I/O events, invoking the callback whenever handle
        becomes readable or writable. ::

            # Callback will be executed twice if handle becomes readable and writable
            def cb(reactor, writable):
                if writable:
                    print('Handle is writable')
                else:
                    print('Handle is readable')

            reactor.io(handle, cb)
        """
        fd = _fileno(handle)
        self._cancel(fd)
        if fd not in self._io:
            self._sequence += 1
            self._order[fd] = self._sequence
        self._io[fd] = {'cb': cb, 'mode': 0}
        return self.watch(handle, True, True)

    @property
    def is_running(self) -> bool:
        """::

            boolean = reactor.is_running

        Check if reactor is running, also inside a run of Trio that runs
        already, which watches its handles and timers.
        """
        return bool(self._running) or self._foreign()

    def one_tick(self) -> None:
        """::

            reactor.one_tick()

        Run reactor until an event occurs or no events are being watched
        anymore, or return right away inside a run of Trio that runs already.
        ::

            # Don't block longer than 0.5 seconds
            id = reactor.timer(0.5, lambda reactor: None)
            reactor.one_tick()
            reactor.remove(id)
        """
        if self._foreign() or self._pumping:
            return

        # Just one tick
        running = self._running
        if not running:
            self._running = 1

        try:
            # Stop automatically if there is nothing to watch
            if not self._watching():
                return self.stop()

            ticks = self._ticks
            self._pump(lambda: self._ticks != ticks or not self._running or self._error is not None)
        finally:
            if not running:
                self._running = running
                self._end()

    def remove(self, remove: Handle | str) -> bool:
        """::

            boolean = reactor.remove(handle)
            boolean = reactor.remove(id)

        Remove handle or timer.
        """
        if isinstance(remove, str):
            return self._timers.pop(remove, None) is not None
        fd = _fileno(remove)
        if self._io.pop(fd, None) is None:
            return False
        self._order.pop(fd, None)
        self._cancel(fd)
        return True

    def reset(self) -> None:
        """::

            reactor.reset()

        Remove all handles and timers.
        """
        for fd in list(self._scopes):
            self._cancel(fd)
        self._events = {}
        self._io = {}
        self._next_tick.clear()
        self._next_timer = None
        self._order = {}
        self._ready = {}
        self._running = 0
        self._timers = {}

    # Not in Mojolicious: a task of the run of Trio
    def run_coroutine(
        self, coro: Coroutine[Any, Any, Any], resolve: Callable[[Any], Any], reject: Callable[[Any], Any]
    ) -> None:
        """::

            reactor.run_coroutine(coro, resolve, reject)

        Run a coroutine as a task of the run of Trio, where it can await
        anything of Trio, and call ``resolve`` with what it returns or
        ``reject`` with the exception it raises, or with
        :class:`trio.Cancelled` when the run ends before it. A coroutine that
        is not done is something to watch, so that :meth:`start` does not stop
        before it, and each step of the coroutine is an event, which ends
        :meth:`one_tick`.
        """
        if self._bind() is None:
            self._own()
        token = next(_TASKS)
        self._tasks.add(token)
        ref = weakref.ref[Pyjo_Reactor_Trio](self)
        step = weakref.WeakMethod(self._step)

        async def task() -> None:
            try:
                result = await _Steps(coro, step)
            except (Exception, trio.Cancelled) as e:
                if (reactor := ref()) is not None:
                    reactor._tasks.discard(token)
                reject(e)
                if isinstance(e, trio.Cancelled):
                    raise
                return
            except BaseException:
                if (reactor := ref()) is not None:
                    reactor._tasks.discard(token)
                raise
            if (reactor := ref()) is not None:
                reactor._tasks.discard(token)
            resolve(result)

        trio.lowlevel.spawn_system_task(task)

    def start(self) -> None:
        """::

            reactor.start()

        Start watching for I/O and timer events, this will block until
        :meth:`stop` is called or no events are being watched anymore, or
        return right away inside a run of Trio that runs already. ::

            # Start reactor only if it is not running already
            if not reactor.is_running:
                reactor.start()
        """
        if self._foreign() or self._pumping:
            return

        running = self._running
        self._running = running + 1
        try:
            # Stop automatically if there is nothing to watch
            if not self._watching():
                return self.stop()
            self._pump(lambda: not self._running or self._error is not None or not self._watching())
        finally:
            self._running = running
            if not running:
                self._end()

    def watch(self, handle: Handle, read: bool, write: bool) -> Self:
        """::

            reactor = reactor.watch(handle, readable, writable)

        Change I/O events to watch handle for with true and false values. Note
        that this method requires an active I/O watcher. ::

            # Watch only for readable events
            reactor.watch(handle, True, False)

            # Watch only for writable events
            reactor.watch(handle, False, True)

            # Watch for readable and writable events
            reactor.watch(handle, True, True)

            # Pause watching for events
            reactor.watch(handle, False, False)
        """
        fd = _fileno(handle)
        if (io := self._io.get(fd)) is None:
            Pyjo.Exception.object.throw('I/O watcher not active')
        io['mode'] = (_READABLE if read else 0) | (_WRITABLE if write else 0)
        self._bind()
        self._watch(fd, io['mode'])
        return self

    # The run of Trio of the thread: the one of the application, which a run of the reactor before it has ended, or the
    # own one, or none
    def _bind(self) -> _Run | None:
        run = self._run
        if trio.lowlevel.in_trio_run():
            token = trio.lowlevel.current_trio_token()
            if run is not None and run.token is token and not run.done:
                return run
            run = self._run = _Run(token, False)
            self._spawn(run)
            return run

        # The run of the application has ended, and the tasks of the reactor with it
        if run is not None and not run.own:
            run = self._run = None
        return run

    def _cancel(self, fd: int) -> None:
        if (scopes := self._scopes.pop(fd, None)) is not None:
            for scope in scopes:
                if scope is not None:
                    scope.cancel()

    # A round of the events, as a tick of Pyjo.Reactor.Poll: the handles that are ready, in the order in which they were
    # added, as poll reports them, where Trio wakes their watchers in an order that it shuffles, then the timers that are
    # due, with the next tick of the handles among them
    def _dispatch(self) -> None:
        ready, self._ready = self._ready, {}
        i = 0
        try:
            order = self._order
            for fd in sorted(ready, key=lambda fd: order.get(fd, 0)):
                bits = ready[fd]
                if bits & _READABLE and (io := self._io.get(fd)) is not None and io['mode'] & _READABLE:
                    i += 1
                    if not self._fire('I/O watcher', io['cb'], False):
                        return
                if bits & _WRITABLE and (io := self._io.get(fd)) is not None and io['mode'] & _WRITABLE:
                    i += 1
                    if not self._fire('I/O watcher', io['cb'], True):
                        return

            # Timers (time should not change in between timers)
            now = time.monotonic()
            for id in list(self._timers):
                if (t := self._timers.get(id)) is None or t['time'] > now:
                    continue

                # Recurring timer
                if t['recurring']:
                    t['time'] = now + t['after']

                # Normal timer
                else:
                    self.remove(id)

                if (cb := t['cb']) is not None:
                    i += 1
                    if not self._fire('Timer', cb):
                        return
        finally:
            if i:
                self._ticks += 1
            if (round := self._round) is not None:
                self._round = None
                round.set()

    # The end of the own run, unless it runs coroutines that are not done, which the end of a with block cancels
    def _end(self, force: bool = False) -> None:
        run = self._run
        if run is None or not run.own or run.done or self._pumping:
            return
        if len(self._tasks) and not force:
            return
        if (main := run.main) is not None:
            main.cancel()
        self._pumping += 1
        try:
            while not run.done:
                run.queue.get()()
        finally:
            self._pumping -= 1
        self._forget(run)

    # An exception of a callback ends start or one_tick of the own run, as one of Pyjo.Reactor.Poll does, and the run of
    # the application, which it is raised in
    def _fire(self, what: str, cb: Any, *args: Any) -> bool:
        try:
            self._try(what, cb, *args)
        except Exception as e:
            if (run := self._run) is not None and run.own and self._pumping:
                self._error = e
                return False
            raise
        return True

    def _foreign(self) -> bool:
        return (run := self._bind()) is not None and not run.own

    def _forget(self, run: _Run) -> None:
        if self._run is run:
            self._run = None
        for scopes in self._scopes.values():
            scopes[0] = scopes[1] = None
        self._ready = {}
        self._ticker = self._signal = self._sleep = self._round = self._gate = None

    # A guest tick of Trio runs its batches until it waits for I/O, so the own run halts the rounds once start or
    # one_tick is done, as after stop or the event of one_tick, and between them
    def _halted(self) -> bool:
        if (run := self._run) is None or not run.own:
            return False
        return (until := self._until) is None or until()

    def _own(self) -> _Run:
        if (run := self._bind()) is not None:
            return run

        q: queue.SimpleQueue[Callable[[], Any]] = queue.SimpleQueue()
        holder: list[_Run] = []
        ref = weakref.ref[Pyjo_Reactor_Trio](self)

        async def main() -> None:
            with trio.CancelScope() as scope:
                holder[0].main = scope
                await trio.sleep_forever()

        def done(result: outcome.Outcome[None]) -> None:
            run = holder[0]
            run.done, run.outcome = True, result
            if (reactor := ref()) is not None:
                reactor._forget(run)

        # The host loop is the queue of the pump, and the program keeps the wakeup fd of the signals, as the servers of
        # Pyjoyment set it for themselves
        trio.lowlevel.start_guest_run(
            main,
            run_sync_soon_threadsafe=q.put_nowait,
            done_callback=done,
            host_uses_signal_set_wakeup_fd=True,
            strict_exception_groups=True,
        )
        run = self._run = _Run(trio.lowlevel.current_trio_token(), True)
        run.queue = q
        holder.append(run)
        self._spawn(run)
        return run

    # The own run, which runs its guest ticks from the queue until the condition, with the cleanups of the finalizers that
    # wait for it before
    def _pump(self, until: Callable[[], bool]) -> None:
        run = self._own()
        self._pumping += 1
        self._until = until
        if (gate := self._gate) is not None:
            self._gate = None
            gate.set()
        try:
            while not run.done and not until():
                if Pyjo.Reactor._DEFERRED.get(threading.get_ident()):  # pyright: ignore[reportPrivateUsage]
                    Pyjo.Reactor._drain()  # pyright: ignore[reportPrivateUsage]
                    continue
                run.queue.get()()
        finally:
            self._pumping -= 1
            self._until = None

        # An exception of a callback, or of the run itself
        if (error := self._error) is not None:
            self._error = None
            raise error
        if run.done and isinstance(result := run.outcome, outcome.Error):
            raise result.error

    # The ticker sleeps until the first timer, which a new one may come before. Its deadline is changed in the run, as a
    # signal handler that adds a timer, as with next_tick, interrupts Trio in the middle of its deadlines, where
    # run_sync_soon is safe
    def _rearm(self, when: float) -> None:
        if self._sleep is None or when >= self._sleep_until:
            return
        self._sleep_until = when
        if (run := self._run) is not None and not run.done:
            try:
                run.token.run_sync_soon(self._deadline, idempotent=True)
            except trio.RunFinishedError:
                pass

    def _deadline(self) -> None:
        if (sleep := self._sleep) is not None and (deadline := _trio_time(self._sleep_until)) < sleep.deadline:
            sleep.deadline = deadline

    def _report(self, fd: int, bit: int) -> trio.Event:
        self._ready[fd] = self._ready.get(fd, 0) | bit
        if (round := self._round) is None:
            round = self._round = trio.Event()
        if (signal := self._signal) is not None:
            signal.set()
        return round

    # The tasks of the reactor in a run, its ticker and the watchers of its handles, whose scopes a finalizer cancels in
    # the run of the application when the reactor is gone without removing them, as a watcher of a handle that is closed
    # would keep its number from the next one in Trio
    def _spawn(self, run: _Run) -> None:
        ref = weakref.ref[Pyjo_Reactor_Trio](self)
        self._ready = {}
        self._signal = self._sleep = self._round = self._gate = None
        ticker = self._ticker = trio.CancelScope()
        trio.lowlevel.spawn_system_task(Pyjo_Reactor_Trio._tick, ref, ticker)
        for fd, io in self._io.items():
            self._scopes[fd] = [None, None]
            self._watch(fd, io['mode'])

        if (finalizer := self._finalizer) is not None:
            finalizer.detach()
        finalizer = self._finalizer = weakref.finalize(self, _abandon, run.token, ticker, self._scopes)
        finalizer.atexit = False

    def _step(self) -> None:
        self._ticks += 1

    def _timer(self, recurring: bool, after: float, cb: TimerCallback) -> str:
        id = super()._timer(recurring, after, cb)
        self._bind()
        self._rearm(self._timers[id]['time'])
        return id

    # The cleanups that finalizers deferred, which the run of the reactor runs, from any thread
    def _wake(self) -> None:
        if (run := self._run) is not None and not run.done:
            try:
                run.token.run_sync_soon(Pyjo.Reactor._drain)  # pyright: ignore[reportPrivateUsage]
            except trio.RunFinishedError:
                pass

    def _watch(self, fd: int, mode: int) -> None:
        scopes = self._scopes.setdefault(fd, [None, None])
        run = self._run
        for writable in (False, True):
            wanted = bool(mode & (_WRITABLE if writable else _READABLE))
            scope = scopes[writable]
            if wanted and scope is None and run is not None and not run.done:
                scope = scopes[writable] = trio.CancelScope()
                ref = weakref.ref[Pyjo_Reactor_Trio](self)
                trio.lowlevel.spawn_system_task(Pyjo_Reactor_Trio._watcher, ref, fd, writable, scope)
            elif not wanted and scope is not None:
                scope.cancel()
                scopes[writable] = None

    def _watching(self) -> bool:
        return bool(len(self._timers) or len(self._io) or len(self._tasks))

    # The ticker of a reactor, which waits for the watchers or the first timer, and then runs a round of the events
    @staticmethod
    async def _tick(ref: 'weakref.ref[Pyjo_Reactor_Trio]', scope: trio.CancelScope) -> None:
        with scope:
            while True:
                if (reactor := ref()) is None:
                    return
                signal = reactor._signal = trio.Event()
                if not reactor._ready:
                    # The sleep is there before the first timer is looked for, as a timer that a signal handler adds
                    # then moves its deadline, and the timers are a copy, as the handler adds one in the middle
                    with trio.CancelScope() as sleep:
                        reactor._sleep, reactor._sleep_until = sleep, math.inf
                        first = min((t['time'] for t in list(reactor._timers.values())), default=math.inf)
                        if (until := min(first, reactor._sleep_until)) < math.inf:
                            sleep.deadline = _trio_time(until)
                        del reactor
                        await signal.wait()
                    if (reactor := ref()) is None:
                        return
                    reactor._sleep = None
                reactor._signal = None

                # The own run waits for start or one_tick
                while reactor._halted():
                    if (gate := reactor._gate) is None:
                        gate = reactor._gate = trio.Event()
                    del reactor
                    await gate.wait()
                    if (reactor := ref()) is None:
                        return

                reactor._dispatch()
                del reactor
                await trio.lowlevel.checkpoint()

    # The watcher of a handle, readable or writable, which waits for it, reports it to the ticker and waits for the end of
    # the round, as the handle stays ready until its callback reads or writes
    @staticmethod
    async def _watcher(ref: 'weakref.ref[Pyjo_Reactor_Trio]', fd: int, writable: bool, scope: trio.CancelScope) -> None:
        wait = trio.lowlevel.wait_writable if writable else trio.lowlevel.wait_readable
        bit = _WRITABLE if writable else _READABLE
        with scope:
            while True:
                # A watcher that was removed before it started, whose handle a new one may wait for already, which Trio
                # checks before it delivers the cancellation
                if scope.cancel_called:
                    return

                # A handle that is closed is ready, as poll reports it, so that its callback finds out
                try:
                    await wait(fd)
                except (OSError, trio.ClosedResourceError):
                    pass

                if (reactor := ref()) is None:
                    return
                round = reactor._report(fd, bit)
                del reactor
                await round.wait()


# The tasks of a reactor that is gone, in the thread of its run, which may have ended
def _abandon(
    token: trio.lowlevel.TrioToken, ticker: trio.CancelScope, scopes: dict[int, list[trio.CancelScope | None]]
) -> None:
    def cancel() -> None:
        ticker.cancel()
        for watchers in scopes.values():
            for scope in watchers:
                if scope is not None:
                    scope.cancel()

    try:
        token.run_sync_soon(cancel)
    except trio.RunFinishedError:
        pass


def _fileno(handle: Handle) -> int:
    fd = handle if isinstance(handle, int) else handle.fileno()
    if fd < 0:
        Pyjo.Exception.object.throw('Handle is closed')
    return fd


async def _settled(promise: Any) -> None:
    event = trio.Event()
    promise.then(lambda *values: event.set(), lambda *values: event.set())  # pyright: ignore[reportUnknownLambdaType]
    await event.wait()


# A coroutine of Pyjoyment, whose steps are events of the reactor, as one_tick returns after one
class _Steps:
    def __init__(self, coro: Coroutine[Any, Any, Any], step: 'weakref.WeakMethod[Callable[[], None]]') -> None:
        self._coro = coro
        self._step = step

    def __await__(self) -> Generator[Any, Any, Any]:
        coro = self._coro
        method: Callable[[Any], Any] = coro.send
        arg: Any = None
        while True:
            try:
                result = method(arg)
            except StopIteration as e:
                self._stepped()
                return e.value
            except BaseException:
                self._stepped()
                raise
            self._stepped()
            try:
                arg = yield result
                method = coro.send
            except GeneratorExit:
                coro.close()
                raise
            except BaseException as e:
                arg = e
                method = coro.throw

    def _stepped(self) -> None:
        if (step := self._step()) is not None:
            step()


# Trio counts its clock from an offset of its own
def _trio_time(when: float) -> float:
    return trio.current_time() + (when - time.monotonic())


_REACTORS: 'weakref.WeakSet[Pyjo_Reactor_Trio]' = weakref.WeakSet()


# The idle threads of the cache of Trio, as the one that waits for the I/O of a guest run, are stopped before a fork, as
# Python warns about a fork of a process with threads, where Pyjo.IOLoop.Client stops the threads of its resolver; an
# idle thread waits for a job for 10 seconds, which an empty job with no wait after it ends now. Trio has no API for it
def _before_fork() -> None:
    from trio._core import _thread_cache  # pyright: ignore[reportPrivateUsage]

    cache = _thread_cache.THREAD_CACHE
    if not (workers := list(cache._idle_workers)):  # pyright: ignore[reportPrivateUsage]
        return

    timeout = _thread_cache.IDLE_TIMEOUT
    _thread_cache.IDLE_TIMEOUT = 0
    try:
        stopping: list[Any] = []
        for worker in workers:
            try:
                del cache._idle_workers[worker]  # pyright: ignore[reportPrivateUsage]
            except KeyError:
                continue
            worker._job = (lambda: None, lambda result: None, None)  # pyright: ignore[reportPrivateUsage]
            worker._worker_lock.release()  # pyright: ignore[reportPrivateUsage]
            stopping.append(worker)
        for worker in stopping:
            worker._thread.join(1)
    finally:
        _thread_cache.IDLE_TIMEOUT = timeout

    # Linux lists a thread that has been joined for a moment more, which Python 3.15 counts as a thread when it forks, as
    # Pyjo.IOLoop.Client waits for it too
    tasks = '/proc/self/task'
    deadline = time.monotonic() + 0.1
    while os.path.isdir(tasks) and len(os.listdir(tasks)) > threading.active_count() and time.monotonic() < deadline:
        time.sleep(0.001)


# The child of a fork leaves the run of the parent, whose epoll is shared with the parent and whose thread of I/O is not
# there, also when it forked in the middle of a guest tick, which binds the run to the thread. Trio has no API for it
def _after_fork_in_child() -> None:
    from trio._core import _run  # pyright: ignore[reportPrivateUsage]

    for reactor in list(_REACTORS):
        if (run := reactor._run) is not None and run.own:  # pyright: ignore[reportPrivateUsage]
            run.done = True
            reactor._forget(run)  # pyright: ignore[reportPrivateUsage]
            reactor._pumping = 0  # pyright: ignore[reportPrivateUsage]
            reactor._tasks.clear()  # pyright: ignore[reportPrivateUsage]
    for name in ('runner', 'task'):
        if hasattr(_run.GLOBAL_RUN_CONTEXT, name):
            delattr(_run.GLOBAL_RUN_CONTEXT, name)


if hasattr(os, 'register_at_fork'):
    os.register_at_fork(before=_before_fork, after_in_child=_after_fork_in_child)


new = Pyjo_Reactor_Trio.new
object = Pyjo_Reactor_Trio
Trio = Pyjo_Reactor_Trio
