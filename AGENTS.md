# Agent Guide: Pyjo-Reactor-Trio

## Overview

`Pyjo.Reactor.Trio` is a reactor of Pyjoyment on Trio, in a distribution of
its own, as every reactor of Pyjoyment with a dependency is: Pyjoyment has no
runtime dependencies, and one reactor does not pull the dependencies of
another. The conventions are those of Pyjoyment: read its `AGENTS.md` in the
checkout next to this one, `../Pyjoyment/AGENTS.md`. The repository is made
of the one of Pyjo-Reactor-Gevent, made of Pyjo-Reactor-Uvloop and
Pyjo-Reactor-Twisted, whose workflows and linters it shares; their
`AGENTS.md`, in `../Pyjo-Reactor-Uvloop` and `../Pyjo-Reactor-Twisted`, say
what each of their loops needed.

## How it works

What the reactor is for is the servers of Pyjoyment, `daemon`, `prefork` and
`prethread`, and its user agent, in a program of Trio, along with its tasks.
The docstring of the module says what a user sees; this is how and why.

- **A class on `Pyjo.Reactor.Poll`**, as `Pyjo.Reactor.Twisted` is, for the
  bookkeeping: `_io` with the modes, `_timers` with their `time`, `again`,
  `_timer`, `next_tick` and `_try`. Trio has no callbacks of I/O or timers.
- **Watchers and the ticker.** Each handle is watched by a system task per
  direction, `_watcher`, which waits with `trio.lowlevel.wait_readable` or
  `wait_writable` in a cancel scope of its own, and does not call the callback:
  it reports the handle to `_ready` and waits for the end of the round, as the
  handle stays ready until the callback reads or writes. One system task per
  reactor, `_tick`, sleeps until a watcher reports or the first timer is due,
  in a cancel scope whose deadline is that timer, and runs a round,
  `_dispatch`, as a tick of `Pyjo.Reactor.Poll`: the handles that are ready, in
  the order in which they were added, and then the timers that are due, with
  the `next_tick` of the handles among them. Timers are no tasks.
- **Why rounds.** Trio shuffles the tasks of a batch, reversing it half of the
  time, so a callback per watcher task ran in a random order, where Poll has
  the order of `_io` and runs the timers after the handles. Tests of
  Pyjoyment depend on it: a listen socket of `single_accept` accepted a second
  connection before the `next_tick` of the first one (Connection limit), and a
  worker of `Pyjo.Server.Prethread` read the byte of its graceful stop before
  it accepted the connection waiting in the backlog, and stopped without the
  request that was to keep it (4 of 10 runs on Linux). `_order` numbers the
  handles at their first `io`, which `_dispatch` sorts the ready ones by.
- **Two kinds of run.** Inside a run of Trio, `trio.lowlevel.in_trio_run()`,
  `_bind` attaches the reactor to it (`_Run(own=False)`) and spawns its tasks
  there; `is_running` is true, so `Pyjo.Promise.wait` returns at once and a
  blocking request raises, as on `Pyjo.Reactor.Asyncio` in a running loop since
  pyjoyment/Pyjoyment#522, and `start` and `one_tick` return. Outside, `_own`
  starts a guest run, `trio.lowlevel.start_guest_run`, whose host loop is a
  `queue.SimpleQueue` that `_pump` takes the guest ticks from, until its
  condition. The guest run ends when `start` or `one_tick` returns (`_end`),
  unless `run_coroutine` tasks are not done, which keep it until they are, or
  until `close` cancels its main task. A run per `start` would cost an empty
  `trio.run` each time, about 184 µs, where a guest tick costs about 6 to 12 µs
  per event, and `trio.run` takes over `signal.set_wakeup_fd`.
- **The gate.** A guest tick runs the batches of Trio until Trio has to wait
  for I/O, so one tick can hold several rounds: a round after `stop`, or after
  the event of `one_tick`, fired callbacks that Poll would not (a second
  `drain` of a stream, three errors of a disabled SOCKS instead of one). In the
  own run, the ticker waits at `_gate` while `_halted`: when no pump runs, or
  the condition of the pump holds already. The run of the application has no
  gate: it is always running.
- **Watcher removed before it started.** `remove` and `io` of the same handle
  in one tick, as `_not_accepting()._maybe_accepting()` of `Pyjo.IOLoop` does,
  cancel a watcher that has not run yet. Trio checks its registry before it
  delivers the cancellation, so that watcher raised `BusyResourceError`
  against the new one, when Trio ran it second; it returns when its scope is
  cancelled already.
- **Watcher removed between the guest ticks.** Between the ticks of the own
  run, the thread of I/O of Trio waits, and may have the event of a watcher
  already. A watcher cancelled there, by the program between `one_tick`s, by
  `_drain` in `_pump`, or by a finalizer that the garbage collector runs in the
  middle of Trio, leaves that event without its registration, which kqueue of
  Trio, on macOS and BSD, raises `KeyError` on, and the run ends with
  `TrioInternalError` (`t/stdlib/IOLoop_gc.py` of Pyjoyment, 4 of 10 runs);
  epoll keeps its registrations in a `defaultdict`. Outside a task of Trio,
  `_stop_watcher` cancels it with `run_sync_soon`, at the next tick, after the
  events, and `_start_watcher` starts a new watcher of that handle after it
  (`_stopping`), as Trio would raise `BusyResourceError` for a second one.
  Other watchers start at once, as the ones that `_own` starts must be there
  before the first round. A watcher is stopped at once, `_Watcher.stopped`,
  and reports nothing more, and its handle leaves `_ready`: until its scope is
  cancelled, a handle of the same number may be added, as a new connection,
  whose callback it woke up (`Socket is not connected` of
  `t/docs/skills_async.py`, the inactivity timeouts of `t/Pyjo/App_ASGI.py`,
  13 of 20 runs). The bug of Trio is to be reported upstream
  (#2), and the workaround dropped when a release fixes it.
- **Finalizers in the middle of the reactor.** `Pyjo.Reactor._finalize` defers
  a cleanup to the next tick below a frame of this module, wherever it is
  installed, and of Trio, which `event_loop_modules` names, since
  pyjoyment/Pyjoyment#534. Before, it ran at once in the frames of the
  reactor when it was installed editable, as in development and CI, and a
  finalizer that PyPy ran in `_spawn` removed a handle in the middle of its
  loop. The reactor still iterates over copies, as `remove` comes from a
  callback too, `_spawn` skips a handle that is gone, and the main task of
  the own run has its scope before it starts, so that `_end` ends a run that
  failed to start. A cleanup that is deferred runs in `_drain` of `_pump`,
  between the guest ticks, where the watcher that it removes is cancelled at
  the next tick, as above.
- **Handles added while a run ends.** After the main task of a run, Trio
  cancels the system tasks and closes their nursery, and still runs the
  callbacks of `run_sync_soon`, as `_drain`, whose cleanups add handles, as
  `_not_accepting()._maybe_accepting()` of `Pyjo.IOLoop` does.
  `spawn_system_task` raises `RuntimeError` there, which `_drain` swallowed,
  and `_maybe_accepting` stopped before the other acceptors: a server did not
  accept a connection until the next change of the acceptors
  (`t/Pyjo/App_ASGI.py`, WebSocket errors, about 1 of 10 runs on Linux).
  `_spawn_watcher` leaves such a watcher to the next run, which starts the
  watchers of all the handles.
- **Closed handles and `notify_closing`.** The reactor does not call
  `trio.lowlevel.notify_closing`: Pyjoyment removes a handle before it closes
  it, and the cancellation of the watcher unregisters it at once. A handle
  that is closed while it is watched leaves its watcher registered in Trio
  under its number: the next handle of that number in the same reactor is
  fine, as `io` cancels the old watcher first, but in another one Trio raises
  `BusyResourceError`. A reactor that is gone in a run of the application has a
  finalizer, `_abandon`, which cancels its tasks with `run_sync_soon`, as the
  garbage collector may run in another thread. `wait_readable` of a closed
  handle raises `OSError`, which the watcher reports as ready, as poll reports
  `POLLNVAL`, so that the callback finds out.
- **Signals.** The guest run has `host_uses_signal_set_wakeup_fd=True`, so it
  leaves `signal.set_wakeup_fd` to the program, which the servers of Pyjoyment
  set for themselves. A signal handler runs in the middle of Trio, so it must
  not change its state: `next_tick` from a handler, as the servers stop, moves
  the deadline of the ticker through `TrioToken.run_sync_soon`, which is
  signal-safe (`_rearm`, `_deadline`), and the ticker copies `_timers` before
  it looks for the first one, as Poll does.
- **Errors.** An exception of a callback that no `error` event handles is
  raised from `start` or `one_tick` of the own run (`_error`), and an error of
  the guest run itself from its outcome. In the run of the application, it
  crashes the system task, and Trio ends the run with `TrioInternalError`.
- **Threads.** The run is thread-local in Trio, so each thread has runs of its
  own, as the workers of `Pyjo.Server.Prethread`. The idle threads of the
  cache of Trio are shared by them.
- **Fork, with private API of Trio.** The child of a fork, as a worker of
  `Pyjo.Server.Prefork`, leaves the run of the parent: its epoll is shared with
  the parent and its thread of I/O is not there. A fork in the middle of a
  guest tick, as `Pyjo.IOLoop.subprocess` does in a callback, leaves the run
  bound to the thread, so `_after_fork_in_child` deletes `runner` and `task`
  of `trio._core._run.GLOBAL_RUN_CONTEXT`. Before a fork, `_before_fork` stops
  the idle threads of `trio._core._thread_cache`, as the one that waits for
  the I/O of a guest run waits 10 seconds for its next job, and Python warns
  about a fork of a process with threads, which `t/stdlib/IOLoop_Subprocess.py`
  of Pyjoyment fails on: each gets an empty job with `IDLE_TIMEOUT` 0, so its
  next wait times out at once, and is joined. Trio has no API for either, so
  both break if Trio renames them.
- Trio is pure Python, on CPython and PyPy, so the matrix of Tests has 3.12 to
  3.15, the free-threaded build and PyPy, as the one of Pyjo-Reactor-Twisted.

## Development

The distribution depends on the release of Pyjoyment on PyPI, with the lowest
version that has what it uses, which `uv.lock` pins. A change of Pyjoyment that
is not released yet would be its Git repository in `[tool.uv.sources]` of
`pyproject.toml`, until its release, and not a path to the checkout next to
this one: uv applies the sources of a dependency of Git, so whoever installs
the reactor from Git would get a subdirectory of this repository that does not
exist. Before a release of the reactor, the source goes and the lowest
version of `dependencies` becomes the release of Pyjoyment that has the
change, as a wheel carries no sources of uv.
The tests lay the checkout next to this one over it with `--with-editable`,
so that they test what is there:

```sh
mise exec -- uv sync
mise exec -- uv run --with-editable ../Pyjoyment python -m Pyjo.Prove -l -r t
```

Perl's `prove` runs them too, with `.proverc`, under `uv run`, whose `python`
is the one of the environment, where Pyjoyment and the reactor are installed,
so that it needs no `PYTHONPATH`, unlike in Pyjoyment:

```sh
mise exec -- uv run --with-editable ../Pyjoyment prove -r t
```

The tests of Pyjoyment run on this reactor as well, with `PYJO_REACTOR`:

```sh
cd ../Pyjoyment
PYJO_REACTOR=Pyjo.Reactor.Trio mise exec -- uv run --project ../Pyjo-Reactor-Trio --with-editable . \
  python -m Pyjo.Prove -l -j 4 -r t
```

The workflow Tests runs the tests of the reactor against the release of
`uv.lock`, without `--with-editable`, so that a pull request tests what an
installation gets, and the suite of Pyjoyment of the tag of that release on
the reactor, or of its commit when `uv.lock` has it from Git. A dispatch with
`pyjoyment-ref` lays a checkout of the ref over it, as in development, so
that what the next release of Pyjoyment breaks is known before the release.
It ends with the job `Tests`, the one check of it
that the ruleset of `main` requires, as in Pyjoyment: it fails when a job of
the matrix failed or was cancelled, so a new version of Python needs no change
of the ruleset.

Trunk checks the files with the linters of Pyjoyment, from `.trunk/trunk.yaml`:

```sh
trunk check --all
```

actionlint is the one of the maintained fork kjanat/actionlint, whose releases
continue those of rhysd/actionlint after 1.7.12, as in Pyjoyment: `downloads`
in `.trunk/trunk.yaml` replaces the URLs of the plugin with those of the fork,
whose files have the same names.

Biome formats and lints JSON, Markdown and YAML, with `biome.jsonc`, in the
place of prettier, markdownlint and yamlfmt, as in Pyjoyment, and yamllint
checks what Biome does not, as the duplicated keys. The version of Biome is
the pinned one of `.trunk/trunk.yaml`, where `!` keeps it from
`trunk upgrade`, and which Renovate bumps: the repository has no
`package.json` of its own. Renovate cannot run `biome migrate`, so the
workflow Biome Migrate runs it with `npx --yes @biomejs/biome@<version>` of
that version, migrates `biome.jsonc` on the branch of Renovate and commits it
with the token of the workflow, as the author that `gitIgnoredAuthors` of
`.renovaterc.json` names, so that Renovate keeps updating the branch, and
starts the checks of the commit with `workflow_dispatch`; anywhere else, a
`biome.jsonc` that this Biome would migrate fails the check. The definition of
Biome in `.trunk/trunk.yaml` is the one of Pyjoyment, which explains why it is
not the one of the plugin of Trunk.

Its pyright needs the environment of `uv sync`, `.venv`, which `tool.pyright` of
`pyproject.toml` names, as the reactor imports Pyjoyment and Trio, while
Pyjoyment has no dependencies to find; Trunk Check of CI runs `uv sync` first.
The examples and the tests, and `docs/conf.py`, are not checked by pyright: for them it takes the
regular package `Pyjo` of Pyjoyment, which hides `Pyjo.Reactor.Trio` of the
checkout.

The documentation is one page, `docs/index.rst`: the docstrings of the
module, which autodoc imports, and the examples from `examples/`, included,
never copied, as in Pyjoyment. Read the Docs installs it with `uv sync` of `uv.lock` and the
group `docs`, not with pip and a `docs/requirements.txt` as Pyjoyment does,
so that it builds with the versions of the lock, as the workflow Docs does.
The links to the modules of Pyjoyment come from intersphinx, to its stable
version from the stable one, and to latest from the others. The examples are
a rubric, not a section: they follow the docstring of the module, whose
sections are its own, so that a section would be a second title of the page.
The workflow Docs builds it with `-W`, as Read the Docs does, with the
Pyjoyment of `uv.lock`:

```sh
mise exec -- uv run --group docs --with-editable ../Pyjoyment sphinx-build -W --keep-going -b html docs docs/_build/html
```

`t/Reactor_Trio.py` is `t/Pyjo/Reactor_Poll.py` of Pyjoyment, the port of
`t/mojo/reactor_poll.t` of Mojolicious, in the subtests of 9.53, without the
abstract methods of `Mojo::Reactor`, as `t/Reactor_Twisted.py` of
Pyjo-Reactor-Twisted, with subtests of its own for Trio after them, for the
points of "How it works", which fail without what they test; "Watcher removed
between the guest ticks" fails on macOS only, where Trio has kqueue.
`t/docs/Reactor_Trio.py` runs the synopsis of the module, in both forms, as
`t/docs` of Pyjoyment does, and `t/examples.py` the examples.

## Releases

The releases are those of Pyjoyment, with its workflows: a release is a tag of
the version of `pyproject.toml`, without a `v`, as `0.2.0`, which Automated
version sets with `uv version`, the components of its `--bump` from a list, as
`patch dev` or `minor rc`, or the version of `manual`, in the pull request of
the branch `automated/version`. After a release, a pre-release goes with the
release that it comes before, as PEP 440 orders `0.2.0.dev1` before `0.2.0`,
so `dev` alone after `0.2.0` fails. Automated tag tags the merge commit of
that pull request when it is merged, or with a comment `/tag` of a maintainer
on it, and starts Release on the tag. A development version, as `0.2.1.dev1`,
is not tagged. The version is static in `pyproject.toml`, as `uv version`
cannot set a dynamic one, and `__version__` of the module reads it from the
metadata of the distribution.

Release builds the sdist and the wheel, attests their build provenance with
`actions/attest`, and makes a draft of the GitHub release with them, whose
notes are rewritten before it is published, and an SBOM in SPDX, attested
with them. The module bundles nothing, unlike the package of Pyjoyment, whose
SBOM lists the JavaScript libraries of its package, so the SBOM is what an
installation of the wheel gets: the distribution and its dependencies, as
Pyjoyment, Trio and its dependencies, at the versions of `uv.lock`, installed in a virtual
environment of their own, which Syft scans. Syft on `uv.lock` itself would
take its dependency groups too, as Sphinx, which an installation does not get.
The sdist does not carry the SBOM, as the one of Pyjoyment does, as it is made
of the wheel, which is built after the sdist.

The workflow PyPI, run manually, publishes the files of the latest published
release, or of the release of the tag of its input, on PyPI once their
attestations of Release verify, in the environment `publishing`, which waits
for an approval and takes `main` and tags only. The trusted publisher of PyPI
names `pypi.yaml` and that environment.

```sh
gh attestation verify pyjo_reactor_trio-0.2.0.tar.gz -R pyjoyment/Pyjo-Reactor-Trio
```
