import os


# The template of the test of the reactor, to be t/Pyjo/Reactor_Poll.py of Pyjoyment, the port of t/mojo/reactor_poll.t
# of Mojolicious, with subtests of its own for Trio. The reactor of the test is selected before Pyjo.IOLoop
# is loaded, as Pyjo.Reactor.detect() prefers Pyjo.Reactor.Asyncio
os.environ['PYJO_REACTOR'] = 'Pyjo.Reactor.Trio'

import Pyjo.Reactor  # noqa: E402
import Pyjo.Reactor.Trio  # noqa: E402

from Pyjo.Test import *  # noqa: E402


with subtest('Reactor detection'):
    is_ok(Pyjo.Reactor.object.detect(), 'Pyjo.Reactor.Trio', 'right class')
    reactor = Pyjo.Reactor.Trio.new()
    is_ok(type(reactor), Pyjo.Reactor.Trio.object, 'right object')

done_testing()
