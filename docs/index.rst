.. automodule:: Pyjo.Reactor.Trio
    :members:

.. The examples follow the API, after the docstring of the module, which ends with its classes: a section here would
   be a second title of the page, as the sections of the docstring are its own.

.. rubric:: Examples
    :heading-level: 2
    :name: examples

``examples/hello_world_trio.py`` runs ``Pyjo.HelloWorld`` on the server of
Pyjoyment on Trio until the signal ``INT`` or ``TERM``, in a ``with`` block,
which closes the listen sockets of the server at its end:

    .. include:: ../examples/hello_world_trio.py
       :code: python

.. code-block:: console

    $ python examples/hello_world_trio.py
    Web application available at http://127.0.0.1:3000

.. code-block:: console

    $ curl http://127.0.0.1:3000/
    Your Pyjo is working!

``examples/trio_nursery.py`` is a program of Trio, whose run has the server
of Pyjoyment and two tasks of a nursery that request it, with a stream of Trio
and with the user agent of Pyjoyment, whose promise a task of Trio awaits:

    .. include:: ../examples/trio_nursery.py
       :code: python

.. code-block:: console

    $ python examples/trio_nursery.py
    trio: HTTP/1.1 200 OK
    Pyjo.UserAgent: 200 Your Pyjo is working!

``examples/loopback_trio.py`` is a server of Pyjoyment and its user agent on
Trio, a blocking request, which runs the event loop until the response, and
the class of the reactor and the async library that its callbacks run in:

    .. include:: ../examples/loopback_trio.py
       :code: python

.. code-block:: console

    $ python examples/loopback_trio.py
    Pyjo_Reactor_Trio trio
    200 Your Pyjo is working!

``examples/echo_client_trio.py`` is the echo client of the `tutorial of Trio
<https://trio.readthedocs.io/en/stable/tutorial.html#an-echo-client>`_, with
its sender and receiver in a nursery, and an echo server of
:mod:`Pyjo.IOLoop` in the same run of Trio. The sender waits for the echo of
each message, and closes its side of the connection after the last one, which
the server closes the connection at:

    .. include:: ../examples/echo_client_trio.py
       :code: python

The tasks run concurrently, so the order of their lines differs between the
runs:

.. code-block:: console

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

``examples/echo_server_trio.py`` is the echo server of the `tutorial of Trio
<https://trio.readthedocs.io/en/stable/tutorial.html#an-echo-server>`_, with
``trio.serve_tcp`` in a nursery, and an echo client of :mod:`Pyjo.IOLoop`,
whose promise a task of Trio awaits:

    .. include:: ../examples/echo_server_trio.py
       :code: python

.. code-block:: console

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
