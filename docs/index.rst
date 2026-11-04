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

``examples/loopback_trio.py`` is a server of Pyjoyment and its user agent on
Trio, a blocking request, which runs the event loop until the response, and
the class of the reactor and the async library that its callbacks run in:

    .. include:: ../examples/loopback_trio.py
       :code: python

.. code-block:: console

    $ python examples/loopback_trio.py
    Pyjo_Reactor_Trio trio
    200 Your Pyjo is working!
