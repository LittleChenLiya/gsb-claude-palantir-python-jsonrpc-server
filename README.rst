Python JSON RPC Server
======================

.. image:: https://circleci.com/gh/palantir/python-jsonrpc-server/tree/develop.svg?style=shield
    :target: https://circleci.com/gh/palantir/python-jsonrpc-server/tree/develop

.. image:: https://ci.appveyor.com/api/projects/status/r0jlmvkqwneieeh6/branch/develop?svg=true
    :target: https://ci.appveyor.com/project/gatesn/python-jsonrpc-server

.. image:: https://img.shields.io/github/license/palantir/python-jsonrpc-server.svg
     :target: https://github.com/palantir/python-jsonrpc-server/blob/develop/LICENSE

A Python 2.7 and 3.4+ server implementation of the `JSON RPC 2.0`_ protocol. This library has been pulled
out of the `Python Language Server`_ project.

Asynchronous request handling is supported using Python 3's ``concurrent.futures`` module and the Python 2 `concurrent.futures backport`_.

Installation
------------

``pip install -U python-jsonrpc-server``

Examples
--------

The examples directory contains two examples of running language servers over websockets. ``examples/langserver.py`` shows how to run a language server in-memory. ``examples/langserver_ext.py`` shows how to run a subprocess language server, in this case the `Python Language Server`_.

Start by installing `tornado` and `python-language-server`

``pip install python-language-server[all] tornado``

Then running `python examples/langserver.py` or `python examples/langserver_ext.py` will host a websocket on ``ws://localhost:3000/python``.

To setup a client, you can use the examples from `Monaco Language Client`_.

Framing
-------

Messages are framed like the Language Server Protocol: a ``Content-Length``
header (counting the body's UTF-8 bytes), an optional ``Content-Type``
header, a blank line (``\r\n\r\n``), then the JSON body. ``JsonRpcStreamReader``
reads frames with explicit bounds and tolerates arbitrary byte chunking of the
underlying stream. Protocol errors raise distinct ``ValueError`` subclasses
(``JsonRpcInvalidContentLength``, ``JsonRpcDuplicateContentLength``,
``JsonRpcHeaderTooLarge``, ``JsonRpcContentLengthTooLarge``,
``JsonRpcTruncatedMessage``); truncated bodies are never delivered. The header
and body limits default to 64 KiB and 16 MiB and can be configured::

    reader = JsonRpcStreamReader(rfile, max_header_size=2**16, max_content_length=2**24)

See ``examples/protocol_framing.py`` for a runnable example.

Development
-----------

To run the test suite:

``pip install .[test] && tox``

License
-------

This project is made available under the MIT License.

.. _JSON RPC 2.0: http://www.jsonrpc.org/specification
.. _Python Language Server: https://github.com/palantir/python-language-server
.. _concurrent.futures backport: https://github.com/agronholm/pythonfutures
.. _Monaco Language Client: https://github.com/TypeFox/monaco-languageclient
