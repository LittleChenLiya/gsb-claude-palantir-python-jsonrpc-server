# Copyright 2018 Palantir Technologies, Inc.
# pylint: disable=redefined-outer-name
"""Acceptance tests for the RPC error ``data`` pass-through convention.

Two in-memory Endpoints are wired directly to each other (each consumer feeds
the peer's ``consume``), so every request/response round-trips through the
real serialization and parsing paths:

* structured object ``data`` is preserved end to end,
* explicit ``null`` data stays distinguishable from an absent data member,
* non JSON-serializable data falls back to the internal error format without
  crashing the sending thread.

The exchanged wire messages are dumped to a temporary JSON file as a small
inspectable fixture of the actual results.
"""
import json
import itertools

import pytest

from pyls_jsonrpc import exceptions
from pyls_jsonrpc.endpoint import Endpoint


class Wire(object):
    """A pair of in-memory connected endpoints recording all traffic."""

    def __init__(self):
        self.messages = []
        ids = ('req-%d' % i for i in itertools.count())
        self.endpoint_a = Endpoint({}, self._send_to_b, id_generator=lambda: next(ids))
        self.endpoint_b = Endpoint({}, self._send_to_a)

    def _send_to_b(self, message):
        self.messages.append({'direction': 'a->b', 'message': message})
        self.endpoint_b.consume(message)

    def _send_to_a(self, message):
        self.messages.append({'direction': 'b->a', 'message': message})
        self.endpoint_a.consume(message)

    def shutdown(self):
        self.endpoint_a.shutdown()
        self.endpoint_b.shutdown()


@pytest.fixture()
def wire():
    connected = Wire()
    yield connected
    connected.shutdown()


def test_roundtrip_object_data(wire):
    data = {'reason': 'invalid field', 'details': {'field': 'name', 'attempts': [1, 2]}}

    def handler(_params):
        raise exceptions.JsonRpcInvalidParams(data=data)
    wire.endpoint_b._dispatcher['fail'] = handler

    future = wire.endpoint_a.request('fail')
    with pytest.raises(exceptions.JsonRpcInvalidParams) as exc_info:
        future.result(timeout=5)

    # The structured data arrived at the caller fully preserved
    assert exc_info.value.data == data
    assert exc_info.value.to_dict()['data'] == data


def test_roundtrip_explicit_null_data(wire):
    def handler(_params):
        raise exceptions.JsonRpcInvalidParams(data=None)
    wire.endpoint_b._dispatcher['fail'] = handler

    future = wire.endpoint_a.request('fail')
    with pytest.raises(exceptions.JsonRpcInvalidParams) as exc_info:
        future.result(timeout=5)

    # Explicit null survives the round trip as an explicit null member
    assert exc_info.value.data is None
    assert 'data' in exc_info.value.to_dict()
    assert exc_info.value.to_dict()['data'] is None


def test_roundtrip_absent_data(wire):
    def handler(_params):
        raise exceptions.JsonRpcInvalidParams()
    wire.endpoint_b._dispatcher['fail'] = handler

    future = wire.endpoint_a.request('fail')
    with pytest.raises(exceptions.JsonRpcInvalidParams) as exc_info:
        future.result(timeout=5)

    # Absent data stays absent (no 'data' member is synthesized)
    assert exc_info.value.data is exceptions.DATA_UNSET
    assert 'data' not in exc_info.value.to_dict()


def test_roundtrip_unserializable_data_does_not_crash_sender(wire):
    def async_handler():
        raise exceptions.JsonRpcInvalidParams(data={'not': 'serializable', 'bad': object()})
    # Returning a callable makes endpoint B compute (and fail) on its
    # executor pool, i.e. the error is produced on the sending thread.
    wire.endpoint_b._dispatcher['fail_async'] = lambda _params: async_handler

    future = wire.endpoint_a.request('fail_async')
    with pytest.raises(exceptions.JsonRpcException) as exc_info:
        future.result(timeout=5)

    # The caller received the existing internal error format instead of a
    # crashed send: internal error code plus traceback data.
    assert exc_info.value.code == exceptions.JsonRpcInternalError.CODE
    assert 'traceback' in exc_info.value.data

    # And endpoint B is still fully functional afterwards
    wire.endpoint_b._dispatcher['ping'] = lambda _params: 'pong'
    assert wire.endpoint_a.request('ping').result(timeout=5) == 'pong'


def test_wire_messages_fixture(wire, tmp_path):
    """Round-trip all three data shapes and dump the wire log to a temp file."""
    data = {'key': 'value', 'nested': {'n': 1}}
    outcomes = {}

    def make_handler(exc):
        def handler(_params):
            raise exc
        return handler

    for method, exc in [
        ('fail_object', exceptions.JsonRpcInvalidParams(data=data)),
        ('fail_null', exceptions.JsonRpcInvalidParams(data=None)),
        ('fail_absent', exceptions.JsonRpcInvalidParams()),
    ]:
        wire.endpoint_b._dispatcher[method] = make_handler(exc)
        future = wire.endpoint_a.request(method)
        with pytest.raises(exceptions.JsonRpcException) as exc_info:
            future.result(timeout=5)
        outcomes[method] = exc_info.value.to_dict()

    # Every message that crossed the wire must be JSON serializable
    artifact = tmp_path / 'wire_messages.json'
    artifact.write_text(json.dumps({
        'wire_messages': wire.messages,
        'parsed_errors': outcomes,
    }, indent=2, sort_keys=True))

    loaded = json.loads(artifact.read_text())
    assert loaded['parsed_errors']['fail_object']['data'] == data
    assert 'data' in loaded['parsed_errors']['fail_null']
    assert loaded['parsed_errors']['fail_null']['data'] is None
    assert 'data' not in loaded['parsed_errors']['fail_absent']
