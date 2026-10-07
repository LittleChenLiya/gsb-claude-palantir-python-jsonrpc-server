# Copyright 2018 Palantir Technologies, Inc.
# pylint: disable=redefined-outer-name
"""Tests for the JSON RPC error ``data`` pass-through convention.

Covers the local exception mapping (optional structured ``data``, JSON
serializability fallback, missing vs explicit null) and its wiring through
two in-memory endpoints connected directly to each other.
"""
import pytest

from pyls_jsonrpc import exceptions
from pyls_jsonrpc.endpoint import Endpoint

OBJECT_DATA = {'key': 'value', 'nested': {'list': [1, 2, 3]}, 'flag': True}


@pytest.fixture()
def connected_endpoints():
    """Two in-memory endpoints whose consumers feed each other directly."""
    dispatcher_a = {}
    endpoint_a = Endpoint(dispatcher_a, consumer=lambda message: endpoint_b.consume(message))
    endpoint_b = Endpoint({}, consumer=lambda message: endpoint_a.consume(message))
    yield endpoint_a, endpoint_b, dispatcher_a
    endpoint_a.shutdown()
    endpoint_b.shutdown()


def serve_error(dispatcher, exception, method='fail'):
    """Register a synchronous handler raising the given exception."""
    def handler(_params):
        raise exception
    dispatcher[method] = handler


#
# Local exception mapping
#

def test_to_dict_omits_missing_data():
    assert 'data' not in exceptions.JsonRpcMethodNotFound().to_dict()


def test_to_dict_keeps_explicit_null_data():
    error_dict = exceptions.JsonRpcMethodNotFound(data=None).to_dict()
    assert 'data' in error_dict
    assert error_dict['data'] is None


def test_to_dict_keeps_structured_data():
    error_dict = exceptions.JsonRpcServerError(
        message='boom', code=-32000, data=OBJECT_DATA).to_dict()
    assert error_dict == {'code': -32000, 'message': 'boom', 'data': OBJECT_DATA}


def test_from_dict_preserves_structured_data():
    error = {'code': -32000, 'message': 'boom', 'data': OBJECT_DATA}
    exc = exceptions.JsonRpcException.from_dict(error)
    assert isinstance(exc, exceptions.JsonRpcServerError)
    assert exc.data == OBJECT_DATA
    assert exc.to_dict() == error


def test_from_dict_distinguishes_null_and_missing_data():
    null_exc = exceptions.JsonRpcException.from_dict(
        {'code': -32600, 'message': 'Invalid Request', 'data': None})
    missing_exc = exceptions.JsonRpcException.from_dict(
        {'code': -32600, 'message': 'Invalid Request'})

    assert null_exc.data is None
    assert 'data' in null_exc.to_dict()
    assert null_exc.to_dict()['data'] is None

    assert missing_exc.data is exceptions.DATA_MISSING
    assert 'data' not in missing_exc.to_dict()


#
# End-to-end over two in-memory endpoints
#

def request_error(endpoint_b, method='fail'):
    future = endpoint_b.request(method)
    with pytest.raises(exceptions.JsonRpcException) as exc_info:
        future.result(timeout=5)
    return exc_info.value


def test_roundtrip_object_data(connected_endpoints):
    _endpoint_a, endpoint_b, dispatcher_a = connected_endpoints
    serve_error(dispatcher_a, exceptions.JsonRpcServerError(
        message='boom', code=-32000, data=OBJECT_DATA))

    exc = request_error(endpoint_b)
    assert isinstance(exc, exceptions.JsonRpcServerError)
    assert exc.code == -32000
    assert exc.message == 'boom'
    assert exc.data == OBJECT_DATA
    assert exc.to_dict()['data'] == OBJECT_DATA


def test_roundtrip_explicit_null_data(connected_endpoints):
    _endpoint_a, endpoint_b, dispatcher_a = connected_endpoints
    serve_error(dispatcher_a, exceptions.JsonRpcInvalidParams(data=None))

    exc = request_error(endpoint_b)
    assert isinstance(exc, exceptions.JsonRpcInvalidParams)
    assert exc.data is None
    assert 'data' in exc.to_dict()
    assert exc.to_dict()['data'] is None


def test_roundtrip_missing_data(connected_endpoints):
    _endpoint_a, endpoint_b, dispatcher_a = connected_endpoints
    serve_error(dispatcher_a, exceptions.JsonRpcInvalidParams())

    exc = request_error(endpoint_b)
    assert isinstance(exc, exceptions.JsonRpcInvalidParams)
    assert exc.data is exceptions.DATA_MISSING
    assert 'data' not in exc.to_dict()


def test_unserializable_data_falls_back_to_internal_error_sync(connected_endpoints):
    _endpoint_a, endpoint_b, dispatcher_a = connected_endpoints
    serve_error(dispatcher_a, exceptions.JsonRpcServerError(
        message='boom', code=-32000, data={'bad': object()}))

    exc = request_error(endpoint_b)
    # Falls back to the existing internal error format (code + traceback data)
    assert exc.code == exceptions.JsonRpcInternalError.CODE
    assert 'traceback' in exc.data


def test_unserializable_data_does_not_crash_sending_thread(connected_endpoints):
    """An async handler raising an error with unserializable data must not kill
    the endpoint's message-sending (executor) thread; the peer receives an
    internal error and the endpoint keeps serving further requests."""
    endpoint_a, endpoint_b, dispatcher_a = connected_endpoints

    def handler(_params):
        def _async_handler():
            raise exceptions.JsonRpcServerError(
                message='boom', code=-32000, data={'bad': object()})
        return _async_handler
    dispatcher_a['fail'] = handler

    exc = request_error(endpoint_b)
    assert exc.code == exceptions.JsonRpcInternalError.CODE
    assert 'traceback' in exc.data

    # The sending thread survived: endpoint A still answers new requests
    dispatcher_a['ok'] = lambda _params: 42
    assert endpoint_b.request('ok').result(timeout=5) == 42
