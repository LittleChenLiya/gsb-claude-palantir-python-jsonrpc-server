# Copyright 2018 Palantir Technologies, Inc.
"""Tests for optional structured error data on local exception mapping."""
import json

from pyls_jsonrpc import exceptions
from pyls_jsonrpc.exceptions import DATA_UNSET, JsonRpcException, is_json_serializable


def test_data_defaults_to_unset_sentinel():
    exc = JsonRpcException(message='msg', code=1)
    assert exc.data is DATA_UNSET
    assert 'data' not in exc.to_dict()


def test_explicit_none_data_is_serialized_as_null():
    exc = JsonRpcException(message='msg', code=1, data=None)
    error_dict = exc.to_dict()
    assert 'data' in error_dict
    assert error_dict['data'] is None
    # And it survives an actual JSON round trip as an explicit null member
    assert json.loads(json.dumps(error_dict))['data'] is None


def test_structured_data_is_serialized_verbatim():
    data = {'reason': 'bad params', 'details': [1, 'two', {'three': 3.0}], 'flag': True}
    exc = JsonRpcException(message='msg', code=1, data=data)
    assert exc.to_dict()['data'] == data


def test_from_dict_preserves_structured_data():
    error = {'code': -32602, 'message': 'Invalid Params', 'data': {'field': 'x', 'count': 2}}
    exc = JsonRpcException.from_dict(error)
    assert isinstance(exc, exceptions.JsonRpcInvalidParams)
    assert exc.data == {'field': 'x', 'count': 2}
    assert exc.to_dict() == error


def test_from_dict_distinguishes_null_and_absent_data():
    with_null = JsonRpcException.from_dict({'code': -32600, 'message': 'Invalid Request', 'data': None})
    without = JsonRpcException.from_dict({'code': -32600, 'message': 'Invalid Request'})

    assert 'data' in with_null.to_dict()
    assert with_null.to_dict()['data'] is None

    assert without.data is DATA_UNSET
    assert 'data' not in without.to_dict()


def test_server_error_data_optional():
    assert 'data' not in exceptions.JsonRpcServerError(message='m', code=-32000).to_dict()
    null_data = exceptions.JsonRpcServerError(message='m', code=-32000, data=None)
    assert 'data' in null_data.to_dict()
    assert null_data.to_dict()['data'] is None


def test_internal_error_of_still_carries_traceback_data():
    try:
        raise ValueError('boom')
    except ValueError:
        import sys
        exc = exceptions.JsonRpcInternalError.of(sys.exc_info())
    assert 'traceback' in exc.to_dict()['data']


def test_is_json_serializable():
    assert is_json_serializable({'a': [1, None, True, 's', 1.5]})
    assert not is_json_serializable(object())
    assert not is_json_serializable({'bad': set([1, 2])})
    assert not is_json_serializable({'code': 1, 'message': 'm', 'data': lambda: None})
