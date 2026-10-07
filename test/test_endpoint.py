# Copyright 2018 Palantir Technologies, Inc.
# pylint: disable=redefined-outer-name
from concurrent import futures
import json
import time
import mock
import pytest

from pyls_jsonrpc import exceptions
from pyls_jsonrpc.endpoint import Endpoint

MSG_ID = 'id'


@pytest.fixture()
def dispatcher():
    return {}


@pytest.fixture()
def consumer():
    return mock.MagicMock()


@pytest.fixture()
def endpoint(dispatcher, consumer):
    return Endpoint(dispatcher, consumer, id_generator=lambda: MSG_ID)


def test_bad_message(endpoint):
    # Ensure doesn't raise for a bad message
    endpoint.consume({'key': 'value'})


def test_notify(endpoint, consumer):
    endpoint.notify('methodName', {'key': 'value'})
    consumer.assert_called_once_with({
        'jsonrpc': '2.0',
        'method': 'methodName',
        'params': {'key': 'value'}
    })


def test_notify_none_params(endpoint, consumer):
    endpoint.notify('methodName', None)
    consumer.assert_called_once_with({
        'jsonrpc': '2.0',
        'method': 'methodName',
    })


def test_request(endpoint, consumer):
    future = endpoint.request('methodName', {'key': 'value'})
    assert not future.done()

    consumer.assert_called_once_with({
        'jsonrpc': '2.0',
        'id': MSG_ID,
        'method': 'methodName',
        'params': {'key': 'value'}
    })

    # Send the response back to the endpoint
    result = 1234
    endpoint.consume({
        'jsonrpc': '2.0',
        'id': MSG_ID,
        'result': result
    })

    assert future.result(timeout=2) == result


def test_request_error(endpoint, consumer):
    future = endpoint.request('methodName', {'key': 'value'})
    assert not future.done()

    consumer.assert_called_once_with({
        'jsonrpc': '2.0',
        'id': MSG_ID,
        'method': 'methodName',
        'params': {'key': 'value'}
    })

    # Send an error back from the client
    error = exceptions.JsonRpcInvalidRequest(data=1234)
    endpoint.consume({
        'jsonrpc': '2.0',
        'id': MSG_ID,
        'error': error.to_dict()
    })

    # Verify the exception raised by the future is the same as the error the client serialized
    with pytest.raises(exceptions.JsonRpcException) as exc_info:
        assert future.result(timeout=2)
    assert exc_info.type == exceptions.JsonRpcInvalidRequest
    assert exc_info.value == error


def test_request_cancel(endpoint, consumer):
    future = endpoint.request('methodName', {'key': 'value'})
    assert not future.done()

    consumer.assert_called_once_with({
        'jsonrpc': '2.0',
        'id': MSG_ID,
        'method': 'methodName',
        'params': {'key': 'value'}
    })

    # Cancel the request
    future.cancel()
    consumer.assert_any_call({
        'jsonrpc': '2.0',
        'method': '$/cancelRequest',
        'params': {'id': MSG_ID}
    })

    with pytest.raises((exceptions.JsonRpcException, futures.CancelledError)) as exc_info:
        assert future.result(timeout=2)
    assert exc_info.type in (exceptions.JsonRpcRequestCancelled, futures.CancelledError)


def test_consume_notification(endpoint, dispatcher):
    handler = mock.Mock()
    dispatcher['methodName'] = handler

    endpoint.consume({
        'jsonrpc': '2.0',
        'method': 'methodName',
        'params': {'key': 'value'}
    })
    handler.assert_called_once_with({'key': 'value'})


def test_consume_notification_error(endpoint, dispatcher):
    handler = mock.Mock(side_effect=ValueError)
    dispatcher['methodName'] = handler
    # Verify the consume doesn't throw
    endpoint.consume({
        'jsonrpc': '2.0',
        'method': 'methodName',
        'params': {'key': 'value'}
    })
    handler.assert_called_once_with({'key': 'value'})


def test_consume_notification_method_not_found(endpoint):
    # Verify consume doesn't throw for method not found
    endpoint.consume({
        'jsonrpc': '2.0',
        'method': 'methodName',
        'params': {'key': 'value'}
    })


def test_consume_async_notification_error(endpoint, dispatcher):
    def _async_handler():
        raise ValueError()
    handler = mock.Mock(return_value=_async_handler)
    dispatcher['methodName'] = handler

    # Verify the consume doesn't throw
    endpoint.consume({
        'jsonrpc': '2.0',
        'method': 'methodName',
        'params': {'key': 'value'}
    })
    handler.assert_called_once_with({'key': 'value'})


def test_consume_request(endpoint, consumer, dispatcher):
    result = 1234
    handler = mock.Mock(return_value=result)
    dispatcher['methodName'] = handler

    endpoint.consume({
        'jsonrpc': '2.0',
        'id': MSG_ID,
        'method': 'methodName',
        'params': {'key': 'value'}
    })

    handler.assert_called_once_with({'key': 'value'})
    consumer.assert_called_once_with({
        'jsonrpc': '2.0',
        'id': MSG_ID,
        'result': result
    })


def test_consume_future_request(endpoint, consumer, dispatcher):
    future_response = futures.ThreadPoolExecutor().submit(lambda: 1234)
    handler = mock.Mock(return_value=future_response)
    dispatcher['methodName'] = handler

    endpoint.consume({
        'jsonrpc': '2.0',
        'id': MSG_ID,
        'method': 'methodName',
        'params': {'key': 'value'}
    })

    handler.assert_called_once_with({'key': 'value'})
    await_assertion(lambda: consumer.assert_called_once_with({
        'jsonrpc': '2.0',
        'id': MSG_ID,
        'result': 1234
    }))


def test_consume_async_request(endpoint, consumer, dispatcher):
    def _async_handler():
        return 1234
    handler = mock.Mock(return_value=_async_handler)
    dispatcher['methodName'] = handler

    endpoint.consume({
        'jsonrpc': '2.0',
        'id': MSG_ID,
        'method': 'methodName',
        'params': {'key': 'value'}
    })

    handler.assert_called_once_with({'key': 'value'})
    await_assertion(lambda: consumer.assert_called_once_with({
        'jsonrpc': '2.0',
        'id': MSG_ID,
        'result': 1234
    }))


@pytest.mark.parametrize('exc_type, error', [
    (ValueError, exceptions.JsonRpcInternalError(message='ValueError')),
    (KeyError, exceptions.JsonRpcInternalError(message='KeyError')),
    (exceptions.JsonRpcMethodNotFound, exceptions.JsonRpcMethodNotFound()),
])
def test_consume_async_request_error(exc_type, error, endpoint, consumer, dispatcher):
    def _async_handler():
        raise exc_type()
    handler = mock.Mock(return_value=_async_handler)
    dispatcher['methodName'] = handler

    endpoint.consume({
        'jsonrpc': '2.0',
        'id': MSG_ID,
        'method': 'methodName',
        'params': {'key': 'value'}
    })

    handler.assert_called_once_with({'key': 'value'})
    await_assertion(lambda: assert_consumer_error(consumer, error))


def test_consume_request_method_not_found(endpoint, consumer):
    endpoint.consume({
        'jsonrpc': '2.0',
        'id': MSG_ID,
        'method': 'methodName',
        'params': {'key': 'value'}
    })
    assert_consumer_error(consumer, exceptions.JsonRpcMethodNotFound.of('methodName'))


@pytest.mark.parametrize('exc_type, error', [
    (ValueError, exceptions.JsonRpcInternalError(message='ValueError')),
    (KeyError, exceptions.JsonRpcInternalError(message='KeyError')),
    (exceptions.JsonRpcMethodNotFound, exceptions.JsonRpcMethodNotFound()),
])
def test_consume_request_error(exc_type, error, endpoint, consumer, dispatcher):
    handler = mock.Mock(side_effect=exc_type)
    dispatcher['methodName'] = handler

    endpoint.consume({
        'jsonrpc': '2.0',
        'id': MSG_ID,
        'method': 'methodName',
        'params': {'key': 'value'}
    })

    handler.assert_called_once_with({'key': 'value'})
    await_assertion(lambda: assert_consumer_error(consumer, error))


def test_consume_request_cancel(endpoint, dispatcher):
    def async_handler():
        time.sleep(3)
    handler = mock.Mock(return_value=async_handler)
    dispatcher['methodName'] = handler

    endpoint.consume({
        'jsonrpc': '2.0',
        'id': MSG_ID,
        'method': 'methodName',
        'params': {'key': 'value'}
    })
    handler.assert_called_once_with({'key': 'value'})

    endpoint.consume({
        'jsonrpc': '2.0',
        'method': '$/cancelRequest',
        'params': {'id': MSG_ID}
    })

    # Because Python's Future cannot be cancelled once it's started, the request is never actually cancelled
    # consumer.assert_called_once_with({
    #     'jsonrpc': '2.0',
    #     'id': MSG_ID,
    #     'error': exceptions.JsonRpcRequestCancelled().to_dict()
    # })


def test_consume_request_cancel_unknown(endpoint):
    # Verify consume doesn't throw
    endpoint.consume({
        'jsonrpc': '2.0',
        'method': '$/cancelRequest',
        'params': {'id': 'unknown identifier'}
    })


def test_consume_request_error_structured_data(endpoint, consumer, dispatcher):
    data = {'reason': 'bad params', 'details': [1, 'two']}
    handler = mock.Mock(side_effect=exceptions.JsonRpcInvalidParams(data=data))
    dispatcher['methodName'] = handler

    endpoint.consume({
        'jsonrpc': '2.0',
        'id': MSG_ID,
        'method': 'methodName',
        'params': {'key': 'value'}
    })

    consumer.assert_called_once_with({
        'jsonrpc': '2.0',
        'id': MSG_ID,
        'error': {
            'code': exceptions.JsonRpcInvalidParams.CODE,
            'message': exceptions.JsonRpcInvalidParams.MESSAGE,
            'data': data,
        }
    })


def test_consume_request_error_explicit_null_data(endpoint, consumer, dispatcher):
    handler = mock.Mock(side_effect=exceptions.JsonRpcInvalidParams(data=None))
    dispatcher['methodName'] = handler

    endpoint.consume({
        'jsonrpc': '2.0',
        'id': MSG_ID,
        'method': 'methodName',
        'params': {'key': 'value'}
    })

    _name, args, _kwargs = consumer.mock_calls[0]
    assert 'data' in args[0]['error']
    assert args[0]['error']['data'] is None


def test_consume_request_error_unset_data(endpoint, consumer, dispatcher):
    handler = mock.Mock(side_effect=exceptions.JsonRpcInvalidParams())
    dispatcher['methodName'] = handler

    endpoint.consume({
        'jsonrpc': '2.0',
        'id': MSG_ID,
        'method': 'methodName',
        'params': {'key': 'value'}
    })

    _name, args, _kwargs = consumer.mock_calls[0]
    assert 'data' not in args[0]['error']


def test_consume_request_error_unserializable_data(endpoint, consumer, dispatcher):
    # A non JSON-serializable data member falls back to the internal error
    # format instead of crashing the consuming thread.
    handler = mock.Mock(side_effect=exceptions.JsonRpcInvalidParams(data=object()))
    dispatcher['methodName'] = handler

    endpoint.consume({
        'jsonrpc': '2.0',
        'id': MSG_ID,
        'method': 'methodName',
        'params': {'key': 'value'}
    })

    assert_internal_error_fallback(consumer)


def test_consume_async_request_error_unserializable_data(endpoint, consumer, dispatcher):
    # The fallback must also hold on the executor thread sending async results.
    def _async_handler():
        raise exceptions.JsonRpcInvalidParams(data={'bad': set([1, 2])})
    handler = mock.Mock(return_value=_async_handler)
    dispatcher['methodName'] = handler

    endpoint.consume({
        'jsonrpc': '2.0',
        'id': MSG_ID,
        'method': 'methodName',
        'params': {'key': 'value'}
    })

    await_assertion(lambda: assert_internal_error_fallback(consumer))


def assert_internal_error_fallback(consumer_mock):
    """Assert the consumer received exactly one error in the internal error format."""
    assert len(consumer_mock.mock_calls) == 1
    _name, args, _kwargs = consumer_mock.mock_calls[0]
    error = args[0]['error']
    assert error['code'] == exceptions.JsonRpcInternalError.CODE
    assert 'traceback' in error['data']
    # The fallback error itself must be JSON serializable
    json.dumps(error)


def assert_consumer_error(consumer_mock, exception):
    """Assert that the consumer mock has had once call with the given error message and code.

    The error's data part is not compared since it contains the traceback.
    """
    assert len(consumer_mock.mock_calls) == 1
    _name, args, _kwargs = consumer_mock.mock_calls[0]
    assert args[0]['error']['message'] == exception.message
    assert args[0]['error']['code'] == exception.code


def await_assertion(condition, timeout=3.0, interval=0.1, exc=None):
    if timeout <= 0:
        raise exc if exc else AssertionError("Failed to wait for condition %s" % condition)
    try:
        condition()
    except AssertionError as e:
        time.sleep(interval)
        await_assertion(condition, timeout=(timeout - interval), interval=interval, exc=e)
