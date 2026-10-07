# Copyright 2018 Palantir Technologies, Inc.
# pylint: disable=redefined-outer-name
from concurrent import futures
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


def _endpoint_with_ids(consumer, dispatcher=None):
    """Build an endpoint whose request ids are taken from a deterministic iterator."""
    ids = iter(['1', '2', '3', '4'])
    return Endpoint(dispatcher if dispatcher is not None else {}, consumer, id_generator=lambda: next(ids))


def _assert_closed(future):
    """Assert a future has been completed with the endpoint closed exception."""
    with pytest.raises(exceptions.JsonRpcEndpointClosed):
        future.result(timeout=2)
    assert future.done()
    assert not future.cancelled()


def test_close_fails_pending_requests(consumer):
    # Two locally pending requests (awaiting a response from the client)
    endpoint = _endpoint_with_ids(consumer)
    first = endpoint.request('firstMethod', {'key': 'one'})
    second = endpoint.request('secondMethod', {'key': 'two'})
    assert not first.done() and not second.done()

    # Closing completes both pending requests with the unified shutdown exception
    endpoint.close()
    _assert_closed(first)
    _assert_closed(second)

    # Nothing new is written to the consumer as a result of closing
    consumer.reset_mock()
    endpoint.consume({
        'jsonrpc': '2.0',
        'id': '1',
        'result': 'late'
    })
    consumer.assert_not_called()
    assert endpoint._server_request_futures == {}


def test_close_late_response_does_not_recomplete(consumer):
    endpoint = _endpoint_with_ids(consumer)
    future = endpoint.request('methodName', {'key': 'value'})

    endpoint.close()
    _assert_closed(future)
    consumer.reset_mock()

    # A response arriving late (after the shutdown) must be ignored, it cannot re-complete
    # the future with a result or flip its exception.
    endpoint.consume({
        'jsonrpc': '2.0',
        'id': '1',
        'result': 1234
    })
    endpoint.consume({
        'jsonrpc': '2.0',
        'id': '1',
        'error': exceptions.JsonRpcInvalidRequest().to_dict()
    })

    with pytest.raises(exceptions.JsonRpcEndpointClosed):
        future.result(timeout=2)
    consumer.assert_not_called()


def test_send_after_close_is_blocked(consumer):
    endpoint = _endpoint_with_ids(consumer)
    endpoint.close()
    consumer.reset_mock()

    # A notification after close must not reach the consumer
    endpoint.notify('methodName', {'key': 'value'})

    # A request after close returns an already failed future and is not written either
    future = endpoint.request('methodName', {'key': 'value'})
    _assert_closed(future)

    consumer.assert_not_called()
    assert endpoint._server_request_futures == {}


def test_close_is_idempotent_and_does_not_cancel_external_future(consumer, dispatcher):
    endpoint = _endpoint_with_ids(consumer, dispatcher=dispatcher)

    # A request whose handler is backed by an externally owned future (and its executor);
    # the endpoint must never cancel it on close.
    external_future = futures.Future()
    handler = mock.Mock(return_value=external_future)
    dispatcher['methodName'] = handler

    endpoint.consume({
        'jsonrpc': '2.0',
        'id': 'client-id',
        'method': 'methodName',
        'params': {'key': 'value'}
    })

    # Repeated close() calls are safe and do not raise
    endpoint.close()
    endpoint.close()
    endpoint.close()

    # The external future/executor is left untouched
    assert not external_future.cancelled()
    assert not external_future.done()

    # Allowing the external work to finish later must not write to the closed consumer
    consumer.reset_mock()
    external_future.set_result(1234)
    consumer.assert_not_called()


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
