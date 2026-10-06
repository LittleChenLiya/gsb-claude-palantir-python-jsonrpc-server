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


def test_consume_batch_mixed(dispatcher):
    # Two valid requests, one notification and one invalid item, interleaved.
    handler_a = mock.Mock(return_value='result-a')
    handler_b = mock.Mock(return_value='result-b')
    notify_handler = mock.Mock()
    dispatcher['requestA'] = handler_a
    dispatcher['requestB'] = handler_b
    dispatcher['notifyMethod'] = notify_handler
    consumer = mock.MagicMock()
    ep = Endpoint(dispatcher, consumer)

    ep.consume_batch([
        {'jsonrpc': '2.0', 'id': 1, 'method': 'requestA', 'params': {'key': 'a'}},
        {'jsonrpc': '2.0', 'method': 'notifyMethod', 'params': {'key': 'n'}},
        'not a valid request',
        {'jsonrpc': '2.0', 'id': 2, 'method': 'requestB'},
    ])

    # The invalid item must not cancel the valid requests: each handler runs once.
    handler_a.assert_called_once_with({'key': 'a'})
    handler_b.assert_called_once_with(None)
    notify_handler.assert_called_once_with({'key': 'n'})

    # Notifications and invalid items produce no response; order follows input positions.
    consumer.assert_called_once_with([
        {'jsonrpc': '2.0', 'id': 1, 'result': 'result-a'},
        {'jsonrpc': '2.0', 'id': 2, 'result': 'result-b'},
    ])


def test_consume_batch_invalid_item_keeps_other_requests(dispatcher):
    # A method-not-found request is still an invalid-ish item for a batch: its failure
    # must not prevent the later valid request from being processed and answered.
    handler_b = mock.Mock(return_value='result-b')
    dispatcher['requestB'] = handler_b
    consumer = mock.MagicMock()
    ep = Endpoint(dispatcher, consumer)

    ep.consume_batch([
        {'jsonrpc': '2.0', 'id': 1, 'method': 'missingMethod'},
        {'jsonrpc': '2.0', 'id': 2, 'method': 'requestB'},
    ])

    handler_b.assert_called_once_with(None)
    consumer.assert_called_once_with([
        {
            'jsonrpc': '2.0',
            'id': 1,
            'error': exceptions.JsonRpcMethodNotFound.of('missingMethod').to_dict(),
        },
        {'jsonrpc': '2.0', 'id': 2, 'result': 'result-b'},
    ])


def test_consume_batch_all_notifications(dispatcher):
    notify_a = mock.Mock()
    notify_b = mock.Mock()
    dispatcher['notifyA'] = notify_a
    dispatcher['notifyB'] = notify_b
    consumer = mock.MagicMock()
    ep = Endpoint(dispatcher, consumer)

    ep.consume_batch([
        {'jsonrpc': '2.0', 'method': 'notifyA', 'params': {'key': 'a'}},
        {'jsonrpc': '2.0', 'method': 'notifyB'},
    ])

    notify_a.assert_called_once_with({'key': 'a'})
    notify_b.assert_called_once_with(None)
    # All notifications (plus an invalid item mixed in) must produce no output at all.
    ep.consume_batch([
        {'jsonrpc': '2.0', 'method': 'notifyA'},
        {'jsonrpc': '1.0', 'method': 'wrongVersion'},
        42,
    ])
    assert consumer.call_count == 0


def test_consume_batch_empty(dispatcher):
    consumer = mock.MagicMock()
    ep = Endpoint(dispatcher, consumer)

    ep.consume_batch([])

    consumer.assert_called_once_with({
        'jsonrpc': '2.0',
        'id': None,
        'error': exceptions.JsonRpcInvalidRequest().to_dict(),
    })


def test_consume_routes_list_to_batch(dispatcher):
    # The original single-message entry is retained, but a list is handled as a batch.
    handler = mock.Mock(return_value=1234)
    dispatcher['methodName'] = handler
    consumer = mock.MagicMock()
    ep = Endpoint(dispatcher, consumer)

    ep.consume([
        {'jsonrpc': '2.0', 'id': MSG_ID, 'method': 'methodName'},
        {'jsonrpc': '2.0', 'method': 'someNotification'},
    ])

    handler.assert_called_once_with(None)
    consumer.assert_called_once_with([
        {'jsonrpc': '2.0', 'id': MSG_ID, 'result': 1234},
    ])


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
