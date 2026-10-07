# Copyright 2018 Palantir Technologies, Inc.
import logging
import uuid
import sys
import threading

from concurrent import futures
from .exceptions import (
    JsonRpcException,
    JsonRpcRequestCancelled,
    JsonRpcEndpointClosed,
    JsonRpcInternalError,
    JsonRpcMethodNotFound,
)

log = logging.getLogger(__name__)
JSONRPC_VERSION = '2.0'
CANCEL_METHOD = '$/cancelRequest'


class Endpoint(object):

    def __init__(self, dispatcher, consumer, id_generator=lambda: str(uuid.uuid4()), max_workers=5):
        """A JSON RPC endpoint for managing messages sent to/from the client.

        Args:
            dispatcher (dict): A dictionary of method name to handler function.
                The handler functions should return either the result or a callable that will be used to asynchronously
                compute the result.
            consumer (fn): A function that consumes JSON RPC message dicts and sends them to the client.
            id_generator (fn, optional): A function used to generate request IDs.
                Defaults to the string value of :func:`uuid.uuid4`.
            max_workers (int, optional): The number of workers in the asynchronous executor pool.
        """
        self._dispatcher = dispatcher
        self._consumer = consumer
        self._id_generator = id_generator

        self._client_request_futures = {}
        self._server_request_futures = {}
        self._executor_service = futures.ThreadPoolExecutor(max_workers=max_workers)

        self._closed = False
        self._write_lock = threading.Lock()

    def shutdown(self):
        """Shut down the endpoint's own asynchronous executor pool.

        This only drains the internal service used to run callable notification/request
        handlers. It does not cancel request handlers that were supplied as an external
        :class:`~concurrent.futures.Future`.
        """
        self._executor_service.shutdown()

    def close(self):
        """Explicitly close the endpoint's RPC channel.

        All locally pending requests (requests this endpoint made and is still awaiting a
        response for) are completed with a :class:`JsonRpcEndpointClosed` exception. Once
        closed, no new message may be written to the consumer, and any response that
        arrives late is ignored rather than re-completing an already settled future.

        Calling this method more than once is safe. External executors and request handler
        futures created by the caller are never cancelled.
        """
        with self._write_lock:
            if self._closed:
                log.debug('Endpoint already closed, ignoring redundant close()')
                return
            self._closed = True
            # Snapshot and clear the pending requests while holding the lock so that a
            # concurrent late response can no longer observe these futures.
            pending_futures = self._server_request_futures
            self._server_request_futures = {}

        log.debug('Closing endpoint, failing %s pending request(s)', len(pending_futures))
        for msg_id, request_future in pending_futures.items():
            if not request_future.done():
                request_future.set_exception(JsonRpcEndpointClosed())
            else:
                log.debug('Pending request %s was already completed, leaving it untouched', msg_id)

    def _write(self, message):
        """Write a message to the consumer unless the endpoint has been closed."""
        with self._write_lock:
            if self._closed:
                log.debug('Endpoint is closed, dropping outgoing message %s', message)
                return False
            self._consumer(message)
            return True

    def notify(self, method, params=None):
        """Send a JSON RPC notification to the client.

         Args:
             method (str): The method name of the notification to send
             params (any): The payload of the notification
         """
        log.debug('Sending notification: %s %s', method, params)

        message = {
            'jsonrpc': JSONRPC_VERSION,
            'method': method,
        }
        if params is not None:
            message['params'] = params

        self._write(message)

    def request(self, method, params=None):
        """Send a JSON RPC request to the client.

        Args:
            method (str): The method name of the message to send
            params (any): The payload of the message

        Returns:
            Future that will resolve once a response has been received. If the endpoint
            has been closed, an already failed future carrying a
            :class:`JsonRpcEndpointClosed` exception is returned and nothing is written
            to the consumer.
        """
        msg_id = self._id_generator()
        log.debug('Sending request with id %s: %s %s', msg_id, method, params)

        message = {
            'jsonrpc': JSONRPC_VERSION,
            'id': msg_id,
            'method': method,
        }
        if params is not None:
            message['params'] = params

        request_future = futures.Future()
        request_future.add_done_callback(self._cancel_callback(msg_id))

        with self._write_lock:
            if self._closed:
                # Do not register the future or write to the consumer once closed.
                request_future.set_exception(JsonRpcEndpointClosed())
                return request_future
            self._server_request_futures[msg_id] = request_future
            self._consumer(message)

        return request_future

    def _cancel_callback(self, request_id):
        """Construct a cancellation callback for the given request ID."""
        def callback(future):
            if future.cancelled():
                self.notify(CANCEL_METHOD, {'id': request_id})
                future.set_exception(JsonRpcRequestCancelled())
        return callback

    def consume(self, message):
        """Consume a JSON RPC message from the client.

        Args:
            message (dict): The JSON RPC message sent by the client
        """
        if self._closed:
            log.debug('Endpoint is closed, ignoring incoming message %s', message)
            return

        if 'jsonrpc' not in message or message['jsonrpc'] != JSONRPC_VERSION:
            log.warning("Unknown message type %s", message)
            return

        if 'id' not in message:
            log.debug("Handling notification from client %s", message)
            self._handle_notification(message['method'], message.get('params'))
        elif 'method' not in message:
            log.debug("Handling response from client %s", message)
            self._handle_response(message['id'], message.get('result'), message.get('error'))
        else:
            try:
                log.debug("Handling request from client %s", message)
                self._handle_request(message['id'], message['method'], message.get('params'))
            except JsonRpcException as e:
                log.exception("Failed to handle request %s", message['id'])
                self._write({
                    'jsonrpc': JSONRPC_VERSION,
                    'id': message['id'],
                    'error': e.to_dict()
                })
            except Exception:  # pylint: disable=broad-except
                log.exception("Failed to handle request %s", message['id'])
                self._write({
                    'jsonrpc': JSONRPC_VERSION,
                    'id': message['id'],
                    'error': JsonRpcInternalError.of(sys.exc_info()).to_dict()
                })

    def _handle_notification(self, method, params):
        """Handle a notification from the client."""
        if method == CANCEL_METHOD:
            self._handle_cancel_notification(params['id'])
            return

        try:
            handler = self._dispatcher[method]
        except KeyError:
            log.warning("Ignoring notification for unknown method %s", method)
            return

        try:
            handler_result = handler(params)
        except Exception:  # pylint: disable=broad-except
            log.exception("Failed to handle notification %s: %s", method, params)
            return

        if callable(handler_result):
            log.debug("Executing async notification handler %s", handler_result)
            notification_future = self._executor_service.submit(handler_result)
            notification_future.add_done_callback(self._notification_callback(method, params))

    @staticmethod
    def _notification_callback(method, params):
        """Construct a notification callback for the given request ID."""
        def callback(future):
            try:
                future.result()
                log.debug("Successfully handled async notification %s %s", method, params)
            except Exception:  # pylint: disable=broad-except
                log.exception("Failed to handle async notification %s %s", method, params)
        return callback

    def _handle_cancel_notification(self, msg_id):
        """Handle a cancel notification from the client."""
        request_future = self._client_request_futures.pop(msg_id, None)

        if not request_future:
            log.warning("Received cancel notification for unknown message id %s", msg_id)
            return

        # Will only work if the request hasn't started executing
        if request_future.cancel():
            log.debug("Cancelled request with id %s", msg_id)

    def _handle_request(self, msg_id, method, params):
        """Handle a request from the client."""
        try:
            handler = self._dispatcher[method]
        except KeyError:
            raise JsonRpcMethodNotFound.of(method)

        handler_result = handler(params)

        if callable(handler_result):
            log.debug("Executing async request handler %s", handler_result)
            request_future = self._executor_service.submit(handler_result)
            self._client_request_futures[msg_id] = request_future
            request_future.add_done_callback(self._request_callback(msg_id))
        elif isinstance(handler_result, futures.Future):
            log.debug("Request handler is already a future %s", handler_result)
            self._client_request_futures[msg_id] = handler_result
            handler_result.add_done_callback(self._request_callback(msg_id))
        else:
            log.debug("Got result from synchronous request handler: %s", handler_result)
            self._write({
                'jsonrpc': JSONRPC_VERSION,
                'id': msg_id,
                'result': handler_result
            })

    def _request_callback(self, request_id):
        """Construct a request callback for the given request ID."""
        def callback(future):
            # Remove the future from the client requests map
            self._client_request_futures.pop(request_id, None)

            if future.cancelled():
                future.set_exception(JsonRpcRequestCancelled())

            message = {
                'jsonrpc': JSONRPC_VERSION,
                'id': request_id,
            }

            try:
                message['result'] = future.result()
            except JsonRpcException as e:
                log.exception("Failed to handle request %s", request_id)
                message['error'] = e.to_dict()
            except Exception:  # pylint: disable=broad-except
                log.exception("Failed to handle request %s", request_id)
                message['error'] = JsonRpcInternalError.of(sys.exc_info()).to_dict()

            self._write(message)

        return callback

    def _handle_response(self, msg_id, result=None, error=None):
        """Handle a response from the client."""
        request_future = self._server_request_futures.pop(msg_id, None)

        if not request_future:
            log.warning("Received response to unknown message id %s", msg_id)
            return

        if error is not None:
            log.debug("Received error response to message %s: %s", msg_id, error)
            request_future.set_exception(JsonRpcException.from_dict(error))
            return

        log.debug("Received result for message %s: %s", msg_id, result)
        request_future.set_result(result)
