# Copyright 2018 Palantir Technologies, Inc.
import logging
import threading
import uuid
import sys

from concurrent import futures
from .exceptions import (
    JsonRpcException,
    JsonRpcInternalError,
    JsonRpcMethodNotFound,
    JsonRpcRequestCancelled,
    JsonRpcServerStopped,
)

log = logging.getLogger(__name__)
JSONRPC_VERSION = '2.0'
CANCEL_METHOD = '$/cancelRequest'


class Endpoint(object):

    def __init__(self, dispatcher, consumer, id_generator=lambda: str(uuid.uuid4()),
                 max_workers=5, executor=None):
        """A JSON RPC endpoint for managing messages sent to/from the client.

        Args:
            dispatcher (dict): A dictionary of method name to handler function.
                The handler functions should return either the result or a callable that will be used to asynchronously
                compute the result.
            consumer (fn): A function that consumes JSON RPC message dicts and sends them to the client.
            id_generator (fn, optional): A function used to generate request IDs.
                Defaults to the string value of :func:`uuid.uuid4`.
            max_workers (int, optional): The number of workers in the asynchronous executor pool.
            executor (concurrent.futures.Executor, optional): An external executor used to run asynchronous
                handlers. When provided, the endpoint does not own an executor and never shuts it down.
        """
        self._dispatcher = dispatcher
        self._consumer = consumer
        self._id_generator = id_generator

        self._client_request_futures = {}
        self._server_request_futures = {}
        self._lock = threading.RLock()
        self._closed = False
        self._shutdown_exception = JsonRpcServerStopped()

        self._owns_executor = executor is None
        if executor is None:
            executor = futures.ThreadPoolExecutor(max_workers=max_workers)
        self._executor_service = executor

    def shutdown(self):
        """Shut down the executor service.

        Only an executor created by the endpoint itself is shut down; an externally provided
        executor is left running so its other clients are unaffected.
        """
        if self._owns_executor:
            self._executor_service.shutdown()

    def close(self):
        """Close the endpoint.

        Pending requests sent to the client are failed with a :class:`JsonRpcServerStopped`
        exception, and no further messages are written to the consumer. Late responses
        arriving after the close do not re-complete the already failed futures. Calling
        ``close`` more than once is safe. The executor service is not cancelled here; use
        :meth:`shutdown` for that.
        """
        with self._lock:
            if self._closed:
                log.debug('Endpoint already closed, ignoring repeated close')
                return
            self._closed = True

            request_futures = list(self._server_request_futures.values())
            self._server_request_futures.clear()
            # Detach in-flight client requests without cancelling their futures, which may be
            # owned by an external executor. Their callbacks must no longer write to the consumer.
            self._client_request_futures.clear()

        for request_future in request_futures:
            if not request_future.done():
                request_future.set_exception(self._shutdown_exception)

    def notify(self, method, params=None):
        """Send a JSON RPC notification to the client.

         Args:
             method (str): The method name of the notification to send
             params (any): The payload of the notification
         """
        log.debug('Sending notification: %s %s', method, params)

        with self._lock:
            if self._closed:
                log.debug('Endpoint is closed, dropping notification %s', method)
                return

            message = {
                'jsonrpc': JSONRPC_VERSION,
                'method': method,
            }
            if params is not None:
                message['params'] = params

            self._consumer(message)

    def request(self, method, params=None):
        """Send a JSON RPC request to the client.

        Args:
            method (str): The method name of the message to send
            params (any): The payload of the message

        Returns:
            Future that will resolve once a response has been received. If the endpoint has
            been closed, an already failed future carrying a :class:`JsonRpcServerStopped`
            exception is returned and the consumer is never called.
        """
        with self._lock:
            if self._closed:
                log.debug('Endpoint is closed, failing request %s', method)
                request_future = futures.Future()
                request_future.set_exception(self._shutdown_exception)
                return request_future

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
                self._consumer({
                    'jsonrpc': JSONRPC_VERSION,
                    'id': message['id'],
                    'error': e.to_dict()
                })
            except Exception:  # pylint: disable=broad-except
                log.exception("Failed to handle request %s", message['id'])
                self._consumer({
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
        with self._lock:
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
            with self._lock:
                if self._closed:
                    # The endpoint was closed while the synchronous part of the handler ran;
                    # do not register the future so its result cannot reach the consumer.
                    return
                self._client_request_futures[msg_id] = request_future
                request_future.add_done_callback(self._request_callback(msg_id))
        elif isinstance(handler_result, futures.Future):
            log.debug("Request handler is already a future %s", handler_result)
            with self._lock:
                if self._closed:
                    return
                self._client_request_futures[msg_id] = handler_result
                handler_result.add_done_callback(self._request_callback(msg_id))
        else:
            log.debug("Got result from synchronous request handler: %s", handler_result)
            self._consumer({
                'jsonrpc': JSONRPC_VERSION,
                'id': msg_id,
                'result': handler_result
            })

    def _request_callback(self, request_id):
        """Construct a request callback for the given request ID."""
        def callback(future):
            # Remove the future from the client requests map
            with self._lock:
                self._client_request_futures.pop(request_id, None)
                closed = self._closed

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

            if closed:
                log.debug("Endpoint is closed, dropping response for request %s", request_id)
                return

            self._consumer(message)

        return callback

    def _handle_response(self, msg_id, result=None, error=None):
        """Handle a response from the client."""
        with self._lock:
            request_future = self._server_request_futures.pop(msg_id, None)
            closed = self._closed

        if not request_future:
            log.warning("Received response to unknown message id %s", msg_id)
            return

        if closed or request_future.done():
            # The endpoint was closed and pending futures were already failed with the
            # shutdown exception; a late response must never re-complete the future.
            log.debug("Ignoring late response for message id %s", msg_id)
            return

        if error is not None:
            log.debug("Received error response to message %s: %s", msg_id, error)
            request_future.set_exception(JsonRpcException.from_dict(error))
            return

        log.debug("Received result for message %s: %s", msg_id, result)
        request_future.set_result(result)
