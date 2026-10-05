# Copyright 2018 Palantir Technologies, Inc.
import logging
import threading

try:
    import ujson as json
except Exception:  # pylint: disable=broad-except
    import json

log = logging.getLogger(__name__)

#: Default maximum size of the header section (bytes). Headers are tiny in
#: practice; anything larger is almost certainly a desynced or malicious peer.
DEFAULT_MAX_HEADER_SIZE = 2 ** 16  # 64 KiB

#: Default maximum declared body size (bytes). Bodies larger than this are
#: rejected before any body buffer is allocated.
DEFAULT_MAX_CONTENT_LENGTH = 2 ** 24  # 16 MiB

_HEADER_TERMINATOR = b'\r\n\r\n'
_HEADER_SEPARATOR = b'\r\n'


class JsonRpcFramingError(ValueError):
    """Base class for JSON-RPC transport framing errors.

    A framing error means the byte stream has lost its frame boundaries; the
    connection can no longer be trusted to deliver complete messages.
    """


class JsonRpcInvalidContentLength(JsonRpcFramingError):
    """The Content-Length header is missing or not a decimal integer."""


class JsonRpcDuplicateContentLength(JsonRpcFramingError):
    """More than one Content-Length header was present in the frame."""


class JsonRpcContentLengthTooLarge(JsonRpcFramingError):
    """The declared body size exceeds the configured limit."""


class JsonRpcHeaderTooLarge(JsonRpcFramingError):
    """The header section exceeds the configured limit."""


class JsonRpcTruncatedMessage(JsonRpcFramingError):
    """The stream ended before a complete frame could be read."""


class JsonRpcStreamReader(object):

    def __init__(self, rfile,
                 max_header_size=DEFAULT_MAX_HEADER_SIZE,
                 max_content_length=DEFAULT_MAX_CONTENT_LENGTH):
        self._rfile = rfile
        self._max_header_size = max_header_size
        self._max_content_length = max_content_length
        # Persistent read-ahead buffer. Header reads may over-read into the
        # body (or even the next frame); keeping the bytes here means they are
        # consumed by the correct frame instead of being lost or mixed in.
        self._read_buf = bytearray()

    def close(self):
        self._rfile.close()

    def listen(self, message_consumer):
        """Blocking call to listen for messages on the rfile.

        Args:
            message_consumer (fn): function that is passed each message as it is read off the socket.
        """
        while not self._rfile.closed:
            try:
                request_str = self._read_message()
            except JsonRpcFramingError:
                # A framing error desyncs the stream: do not deliver a half
                # message and do not try to skip ahead to the next frame.
                log.exception("Failed to read frame from rfile")
                break
            except ValueError:
                if self._rfile.closed:
                    return
                log.exception("Failed to read from rfile")
                break

            if request_str is None:
                break

            try:
                message_consumer(json.loads(request_str.decode('utf-8')))
            except ValueError:
                log.exception("Failed to parse JSON message %s", request_str)
                continue

    def _read_message(self):
        """Reads the contents of a message.

        Framing is byte based: the header section is read until the blank
        line, then exactly ``Content-Length`` body bytes are read. Both header
        and body reads tolerate arbitrary chunking of the underlying stream.

        Returns:
            body of message if parsable else None
        """
        header = self._read_header()

        if header is None:
            return None

        content_length = self._content_length(header)

        # Fail before allocating the body buffer.
        if content_length > self._max_content_length:
            raise JsonRpcContentLengthTooLarge(
                "Content-Length {} exceeds limit of {} bytes".format(
                    content_length, self._max_content_length))

        return self._read_exact(content_length)

    def _read_header(self):
        """Read header bytes up to and including the blank line terminator.

        Returns:
            The raw header bytes (including the terminator), or None if the
            stream closed cleanly before any bytes arrived.
        """
        buf = self._read_buf
        while True:
            terminator_at = buf.find(_HEADER_TERMINATOR)
            if terminator_at != -1:
                header = bytes(buf[:terminator_at + len(_HEADER_TERMINATOR)])
                del buf[:terminator_at + len(_HEADER_TERMINATOR)]
                return header

            if len(buf) > self._max_header_size:
                raise JsonRpcHeaderTooLarge(
                    "Header section exceeds limit of {} bytes".format(
                        self._max_header_size))

            chunk = self._rfile.read(self._max_header_size - len(buf) + len(_HEADER_TERMINATOR))
            if not chunk:
                if buf:
                    raise JsonRpcTruncatedMessage(
                        "Stream ended after {} header bytes without a terminator".format(len(buf)))
                return None
            buf.extend(chunk)

    def _read_exact(self, size):
        """Read exactly ``size`` body bytes, never consuming past them.

        Bytes already buffered from an over-read header are used first; the
        underlying stream is only asked for the remaining bytes. A short read
        at EOF raises rather than returning a truncated body.
        """
        buf = self._read_buf
        if size == 0:
            return b''

        while len(buf) < size:
            chunk = self._rfile.read(size - len(buf))
            if not chunk:
                raise JsonRpcTruncatedMessage(
                    "Expected {} body bytes but only {} were available".format(size, len(buf)))
            buf.extend(chunk)

        body = bytes(buf[:size])
        del buf[:size]
        return body

    @staticmethod
    def _content_length(header):
        """Extract and validate the Content-Length from a raw header section.

        The length is a count of UTF-8 bytes (the body is delivered as raw
        bytes). Duplicate headers and non-decimal values are rejected before
        any body bytes are read.
        """
        lengths = []
        for line in header.split(_HEADER_SEPARATOR):
            name, sep, value = line.partition(b':')
            if sep and name.strip().lower() == b'content-length':
                lengths.append(value.strip())

        if not lengths:
            raise JsonRpcInvalidContentLength("Missing Content-Length header")

        if len(lengths) > 1:
            raise JsonRpcDuplicateContentLength(
                "Duplicate Content-Length header: {}".format(
                    b', '.join(lengths).decode('ascii', 'replace')))

        value = lengths[0]
        if not value or not value.isdigit():
            raise JsonRpcInvalidContentLength(
                "Invalid Content-Length header: {!r}".format(value))

        return int(value)


class JsonRpcStreamWriter(object):

    def __init__(self, wfile, **json_dumps_args):
        self._wfile = wfile
        self._wfile_lock = threading.Lock()
        self._json_dumps_args = json_dumps_args

    def close(self):
        with self._wfile_lock:
            self._wfile.close()

    def write(self, message):
        with self._wfile_lock:
            if self._wfile.closed:
                return
            try:
                body = json.dumps(message, **self._json_dumps_args)

                # Ensure we get the byte length, not the character length
                content_length = len(body) if isinstance(body, bytes) else len(body.encode('utf-8'))

                response = (
                    "Content-Length: {}\r\n"
                    "Content-Type: application/vscode-jsonrpc; charset=utf8\r\n\r\n"
                    "{}".format(content_length, body)
                )

                self._wfile.write(response.encode('utf-8'))
                self._wfile.flush()
            except Exception:  # pylint: disable=broad-except
                log.exception("Failed to write message to output file %s", message)
