# Copyright 2018 Palantir Technologies, Inc.
import logging
import threading

try:
    import ujson as json
except Exception:  # pylint: disable=broad-except
    import json

log = logging.getLogger(__name__)

#: Default maximum size in bytes of one frame's header section. The header
#: section has its own limit, independent of the body limit.
DEFAULT_MAX_HEADER_SIZE = 8 * 1024

#: Default maximum size in bytes of one frame's body.
DEFAULT_MAX_BODY_SIZE = 64 * 1024 * 1024

#: How many bytes to pull from the underlying stream per read.
_READ_CHUNK_SIZE = 4096


class JsonRpcStreamError(ValueError):
    """Base class for framing errors on an input stream.

    Subclasses ValueError for backwards compatibility with callers that
    caught ValueError from previous versions of the reader.
    """


class JsonRpcHeaderError(JsonRpcStreamError):
    """A frame header line was malformed."""


class JsonRpcMissingContentLength(JsonRpcHeaderError):
    """The header section ended without any Content-Length header."""


class JsonRpcInvalidContentLength(JsonRpcHeaderError):
    """The Content-Length value was not a non-negative decimal integer."""


class JsonRpcDuplicateContentLength(JsonRpcHeaderError):
    """More than one Content-Length header appeared in a single frame."""


class JsonRpcHeaderTooLarge(JsonRpcStreamError):
    """The header section exceeded the configured header size limit."""


class JsonRpcMessageTooLarge(JsonRpcStreamError):
    """The declared body length exceeded the configured body size limit.

    Raised before any buffer for the body is allocated.
    """

    def __init__(self, content_length, max_body_size):
        super(JsonRpcMessageTooLarge, self).__init__(
            "Content-Length of {} bytes exceeds the limit of {} bytes".format(
                content_length, max_body_size))
        self.content_length = content_length
        self.max_body_size = max_body_size


class JsonRpcTruncatedMessage(JsonRpcStreamError):
    """The stream reached EOF in the middle of a frame.

    No partial message is delivered for a truncated frame.
    """


class JsonRpcStreamReader(object):

    def __init__(self, rfile, max_header_size=DEFAULT_MAX_HEADER_SIZE,
                 max_body_size=DEFAULT_MAX_BODY_SIZE):
        self._rfile = rfile
        self._max_header_size = max_header_size
        self._max_body_size = max_body_size
        # Bytes pulled from the stream but not yet consumed by a frame.
        # Kept across frames so that a read which straddles a frame
        # boundary never leaks the next frame's prefix into the current one.
        self._buffer = bytearray()

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
            except JsonRpcTruncatedMessage:
                # EOF in the middle of a frame: nothing further can be
                # framed, and no partial message is delivered.
                log.exception("Truncated message on rfile; stopping listener")
                return
            except JsonRpcMessageTooLarge as exc:
                # The declared body length is known, so skip exactly that
                # many bytes to land on the next frame boundary.
                log.exception("Message too large; discarding %d body bytes",
                              exc.content_length)
                self._discard(exc.content_length)
                continue
            except JsonRpcStreamError:
                log.exception("Failed to read frame from rfile; resynchronizing")
                self._resync()
                continue
            except ValueError:
                if self._rfile.closed:
                    return
                log.exception("Failed to read from rfile")
                self._resync()
                continue

            if request_str is None:
                break

            try:
                message_consumer(json.loads(request_str.decode('utf-8')))
            except ValueError:
                log.exception("Failed to parse JSON message %s", request_str)
                continue

    def _read_message(self):
        """Reads the contents of a message.

        Returns:
            body of message if parsable else None
        """
        content_length = self._read_headers()

        if content_length is None:
            return None

        # Reject oversized bodies before any buffer for them is allocated.
        if content_length > self._max_body_size:
            raise JsonRpcMessageTooLarge(content_length, self._max_body_size)

        return self._read_exactly(content_length)

    def _read_headers(self):
        """Read one header section, bounded by the header size limit.

        Returns the declared Content-Length, or None on a clean EOF between
        frames. Raises a JsonRpcStreamError subclass on any framing error,
        before the body is touched.
        """
        budget = self._max_header_size
        content_length = None
        read_any = False

        while True:
            line = self._readline(budget)

            if not line:
                if read_any:
                    raise JsonRpcTruncatedMessage(
                        "EOF in the middle of a header section")
                return None

            read_any = True
            budget -= len(line)
            if budget < 0:
                raise JsonRpcHeaderTooLarge(
                    "Header section exceeds the limit of {} bytes".format(
                        self._max_header_size))

            if not line.strip():
                # Blank line: end of the header section.
                if content_length is None:
                    raise JsonRpcMissingContentLength(
                        "Header section ended without a Content-Length header")
                return content_length

            name, sep, value = line.partition(b':')
            if not sep:
                raise JsonRpcHeaderError(
                    "Malformed header line: {!r}".format(line))

            if name.strip().lower() == b'content-length':
                # Duplicate (equal or conflicting) lengths are rejected
                # before anything else is done with the frame.
                if content_length is not None:
                    raise JsonRpcDuplicateContentLength(
                        "Duplicate Content-Length headers: {} and {!r}".format(
                            content_length, value.strip()))
                value = value.strip()
                # Strictly decimal ASCII digits only: this rejects signs,
                # whitespace, underscores, hex and float spellings that
                # int() would otherwise accept.
                if not value.isdigit():
                    raise JsonRpcInvalidContentLength(
                        "Invalid Content-Length header: {!r}".format(value))
                content_length = int(value)

    def _readline(self, limit):
        """Read one line (including the trailing newline) from the stream.

        Tolerates arbitrary chunking of the underlying stream. Returns b''
        if the stream is at EOF and no more data is available. Raises
        JsonRpcHeaderTooLarge if the line grows beyond ``limit`` bytes.
        """
        while True:
            newline = self._buffer.find(b'\n')
            if newline != -1:
                if newline + 1 > limit:
                    raise JsonRpcHeaderTooLarge(
                        "Header line exceeds the limit of {} bytes".format(limit))
                line = bytes(self._buffer[:newline + 1])
                del self._buffer[:newline + 1]
                return line
            if len(self._buffer) > limit:
                raise JsonRpcHeaderTooLarge(
                    "Header line exceeds the limit of {} bytes".format(limit))
            if not self._read_more():
                line = bytes(self._buffer)
                del self._buffer[:]
                return line

    def _read_exactly(self, size):
        """Read exactly ``size`` body bytes, tolerating arbitrary chunking.

        Raises JsonRpcTruncatedMessage if EOF arrives first; the partial
        bytes are dropped and never delivered as a message.
        """
        while len(self._buffer) < size:
            if not self._read_more():
                received = len(self._buffer)
                del self._buffer[:]
                raise JsonRpcTruncatedMessage(
                    "EOF after {} of {} body bytes".format(received, size))
        body = bytes(self._buffer[:size])
        del self._buffer[:size]
        return body

    def _read_more(self):
        """Pull one chunk from the underlying stream into the buffer."""
        # Prefer read1 (when available) so interactive streams return as
        # soon as any data is available instead of blocking for a full chunk.
        read = getattr(self._rfile, 'read1', None) or self._rfile.read
        chunk = read(_READ_CHUNK_SIZE)
        if chunk:
            self._buffer.extend(chunk)
            return True
        return False

    def _discard(self, size):
        """Discard exactly ``size`` bytes, stopping early at EOF."""
        while size > 0:
            if self._buffer:
                discarded = min(size, len(self._buffer))
                del self._buffer[:discarded]
                size -= discarded
            elif not self._read_more():
                return

    def _resync(self):
        """Discard input until the start of the next frame's header.

        After a framing error the body boundary is unknown, so bytes are
        dropped until the next Content-Length marker is found; the marker
        and everything after it is kept so the next _read_message sees the
        new frame whole, and no byte of the next frame's prefix is ever
        blended into the failed frame.
        """
        marker = b'content-length:'
        while True:
            idx = self._buffer.lower().find(marker)
            if idx != -1:
                del self._buffer[:idx]
                return
            # Keep a tail in case the marker straddles a read boundary.
            keep = len(marker) - 1
            if len(self._buffer) > keep:
                del self._buffer[:len(self._buffer) - keep]
            if not self._read_more():
                del self._buffer[:]
                return


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
