# Copyright 2018 Palantir Technologies, Inc.
# pylint: disable=redefined-outer-name
from io import BytesIO
import datetime
import json
import os
import sys

try:
    import mock
except ImportError:  # Python 3.3+
    from unittest import mock

import pytest

from pyls_jsonrpc.streams import (
    DEFAULT_MAX_BODY_SIZE,
    DEFAULT_MAX_HEADER_SIZE,
    JsonRpcDuplicateContentLength,
    JsonRpcHeaderError,
    JsonRpcHeaderTooLarge,
    JsonRpcInvalidContentLength,
    JsonRpcMessageTooLarge,
    JsonRpcMissingContentLength,
    JsonRpcStreamError,
    JsonRpcStreamReader,
    JsonRpcStreamWriter,
    JsonRpcTruncatedMessage,
)


@pytest.fixture()
def rfile():
    return BytesIO()


@pytest.fixture()
def wfile():
    return BytesIO()


@pytest.fixture()
def reader(rfile):
    return JsonRpcStreamReader(rfile)


@pytest.fixture()
def writer(wfile):
    return JsonRpcStreamWriter(wfile, sort_keys=True)


def test_reader(rfile, reader):
    rfile.write(
        b'Content-Length: 49\r\n'
        b'Content-Type: application/vscode-jsonrpc; charset=utf8\r\n'
        b'\r\n'
        b'{"id": "hello", "method": "method", "params": {}}'
    )
    rfile.seek(0)

    consumer = mock.Mock()
    reader.listen(consumer)

    consumer.assert_called_once_with({
        'id': 'hello',
        'method': 'method',
        'params': {}
    })


def test_reader_bad_message(rfile, reader):
    rfile.write(b'Hello world')
    rfile.seek(0)

    # Ensure the listener doesn't throw
    consumer = mock.Mock()
    reader.listen(consumer)
    consumer.assert_not_called()


def test_reader_bad_json(rfile, reader):
    rfile.write(
        b'Content-Length: 8\r\n'
        b'Content-Type: application/vscode-jsonrpc; charset=utf8\r\n'
        b'\r\n'
        b'{hello}}'
    )
    rfile.seek(0)

    # Ensure the listener doesn't throw
    consumer = mock.Mock()
    reader.listen(consumer)
    consumer.assert_not_called()


def test_writer(wfile, writer):
    writer.write({
        'id': 'hello',
        'method': 'method',
        'params': {}
    })

    if os.name == 'nt':
        assert wfile.getvalue() == (
            b'Content-Length: 49\r\n'
            b'Content-Type: application/vscode-jsonrpc; charset=utf8\r\n'
            b'\r\n'
            b'{"id": "hello", "method": "method", "params": {}}'
        )
    else:
        assert wfile.getvalue() == (
            b'Content-Length: 44\r\n'
            b'Content-Type: application/vscode-jsonrpc; charset=utf8\r\n'
            b'\r\n'
            b'{"id":"hello","method":"method","params":{}}'
        )


class JsonDatetime(datetime.datetime):
    """Monkey path json datetime."""
    def __json__(self):
        if sys.version_info.major == 3:
            dif = int(self.timestamp())
        else:
            dif = int((self - datetime.datetime(1970, 1, 1)).total_seconds())
        return '{0}'.format(dif)


def test_writer_bad_message(wfile, writer):
    # A datetime isn't serializable(or poorly serializable),
    # ensure the write method doesn't throw, but the result could be empty
    # or the correct datetime
    datetime.datetime = JsonDatetime
    writer.write(datetime.datetime(
        year=2019,
        month=1,
        day=1,
        hour=1,
        minute=1,
        second=1,
    ))

    assert wfile.getvalue() in [
        b'',
        b'Content-Length: 10\r\n'
        b'Content-Type: application/vscode-jsonrpc; charset=utf8\r\n'
        b'\r\n'
        b'1546304461'
    ]


def _frame(message):
    """Build a wire frame, with Content-Length counted in UTF-8 bytes."""
    body = json.dumps(message, ensure_ascii=False).encode('utf-8')
    return b'Content-Length: %d\r\n\r\n' % len(body) + body


class ChunkedBytesIO(BytesIO):
    """A BytesIO that yields at most ``chunk_size`` bytes per read call.

    Simulates a transport that fragments headers and bodies arbitrarily.
    """

    def __init__(self, data=b'', chunk_size=1):
        super(ChunkedBytesIO, self).__init__(data)
        self._chunk_size = chunk_size

    def read(self, size=-1):
        if size is None or size < 0:
            size = self._chunk_size
        return super(ChunkedBytesIO, self).read(min(size, self._chunk_size))

    def read1(self, size=-1):
        return self.read(size)


def test_reader_defaults_are_bounded(reader):
    assert reader._max_header_size == DEFAULT_MAX_HEADER_SIZE
    assert reader._max_body_size == DEFAULT_MAX_BODY_SIZE


def test_reader_chinese_body_byte_by_byte():
    # A multi-byte UTF-8 body delivered one byte at a time must be framed
    # by byte count and delivered exactly once.
    message = {'id': 1, 'method': '打招呼', 'params': {'文本': '你好，世界'}}
    body = json.dumps(message, ensure_ascii=False).encode('utf-8')
    assert len(body) > len(body.decode('utf-8'))  # length is in bytes, not chars

    rfile = ChunkedBytesIO(
        b'Content-Length: %d\r\n'
        b'Content-Type: application/vscode-jsonrpc; charset=utf8\r\n'
        b'\r\n' % len(body) + body,
        chunk_size=1,
    )
    reader = JsonRpcStreamReader(rfile)

    consumer = mock.Mock()
    reader.listen(consumer)

    consumer.assert_called_once_with(message)


@pytest.mark.parametrize('chunk_size', [1, 3, 7, 4096])
def test_reader_two_consecutive_frames(chunk_size):
    first = {'id': 1, 'method': 'first'}
    second = {'id': 2, 'method': 'second', 'params': {'文本': '中文'}}
    data = _frame(first) + _frame(second)

    reader = JsonRpcStreamReader(ChunkedBytesIO(data, chunk_size=chunk_size))
    consumer = mock.Mock()
    reader.listen(consumer)

    assert consumer.call_args_list == [mock.call(first), mock.call(second)]


@pytest.mark.parametrize('header', [
    b'Content-Length: 2\r\nContent-Length: 3\r\n\r\n',  # conflicting lengths
    b'Content-Length: 2\r\nContent-Length: 2\r\n\r\n',  # repeated equal length
])
def test_reader_duplicate_content_length_rejected(header):
    reader = JsonRpcStreamReader(BytesIO(header + b'{}'))
    with pytest.raises(JsonRpcDuplicateContentLength):
        reader._read_message()


@pytest.mark.parametrize('value', [
    b'1_0', b'0x10', b'+10', b'-3', b'1.5', b'ten', b'',
])
def test_reader_non_decimal_content_length_rejected(value):
    reader = JsonRpcStreamReader(
        BytesIO(b'Content-Length: %s\r\n\r\n{}' % value))
    with pytest.raises(JsonRpcInvalidContentLength):
        reader._read_message()


def test_reader_missing_content_length():
    reader = JsonRpcStreamReader(
        BytesIO(b'Content-Type: application/vscode-jsonrpc\r\n\r\n{}'))
    with pytest.raises(JsonRpcMissingContentLength):
        reader._read_message()


def test_reader_malformed_header_line():
    reader = JsonRpcStreamReader(BytesIO(b'not a header\r\n\r\n{}'))
    with pytest.raises(JsonRpcHeaderError):
        reader._read_message()


def test_reader_truncated_body_not_delivered():
    body = b'{"id": 1}'
    reader = JsonRpcStreamReader(
        BytesIO(b'Content-Length: %d\r\n\r\n' % (len(body) + 10) + body))
    with pytest.raises(JsonRpcTruncatedMessage):
        reader._read_message()

    # The listener must not deliver the partial message either.
    reader = JsonRpcStreamReader(
        BytesIO(b'Content-Length: %d\r\n\r\n' % (len(body) + 10) + body))
    consumer = mock.Mock()
    reader.listen(consumer)
    consumer.assert_not_called()


def test_reader_truncated_header():
    reader = JsonRpcStreamReader(BytesIO(b'Content-Length: 10\r\n'))
    with pytest.raises(JsonRpcTruncatedMessage):
        reader._read_message()


def test_reader_header_size_limit():
    header = b'Content-Length: 2\r\n' + b'X-Padding: ' + b'x' * 100 + b'\r\n\r\n'
    reader = JsonRpcStreamReader(BytesIO(header + b'{}'), max_header_size=32)
    with pytest.raises(JsonRpcHeaderTooLarge):
        reader._read_message()


def test_reader_oversized_body_rejected_before_body_read():
    # No body bytes are present at all: if the limit were enforced only
    # after reading the body, this would raise JsonRpcTruncatedMessage.
    reader = JsonRpcStreamReader(
        BytesIO(b'Content-Length: 100\r\n\r\n'), max_body_size=10)
    with pytest.raises(JsonRpcMessageTooLarge) as exc_info:
        reader._read_message()
    assert exc_info.value.content_length == 100
    assert exc_info.value.max_body_size == 10


def test_reader_oversized_frame_does_not_corrupt_next_frame():
    good = {'id': 2, 'method': 'good'}
    data = b'Content-Length: 40\r\n\r\n' + b'0123456789' * 4 + _frame(good)

    reader = JsonRpcStreamReader(BytesIO(data), max_body_size=30)
    consumer = mock.Mock()
    reader.listen(consumer)

    consumer.assert_called_once_with(good)


def test_reader_invalid_frame_does_not_mix_with_next_frame():
    # The bad frame's body must not be blended into the next frame's header.
    good = {'id': 2, 'method': 'good'}
    data = (
        b'Content-Length: 2\r\n'
        b'Content-Length: 3\r\n'
        b'\r\n'
        b'{}' + _frame(good)
    )

    reader = JsonRpcStreamReader(BytesIO(data))
    consumer = mock.Mock()
    reader.listen(consumer)

    consumer.assert_called_once_with(good)


def test_reader_stream_errors_are_value_errors():
    # Backwards compatibility: framing errors remain ValueError subclasses.
    for exc_class in (JsonRpcHeaderError, JsonRpcMissingContentLength,
                      JsonRpcInvalidContentLength, JsonRpcDuplicateContentLength,
                      JsonRpcHeaderTooLarge, JsonRpcMessageTooLarge,
                      JsonRpcTruncatedMessage):
        assert issubclass(exc_class, JsonRpcStreamError)
        assert issubclass(exc_class, ValueError)
