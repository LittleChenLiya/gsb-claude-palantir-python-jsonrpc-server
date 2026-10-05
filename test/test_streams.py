# Copyright 2018 Palantir Technologies, Inc.
# pylint: disable=redefined-outer-name
from io import BytesIO
import datetime
import json
import os
import sys
from unittest import mock
import pytest

from pyls_jsonrpc.streams import (
    JsonRpcContentLengthTooLarge,
    JsonRpcDuplicateContentLength,
    JsonRpcHeaderTooLarge,
    JsonRpcInvalidContentLength,
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


# ---------------------------------------------------------------------------
# Explicit bounded framing over arbitrary chunking
# ---------------------------------------------------------------------------

class ChunkedReader(object):
    """File-like object returning data in fixed-size chunks.

    Exercises framing against streams whose read() calls return fewer bytes
    than requested (sockets, pipes, ...).
    """

    def __init__(self, data, chunk_size=1):
        self._data = bytearray(data)
        self._chunk_size = chunk_size
        self.closed = False

    def read(self, size=-1):
        if size is None or size < 0:
            size = len(self._data)
        if size == 0:
            return b''
        n = min(size, self._chunk_size, len(self._data))
        chunk = bytes(self._data[:n])
        del self._data[:n]
        if not chunk:
            self.closed = True
        return chunk

    def close(self):
        self.closed = True


def _frame(body, content_length=None, extra_headers=()):
    """Build a Content-Length framed message from a (unicode or bytes) body."""
    if isinstance(body, str):
        body = body.encode('utf-8')
    if content_length is None:
        content_length = len(body)
    header = [b'Content-Length: %d' % content_length]
    header.extend(extra_headers)
    return b'\r\n'.join(header) + b'\r\n\r\n' + body


def _chunked(data, chunk_size):
    return ChunkedReader(data, chunk_size=chunk_size)


def test_reader_chinese_body_byte_by_byte():
    body = '{"jsonrpc": "2.0", "method": "textDocument/didOpen",' \
           ' "params": {"text": "你好，世界"}}'
    reader = JsonRpcStreamReader(_chunked(_frame(body), chunk_size=1))

    consumer = mock.Mock()
    reader.listen(consumer)

    # The message is delivered exactly once despite byte-at-a-time reads.
    consumer.assert_called_once_with({
        'jsonrpc': '2.0',
        'method': 'textDocument/didOpen',
        'params': {'text': '你好，世界'},
    })


@pytest.mark.parametrize('chunk_size', [1, 2, 3, 7, 16, 64, 100000])
def test_reader_arbitrary_chunking(chunk_size):
    body = '{"method": "test", "params": {"text": "中文body逐字节输入只交付一次"}}'
    reader = JsonRpcStreamReader(_chunked(_frame(body), chunk_size=chunk_size))

    consumer = mock.Mock()
    reader.listen(consumer)

    consumer.assert_called_once()
    assert consumer.call_args[0][0]['params']['text'] == '中文body逐字节输入只交付一次'


@pytest.mark.parametrize('chunk_size', [1, 3, 11, 100000])
def test_reader_two_frames_not_mixed(chunk_size):
    body1 = '{"id": 1, "method": "m1", "params": {"text": "第一帧"}}'
    body2 = '{"id": 2, "method": "m2", "params": {"text": "第二帧"}}'
    data = _frame(body1) + _frame(body2)
    reader = JsonRpcStreamReader(_chunked(data, chunk_size=chunk_size))

    consumer = mock.Mock()
    reader.listen(consumer)

    assert consumer.call_count == 2
    assert consumer.call_args_list[0][0][0]['id'] == 1
    assert consumer.call_args_list[1][0][0]['id'] == 2


def test_reader_header_name_case_insensitive():
    reader = JsonRpcStreamReader(BytesIO(b'content-length: 5\r\n\r\nhello'))
    assert reader._read_message() == b'hello'


@pytest.mark.parametrize('stream_factory', [
    lambda data: BytesIO(data),
    lambda data: _chunked(data, chunk_size=1),
    lambda data: _chunked(data, chunk_size=3),
])
def test_reader_duplicate_content_length(stream_factory):
    data = b'Content-Length: 5\r\nContent-Length: 5\r\n\r\nhello'
    reader = JsonRpcStreamReader(stream_factory(data))
    with pytest.raises(JsonRpcDuplicateContentLength):
        reader._read_message()


@pytest.mark.parametrize('value', [b'abc', b'+5', b'-1', b'1.5', b''])
def test_reader_non_decimal_content_length(value):
    data = b'Content-Length: ' + value + b'\r\n\r\nhello'
    reader = JsonRpcStreamReader(BytesIO(data))
    with pytest.raises(JsonRpcInvalidContentLength):
        reader._read_message()


def test_reader_missing_content_length():
    data = b'Content-Type: application/vscode-jsonrpc; charset=utf8\r\n\r\nhello'
    reader = JsonRpcStreamReader(BytesIO(data))
    with pytest.raises(JsonRpcInvalidContentLength):
        reader._read_message()


def test_reader_truncated_body_is_not_delivered():
    data = b'Content-Length: 100\r\n\r\nshort body'
    reader = JsonRpcStreamReader(BytesIO(data))

    with pytest.raises(JsonRpcTruncatedMessage):
        reader._read_message()

    # The listener must not deliver a half message nor raise to its caller.
    consumer = mock.Mock()
    reader.listen(consumer)
    consumer.assert_not_called()


def test_reader_truncated_header_is_not_delivered():
    reader = JsonRpcStreamReader(BytesIO(b'Content-Length: 5\r\n'))
    with pytest.raises(JsonRpcTruncatedMessage):
        reader._read_message()


def test_reader_content_length_too_large_fails_before_body():
    body = b'{"id": 1, "method": "m", "params": {}}'
    second_frame = _frame('{"id": 2, "method": "m2"}')
    data = _frame(body) + second_frame
    reader = JsonRpcStreamReader(BytesIO(data), max_content_length=10)

    with pytest.raises(JsonRpcContentLengthTooLarge):
        reader._read_message()

    # The body was never allocated or read: this frame's body bytes and the
    # next frame's prefix stay in the read-ahead buffer for the next frame
    # instead of being consumed into a failed body allocation.
    assert bytes(reader._read_buf) == body + second_frame

    # The listener does not deliver anything for the failed frame.
    consumer = mock.Mock()
    reader.listen(consumer)
    consumer.assert_not_called()


def test_reader_header_has_separate_limit():
    data = b'Content-Length: 5\r\n\r\nhello'
    reader = JsonRpcStreamReader(BytesIO(data), max_header_size=4)
    with pytest.raises(JsonRpcHeaderTooLarge):
        reader._read_message()

    # The body limit is independent: a long header with a small body is still
    # accepted when only the header limit is raised.
    long_header = b'X-Padding: ' + b'x' * 100 + b'\r\n'
    reader = JsonRpcStreamReader(
        BytesIO(long_header + b'Content-Length: 5\r\n\r\nhello'),
        max_header_size=200, max_content_length=10)
    assert reader._read_message() == b'hello'


def test_writer_uses_utf8_byte_length(wfile):
    message = {'method': 'textDocument/didOpen', 'params': {'text': '你好，世界'}}
    writer = JsonRpcStreamWriter(wfile, sort_keys=True, ensure_ascii=False)
    writer.write(message)

    raw = wfile.getvalue()
    header, _, body = raw.partition(b'\r\n\r\n')

    # Content-Length counts UTF-8 bytes, not code points.
    length = int(header.split(b'Content-Length:')[1].split(b'\r\n')[0])
    assert length == len(body)
    assert len(body) > len(body.decode('utf-8'))
    assert json.loads(body.decode('utf-8')) == message
