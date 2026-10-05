# Copyright 2018 Palantir Technologies, Inc.
"""Example: JSON-RPC framing over a byte stream.

Messages are delimited by a ``Content-Length`` header counting the body's
UTF-8 bytes, followed by a blank line (``\r\n\r\n``) and the raw JSON body.
The reader frames messages from arbitrary byte chunking, so the stream may
deliver data one byte at a time.

Run with::

    python examples/protocol_framing.py
"""
import io

from pyls_jsonrpc.streams import JsonRpcStreamReader, JsonRpcStreamWriter


class ChunkedStream(object):
    """File-like object that yields one byte per read() call.

    This demonstrates that framing does not depend on how the underlying
    stream chunks its bytes.
    """

    def __init__(self, data):
        self._data = bytearray(data)
        self.closed = False

    def read(self, size=-1):
        if size is None or size < 0:
            size = len(self._data)
        chunk = bytes(self._data[:min(size, 1)])
        del self._data[:min(size, 1)]
        if not chunk:
            self.closed = True
        return chunk

    def close(self):
        self.closed = True


def main():
    message = {
        'jsonrpc': '2.0',
        'id': 1,
        'method': 'textDocument/didOpen',
        'params': {
            'uri': 'file:///example.txt',
            'text': '你好，世界',
        },
    }

    # Write a frame and show the wire format.
    buf = io.BytesIO()
    writer = JsonRpcStreamWriter(buf, sort_keys=True, ensure_ascii=False)
    writer.write(message)
    raw = buf.getvalue()
    print('wire format ({} bytes):'.format(len(raw)))
    print(raw.decode('utf-8'))

    # Read it back one byte at a time.
    reader = JsonRpcStreamReader(ChunkedStream(raw))
    received = []
    reader.listen(received.append)

    assert received == [message]
    print('decoded message:', received[0])


if __name__ == '__main__':
    main()
