# JSON-RPC wire protocol framing examples

`JsonRpcStreamReader` frames messages on a byte stream using
`Content-Length` headers, the same framing used by the Language Server
Protocol. `Content-Length` always counts **UTF-8 bytes**, not characters.
Headers and bodies may arrive in arbitrarily small chunks; the reader
re-assembles them into whole frames before delivering anything.

In the examples below, `\r\n` denotes the two bytes `0x0D 0x0A` (CRLF).

## A single frame

```
Content-Length: 49\r\n
Content-Type: application/vscode-jsonrpc; charset=utf8\r\n
\r\n
{"id": "hello", "method": "method", "params": {}}
```

The header section ends at the blank line; exactly 49 body bytes follow.

## UTF-8 bodies: Content-Length counts bytes

The body `{"method": "你好"}` is 16 characters but 20 UTF-8 bytes
(`你` and `好` are 3 bytes each), so the header must say 20:

```
Content-Length: 20\r\n
\r\n
{"method": "你好"}
```

The reader delivers the message only after all 20 body bytes have
arrived, even if they trickle in one byte per `read()` call, and it
delivers it exactly once.

## Two consecutive frames

Frames are concatenated directly; the next frame's header starts
immediately after the previous frame's body:

```
Content-Length: 28\r\n
\r\n
{"id": 1, "method": "first"}Content-Length: 29\r\n
\r\n
{"id": 2, "method": "second"}
```

The reader buffers whatever it over-reads, so bytes of the second
frame's header are never blended into the first frame's body.

## Limits

* The header section has its own limit (default 8192 bytes,
  `max_header_size`).
* The body has a separate limit (default 64 MiB, `max_body_size`); an
  oversized `Content-Length` is rejected before any buffer for the body
  is allocated.

## Error taxonomy

Every framing error is a subclass of `JsonRpcStreamError` (which is a
`ValueError` for backwards compatibility):

| Wire condition                                   | Exception                        |
|--------------------------------------------------|----------------------------------|
| `Content-Length: 1_0`, `0x10`, `+10`, `ten`, ... | `JsonRpcInvalidContentLength`    |
| Two `Content-Length` headers (equal or not)      | `JsonRpcDuplicateContentLength`  |
| Header section without `Content-Length`          | `JsonRpcMissingContentLength`    |
| Header line without `:`                          | `JsonRpcHeaderError`             |
| Header section over `max_header_size`            | `JsonRpcHeaderTooLarge`          |
| `Content-Length` over `max_body_size`            | `JsonRpcMessageTooLarge`         |
| EOF in the middle of header or body              | `JsonRpcTruncatedMessage`        |

A truncated frame is never delivered as a partial message. After a
recoverable error the listener discards bytes up to the next frame's
`Content-Length` marker (or, for an oversized body, exactly the declared
number of body bytes) and keeps listening, so one bad frame does not
corrupt the frames that follow it.
