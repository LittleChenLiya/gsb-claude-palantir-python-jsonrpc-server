"""Tiny JSON RPC 2.0 batch example.

Run with: python examples/batch_example.py

It feeds a single batch containing two requests, one notification and one
invalid item to an :class:`Endpoint`. The items are processed sequentially;
the notification and the invalid item produce no response, and the two
responses come back in input position order as one list.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyls_jsonrpc.endpoint import Endpoint  # noqa: E402


def add(params):
    return params['a'] + params['b']


def ping(_params=None):
    print('notification ping received')
    return None


def main():
    sent = []
    endpoint = Endpoint(
        dispatcher={'add': add, 'ping': ping},
        consumer=sent.append,
    )

    batch = [
        {'jsonrpc': '2.0', 'id': 1, 'method': 'add', 'params': {'a': 1, 'b': 2}},
        {'jsonrpc': '2.0', 'method': 'ping'},
        'this is not a valid request',
        {'jsonrpc': '2.0', 'id': 2, 'method': 'add', 'params': {'a': 10, 'b': 20}},
    ]

    endpoint.consume_batch(batch)
    print('responses:', sent)

    # A batch made of only notifications writes nothing at all.
    endpoint.consume_batch([{'jsonrpc': '2.0', 'method': 'ping'}])
    print('message count after all-notification batch:', len(sent))

    endpoint.shutdown()


if __name__ == '__main__':
    main()
