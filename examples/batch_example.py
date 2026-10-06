"""Small example showing sequential JSON RPC batch handling.

Run with:  python examples/batch_example.py

No network is involved: a batch of requests, notifications and invalid items
is fed straight to the Endpoint, and the function used as the consumer simply
records what would be sent back to the client.
"""
from pyls_jsonrpc.endpoint import Endpoint


def main():
    dispatcher = {
        'subtract': lambda params: params['a'] - params['b'],
        'notify': lambda _params: None,
    }

    sent = []
    endpoint = Endpoint(dispatcher, sent.append)

    # Two requests, one notification and one invalid item, mixed together.
    endpoint.consume_batch([
        {'jsonrpc': '2.0', 'id': 1, 'method': 'subtract', 'params': {'a': 5, 'b': 3}},
        {'jsonrpc': '2.0', 'method': 'notify', 'params': {'event': 'ping'}},
        {'not': 'a valid json-rpc message'},
        {'jsonrpc': '2.0', 'id': 2, 'method': 'subtract', 'params': {'a': 9, 'b': 4}},
    ])

    # Only the two request responses are emitted, in their input positions.
    print('Mixed batch output:', sent)

    # A batch made entirely of notifications produces no output at all.
    endpoint.consume_batch([
        {'jsonrpc': '2.0', 'method': 'notify', 'params': {'event': 'one'}},
        {'jsonrpc': '2.0', 'method': 'notify', 'params': {'event': 'two'}},
    ])
    print('All-notification batch output:', sent[1:] if len(sent) > 1 else 'nothing sent')

    endpoint.shutdown()


if __name__ == '__main__':
    main()
