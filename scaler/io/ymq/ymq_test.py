import ymq

context = ymq.IOContext(num_threads=2)
socket = context.createIOSocket("my-socket", ymq.IOSocketType.Dealer)

print(context, ";", socket)

assert context.num_threads == 2
assert socket.identity == "my-socket"
assert socket.socket_type == ymq.IOSocketType.Dealer
