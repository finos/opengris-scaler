import array
import asyncio
import ymq


async def main():
    ctx = ymq.IOContext()
    assert ctx.num_threads == 1

    socket = await ctx.createIOSocket("ident", ymq.IOSocketType.Binder)
    assert socket.identity == "ident"
    assert socket.socket_type == ymq.IOSocketType.Binder

    exc = ymq.YMQException(ymq.ErrorCode.InvalidAddressFormat, "the address has an invalid format")
    assert exc.code == ymq.ErrorCode.InvalidAddressFormat
    assert exc.message == "the address has an invalid format"
    assert exc.code.explanation()

    msg = ymq.Message(b"addr", b"payload")
    assert msg.address.data == b"addr"
    assert msg.payload.data == b"payload"

    msg2 = ymq.Message(ymq.Bytes(b"ident"), payload=b"data")
    assert msg2.address.data == b"ident"
    assert msg2.payload.data == b"data"

    b = ymq.Bytes(b"data")
    assert b.len == 4
    assert b.data == b"data"

    b = ymq.Bytes(array.array('B', [115, 99, 97, 108, 101, 114]))
    assert b.len == 6
    assert b.data == b"scaler"


asyncio.run(main())
