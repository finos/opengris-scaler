import asyncio
import ymq


async def main():
    ctx = ymq.IOContext()
    socket = await ctx.createIOSocket("ident", ymq.IOSocketType.Binder)
    print(ctx, ";", socket)

    assert socket.identity == "ident"
    assert socket.socket_type == ymq.IOSocketType.Binder

    exc = ymq.YMQException(ymq.ErrorCode.InvalidAddressFormat, "the address has an invalid format")
    assert exc.code == ymq.ErrorCode.InvalidAddressFormat
    assert exc.message == "the address has an invalid format"
    assert exc.code.explanation()


asyncio.run(main())
