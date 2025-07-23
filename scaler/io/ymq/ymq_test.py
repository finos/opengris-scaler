import asyncio
import ymq


async def main():
    ctx = ymq.IOContext()
    socket = await ctx.createIOSocket("ident", ymq.IOSocketType.Binder)
    print(ctx, ";", socket)

    assert socket.identity == "ident"
    assert socket.socket_type == ymq.IOSocketType.Binder

    exc = ymq.YMQException(ymq.ErrorCode.InvalidAddressFormat, "oh no!")
    assert exc.code == ymq.ErrorCode.InvalidAddressFormat
    assert exc.message == "oh no!"
    assert exc.code.explanation()


asyncio.run(main())
