import struct
import asyncio

HEADER_FORMAT = "!HI"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

async def send_packet(w, port, data):
    w.write(struct.pack(HEADER_FORMAT, port, len(data)) + data)
    await w.drain()

async def recv_packet(r):
    try:
        p, l = struct.unpack(HEADER_FORMAT, await r.readexactly(HEADER_SIZE))
        return p, await r.readexactly(l)
    except asyncio.IncompleteReadError:
        return None, b""
