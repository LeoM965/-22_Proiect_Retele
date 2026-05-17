import asyncio
import argparse
import os
from protocol import send_packet, recv_packet

# Configurari implicite (Default)
DEFAULT_PORT = 9999
# Mapare porturi catre host-uri (in Docker folosim nume, local folosim 127.0.0.1)
DEFAULT_MAP = {
    "5001": os.environ.get("TIME_SVC_HOST", "127.0.0.1"),
    "5002": os.environ.get("ECHO_SVC_HOST", "127.0.0.1"),
    "9990": "127.0.0.1"
}

async def rts_conn(r, w):
    port, init_data = await recv_packet(r)
    if not port:
        w.close()
        return

    target_host = DEFAULT_MAP.get(str(port))
    if not target_host:
        await send_packet(w, port, f"ERR: Port invalid ({port})\n".encode())
        w.close()
        return

    try:
        sr, sw = await asyncio.open_connection(target_host, port)
        print(f"[RTS] Forward catre {target_host}:{port}")
    except Exception:
        await send_packet(w, port, f"ERR: Serviciu indisponibil ({port})\n".encode())
        w.close()
        return

    if init_data:
        sw.write(init_data)
        await sw.drain()

    async def fwd(r_in, w_out, is_enc=False):
        try:
            while True:
                if is_enc:
                    data = await r_in.read(4096)
                    if not data: break
                    await send_packet(w_out, port, data)
                else:
                    _, data = await recv_packet(r_in)
                    if not data: break
                    w_out.write(data)
                    await w_out.drain()
        except: pass
        finally: w_out.close()

    asyncio.create_task(fwd(r, sw, False))
    asyncio.create_task(fwd(sr, w, True))

async def main():
    port = int(os.environ.get("RTS_PORT", DEFAULT_PORT))
    server = await asyncio.start_server(rts_conn, '0.0.0.0', port)
    print(f"[RTS] Serverul de tunel remote asculta pe portul: {port}")
    async with server:
        await server.serve_forever()

if __name__ == '__main__':
    asyncio.run(main())
