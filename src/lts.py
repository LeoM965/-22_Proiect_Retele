import asyncio
import os
from protocol import send_packet, recv_packet

# Configurari implicite (Default)
RTS_HOST = os.environ.get("RTS_HOST", "127.0.0.1")
RTS_PORT = int(os.environ.get("RTS_PORT", 9999))
# Porturi locale -> Porturi destinatie
PORT_MAP = {
    8001: 5001,
    8002: 5002,
    8003: 9990
}

async def handle_client(cr, cw, target_port):
    try:
        rr, rw = await asyncio.open_connection(RTS_HOST, RTS_PORT)
        # Trimitem un pachet gol doar ca sa anuntam portul tinta
        await send_packet(rw, target_port, b"")
    except Exception as e:
        cw.write(f"ERR: Tunel remote ({RTS_HOST}:{RTS_PORT}) indisponibil\n".encode())
        await cw.drain()
        cw.close()
        return

    async def fwd(r_in, w_out, is_enc=False):
        try:
            while True:
                if is_enc:
                    data = await r_in.read(4096)
                    if not data: break
                    await send_packet(w_out, target_port, data)
                else:
                    _, data = await recv_packet(r_in)
                    if not data: break
                    w_out.write(data)
                    await w_out.drain()
        except: pass
        finally: w_out.close()

    asyncio.create_task(fwd(cr, rw, True))
    asyncio.create_task(fwd(rr, cw, False))

async def main():
    servers = []
    for lp, tp in PORT_MAP.items():
        s = await asyncio.start_server(
            lambda r, w, tp=tp: handle_client(r, w, tp),
            '0.0.0.0', lp
        )
        servers.append(s)
        
    print(f"[LTS] Serverul de tunel local asculta pe: {list(PORT_MAP.keys())}")
    print(f"[LTS] Redirectioneaza catre RTS la: {RTS_HOST}:{RTS_PORT}")
    await asyncio.gather(*(s.serve_forever() for s in servers))

if __name__ == '__main__':
    asyncio.run(main())
