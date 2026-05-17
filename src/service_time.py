import asyncio
import datetime
import argparse

async def time_svc(r, w):
    w.write(f"Ora de pe server: {datetime.datetime.now()}\n".encode())
    await w.drain()
    w.close()

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=5001)
    args = parser.parse_args()
    
    s = await asyncio.start_server(time_svc, '0.0.0.0', args.port)
    print(f"Time Service a pornit pe portul {args.port}")
    async with s:
        await s.serve_forever()

if __name__ == '__main__':
    asyncio.run(main())
