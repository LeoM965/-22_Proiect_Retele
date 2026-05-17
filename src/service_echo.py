import asyncio
import argparse

# Lista de clienti conectati pentru broadcast
clients = set()

async def chat_svc(reader, writer):
    addr = writer.get_extra_info('peername')
    print(f"Client nou conectat: {addr}")
    clients.add(writer)
    
    writer.write(b"*** Talk/Chat Service Broadcast ***\n")
    writer.write(f"Sunteti conectat ca {addr}. Mesajele tale vor fi trimise tuturor.\n".encode())
    await writer.drain()

    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break
            
            message = f"[{addr}] {data.decode().strip()}\n".encode()
            print(f"Broadcast: {message.decode().strip()}")
            
            # Trimitem mesajul la TOTI ceilalti clienti
            disconnected = set()
            for client in clients:
                try:
                    client.write(message)
                    await client.drain()
                except Exception:
                    disconnected.add(client)
            
            for d in disconnected:
                clients.remove(d)
                
    except Exception as e:
        print(f"Eroare chat: {e}")
    finally:
        print(f"Client deconectat: {addr}")
        clients.remove(writer)
        writer.close()
        await writer.wait_closed()

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=5002)
    args = parser.parse_args()
    
    server = await asyncio.start_server(chat_svc, '0.0.0.0', args.port)
    print(f"Chat Service (Talk) a pornit pe portul {args.port}")
    async with server:
        await server.serve_forever()

if __name__ == '__main__':
    asyncio.run(main())
