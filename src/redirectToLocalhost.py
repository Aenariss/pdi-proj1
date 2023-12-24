"""
PDI Projekt 2023 - pomocný server na zpracování websocketu.
Author: Vojtěch Fiala \<xfiala61\>
"""

import websockets
import asyncio
import socket
import base64
import secrets
import argparse

class Redirecter:
    stream_link = "wss://gis.brno.cz/geoevent/ws/services/ODAE_public_transit_stream/StreamServer/subscribe?outSR=4326"
    data = {}
    key = ""
    host = "localhost"

    def __init__(this, port):
        this.port = port
    
    def startServer(this):
        asyncio.run(this.loadFromSource())

    async def loadFromSource(this):
        local_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        local_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        local_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        local_server.bind((this.host, this.port))
        local_server.listen()

        connection,_ = local_server.accept()  

        key = base64.b64encode(secrets.token_bytes(16)).decode()

        headers = {
            'Upgrade': 'websocket',
            'Sec-WebSocket-Key': key,
            'Sec-WebSocket-Version': '13',
        }

        async with websockets.connect(this.stream_link, extra_headers=headers) as websocket:
            while True:
                js = await websocket.recv()
                try:
                    connection.sendall(js.encode() + "\n".encode())
                except:
                    break

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", action="store", default="9999", help="port where redirected websocket goes", type=int)
    args = parser.parse_args()

    r = Redirecter(args.port)
    r.startServer()
