"""Tests for Kodi.call_method."""

import asyncio
import json

import aiohttp
from aiohttp import web

from pykodi import Kodi, get_kodi_connection


def test_param_named_method_is_sent_to_kodi() -> None:
    async def run() -> None:
        async def jsonrpc(request: web.Request) -> web.WebSocketResponse:
            websocket = web.WebSocketResponse()
            await websocket.prepare(request)
            async for message in websocket:
                call = json.loads(message.data)
                await websocket.send_json(
                    {
                        "jsonrpc": "2.0",
                        "id": call["id"],
                        "result": {"method": call["method"], "params": call["params"]},
                    }
                )
            return websocket

        app = web.Application()
        app.router.add_get("/jsonrpc", jsonrpc)
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, "127.0.0.1", 0).start()
        port = runner.addresses[0][1]
        try:
            async with aiohttp.ClientSession() as session:
                connection = get_kodi_connection(
                    "127.0.0.1", 8080, port, None, None, timeout=5, session=session
                )
                await connection.connect()
                try:
                    result = await Kodi(connection).call_method(
                        "Addons.ExecuteAddon", method="x"
                    )
                finally:
                    await connection.close()
        finally:
            await runner.cleanup()

        assert result == {"method": "Addons.ExecuteAddon", "params": {"method": "x"}}

    asyncio.run(run())
