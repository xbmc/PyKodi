"""Tests for Kodi.set_shuffle."""

import asyncio
import json
from typing import Any, Dict, List, Tuple

import aiohttp
from aiohttp import web

from pykodi import Kodi, get_kodi_connection


def set_shuffle(
    players: List[Dict[str, Any]], shuffle: bool = True
) -> Tuple[bool, List[Dict[str, Any]]]:
    calls: List[Dict[str, Any]] = []

    async def run() -> bool:
        async def jsonrpc(request: web.Request) -> web.WebSocketResponse:
            websocket = web.WebSocketResponse()
            await websocket.prepare(request)
            async for message in websocket:
                call = json.loads(message.data)
                calls.append(call)
                if call["method"] == "Player.GetActivePlayers":
                    result: Any = players
                else:
                    result = "OK"
                await websocket.send_json(
                    {"jsonrpc": "2.0", "id": call["id"], "result": result}
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
                    return await Kodi(connection).set_shuffle(shuffle)
                finally:
                    await connection.close()
        finally:
            await runner.cleanup()

    return asyncio.run(run()), calls


def test_shuffle_is_sent_to_the_first_player() -> None:
    result, calls = set_shuffle([{"playerid": 2, "type": "audio"}], False)
    assert result is True
    assert [call["method"] for call in calls] == [
        "Player.GetActivePlayers",
        "Player.SetShuffle",
    ]
    assert calls[1]["params"] == {"playerid": 2, "shuffle": False}


def test_no_request_without_active_player() -> None:
    result, calls = set_shuffle([])
    assert result is False
    assert [call["method"] for call in calls] == ["Player.GetActivePlayers"]
