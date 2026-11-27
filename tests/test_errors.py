"""Tests for the errors exported by pykodi."""

import asyncio
import json
import socket
from typing import Any, Dict

import aiohttp
from aiohttp import web
import jsonrpc_base
import pytest

import pykodi
from pykodi import Kodi, get_kodi_connection


def test_errors_are_the_jsonrpc_base_errors() -> None:
    assert pykodi.ProtocolError is jsonrpc_base.ProtocolError
    assert pykodi.TransportError is jsonrpc_base.TransportError


def test_kodi_error_reply_raises_protocol_error() -> None:
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
                        "error": {"code": -32601, "message": "Method not found."},
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
                    with pytest.raises(pykodi.ProtocolError):
                        await Kodi(connection).get_application_properties(["version"])
                finally:
                    await connection.close()
        finally:
            await runner.cleanup()

    asyncio.run(run())


def test_request_without_connection_raises_transport_error() -> None:
    async def run() -> None:
        async with aiohttp.ClientSession() as session:
            connection = get_kodi_connection(
                "127.0.0.1", 8080, 9090, None, None, timeout=5, session=session
            )
            with pytest.raises(pykodi.TransportError):
                await Kodi(connection).get_application_properties(["version"])

    asyncio.run(run())


def protocol_error(reply: Dict[str, Any]) -> pykodi.ProtocolError:
    """Return the ProtocolError of a request that Kodi answers with the reply."""

    async def run() -> pykodi.ProtocolError:
        async def jsonrpc(request: web.Request) -> web.WebSocketResponse:
            websocket = web.WebSocketResponse()
            await websocket.prepare(request)
            async for message in websocket:
                call = json.loads(message.data)
                await websocket.send_json({"jsonrpc": "2.0", "id": call["id"], **reply})
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
                    with pytest.raises(pykodi.ProtocolError) as error:
                        await Kodi(connection).get_application_properties(["version"])
                finally:
                    await connection.close()
        finally:
            await runner.cleanup()
        return error.value

    return asyncio.run(run())


@pytest.mark.parametrize(
    ("reply", "expected"),
    [
        pytest.param(
            {"error": {"code": -32601, "message": "Method not found."}},
            True,
            id="error_object",
        ),
        pytest.param({}, False, id="no_result"),
    ],
)
def test_is_error_response(reply: Dict[str, Any], expected: bool) -> None:
    assert pykodi.is_error_response(protocol_error(reply)) is expected


def transport_error(status: int, raise_for_status: bool) -> pykodi.TransportError:
    """Return the TransportError of an HTTP request that gets the status."""

    async def run() -> pykodi.TransportError:
        async def jsonrpc(request: web.Request) -> web.Response:
            return web.Response(status=status)

        app = web.Application()
        app.router.add_post("/jsonrpc", jsonrpc)
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, "127.0.0.1", 0).start()
        port = runner.addresses[0][1]
        try:
            async with aiohttp.ClientSession(
                raise_for_status=raise_for_status
            ) as session:
                connection = get_kodi_connection(
                    "127.0.0.1",
                    port,
                    None,
                    "kodi",
                    "secret",
                    timeout=5,
                    session=session,
                )
                with pytest.raises(pykodi.TransportError) as error:
                    await Kodi(connection).get_application_properties(["version"])
        finally:
            await runner.cleanup()
        return error.value

    return asyncio.run(run())


@pytest.mark.parametrize(
    ("status", "raise_for_status", "expected"),
    [
        pytest.param(401, False, True, id="401"),
        pytest.param(401, True, True, id="401_raise_for_status"),
        pytest.param(403, False, False, id="403"),
        pytest.param(403, True, False, id="403_raise_for_status"),
        pytest.param(500, False, False, id="500"),
    ],
)
def test_is_unauthorized(status: int, raise_for_status: bool, expected: bool) -> None:
    assert pykodi.is_unauthorized(transport_error(status, raise_for_status)) is expected


def test_refused_connection_is_not_unauthorized() -> None:
    async def run() -> pykodi.TransportError:
        with socket.socket() as unused:
            unused.bind(("127.0.0.1", 0))
            port = unused.getsockname()[1]
        async with aiohttp.ClientSession() as session:
            connection = get_kodi_connection(
                "127.0.0.1", port, None, "kodi", "secret", timeout=5, session=session
            )
            with pytest.raises(pykodi.TransportError) as error:
                await Kodi(connection).get_application_properties(["version"])
        return error.value

    assert not pykodi.is_unauthorized(asyncio.run(run()))
