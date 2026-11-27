"""Tests for HTTP Basic authentication."""

import asyncio
import base64
import errno
import re
import socket
from http import HTTPStatus
from typing import Any, Callable, Dict, List, Optional, Type
from unittest.mock import MagicMock
import warnings

import aiohttp
from aiohttp import web
from aiohttp.abc import AbstractResolver
import pytest

from pykodi import (
    CannotConnectError,
    InvalidAuthError,
    Kodi,
    ProtocolError,
    TransportError,
    get_kodi_connection,
)


def basic(credentials: str) -> str:
    return "Basic " + base64.b64encode(credentials.encode("utf-8")).decode("ascii")


async def start_server(app: web.Application) -> web.AppRunner:
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    return runner


def http_authorization(username: Optional[str], password: Optional[str]) -> Any:
    """Ping over HTTP and return the Authorization header Kodi received."""
    received: List[Optional[str]] = []

    async def jsonrpc(request: web.Request) -> web.Response:
        received.append(request.headers.get("Authorization"))
        message = await request.json()
        return web.json_response(
            {"jsonrpc": "2.0", "id": message["id"], "result": "pong"}
        )

    async def run() -> None:
        app = web.Application()
        app.router.add_post("/jsonrpc", jsonrpc)
        runner = await start_server(app)
        port = runner.addresses[0][1]
        try:
            async with aiohttp.ClientSession() as session:
                connection = get_kodi_connection(
                    "127.0.0.1", port, None, username, password, session=session
                )
                assert await Kodi(connection).ping()
        finally:
            await runner.cleanup()

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        asyncio.run(run())

    auth_warnings = [w for w in caught if "auth" in str(w.message).lower()]
    assert auth_warnings == []
    assert len(received) == 1
    return received[0]


def test_http_sends_authorization_header() -> None:
    assert http_authorization("kodi", "secret") == basic("kodi:secret")


def test_http_without_username_sends_no_header() -> None:
    assert http_authorization(None, None) is None


def test_password_is_encoded_as_utf8() -> None:
    assert http_authorization("kodi", "jelszó") == basic("kodi:jelszó")


def test_missing_password_is_sent_empty() -> None:
    assert http_authorization("kodi", None) == basic("kodi:")


def test_websocket_sends_authorization_header() -> None:
    received: Dict[str, Optional[str]] = {}

    async def jsonrpc(request: web.Request) -> web.WebSocketResponse:
        received["authorization"] = request.headers.get("Authorization")
        websocket = web.WebSocketResponse()
        await websocket.prepare(request)
        async for _ in websocket:
            pass
        return websocket

    async def run() -> None:
        app = web.Application()
        app.router.add_get("/jsonrpc", jsonrpc)
        runner = await start_server(app)
        port = runner.addresses[0][1]
        try:
            async with aiohttp.ClientSession() as session:
                connection = get_kodi_connection(
                    "127.0.0.1", 8080, port, "kodi", "secret", session=session
                )
                await connection.connect()
                await connection.close()
        finally:
            await runner.cleanup()

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        asyncio.run(run())

    assert [w for w in caught if "auth" in str(w.message).lower()] == []
    assert received["authorization"] == basic("kodi:secret")


def test_colon_in_username_is_rejected() -> None:
    with pytest.raises(ValueError):
        get_kodi_connection("kodi", 8080, None, "ko:di", "secret", session=MagicMock())


def test_missing_password_in_image_url() -> None:
    connection = get_kodi_connection(
        "kodi", 8080, None, "kodi", None, session=MagicMock()
    )
    assert connection.thumbnail_url("image://cover.jpg/").startswith(
        "http://kodi:@kodi:8080/image/"
    )


class NoHosts(AbstractResolver):
    """Resolver that finds no host, without asking DNS."""

    async def resolve(
        self, host: str, port: int = 0, family: socket.AddressFamily = socket.AF_INET
    ) -> List[Any]:
        raise OSError(f"{host} not found")

    async def close(self) -> None:
        pass


async def ping(
    host: str,
    port: int,
    raise_for_status: bool = False,
    resolver: Optional[AbstractResolver] = None,
    request_timeout: float = 5,
) -> None:
    """Ping over HTTP with a new session."""
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(resolver=resolver),
        raise_for_status=raise_for_status,
    ) as session:
        connection = get_kodi_connection(
            host,
            port,
            None,
            "kodi",
            "secret",
            timeout=request_timeout,
            session=session,
        )
        await Kodi(connection).ping()


def ping_with_status(
    status: int, reason: Optional[str] = None, raise_for_status: bool = False
) -> None:
    """Ping a server that answers with the given status."""

    async def jsonrpc(request: web.Request) -> web.Response:
        return web.Response(status=status, reason=reason)

    async def run() -> None:
        app = web.Application()
        app.router.add_post("/jsonrpc", jsonrpc)
        runner = await start_server(app)
        try:
            await ping("127.0.0.1", runner.addresses[0][1], raise_for_status)
        finally:
            await runner.cleanup()

    asyncio.run(run())


def assert_no_login(error: pytest.ExceptionInfo[Exception]) -> None:
    """Assert that the error does not show the login of ping()."""
    for login in ("secret", basic("kodi:secret").removeprefix("Basic ")):
        assert login not in str(error.value)
        assert login not in repr(error.value)


def assert_status(
    error: pytest.ExceptionInfo[Exception], status: int, raise_for_status: bool
) -> None:
    """Assert the status that the TransportError behind the error reports."""
    cause = error.value.__cause__
    assert isinstance(cause, TransportError)
    args = cause.args
    if raise_for_status:
        assert args[1].status == status
    else:
        assert f": HTTP {status} " in args[0]


@pytest.mark.parametrize("reason", [None, ""])
@pytest.mark.parametrize("raise_for_status", [False, True])
def test_http_rejected_credentials(
    reason: Optional[str], raise_for_status: bool
) -> None:
    with pytest.raises(InvalidAuthError) as error:
        ping_with_status(401, reason, raise_for_status)
    assert_status(error, 401, raise_for_status)


@pytest.mark.parametrize("status", [403, 407, 500])
@pytest.mark.parametrize(
    ("raise_for_status", "detail"),
    [
        (False, lambda status, cause: f"HTTP {status} {HTTPStatus(status).phrase}"),
        (True, lambda status, cause: f"Transport Error: {cause.args[1]}"),
    ],
    ids=["status_line", "raise_for_status"],
)
def test_http_error_status(
    status: int, raise_for_status: bool, detail: Callable[[int, Any], str]
) -> None:
    with pytest.raises(CannotConnectError) as error:
        ping_with_status(status, raise_for_status=raise_for_status)
    assert_status(error, status, raise_for_status)
    assert str(error.value) == "Error calling method 'JSONRPC.Ping': " + detail(
        status, error.value.__cause__
    )
    assert_no_login(error)


def test_http_server_error_with_401_in_reason() -> None:
    with pytest.raises(CannotConnectError) as error:
        ping_with_status(502, reason="Upstream: HTTP 401 Unauthorized")
    assert "HTTP 401" in str(error.value.__cause__)


def test_refused_connection_on_port_with_401() -> None:
    for port in range(4010, 4020):
        with socket.socket() as blocker:
            try:
                blocker.bind(("127.0.0.1", port))
            except OSError:
                continue
            # Bound without listen(): the port refuses connections and stays taken
            with pytest.raises(CannotConnectError) as error:
                asyncio.run(ping("127.0.0.1", port))
            cause = error.value.__cause__
            assert isinstance(cause, TransportError)
            assert "401" in str(cause)
            wrapped = cause.args[1]
            assert isinstance(wrapped, aiohttp.ClientConnectorError)
            assert wrapped.os_error.errno == errno.ECONNREFUSED
            return
    pytest.skip("no free port between 4010 and 4019")


def test_unknown_host_with_401() -> None:
    with pytest.raises(CannotConnectError) as error:
        asyncio.run(ping("kodi401.invalid", 8080, resolver=NoHosts()))
    assert "kodi401.invalid not found" in str(error.value.__cause__)


def ping_with_raw_reply(reply: bytes, keep_open: bool, request_timeout: float) -> None:
    """Ping a server that sends the given bytes as its whole reply."""

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        head = await reader.readuntil(b"\r\n\r\n")
        length = re.search(rb"(?im)^content-length: *(\d+)", head)
        await reader.readexactly(int(length.group(1)) if length else 0)
        writer.write(reply)
        await writer.drain()
        if keep_open:
            await reader.read()
        writer.close()

    async def run() -> None:
        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        try:
            await ping(
                "127.0.0.1",
                server.sockets[0].getsockname()[1],
                request_timeout=request_timeout,
            )
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(run())


@pytest.mark.parametrize(
    ("reply", "keep_open", "request_timeout", "wrapped", "text"),
    [
        (
            b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n"
            b"Content-Length: 5\r\n\r\nlogin",
            False,
            5,
            aiohttp.ContentTypeError,
            "Cannot deserialize response body: 200, message='Attempt to decode JSON",
        ),
        (
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            b"Content-Length: 100\r\n\r\n{",
            False,
            5,
            aiohttp.ClientPayloadError,
            "Transport Error: Response payload is not completed",
        ),
        (
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            b"Content-Length: 100\r\n\r\n{",
            True,
            2,
            asyncio.TimeoutError,
            "Transport Error: TimeoutError",
        ),
    ],
    ids=["html", "cut_off", "stalled"],
)
def test_http_unusable_reply(
    reply: bytes,
    keep_open: bool,
    request_timeout: float,
    wrapped: Type[Exception],
    text: str,
) -> None:
    with pytest.raises(CannotConnectError) as error:
        ping_with_raw_reply(reply, keep_open, request_timeout)
    assert isinstance(error.value.__cause__, TransportError)
    assert isinstance(error.value.__cause__.args[1], wrapped)
    assert str(error.value).startswith(f"Error calling method 'JSONRPC.Ping': {text}")
    assert_no_login(error)


def test_http_protocol_error_passes_through() -> None:
    async def jsonrpc(request: web.Request) -> web.Response:
        message = await request.json()
        return web.json_response(
            {
                "jsonrpc": "2.0",
                "id": message["id"],
                "error": {"code": -32601, "message": "Method not found."},
            }
        )

    async def run() -> None:
        app = web.Application()
        app.router.add_post("/jsonrpc", jsonrpc)
        runner = await start_server(app)
        try:
            await ping("127.0.0.1", runner.addresses[0][1])
        finally:
            await runner.cleanup()

    with pytest.raises(ProtocolError) as error:
        asyncio.run(run())
    assert error.value.args[0] == -32601


def test_http_cancelled_ping() -> None:
    async def run() -> None:
        received = asyncio.Event()

        async def handle(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            await reader.readuntil(b"\r\n\r\n")
            received.set()
            await reader.read()
            writer.close()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        task = asyncio.create_task(
            ping("127.0.0.1", server.sockets[0].getsockname()[1])
        )
        waiter = asyncio.create_task(received.wait())
        try:
            await asyncio.wait(
                {task, waiter}, timeout=5, return_when=asyncio.FIRST_COMPLETED
            )
            assert not task.done(), task.result()
            assert received.is_set()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            waiter.cancel()
            task.cancel()
            await asyncio.gather(task, waiter, return_exceptions=True)
            server.close()
            await server.wait_closed()

    asyncio.run(run())
