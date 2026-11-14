"""Tests for connecting over websocket."""

import asyncio
import gc
import inspect
import logging
import sys
from typing import Any, List

import aiohttp
from aiohttp import web
from jsonrpc_base import TransportError
import pytest

from pykodi import CannotConnectError, get_kodi_connection


async def start_silent_server() -> asyncio.Server:
    """Accept connections but never answer the websocket handshake."""

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        await reader.read()
        writer.close()

    return await asyncio.start_server(handle, "127.0.0.1", 0)


async def start_kodi(mode: str = "open") -> web.AppRunner:
    """Start a websocket server.

    mode "open" keeps the websocket open, "silent" never answers the closing
    handshake.
    """

    async def jsonrpc(request: web.Request) -> web.WebSocketResponse:
        websocket = web.WebSocketResponse(autoclose=mode != "silent")
        await websocket.prepare(request)
        if mode == "silent":
            await asyncio.sleep(30)
        else:
            async for _ in websocket:
                pass
        return websocket

    app = web.Application()
    app.router.add_get("/jsonrpc", jsonrpc)
    runner = web.AppRunner(app, handler_cancellation=True, shutdown_timeout=0.1)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", 0).start()
    return runner


async def wait_for_handshake(connection: Any) -> "asyncio.Task[Any]":
    """Return the handshake of the running connect() once it started."""
    for _ in range(1000):
        if connection._handshake is not None:
            return connection._handshake
        await asyncio.sleep(0)
    raise AssertionError("connect() did not start a handshake")


async def cancel_when_handshake_ends(
    connection: Any, connecting: "asyncio.Future[Any]"
) -> None:
    """Cancel connect() once the handshake ended, before connect() goes on."""
    handshake = await wait_for_handshake(connection)
    handshake.add_done_callback(lambda _: connecting.cancel())


def test_connect_times_out() -> None:
    async def run() -> None:
        server = await start_silent_server()
        port = server.sockets[0].getsockname()[1]
        try:
            async with aiohttp.ClientSession() as session:
                connection = get_kodi_connection(
                    "127.0.0.1", 8080, port, None, None, timeout=0.2, session=session
                )
                ended: List[bool] = []

                async def connect() -> None:
                    try:
                        await connection.connect()
                    finally:
                        # The handshake has ended once connect() raises
                        ended.append(handshake.done())

                start = asyncio.get_running_loop().time()
                connecting = asyncio.ensure_future(connect())
                handshake = await wait_for_handshake(connection)
                with pytest.raises(CannotConnectError) as error:
                    await asyncio.wait_for(connecting, 5)
                assert asyncio.get_running_loop().time() - start < 2
                assert isinstance(error.value.__cause__, asyncio.TimeoutError)
                assert not connection.connected
                assert ended == [True]
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(run())


def test_connect_refused() -> None:
    async def run() -> None:
        # A port nothing listens on anymore
        server = await start_silent_server()
        port = server.sockets[0].getsockname()[1]
        server.close()
        await server.wait_closed()
        async with aiohttp.ClientSession() as session:
            connection = get_kodi_connection(
                "127.0.0.1", 8080, port, None, None, timeout=5, session=session
            )
            with pytest.raises(CannotConnectError) as error:
                await asyncio.wait_for(connection.connect(), 5)
            assert isinstance(error.value.__cause__, TransportError)
            assert not connection.connected

    asyncio.run(run())


def test_connect_can_be_cancelled() -> None:
    async def run() -> None:
        server = await start_silent_server()
        port = server.sockets[0].getsockname()[1]
        try:
            async with aiohttp.ClientSession() as session:
                connection = get_kodi_connection(
                    "127.0.0.1", 8080, port, None, None, timeout=30, session=session
                )
                ended: List[bool] = []

                async def connect() -> None:
                    try:
                        await connection.connect()
                    finally:
                        # The handshake has ended once connect() raises
                        ended.append(handshake.done())

                task = asyncio.ensure_future(connect())
                handshake = await wait_for_handshake(connection)
                await asyncio.sleep(0.1)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 5)
                assert task.cancelled()
                assert ended == [True]
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(run())


def test_read_loop_error_is_retrieved(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="pykodi.kodi")
    unhandled: List[Any] = []

    async def jsonrpc(request: web.Request) -> web.WebSocketResponse:
        websocket = web.WebSocketResponse()
        await websocket.prepare(request)
        await websocket.send_str("not json")
        async for _ in websocket:
            pass
        return websocket

    async def run() -> None:
        asyncio.get_running_loop().set_exception_handler(
            lambda loop, context: unhandled.append(context)
        )
        app = web.Application()
        app.router.add_get("/jsonrpc", jsonrpc)
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, "127.0.0.1", 0).start()
        port = runner.addresses[0][1]
        try:
            async with aiohttp.ClientSession() as session:
                connection = get_kodi_connection(
                    "127.0.0.1", 8080, port, None, None, session=session
                )
                await connection.connect()
                for _ in range(200):
                    if not connection.connected:
                        break
                    await asyncio.sleep(0.01)
                assert not connection.connected
                await connection.close()
        finally:
            await runner.cleanup()
        gc.collect()
        await asyncio.sleep(0)

    asyncio.run(run())

    assert unhandled == []
    lost = [
        record
        for record in caplog.records
        if record.getMessage() == "Websocket connection to Kodi lost"
    ]
    assert len(lost) == 1
    assert lost[0].exc_info is not None


def test_close_stops_read_loop() -> None:
    async def jsonrpc(request: web.Request) -> web.WebSocketResponse:
        websocket = web.WebSocketResponse()
        await websocket.prepare(request)
        async for _ in websocket:
            pass
        return websocket

    async def run() -> None:
        app = web.Application()
        app.router.add_get("/jsonrpc", jsonrpc)
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, "127.0.0.1", 0).start()
        port = runner.addresses[0][1]
        try:
            async with aiohttp.ClientSession() as session:
                connection = get_kodi_connection(
                    "127.0.0.1", 8080, port, None, None, session=session
                )
                before = asyncio.all_tasks()
                await connection.connect()
                await asyncio.sleep(0)
                assert connection.connected
                await connection.close()
                assert not connection.connected
                assert asyncio.all_tasks() - before == set()
        finally:
            await runner.cleanup()

    asyncio.run(run())


def test_close_from_callback() -> None:
    async def run() -> None:
        quit_sent = asyncio.Event()

        async def jsonrpc(request: web.Request) -> web.WebSocketResponse:
            websocket = web.WebSocketResponse()
            await websocket.prepare(request)
            await quit_sent.wait()
            await websocket.send_json(
                {
                    "jsonrpc": "2.0",
                    "method": "System.OnQuit",
                    "params": {"sender": "xbmc", "data": None},
                }
            )
            async for _ in websocket:
                pass
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
                    "127.0.0.1", 8080, port, None, None, session=session
                )
                closed = asyncio.Event()

                async def on_quit(sender: str, data: Any) -> None:
                    await connection.close()
                    closed.set()

                connection.subscribe("System.OnQuit", on_quit)
                await connection.connect()
                quit_sent.set()
                await asyncio.wait_for(closed.wait(), 2)
                assert not connection.connected
        finally:
            await runner.cleanup()

    asyncio.run(run())


def test_close_from_callback_right_after_connect() -> None:
    async def run() -> None:
        release = asyncio.Event()
        open_connections = 0

        async def jsonrpc(request: web.Request) -> web.WebSocketResponse:
            nonlocal open_connections
            open_connections += 1
            websocket = web.WebSocketResponse(autoclose=False)
            await websocket.prepare(request)
            # Keep sending notifications and never answer the closing handshake
            try:
                while not release.is_set():
                    await websocket.send_json(
                        {
                            "jsonrpc": "2.0",
                            "method": "System.OnQuit",
                            "params": {"sender": "xbmc", "data": None},
                        }
                    )
                    await asyncio.sleep(0.1)
            except ConnectionResetError:
                pass
            finally:
                open_connections -= 1
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
                    "127.0.0.1", 8080, port, None, None, timeout=0.5, session=session
                )
                closed = asyncio.Event()

                async def on_quit(sender: str, data: Any) -> None:
                    if not closed.is_set():
                        await connection.close()
                        closed.set()

                connection.subscribe("System.OnQuit", on_quit)
                await connection.connect()
                await asyncio.wait_for(closed.wait(), 2)
                assert not connection.connected
                # The read loop started by connect() is aborted as well
                for _ in range(200):
                    if open_connections == 0:
                        break
                    await asyncio.sleep(0.01)
                assert open_connections == 0
        finally:
            release.set()
            await runner.cleanup()

    asyncio.run(run())


def test_connect_after_cancelled_close() -> None:
    async def run() -> None:
        release = asyncio.Event()
        open_connections = 0

        async def jsonrpc(request: web.Request) -> web.WebSocketResponse:
            nonlocal open_connections
            open_connections += 1
            websocket = web.WebSocketResponse(autoclose=False)
            await websocket.prepare(request)
            # Keep sending notifications and never answer the closing handshake
            try:
                while not release.is_set():
                    await websocket.send_json(
                        {
                            "jsonrpc": "2.0",
                            "method": "Player.OnSeek",
                            "params": {"sender": "xbmc", "data": None},
                        }
                    )
                    await asyncio.sleep(0.1)
            except ConnectionResetError:
                pass
            finally:
                open_connections -= 1
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
                    "127.0.0.1", 8080, port, None, None, timeout=0.5, session=session
                )
                await connection.connect()
                await asyncio.sleep(0)
                with pytest.raises(asyncio.TimeoutError):
                    await asyncio.wait_for(connection.close(), 0.2)
                assert not connection.connected
                start = asyncio.get_running_loop().time()
                await asyncio.wait_for(connection.connect(), 5)
                assert asyncio.get_running_loop().time() - start < 2
                assert connection.connected
                # The read loop of the first connection is aborted
                for _ in range(200):
                    if open_connections == 1:
                        break
                    await asyncio.sleep(0.01)
                assert open_connections == 1
                release.set()
                await asyncio.wait_for(connection.close(), 5)
        finally:
            release.set()
            await runner.cleanup()

    asyncio.run(run())


def test_close_is_bounded() -> None:
    async def run() -> None:
        release = asyncio.Event()
        open_connections = 0

        async def jsonrpc(request: web.Request) -> web.WebSocketResponse:
            nonlocal open_connections
            open_connections += 1
            websocket = web.WebSocketResponse(autoclose=False)
            await websocket.prepare(request)
            # Keep sending notifications and never answer the closing handshake
            try:
                while not release.is_set():
                    await websocket.send_json(
                        {
                            "jsonrpc": "2.0",
                            "method": "Player.OnSeek",
                            "params": {"sender": "xbmc", "data": None},
                        }
                    )
                    await asyncio.sleep(0.1)
            except ConnectionResetError:
                pass
            finally:
                open_connections -= 1
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
                    "127.0.0.1", 8080, port, None, None, timeout=0.5, session=session
                )
                await connection.connect()
                start = asyncio.get_running_loop().time()
                await asyncio.wait_for(connection.close(), 5)
                assert asyncio.get_running_loop().time() - start < 2
                assert not connection.connected
                # The connection is aborted, not left to the next connect()
                for _ in range(200):
                    if open_connections == 0:
                        break
                    await asyncio.sleep(0.01)
                assert open_connections == 0
                await asyncio.wait_for(connection.connect(), 5)
                assert connection.connected
                release.set()
                await asyncio.wait_for(connection.close(), 5)
        finally:
            release.set()
            await runner.cleanup()

    asyncio.run(run())


def test_close_stops_a_pending_handshake() -> None:
    async def run() -> None:
        release = asyncio.Event()
        websockets = 0

        async def jsonrpc(request: web.Request) -> web.WebSocketResponse:
            nonlocal websockets
            # Answer the handshake only once the test lets it
            await release.wait()
            websocket = web.WebSocketResponse()
            await websocket.prepare(request)
            websockets += 1
            async for _ in websocket:
                pass
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
                    "127.0.0.1", 8080, port, None, None, timeout=30, session=session
                )
                lost: List[Any] = []
                connection.on_disconnect(lost.append)
                connecting = asyncio.ensure_future(connection.connect())
                handshake = await wait_for_handshake(connection)
                await asyncio.sleep(0.1)
                start = asyncio.get_running_loop().time()
                await asyncio.wait_for(connection.close(), 5)
                # close() waits until the handshake has stopped
                assert handshake.done()
                with pytest.raises(CannotConnectError):
                    await asyncio.wait_for(connecting, 5)
                assert asyncio.get_running_loop().time() - start < 2
                release.set()
                await asyncio.sleep(0.2)
                assert not connection.connected
                assert websockets == 0
                assert lost == []
        finally:
            release.set()
            await runner.cleanup()

    asyncio.run(run())


def test_close_stops_a_waiting_connect() -> None:
    async def run() -> None:
        server = await start_silent_server()
        port = server.sockets[0].getsockname()[1]
        try:
            async with aiohttp.ClientSession() as session:
                connection = get_kodi_connection(
                    "127.0.0.1", 8080, port, None, None, timeout=30, session=session
                )
                first = asyncio.ensure_future(connection.connect())
                second = asyncio.ensure_future(connection.connect())
                await asyncio.sleep(0.1)
                await asyncio.wait_for(connection.close(), 5)
                results = await asyncio.wait_for(
                    asyncio.gather(first, second, return_exceptions=True), 5
                )
                assert [type(result) for result in results] == [
                    CannotConnectError,
                    CannotConnectError,
                ]
                assert not connection.connected
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(run())


def test_connect_after_a_stopped_connect() -> None:
    async def run() -> None:
        answer = False
        release = asyncio.Event()

        async def jsonrpc(request: web.Request) -> web.WebSocketResponse:
            if not answer:
                await release.wait()
            websocket = web.WebSocketResponse()
            await websocket.prepare(request)
            async for _ in websocket:
                pass
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
                    "127.0.0.1", 8080, port, None, None, timeout=30, session=session
                )
                connecting = asyncio.ensure_future(connection.connect())
                await asyncio.sleep(0.1)
                await asyncio.wait_for(connection.close(), 5)
                with pytest.raises(CannotConnectError):
                    await asyncio.wait_for(connecting, 5)
                answer = True
                await asyncio.wait_for(connection.connect(), 5)
                assert connection.connected
                await asyncio.wait_for(connection.close(), 5)
        finally:
            release.set()
            await runner.cleanup()

    asyncio.run(run())


def test_cancel_right_after_the_handshake_leaves_no_connection() -> None:
    async def run() -> None:
        websockets = 0

        async def jsonrpc(request: web.Request) -> web.WebSocketResponse:
            nonlocal websockets
            websocket = web.WebSocketResponse()
            await websocket.prepare(request)
            websockets += 1
            try:
                async for _ in websocket:
                    pass
            finally:
                websockets -= 1
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
                lost: List[Any] = []
                connection.on_disconnect(lost.append)
                connecting = asyncio.ensure_future(connection.connect())
                await cancel_when_handshake_ends(connection, connecting)
                with pytest.raises(asyncio.CancelledError):
                    await connecting
                assert not connection.connected
                for _ in range(200):
                    if websockets == 0:
                        break
                    await asyncio.sleep(0.01)
                assert websockets == 0
                assert lost == []
        finally:
            await runner.cleanup()

    asyncio.run(run())


def test_cancel_right_after_the_handshake_is_bounded() -> None:
    async def run() -> None:
        runner = await start_kodi("silent")
        port = runner.addresses[0][1]
        try:
            async with aiohttp.ClientSession() as session:
                connection = get_kodi_connection(
                    "127.0.0.1", 8080, port, None, None, timeout=0.5, session=session
                )
                connecting = asyncio.ensure_future(connection.connect())
                await cancel_when_handshake_ends(connection, connecting)
                start = asyncio.get_running_loop().time()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(connecting, 5)
                assert asyncio.get_running_loop().time() - start < 2
        finally:
            await runner.cleanup()

    asyncio.run(run())


def test_cancel_right_after_the_handshake_waits_for_the_close() -> None:
    async def run() -> None:
        runner = await start_kodi()
        port = runner.addresses[0][1]
        try:
            async with aiohttp.ClientSession() as session:
                connection = get_kodi_connection(
                    "127.0.0.1", 8080, port, None, None, timeout=5, session=session
                )
                connecting = asyncio.ensure_future(connection.connect())
                await cancel_when_handshake_ends(connection, connecting)
                with pytest.raises(asyncio.CancelledError):
                    await connecting
                assert connection._ws_task.done()
        finally:
            await runner.cleanup()

    asyncio.run(run())


def test_cancel_right_after_a_failed_handshake() -> None:
    unhandled: List[Any] = []

    async def run() -> None:
        asyncio.get_running_loop().set_exception_handler(
            lambda loop, context: unhandled.append(context)
        )
        # A port nothing listens on anymore
        server = await start_silent_server()
        port = server.sockets[0].getsockname()[1]
        server.close()
        await server.wait_closed()
        async with aiohttp.ClientSession() as session:
            connection = get_kodi_connection(
                "127.0.0.1", 8080, port, None, None, timeout=5, session=session
            )
            connecting = asyncio.ensure_future(connection.connect())
            await cancel_when_handshake_ends(connection, connecting)
            with pytest.raises(asyncio.CancelledError):
                await connecting
            del connecting
        # The error of the failed handshake was retrieved
        gc.collect()
        await asyncio.sleep(0)

    asyncio.run(run())
    assert unhandled == []


def test_connect_after_a_cancelled_connect_with_own_session() -> None:
    async def run() -> None:
        runner = await start_kodi()
        port = runner.addresses[0][1]
        try:
            connection = get_kodi_connection(
                "127.0.0.1", 8080, port, None, None, timeout=5
            )
            connecting = asyncio.ensure_future(connection.connect())
            await cancel_when_handshake_ends(connection, connecting)
            with pytest.raises(asyncio.CancelledError):
                await connecting
            await asyncio.wait_for(connection.connect(), 5)
            assert connection.connected
            await connection.close()
        finally:
            await runner.cleanup()

    asyncio.run(run())


def test_other_handshake_errors_are_raised() -> None:
    async def run() -> None:
        session = aiohttp.ClientSession()
        await session.close()
        connection = get_kodi_connection(
            "127.0.0.1", 8080, 9, None, None, timeout=5, session=session
        )
        with pytest.raises(RuntimeError):
            await connection.connect()

    asyncio.run(run())


@pytest.mark.parametrize(
    "eager",
    [
        False,
        pytest.param(
            True,
            marks=pytest.mark.skipif(
                sys.version_info < (3, 12), reason="needs eager_task_factory"
            ),
        ),
    ],
)
def test_read_loop_started_when_connect_returns(eager: bool) -> None:
    async def run() -> None:
        if eager:
            asyncio.get_running_loop().set_task_factory(asyncio.eager_task_factory)
        runner = await start_kodi()
        port = runner.addresses[0][1]
        try:
            async with aiohttp.ClientSession() as session:
                connection = get_kodi_connection(
                    "127.0.0.1", 8080, port, None, None, timeout=5, session=session
                )
                await connection.connect()
                coroutine = connection._ws_task.get_coro()
                assert inspect.getcoroutinestate(coroutine) != inspect.CORO_CREATED
                await connection.close()
        finally:
            await runner.cleanup()

    asyncio.run(run())


def test_second_cancel_right_after_the_handshake_leaves_no_connection() -> None:
    async def run() -> None:
        websockets = 0

        async def jsonrpc(request: web.Request) -> web.WebSocketResponse:
            nonlocal websockets
            websocket = web.WebSocketResponse()
            await websocket.prepare(request)
            websockets += 1
            try:
                async for _ in websocket:
                    pass
            finally:
                websockets -= 1
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
                connecting = asyncio.ensure_future(connection.connect())
                handshake = await wait_for_handshake(connection)
                loop = asyncio.get_running_loop()

                def cancel_twice(_: Any) -> None:
                    connecting.cancel()
                    # Cancelled again a loop iteration later, for example by an
                    # enclosing timeout
                    loop.call_soon(loop.call_soon, connecting.cancel)

                handshake.add_done_callback(cancel_twice)
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(connecting, 5)
                assert not connection.connected
                for _ in range(200):
                    if websockets == 0:
                        break
                    await asyncio.sleep(0.01)
                assert websockets == 0
        finally:
            await runner.cleanup()

    asyncio.run(run())


def test_read_loop_error_is_retrieved_after_a_cancel_right_after_the_handshake(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="pykodi.kodi")
    unhandled: List[Any] = []

    async def run() -> None:
        asyncio.get_running_loop().set_exception_handler(
            lambda loop, context: unhandled.append(context)
        )

        async def jsonrpc(request: web.Request) -> web.WebSocketResponse:
            websocket = web.WebSocketResponse()
            await websocket.prepare(request)
            await websocket.send_str("not json")
            async for _ in websocket:
                pass
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
                connecting = asyncio.ensure_future(connection.connect())
                await cancel_when_handshake_ends(connection, connecting)
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(connecting, 5)
        finally:
            await runner.cleanup()
        gc.collect()
        await asyncio.sleep(0)

    asyncio.run(run())
    assert unhandled == []
    lost = [
        record
        for record in caplog.records
        if record.getMessage() == "Websocket connection to Kodi lost"
    ]
    assert len(lost) == 1
