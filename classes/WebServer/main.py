# Description: main.py — FastAPI application for the MPG Web Management UI.
# File: main.py
#
# Copyright 2026 Kevin Burke
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://apache.org
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
main.py — FastAPI application for the MPG Web Management UI.

Entry point called from protocol_gateway.py:

    from classes.WebServer.main import start_webserver
    start_webserver(config_file=config_path, gateway_instance=mpg)

The server runs on a daemon thread (port 1717) and shuts down automatically
when the gateway process exits.

config_path passed from protocol_gateway.main() is the fully-resolved Path to
the config file (e.g. <project_root>/config/config.cfg).  start_webserver()
derives project_root by walking up until pyproject.toml is found, matching
the same logic used in protocol_gateway.main().
"""

from __future__ import annotations

import asyncio
import hashlib as _hashlib
import logging
import logging.handlers
import queue as _queue
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Protocol, cast

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.engine import Engine
from starlette.types import ASGIApp, Receive, Scope, Send

from classes.WebServer.models import Base, ConfigBackup

from .database import ensure_app_state, init_db, run_migrations, session_scope
from .file_watcher import FileWatcher
from .routers.analysis import router as analysis_router
from .routers.bridges import router as bridges_router
from .routers.commit import router as commit_router
from .routers.devices import router as devices_router
from .routers.gateway_status import router as gateway_status_router
from .routers.help import FileResponse
from .routers.help import router as help_router
from .routers.pages import router as pages_router
from .routers.protocols import router as protocols_router
from .routers.timescale import router as timescale_router
from .routers.transport_settings import router as transport_settings_router
from .scanner import Scanner
from .services.bridge_service import is_timescale_available
from .services.setting_description_service import seed_setting_descriptions

_log: logging.Logger = logging.getLogger(__name__)

JsonValue = str | int | float | bool | dict[str, str | bool] | None


class GatewayManagerLike(Protocol):
    @property
    def current(self) -> object | None: ...

    def reload(self, trigger: str) -> "ReloadStatusLike": ...


class ReloadStatusLike(Protocol):
    ok: bool
    message: str


class GatewayInstanceLike(Protocol):
    web_server: "NoSignalServer | None"


# ---------------------------------------------------------------------------
# Logging — attach a dedicated rotating file handler to the "classes.WebServer"
# logger subtree only.  We never touch the root logger or the gateway's
# handlers.  All webserver loggers inherit from "classes.WebServer.*" so they
# naturally pick up this handler without taking anything from the gateway.
#
# A QueueHandler + QueueListener pair is still used so the uvicorn event-loop
# thread never blocks on file I/O (the listener drains on its own thread).
# ---------------------------------------------------------------------------

_queue_listener: logging.handlers.QueueListener | None = None


# Define custom filter class
class UvicornInfoRenameFilter(logging.Filter):
    """
    Interceptors and renames the logger from 'uvicorn.error' to
    'uvicorn.info' ONLY when the log level is strictly INFO.
    """
    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelname == "INFO" and record.name == "uvicorn.error":
            record.name = "uvicorn.info"
        return True


def _install_webserver_logging() -> None:
    """
    Reroutes WebServer and Uvicorn logs into the central Gateway _log.
    Uses a QueueListener to prevent Windows file-locking (IO) contention.
    """
    global _queue_listener

    ws_logger: logging.Logger = logging.getLogger("classes.WebServer")

    # Idempotency check: don't re-install if QueueHandler is present
    if any(isinstance(h, logging.handlers.QueueHandler) for h in ws_logger.handlers):
        return

    # ── Identify Central Handlers ──────────────────────────────────────────
    root_handlers: List[logging.Handler] = logging.getLogger().handlers

    if not root_handlers:
        ws_logger.propagate = True
        return

    # ── Non-Blocking Queue Setup ───────────────────────────────────────────
    log_queue: _queue.SimpleQueue[logging.LogRecord] = _queue.SimpleQueue()
    queue_handler = logging.handlers.QueueHandler(log_queue)

    # Attach the filter to the queue_handler here
    # This guarantees it processes records from ALL intercepted loggers
    queue_handler.addFilter(UvicornInfoRenameFilter())

    # The listener picks up items from the queue and feeds them
    # into the existing root handlers (File, Console, etc.)
    listener = logging.handlers.QueueListener(
        log_queue,
        *root_handlers,
        respect_handler_level=True
    )
    listener.start()
    _queue_listener = listener

    # ── Configuration ──────────────────────────────────────────────────────
    ws_logger.addHandler(queue_handler)
    ws_logger.setLevel(logging.DEBUG)

    # Important: False prevents double-logging (Queue -> Root -> File)
    ws_logger.propagate = False

    # Redirect 3rd party libraries to use the same non-blocking queue
    intercept_loggers = (
        "uvicorn", "uvicorn.access", "uvicorn.error",
        "fastapi", "alembic", "sqlalchemy.engine"
    )

    for name in intercept_loggers:
        logs: logging.Logger = logging.getLogger(name)
        # Remove any default handlers to ensure they only use our queue
        for hands in logs.handlers[:]:
            logs.removeHandler(hands)
        logs.addHandler(queue_handler)
        logs.propagate = False

    ws_logger.info("WebServer logging merged into central log via QueueListener.")


# ---------------------------------------------------------------------------
# Module-level path constants — resolved relative to this file
# (classes/WebServer/main.py → classes/WebServer/)
# ---------------------------------------------------------------------------
_WEB_DIR:       Path = Path(__file__).resolve().parent
_TEMPLATES_DIR: Path = _WEB_DIR / "templates"
_STATIC_DIR:    Path = _WEB_DIR / "static"
_ALEMBIC_INI:   Path = _WEB_DIR / "alembic.ini"


# ---------------------------------------------------------------------------
# Uvicorn server subclass — disables signal handling so the background
# thread does not conflict with the gateway's own signal handlers.
# ---------------------------------------------------------------------------

class NoSignalServer(uvicorn.Server):
    def install_signal_handlers(self) -> None:
        """No-op: background threads cannot install signal handlers."""
        pass


# ---------------------------------------------------------------------------
# Prometheus bridge auto-mount
# ---------------------------------------------------------------------------

def _get_prometheus_bridges(gateway_instance: object | None) -> list[object]:
    """
    Shared duck-typed lookup for every configured prometheus_out transport
    on gateway_instance, mirroring
    services.bridge_service.get_prometheus_bridge(). Used by both
    _mount_prometheus_bridges() (below) and _collect_metrics_ports() /
    start_webserver() so all three agree on what counts as "a Prometheus
    bridge" without a hard import of prometheus_out at module load time.
    """
    if gateway_instance is None:
        return []
    transports: list[object] = getattr(gateway_instance, "_Protocol_Gateway__transports", [])
    return [t for t in transports if type(t).__name__ == "prometheus_out"]


def _collect_metrics_ports(gateway_instance: object | None) -> dict[int, list[str]]:
    """
    Groups configured prometheus_out bridges by their optional
    `metrics_port`, returning {port: [metrics_path, ...]}.

    A bridge that leaves `metrics_port` unset (the default) is omitted
    here entirely -- it's reachable only on the main WebServer port via
    _mount_prometheus_bridges(), same as always. `metrics_port` doesn't
    start a second server: it's an additional listening socket on the
    SAME uvicorn.Server / event loop / app object (see start_webserver()),
    restricted at the ASGI layer (RestrictPortMiddleware, below) to only
    ever serve that bridge's metrics_path -- not the rest of the web UI --
    so it's safe to expose to a separate network segment than the config
    UI itself.
    """
    ports: dict[int, list[str]] = {}
    for bridge in _get_prometheus_bridges(gateway_instance):
        port: int | None = getattr(bridge, "metrics_port", None)
        if port is None:
            continue
        path: str = getattr(bridge, "metrics_path", "/metrics")
        ports.setdefault(int(port), []).append(path)
    return ports


class RestrictPortMiddleware:
    """
    ASGI middleware: when a request arrives on `restricted_port`, only
    paths starting with one of `allowed_prefixes` are served -- everything
    else gets a 404. Requests arriving on any other port are untouched.

    This is what makes a Prometheus bridge's optional `metrics_port` a
    genuine network-segmentation boundary rather than just an alias for
    the same web UI on a second socket: even if the metrics port is
    reachable from somewhere the config UI shouldn't be, only
    metrics_path is actually servable from there. There's still exactly
    one FastAPI app, one uvicorn.Server, one event loop, one thread --
    this middleware runs inside that same single request-handling path.
    """

    def __init__(self, app: ASGIApp, restricted_port: int, allowed_prefixes: tuple[str, ...]) -> None:
        self.app: ASGIApp = app
        self.restricted_port: int = restricted_port
        self.allowed_prefixes: tuple[str, ...] = allowed_prefixes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            server: tuple[str, int] | None = scope.get("server")
            incoming_port: int | None = server[1] if server else None
            if incoming_port == self.restricted_port:
                path: str = scope.get("path", "")
                if not path.startswith(self.allowed_prefixes):
                    await PlainTextResponse("Not Found", status_code=404)(scope, receive, send)
                    return
        await self.app(scope, receive, send)


def _mount_prometheus_bridges(app: FastAPI, gateway_instance: object | None) -> None:
    """
    Mount every configured prometheus_out bridge's /metrics endpoint onto
    this WebServer's own FastAPI app (the same process already serving the
    web UI on _current_port, e.g. 1717). There is no other way to serve
    a prometheus_out bridge -- it has no standalone-server mode of its
    own, since MPG always runs this WebServer (protocol_gateway.main()
    calls start_webserver() unconditionally, with no headless code path).
    A bridge's optional `metrics_port` (see _collect_metrics_ports() and
    start_webserver()) adds an extra restricted listening socket for this
    same mount -- it doesn't change how the mount itself is created here.

    This is exactly the wiring prometheus_out.attach_metrics_route()'s own
    docstring describes as the intended "clean setup" call from main.py --
    it was just never actually called anywhere before, so a configured
    bridge served nothing at all.

    Does NOT update the "Configured Devices" dashboard's Host column --
    that reads literal Setting DB rows sourced from config.cfg text (see
    device_service.get_nav_data()), not this live transport object. See
    transport_defaults.json's prometheus_out entry for the actual fix.

    Caveat: this runs once, at process startup, mounting whatever bridge
    object(s) exist on gateway_instance at that moment. A live config
    reload (gateway_manager.reload(), see _on_config_changed below) builds
    a brand-new Protocol_Gateway with brand-new transport objects, but does
    NOT re-run this function -- the mounted route would keep serving the
    original (now-orphaned) bridge object's registry. This mirrors an
    existing limitation elsewhere in this codebase (see the MQTT bridge's
    write-topic "Startup Requirement" docs): adding, removing, or changing
    the metrics_path/metrics_port of a Prometheus bridge requires a full
    process restart to take effect, not just a config commit.
    """
    prometheus_bridges: List[object] = _get_prometheus_bridges(gateway_instance)
    if not prometheus_bridges:
        return

    try:
        from classes.transports.prometheus_out import attach_metrics_route
        from classes.transports.prometheus_out import prometheus_out as _PrometheusOut
    except ImportError as exc:
        _log.error(
            "Found a configured prometheus_out bridge but prometheus_client "
            "is not installed (%s). Install it with `pip install "
            "prometheus_client` or add it to requirements.txt.",
            exc,
        )
        return

    mounted_paths: set[str] = set()
    for raw_bridge in prometheus_bridges:
        # raw_bridge is `object` here on purpose -- _get_prometheus_bridges()
        # duck-types on the class name so this module never needs a hard
        # import of prometheus_out at module load time. We've already
        # confirmed type(raw_bridge).__name__ == "prometheus_out" there, so
        # this cast is just telling the type checker what we already know
        # at runtime; it has no runtime effect of its own, and lets typed
        # attribute access below (bridge.metrics_port, etc.) type-check
        # normally instead of needing getattr() everywhere.
        bridge: _PrometheusOut = cast(_PrometheusOut, raw_bridge)
        name: str = getattr(bridge, "transport_name", "?")
        mount_path: str = getattr(bridge, "metrics_path", "/metrics")

        if mount_path in mounted_paths:
            _log.error(
                "Prometheus bridge '%s' wants metrics_path '%s', which "
                "another Prometheus bridge on this gateway already "
                "mounted. Give each bridge a distinct metrics_path in "
                "config.cfg -- skipping this one.",
                name, mount_path,
            )
            continue

        try:
            attach_metrics_route(app, bridge, mount_path)
        except Exception as exc:
            _log.error("Failed to mount Prometheus bridge '%s' at '%s': %s", name, mount_path, exc)
            continue

        mounted_paths.add(mount_path)
        extra_port: int | None = cast("int | None", bridge.metrics_port)
        reachable_port: int = extra_port if extra_port is not None else _current_port

        if extra_port is None:
            _log.info(
                "Prometheus bridge '%s' mounted at %s on the web UI app "
                "(0.0.0.0:%s) -- no separate port required.",
                name, mount_path, reachable_port,
            )
        else:
            _log.info(
                "Prometheus bridge '%s' mounted at %s on the web UI app, "
                "also reachable (restricted to %s only) on 0.0.0.0:%s.",
                name, mount_path, mount_path, reachable_port,
            )


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app(
    config_path: Path,
    config_dir: Path,
    db_dir: Path,
    gateway_instance: object,
    gateway_manager: GatewayManagerLike,
    log_path: Path,
    project_root: Path,
    protocols_dir: Path,

    ) -> FastAPI:
    """
    Build and return the FastAPI application.

    config_path   — fully-resolved path to config.cfg
    project_root  — root of MultiProtocolGateway (contains protocols/, classes/)
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # ---- Startup ----
        _install_webserver_logging()
        _log.info("MPG WebServer starting up...")


        # init_db is fast (in-memory setup only) — safe to run inline.
        engine: Engine = init_db(db_dir / "mpg_staging.db")

        def _startup_io() -> Scanner:
            run_migrations(db_dir / "mpg_staging.db", _ALEMBIC_INI)
            Base.metadata.create_all(bind=engine)
            with session_scope() as db:
                ensure_app_state(db)
            s = Scanner(config_path, project_root)

            # If config.cfg differs from the most recent backup, someone edited
            # it manually (or it was rolled back externally). Treat cfg as truth
            # so value_staged syncs to value_disk and no stale staged edits remain.
            try:

                cfg_hash: str = _hashlib.md5(config_path.read_bytes()).hexdigest()  # noqa: S324
                with session_scope() as _db:
                    latest: ConfigBackup | None = _db.query(ConfigBackup).order_by(
                        ConfigBackup.created_at.desc()
                    ).first()
                    if latest:
                        latest_path = Path(latest.filepath)
                        if latest_path.exists():
                            backup_hash: str = _hashlib.md5(latest_path.read_bytes()).hexdigest()  # noqa: S324
                            if cfg_hash != backup_hash:
                                _log.info(
                                    "config.cfg differs from last backup — "
                                    "treating cfg as ground truth for this scan"
                                )
                                s.set_cfg_is_truth(True)
            except Exception as _exc:
                _log.debug("Startup cfg-truth check skipped: %s", _exc)

            s.run()
            return s

        try:
            loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
            with ThreadPoolExecutor(max_workers=1, thread_name_prefix="MPGWebInit") as executor:
                scanner: Scanner = await loop.run_in_executor(executor, _startup_io)

        except Exception as exc:
            msg: str = f"Startup I/O failed (server still starting): {exc}"
            _log.error(msg)
            scanner = Scanner(config_path, project_root)

        _log.info(f"Database {engine.url} initialized and migrations applied.")

        # State management
        app.state.config_dir      = config_dir or config_path.parent
        app.state.config_path     = config_path
        app.state.db_dir          = db_dir
        app.state.gateway         = gateway_instance
        app.state.gateway_manager = gateway_manager
        app.state.log_path        = log_path
        app.state.project_root    = project_root
        app.state.protocols_dir   = protocols_dir
        app.state.scanner         = scanner
        app.state.transports_dir  = project_root / "classes" / "transports"

        # Seed/update the setting_descriptions table on every startup
        with session_scope() as db:
            n, _ = seed_setting_descriptions(db, app.state.transports_dir)
            if n:
                _log.info("Setting descriptions: %d rows seeded/updated", n)

        # Mount any configured Prometheus bridge's /metrics onto this same
        # app/port. See _mount_prometheus_bridges() docstring for the one
        # caveat (a live config reload doesn't re-run this).
        _mount_prometheus_bridges(app, gateway_instance)

        # In-memory staging for the Timescale DB "Delete Columns" screen —
        # see services/bridge_service.py. Lives alongside the gateway
        # rather than in the staging DB since wide-table columns are live
        # Postgres schema, not config.cfg settings.
        app.state.timescale_pending_deletions = {}
        app.state.timescale_pending_lock = threading.RLock()

        def _on_config_changed() -> None:
            """FileWatcher callback: config.cfg was edited outside the
            webUI's own commit flow (e.g. hand-edited over SSH). Reload the
            live gateway from it, same as a commit would, and refresh
            app.state.gateway afterward since reload() may have fallen back
            to the last-known-good backup instead of the (broken) edit."""
            status: ReloadStatusLike = gateway_manager.reload(trigger="file_watch")
            app.state.gateway = gateway_manager.current
            if not status.ok:
                _log.error(f"Gateway reload (file_watch) did not fully succeed: {status.message}")

        watcher: FileWatcher = FileWatcher(
            scanner, config_path, protocols_dir,
            on_config_changed=_on_config_changed,
        )
        watcher.start()
        app.state.file_watcher = watcher

        _log.info(f"MPG WebServer ready on http://0.0.0.0:{_current_port}")

        yield  # -----------------------------------------------------------

        # ---- Shutdown ----
        _log.info("MPG WebServer shutting down...")
        if hasattr(app.state, "file_watcher"):
            app.state.file_watcher.stop()
        if _queue_listener is not None:
            _queue_listener.stop()

    app = FastAPI(
        title="MPG Web Management UI",
        description="Protocol Gateway Configuration & Monitoring",
        version="1.0.0",
        lifespan=lifespan,
    )

    # Restrict any Prometheus bridge's optional dedicated metrics_port (see
    # _collect_metrics_ports()) to serving only that bridge's metrics_path --
    # not the rest of this UI -- before this app receives its first ASGI
    # call. Must happen here, not inside lifespan startup: Starlette builds
    # (and freezes) its middleware stack on the very first __call__, which
    # happens before the lifespan "startup" event fires.
    for extra_port, allowed_paths in _collect_metrics_ports(gateway_instance).items():
        app.add_middleware(
            RestrictPortMiddleware,
            restricted_port=extra_port,
            allowed_prefixes=tuple(allowed_paths),
        )

    # Static files
    if _STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")

    # Templates
    templates: Jinja2Templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))
    app.state.templates = templates

    # Jinja global — lets base.html decide whether to show the "Timescale DB"
    # nav pad without every single page route having to thread the answer
    # through its own context dict. app.state.gateway is set once below
    # during lifespan startup; this closes over `app` (not `request`), so it
    # always reads the current live value at render time.
    templates.env.globals["timescale_bridge_available"] = (  # type: ignore[reportArgumentType]
        lambda: is_timescale_available(getattr(app.state, "gateway", None))
    )

    # Jinja global — base.html's reload-status banner. Returns the
    # GatewayManager's current ReloadStatus (or None before the gateway has
    # ever been (re)built), same closure-over-app pattern as
    # timescale_bridge_available above so it always reads live state rather
    # than whatever it was when the template was first rendered.
    templates.env.globals["gateway_reload_status"] = (  # type: ignore[reportArgumentType]
        lambda: getattr(getattr(app.state, "gateway_manager", None), "status", None)
    )
    # Routers
    # ------------------------------------------------------------------

    app.include_router(analysis_router)
    app.include_router(bridges_router)
    app.include_router(commit_router)
    app.include_router(devices_router)
    app.include_router(gateway_status_router)
    app.include_router(help_router)
    app.include_router(pages_router)
    app.include_router(protocols_router)
    app.include_router(timescale_router)
    app.include_router(transport_settings_router)

    # ------------------------------------------------------------------
    # Core routes
    # Everything else lives in the router modules (routers/pages.py,
    # routers/protocols.py, routers/devices.py, routers/commit.py, etc. —
    # see the include_router calls above). Only routes that don't fit any
    # domain router stay here.
    #
    # Each handler below carries a `# pyright: ignore[reportUnusedFunction]`:
    # pyright's unused-function check doesn't recognize the FastAPI
    # `@app.get(...)` decorator as "using" a function defined inside this
    # factory (it only suppresses the check for module-level definitions),
    # so every locally-scoped route handler is a false positive here.
    # ------------------------------------------------------------------

    BASE_WEB_DIR: Path = Path(__file__).resolve().parent

    @app.get('/favicon.ico', include_in_schema=False)
    async def favicon() -> FileResponse:

        favicon_path: Path = BASE_WEB_DIR / "static" / "favicon.ico"

        return FileResponse(favicon_path)

    @app.get("/api/scan", tags=["admin"])
    async def trigger_scan(request: Request) -> dict[str, JsonValue]:
        """Manually trigger a re-scan of config + protocols, then reload the
        live gateway from what that scan just wrote to the DB and disk.

        A re-scan alone only updates the staging DB (settings, protocol
        registers, orphans) — the running engine's live transports are
        built once at startup/last-reload and don't pick up a re-scan's
        changes on their own. Without this, "Re-scan Configuration" would
        make the UI show fresh state while the engine kept serving on
        stale transports/settings until something else (a commit, or the
        separate "Reload Engine" button) happened to reload it.
        """
        # from classes.WebServer.debug_defaults import check_stale_db_rows, run_debug
        # run_debug(project_root=request.app.state.project_root, config_path=request.app.state.config_path)
        # check_stale_db_rows(
        #     db_path=request.app.state.db_dir / "mpg_staging.db",
        #     config_path=request.app.state.config_path,
        #     transports_dir=request.app.state.transports_dir,
        # )

        try:
            stats: dict[str, int] = request.app.state.scanner.run()
        except Exception as exc:
            return {"status": "error", "detail": str(exc)}

        response: dict[str, JsonValue] = {"status": "ok", **stats}

        manager: GatewayManagerLike | None = getattr(request.app.state, "gateway_manager", None)
        if manager is not None:
            try:
                reload_status: ReloadStatusLike = manager.reload(trigger="manual")
                request.app.state.gateway = manager.current
                response["gateway_reload"] = {"ok": reload_status.ok, "message": reload_status.message}
                if not reload_status.ok:
                    _log.error(f"trigger_scan: gateway reload did not fully succeed: {reload_status.message}")
            except Exception as exc:
                _log.error(f"trigger_scan: gateway reload failed: {exc}")
                response["gateway_reload"] = {"ok": False, "message": str(exc)}

        return response

    return app


# ---------------------------------------------------------------------------
# Module-level port holder — lets the startup log message show the real port
# ---------------------------------------------------------------------------
_current_port: int = 1717


# ---------------------------------------------------------------------------
# Launcher — called from protocol_gateway.py
# ---------------------------------------------------------------------------

def start_webserver(
    config_file_path: Path,
    log_path: Path,
    gateway_instance: GatewayInstanceLike,
    gateway_manager: GatewayManagerLike,
    port: int = 1717,
) -> None:
    """
    Launch the FastAPI web server in a daemon thread.
    Returns immediately; the server runs in the background.

    config_file_path   — the fully-resolved Path to config.cfg as built by
                    protocol_gateway.main() (e.g. <root>/config/config.cfg)
    log_path     — the single, already-resolved absolute Path of the log
                    file, as set by Protocol_Gateway._setup_logging() and
                    exposed via the Protocol_Gateway instance's own .log_path.
    Usage in protocol_gateway.main():

        config_path: Path = root / "config" / config_file
        manager = GatewayManager(config_file, config_path)
        mpg = manager.start()
        start_webserver(config_path, mpg.log_path, gateway_instance=mpg, gateway_manager=manager)
        # run() lives on its own thread now (started inside manager.start());
        # this thread just needs to stay alive for the process to keep running.
    """
    global _current_port
    _current_port = port

    # config_file is already a fully-resolved absolute Path from the gateway.
    config_path: Path  = config_file_path.resolve()  # with file name.
    config_dir: Path   = config_path.parent          # e.g. <root>/config/
    project_root: Path = config_dir.parent           # e.g. <root>/
    db_dir: Path = config_dir / "data-db" if config_dir else project_root / "config" / "data-db"
    protocols_dir: Path = project_root / "protocols"
    if not protocols_dir.exists():
        _log.warning(f"Protocols directory missing at {protocols_dir}")

    _log.info(f"WebServer config_path  : {config_path}")
    _log.info(f"WebServer project_root : {project_root}")

    app: FastAPI = create_app(
        config_path=config_path,
        config_dir=config_dir,
        db_dir=db_dir,
        gateway_instance=gateway_instance,
        gateway_manager=gateway_manager,
        log_path=log_path,
        project_root=project_root,
        protocols_dir=protocols_dir,
    )

    uv_config = uvicorn.Config(
        app,
        host="0.0.0.0",  # noqa: S104
        port=port,
        log_level="info",
        access_log=False,
    )
    server: NoSignalServer = NoSignalServer(uv_config)

    # One Server, one event loop, one thread -- but possibly more than one
    # listening socket. Each configured Prometheus bridge's optional
    # metrics_port (see _collect_metrics_ports()) gets its own bound socket
    # here, restricted at the ASGI layer by RestrictPortMiddleware (added in
    # create_app(), above) to serve only that bridge's metrics_path. This is
    # NOT a second web server: uvicorn.Server.serve() natively accepts a
    # list of sockets and multiplexes them all through the same app/loop.
    sockets: list[socket.socket] = [uv_config.bind_socket()]
    for extra_port in _collect_metrics_ports(gateway_instance):
        try:
            sockets.append(uvicorn.Config(app, host="0.0.0.0", port=extra_port).bind_socket())  # noqa: S104
        except OSError as exc:
            _log.error(
                f"Could not bind Prometheus metrics_port {extra_port} ({exc}); "
                f"that bridge remains reachable on the main WebServer port "
                f"({port}) only."
            )

    def _run() -> None:

        try:
            asyncio.run(server.serve(sockets=sockets))
        except KeyboardInterrupt:
            # Graceful exit on Ctrl+C
            pass

    thread = threading.Thread(target=_run, name="MPGWebServer", daemon=True)
    thread.start()
    _log.info(f"MPG WebServer launched on http://0.0.0.0:{port}") # noqa: G004
    for sock in sockets[1:]:
        _log.info(f"MPG WebServer also listening on {sock.getsockname()} (Prometheus metrics_port)")  # noqa: G004

    # Store server reference on gateway so it can trigger graceful shutdown
    # via:  mpg.web_server.should_exit = True

    gateway_instance.web_server = server
