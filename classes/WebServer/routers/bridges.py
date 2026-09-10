# Description: routers/bridges.py — Read-only bridge status partials for the Bridges pulldown / device pages.
# File: bridges.py
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
routers/bridges.py — Read-only bridge status partials, plus Prometheus's
app-assembly wiring.
Registered in main.py via app.include_router(bridges_router).

These back the passive "Bridges" pulldown / device-page panels (see
partials/bridge_panes.html) — Bridge Health, Storage Overview, Indexes,
Compression & Retention Status, Background Job Status for TimescaleDB;
Bridge Health/Storage for InfluxDB v1/v3; Bridge Health for MQTT; Bridge
Health/Target Health for Prometheus. Every ROUTE in this module is purely
observational: nothing on these panels is clickable, and nothing here
mutates bridge state.

The one exception to "purely observational" is a pair of plain functions
at the bottom of this file -- mount_prometheus_bridges() and
collect_prometheus_metrics_ports() -- which aren't routes at all. They're
called directly by classes.WebServer.main during app assembly and server
startup, not by an HTTP request, and the former does mutate the FastAPI
app object (app.mount()). They live here rather than in
services/bridge_service.py because that module's contract is strictly
"take a gateway, return read-only data for a template" -- no function
there touches a FastAPI `app` object or runs outside a request. Prometheus
is the only bridge type that needs this kind of app-assembly wiring at
all (it's pull-based: it has to actually serve HTTP itself, not just
report on a connection it owns), so it stays paired with this module's
other Prometheus-specific code rather than being split across two files.

This is deliberately separate from routers/timescale.py, which owns the
"Timescale DB" admin menu (Delete Columns / Rebuild Rollups / Rebuild
Compression) — that menu's own read-only inventory partials (wide-table
picker, rollup-view list, compression-group list) stay there because
they exist to feed those maintenance screens, not the passive bridge
pane. See routers/timescale.py's module docstring for the other half of
this split.

TemplateResponse convention used throughout this module:
    request.app.state.templates.TemplateResponse(
        request=request,
        name="template/path.html",
        context={"key": value, ...},
    )
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse

from ..services.bridge_service import (
    get_background_jobs,
    get_compression_retention_summary,
    get_index_overview,
    get_influxdb_health,
    get_influxdb_storage,
    get_mqtt_health,
    get_prometheus_health,
    get_prometheus_targets,
    get_storage_overview,
    get_timescale_health,
    is_timescale_available,
)

if TYPE_CHECKING:
    # Deferred at runtime — importing protocol_gateway at module load time
    # risks a circular import, since it's what wires up the WebServer app
    # in the first place (see the same pattern in commit.py/devices.py/
    # timescale.py/pages.py). Only needed here, under TYPE_CHECKING, for
    # the annotations below.
    # Soft dependency only, for the type annotation on
    # mount_prometheus_bridges() below -- never evaluated at runtime under
    # `from __future__ import annotations`. main.py imports fastapi
    # directly for its own app; this module doesn't need it at runtime.
    from fastapi import FastAPI

    from protocol_gateway import Protocol_Gateway

router = APIRouter(tags=["bridges"])
_log: logging.Logger = logging.getLogger(__name__)

# A number of endpoints below keep dict[str, Any]/list[dict[str, Any]]
# return-shaped locals rather than a tightened union. In every one of
# those cases the dict is a near-verbatim pass-through of a
# services/bridge_service.py introspection call (get_timescale_health,
# get_storage_overview, get_background_jobs, get_index_overview,
# get_compression_retention_summary, get_influxdb_health,
# get_influxdb_storage, get_mqtt_health, get_prometheus_health,
# get_prometheus_targets) — a service module, out of this pass's "router
# modules" scope, reading genuinely heterogeneous live-bridge data with
# no fixed shape this file can assert without guessing at internals it
# doesn't own (see the identical note in timescale.py).


# ---------------------------------------------------------------------------
# TimescaleDB bridge panels — device page (see partials/bridge_panes.html)
# ---------------------------------------------------------------------------

@router.get("/pages/timescale/health", response_class=HTMLResponse, response_model=None)
async def timescale_health_partial(request: Request):
    """
    Bridge Health panel for the TimescaleDB bridge's device page — connection
    state, backlog buffering, and rollup setup completion. Read-only; lazy-
    loaded so a slow query here can't block the rest of the page.
    """
    gateway: "Protocol_Gateway | None" = getattr(request.app.state, "gateway", None)
    if not is_timescale_available(gateway):
        _log.warning("No TimescaleDB bridge is attached to this gateway; returning 404 for /pages/timescale/health")
        raise HTTPException(status_code=404, detail="No TimescaleDB bridge is attached to this gateway.")

    try:
        health: dict[str, Any] = get_timescale_health(gateway)
    except RuntimeError:
        health = {}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return request.app.state.templates.TemplateResponse(
        request=request,
        name="partials/bridge_timescale_health_panel.html",
        context={"health": health},
    )


@router.get("/pages/timescale/storage", response_class=HTMLResponse, response_model=None)
async def timescale_storage_partial(request: Request):
    """
    Storage Overview panel for the TimescaleDB bridge's device page — row
    count, size, chunk count, and time range per source table. Read-only;
    lazy-loaded since this queries every source table individually.
    """
    gateway: "Protocol_Gateway | None" = getattr(request.app.state, "gateway", None)
    if not is_timescale_available(gateway):
        _log.warning("No TimescaleDB bridge is attached to this gateway; returning 404 for /pages/timescale/storage")
        raise HTTPException(status_code=404, detail="No TimescaleDB bridge is attached to this gateway.")

    try:
        tables: list[dict[str, Any]] = get_storage_overview(gateway)
    except RuntimeError:
        tables = []
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return request.app.state.templates.TemplateResponse(
        request=request,
        name="partials/bridge_timescale_storage_panel.html",
        context={"tables": tables},
    )


@router.get("/pages/timescale/indexes", response_class=HTMLResponse, response_model=None)
async def timescale_indexes_partial(request: Request):
    """
    Indexes panel for the TimescaleDB bridge's device page — every index
    on the shared narrow table and each wide table, with size and scan
    counts. Read-only; lazy-loaded since this queries every source table
    individually, same as the Storage Overview panel.
    """
    gateway: "Protocol_Gateway | None" = getattr(request.app.state, "gateway", None)
    if not is_timescale_available(gateway):
        _log.warning("No TimescaleDB bridge is attached to this gateway; returning 404 for /pages/timescale/indexes")
        raise HTTPException(status_code=404, detail="No TimescaleDB bridge is attached to this gateway.")

    try:
        indexes: list[dict[str, Any]] = get_index_overview(gateway)
    except RuntimeError:
        indexes = []
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return request.app.state.templates.TemplateResponse(
        request=request,
        name="partials/bridge_timescale_indexes_panel.html",
        context={"indexes": indexes},
    )


@router.get("/pages/timescale/compression-retention", response_class=HTMLResponse, response_model=None)
async def timescale_compression_retention_partial(request: Request):
    """
    Compression & Retention Status panel for the TimescaleDB bridge's
    device page — the configured compression schedule and raw-data
    retention interval. Read-only; this is config, not a live query.
    """
    gateway: "Protocol_Gateway | None" = getattr(request.app.state, "gateway", None)
    if not is_timescale_available(gateway):
        _log.warning("No TimescaleDB bridge is attached to this gateway; returning 404 for /pages/timescale/compression-retention")
        raise HTTPException(status_code=404, detail="No TimescaleDB bridge is attached to this gateway.")

    try:
        summary: dict[str, Any] | None = get_compression_retention_summary(gateway)
    except RuntimeError:
        summary = None
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return request.app.state.templates.TemplateResponse(
        request=request,
        name="partials/bridge_timescale_compression_panel.html",
        context={"summary": summary},
    )


@router.get("/pages/timescale/jobs", response_class=HTMLResponse, response_model=None)
async def timescale_jobs_partial(request: Request):
    """
    Background Job Status panel for the TimescaleDB bridge's device page —
    TimescaleDB's own compression/retention/refresh scheduler jobs for
    every hypertable and rollup view this bridge manages. Read-only.
    """
    gateway: "Protocol_Gateway | None" = getattr(request.app.state, "gateway", None)
    if not is_timescale_available(gateway):
        _log.warning("No TimescaleDB bridge is attached to this gateway; returning 404 for /pages/timescale/jobs")
        raise HTTPException(status_code=404, detail="No TimescaleDB bridge is attached to this gateway.")

    try:
        jobs: list[dict[str, Any]] = get_background_jobs(gateway)
    except RuntimeError:
        jobs = []
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return request.app.state.templates.TemplateResponse(
        request=request,
        name="partials/bridge_timescale_jobs_panel.html",
        context={"jobs": jobs},
    )



# ---------------------------------------------------------------------------
# InfluxDB v1/v3 bridge panels — device page. Unlike TimescaleDB (a
# singleton bridge), a gateway can have more than one InfluxDB bridge
# configured, so these are scoped by device_name.
# ---------------------------------------------------------------------------

@router.get("/pages/influxdb/{device_name}/health", response_class=HTMLResponse, response_model=None)
async def influxdb_health_partial(device_name: str, request: Request):
    """
    Bridge Health panel for an InfluxDB v1 (influxdb_out) or v3
    (influxdb3_out) device page — connection/backlog/staleness state.
    Read-only; lazy-loaded like the TimescaleDB panels.

    Unlike the TimescaleDB bridge (a singleton), a gateway can have more
    than one InfluxDB v1/v3 bridge configured, so this is scoped by
    device_name rather than assuming "the" InfluxDB bridge — see
    services/bridge_service.get_influxdb_bridge.
    """
    gateway: "Protocol_Gateway | None" = getattr(request.app.state, "gateway", None)
    device_section: str = f"transport.{device_name}"

    try:
        health: dict[str, Any] = get_influxdb_health(gateway, device_section)
    except RuntimeError:
        raise HTTPException(status_code=404, detail=f"No InfluxDB bridge named '{device_name}' is attached to this gateway.")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return request.app.state.templates.TemplateResponse(
        request=request,
        name="partials/bridge_influxdb_health_panel.html",
        context={"health": health},
    )


@router.get("/pages/influxdb/{device_name}/storage", response_class=HTMLResponse, response_model=None)
async def influxdb_storage_partial(device_name: str, request: Request):
    """
    Storage Overview panel for an InfluxDB v1 (influxdb_out) or v3
    (influxdb3_out) device page — discovered measurements/tables, a
    sample row-count estimate, and (v1 only) retention policies and
    optional on-disk data directory size. Read-only, best-effort; a
    failed underlying query is reported inline rather than erroring the
    whole panel — see services/bridge_service.get_influxdb_storage.
    """
    gateway: "Protocol_Gateway | None" = getattr(request.app.state, "gateway", None)
    device_section: str = f"transport.{device_name}"

    try:
        storage: dict[str, Any] = get_influxdb_storage(gateway, device_section)
    except RuntimeError:
        raise HTTPException(status_code=404, detail=f"No InfluxDB bridge named '{device_name}' is attached to this gateway.")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return request.app.state.templates.TemplateResponse(
        request=request,
        name="partials/bridge_influxdb_storage_panel.html",
        context={"storage": storage},
    )



# ---------------------------------------------------------------------------
# MQTT bridge panel — device page. Scoped by device_name for the same
# reason as InfluxDB above.
# ---------------------------------------------------------------------------

@router.get("/pages/mqtt/{device_name}/health", response_class=HTMLResponse, response_model=None)
async def mqtt_health_partial(device_name: str, request: Request):
    """
    Bridge Health panel for an MQTT device page — connection/reconnect/
    write-topic state. Read-only; lazy-loaded like the other bridge panels.

    Scoped by device_name rather than assuming "the" MQTT bridge, since a
    gateway can have more than one configured — see services/bridge_service
    .get_mqtt_bridge.
    """
    gateway: "Protocol_Gateway | None" = getattr(request.app.state, "gateway", None)
    device_section: str = f"transport.{device_name}"

    try:
        health: dict[str, Any] = get_mqtt_health(gateway, device_section)
    except RuntimeError:
        raise HTTPException(status_code=404, detail=f"No MQTT bridge named '{device_name}' is attached to this gateway.")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return request.app.state.templates.TemplateResponse(
        request=request,
        name="partials/bridge_mqtt_health_panel.html",
        context={"health": health},
    )



# ---------------------------------------------------------------------------
# Prometheus bridge panels — device page. Scoped by device_name for the
# same reason as InfluxDB above.
# ---------------------------------------------------------------------------

@router.get("/pages/prometheus/{device_name}/health", response_class=HTMLResponse, response_model=None)
async def prometheus_health_partial(device_name: str, request: Request):
    """
    Bridge Health panel for a Prometheus device page — in-memory metrics
    registry summary (metrics registered, mount path, machine counts by
    connectivity bucket) plus uptime. Read-only; lazy-loaded like
    the other bridge panels.

    Scoped by device_name rather than assuming "the" Prometheus bridge,
    since a gateway can have more than one configured (e.g. separate
    /metrics paths mounted on the same WebServer app) — see
    services/bridge_service.get_prometheus_bridge.
    """
    gateway: "Protocol_Gateway | None" = getattr(request.app.state, "gateway", None)
    device_section: str = f"transport.{device_name}"

    try:
        health: dict[str, Any] = get_prometheus_health(gateway, device_section)
    except RuntimeError:
        raise HTTPException(status_code=404, detail=f"No Prometheus bridge named '{device_name}' is attached to this gateway.")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return request.app.state.templates.TemplateResponse(
        request=request,
        name="partials/bridge_prometheus_health_panel.html",
        context={"health": health},
    )


@router.get("/pages/prometheus/{device_name}/targets", response_class=HTMLResponse, response_model=None)
async def prometheus_targets_partial(device_name: str, request: Request):
    """
    Target Health panel for a Prometheus device page — one row per
    upstream machine this bridge has ever been wired to or received data
    from: connectivity status, configured scrape interval, accumulated
    scrape_failures_total, and time since last_scrape_timestamp_seconds.
    Read-only.
    """
    gateway: "Protocol_Gateway | None" = getattr(request.app.state, "gateway", None)
    device_section: str = f"transport.{device_name}"

    try:
        targets: list[dict[str, Any]] = get_prometheus_targets(gateway, device_section)
    except RuntimeError:
        raise HTTPException(status_code=404, detail=f"No Prometheus bridge named '{device_name}' is attached to this gateway.")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return request.app.state.templates.TemplateResponse(
        request=request,
        name="partials/bridge_prometheus_targets_panel.html",
        context={"targets": targets},
    )


# ---------------------------------------------------------------------------
# Prometheus bridge app-assembly wiring
#
# Unlike every other bridge type in this module (TimescaleDB, InfluxDB,
# MQTT), Prometheus is pull-based: it needs to actually serve HTTP itself,
# not just report on a connection it owns. classes.WebServer.main calls
# the two functions below during app assembly and server startup; they're
# kept here (not in main.py) so nothing Prometheus-specific has to live in
# that module at all -- see classes.transports.prometheus_out for the
# actual bridge-lookup/mounting logic these two functions just call
# through to.
#
# Both use a deferred, function-local import of
# classes.transports.prometheus_out, same reasoning as
# get_prometheus_health()/get_prometheus_targets() above (via
# services.bridge_service): prometheus_client is only required if a
# prometheus_out bridge is actually configured, and this router module is
# imported unconditionally by main.py at startup (see
# `from .routers.bridges import router as bridges_router`), so it must
# stay importable even when that optional dependency is missing.
# ---------------------------------------------------------------------------

def collect_prometheus_metrics_ports(gateway_instance: object | None) -> dict[int, list[str]]:
    """
    ``{port: [metrics_path, ...]}`` for every configured prometheus_out
    bridge's dedicated `metrics_port` -- every bridge has one (default
    9110; see classes.transports.prometheus_out.DEFAULT_METRICS_PORT),
    there is no "share the main WebServer port" mode. Called from
    classes.WebServer.main.start_webserver(), which binds one extra
    listening socket per port returned here.

    Returns `{}` if no prometheus_out bridge is configured, or if
    prometheus_client isn't installed -- in the latter case there is
    nothing to report, since MPG can't build a prometheus_out transport at
    all without that dependency (protocol_gateway.py's transport loader
    would already have failed before this function ever runs).
    """
    try:
        from classes.transports.prometheus_out import collect_metrics_ports
    except ImportError:
        return {}
    return collect_metrics_ports(gateway_instance)


def mount_prometheus_bridges(app: "FastAPI", gateway_instance: object | None, webserver_port: int) -> None:
    """
    Mount every configured prometheus_out bridge's /metrics endpoint onto
    `app` (the WebServer's own FastAPI app -- the process that's always
    running, since MPG has no headless mode). There is no other way to
    serve a prometheus_out bridge; it has no standalone-server mode of
    its own. Called from classes.WebServer.main.create_app()'s lifespan
    startup.

    `webserver_port` is passed straight through to
    classes.transports.prometheus_out.mount_bridges() -- see its
    docstring for what it's used for.

    Deferred import, mirroring collect_prometheus_metrics_ports() above:
    if prometheus_client isn't installed, there cannot be a live
    prometheus_out bridge to mount either (see that function's
    docstring), so this quietly does nothing rather than erroring.
    """
    try:
        from classes.transports.prometheus_out import mount_bridges
    except ImportError:
        # No prometheus_out bridge can be live on gateway_instance without
        # prometheus_client already having imported successfully once
        # (protocol_gateway.py's transport loader would have raised first)
        # -- so reaching here just means none is configured.
        return
    mount_bridges(app, gateway_instance, webserver_port)
