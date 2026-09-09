# Description: routers/pages.py — Page routes and supporting API endpoints. Registered in main.py via app.include_router(pages_router).
# File: pages.py
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
routers/pages.py — Page routes and supporting API endpoints.
Registered in main.py via app.include_router(pages_router).

TemplateResponse convention used throughout this module:
    request.app.state.templates.TemplateResponse(
        request=request,
        name="template/path.html",
        context={"key": value, ...},
    )

Session scope convention:
    Data is fetched inside `with session_scope() as db:` and the session
    is closed before TemplateResponse is constructed — ORM objects must
    be fully loaded (no lazy-load) before the session closes.
"""
from __future__ import annotations

import csv
import json
import logging
import os
import re
from pathlib import Path
from typing import TYPE_CHECKING, List, Sequence, Tuple

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import (
    HTMLResponse,
    PlainTextResponse,
    RedirectResponse,
    StreamingResponse,
)
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import func, select
from sqlalchemy.engine.row import Row
from sqlalchemy.orm import Session

from classes.messaging.message_handler import is_active
from classes.messaging.message_handler import send_message as _send_message
from classes.WebServer.services.device_service import TransportLibraryRow

from ...transports.modbus_base import modbus_base
from ...transports.transport_base import transport_base
from ..config_writer import create_backup
from ..database import get_session, refresh_app_state, session_scope
from ..models import AppState, ProtocolRegister, Setting, SettingDescription
from ..scanner import (
    EMPTY_TRANSPORT_ENTRY,
    TRANSPORT_BASE_KEYS,
    TransportLibraryEntry,
    load_config,
    scan_transport_library,
)
from ..services.analysis_service import get_transport_connection_status
from ..services.device_service import (
    DeviceSummary,
    NavData,
    get_app_state,
    get_device_settings,
    get_device_summary,
    get_nav_data,
    get_transport_library,
)
from ..services.protocol_service import (
    JSONValue,
    export_protocol_registers,
    get_device_metric_summary,
    get_protocol_groups,
    get_protocol_json,
    get_protocols_for_device,
)
from ..services.setting_description_service import get_all_setting_descriptions

if TYPE_CHECKING:
    # Deferred at runtime — importing protocol_gateway at module load time
    # risks a circular import, since it's what wires up the WebServer app
    # in the first place (see the same pattern in commit.py/devices.py).
    # Only needed here, under TYPE_CHECKING, for the annotations below.
    from protocol_gateway import Protocol_Gateway

router = APIRouter(tags=["pages"])
_log: logging.Logger = logging.getLogger(__name__)


def base_context(request: Request, nav: NavData) -> dict[str, NavData | list[dict[str, str | list[str]]]]:
    return {
        "nav": nav,
        "proto_groups": get_protocol_groups(request.app.state.protocols_dir),
    }


def _analysis_protocol_options(protocol_groups: list[dict[str, str | list[str]]]) -> list[dict[str, str]]:
    options: list[dict[str, str]] = []
    for group in protocol_groups:
        group_name = str(group.get("group", ""))
        if group_name.startswith("_"):
            continue
        for protocol_name in group.get("protocols", []):
            protocol_name = str(protocol_name)
            lower_name: str = protocol_name.lower()
            if protocol_name.startswith("_"):
                continue
            if "registry" in lower_name or "debug" in lower_name:
                continue
            options.append({
                "group": group_name,
                "name": protocol_name,
            })
    return options


# ---------------------------------------------------------------------------
# Dashboard & device pages
# ---------------------------------------------------------------------------

@router.get("/", response_class=HTMLResponse, response_model=None)
async def dashboard(request: Request):
    with session_scope() as db:
        nav: NavData = get_nav_data(db)
        state: AppState = get_app_state(db)

    proto_groups: List[dict[str, str | list[str]]] = get_protocol_groups(request.app.state.protocols_dir)

    return request.app.state.templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "nav":          nav,
            "app_state":    state,
            "proto_groups": proto_groups,
        },
    )


@router.get("/device/{device_name}", response_class=HTMLResponse, response_model=None)
async def device_page(request: Request, device_name: str):
    section: str = f"transport.{device_name}"

    # Resolved up front (doesn't need a db session) so both the connection
    # status below and the metric summary computed inside the session block
    # can use the same live transport instance.
    gateway: "Protocol_Gateway | None" = getattr(request.app.state, "gateway", None)
    live_transport: transport_base | None = None
    if gateway is not None:
        live_transport = next(
            (
                t for t in getattr(gateway, "_Protocol_Gateway__transports", [])
                if t.transport_name in (f"transport.{device_name}", device_name)
            ),
            None,
        )

    with session_scope() as db:
        nav:      NavData                  = get_nav_data(db)
        summary:  DeviceSummary | None     = get_device_summary(db, device_name)
        all_settings: List[Setting]        = get_device_settings(db, section)

        # For bridge devices, filter displayed settings to only the keys
        # the bridge module actually reads (AST-scanned keys only).
        # This prevents scraper base keys (protocol_version, read_interval,
        # variable_mask, etc.) from appearing in the bridge settings pane.
        if summary and summary.transport_type == "bridge":
            library: dict[str, TransportLibraryEntry] = scan_transport_library(request.app.state.transports_dir)

            bridge_info: TransportLibraryEntry = library.get(summary.transport_class, EMPTY_TRANSPORT_ENTRY)

            # bridge_info is now a real TransportLibraryEntry (not a loose
            # dict), so "keys" resolves to its declared dict[str, str | None]
            # on its own — no | Any escape hatch needed here anymore.
            raw_keys: dict[str, str | None] = bridge_info.get("keys", {})
            bridge_keys: set[str] = set(raw_keys.keys()) if raw_keys else set()

            # Always keep log_level as it's shown in a dedicated dropdown
            bridge_keys.add("log_level")

            settings: List[Setting] = [
                s for s in all_settings if s.key in bridge_keys
            ] if bridge_keys else all_settings
        else:
            settings = all_settings
        proto_tabs: List[dict[str, str | int]]   = (
            get_protocols_for_device(db, summary.protocol_version, device_name=device_name)
            if summary and summary.protocol_version
            else []
        )
        # Pre-compute whether any M/S/W selection exists across all tabs so
        # protocol_section.html can show "No chosen metrics" without Jinja sum.
        has_no_selections: bool = not any(
            t.get("mask_count", 0) or t.get("screen_count", 0) or t.get("write_count", 0)
            for t in proto_tabs
        ) if proto_tabs else False
        metric_summary: dict[str, int | bool | dict[str, dict[str, int]]] | None = (
            get_device_metric_summary(db, summary.protocol_version, device_name, transport=live_transport)
            if summary and summary.protocol_version and summary.transport_type == "scraper"
            else None
        )
        protocol_match = None
        if summary is None:
            protocol_match: Row[Tuple[str, str]] | None = (
                db.query(ProtocolRegister.protocol_group, ProtocolRegister.protocol_name)
                .filter(ProtocolRegister.protocol_name == device_name)
                .first()
            )

    if summary is None:
        if protocol_match:
            return RedirectResponse(
                url=f"/protocol-editor/{protocol_match[0]}/{protocol_match[1]}",
                status_code=307,
            )
        return HTMLResponse("<p>Device not found.</p>", status_code=404)

    proto_groups: List[dict[str, str | list[str]]] = get_protocol_groups(request.app.state.protocols_dir)

    partial_template_name: str = (
        "partials/scraper_panes.html"
        if summary.transport_type == "scraper"
        else "partials/bridge_panes.html"
    )

    template_name = (
        partial_template_name
        if request.headers.get("HX-Request")
        else "device.html"
    )

    # Populate live connection status from the gateway instance
    # (gateway / live_transport were already resolved above the session
    # block so get_device_metric_summary could use the same transport.)
    analyze_enabled = False
    if gateway is not None:
        conn_status: dict[str, bool] = get_transport_connection_status(gateway)
        # Gateway uses section name (e.g. "transport.mqtt") as transport_name
        summary.is_connected = conn_status.get(
            summary.section,                          # try "transport.mqtt"
            conn_status.get(summary.name, False)      # fall back to "mqtt"
        )
        analyze_enabled = bool(
            summary.transport_type == "scraper"
            and isinstance(live_transport, modbus_base)
        )

    return request.app.state.templates.TemplateResponse(
        request=request,
        name=template_name,
        context={
            "nav":          nav,
            "device":       summary,
            "settings":     settings,
            "proto_tabs":   proto_tabs,
            "has_no_selections": has_no_selections,
            "metric_summary": metric_summary,
            "proto_groups": proto_groups,
            "transport_library": get_transport_library(request.app.state.transports_dir),
            "device_partial_template": partial_template_name,
            "analyze_enabled": analyze_enabled,
        },
    )


# ---------------------------------------------------------------------------
# Global Settings pages
# ---------------------------------------------------------------------------

@router.get("/pages/global-settings", response_class=HTMLResponse, response_model=None)
async def global_settings_page(request: Request):
    with session_scope() as db:
        nav: NavData = get_nav_data(db)
        settings: List[Setting] = (
            db.query(Setting)
            .filter(
                Setting.section == "general",
                Setting.key != "log_level",
            )
            .order_by(Setting.key)
            .all()
        )
    return request.app.state.templates.TemplateResponse(
        request=request,
        name="pages/global_settings.html",
        context={**base_context(request, nav), "settings": settings},
    )


@router.get("/pages/logging-settings", response_class=HTMLResponse, response_model=None)
async def logging_settings_page(request: Request):
    with session_scope() as db:
        nav: NavData = get_nav_data(db)
        settings: List[Setting] = (
            db.query(Setting)
            .filter_by(section="logging")
            .order_by(Setting.key)
            .all()
        )
    return request.app.state.templates.TemplateResponse(
        request=request,
        name="pages/logging_settings.html",
        context={**base_context(request, nav), "settings": settings},
    )

@router.get("/pages/messaging-settings", response_class=HTMLResponse, response_model=None)
async def messaging_settings_page(request: Request):
    """Render the [messages] config section as an editable settings page."""
    with session_scope() as db:
        nav: NavData = get_nav_data(db)
        settings: List[Setting] = (
            db.query(Setting)
            .filter_by(section="messages")
            .order_by(Setting.key)
            .all()
        )
    return request.app.state.templates.TemplateResponse(
        request=request,
        name="pages/messaging_settings.html",
        context={**base_context(request, nav), "settings": settings},
    )


# ---------------------------------------------------------------------------
# Messaging test endpoint
# ---------------------------------------------------------------------------

@router.post("/api/devices/general/messages/test")
async def messaging_test(request: Request) -> dict[str, str]:
    """
    Send a test notification through every active messaging service.
    Returns 200 on success, 500 if no handler is initialized or all
    services fail.
    """

    if not is_active():
        raise HTTPException(
            status_code=500,
            detail="Messaging subsystem is not initialized. "
                   "Check that [messages] enabled = true in config.cfg and restart.",
        )
    _send_message(
        message="This is a test notification from MPG Admin.",
        title="MPG Test",
        priority=0,
    )
    return {"status": "sent"}

@router.get("/pages/view-log", response_class=HTMLResponse, response_model=None)
async def view_log_page(request: Request):
    with session_scope() as db:
        nav: NavData = get_nav_data(db)
    return request.app.state.templates.TemplateResponse(
        request=request,
        name="pages/view_log.html",
        context=base_context(request, nav),
    )


@router.get("/pages/transport-library", response_class=HTMLResponse, response_model=None)
async def transport_library_page(request: Request):

    with session_scope() as db:
        nav: NavData = get_nav_data(db)
    library: List[TransportLibraryRow]  = get_transport_library(
        request.app.state.transports_dir
    )
    return request.app.state.templates.TemplateResponse(
        request=request,
        name="pages/transport_library.html",
        context={**base_context(request, nav), "library": library},
    )


@router.get("/pages/transport-settings", response_class=HTMLResponse, response_model=None)
async def transport_settings_page(request: Request):
    with session_scope() as db:
        nav: NavData = get_nav_data(db)
        settings: List[SettingDescription] = get_all_setting_descriptions(db)
        # Convert to plain dicts for template (avoids lazy-load issues outside session)
        settings_data: List[dict[str, int | str | bool]] = [
            {
                "id": s.id,
                "key": s.key,
                "transports": s.transports or "",
                "description": s.description or "",
                "is_dirty": s.is_dirty,
            }
            for s in settings
        ]
    return request.app.state.templates.TemplateResponse(
        request=request,
        name="pages/transport_settings.html",
        context={**base_context(request, nav), "settings": settings_data},
    )


@router.get("/pages/faq", response_class=HTMLResponse, response_model=None)
async def faq_page(request: Request):
    with session_scope() as db:
        nav: NavData = get_nav_data(db)
    return request.app.state.templates.TemplateResponse(
        request=request,
        name="pages/faq.html",
        context=base_context(request, nav),
    )


@router.get("/pages/about", response_class=HTMLResponse, response_model=None)
async def about_page(request: Request):
    with session_scope() as db:
        nav: NavData = get_nav_data(db)
    return request.app.state.templates.TemplateResponse(
        request=request,
        name="pages/about.html",
        context=base_context(request, nav),
    )


@router.get("/pages/create-device", response_class=HTMLResponse, response_model=None)
async def create_device_page(request: Request):
    with session_scope() as db:
        nav: NavData = get_nav_data(db)
    proto_groups: List[dict[str, str | list[str]]] = get_protocol_groups(
        request.app.state.protocols_dir
    )
    transport_library: dict[str, TransportLibraryEntry] = scan_transport_library(request.app.state.transports_dir)
    create_device_data: dict[
        str,
        list[dict[str, str | dict[str, str | None]]] | list[dict[str, str]] | list[str]
    ] = {
        "scrapers": [
            {
                "name": name,
                "keys": info.get("keys", {}),
            }
            for name, info in sorted(transport_library.items())
            if info.get("classification") == "scraper"
        ],
        "bridges": [
            {
                "name": name,
                "section": f"transport.{name}",
            }
            for name, info in sorted(transport_library.items())
            if info.get("classification") == "bridge"
        ],
        "shared_keys": sorted(
            key for key in TRANSPORT_BASE_KEYS.keys()
            if key not in {"transport", "bridge", "protocol_version", "log_level"}
        ),
    }
    return request.app.state.templates.TemplateResponse(
        request=request,
        name="pages/create_device.html",
        context={
            "nav": nav,
            "proto_groups": proto_groups,
            "create_device_data": create_device_data,
        },
    )



def _protocol_create_groups(protocols_dir: Path) -> list[dict[str, str | list[str]]]:
    groups: list[dict[str, str | list[str]]] = []
    if not protocols_dir.exists():
        return groups

    for group_dir in sorted(protocols_dir.iterdir()):
        if not group_dir.is_dir() or group_dir.name.startswith("_"):
            continue
        protocol_names: set[str] = set()
        for item in group_dir.iterdir():
            if item.suffix.lower() not in (".csv", ".json"):
                continue
            name: str = item.stem
            if name.startswith("_") or name.endswith(".override"):
                continue
            protocol_names.add(
                re.sub(r"\.(coil|discrete|input|holding)_registry_map$", "", name)
            )
            protocol_names.add(re.sub(r"\.registry_map$", "", name))
        groups.append({"manufacturer": group_dir.name, "protocols": sorted(protocol_names)})
    return groups


@router.get("/pages/create-protocol", response_class=HTMLResponse, response_model=None)
async def create_protocol_page(request: Request):
    with session_scope() as db:
        nav: NavData = get_nav_data(db)

    protocol_create_data: dict[
        str,
        list[dict[str, str | list[str]]] | list[dict[str, str]] | tuple[str, ...]
    ] = {
        "manufacturers": _protocol_create_groups(request.app.state.protocols_dir),
        "protocol_types": [
            {"label": "Coil", "value": "coil"},
            {"label": "Discrete", "value": "discrete"},
            {"label": "Input", "value": "input"},
            {"label": "Holding", "value": "holding"},
            {"label": "Other", "value": "other"},
        ],
        "csv_headers": CREATE_PROTOCOL_CSV_HEADERS,
    }

    return request.app.state.templates.TemplateResponse(
        request=request,
        name="pages/create_protocol.html",
        context={
            "nav": nav,
            "proto_groups": get_protocol_groups(request.app.state.protocols_dir),
            "protocol_create_data": protocol_create_data,
        },
    )


@router.get("/pages/analyze/{device_name}", response_class=HTMLResponse, response_model=None)
async def analyze_device_page(request: Request, device_name: str):
    gateway: "Protocol_Gateway | None" = getattr(request.app.state, "gateway", None)
    transport: transport_base | None = None
    if gateway is not None:
        transports: list[transport_base] = getattr(gateway, "_Protocol_Gateway__transports", [])
        transport = next(
            (
                t for t in transports
                if t.transport_name in (device_name, f"transport.{device_name}")
            ),
            None,
        )

    with session_scope() as db:
        nav: NavData = get_nav_data(db)
        device: DeviceSummary | None = get_device_summary(db, device_name)

    if device is None:
        raise HTTPException(status_code=404, detail=f"Device '{device_name}' not found")
    if device.transport_type != "scraper":
        raise HTTPException(status_code=400, detail="Analyze is only available for scrapers")
    if transport is None or not isinstance(transport, modbus_base):
        raise HTTPException(
            status_code=400,
            detail="Analyze is only available for Modbus-based scrapers",
        )

    proto_groups: list[dict[str, str | list[str]]] = get_protocol_groups(request.app.state.protocols_dir)
    protocol_options: List[dict[str, str]] = _analysis_protocol_options(proto_groups)
    current_protocol: str = device.protocol_version or ""

    return request.app.state.templates.TemplateResponse(
        request=request,
        name="pages/analyze_device.html",
        context={
            "nav": nav,
            "proto_groups": proto_groups,
            "device": device,
            "analysis_protocols": protocol_options,
            "current_protocol": current_protocol,
        },
    )


# ---------------------------------------------------------------------------
# Protocol editor
# ---------------------------------------------------------------------------

@router.get("/protocol-editor/{protocol_group}/{protocol_name}",
            response_class=HTMLResponse, response_model=None)
async def protocol_editor(
    request: Request,
    protocol_group: str,
    protocol_name: str,
):
    """
    Standalone protocol viewer/editor reached from the Protocols nav menu.
    Shows the register table for a CSV/JSON file independent of any device.

    Responds with a partial (HTMX swap) or a full page depending on whether
    the HX-Request header is present.
    """
    protocols_dir: Path = request.app.state.protocols_dir

    with session_scope() as db:
        nav: NavData = get_nav_data(db)

        rows: Sequence[Row[Tuple[str, str, int]]] = (
            db.execute(
                select(
                    ProtocolRegister.protocol_name,
                    ProtocolRegister.registry_type,
                    func.count(ProtocolRegister.id).label("total"),
                )
                .where(ProtocolRegister.protocol_name == protocol_name)
                .group_by(
                    ProtocolRegister.protocol_name,
                    ProtocolRegister.registry_type,
                )
                .order_by(ProtocolRegister.registry_type)
            )
            .all()
        )

    proto_tabs: List[dict[str, str | int]] = [
        {
            "protocol_name": r[0],
            "registry_type": r[1],
            "total":         r[2],
        }
        for r in rows
    ]

    json_data_raw, _is_override = get_protocol_json(
        protocols_dir, protocol_group, protocol_name
    )
    # See protocols.py's protocol_table_partial() for why this uses a
    # separate raw-result name with an explicit `is not None` check rather
    # than annotating "json_data" directly on the unpack line or falling
    # back via `or {}` — get_protocol_json() can genuinely return None.
    json_data: dict[str, JSONValue] = json_data_raw if json_data_raw is not None else {}

    csv_path: str | None = None
    candidate: Path = protocols_dir / protocol_group / f"{protocol_name}.csv"
    if candidate.exists():
        csv_path = str(candidate)

    proto_groups: List[dict[str, str | list[str]]] = get_protocol_groups(protocols_dir)

    context: dict[
        str,
        NavData | list[dict[str, str | list[str]]] | str | list[dict[str, str | int]] | dict[str, JSONValue] | None
    ] = {
        "nav":            nav,
        "proto_groups":   proto_groups,
        "protocol_group": protocol_group,
        "protocol_name":  protocol_name,
        "proto_tabs":     proto_tabs,
        "json_data":      json_data,
        "csv_path":       csv_path,
        "app_state":      None,
    }

    template_name = (
        "partials/protocol_editor_panes.html"
        if request.headers.get("HX-Request")
        else "protocol_editor.html"
    )

    return request.app.state.templates.TemplateResponse(
        request=request,
        name=template_name,
        context=context,
    )


# ---------------------------------------------------------------------------
# Protocol register export
# ---------------------------------------------------------------------------

@router.get("/api/protocols/{protocol_name}/{registry_type}/export.csv")
def export_registers_csv(
    protocol_name: str,
    registry_type: str,
    device_name: str | None = None,
    db: Session = Depends(get_session),
) -> StreamingResponse:
    """
    Export all registers for a protocol/registry_type as a CSV download.
    When device_name is supplied, W/M/S selection columns are included.
    Paired _l/_h registers show an address range (e.g. 40-41) and a single
    logical row — mirroring what the table displays.
    """
    rows: list[dict[str, str | bool]] = export_protocol_registers(
        db, protocol_name, registry_type, device_name
    )
    if not rows:
        raise HTTPException(status_code=404, detail="No registers found")

    import io
    buf = io.StringIO()
    writer: csv.DictWriter[str] = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    buf.seek(0)

    filename: str = (
        f"{protocol_name}_{registry_type}"
        + (f"_{device_name}" if device_name else "")
        + ".csv"
    )
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/api/protocols/{protocol_name}/{registry_type}/export.json")
def export_registers_json(
    protocol_name: str,
    registry_type: str,
    device_name: str | None = None,
    db: Session = Depends(get_session),
) -> StreamingResponse:
    """
    Export all registers for a protocol/registry_type as a JSON download.
    Same contract as the CSV export — address ranges for paired registers,
    optional W/M/S fields when device_name is provided.
    """
    rows: list[dict[str, str | bool]] = export_protocol_registers(
        db, protocol_name, registry_type, device_name
    )
    if not rows:
        raise HTTPException(status_code=404, detail="No registers found")

    payload: dict[str, str | int | None | list[dict[str, str | bool]]] = {
        "protocol_name":  protocol_name,
        "registry_type":  registry_type,
        "device_name":    device_name,
        "register_count": len(rows),
        "registers":      rows,
    }
    filename = (
        f"{protocol_name}_{registry_type}"
        + (f"_{device_name}" if device_name else "")
        + ".json"
    )
    return StreamingResponse(
        iter([json.dumps(payload, indent=2)]),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# Log reader API
# ---------------------------------------------------------------------------

@router.get("/api/log", response_class=PlainTextResponse)
async def read_log(request: Request, lines: int = 250) -> PlainTextResponse:
    # defaults to 250 lines. The UI allows up to 1000, but we set a reasonable default to prevent
    # accidentally trying to read a huge log file when just opening the page. The endpoint can still be used to read more
    # lines if needed.

    log_path: Path = request.app.state.log_path

    if not log_path.exists():
        return PlainTextResponse("Log file not found.", status_code=404)

    # Efficient Tail
    try:
        # We read in binary mode to seek accurately from the end
        with open(log_path, "rb") as f:
            f.seek(0, os.SEEK_END)
            file_size: int = f.tell()

            # Start by looking at the last 256KB (plenty for 1000 lines)
            # which is the max allowed by the UI.
            # This avoids loading a multi-gig file into memory
            buffer_size: int = min(file_size, 262144)
            f.seek(file_size - buffer_size)

            # Decode only what we need
            chunk: str = f.read(buffer_size).decode("utf-8", errors="replace")
            log_lines: List[str] = chunk.splitlines()

            # Handle the case where the first line might be partial
            if len(log_lines) > lines:
                result: str = "\n".join(log_lines[-lines:])
            else:
                result = "\n".join(log_lines)

            return PlainTextResponse(result)

    except Exception as exc:
        return PlainTextResponse(f"Error: {exc}", status_code=500)

# ---------------------------------------------------------------------------
# Create Device API
# ---------------------------------------------------------------------------

FIXED_CREATE_KEYS: tuple[str, ...] = (
    "transport",
    "bridge",
    "protocol_version",
    "log_level",
    "transport_type_cached",
)
LOG_LEVELS: tuple[str, ...] = (
    "CRITICAL",
    "FATAL",
    "ERROR",
    "WARNING",
    "INFO",
    "DEBUG",
    "EXCEPTION",
)
CREATE_PROTOCOL_CSV_HEADERS: tuple[str, ...] = (
    "register",
    "variable_name",
    "documented_name",
    "unit",
    "data_type",
    "values",
    "read_interval",
    "writable",
    "adjustments",
    "note",
)
PROTOCOL_TYPES: tuple[str, ...] = ("coil", "discrete", "input", "holding", "other")


class CreateDeviceSettingInput(BaseModel):
    key: str = Field(..., min_length=1, max_length=128)
    value: str = ""
    is_active: bool = True

    @field_validator("key")
    @classmethod
    def validate_key(cls, value: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9_]+", value):
            raise ValueError("Setting keys must be alphanumeric or underscore.")
        return value


class CreateDeviceRequest(BaseModel):
    device_name: str = Field(..., pattern=r"^[a-zA-Z0-9_]+$", min_length=1, max_length=64)
    scraper_transport: str
    bridge: str
    protocol_version: str = ""
    log_level: str = "INFO"
    settings: list[CreateDeviceSettingInput] = []

    @field_validator("bridge")
    @classmethod
    def validate_bridge(cls, value: str) -> str:
        if not value:
            return value  # empty = None, valid
        for part in value.split(","):
            part = part.strip()
            if part and not part.startswith("transport."):
                raise ValueError("Each bridge must be a transport section reference (e.g. transport.mqtt).")
        return value

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        upper: str = value.upper()
        if upper not in LOG_LEVELS:
            msg: str = (f"Unknown log level '{value}'.")
            raise ValueError(msg)
        return upper


class CreateProtocolRowInput(BaseModel):
    register: str = ""
    variable_name: str = ""
    documented_name: str = ""
    unit: str = ""
    data_type: str = ""
    values: str = ""
    read_interval: str = ""
    writable: str = "R"
    adjustments: str = ""
    note: str = ""


class CreateProtocolRequest(BaseModel):
    manufacturer: str = Field(..., min_length=1, max_length=128)
    protocol_name: str = Field(..., min_length=1, max_length=128)
    protocol_type: str
    rows: list[CreateProtocolRowInput] = []

    @field_validator("manufacturer", "protocol_name")
    @classmethod
    def validate_slug(cls, value: str) -> str:
        value = value.strip().lower().replace("-", "_").replace(" ", "_")
        value = re.sub(r"_+", "_", value)
        if not re.fullmatch(r"[a-z0-9_]+", value):
            raise ValueError("Use letters, numbers, spaces, hyphens, or underscores only.")
        return value

    @field_validator("protocol_type")
    @classmethod
    def validate_protocol_type(cls, value: str) -> str:
        value = value.strip().lower()
        if value not in PROTOCOL_TYPES:
            raise ValueError("Unknown protocol type.")
        return value


def _append_section_to_config(config_path: Path, section: str, fields: list[tuple[str, str]]) -> None:
    section_text: str = "\n".join(
        [f"[{section}]", *[f"{key} = {value}" for key, value in fields]]
    ) + "\n"

    existing_text: str = config_path.read_text(encoding="utf-8") if config_path.exists() else ""
    if existing_text:
        separator = ""
        if not existing_text.endswith("\n"):
            separator = "\n"
        if not existing_text.endswith("\n\n"):
            separator += "\n"
        config_path.write_text(existing_text + separator + section_text, encoding="utf-8")
    else:
        config_path.write_text(section_text, encoding="utf-8")


def _csv_filename(protocol_name: str, protocol_type: str) -> str:
    if protocol_type == "other":
        return f"{protocol_name}.registry_map.csv"
    return f"{protocol_name}.{protocol_type}_registry_map.csv"


def _base_protocol_json(manufacturer: str, protocol_name: str, protocol_type: str) -> dict[str, str | int | bool]:
    return {
        "manufacturer": manufacturer,
        "protocol": protocol_name,
        "batch_size": 40,
        "send_holding_register": protocol_type in ("holding", "other"),
        "send_input_register": protocol_type in ("input", "other"),
        "send_coil_register": protocol_type in ("coil", "other"),
        "send_discrete_register": protocol_type in ("discrete", "other"),
    }


@router.post("/api/devices/create")
def create_device(request: Request, payload: CreateDeviceRequest, db: Session = Depends(get_session)) -> dict[str, str | int]:
    """
    Create a new device directly in config.cfg, then re-scan so the staging
    database reflects the new on-disk section without disturbing existing rows.
    """
    section: str = f"transport.{payload.device_name}"
    config_path: Path = request.app.state.config_path
    config_data: dict[str, dict[str, str]] = load_config(config_path)
    if section in config_data or db.query(Setting).filter_by(section=section).first():
        raise HTTPException(status_code=409, detail=f"Device '{payload.device_name}' already exists.")

    library: dict[str, TransportLibraryEntry] = scan_transport_library(request.app.state.transports_dir)
    scraper_info: TransportLibraryEntry | None = library.get(payload.scraper_transport)
    if not scraper_info or scraper_info.get("classification") != "scraper":
        raise HTTPException(status_code=400, detail="Selected scraper transport is not valid.")

    # Validate each bridge section reference against the library
    if payload.bridge:
        for bridge_part in payload.bridge.split(","):
            bridge_part: str = bridge_part.strip()
            if not bridge_part:
                continue
            bridge_name: str = bridge_part.removeprefix("transport.")
            bridge_info: TransportLibraryEntry | None = library.get(bridge_name)
            if not bridge_info or bridge_info.get("classification") != "bridge":
                raise HTTPException(status_code=400, detail=f"Selected bridge '{bridge_part}' is not valid.")

    allowed_keys: set[str] = {
        key for key in scraper_info.get("keys", {}).keys()
        if key not in FIXED_CREATE_KEYS
    }

    seen_keys: set[str] = set()
    ordered_fields: list[tuple[str, str]] = [
        ("transport", payload.scraper_transport),
        ("bridge", payload.bridge),
        ("protocol_version", payload.protocol_version),
        ("log_level", payload.log_level),
    ]

    for item in payload.settings:
        if not item.is_active:
            continue
        if item.key in FIXED_CREATE_KEYS:
            continue
        if item.key not in allowed_keys:
            raise HTTPException(status_code=400, detail=f"Unknown setting key '{item.key}' for selected scraper.")
        if item.key in seen_keys:
            continue
        seen_keys.add(item.key)
        ordered_fields.append((item.key, item.value))

    try:
        create_backup(config_path, db, trigger="create_device")
        db.commit()
        _append_section_to_config(config_path, section, ordered_fields)
        request.app.state.scanner.set_cfg_is_truth(True)
        request.app.state.scanner.run(db)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    refresh_app_state(db)
    db.commit()

    _log.info("Device '%s' appended to config.cfg", payload.device_name)
    return {
        "status": "created",
        "device_name": payload.device_name,
        "section": section,
        "keys_added": len(ordered_fields),
    }


@router.post("/api/protocols/create")
def create_protocol(request: Request, payload: CreateProtocolRequest, db: Session = Depends(get_session)) -> dict[str, str]:
    """
    Create a protocol JSON file and one register-map CSV under protocols/<manufacturer>.
    The browser keeps drafts in memory until this endpoint is called, so canceling
    the wizard leaves disk untouched.
    """
    manufacturer: str = payload.manufacturer
    protocol_name: str = payload.protocol_name
    if not protocol_name.startswith(f"{manufacturer}_"):
        raise HTTPException(
            status_code=400,
            detail=f"Protocol name must start with '{manufacturer}_'.",
        )

    protocols_dir: Path = request.app.state.protocols_dir
    manufacturer_dir: Path = protocols_dir / manufacturer
    try:
        manufacturer_dir.relative_to(protocols_dir)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid manufacturer path.")

    csv_path: Path = manufacturer_dir / _csv_filename(protocol_name, payload.protocol_type)
    json_path: Path = manufacturer_dir / f"{protocol_name}.json"

    if csv_path.exists():
        raise HTTPException(status_code=409, detail=f"{csv_path.name} already exists.")

    created_dir = False
    written_paths: list[Path] = []
    try:
        if not manufacturer_dir.exists():
            manufacturer_dir.mkdir(parents=True)
            created_dir = True

        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer: csv.DictWriter[str] = csv.DictWriter(f, fieldnames=CREATE_PROTOCOL_CSV_HEADERS)
            writer.writeheader()
            for item in payload.rows:
                row: dict[str, str] = {
                    key: str(getattr(item, key, "") or "").strip()
                    for key in CREATE_PROTOCOL_CSV_HEADERS
                }
                if not any(row.values()):
                    continue
                writer.writerow(row)
        written_paths.append(csv_path)

        if not json_path.exists():
            json_path.write_text(
                json.dumps(
                    _base_protocol_json(manufacturer, protocol_name, payload.protocol_type),
                    indent=2,
                ) + "\n",
                encoding="utf-8",
            )
            written_paths.append(json_path)

        request.app.state.scanner.run(db)
        refresh_app_state(db)
        db.commit()
    except Exception as exc:
        db.rollback()
        for path in written_paths:
            try:
                if path.exists():
                    path.unlink()
            except OSError:
                _log.warning("Failed to roll back created protocol file %s", path)
        if created_dir:
            try:
                manufacturer_dir.rmdir()
            except OSError:
                _log.warning("Failed to roll back created manufacturer folder %s", manufacturer_dir)
        raise HTTPException(status_code=500, detail=str(exc))

    return {
        "status": "created",
        "manufacturer": manufacturer,
        "protocol_name": protocol_name,
        "protocol_type": payload.protocol_type,
        "csv_path": str(csv_path),
        "json_path": str(json_path),
        "editor_url": f"/protocol-editor/{manufacturer}/{protocol_name}",
    }
