# Description: routers/devices.py — Device settings endpoints.
# File: devices.py
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

"""routers/devices.py — Device settings endpoints."""

from __future__ import annotations

import asyncio
import logging
import threading
import types
from pathlib import Path
from typing import TYPE_CHECKING, Any, List

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from classes.WebServer.models import AppState
from classes.WebServer.services.device_service import DeviceSummary, NavData

from ...transports.modbus_base import modbus_base
from ...transports.transport_base import transport_base
from ..database import get_session, refresh_app_state, session_scope
from ..models import Setting
from ..scanner import (
    EMPTY_TRANSPORT_ENTRY,
    TransportLibraryEntry,
    scan_transport_library,
)
from ..services.analysis_service import get_transport_connection_status
from ..services.bridge_service import has_staged_deletions, staged_deletion_count
from ..services.device_service import (
    delete_orphans_bulk,
    ensure_bridge_sections_exist,
    get_app_state,
    get_device_settings,
    get_device_summary,
    get_nav_data,
    get_orphaned_settings,
)
from ..services.protocol_service import (
    get_device_metric_summary,
    get_protocols_for_device,
)

if TYPE_CHECKING:
    # Deferred at runtime — importing protocol_gateway at module load time
    # risks a circular import, since it's what wires up the WebServer app
    # in the first place (see the same pattern in commit.py/gateway_status.py).
    # Only needed here, under TYPE_CHECKING, for the annotations below.
    from protocol_gateway import Protocol_Gateway

_log: logging.Logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/devices", tags=["devices"])


class SettingUpdate(BaseModel):
    value_staged: str | None = None
    is_active: bool | None = None


class ReconcileRequest(BaseModel):
    new_transport: str | None = None   # e.g. "modbus_tcp"
    new_bridge: str | None = None      # e.g. "transport.mqtt"


class CreateAndActivateRequest(BaseModel):
    key: str
    default_value: str | None = None


class RefreshProtocolRequest(BaseModel):
    new_protocol: str


class OrphanDeleteRequest(BaseModel):
    ids: list[int]


# ── Static routes must come before wildcard /{device_name} routes ──────────
# FastAPI matches in registration order; /state, /orphans, /nav, /connection-status
# would otherwise be captured by /{device_name}/settings as device_name="state" etc.


@router.patch("/diag/patch-test")
def diag_patch_test(payload: SettingUpdate, request: Request, db: Session = Depends(get_session)) -> dict[str, str | bool | dict[str, str | bool | None]]:
    """
    Test endpoint: POST a JSON body here to verify FastAPI receives it correctly.
    Example: curl -X PATCH http://host:1717/api/devices/diag/patch-test \
               -H "Content-Type: application/json" \
               -d '{"value_staged": "hello"}'
    """
    return {
        "content_type_received": request.headers.get("content-type", "MISSING"),
        "payload_received": payload.model_dump(),
        "value_staged_is_none": payload.value_staged is None,
        "is_active_is_none": payload.is_active is None,
        "verdict": "JSON body parsed correctly" if payload.value_staged is not None or payload.is_active is not None
                   else "PAYLOAD IS EMPTY — json-enc is not sending data or Content-Type is wrong",
    }


@router.get("/diag")
def diagnostics(db: Session = Depends(get_session)) -> dict[str, Any]:
    """
    Self-test endpoint. Visit /api/devices/diag in your browser to verify
    routing, DB connectivity, and app state. Safe — read-only.

    Genuinely Any here rather than a tightened union: this is a free-form
    debug grab-bag by design (each try/except branch stuffs in whatever
    succeeded — a row count, an error string, a nested state dict — and
    that shape is meant to grow as new debug checks get added ad hoc).
    A precise type would just be relisted every time a check is added,
    with no caller ever consuming this beyond eyeballing it in a browser.
    """
    result: dict[str, Any] = {}

    # DB connectivity
    try:
        row_count: int | None = db.execute(text("SELECT COUNT(*) FROM settings")).scalar()
        result["db_ok"] = True
        result["settings_row_count"] = row_count
    except Exception as exc:
        result["db_ok"] = False
        result["db_error"] = str(exc)

    # Dirty counts
    try:
        dirty: int | None = db.execute(text("SELECT COUNT(*) FROM settings WHERE is_dirty=1")).scalar()
        result["dirty_settings"] = dirty
    except Exception as exc:
        result["dirty_settings"] = f"error: {exc}"

    # App state row
    try:
        state: AppState = get_app_state(db)
        result["app_state"] = {
            "has_dirty_settings": state.has_dirty_settings,
            "has_dirty_protocols": state.has_dirty_protocols,
            "dirty_settings_count": state.dirty_settings_count,
            "scanner_status": state.scanner_status,
        }
    except Exception as exc:
        result["app_state"] = f"error: {exc}"

    # Route self-check — confirm this endpoint resolved correctly
    result["route_resolution_ok"] = True
    result["note"] = ("If you see this response, /api/devices/diag resolved correctly. ")

    return result


@router.get("/nav")
def nav_data(db: Session = Depends(get_session)) -> dict[str, list[dict[str, str]] | list[str]]:
    """Returns scraper/bridge lists and protocol groups for nav rendering."""
    nav: NavData = get_nav_data(db)
    return {
        "scrapers": [
            {"name": s.name, "transport_class": s.transport_class,
             "protocol_version": s.protocol_version}
            for s in nav.scrapers
        ],
        "bridges": [
            {"name": b.name, "transport_class": b.transport_class}
            for b in nav.bridges
        ],
        "protocol_groups": nav.protocol_groups,
    }


@router.get("/state")
def app_state(request: Request, db: Session = Depends(get_session)) -> dict[str, bool | int | str | None]:
    state: AppState = get_app_state(db)
    return {
        "has_dirty_settings": state.has_dirty_settings,
        "has_dirty_protocols": state.has_dirty_protocols,
        # Timescale column deletions are staged in-memory on app.state
        # (see services/bridge_service.py), not in the staging DB, since
        # they're live Postgres schema rather than config.cfg settings.
        "has_dirty_timescale": has_staged_deletions(request.app.state),
        "has_orphans": state.has_orphans,
        "dirty_settings_count": state.dirty_settings_count,
        "dirty_protocols_count": state.dirty_protocols_count,
        "dirty_timescale_count": staged_deletion_count(request.app.state),
        "orphan_count": state.orphan_count,
        "last_scan_at": state.last_scan_at.isoformat() if state.last_scan_at else None,
        "last_commit_at": state.last_commit_at.isoformat() if state.last_commit_at else None,
        "scanner_status": state.scanner_status,
    }


@router.get("/orphans")
def list_orphans(db: Session = Depends(get_session)) -> list[dict[str, int | str | None]]:
    rows: list[Setting] = get_orphaned_settings(db)
    return [
        {
            "id": r.id,
            "section": r.section,
            "key": r.key,
            "value_disk": r.value_disk,
            "transport_type": r.transport_type,
        }
        for r in rows
    ]


@router.delete("/orphans")
def delete_orphans(payload: OrphanDeleteRequest, db: Session = Depends(get_session)) -> dict[str, int]:
    count: int = delete_orphans_bulk(db, payload.ids)
    refresh_app_state(db)
    db.commit()
    return {"deleted": count}


@router.get("/connection-status")
def connection_status(request: Request) -> dict[str, bool]:
    """Returns live connection status for all transports from the gateway instance."""
    gateway: "Protocol_Gateway | None" = getattr(request.app.state, "gateway", None)
    result: dict[str, bool] = get_transport_connection_status(gateway)
    if result:
        _log.debug("connection-status keys: %s", list(result.keys()))
    return result


@router.patch("/general/{section}/{setting_id}")
def update_general_setting(section: str, setting_id: int, payload: SettingUpdate, request: Request, db: Session = Depends(get_session)) -> dict[str, int | bool | str | None]:
    """Update a setting in general/logging sections."""
    _log.warning("PATCH update_general: section=%s id=%s content-type=%s payload=%s",
                 section, setting_id,
                 request.headers.get("content-type", "MISSING"),
                 payload.model_dump())
    row: Setting | None = db.get(Setting, setting_id)
    if not row:
        raise HTTPException(status_code=404, detail="Setting not found")
    if row.section != section:
        raise HTTPException(status_code=403, detail="Section mismatch")

    if payload.value_staged is not None:
        row.value_staged = payload.value_staged
    if payload.is_active is not None:
        row.is_active = payload.is_active
        if row.is_active and not row.value_staged:
            row.value_staged = row.default_value or ""

    row.mark_dirty()
    db.flush()
    refresh_app_state(db)
    db.commit()

    return {"id": row.id, "is_dirty": row.is_dirty, "value_staged": row.value_staged}


# ── Transport / Bridge reconcile ───────────────────────────────────────────

@router.post("/{device_name}/reconcile-settings")
def reconcile_settings(
    device_name: str,
    payload: ReconcileRequest,
    request: Request,
    db: Session = Depends(get_session),
) -> HTMLResponse:
    """
    Called when the user picks a new TRANSPORT or BRIDGE.

    Only stages the transport/bridge value change (reversible by Cancel).
    Does NOT insert rows or mutate is_orphan in the DB — those structural
    changes are permanent and would survive a Cancel. Instead the display
    list is computed on-the-fly by merging:
      - DB rows that belong to the new transport's key set  (shown normally)
      - DB rows outside the new transport's key set         (hidden — inactive orphans)
      - Library keys with no DB row yet                    (shown as virtual inactive rows)
    The virtual rows are passed to the template as plain dicts with id=None.
    They only get persisted to the DB when the user activates them and commits.
    """

    section: str = f"transport.{device_name}"
    transports_dir: Path = request.app.state.transports_dir

    # Stage the new transport / bridge value only — no db.commit() here.
    #   This keeps the change reversible via the Cancel button.
    if payload.new_transport is not None:
        row: Setting | None = db.query(Setting).filter(
            Setting.section == section, Setting.key == "transport"
        ).first()
        if row:
            row.value_staged = payload.new_transport
            row.mark_dirty()

    if payload.new_bridge is not None:
        row = db.query(Setting).filter(
            Setting.section == section, Setting.key == "bridge"
        ).first()
        if row:
            row.value_staged = payload.new_bridge
            row.mark_dirty()

    db.flush()
    refresh_app_state(db)
    db.commit()

    # Determine expected keys for the newly selected transport
    transport_row: Setting | None = db.query(Setting).filter(
        Setting.section == section, Setting.key == "transport"
    ).first()
    current_transport: str | None = transport_row.value_staged if transport_row else ""

    library: dict[str, TransportLibraryEntry] = scan_transport_library(transports_dir)
    transport_info: TransportLibraryEntry = EMPTY_TRANSPORT_ENTRY
    if current_transport is not None:
        transport_info = library.get(current_transport, EMPTY_TRANSPORT_ENTRY)
    expected_keys: dict[str, str | None] = transport_info.get("keys", {})


    FIXED_KEYS: set[str] = {
        "transport",
        "bridge",
        "protocol_version",
        "log_level",
        "transport_type_cached"}

    # Build the display list without touching the DB
    existing_rows: list[Setting] = get_device_settings(db, section)
    existing_map: dict[str, Setting] = {r.key: r for r in existing_rows}

    # Both branches below populate this with either a real Setting row or a
    # types.SimpleNamespace "virtual row" placeholder (see below) — the
    # template only relies on their shared duck-typed attribute set
    # (key, value_staged, default_value, is_active, is_dirty, is_orphan, ...).
    display_rows: list[Setting | types.SimpleNamespace] = []

    if expected_keys:
        # Keys the new transport defines — use DB row if it exists, else virtual.
        # If the DB row exists but was orphaned by a previous transport switch,
        # clear is_orphan in memory so the template shows it correctly.
        # This is a display-only mutation — not committed to DB until user commits.
        for key in sorted(expected_keys.keys()):
            if key in FIXED_KEYS:
                continue
            if key in existing_map:
                row = existing_map[key]
                row.is_orphan = False   # belongs to this transport — not an orphan
                display_rows.append(row)
            else:
                # Virtual row — not yet in DB, shown as inactive placeholder
                default_val: str = expected_keys[key] or ""
                display_rows.append(types.SimpleNamespace(
                    id=None,
                    key=key,
                    value_disk="",
                    value_staged=default_val,
                    default_value=default_val,
                    is_active=False,
                    is_dirty=False,
                    is_orphan=False,
                    section=section,
                ))

        # DB rows NOT in the new transport — only include if active (user-intentional)
        for key, row in existing_map.items():
            if key in FIXED_KEYS or key in expected_keys:
                continue
            if row.is_active:
                display_rows.append(row)
    else:
        # Unknown transport — show all existing non-fixed rows as-is
        for key, row in existing_map.items():
            if key not in FIXED_KEYS:
                display_rows.append(row)

    display_rows.sort(key=lambda r: r.key)

    # Render the settings rows partial with the computed display list
    summary: DeviceSummary | None = get_device_summary(db, device_name)
    templates = request.app.state.templates
    html = templates.get_template("partials/settings_rows.html").render(
        {"device": summary, "settings": display_rows, "request": request}
    )
    return HTMLResponse(content=html)


@router.post("/{device_name}/settings/create-and-activate")
def create_and_activate(
    device_name: str,
    payload: CreateAndActivateRequest,
    db: Session = Depends(get_session),
) -> dict[str, int | str | bool | None]:
    """
    Creates a new Setting row for a virtual key (one from the transport library
    that has no DB row yet) and immediately marks it active and dirty.
    Called when the user checks a virtual row's checkbox.
    """
    section: str = f"transport.{device_name}"

    # Check it doesn't already exist (race condition guard)
    existing: Setting | None = db.query(Setting).filter(
        Setting.section == section, Setting.key == payload.key
    ).first()

    if existing:
        existing.is_active = True
        existing.is_dirty = True
        if not existing.value_staged:
            existing.value_staged = payload.default_value or existing.default_value or ""
        row: Setting = existing
    else:
        row = Setting(
            section=section,
            key=payload.key,
            value_disk="",
            value_staged=payload.default_value or "",
            default_value=payload.default_value or "",
            transport_type="scraper",
            is_active=True,
            is_dirty=True,
            is_orphan=False,
        )
        db.add(row)

    db.flush()
    refresh_app_state(db)
    db.commit()

    return {
        "id": row.id,
        "key": row.key,
        "value_staged": row.value_staged,
        "is_active": row.is_active,
        "is_dirty": row.is_dirty,
    }


@router.post("/{device_name}/refresh-protocol-tabs")
def refresh_protocol_tabs(
    device_name: str,
    payload: RefreshProtocolRequest,
    request: Request,
    db: Session = Depends(get_session),
) -> HTMLResponse:
    """
    Called when the user changes Protocol Version.
    Stages the new protocol value and returns the rendered protocol section
    (tabs with W/M/S counts + empty table container) for the new protocol.
    """
    section: str = f"transport.{device_name}"

    # Stage the new protocol_version value
    row: Setting | None = db.query(Setting).filter(
        Setting.section == section, Setting.key == "protocol_version"
    ).first()
    if row:
        row.value_staged = payload.new_protocol
        row.mark_dirty()
        db.flush()
        refresh_app_state(db)
        db.commit()

    proto_tabs: List[dict[str, Any]] = get_protocols_for_device(db, payload.new_protocol, device_name=device_name)
    # proto_tabs / metric_summary below keep Any: both come from
    # protocol_service.py (a service module, not this router — its own
    # return types are out of this pass's scope; see the equivalent note
    # for commit_staged_deletions() in commit.py).
    summary: DeviceSummary | None = get_device_summary(db, device_name)

    # Rebuild summary with updated protocol_version so the heading shows correctly
    if summary:
        summary.protocol_version = payload.new_protocol

    has_no_selections: bool = not any(
        t.get("mask_count", 0) or t.get("screen_count", 0) or t.get("write_count", 0)
        for t in proto_tabs
    ) if proto_tabs else False

    gateway: "Protocol_Gateway | None" = getattr(request.app.state, "gateway", None)
    live_transport: transport_base | None = gateway.get_transport(f"transport.{device_name}") if gateway is not None else None
    metric_summary: dict[str, Any] | None = (
        get_device_metric_summary(db, payload.new_protocol, device_name, transport=live_transport)
        if payload.new_protocol else None
    )

    templates: Any = request.app.state.templates
    html: Any  = templates.get_template("partials/protocol_section.html").render({
        "device": summary,
        "proto_tabs": proto_tabs,
        "has_no_selections": has_no_selections,
        "metric_summary": metric_summary,
        "request": request,
    })
    return HTMLResponse(content=html)


# ── Wildcard /{device_name} routes — registered last ───────────────────────

@router.get("/{device_name}/settings")
def device_settings(device_name: str, db: Session = Depends(get_session)) -> list[dict[str, int | str | bool | None]]:
    """Return all settings rows for a device."""
    section: str = f"transport.{device_name}"
    rows: list[Setting] = get_device_settings(db, section)
    return [
        {
            "id": r.id,
            "key": r.key,
            "value_disk": r.value_disk,
            "value_staged": r.value_staged,
            "default_value": r.default_value,
            "is_active": r.is_active,
            "is_dirty": r.is_dirty,
            "is_orphan": r.is_orphan,
            "transport_type": r.transport_type,
        }
        for r in rows
    ]


@router.patch("/{device_name}/settings/{setting_id}")
def update_setting(device_name: str, setting_id: int, payload: SettingUpdate, request: Request, db: Session = Depends(get_session)) -> dict[str, int | str | bool | None]:
    """Update a single setting's staged value or active state."""
    _log.warning("PATCH update_setting: device=%s id=%s content-type=%s payload=%s",
                 device_name, setting_id,
                 request.headers.get("content-type", "MISSING"),
                 payload.model_dump())
    row: Setting | None = db.get(Setting, setting_id)
    if not row:
        raise HTTPException(status_code=404, detail="Setting not found")
    if row.section != f"transport.{device_name}":
        raise HTTPException(status_code=403, detail="Section mismatch")

    if payload.value_staged is not None:
        row.value_staged = payload.value_staged
    if payload.is_active is not None:
        row.is_active = payload.is_active
        if row.is_active and not row.value_staged:
            row.value_staged = row.default_value or ""

    # Compute dirty correctly:
    # - Active row: dirty if staged value differs from what's on disk
    # - Deactivated row: dirty only if the disk has a real value that needs removing.
    #   If disk was already empty, deactivating changes nothing on disk → not dirty.
    if row.is_active:
        row.mark_dirty()
    else:
        row.is_dirty = bool(row.value_disk)  # dirty only if disk had something to remove

    if row.key == "bridge" and payload.value_staged is not None:
        # The bridge multi-select (scraper_panes.html) PATCHes this row
        # directly rather than going through reconcile-settings, so this is
        # the only place that sees a new bridge selection land. Auto-create
        # DB rows for any newly-referenced bridge section that doesn't exist
        # yet — otherwise the staged "bridge = transport.X" reference points
        # at a section commit_all() has no rows for and will never write.
        ensure_bridge_sections_exist(db, row.value_staged)

    db.flush()
    refresh_app_state(db)
    db.commit()

    return {
        "id": row.id,
        "key": row.key,
        "value_staged": row.value_staged,
        "is_dirty": row.is_dirty,
        "is_active": row.is_active,
    }


# ---------------------------------------------------------------------------
# Page-partial / device-data routes — now on the same /api/devices router
# as everything else above.
# ---------------------------------------------------------------------------


@router.get("/orphan-modal", response_class=HTMLResponse, response_model=None)
async def orphan_modal(request: Request) -> Any:
    """HTMX partial — orphan review modal content."""
    with session_scope() as db:
        orphans: List[Setting] = get_orphaned_settings(db)

    return request.app.state.templates.TemplateResponse(
        request=request,
        name="partials/orphan_modal.html",
        context={"orphans": orphans},
    )


def _failed_register_keys(transport: transport_base | None) -> dict[str, str]:
    """Best-effort {variable_name: "disabled"|"failing"} map for a transport.

    Only modbus_base subclasses track register failures; anything else
    (bridges, non-modbus scrapers, or no transport at all) returns an empty
    map so the caller can merge this in unconditionally.
    """
    if not isinstance(transport, modbus_base):
        return {}
    try:
        return transport.get_failed_register_variables()
    except Exception as e:
        _log.debug(f"error retrieving register failure status: {e}")
        return {}


def _clean_last_known_data(raw: dict[str, Any]) -> dict[str, str]:
    """Stringify a transport's last_known_data for JSON response.

    ``raw``'s values genuinely span whatever a register or synthetic/computed
    metric produced for a given cycle — int, float, str, bool, ASCII decodes,
    etc. (see protocol_settings.py's process_registry_map()) — there's no
    single value type that's actually accurate here, hence Any rather than a
    tightened union.

    Includes "*_desc" entries — these are real registry_map rows in their
    own right (protocol_settings.py's _add_code_description_entries()
    generates a synthetic "<name>_desc" entry for any code-mapped field,
    and it gets its own row in the Protocol Metrics table via
    data-value-key="<name>_desc"), not incidental duplicates of their
    source field. Previously filtered out here on the assumption they were
    redundant, which left every "_desc" row's VALUE column permanently
    showing the "—" not-yet-populated placeholder (see populateValues() in
    scraper_panes.html) instead of the human-readable text
    _code_description_for_value() computed for it. Shared by the immediate
    and wait variants of /last-values, which otherwise present the same
    values in the same shape at two different points in the scrape cycle.
    """
    clean: dict[str, str] = {}
    for k, v in raw.items():
        try:
            clean[k] = str(round(v, 4)) if isinstance(v, float) else str(v)
        except Exception as e:
            _log.debug(f"error retrieving last_known_data {e}")
    return clean


@router.get("/{device_name}/last-values")
async def device_last_values(request: Request, device_name: str) -> JSONResponse:
    """Return the last bridge-confirmed scrape values for a device transport.

    Values come from ``last_known_data`` which is populated in
    ``protocol_gateway._snapshot_scraper_data`` immediately before each
    ``bridge.write_data()`` call — the authoritative point where a cycle
    is confirmed complete and bridge-bound.
    """
    gateway: "Protocol_Gateway | None" = getattr(request.app.state, "gateway", None)
    if gateway is None:
        return JSONResponse({"values": {}, "status": "no_gateway"})
    transport: transport_base | None = gateway.get_transport(f"transport.{device_name}")
    if transport is None:
        return JSONResponse({"values": {}, "status": "not_found"})

    # last_known_data's value type is genuinely heterogeneous — see
    # _clean_last_known_data()'s docstring.
    raw: dict[str, Any] = getattr(transport, "last_known_data", {})
    clean: dict[str, str] = _clean_last_known_data(raw)

    return JSONResponse({"values": clean, "status": "ok", "failed_keys": _failed_register_keys(transport)})


@router.get("/{device_name}/last-values/wait")
async def device_last_values_wait(request: Request, device_name: str) -> JSONResponse:
    """Block until the next scrape cycle completes, then return its values.

    The refresh button calls this endpoint.  It waits on
    ``transport.values_ready_event`` which is set (then immediately
    cleared) in ``_snapshot_scraper_data`` each time a cycle's data is
    forwarded to a bridge.  The client therefore receives the values from
    the next complete cycle rather than a cached stale snapshot.

    Times out after ``timeout`` seconds (default 90 — enough for even a
    slow polling interval plus retries) and returns ``status: timeout``
    so the client can show an appropriate message.
    """

    timeout: float = 90.0
    gateway: "Protocol_Gateway | None" = getattr(request.app.state, "gateway", None)
    if gateway is None:
        return JSONResponse({"values": {}, "status": "no_gateway"})
    transport: transport_base | None = gateway.get_transport(f"transport.{device_name}")
    if transport is None:
        return JSONResponse({"values": {}, "status": "not_found"})

    event: threading.Event = getattr(transport, "values_ready_event", threading.Event())
    if event is None:  # pyright: ignore[reportUnnecessaryComparison]
        return JSONResponse({"values": {}, "status": "no_event"})

    # Run the blocking wait() in a thread pool so we don't block the
    # async event loop.  asyncio.to_thread requires Python 3.9+.
    fired: bool = await asyncio.to_thread(event.wait, timeout)
    if not fired:
        return JSONResponse({"values": {}, "status": "timeout"})

    raw: dict[str, Any] = getattr(transport, "last_known_data", {})
    clean: dict[str, str] = _clean_last_known_data(raw)

    return JSONResponse({"values": clean, "status": "ok", "failed_keys": _failed_register_keys(transport)})
