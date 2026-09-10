# Description: services/bridge_service.py — Consolidated runtime helpers for every bridge-type transport's admin screens and device-page info panels (TimescaleDB, InfluxDB v1/v3, MQTT).
# File: bridge_service.py
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
services/bridge_service.py — Consolidated runtime helpers for every bridge-
type transport's admin screens and device-page info panels.

Merged from three previously separate modules (timescale_service.py,
influxdb_service.py, mqtt_service.py) into one file, organized into three
clearly-delineated sections below (search for the "====" banners): each
section keeps that transport's original module-level docstring inline, so
the reasoning behind that transport's specific design choices (singleton
vs. name-scoped bridge lookup, what is/isn't queryable, etc.) stays right
next to its code rather than living only in a merge commit message.

    TIMESCALEDB   — Delete Columns / Rebuild Rollup Views admin screens,
                    plus Bridge Health / Storage Overview / Compression &
                    Retention / Background Jobs device-page info panels.
                    The only bridge type with a staging + commit flow (see
                    that section's docstring) and the only one treated as
                    a gateway-wide singleton (get_timescale_bridge) rather
                    than looked up by name — there's only ever one
                    TimescaleDB bridge.

    INFLUXDB      — Bridge Health / Storage Overview device-page info
                    panels for InfluxDB v1 (influxdb_out) and v3
                    (influxdb3_out). Name-scoped (get_influxdb_bridge),
                    since a gateway can have more than one v1/v3 bridge
                    configured at once.

    MQTT          — Bridge Health device-page info panel only, no Storage
                    Overview (MQTT brokers generally don't persist
                    historical data). Also name-scoped, for the same
                    reason as InfluxDB.

    PROMETHEUS    — Bridge Health (in-memory registry summary) / Target
                    Health (per-machine scrape_failures_total / last-scrape
                    table) device-page info panels for the pull-model
                    prometheus_out bridge. Name-scoped, same reasoning as
                    InfluxDB/MQTT — a gateway could run more than one
                    Prometheus bridge on different ports/paths.

    DELETION      — delete_bridge(), at the bottom of this file. The one
                    function here that isn't read-only introspection of a
                    live bridge object: it edits the staging DB directly
                    to remove a bridge's section (and any scraper "bridge"
                    references to it) ahead of the next commit. See its
                    own docstring for why it lives here paired with
                    routers/bridges.py rather than in device_service.py.

"""
from __future__ import annotations

import itertools
import logging
import sys
import threading
import time
from typing import TYPE_CHECKING, Any, Generator, cast

from starlette.datastructures import State

from ...protocol_settings import Registry_Type, registry_map_entry
from ...transports.transport_base import transport_base
from .protocol_service import SyntheticFieldMetadata

if sys.version_info >= (3, 12):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict

_log: logging.Logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared formatting helpers — used across more than one bridge type's info
# panels. See module docstring above for why _format_dt / _format_elapsed
# are NOT here (each has exactly one caller, in one section).
# ---------------------------------------------------------------------------

def _format_bytes(n: int | None) -> str:
    """Human-readable byte size, e.g. 1536 -> '1.5 KB'. None/0 -> '0 B'."""
    if not n:
        return "0 B"
    size = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} {unit}"
        size /= 1024
    return f"{size:.1f} TB"  # unreachable, keeps type checkers happy


# ============================================================================
# TIMESCALEDB
# ============================================================================

# The timescaledb bridge module is an optional/pluggable transport — a
# deployment without it configured shouldn't crash the webserver on import.
# Timescale_Available gates every function below and drives whether the
# "Timescale DB" nav pad is shown at all (see is_timescale_available()).
#
# The TYPE_CHECKING import below is for static type checkers only and is
# never executed. It gives every annotation in this file one single,
# unconditional class binding to check against. The runtime try/except
# further down binds the one class this module actually needs to
# instantiate (BridgeAdminManager) under a *different* name,
# _BridgeAdminManagerImpl — deliberately not reusing the same name,
# because `except ImportError: BridgeAdminManager = None` would make
# the type checker infer `type[BridgeAdminManager] | None` for that
# name (a variable, not a type), which is exactly what produces
# "Variable not allowed in type expression" on every annotation that
# references it. WideTableField / WideTableFieldDeletionResult are only
# ever used in annotations in this file (never instantiated here), so they
# don't need a runtime binding at all — under `from __future__ import
# annotations` those annotations are never evaluated at runtime.
#
# Protocol_Gateway is included here for the same reason every other
# router/service in this app defers it under TYPE_CHECKING (see
# commit.py, devices.py, gateway_status.py, timescale.py, pages.py) —
# importing protocol_gateway at module load time risks a circular import,
# since it's what wires up the WebServer app in the first place.
if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from protocol_gateway import Protocol_Gateway

    from ...transports.timescaledb import (
        BridgeAdminManager,
        WideTableField,
        WideTableFieldDeletionResult,
        timescaledb,
    )
    from ..models import Setting

# A great many functions below deliberately keep `Any` for values that
# originate from calling a method on an mqtt/influxdb/prometheus bridge
# object found via duck-typing (type(t).__name__ == "mqtt"/etc. — see
# get_influxdb_bridge/get_mqtt_bridge/get_prometheus_bridge) rather than
# isinstance against an imported class. That's this module's own
# documented design choice (see the module docstring above): every
# bridge transport module is optional/pluggable, and this file must not
# fail to import just because one of them isn't installed in a given
# deployment. Once a value comes from one of those bridges, there is no
# type available to assert against without hard-importing that specific
# transport class — which is exactly what the duck-typing exists to
# avoid. Those Any usages are intentional and are not re-justified
# individually at each occurrence below; only genuinely fixable Any usage
# (gateway/app_state parameters, the locally-defined staging-dict shape,
# locally-built dict literals with a fully known shape) is narrowed.
#
# get_timescale_bridge() below is the one exception to that pattern: it
# returns a real `timescaledb | None`, not `Any | None`. Runtime
# behavior is identical to the other three bridge getters — the walk of
# __transports is still duck-typed on type(t).__name__, no hard import
# of transports.timescaledb happens at module load — but timescaledb is
# additionally imported under TYPE_CHECKING (same idiom already used for
# BridgeAdminManager/WideTableField above), so a type checker can follow
# the cast() at the one point the duck-typing check proves it's safe,
# and every TimescaleDB call site below gets full static checking on
# bridge.rollup_mgr / bridge.hypertable_mgr / bridge.get_health_snapshot()
# etc. instead of falling through Any. Not extended to the mqtt/influxdb/
# prometheus getters here — out of scope for this file's TimescaleDB
# section; the same idiom would apply if those ever need it.

# Declared Any up front, before either branch assigns to it: this is what
# makes _field_manager() calling it not get flagged as "Object of type
# 'None' cannot be called". Without this, the type checker infers
# `type[BridgeAdminManager] | None` from the two conditional
# assignments below and won't treat it as callable — even though the
# `bridge is None` check in _field_manager() already guarantees this is
# never actually None by the time it's called.
_BridgeAdminManagerImpl: Any

try:
    from ...transports.timescaledb import (
        BridgeAdminManager as _BridgeAdminManagerImpl,
    )
    Timescale_Available = True
except ImportError:
    _log.debug("timescale_service: transports.timescaledb is not importable — the TimescaleDB admin UI will stay hidden.")
    _BridgeAdminManagerImpl = None
    Timescale_Available = False

# ---------------------------------------------------------------------------
# Live bridge discovery
# ---------------------------------------------------------------------------

def get_timescale_bridge(gateway: "Protocol_Gateway | None") -> "timescaledb | None":
    """
    Finds the live timescaledb bridge transport on the gateway, if any.

    Returns None if there's no gateway yet (startup race), no such
    transport is configured, or the timescaledb module isn't importable in
    this deployment. Duck-typed on the class name (rather than isinstance)
    so this module never needs a hard import of the transport class itself
    at runtime — the cast() below only affects static analysis (see
    `timescaledb` under the TYPE_CHECKING import above), not runtime
    behavior; it's safe precisely because the type(t).__name__ check just
    above it is what actually proves the identity at runtime.

    Mirrors analysis_service.get_scraper_transports()'s access pattern.
    """
    if gateway is None or not Timescale_Available:
        return None
    transports: list[transport_base] = getattr(gateway, "_Protocol_Gateway__transports", [])
    for t in transports:
        if type(t).__name__ == "timescaledb":
            return cast("timescaledb", t)
    return None


def is_timescale_available(gateway: "Protocol_Gateway | None") -> bool:
    """
    True when a live TimescaleDB bridge is attached to this gateway.
    Drives whether the "Timescale DB" nav pad is shown — see the
    `timescale_bridge_available()` Jinja global registered in main.py.
    """
    return get_timescale_bridge(gateway) is not None


def _get_live_transport(gateway: "Protocol_Gateway | None", protocol_name: str) -> transport_base | None:
    """
    Finds the live transport instance whose protocol_name matches, so its
    current (post variable_mask/variable_screen) registry_map can be
    compared against the columns already committed to the wide table.

    Duck-typed on the `protocol_name` property (see
    transport_base.protocol_name) rather than isinstance, mirroring
    get_timescale_bridge()'s walk of __transports. Returns None if the
    protocol has no live transport right now — a wide table can outlive
    its transport (e.g. config was edited to remove it, or it just hasn't
    connected yet), and that's a legitimate state, not an error.
    """
    if gateway is None:
        return None
    transports: list[transport_base] = getattr(gateway, "_Protocol_Gateway__transports", [])
    for t in transports:
        if getattr(t, "protocol_name", None) == protocol_name:
            return t
    return None


def _active_metric_names_for_protocol(gateway: "Protocol_Gateway | None", protocol_name: str) -> set[str] | None:
    """
    Returns the metric/variable names the live transport for
    protocol_name is currently configured to produce, via its
    variable_mask/variable_screen-filtered registry_map, plus any
    transport-declared synthetic fields (see
    transport_base.synthetic_fields_metadata). This is the same source
    timescaledb._extract_metric_names reads at schema-registration time.

    This is the "expected" side of the comparison
    timescaledb._validate_wide_row makes against a live scrape row
    (row_keys vs wide_columns) — used by list_wide_table_fields() to flag
    wide-table columns the current mask/screen config no longer produces,
    so the Delete Columns UI can highlight them for the admin instead of
    only surfacing the mismatch as a warning log line the next time data
    is scraped.

    Returns None — "unknown, don't flag anything" — if the protocol has
    no live transport attached right now, or that transport hasn't loaded
    a registry map yet. Callers must treat None as "no opinion", not as
    "everything is stale", since flagging every column red just because a
    transport hasn't connected yet would be misleading.
    """
    transport: transport_base | None = _get_live_transport(gateway, protocol_name)
    if transport is None:
        return None

    registry_map: dict[Registry_Type, list[registry_map_entry]] = getattr(transport, "registry_map", None) or {}
    if not registry_map:
        return None

    names: set[str] = set()
    for entries in registry_map.values():
        for entry in entries:
            variable_name: str | None = getattr(entry, "variable_name", None)
            if variable_name:
                names.add(variable_name)

    metadata: list[SyntheticFieldMetadata] = getattr(transport, "synthetic_fields_metadata", [])
    for synthetic in metadata:
        # synthetic is a (variable_name, data_type, unit_mod, note[, registry_type])
        # tuple — see protocol_service.SyntheticFieldMetadata / transport_base.
        # synthetic_fields_metadata.
        if synthetic:
            names.add(synthetic[0])

    return names


def _field_manager(gateway: "Protocol_Gateway | None") -> "BridgeAdminManager":
    bridge = get_timescale_bridge(gateway)
    if bridge is None:
        raise RuntimeError("No TimescaleDB bridge is attached to this gateway.")
    return _BridgeAdminManagerImpl(bridge)


# ---------------------------------------------------------------------------
# Read-only listings for the UI
# ---------------------------------------------------------------------------

def list_wide_tables(gateway: "Protocol_Gateway | None") -> list[dict[str, str]]:
    """
    Returns [{protocol_name, wide_table_name}, ...] for the wide-table
    picker screen (step 4 of the Delete Columns flow).
    """
    mgr: BridgeAdminManager = _field_manager(gateway)
    return [
        {"protocol_name": protocol_name, "wide_table_name": wide_table_name}
        for protocol_name, wide_table_name in mgr.list_editable_protocols()
    ]


def resolve_wide_table_name(gateway: "Protocol_Gateway | None", protocol_name: str) -> str:
    """Returns the wide_table_name for protocol_name. Raises ValueError if unknown/narrow-only."""
    mgr: BridgeAdminManager = _field_manager(gateway)
    return mgr.resolve_wide_table_name(protocol_name)


def list_wide_table_fields(
    gateway: "Protocol_Gateway | None",
    protocol_name: str,
    staged_columns: set[str] | None = None,
    ) -> list[dict[str, str | bool]]:
    """
    Returns the alpha-ordered field list (step 5 of the Delete Columns
    flow) for one protocol's wide table, each row annotated with:
      - `checked` reflecting the currently staged-for-deletion set — so the
        checklist re-renders with the right boxes ticked if the admin
        navigates away and comes back before committing.
      - `stale` flagging columns the protocol's live variable_mask/
        variable_screen config no longer produces (see
        _active_metric_names_for_protocol) — the same condition
        timescaledb._validate_wide_row logs as `fewer_keys` when it turns
        up in a scraped row, surfaced here proactively so the UI can
        render likely-deletable columns in a distinct color. Never True
        when the protocol has no live transport attached right now — see
        _active_metric_names_for_protocol for why that's "no opinion"
        rather than "everything is stale".
    """
    mgr: BridgeAdminManager = _field_manager(gateway)
    staged: set[str] = staged_columns or set()
    active_metric_names: set[str] | None = _active_metric_names_for_protocol(gateway, protocol_name)
    fields: list[WideTableField] = mgr.list_fields(protocol_name, active_metric_names=active_metric_names)
    # str(...) here isn't defensive filler — it's what makes the dict[str,
    # str | bool] return type actually true by construction, rather than an
    # assumed pass-through of whatever WideTableField.metric_name/
    # column_name/data_type happen to be declared as (this file doesn't
    # import that class at runtime — see the TYPE_CHECKING note above — so
    # their exact field types aren't something this function can assert
    # without guessing).
    return [
        {
            "metric_name": str(f.metric_name),
            "column_name": str(f.column_name),
            "data_type": str(f.data_type),
            "checked": f.column_name in staged,
            "stale": bool(f.stale),
        }
        for f in fields
    ]


# ---------------------------------------------------------------------------
# Rollup views — read-only inventory + on-demand full rebuild.
#
# Backs the "Timescale DB -> Rebuild Rollup Views" admin screen. Unlike the
# column deletions above, there's no staging step here: rollup views are
# always derived/recreatable from the wide/narrow tables (never a source of
# truth themselves), so a rebuild runs immediately on click rather than
# riding the app's "Commit All Changes" flow.
# ---------------------------------------------------------------------------

def list_rollup_views(gateway: "Protocol_Gateway | None") -> list[dict[str, Any]]:
    """
    Returns the rollup-view inventory (shared narrow stack + every wide
    protocol's hourly/daily/weekly/monthly stack) for the Rebuild Rollup
    Views screen. See RollupManager.list_rollup_views for the per-row shape.

    Raises RuntimeError if no bridge is attached, or the bridge is attached
    but hasn't finished connecting to TimescaleDB yet (rollup_mgr is None).
    """
    bridge: "timescaledb | None" = get_timescale_bridge(gateway)
    if bridge is None:
        raise RuntimeError("No TimescaleDB bridge is attached to this gateway.")
    if bridge.rollup_mgr is None:
        raise RuntimeError("Rollup manager is not initialized yet — the bridge is not connected to TimescaleDB.")
    return bridge.rollup_mgr.list_rollup_views()


def list_rollup_view_groups(gateway: "Protocol_Gateway | None") -> list[dict[str, Any]]:
    """
    Same inventory as list_rollup_views(), grouped into one entry per
    source table -- the shared narrow stack, plus one entry per wide-table
    protocol -- for the Rebuild Rollup Views screen's group checkboxes.

    Rebuilding is only ever offered at this per-source-table granularity,
    never per individual hourly/daily/weekly/monthly view, because the
    finer-grained views in a stack are hierarchically dependent on the
    coarser ones within that same stack -- see
    RollupManager.rebuild_all_rollups for why.

    Uses itertools.groupby rather than Jinja's `groupby` filter: the latter
    re-sorts by the grouping key first, which would separate "shared_narrow"
    from wherever it falls alphabetically among protocol names.
    itertools.groupby only merges already-consecutive equal keys, which
    preserves list_rollup_views()'s ordering (shared narrow stack first,
    then wide protocols alphabetically) instead.
    """
    flat: list[dict[str, Any]] = list_rollup_views(gateway)
    groups: list[dict[str, Any]] = []
    for protocol_name, rows_iter in itertools.groupby(flat, key=lambda r: r["protocol_name"]):
        rows: list[dict[str, Any]] = list(rows_iter)
        groups.append({
            "protocol_name": protocol_name,
            "wide_table_name": rows[0]["wide_table_name"],
            "views": rows,
        })
    return groups


def rebuild_all_rollups(
    gateway: "Protocol_Gateway | None",
    protocol_names: set[str] | None = None,
    force: bool = False
    ) -> Generator[dict[str, Any], None, None]:
    """
    Triggers an immediate rebuild pass across the selected rollup stack(s)
    on the live bridge. Called from the "Rebuild Rollups" / "Force Rebuild"
    buttons on the Rebuild Rollup Views screen (PATCH /api/timescale/
    rollups/rebuild).

    This is a GENERATOR wrapping RollupManager.rebuild_all_rollups -- same
    shape as rebuild_compression below, forwarding every event through
    UNCHANGED. There's no per-event annotation to add here the way
    rebuild_compression annotates its "done" event with size deltas; a
    rollup rebuild has nothing analogous to report per group beyond what
    RollupManager already includes (ok/error/skipped/changed), so this is
    a plain passthrough.

    Args:
        protocol_names: Which groups to act on -- protocol_name values as
            returned by list_rollup_view_groups(), plus "shared_narrow" for
            the shared narrow stack. None acts on every group.
        force: False ("Rebuild Rollups") only purges + re-materializes a
            group if it's actually out of date. True ("Force Rebuild")
            always purges + re-materializes every selected group.
        See RollupManager.rebuild_all_rollups for the full set of event
        shapes (including each group's `changed` flag) and why selection
        stops at the per-source-table granularity.

    Raises RuntimeError if no bridge is attached, or the bridge hasn't
    finished connecting to TimescaleDB yet. Because this function is
    itself a generator, that RuntimeError is only actually raised once the
    caller starts iterating it -- see rebuild_compression's docstring
    below for why routers/timescale.py's endpoint relies on that.
    """
    bridge: "timescaledb | None" = get_timescale_bridge(gateway)
    if bridge is None:
        raise RuntimeError("No TimescaleDB bridge is attached to this gateway.")
    if bridge.rollup_mgr is None:
        raise RuntimeError("Rollup manager is not initialized yet — the bridge is not connected to TimescaleDB.")
    for event in bridge.rollup_mgr.rebuild_all_rollups(protocol_names=protocol_names, force=force):
        yield event


def refresh_selected_rollups(
    gateway: "Protocol_Gateway | None",
    protocol_names: set[str] | None = None,
    force_full: bool = False
    ) -> Generator[dict[str, Any], None, None]:
    """
    Pulls the latest raw data into the selected rollup groups' existing
    views, without dropping or recreating anything. Called from the
    "Refresh Now" button on the Rebuild Rollup Views screen (PATCH
    /api/timescale/rollups/refresh) -- the lighter, non-structural
    counterpart to rebuild_all_rollups().

    This is a GENERATOR wrapping RollupManager.refresh_selected_rollups --
    same passthrough shape as rebuild_all_rollups() above.

    Args:
        protocol_names: Which groups to refresh -- protocol_name values as
            returned by list_rollup_view_groups(), plus "shared_narrow" for
            the shared narrow stack. None refreshes every group.
        force_full: False performs each view's normal incremental refresh.
            True refreshes each view's entire time range from scratch.
        See RollupManager.refresh_selected_rollups for the full set of
        event shapes.

    Raises RuntimeError if no bridge is attached, or the bridge hasn't
    finished connecting to TimescaleDB yet (surfaced on first iteration --
    see rebuild_all_rollups above).
    """
    bridge: "timescaledb | None" = get_timescale_bridge(gateway)
    if bridge is None:
        raise RuntimeError("No TimescaleDB bridge is attached to this gateway.")
    if bridge.rollup_mgr is None:
        raise RuntimeError("Rollup manager is not initialized yet — the bridge is not connected to TimescaleDB.")
    for event in bridge.rollup_mgr.refresh_selected_rollups(protocol_names=protocol_names, force_full=force_full):
        yield event


# ---------------------------------------------------------------------------
# Rebuild Compression — backs the "Timescale DB -> Rebuild Compression"
# admin screen, a sibling of Rebuild Rollup Views above using the exact
# same group keys (a wide-table protocol_name, plus "shared_narrow"). Unlike
# the rollup functions, this doesn't touch view definitions at all -- it
# decompresses and recompresses already-compressed chunks in place, so a
# compress_segmentby/compress_orderby change (or a Delete Columns edit)
# actually takes effect on historical data instead of only new chunks.
# ---------------------------------------------------------------------------

def list_compression_groups(gateway: "Protocol_Gateway | None") -> list[dict[str, Any]]:
    """
    Read-only compression inventory (one row per group, each carrying a
    `tables` breakdown of that group's raw table + all four rollup views'
    chunk counts, size, and raw/uncompressed size) for the Rebuild
    Compression screen's group checkboxes, with human-readable
    `size_display` / `raw_size_display` added to each group row AND each
    of its per-table rows.

    Raises RuntimeError if no bridge is attached, or the bridge is
    attached but hasn't finished connecting to TimescaleDB yet.
    """
    bridge: "timescaledb | None" = get_timescale_bridge(gateway)
    if bridge is None:
        raise RuntimeError("No TimescaleDB bridge is attached to this gateway.")
    if bridge.hypertable_mgr is None:
        raise RuntimeError("Hypertable manager is not initialized yet — the bridge is not connected to TimescaleDB.")
    rows: list[dict[str, Any]] = bridge.hypertable_mgr.list_compression_groups()
    for row in rows:
        row["size_display"] = _format_bytes(row.get("size_bytes"))
        row["raw_size_display"] = _format_bytes(row.get("raw_size_bytes"))
        for table_row in row.get("tables", []):
            table_row["size_display"] = _format_bytes(table_row.get("size_bytes"))
            table_row["raw_size_display"] = _format_bytes(table_row.get("raw_size_bytes"))
    return rows


def _annotate_compression_change(entry: dict[str, Any]) -> None:
    """
    Adds human-readable display fields to one before/after-size entry --
    either a group result or one of its `tables` rows from
    RollupManager.rebuild_compression -- in place: `size_before_display`,
    `size_after_display`, and `percent_reduced` (float, None when there's
    no size_before to compute a ratio against, e.g. a skipped group or an
    untouched table). Mirrors the size_display convention used elsewhere
    in this module (e.g. list_compression_groups) -- this is where display
    formatting belongs, not the transport layer or the frontend.
    """
    size_before: int = entry.get("size_before_bytes") or 0
    size_after: int = entry.get("size_after_bytes") or 0
    entry["size_before_display"] = _format_bytes(size_before)
    entry["size_after_display"] = _format_bytes(size_after)
    entry["percent_reduced"] = round((1 - (size_after / size_before)) * 100, 1) if size_before else None


def rebuild_compression(
    gateway: "Protocol_Gateway | None",
    protocol_names: set[str] | None = None
    ) -> Generator[dict[str, Any], None, None]:
    """
    Triggers an immediate decompress -> recompress pass across every
    already-compressed chunk of the selected group(s)' raw table and all
    four rollup views. Called from the "Rebuild Compression" button (PATCH
    /api/timescale/compression/rebuild), which streams every yielded
    event straight to the browser as it happens rather than waiting for
    the whole rebuild to finish.

    This is a GENERATOR wrapping RollupManager.rebuild_compression --
    forwards every event through UNCHANGED except the terminal
    {"type": "done"} event, whose result dict gets the same
    size_before_display/size_after_display/percent_reduced annotation
    (per group AND per table) this used to apply to the whole return
    value back when it returned once instead of streaming. See
    RollupManager.rebuild_compression for the full set of event shapes
    and the weighted-by-bytes reasoning behind "progress" events.

    Args:
        protocol_names: Which groups to act on -- protocol_name values as
            returned by list_compression_groups(), plus "shared_narrow"
            for the shared narrow stack. None acts on every group.

    Raises RuntimeError if no bridge is attached, or the bridge hasn't
    finished connecting to TimescaleDB yet. Because this function is
    itself a generator (it has a yield below), that RuntimeError is only
    actually raised once the caller starts iterating it (Python doesn't
    run any of a generator function's body, including this check, until
    the first `next()`) -- routers/timescale.py's endpoint iterates this
    inside a try/except specifically so that first iteration is where
    the error surfaces, before the streaming response has sent anything.
    """
    bridge: "timescaledb | None" = get_timescale_bridge(gateway)
    if bridge is None:
        raise RuntimeError("No TimescaleDB bridge is attached to this gateway.")
    if bridge.hypertable_mgr is None:
        raise RuntimeError("Hypertable manager is not initialized yet — the bridge is not connected to TimescaleDB.")

    for event in bridge.hypertable_mgr.rebuild_compression(protocol_names=protocol_names):
        if event.get("type") == "done":
            result: dict[str, Any] = event.get("result", {})
            for group in result.get("groups", []):
                _annotate_compression_change(group)
                for table_row in group.get("tables", []):
                    _annotate_compression_change(table_row)
        yield event


# ---------------------------------------------------------------------------
# Bridge info pane — read-only snapshots for the device page's "Bridge
# Health", "Storage Overview", "Compression & Retention Status", and
# "Background Job Status" panels (see routers/timescale.py GET /health,
# /storage, /compression-retention, /jobs). Purely observational: none of
# these mutate anything, unlike the rollup functions above.
# ---------------------------------------------------------------------------
def _format_dt(value: Any) -> str:
    """
    Human-readable timestamp for template display, e.g. '2026-07-24 09:15
    UTC'. Formatted here rather than in the template since this app has no
    established Jinja datetime filter to rely on. None -> '—'.
    """
    if value is None:
        return "—"
    try:
        return value.strftime("%Y-%m-%d %H:%M %Z").strip()
    except AttributeError:
        return str(value)


def get_timescale_health(gateway: "Protocol_Gateway | None") -> dict[str, Any]:
    """
    Read-only connection/background-worker snapshot for the "Bridge
    Health" panel. See BridgeAdminManager.get_health_snapshot for the
    field list.

    Raises RuntimeError if no bridge is attached. Unlike the rollup
    functions, this does NOT require rollup_mgr to be initialized — a
    bridge that's still connecting is itself a valid, useful thing to show
    on a health panel, so this returns whatever it can rather than erroring.
    """
    bridge: "timescaledb | None" = get_timescale_bridge(gateway)
    if bridge is None:
        raise RuntimeError("No TimescaleDB bridge is attached to this gateway.")
    return _field_manager(gateway).get_health_snapshot()


def get_storage_overview(gateway: "Protocol_Gateway | None") -> list[dict[str, Any]]:
    """
    Read-only per-source-table storage snapshot for the "Storage Overview"
    panel, with a human-readable `size_display` added to each row. See
    BridgeAdminManager.get_storage_overview for the rest of the field list.

    Raises RuntimeError if no bridge is attached. Returns an empty list
    (rather than raising) if the bridge is attached but not yet connected
    to TimescaleDB, since there's simply nothing to report yet.
    """
    bridge: "timescaledb | None" = get_timescale_bridge(gateway)
    if bridge is None:
        raise RuntimeError("No TimescaleDB bridge is attached to this gateway.")
    rows: list[dict[str, Any]] = _field_manager(gateway).get_storage_overview()
    for row in rows:
        row["size_display"] = _format_bytes(row.get("size_bytes"))
        row["oldest_display"] = _format_dt(row.get("oldest"))
        row["newest_display"] = _format_dt(row.get("newest"))
    return rows


def get_index_overview(gateway: "Protocol_Gateway | None") -> list[dict[str, Any]]:
    """
    Read-only per-index snapshot for the "Indexes" panel, with a human-
    readable `size_display` added to each row. See BridgeAdminManager.
    get_index_overview for the rest of the field list.

    Raises RuntimeError if no bridge is attached. Returns an empty list
    (rather than raising) if the bridge is attached but not yet connected
    to TimescaleDB, since there's simply nothing to report yet.
    """
    bridge: "timescaledb | None" = get_timescale_bridge(gateway)
    if bridge is None:
        raise RuntimeError("No TimescaleDB bridge is attached to this gateway.")
    rows: list[dict[str, Any]] = _field_manager(gateway).get_index_overview()
    for row in rows:
        row["size_display"] = _format_bytes(row.get("size_bytes"))
    return rows


def get_compression_retention_summary(gateway: "Protocol_Gateway | None") -> dict[str, Any]:
    """
    Read-only compression/retention configuration summary for the
    "Compression & Retention Status" panel, with `dynamic_raw_tables` and
    `dynamic_view_groups` lists merged in — see BridgeAdminManager.
    get_compression_retention_summary for the static-config field list
    plus the per-raw-table dynamic sizing rows (narrow plus every wide
    table, each with its own live-computed band, sourced from
    HyperTableManager.get_dynamic_raw_table_overview), and RollupManager.
    get_dynamic_view_overview for the per-VIEW rows (narrow's four rollup
    views plus every wide table's own four, one row per (table,
    granularity) pair since a view's load is granularity-specific — see
    that method's docstring).

    Raises RuntimeError if no bridge is attached, or the bridge hasn't
    finished connecting to TimescaleDB yet (this is config sourced from
    the hypertable/rollup managers' own attributes, not a live query, but
    neither manager exists until connected).
    """
    bridge: "timescaledb | None" = get_timescale_bridge(gateway)
    if bridge is None:
        raise RuntimeError("No TimescaleDB bridge is attached to this gateway.")
    if bridge.rollup_mgr is None or bridge.hypertable_mgr is None:
        raise RuntimeError("Hypertable/rollup managers are not initialized yet — the bridge is not connected to TimescaleDB.")
    summary: dict[str, Any] = _field_manager(gateway).get_compression_retention_summary()

    # Grouped by protocol for display (4 granularity rows per table) --
    # itertools.groupby only merges already-consecutive equal keys, not a
    # re-sort, which matters here the same way it does in list_rollup_
    # view_groups: it preserves get_dynamic_view_overview()'s narrow-first
    # ordering instead of alphabetizing "shared_narrow" in among protocol
    # names.
    flat_views: list[dict[str, Any]] = bridge.rollup_mgr.get_dynamic_view_overview()
    view_groups: list[dict[str, Any]] = []
    for protocol_name, rows_iter in itertools.groupby(flat_views, key=lambda r: r["protocol_name"]):
        view_groups.append({"protocol_name": protocol_name, "views": list(rows_iter)})
    summary["dynamic_view_groups"] = view_groups

    return summary


def get_background_jobs(gateway: "Protocol_Gateway | None") -> list[dict[str, Any]]:
    """
    Read-only snapshot of TimescaleDB's background scheduler jobs for
    every hypertable/view this bridge manages, for the "Background Job
    Status" panel. See BridgeAdminManager.get_background_jobs for the
    field list.

    Raises RuntimeError if no bridge is attached, or the bridge hasn't
    finished connecting to TimescaleDB yet.
    """
    bridge: "timescaledb | None" = get_timescale_bridge(gateway)
    if bridge is None:
        raise RuntimeError("No TimescaleDB bridge is attached to this gateway.")
    if bridge.rollup_mgr is None:
        raise RuntimeError("Rollup manager is not initialized yet — the bridge is not connected to TimescaleDB.")
    jobs: list[dict[str, Any]] = _field_manager(gateway).get_background_jobs()
    for job in jobs:
        job["last_successful_finish_display"] = _format_dt(job.get("last_successful_finish"))
        job["next_start_display"] = _format_dt(job.get("next_start"))
    return jobs


# ---------------------------------------------------------------------------
# Staging — in-memory, lives on app.state alongside the gateway.
#
# Shape: { protocol_name: { "wide_table_name": str,
#                            "columns": { column_name: data_type } } }
# ---------------------------------------------------------------------------

# One protocol's staged-for-deletion entry — fully local to this module
# (unlike the bridge-derived dicts above), so this can be precise rather
# than Any. A TypedDict rather than dict[str, str | dict[str, str]]
# specifically so entry["columns"] resolves to dict[str, str] on its own
# (supporting .pop()/.keys()/item assignment) instead of the whole
# str | dict[str, str] union every value would otherwise carry.
class StagedEntry(TypedDict):
    wide_table_name: str
    columns: dict[str, str]


def _store(app_state: State) -> dict[str, StagedEntry]:
    """Lazily initializes and returns the staging dict on app.state."""
    # Check if the attribute exists
    if not hasattr(app_state, "timescale_pending_deletions"):
        #Use setattr to dynamically apply it safely
        setattr(app_state, "timescale_pending_deletions", {})

    # Retrieve it via getattr to satisfy the static analyzer
    deletions: dict[str, StagedEntry] = getattr(app_state, "timescale_pending_deletions")
    return deletions


def _lock(app_state: State) -> threading.RLock:
    """Lazily initializes and returns the staging lock on app.state."""
    if not hasattr(app_state, "timescale_pending_lock"):
        app_state.timescale_pending_lock = threading.RLock()
    return app_state.timescale_pending_lock


def stage_field_deletion(
    app_state: State,
    protocol_name: str,
    wide_table_name: str,
    column_name: str,
    data_type: str,
    checked: bool,
) -> None:
    """
    Stages or un-stages a single field for deletion. Called on every
    checkbox toggle from the Delete Columns screen (PATCH
    /api/timescale/wide-tables/{protocol}/fields/{column}/stage).
    """
    with _lock(app_state):
        store: dict[str, StagedEntry] = _store(app_state)
        entry: StagedEntry | None = None
        if checked:
            entry = store.setdefault(
                protocol_name, {"wide_table_name": wide_table_name, "columns": {}}
            )
            entry["wide_table_name"] = wide_table_name
            entry["columns"][column_name] = data_type
        else:
            entry = store.get(protocol_name)
            if entry:
                entry["columns"].pop(column_name, None)
                if not entry["columns"]:
                    store.pop(protocol_name, None)


def get_staged_columns(app_state: State, protocol_name: str) -> set[str]:
    """Column names currently staged for deletion on one protocol's wide table."""
    with _lock(app_state):
        entry: StagedEntry | None = _store(app_state).get(protocol_name)
        return set(entry["columns"].keys()) if entry else set()


def get_all_staged(app_state: State) -> dict[str, StagedEntry]:
    """Returns a shallow copy of the full staged-deletions map, for a review/diff panel."""
    with _lock(app_state):
        return {
            protocol_name: {
                "wide_table_name": entry["wide_table_name"],
                "columns": dict(entry["columns"]),
            }
            for protocol_name, entry in _store(app_state).items()
        }


def has_staged_deletions(app_state: State) -> bool:
    """Drives the commit/discard buttons' lit-up state, alongside has_dirty_settings/has_dirty_protocols."""
    with _lock(app_state):
        return bool(_store(app_state))


def staged_deletion_count(app_state: State) -> int:
    """Total number of columns staged for deletion, across all protocols."""
    with _lock(app_state):
        return sum(len(entry["columns"]) for entry in _store(app_state).values())


def clear_staged_deletions(app_state: State) -> None:
    """Discards all staged deletions without touching the database. Wired into /api/commit/discard."""
    with _lock(app_state):
        _store(app_state).clear()


# ---------------------------------------------------------------------------
# Commit — actually performs the drops via BridgeAdminManager
# ---------------------------------------------------------------------------

def commit_staged_deletions(gateway: "Protocol_Gateway | None", app_state: State) -> list[dict[str, Any]]:
    """
    Executes every staged deletion against the live TimescaleDB bridge, one
    protocol at a time. Called from routers/commit.py's do_commit() as part
    of the global "Commit All Changes" flow.

    Each protocol that completes successfully is cleared from staging
    immediately, so a failure partway through does not re-offer
    already-applied deletions for retry on the next commit attempt. Any
    failure aborts the remaining protocols and re-raises so the caller's
    existing try/except turns it into a 500, matching do_commit()'s
    all-or-error behavior for the rest of the commit.

    Returns a list of per-protocol result summaries (successes only — the
    caller's except block handles the failure case).

    No-ops (returns []) if nothing is staged, without requiring a live
    bridge — so a commit with no pending column deletions never fails here
    even if TimescaleDB happens to be disconnected.
    """
    if not has_staged_deletions(app_state):
        return []

    mgr: BridgeAdminManager = _field_manager(gateway)
    staged: dict[str, StagedEntry] = get_all_staged(app_state)
    results: list[dict[str, Any]] = []

    with _lock(app_state):
        store: dict[str, StagedEntry] = _store(app_state)
        for protocol_name, entry in staged.items():
            column_names: list[str] = sorted(entry["columns"].keys())
            try:
                result: WideTableFieldDeletionResult = mgr.delete_fields(protocol_name, column_names)
            except Exception:
                _log.error(
                    "commit_staged_deletions: failed deleting %s from protocol '%s' — leaving it staged for retry.",
                    column_names, protocol_name,
                )
                raise
            else:
                store.pop(protocol_name, None)
                results.append({
                    "protocol_name": result.protocol_name,
                    "wide_table_name": result.wide_table_name,
                    "deleted": result.deleted,
                    "not_found": result.not_found,
                    "remaining_fields": result.remaining_fields,
                    "rollups_rebuilt": result.rollups_rebuilt,
                })

    _log.info(
        "commit_staged_deletions: committed %d protocol(s), %d column(s) total.",
        len(results), sum(len(r["deleted"]) for r in results),
    )
    return results


# ============================================================================
# INFLUXDB (v1 / v3)
# ============================================================================

def get_influxdb_bridge(gateway: "Protocol_Gateway | None", device_section: str) -> transport_base | None:
    """
    Finds the live influxdb_out / influxdb3_out bridge transport whose
    transport_name matches device_section (e.g. "transport.influxdb_out"),
    if any.

    Returns None if there's no gateway yet (startup race), or no such
    transport is configured under that name. Duck-typed on the class name
    (rather than isinstance) so this module never needs a hard import of
    either transport class, mirroring timescale_service.get_timescale_
    bridge()'s access pattern.
    """
    if gateway is None:
        return None
    transports: list[transport_base] = getattr(gateway, "_Protocol_Gateway__transports", [])
    for t in transports:
        if type(t).__name__ in ("influxdb_out", "influxdb3_out") and getattr(t, "transport_name", None) == device_section:
            return t
    return None


def is_influxdb_bridge(gateway: "Protocol_Gateway | None", device_section: str) -> bool:
    """True when an InfluxDB v1 or v3 bridge with this name is attached to the gateway."""
    return get_influxdb_bridge(gateway, device_section) is not None


def _format_elapsed(seconds: float) -> str:
    """Human-readable elapsed time, e.g. 125.0 -> '2m ago'. Negative/near-zero -> 'just now'."""
    if seconds < 5:
        return "just now"
    seconds_int: int = int(seconds)
    if seconds_int < 60:
        return f"{seconds_int}s ago"
    minutes: int = seconds_int // 60
    if minutes < 60:
        return f"{minutes}m ago"
    hours: int = minutes // 60
    if hours < 24:
        return f"{hours}h ago"
    days: int = hours // 24
    return f"{days}d ago"


def get_influxdb_health(gateway: "Protocol_Gateway | None", device_section: str) -> dict[str, Any]:
    """
    Read-only connection/backlog/staleness snapshot for the "Bridge
    Health" panel, with a human-readable `last_periodic_reconnect_display`
    added. See influxdb_out.get_health_snapshot (identical on influxdb3_
    out) for the rest of the field list.

    Raises RuntimeError if no InfluxDB bridge with this name is attached
    to the gateway.
    """
    bridge: Any | None = get_influxdb_bridge(gateway, device_section)
    if bridge is None:
        msg: str = f"No InfluxDB bridge named '{device_section}' is attached to this gateway."
        raise RuntimeError(msg)

    health: dict[str, Any] = bridge.get_health_snapshot()

    last_attempt: float = health.get("last_periodic_reconnect_attempt") or 0.0
    if last_attempt > 0:
        health["last_periodic_reconnect_display"] = _format_elapsed(time.time() - last_attempt)
    else:
        health["last_periodic_reconnect_display"] = "never"

    return health

def get_influxdb_storage(gateway: "Protocol_Gateway | None", device_section: str) -> dict[str, Any]:
    """
    Best-effort, read-only storage snapshot for the "Storage Overview"
    panel. See influxdb_out.get_storage_overview / influxdb3_out.
    get_storage_overview for the raw field list (the two line up so one
    template renders both). This adds display-only formatting on top:

    - `data_dir_size_display` — human-readable data_dir/object_store_dir size.
    - Each `table_stats` row gets `file_size_display` / `memory_display`
      (v3 only — `table_stats` is always empty on v1).
    - `columns_by_table` — the flat `columns` list (v3 only) grouped into
      {table_name: [{column_name, data_type, iox_column_type}, ...]} for
      per-table display, plus a `column_count` added onto each matching
      `table_stats` row.
    - `heap_profile.size_display` — human-readable raw profile size, when
      the heap-profile probe succeeded. This is still just the size of the
      undecoded binary profile, not a memory-usage figure — see
      influxdb3_out._probe_heap_profile for why nothing here decodes it
      into real allocation numbers.

    Raises RuntimeError if no InfluxDB bridge with this name is attached
    to the gateway.
    """
    bridge: Any | None = get_influxdb_bridge(gateway, device_section)
    if bridge is None:
        msg: str = f"No InfluxDB bridge named '{device_section}' is attached to this gateway."
        raise RuntimeError(msg)

    storage: dict[str, Any] = bridge.get_storage_overview()

    data_dir_size_bytes: int | None = storage.get("data_dir_size_bytes")
    storage["data_dir_size_display"] = (
        _format_bytes(data_dir_size_bytes) if data_dir_size_bytes is not None else None
    )

    # Group the flat schema-map rows by table, and fold a column count
    # onto each table_stats row so the panel can show it in one place.
    columns: list[dict[str, Any]] = storage.get("columns") or list[dict[str, Any]]()
    columns_by_table: dict[str, list[dict[str, Any]]] = {}
    for col in columns:
        table_name: str | None = col.get("table_name")
        if not table_name:
            continue
        columns_by_table.setdefault(table_name, list[dict[str, Any]]()).append(col)
    storage["columns_by_table"] = columns_by_table

    table_stats: list[dict[str, Any]] = storage.get("table_stats") or list[dict[str, Any]]()
    for row in table_stats:
        row["file_size_display"] = _format_bytes(row.get("file_size_bytes"))
        row["memory_display"] = _format_bytes(row.get("memory_bytes"))
        row_table_name: str | None = row.get("table_name")
        row["column_count"] = (
            len(columns_by_table.get(row_table_name, list[dict[str, Any]]())) if row_table_name else 0
        )

    heap_profile: dict[str, Any] | None = storage.get("heap_profile")
    if heap_profile is not None:
        heap_size_bytes: int | None = heap_profile.get("size_bytes")
        heap_profile["size_display"] = _format_bytes(heap_size_bytes) if heap_size_bytes is not None else None

    return storage


# ============================================================================
# MQTT
# ============================================================================

def get_mqtt_bridge(gateway: "Protocol_Gateway | None", device_section: str) -> transport_base | None:
    """
    Finds the live mqtt bridge transport whose transport_name matches
    device_section (e.g. "transport.mqtt"), if any.

    Returns None if there's no gateway yet (startup race), or no such
    transport is configured under that name. Duck-typed on the class name
    (rather than isinstance) so this module never needs a hard import of
    the mqtt transport class, mirroring timescale_service.get_timescale_
    bridge() / influxdb_service.get_influxdb_bridge()'s access pattern.
    """
    if gateway is None:
        return None
    transports: list[transport_base] = getattr(gateway, "_Protocol_Gateway__transports", [])
    for t in transports:
        if type(t).__name__ == "mqtt" and getattr(t, "transport_name", None) == device_section:
            return t
    return None


def is_mqtt_bridge(gateway: "Protocol_Gateway | None", device_section: str) -> bool:
    """True when an MQTT bridge with this name is attached to the gateway."""
    return get_mqtt_bridge(gateway, device_section) is not None


def get_mqtt_health(gateway: "Protocol_Gateway | None", device_section: str) -> dict[str, Any]:
    """
    Read-only connection/reconnect/write-topic snapshot for the "Bridge
    Health" panel. See mqtt.get_health_snapshot for the field list.

    Raises RuntimeError if no MQTT bridge with this name is attached to
    the gateway.
    """
    bridge: Any | None = get_mqtt_bridge(gateway, device_section)
    if bridge is None:
        msg: str = f"No MQTT bridge named '{device_section}' is attached to this gateway."
        raise RuntimeError(msg)
    return bridge.get_health_snapshot()


# ============================================================================
# PROMETHEUS
# ============================================================================

def get_prometheus_bridge(gateway: "Protocol_Gateway | None", device_section: str) -> transport_base | None:
    """
    Finds the live prometheus_out bridge transport whose transport_name
    matches device_section (e.g. "transport.prometheus_out"), if any.

    Returns None if there's no gateway yet (startup race), or no such
    transport is configured under that name. Duck-typed on the class name
    (rather than isinstance) so this module never needs a hard import of
    the prometheus_out transport class, mirroring get_influxdb_bridge() /
    get_mqtt_bridge()'s access pattern.
    """
    if gateway is None:
        return None
    transports: list[transport_base] = getattr(gateway, "_Protocol_Gateway__transports", [])
    for t in transports:
        if type(t).__name__ == "prometheus_out" and getattr(t, "transport_name", None) == device_section:
            return t
    return None


def is_prometheus_bridge(gateway: "Protocol_Gateway | None", device_section: str) -> bool:
    """True when a Prometheus bridge with this name is attached to the gateway."""
    return get_prometheus_bridge(gateway, device_section) is not None


def _format_duration(seconds: float | None) -> str:
    """
    Human-readable plain duration for the bridge summary's "Uptime" field,
    e.g. 3725.0 -> '1h 2m'. Deliberately distinct from _format_elapsed
    above (which appends "ago" and is meant for "time since an event") --
    an uptime figure reads oddly as "1h 2m ago". None/0/negative -> '—'.
    """
    if not seconds or seconds < 0:
        return "—"
    total_seconds: int = int(seconds)
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, _ = divmod(remainder, 60)
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m"
    return f"{total_seconds}s"


def get_prometheus_health(gateway: "Protocol_Gateway | None", device_section: str) -> dict[str, Any]:
    """
    Read-only in-memory-registry summary for the "Bridge Health" panel —
    metrics registered, mount path, and machine counts by connectivity
    bucket. See prometheus_out.get_bridge_summary for the raw field list;
    this adds a human-readable `uptime_display`.

    Raises RuntimeError if no Prometheus bridge with this name is attached
    to the gateway.
    """
    bridge: Any | None = get_prometheus_bridge(gateway, device_section)
    if bridge is None:
        msg: str = f"No Prometheus bridge named '{device_section}' is attached to this gateway."
        raise RuntimeError(msg)
    summary: dict[str, Any] = bridge.get_bridge_summary()
    summary["uptime_display"] = _format_duration(summary.get("uptime_seconds"))
    return summary


def get_prometheus_targets(gateway: "Protocol_Gateway | None", device_section: str) -> list[dict[str, Any]]:
    """
    Read-only per-machine scrape-target snapshot for the "Target Health"
    panel — one row per machine this bridge has ever been wired to or
    received data from. See prometheus_out.get_target_health for the raw
    field list; this adds `seconds_since_last_scrape` and a human-readable
    `last_scrape_display` (reusing _format_elapsed from the INFLUXDB
    section above — "time since an event" is exactly what it's for, and
    duplicating it here would just be the same function twice).

    Raises RuntimeError if no Prometheus bridge with this name is attached
    to the gateway.
    """
    bridge: Any | None = get_prometheus_bridge(gateway, device_section)
    if bridge is None:
        msg: str = f"No Prometheus bridge named '{device_section}' is attached to this gateway."
        raise RuntimeError(msg)

    rows: list[dict[str, Any]] = bridge.get_target_health()
    now: float = time.time()
    for row in rows:
        last_ts: float | None = row.get("last_scrape_timestamp")
        if last_ts:
            elapsed: float = now - last_ts
            row["seconds_since_last_scrape"] = elapsed
            row["last_scrape_display"] = _format_elapsed(elapsed)
        else:
            row["seconds_since_last_scrape"] = None
            row["last_scrape_display"] = "never"
    return rows


# ---------------------------------------------------------------------------
# BRIDGE DELETION — "Delete Bridge" button on bridge_panes.html
# ---------------------------------------------------------------------------
#
# Unlike every other function in this file, this one touches the staging
# DB (Setting rows) rather than a live bridge/gateway object — deleting a
# bridge is a config-shape edit, not a runtime introspection. It lives
# here rather than in device_service.py because the caller
# (routers/bridges.py) is the bridge-specific router, and this is the
# bridge-specific half of a two-sided relationship: the other half —
# ensure_bridge_sections_exist(), which *creates* a bridge section the
# first time a scraper's "bridge" multi-select references one that
# doesn't exist yet — lives in device_service.py because *its* caller is
# devices.py's update_setting(). Same relationship, opposite direction,
# each paired with the router that triggers it.

class BridgeDeletionResult(TypedDict):
    section: str                  # "transport.<bridge_name>" that was removed
    deleted_keys: int             # number of Setting rows removed for it
    updated_scrapers: list[str]   # scraper device names whose "bridge" value changed


def delete_bridge(db: "Session", bridge_name: str) -> BridgeDeletionResult:
    """
    Stages the deletion of a bridge's [transport.<bridge_name>] section —
    and, in the same transaction, strips any reference to that bridge from
    every scraper's "bridge" setting (the comma-separated
    "transport.<name>, ..." multi-select value written by the bridge
    picker in scraper_panes.html; see device_service.ensure_bridge_sections_exist
    for the create-time half of this relationship).

    This is a SOFT delete — every Setting row in the section is marked
    is_active=False (with is_dirty set so it's correctly counted as a
    pending change; see below), not removed from the DB outright. That's
    a deliberate departure from device_service.delete_orphan()'s hard
    db.delete(): an orphan is dead weight nothing depends on, but a
    bridge section might currently be sitting on disk with real values
    that need to be actively removed from config.cfg — is_active=False
    is this app's established way of staging "this key should no longer
    be in the config" (see routers/devices.py's update_setting()/
    update_device_setting() for the identical per-key convention this
    mirrors, and config_writer._reset_dirty_flags(), which already
    performs the disk-side half of this contract on every commit:  for
    an inactive+dirty row, it blanks value_disk/value_staged to "" and
    clears is_dirty, reflecting that the key is now confirmed gone from
    config.cfg). config_writer._build_config_text() already skips any
    row where is_active is False when grouping rows into sections (see
    that function's docstring), so once every row in this section is
    inactive, the section simply stops appearing in config.cfg — header
    included — the next time "Commit" runs. Like every other function in
    this module, this doesn't touch config.cfg (or any other file)
    directly; the disk write stays commit_all()'s job.

    Each row's is_dirty is computed the same way update_device_setting()
    computes it when deactivating a key — bool(value_disk), not forced
    True unconditionally — so a bridge that was staged but never
    committed (value_disk still empty for every key) produces zero dirty
    rows on delete: removing something that was never actually on disk
    isn't a change disk needs to know about.

    Because these rows are deactivated rather than removed, the caller
    is also responsible for making sure a soft-deleted bridge stops
    appearing as a live device — see device_service.get_nav_data() and
    get_device_summary(), both of which now require at least one active
    row per section before treating it as an existing device. Without
    that half of the fix, a soft-deleted bridge would still show up in
    the nav/device page (since its rows still physically exist), even
    though it's correctly staged for removal on next commit.

    The scraper-side "bridge" values are edited by removing just the
    deleted bridge's "transport.<bridge_name>" entry from the comma list
    and re-staging the remainder (mark_dirty()), the same partial-edit
    approach update_setting() uses for every other staged field — the
    scraper's other bridge references (if it fans out to more than one)
    and every other setting on that scraper are left untouched.

    Raises ValueError if:
      - no [transport.<bridge_name>] section exists at all, or
      - a section by that name exists but isn't a bridge (transport_type
        not = to "bridge") — e.g. the name of a scraper. Callers
        (routers/bridges.py) turn either case into an HTTP error rather
        than silently deleting the wrong kind of section.
    """
    from ..models import Setting  # noqa: PLC0415 — see module docstring: this
    # file otherwise has zero DB dependency, so the import is scoped to this
    # one function rather than added to the module's always-run imports.

    section: str = f"transport.{bridge_name}"

    rows: list["Setting"] = (
        db.query(Setting).filter(Setting.section == section).all()
    )
    if not rows:
        msg: str = f"No bridge section '{section}' exists."
        _log.warning("delete_bridge: %s", msg)
        raise ValueError(msg)
    if rows[0].transport_type != "bridge":
        msg: str = f"Section '{section}' is not a bridge."
        _log.warning("delete_bridge: %s", msg)
        raise ValueError(msg)

    # Soft-delete: mark every row inactive rather than db.delete()-ing it.
    # This matters for one specific case — a bridge whose section header
    # exists in config.cfg with few or no explicit keys under it, relying
    # entirely on its transport class's code-level defaults. Such a row
    # can easily have value_staged == value_disk (nothing ever staged an
    # edit to it), so a hard delete here removes it from the DB with zero
    # trace that a change is now pending: refresh_app_state()'s dirty
    # count (services this file doesn't otherwise touch — see
    # database.refresh_app_state()) is a COUNT of Setting.is_dirty rows,
    # and a row that's simply gone can never be counted, so "Commit All
    # Changes" would never light up and the deletion would never reach
    # config.cfg — exactly the bug this replaced.
    #
    # is_active=False + is_dirty=<computed> is the established mechanism
    # for exactly this ("a key is being intentionally removed") — see
    # routers/devices.py's update_setting()/update_device_setting(), and
    # config_writer._reset_dirty_flags()'s handling of inactive rows,
    # which already does the disk-side half of this contract (clears
    # value_disk/value_staged to "" and un-dirties once the removal is
    # actually committed). Deleting an entire bridge is just this same
    # per-key mechanism applied to every row in its section at once.
    #
    # Dirty is computed the same way devices.py does it, not forced to
    # True unconditionally: a row that never had a value on disk in the
    # first place (value_disk falsy — e.g. a bridge staged-but-never-
    # committed, then deleted before ever reaching config.cfg) removing
    # it changes nothing on disk, so it shouldn't count as a pending
    # change requiring a commit either.
    deleted_keys: int = len(rows)
    for row in rows:
        row.is_active = False
        row.is_dirty = bool(row.value_disk)

    # Strip this bridge from every scraper's "bridge" multi-select value so
    # a commit doesn't leave a dangling "bridge = transport.<bridge_name>"
    # reference pointing at a section that no longer exists.
    reference: str = section
    updated_scrapers: list[str] = []

    bridge_refs: list["Setting"] = (
        db.query(Setting)
        .filter(Setting.transport_type == "scraper", Setting.key == "bridge")
        .all()
    )
    for setting in bridge_refs:
        current_value: str | None = (
            setting.value_staged if setting.value_staged is not None else setting.value_disk
        )
        if not current_value:
            continue

        parts: list[str] = [p.strip() for p in current_value.split(",") if p.strip()]
        if reference not in parts:
            continue

        remaining: list[str] = [p for p in parts if p != reference]
        setting.value_staged = ", ".join(remaining)
        setting.mark_dirty()
        updated_scrapers.append(setting.section.removeprefix("transport."))

    _log.info(
        "delete_bridge: removed '%s' (%d keys); cleared reference from scraper(s): %s",
        section, deleted_keys, ", ".join(updated_scrapers) or "(none)",
    )

    return {
        "section": section,
        "deleted_keys": deleted_keys,
        "updated_scrapers": updated_scrapers,
    }
