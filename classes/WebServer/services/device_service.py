# Description: services/device_service.py — Queries the staging DB for device/transport data used to build the navigation menus and device panes.
# File: device_service.py
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
services/device_service.py — Queries the staging DB for device/transport data
used to build the navigation menus and device panes.
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import AppState, Setting
from ..scanner import TransportLibraryEntry, scan_transport_library
from ..transport_registry import get_known_transport_keys

if sys.version_info >= (3, 12):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict

_log: logging.Logger = logging.getLogger(__name__)


class TransportLibraryRow(TypedDict):
    """Shape of each row returned by get_transport_library()."""
    name: str
    classification: str
    key_count: int
    sample_keys: list[str]
    all_keys: list[str]


@dataclass
class DeviceSummary:
    name: str
    section: str
    transport_type: str      # "scraper" | "bridge" | "general"
    transport_class: str     # e.g. "modbus_tcp", "mqtt"
    protocol_version: str    # "" for bridges
    host: str
    port: str
    is_connected: bool = False   # runtime status — set by gateway if available
    # For a scraper: the bridge device_names it writes to (parsed from its
    # "bridge" setting). For a bridge: the scraper device_names that write
    # to it (the reverse of the above, computed in get_nav_data()).
    linked_bridges: list[str] = field(default_factory=list[str])
    linked_scrapers: list[str] = field(default_factory=list[str])


@dataclass
class NavData:
    scrapers: list[DeviceSummary]
    bridges: list[DeviceSummary]
    protocol_groups: list[str]


def _parse_bridge_names(bridge_value: str) -> list[str]:
    """
    Parse a scraper's raw "bridge" setting value — a comma-separated list of
    "transport.<name>" entries (see scraper_panes.html's bridge multi-select)
    — into a clean, ordered list of bridge device_names, dropping any blank
    or malformed entries. Shared by get_nav_data() (dashboard display) and
    ensure_bridge_sections_exist() (staging DB seeding).
    """
    if not bridge_value:
        return []
    names: list[str] = []
    for part in bridge_value.split(","):
        part = part.strip()
        if part.startswith("transport."):
            name = part.removeprefix("transport.")
            if name:
                names.append(name)
    return names


def get_nav_data(db: Session) -> NavData:
    """
    Returns all the data needed to render the three nav dropdowns.
    """
    scrapers: list[DeviceSummary] = []
    bridges: list[DeviceSummary] = []

    # Find all transport sections that still have at least one active row.
    # The is_active filter matters for a section every one of whose rows
    # has been soft-deleted (see bridge_service.delete_bridge(), which
    # marks a bridge's rows is_active=False rather than removing them, so
    # the pending removal can still be counted as dirty for the Commit
    # button) — without it, such a section's row(s) still physically
    # exist, so it would keep showing up here as a live device even
    # though it's staged for removal on the next commit. This does NOT
    # affect sections where only SOME rows are inactive (e.g. Prometheus
    # bridges' dashboard-only derived host/port rows, is_active=False by
    # design — see scanner.py's prometheus_out handling) since those
    # sections still have other active rows and are found here as before.
    sections: Sequence[str] = (
        db.execute(
            select(Setting.section)
            .where(Setting.section.like("transport.%"), Setting.is_active == True)  # noqa: E712
            .distinct()
        )
        .scalars()
        .all()
    )

    # scraper device_name -> list of bridge device_names it writes to,
    # keyed off each scraper's raw "bridge" setting value (a comma-separated
    # list of "transport.<name>" entries — same format written by the
    # bridge multi-select in scraper_panes.html and read by
    # ensure_bridge_sections_exist() above).
    bridge_names_by_scraper: dict[str, list[str]] = {}

    for section in sorted(sections):
        device_name: str = section.removeprefix("transport.")
        keys: dict[str, str] = _get_section_keys(db, section)

        transport_class: str = keys.get("transport", "")
        protocol_version: str = keys.get("protocol_version", "")
        transport_type: str = keys.get("transport_type_cached", "general")

        linked_bridges: list[str] = _parse_bridge_names(keys.get("bridge", ""))
        if transport_type == "scraper":
            bridge_names_by_scraper[device_name] = linked_bridges

        summary = DeviceSummary(
            name=device_name,
            section=section,
            transport_type=transport_type,
            transport_class=transport_class,
            protocol_version=protocol_version,
            host=keys.get("host", ""),
            port=keys.get("port", ""),
            linked_bridges=linked_bridges,
        )

        if transport_type == "scraper":
            scrapers.append(summary)
        elif transport_type == "bridge":
            bridges.append(summary)

    # Reverse the scraper -> bridges mapping so each bridge in the dashboard
    # can list every scraper that feeds it (the "Scrapers" column on the
    # Bridges table — see index.html).
    scraper_names_by_bridge: dict[str, list[str]] = {}
    for scraper_name, linked in bridge_names_by_scraper.items():
        for bridge_name in linked:
            scraper_names_by_bridge.setdefault(bridge_name, []).append(scraper_name)
    for bridge_summary in bridges:
        bridge_summary.linked_scrapers = sorted(
            scraper_names_by_bridge.get(bridge_summary.name, [])
        )

    # Protocol groups from ProtocolRegister table
    from ..models import ProtocolRegister
    groups: Sequence[str] = (
        db.execute(
            select(ProtocolRegister.protocol_group).distinct()
        )
        .scalars()
        .all()
    )

    return NavData(
        scrapers=scrapers,
        bridges=bridges,
        protocol_groups=sorted(groups),
    )


def get_device_settings(db: Session, section: str) -> list[Setting]:
    """Return all Setting rows for a device section, ordered by key."""
    return (
        db.query(Setting)
        .filter(Setting.section == section)
        .order_by(Setting.key)
        .all()
    )


def ensure_bridge_sections_exist(db: Session, bridge_value: str | None) -> list[str]:
    """
    Given a staged "bridge" setting value — a comma-separated list of
    "transport.<name>" entries, as written by the bridge multi-select (see
    scraper_panes.html) — create the DB rows for any referenced bridge
    section that doesn't exist yet, seeded from that transport class's
    resolved defaults in transport_defaults.json.

    Without this, picking a bridge that has never been configured before
    (no existing [transport.<name>] section) stages a "bridge = ...,
    transport.<name>, ..." reference on the scraper side that points at
    nothing: config_writer.commit_all() only ever writes sections it finds
    rows for (see _group_settings there), so the referenced bridge section
    would silently never appear in config.cfg even though the scraper's
    bridge= line names it. Called from update_setting() in devices.py
    whenever the patched row is the "bridge" key.

    Only creates rows for sections that don't already exist at all — an
    existing bridge (however it got there: prior manual creation, a scan
    of a hand-edited config.cfg, etc.) is left untouched.

    Returns the list of newly-created bridge section names (device_name,
    not the full "transport.<name>" section string), empty if nothing
    needed creating, so the caller can log/report what happened.
    """
    if not bridge_value:
        return []

    known: dict[str, dict[str, str]] = get_known_transport_keys()
    created: list[str] = []

    for part in bridge_value.split(","):
        part = part.strip()
        if not part.startswith("transport."):
            continue
        bridge_name: str = part.removeprefix("transport.")
        if not bridge_name:
            continue

        section: str = f"transport.{bridge_name}"
        exists: bool = (
            db.query(Setting.id).filter(Setting.section == section).first()
            is not None
        )
        if exists:
            continue

        defaults: dict[str, str] | None = known.get(bridge_name)
        if defaults is None:
            # Not a recognized transport class (stale value, or a bridge
            # module that's since been removed from the transports dir) —
            # nothing to seed. Leave the dangling reference as-is; the diff
            # panel / orphan detection is the right place to surface that,
            # not a guess made here.
            _log.warning(
                "ensure_bridge_sections_exist: '%s' is not a known transport "
                "class — leaving section unseeded", bridge_name
            )
            continue

        # Most bridge entries in transport_defaults.json don't $extends
        # "_base" (only scraper-style transports generally do), so "transport"
        # usually isn't among their resolved default keys at all. Set it
        # explicitly to the class name being instantiated regardless — same
        # as new-scraper creation does — so get_nav_data()'s transport_class
        # lookup and the config.cfg output both have it.
        seed: dict[str, str] = {**defaults, "transport": bridge_name}

        for key, default_value in seed.items():
            row = Setting(
                section=section,
                key=key,
                value_disk=None,
                value_staged=default_value,
                default_value=default_value,
                transport_type="bridge",
                is_active=True,
            )
            row.mark_dirty()
            db.add(row)

        created.append(bridge_name)
        _log.info(
            "ensure_bridge_sections_exist: created new bridge section '%s' "
            "(%d keys) from transport defaults", section, len(seed)
        )

    return created


def get_device_summary(db: Session, device_name: str) -> DeviceSummary | None:
    section: str = f"transport.{device_name}"

    # Guard the section on "has at least one active row" the same way
    # get_nav_data() now does, and for the same reason: a soft-deleted
    # bridge (bridge_service.delete_bridge() — every row is_active=False,
    # pending removal on next commit) should 404 here just like it's
    # hidden from nav, even though its rows still physically exist in the
    # DB. _get_section_keys() below is deliberately left reading every
    # row regardless of is_active — see its own docstring — so this check
    # has to happen here rather than by filtering that function.
    has_active_row: bool = (
        db.query(Setting.id)
        .filter(Setting.section == section, Setting.is_active == True)  # noqa: E712
        .first()
        is not None
    )
    if not has_active_row:
        return None

    keys: dict[str, str] = _get_section_keys(db, section)
    if not keys:
        return None
    return DeviceSummary(
        name=device_name,
        section=section,
        transport_type=keys.get("transport_type_cached", "general"),
        transport_class=keys.get("transport", ""),
        protocol_version=keys.get("protocol_version", ""),
        host=keys.get("host", ""),
        port=keys.get("port", ""),
    )


def _get_section_keys(db: Session, section: str) -> dict[str, str]:
    """
    Deliberately reads every Setting row for this section regardless of
    is_active — do not add an is_active filter here. scanner.py's
    prometheus_out handling relies on this: it stores that bridge's
    dashboard-only derived host/port as is_active=False rows (they have
    no config.cfg representation to begin with — see the long comment
    in scanner.py above that code), and this function is how the
    dashboard reads them back. Filtering to active-only here would make
    those two values disappear from the dashboard.

    Callers that need to know whether the SECTION ITSELF still counts as
    an existing device (as opposed to reading whichever values it has)
    — get_nav_data() and get_device_summary() — do that check
    themselves, on "does this section have at least one active row",
    before calling this function; see their own comments for why.
    """
    rows: List[Setting] = db.query(Setting).filter(Setting.section == section).all()
    result: dict[str, str] = {}
    for row in rows:
        result[row.key] = row.value_staged or row.value_disk or ""
        result["transport_type_cached"] = row.transport_type
    return result


def get_transport_library(transports_dir: Path) -> list[TransportLibraryRow]:
    """
    Returns the transport library list for the TRANSPORT LIBRARY page.
    Each entry has: name, classification, keys.
    """
    library: dict[str, TransportLibraryEntry] = scan_transport_library(transports_dir)
    result: list[TransportLibraryRow] = []
    for name, info in sorted(library.items()):
        all_keys: list[str] = list(info["keys"].keys())
        result.append({
            "name": name,
            "classification": info["classification"],
            "key_count": len(all_keys),
            "sample_keys": all_keys[:5],
            "all_keys": all_keys,
        })
    return result


def get_app_state(db: Session) -> AppState:
    state: AppState | None = db.get(AppState, 1)
    if state is None:
        from ..database import ensure_app_state
        state = ensure_app_state(db)
    return state


def get_orphaned_settings(db: Session) -> list[Setting]:
    return (
        db.query(Setting)
        .filter(Setting.is_orphan == True)  # noqa: E712
        .order_by(Setting.section, Setting.key)
        .all()
    )


def delete_orphan(db: Session, setting_id: int) -> bool:
    row: Setting | None = db.get(Setting, setting_id)
    if row and row.is_orphan:
        db.delete(row)
        db.commit()
        return True
    return False


def delete_orphans_bulk(db: Session, setting_ids: list[int]) -> int:
    count = 0
    for sid in setting_ids:
        if delete_orphan(db, sid):
            count += 1
    return count
