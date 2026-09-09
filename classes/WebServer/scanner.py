# Description: scanner.py — The authoritative source-of-truth builder for the staging DB.
# File: scanner.py
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
scanner.py — The authoritative source-of-truth builder for the staging DB.

Runs on startup and whenever the file watcher fires.  Reads config.cfg and
the transports folder, then upserts Setting and ProtocolRegister rows using
a merge strategy:

  Merge:  New keys from disk are added to the DB with value_staged = value_disk.
          Existing rows keep their value_staged (user edits survive a rescan).
  Orphan: Keys in the DB that are no longer found in source are flagged
          is_orphan = True (not deleted — user must manually prune them).
"""

from __future__ import annotations

import ast
import csv
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, List, Literal

from sqlalchemy.orm import Session

from .database import refresh_app_state, session_scope
from .models import (
    AppState,
    DeviceProtocolSelection,
    OrphanedFilterName,
    ProtocolRegister,
    Setting,
)
from .transport_registry import (
    TransportLibraryEntry,
    get_known_transport_keys,
    get_transport_base_keys,
    sync_from_library,
)

_log: logging.Logger = logging.getLogger(__name__)


EMPTY_TRANSPORT_ENTRY: TransportLibraryEntry = {"classification": "", "keys": {}, "file": ""}

# ---------------------------------------------------------------------------
# Module-level alias kept for backwards compatibility.
# pages.py and any other callers that do:
#   from ..scanner import TRANSPORT_BASE_KEYS
# will still work without modification.
# ---------------------------------------------------------------------------
TRANSPORT_BASE_KEYS: dict[str, str] = get_transport_base_keys()

# The WebServer's own default port -- see
# classes.WebServer.main.start_webserver()'s `port` parameter. Used as the
# fallback for a prometheus_out section's derived, display-only `port` when
# that bridge's own metrics_port is unset. There's no config-based way to
# override the WebServer's actual port today, so this constant and that
# parameter's default must be kept in sync by hand if that ever changes.
PROMETHEUS_OUT_DEFAULT_PORT: int = 1717


# ---------------------------------------------------------------------------
# Config parser  (reuses the CustomConfigParser from protocol_gateway)
# ---------------------------------------------------------------------------

def load_config(config_path: Path) -> dict[str, dict[str, str]]:
    """
    Parse config.cfg using the same CustomConfigParser as the gateway.
    Returns {section: {key: value}}.
    Falls back to stdlib ConfigParser if the import fails.
    """
    try:
        # Add project root to path so we can import from protocol_gateway
        root: Path = config_path.parent
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        from protocol_gateway import CustomConfigParser
        parser = CustomConfigParser()
    except ImportError:
        from configparser import ConfigParser
        parser = ConfigParser()

    parser.read(str(config_path))
    removed_keys: set[str] = {"analyze_protocol", "analyze_protocol_save_load"}
    result: dict[str, dict[str, str]] = {}
    for section in parser.sections():
        result[section] = {}
        for key, value in parser.items(section):
            if key in removed_keys:
                continue
            # Strip inline comments
            value: str = value.split("#")[0].strip()
            result[section][key] = value
    return result


# ---------------------------------------------------------------------------
# Transport classification
# ---------------------------------------------------------------------------

def _classify_transport(section: str, keys: dict[str, str], transports_dir: Path) -> str:
    """
    Returns "scraper", "bridge", or "general".

    Classification is declared by the transport class itself via its
    class-level transport_type attribute. This avoids relying on comments
    while still keeping the scanner import-free.
    """
    transport_type: str = keys.get("transport", "").strip()
    if transport_type:
        py_file: Path = transports_dir / f"{transport_type}.py"
        classification: str = _transport_type_from_ast(py_file)
        if classification in ("scraper", "bridge"):
            return classification

    return "general"


def _transport_type_from_ast(py_file: Path) -> str:
    """
    Read a transport module's class-level transport_type value without
    importing the module. Importing transport modules can touch hardware,
    network clients, or optional database libraries, so AST parsing is the
    safest source of truth for WebServer discovery.
    """
    if not py_file.exists():
        return "base class"

    try:
        source: str = py_file.read_text(encoding="utf-8")
        tree: ast.Module = ast.parse(source)
    except Exception as exc:
        _log.warning(f"AST transport_type parse failed for {py_file.name}: {exc}")
        return "base class"

    valid_types: set[str] = {"scraper", "bridge", "base class", "general"}
    module_stem: str = py_file.stem
    discovered: list[tuple[str, str]] = []

    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        for stmt in node.body:
            target_name = ""
            value_node: ast.AST | None = None
            if isinstance(stmt, ast.Assign):
                for target in stmt.targets:
                    if isinstance(target, ast.Name) and target.id == "transport_type":
                        target_name = target.id
                        value_node = stmt.value
                        break
            elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                if stmt.target.id == "transport_type":
                    target_name = stmt.target.id
                    value_node = stmt.value

            if target_name and isinstance(value_node, ast.Constant) and isinstance(value_node.value, str):
                value: str = value_node.value.strip().lower()
                if value in valid_types:
                    discovered.append((node.name, value))

    for class_name, value in discovered:
        if class_name == module_stem:
            return value

    if discovered:
        return discovered[0][1]

    return "base class"


# ---------------------------------------------------------------------------
# AST settings-proxy scanner
# ---------------------------------------------------------------------------

def _extract_class_attr_defaults(tree: ast.Module) -> dict[str, str]:
    """
    Scan a parsed module for class-level and __init__ constant assignments
    so that self.attr references in settings.get(fallback=self.attr) can
    be resolved without importing the module.

    Handles:
        class Foo:
            host: str = "localhost"   # AnnAssign
            batch_size = 100          # Assign

        def __init__(self, ...):
            self.reconnect_attempts = 5   # instance assign before settings.get
    """
    attrs: dict[str, str] = {}

    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue

        # Class-level attribute assignments
        for stmt in node.body:
            if isinstance(stmt, ast.Assign):
                for target in stmt.targets:
                    if (isinstance(target, ast.Name)
                            and isinstance(stmt.value, ast.Constant)
                            and stmt.value.value is not None):
                        attrs[target.id] = str(stmt.value.value)
            elif isinstance(stmt, ast.AnnAssign):
                if (isinstance(stmt.target, ast.Name)
                        and stmt.value is not None
                        and isinstance(stmt.value, ast.Constant)
                        and stmt.value.value is not None):
                    attrs[stmt.target.id] = str(stmt.value.value)

        # __init__ bare self.attr = <constant> before settings.get() calls
        for stmt in node.body:
            if not (isinstance(stmt, ast.FunctionDef) and stmt.name == "__init__"):
                continue
            for inner in ast.walk(stmt):
                if not isinstance(inner, ast.Assign):
                    continue
                for target in inner.targets:
                    if (isinstance(target, ast.Attribute)
                            and isinstance(target.value, ast.Name)
                            and target.value.id == "self"
                            and isinstance(inner.value, ast.Constant)
                            and inner.value.value is not None):
                        # Only set if not already present — first assignment wins
                        if target.attr not in attrs:
                            attrs[target.attr] = str(inner.value.value)

    return attrs


def _extract_settings_keys_from_ast(py_path: Path) -> dict[str, str | None]:
    """
    Walk the AST of a transport .py file and find all patterns like:
        settings.get("key", ...)
        settings.getint("key", fallback=...)
        settings.getfloat("key", ...)
        settings.getboolean("key", ...)

    Returns {key: default_value_or_None}.

    Resolution order for the default value:
      1. Literal constant in the call:  settings.get("key", "default")
      2. self.attr reference resolved via class body or __init__ assignment:
             self.batch_size = 100
             settings.getint("batch_size", fallback=self.batch_size)  → "100"
      3. None — no default resolvable; callers fall through to JSON registry.
    """
    found: dict[str, str | None] = {}
    try:
        source: str = py_path.read_text(encoding="utf-8")
        tree: ast.Module = ast.parse(source)
    except Exception as exc:
        _log.warning(f"AST parse failed for {py_path.name}: {exc}")
        return found

    # Build attr-name → default map for resolving self.attr fallbacks
    class_attrs: dict[str, str] = _extract_class_attr_defaults(tree)

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func_node: ast.expr = node.func
        # Match: settings.get / settings.getint / settings.getfloat / settings.getboolean
        if not (isinstance(func_node, ast.Attribute)
                and func_node.attr in ("get", "getint", "getfloat", "getboolean")
                and isinstance(func_node.value, ast.Name)
                and func_node.value.id == "settings"):
            continue

        # First positional arg is the key (may be a string or a list)
        if not node.args:
            continue
        first_arg: ast.expr = node.args[0]

        keys_to_add: list[str] = []
        if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
            keys_to_add.append(first_arg.value)
        elif isinstance(first_arg, ast.List):
            for elt in first_arg.elts:
                if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                    keys_to_add.append(elt.value)

        # Extract fallback — literal constant, self.attr lookup, or None
        default_val: str | None = None
        for kw in node.keywords:
            if kw.arg == "fallback":
                if isinstance(kw.value, ast.Constant):
                    default_val = str(kw.value.value)
                elif (isinstance(kw.value, ast.Attribute)
                        and isinstance(kw.value.value, ast.Name)
                        and kw.value.value.id == "self"):
                    default_val = class_attrs.get(kw.value.attr)  # None if unresolvable
                break
        # Positional arg[1] fallback
        if default_val is None and len(node.args) >= 2:
            arg2 = node.args[1]
            if isinstance(arg2, ast.Constant):
                default_val = str(arg2.value)
            elif (isinstance(arg2, ast.Attribute)
                    and isinstance(arg2.value, ast.Name)
                    and arg2.value.id == "self"):
                default_val = class_attrs.get(arg2.attr)

        for k in keys_to_add:
            if k not in found:
                found[k] = default_val

    return found


def scan_transport_library(transports_dir: Path) -> dict[str, TransportLibraryEntry]:
    """
    Scan all .py files in the transports directory.
    Returns {filename_stem: {classification, keys: {key: default}}}.

    After scanning, calls sync_from_library() so transport_defaults.json and
    setting_descriptions.json are automatically updated with any newly
    discovered transports or keys.
    """
    result: dict[str, TransportLibraryEntry] = {}
    if not transports_dir.exists():
        _log.warning(f"Transports directory not found: {transports_dir}")
        return result

    known_transport_keys: dict[str, dict[str, str]] = get_known_transport_keys()
    transport_base_keys: dict[str, str] = get_transport_base_keys()

    for py_file in sorted(transports_dir.glob("*.py")):
        if py_file.name.startswith("__"):
            continue
        stem: str = py_file.stem

        classification: str = _transport_type_from_ast(py_file)

        # Keys from AST scan of this file.
        # Values are str | None — None means the settings.get() call had no
        # default argument in source code.
        ast_keys: dict[str, str | None] = _extract_settings_keys_from_ast(py_file)

        if classification == "bridge":
            # Bridges: only expose the keys they explicitly read via settings.get(...).
            # Do not inject scraper-oriented base keys (protocol_version, read_interval,
            # variable_mask, device_location, bridge, analyze_protocol, etc.).
            merged: dict[str, str | None] = ast_keys
        else:
            # Scrapers and base classes: start with the JSON registry defaults, then
            # overlay AST values — but only where the AST found an explicit default.
            # A None AST value means settings.get("key") had no second argument;
            # in that case the JSON registry default is the better value to keep.
            known_keys: dict[str, str] = known_transport_keys.get(stem, transport_base_keys)
            merged = dict(known_keys)  # start with full JSON defaults (all str)
            for k, v in ast_keys.items():
                if v is not None:
                    merged[k] = v   # AST has an explicit default — it wins
                elif k not in merged:
                    merged[k] = ""  # new key, no default anywhere

        result[stem] = {
            "classification": classification,
            "keys": merged,
            "file": str(py_file),
        }

    # Keep the JSON registry files current with whatever the AST found.
    # New transports and keys are added automatically; existing entries are
    # never overwritten so hand-edited defaults and descriptions survive.
    sync_from_library(result)

    return result


# ---------------------------------------------------------------------------
# Protocol scanner
# ---------------------------------------------------------------------------

def scan_protocols_dir(protocols_dir: Path) -> list[dict[str, Any]]:
    """
    Scan the protocols directory.
    Structure expected:  protocols/<group>/<protocol>.csv|json

    Returns a flat list of register dicts ready for DB upsert.
    """
    registers: list[dict[str, Any]] = []
    if not protocols_dir.exists():
        _log.warning(f"Protocols directory not found: {protocols_dir}")
        return registers

    for group_dir in sorted(protocols_dir.iterdir()):
        if not group_dir.is_dir():
            continue
        group_name: str = group_dir.name

        for proto_file in sorted(group_dir.iterdir()):
            if proto_file.suffix.lower() == ".csv":
                regs: List[dict[str, Any]] = _parse_protocol_csv(proto_file, group_name)
                registers.extend(regs)
            elif proto_file.suffix.lower() == ".json" and not proto_file.name.endswith(".override.json"):
                regs = _parse_protocol_json(proto_file, group_name)
                registers.extend(regs)

    _log.info(f"Protocol scan found {len(registers)} register entries across all protocols.")
    return registers


def _parse_protocol_csv(csv_path: Path, group_name: str) -> list[dict[str, Any]]:
    """
    Parse a protocol CSV file into a list of register dicts.

    Robustness rules
    ----------------
    * Delimiter auto-detected (comma vs semicolon).
    * Quoted fields handled by csv.reader so embedded commas in note/values
      columns do not corrupt the register address column.
    * Duplicate register addresses within the same file are deduplicated:
      the last row for a given address wins (matches protocol_settings.py
      behavior where later definitions override earlier ones).
    * Column names are normalized to snake_case and matched by multiple
      possible spellings (e.g. "Variable Name" / "variable name" / "variable_name").
    """
    protocol_name: str = csv_path.stem  # e.g. "eg4_18kpv_holding"

    # Detect registry type from filename. Runtime uses
    # <protocol>.registry_map.csv for Registry_Type.ZERO, which the UI labels
    # as "other".
    name_lower: str = protocol_name.lower()
    if "holding" in name_lower:
        registry_type = "holding"
    elif "input" in name_lower:
        registry_type = "input"
    elif "coil" in name_lower:
        registry_type = "coil"
    elif "discrete" in name_lower:
        registry_type = "discrete"
    else:
        registry_type = "other"

    # Ordered dict so later rows overwrite earlier ones for the same address.
    # This silently resolves duplicate rows in source CSVs (like the PF_S case).
    rows_by_address: dict[str, dict[str, Any]] = {}

    try:
        with open(csv_path, newline="", encoding="latin-1") as f:
            # ── Detect delimiter ──────────────────────────────────────────
            sample: str = f.read(4096)
            f.seek(0)
            delimiter: Literal[';'] | Literal[','] = ";" if sample.count(";") > sample.count(",") else ","

            reader: csv.DictReader[str] = csv.DictReader(
                f,
                delimiter=delimiter,
                quotechar='"',
                quoting=csv.QUOTE_MINIMAL,
                skipinitialspace=True,
            )
            if reader.fieldnames is None:
                _log.warning(f"CSV has no header row: {csv_path}")
                return []

            # ── Normalize header names ────────────────────────────────────
            # Maps original header → normalized snake_case key
            def _norm(s: str) -> str:
                return s.strip().lower().replace(" ", "_").replace("/", "_")

            norm_fields: dict[str, str] = {k: _norm(k) for k in reader.fieldnames if k}

            # Canonical field aliases — maps normalized header → canonical key
            ALIASES: dict[str, str] = {
                # variable name spellings
                "variable_name":  "variable_name",
                "variable":       "variable_name",
                "var_name":       "variable_name",
                "varname":        "variable_name",
                # documented name
                "documented_name": "documented_name",
                "documented":      "documented_name",
                "doc_name":        "documented_name",
                "name":            "documented_name",
                # register
                "register":        "register",
                "reg":             "register",
                "address":         "register",
                "register_address":"register",
                # unit
                "unit":            "unit",
                "units":           "unit",
                # data type
                "data_type":       "data_type",
                "datatype":        "data_type",
                "type":            "data_type",
                # values / range
                "values":          "values",
                "value":           "values",
                "range":           "values",
                "values_range":    "values",
                # adjustments
                "adjustments":     "adjustments",
                # note
                "note":            "note",
                "notes":           "note",
                "description":     "note",
                "desc":            "note",
                # write mode
                "writable":        "write_mode",
                "write":           "write_mode",
                "r_w":             "write_mode",
                "r/w":             "write_mode",
                "write_mode":      "write_mode",
                # read interval
                "read_interval":   "read_interval",
                "read interval":   "read_interval",
                "interval":        "read_interval",
            }

            # Build final header → canonical mapping for this file
            header_to_canonical: dict[str, str] = {}
            for original, normed in norm_fields.items():
                canonical: str = ALIASES.get(normed, normed)
                header_to_canonical[original] = canonical

            # ── Parse rows ────────────────────────────────────────────────
            for raw_row in reader:
                # Map all headers to canonical names, strip whitespace
                row: dict[str, str] = {}
                for orig_key, value in raw_row.items():
                    if orig_key is None:
                        continue
                    canonical = header_to_canonical.get(orig_key, _norm(orig_key))
                    row[canonical] = (value or "").strip()

                register: str = row.get("register", "").strip()
                var_name: str = row.get("variable_name", "").strip()
                doc_name: str = row.get("documented_name", "").strip()

                # Skip empty, header-repeat, or comment rows
                if not register:
                    continue
                if not (var_name or doc_name):
                    continue
                if var_name.startswith("#") or doc_name.startswith("#"):
                    continue
                # Skip rows where register is literally the header word
                # (happens when CSV has a repeated header mid-file)
                if register.lower() in ("register", "reg", "address"):
                    continue

                clean_var: str = (var_name or doc_name).strip().lower().replace(" ", "_")

                write_mode: str = row.get("write_mode", "R").strip().upper() or "R"

                entry: dict[str, str] = {
                    "protocol_group":       group_name,
                    "protocol_name":        protocol_name,
                    "registry_type":        registry_type,
                    "register_address":     register,
                    "variable_name":        clean_var,
                    "documented_name":      doc_name or var_name,
                    "unit":                 row.get("unit", ""),
                    "data_type":            row.get("data_type", ""),
                    "values_range":         row.get("values", ""),
                    "adjustments":          row.get("adjustments", ""),
                    "note":                 row.get("note", ""),
                    "read_interval":        row.get("read_interval", ""),
                    "write_mode_protocol":  write_mode,
                }

                # Last definition for a given address wins — resolves CSV duplicates
                rows_by_address[register] = entry

    except Exception as exc:
        _log.error(f"Error parsing protocol CSV {csv_path}: {exc}", exc_info=True)

    # ── Merge consecutive _l / _h register pairs ─────────────────────────
    # Mirrors the same merge logic in protocol_settings.load__registry so
    # the DB stores logical stem names rather than raw half-register names.
    #
    # Detection rule (identical to runtime):
    #   row N  variable_name ends with "_l"
    #   row N+1 variable_name == row N's stem + "_h"
    #   and both rows have consecutive integer addresses
    #
    # The _l row is promoted to the combined entry (stem name, lower addr).
    # The _h row is removed — it will not be upserted into ProtocolRegister.
    # The combined row gains a "paired_high_address" key so the UI can render
    # the address range (e.g. "40–41") and show the expand/collapse detail.

    result = list(rows_by_address.values())

    merged_count: int = 0
    i: int = 0
    while i < len(result) - 1:
        low: dict[str, Any]  = result[i]
        high: dict[str, Any] = result[i + 1]
        low_var:  str = low["variable_name"]
        high_var: str = high["variable_name"]

        # Both addresses must be parseable integers (not bit-field addresses like "5.b0")
        try:
            low_addr:  int = int(low["register_address"])
            high_addr: int = int(high["register_address"])
        except (ValueError, TypeError):
            i += 1
            continue

        if (
            low_var.endswith("_l")
            and high_var == low_var[:-2] + "_h"
        ):
            # Addresses need not be consecutive — the new "low_high" format
            # encodes the actual addresses so non-contiguous pairs are valid.
            stem: str = low_var[:-2]  # strip "_l"

            # Promote _l row to the combined stem entry.
            # register_address becomes "low_high" (e.g. "40_41") — the first
            # token is the low word address, the second is the high word address.
            # Non-contiguous pairs ("40_50") and reversed-order pairs ("41_40")
            # are representable by this same format.  The runtime decoder reads
            # the address list directly — no consecutive-address assumption.
            # paired_high_address is kept separately for the UI expand display.
            low["register_address"]    = f"{low_addr}_{high_addr}"
            low["paired_high_address"] = str(high_addr)
            low["variable_name"]       = stem
            low["documented_name"]     = (low["documented_name"][:-2].strip()
                                          if low["documented_name"].endswith("_l")
                                          else low["documented_name"])

            # Inherit missing metadata from the _h row
            if not low.get("unit") and high.get("unit"):
                low["unit"]        = high["unit"]
            if not low.get("data_type") or low.get("data_type") in ("", "USHORT"):
                if high.get("data_type") and high["data_type"] != "USHORT":
                    low["data_type"] = high["data_type"]
                else:
                    low["data_type"] = "UINT"
            if not low.get("adjustments") and high.get("adjustments"):
                low["adjustments"] = high["adjustments"]
            if not low.get("note") and high.get("note"):
                low["note"] = high["note"]

            # Remove the _h row — it has no independent DB existence
            del result[i + 1]
            merged_count += 1
            # Don't advance i — the next row is now at i+1 and might itself
            # be the _l half of another pair (unlikely but safe)
        else:
            i += 1

    if merged_count:
        _log.debug(
            f"Merged {merged_count} _l/_h register pair(s) into combined "
            f"stem entries in {csv_path.name}"
        )

    # Ensure every row has the paired_high_address key (None for non-paired rows)
    for row in result:
        row.setdefault("paired_high_address", "")

    _log.debug(f"Parsed {len(result)} logical registers from {csv_path.name} "
               f"({merged_count} merged pairs)")
    return result


def _parse_protocol_json(json_path: Path, group_name: str) -> list[dict[str, Any]]:
    """Parse a protocol JSON file. Returns a minimal single-row entry per JSON."""
    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
        # JSON files are config, not register maps — represent as one entry
        return [{
            "protocol_group": group_name,
            "protocol_name": json_path.stem,
            "registry_type": "json",
            "register_address": "0",
            "variable_name": "_json_config",
            "documented_name": json_path.stem,
            "unit": "",
            "data_type": "json",
            "values_range": "",
            "adjustments": f"JSON config file: {len(data)} keys",
            "note": f"JSON config file: {len(data)} keys",
            "read_interval": "",
            "write_mode_protocol": "R",
        }]
    except Exception as exc:
        _log.error(f"Error parsing protocol JSON {json_path}: {exc}")
        return []


# ---------------------------------------------------------------------------
# DB upsert helpers
# ---------------------------------------------------------------------------

def _upsert_setting(
    db: Session,
    section: str,
    key: str,
    value_disk: str,
    transport_type: str,
    default_value: str | None = None,
    is_active: bool = True,
    cfg_is_truth: bool = False,
) -> Setting:
    """
    Upsert a Setting row using merge strategy:
    - If row exists: update value_disk, recompute is_dirty, leave value_staged alone.
    - If row is new: value_staged = value_disk (no staged edit yet).
    - If cfg_is_truth=True (startup scan when config changed, or post-rollback):
      also sync value_staged = value_disk so config is ground truth with no stale edits.

    default_value uses None as a sentinel meaning "caller has no default to offer;
    leave whatever is already stored in the DB".  An explicit "" means the transport
    genuinely has no default for this key and the DB should reflect that.
    This is important because _get_default() returns None (not "") when the registry
    has no entry for a key — so existing stored defaults are never overwritten by
    a missing registry lookup.
    """
    existing: Setting | None = (
        db.query(Setting)
        .filter(Setting.section == section, Setting.key == key)
        .first()
    )

    if existing:
        existing.value_disk = value_disk
        # Only update stored default when the caller explicitly provides one.
        # None means "no default known" — leave the existing DB value untouched.
        # "" means the transport genuinely has no default — update accordingly.
        if default_value is not None:
            existing.default_value = default_value
        existing.transport_type = transport_type
        existing.is_orphan = False
        existing.is_active = is_active
        if cfg_is_truth:
            existing.value_staged = value_disk
            existing.is_dirty = False
        else:
            existing.mark_dirty()
    else:
        existing = Setting(
            section=section,
            key=key,
            value_disk=value_disk,
            value_staged=value_disk,
            default_value=default_value if default_value is not None else "",
            transport_type=transport_type,
            is_active=is_active,
            is_dirty=False,
            is_orphan=False,
        )
        db.add(existing)

    return existing


def _mark_orphaned_settings(db: Session, seen_keys: set[tuple[str, str]]) -> int:
    """
    Mark any Setting rows not in seen_keys as orphaned.
    Returns count of newly orphaned rows.
    """
    count = 0
    for row in db.query(Setting).all():
        key_tuple: tuple[str, str] = (row.section, row.key)
        if key_tuple not in seen_keys:
            if not row.is_orphan:
                row.is_orphan = True
                count += 1
    return count


def _upsert_protocol_register(db: Session, reg: dict[str, Any]) -> ProtocolRegister | None:
    """
    Upsert a ProtocolRegister row using a check-then-insert/update pattern.

    - If a matching row exists: update protocol fields, preserve user toggles.
    - If no match: insert a new row.
    - Uses a nested savepoint so a constraint error on one row does NOT roll
      back the entire scan session — the bad row is skipped with a warning.

    Returns the upserted row, or None if the row was skipped due to an error.
    """
    # Check-first approach avoids hitting the UNIQUE constraint on INSERT.
    # The constraint can still fire in a concurrent scenario, so we also
    # catch IntegrityError inside a savepoint as a backstop.
    existing: ProtocolRegister | None = (
        db.query(ProtocolRegister)
        .filter(
            ProtocolRegister.protocol_name == reg["protocol_name"],
            ProtocolRegister.registry_type == reg["registry_type"],
            ProtocolRegister.register_address == reg["register_address"],
        )
        .first()
    )

    if existing:
        # Row found — update protocol-sourced fields only, preserve user toggles
        existing.protocol_group    = reg["protocol_group"]
        if not existing.is_dirty:
            existing.variable_name       = reg["variable_name"]
            existing.documented_name     = reg["documented_name"]
            existing.unit                = reg.get("unit", "")
            existing.data_type           = reg.get("data_type", "")
            existing.values_range        = reg.get("values_range", "")
            existing.adjustments         = reg.get("adjustments", "")
            existing.note                = reg.get("note", "")
            existing.read_interval       = reg.get("read_interval", "")
            existing.write_mode_protocol = reg["write_mode_protocol"]
        # paired_high_address is structural metadata — always keep current from CSV.
        existing.paired_high_address = reg.get("paired_high_address")
        return existing

    # Row not found — attempt INSERT inside a savepoint so a race-condition
    # duplicate (or any other IntegrityError) skips only this row.
    try:
        with db.begin_nested():   # SQLAlchemy SAVEPOINT
            new_row = ProtocolRegister(
                protocol_group         = reg["protocol_group"],
                protocol_name          = reg["protocol_name"],
                registry_type          = reg["registry_type"],
                register_address       = reg["register_address"],
                variable_name          = reg["variable_name"],
                documented_name        = reg["documented_name"],
                unit                   = reg.get("unit", ""),
                data_type              = reg.get("data_type", ""),
                values_range           = reg.get("values_range", ""),
                adjustments            = reg.get("adjustments", ""),
                note                   = reg.get("note", ""),
                read_interval          = reg.get("read_interval", ""),
                write_mode_protocol    = reg["write_mode_protocol"],
                paired_high_address    = reg.get("paired_high_address"),
                is_dirty                 = False,
            )
            db.add(new_row)
            db.flush()   # flush inside savepoint — raises here on constraint error

    except Exception as exc:
        # Savepoint automatically rolled back; outer transaction is still alive.
        _log.warning(
            f"Skipping register {reg['protocol_name']}/{reg['registry_type']}"
            f"@{reg['register_address']} ({reg.get('variable_name','?')}): {exc}"
        )
        return None

    else:
        return new_row


def _load_filter_names(path: Path) -> set[str]:
    """Load a line-delimited filter file and return a normalized set of metric names.

    Half-register suffix normalization
    ------------------------------------
    Since _parse_protocol_csv now merges _l/_h pairs into a single stem entry,
    the variable_name stored in ProtocolRegister is the stem (e.g. "echg_all"),
    not "echg_all_l".  Filter files written before this change — or by users who
    selected the _l register name in the UI — will contain the old suffix form.
    Stripping _l/_h here ensures the set still matches the stem names in the DB.
    """
    if not path.exists():
        return set()
    names: set[str] = set()
    try:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            name: str = line.strip().lower().replace(" ", "_")
            if not name:
                continue
            # Normalize half-register names to their stem so they match the
            # merged ProtocolRegister.variable_name (e.g. "echg_all_l" → "echg_all")
            if name.endswith("_l") or name.endswith("_h"):
                name = name[:-2]
            names.add(name)
    except Exception as exc:
        _log.warning(f"Could not read filter file {path}: {exc}")
    return names


def _load_writable_names(
    device_name: str,
    config_dir: Path | None = None,
) -> set[str]:
    """
    Load write-enabled register names from the device's writable CSV.

    Keyed by device_name, not protocol_name — write-enable selections are
    per-device (DeviceProtocolSelection.device_name), so two devices sharing
    the same protocol can have different write-enabled registers (e.g. only
    one of a pair of identical inverters wired up for remote control). A
    protocol-scoped file couldn't represent that at all, since every device
    on that protocol would share the same file and therefore the same
    write-enabled set.

    Only checks config_dir (where commit_all writes it via
    config_writer._write_writable_csv) — unlike the old protocol-scoped
    version, there's no protocols_dir fallback here, since protocols_dir is
    organized by protocol name, not device name, so a per-device file has no
    sensible home there.

    """
    if config_dir is None:
        return set()

    writable_path: Path = config_dir / f"{device_name}.writable.csv"
    if not writable_path.exists():
        return set()

    _log.debug(f"Loading writable names from config_dir: {writable_path}")

    names: set[str] = set()
    try:
        with open(writable_path, newline="", encoding="utf-8", errors="replace") as f:
            reader: csv.DictReader[str] = csv.DictReader(f)
            for row in reader:
                value: str = (row.get("documented name") or row.get("variable_name") or "").strip()
                if value:
                    names.add(value.lower().replace(" ", "_"))
    except Exception as exc:
        _log.warning(f"Could not read writable-names file {writable_path}: {exc}")
    return names


def _upsert_device_protocol_selection(
    db: Session,
    device_name: str,
    protocol_row: ProtocolRegister,
    mask_names: set[str],
    screen_names: set[str],
    write_names: set[str],
) -> None:
    existing: DeviceProtocolSelection | None = (
        db.query(DeviceProtocolSelection)
        .filter(
            DeviceProtocolSelection.device_name == device_name,
            DeviceProtocolSelection.protocol_name == protocol_row.protocol_name,
            DeviceProtocolSelection.registry_type == protocol_row.registry_type,
            DeviceProtocolSelection.register_address == protocol_row.register_address,
        )
        .first()
    )

    var_key: str = protocol_row.variable_name.strip().lower().replace(" ", "_")
    doc_key: str = protocol_row.documented_name.strip().lower().replace(" ", "_")
    mask_enabled: bool = var_key in mask_names or doc_key in mask_names
    screen_enabled: bool = var_key in screen_names or doc_key in screen_names
    user_write_enabled: bool = var_key in write_names or doc_key in write_names

    if existing:
        existing.mask_enabled_disk = mask_enabled
        existing.screen_enabled_disk = screen_enabled
        existing.user_write_enabled_disk = user_write_enabled
        if not existing.is_dirty:
            existing.mask_enabled = mask_enabled
            existing.screen_enabled = screen_enabled
            existing.user_write_enabled = user_write_enabled
        existing.mark_dirty()
        return

    row = DeviceProtocolSelection(
        device_name=device_name,
        protocol_name=protocol_row.protocol_name,
        registry_type=protocol_row.registry_type,
        register_address=protocol_row.register_address,
        user_write_enabled=user_write_enabled,
        mask_enabled=mask_enabled,
        screen_enabled=screen_enabled,
        user_write_enabled_disk=user_write_enabled,
        mask_enabled_disk=mask_enabled,
        screen_enabled_disk=screen_enabled,
        is_dirty=False,
    )
    db.add(row)


def _sync_device_protocol_selections(
    db: Session,
    config_data: dict[str, dict[str, str]],
    project_root: Path,
) -> None:
    config_dir: Path = project_root / "config"

    for section, keys in config_data.items():
        if not section.startswith("transport."):
            continue

        device_name: str = section.removeprefix("transport.")
        protocol_version: str = keys.get("protocol_version", "").strip()
        if not protocol_version:
            continue

        mask_file: str = keys.get("variable_mask", f"variable_mask_{device_name}.txt").strip() or f"variable_mask_{device_name}.txt"
        screen_file: str = keys.get("variable_screen", f"variable_screen_{device_name}.txt").strip() or f"variable_screen_{device_name}.txt"

        mask_names: set[str] = _load_filter_names(config_dir / mask_file)
        screen_names: set[str] = _load_filter_names(config_dir / screen_file)

        write_names: set[str] = set()
        device_write_enabled: bool = keys.get("write_enabled", "false").strip().lower() == "true"
        if device_write_enabled:
            # Keyed by device_name, not protocol_version — write-enable is a
            # per-device decision (DeviceProtocolSelection.device_name), not a
            # per-protocol one. Two inverters on the same protocol can (and
            # often should) have different write selections, e.g. only one of
            # a pair of identical inverters actually wired up for remote
            # control. A single {protocol_version}.writable.csv shared by
            # every device on that protocol couldn't represent that at all —
            # it used to loop over every protocol_name matching this
            # protocol_version and union their (protocol-scoped) writable
            # files together, which meant every device sharing a protocol
            # necessarily shared the same write-enabled set too.
            write_names = _load_writable_names(device_name, config_dir=config_dir)

        protocol_rows: List[ProtocolRegister] = (
            db.query(ProtocolRegister)
            .filter(ProtocolRegister.protocol_name.like(f"{protocol_version}%"))
            .all()
        )
        for protocol_row in protocol_rows:
            _upsert_device_protocol_selection(
                db,
                device_name,
                protocol_row,
                mask_names,
                screen_names,
                write_names,
            )

        _sync_orphaned_filter_names(
            db, device_name, mask_file, mask_names, screen_file, screen_names, protocol_rows,
        )


def _sync_orphaned_filter_names(
    db: Session,
    device_name: str,
    mask_file: str,
    mask_names: set[str],
    screen_file: str,
    screen_names: set[str],
    protocol_rows: List[ProtocolRegister],
) -> None:
    """
    Cross-check every name in a device's mask/screen file against the
    device's *actual, current* registry (every variable_name/documented_name
    across all its ProtocolRegister rows, any registry type).

    _upsert_device_protocol_selection only ever asks "is this known register
    in the filter file?" — it has no way to notice a filter-file line that
    matches nothing at all (e.g. a register renamed or removed from the
    protocol CSV since the file was written). That's exactly the case that
    silently zeroes out an entire registry type in protocol_settings.py's
    screen/mask application (registry_map.clear()) — a single stale line
    can take down every metric for a device with no error anywhere.

    This is deliberately just a WARNING log, not a toggle: an orphaned
    filter-file entry is unambiguously stale (it cannot ever match
    anything), so there's no legitimate case where suppressing this
    warning is correct.
    """
    known_names: set[str] = set()
    for row in protocol_rows:
        known_names.add(row.variable_name.strip().lower())
        known_names.add(row.documented_name.strip().lower())

    orphaned_mask: set[str] = {n for n in mask_names if n not in known_names} if mask_names else set()
    orphaned_screen: set[str] = {n for n in screen_names if n not in known_names} if screen_names else set()

    if orphaned_mask:
        _log.warning(
            f"[{device_name}] {mask_file} contains {len(orphaned_mask)} name(s) that "
            f"don't match ANY currently known register for this device's protocol: "
            f"{sorted(orphaned_mask)}. These entries can never match anything and are "
            f"most likely stale (e.g. a register renamed/removed in a protocol update). "
            f"If this mask is meant to allow-list specific registers, verify these names "
            f"against the current protocol CSV."
        )
    if orphaned_screen:
        _log.warning(
            f"[{device_name}] {screen_file} contains {len(orphaned_screen)} name(s) that "
            f"don't match ANY currently known register for this device's protocol: "
            f"{sorted(orphaned_screen)}. Since screen is applied per registry type, if "
            f"NONE of a type's registers are in this file, that ENTIRE type gets excluded "
            f"from scraping (see protocol_settings.py) — a stale name here can silently "
            f"zero out all data for a registry type. Verify these names against the "
            f"current protocol CSV and remove/update them if they're leftover from a "
            f"prior protocol version."
        )

    # Persist so the web UI can surface this without anyone needing to go
    # looking through logs. Delete-then-insert per device: these rows are
    # purely derived from the current scan, there's nothing to preserve
    # across scans (an orphan that gets fixed should simply stop appearing).
    db.query(OrphanedFilterName).filter(
        OrphanedFilterName.device_name == device_name
    ).delete(synchronize_session=False)

    for name in sorted(orphaned_mask):
        db.add(OrphanedFilterName(
            device_name=device_name, file_type="mask", filename=mask_file, name=name,
        ))
    for name in sorted(orphaned_screen):
        db.add(OrphanedFilterName(
            device_name=device_name, file_type="screen", filename=screen_file, name=name,
        ))


# ---------------------------------------------------------------------------
# Main scanner entry point
# ---------------------------------------------------------------------------

class Scanner:
    """
    Orchestrates a full scan of config.cfg + transports + protocols.
    Designed to be instantiated once and called on startup and from the
    file watcher.
    """

    def __init__(self, config_path: Path, project_root: Path) -> None:
        self.config_path: Path = config_path
        self.project_root: Path = project_root
        self.transports_dir: Path = project_root / "classes" / "transports"
        self.protocols_dir: Path = project_root / "protocols"

    @property
    def _cfg_is_truth(self) -> bool:
        """
        Returns True when the scanner should treat config.cfg as ground truth
        and sync value_staged = value_disk (used on startup when the file has
        been modified externally, or after a rollback).
        Set via scanner.set_cfg_is_truth(True) before calling run().
        """
        return getattr(self, "_cfg_truth_flag", False)

    def set_cfg_is_truth(self, value: bool = True) -> None:
        """Enable/disable cfg-as-truth mode for the next run() call."""
        self._cfg_truth_flag: bool = value

    def run(self, db: Session | None = None) -> dict[str, int]:
        """
        Full scan.  Accepts an optional existing session (for tests);
        otherwise opens its own session_scope.
        After run() completes, _cfg_is_truth resets to False.
        """
        try:
            if db is not None:
                return self._scan(db)
            with session_scope() as db:
                return self._scan(db)
        finally:
            self._cfg_truth_flag = False   # always reset after a scan

    def _scan(self, db: Session) -> dict[str, int]:
        _log.info(f"Starting scanner scan: {self.config_path}")

        # Update AppState scanner status
        state: AppState | None = db.get(AppState, 1)
        if state:
            state.scanner_status = "running"
            db.flush()

        stats: dict[str, int] = {
            "settings_upserted": 0,
            "settings_orphaned": 0,
            "registers_upserted": 0,
        }

        seen_setting_keys: set[tuple[str, str]] = set()

        try:
            # ----------------------------------------------------------------
            # Scan config.cfg
            # ----------------------------------------------------------------
            config_data: dict[str, dict[str, str]] = load_config(self.config_path)
            transport_library: dict[str, TransportLibraryEntry] = scan_transport_library(self.transports_dir)

            for section, keys in config_data.items():
                transport_type = "general"

                if section.startswith("transport."):
                    transport_type: str = _classify_transport(section, keys, self.transports_dir)
                elif section.lower() == "logging":
                    transport_type = "logging"

                for key, value in keys.items():
                    _upsert_setting(
                        db, section, key, value, transport_type,
                        default_value=self._get_default(section, key, keys, transport_library),
                        cfg_is_truth=self._cfg_is_truth,
                    )
                    seen_setting_keys.add((section, key))
                    stats["settings_upserted"] += 1

                # ------------------------------------------------------------
                # Derive host/port for prometheus_out sections
                # ------------------------------------------------------------
                # prometheus_out has no real "connect out to" host/port the
                # way most transports do -- it's scraped, not connecting
                # out. host/port here exist purely so the "Configured
                # Devices" dashboard's Host column (device_service.
                # get_nav_data(), which reads these two keys as plain
                # Setting rows) shows something meaningful for this bridge
                # too, same as every other transport type.
                #
                # Deliberately NOT independently configurable: computed
                # fresh on every scan from this section's actual
                # metrics_port (falling back to PROMETHEUS_OUT_DEFAULT_PORT
                # -- the WebServer's own default port, see
                # classes.WebServer.main.start_webserver()'s `port`
                # parameter -- when metrics_port is unset), so they can
                # never drift out of sync with it the way two independently
                # user-edited values could. Any literal host/port a user
                # has in config.cfg for a prometheus_out section is
                # intentionally ignored and overwritten here -- there is
                # deliberately only one source of truth (metrics_port).
                #
                # cfg_is_truth=True is forced (regardless of
                # self._cfg_is_truth) because these two values are never a
                # "staged, uncommitted edit" -- they should always reflect
                # the current config.cfg state immediately, every scan.
                # ------------------------------------------------------------
                # Derive host/port for prometheus_out sections
                # ------------------------------------------------------------
                # prometheus_out has no real "connect out to" host/port the
                # way most transports do -- it's scraped, not connecting
                # out. host/port here exist purely so the "Configured
                # Devices" dashboard's Host column (device_service.
                # get_nav_data(), which reads these two keys as plain
                # Setting rows) shows something meaningful for this bridge
                # too, same as every other transport type.
                #
                # Deliberately NOT independently configurable: computed
                # fresh on every scan from this section's actual
                # metrics_port (falling back to PROMETHEUS_OUT_DEFAULT_PORT
                # -- the WebServer's own default port, see
                # classes.WebServer.main.start_webserver()'s `port`
                # parameter -- when metrics_port is unset), so they can
                # never drift out of sync with it the way two independently
                # user-edited values could. Any literal host/port a user
                # has in config.cfg for a prometheus_out section is
                # intentionally ignored and overwritten here -- there is
                # deliberately only one source of truth (metrics_port).
                #
                # cfg_is_truth=True is forced (regardless of
                # self._cfg_is_truth) because these two values are never a
                # "staged, uncommitted edit" -- they should always reflect
                # the current config.cfg state immediately, every scan.
                # Without this, an existing row's value_staged (what the
                # dashboard actually reads -- see
                # device_service._get_section_keys()) would NOT update
                # after the first scan, silently reintroducing the exact
                # host/port drift problem this mechanism exists to prevent.
                #
                # IMPORTANT: transport_type here is the broad CATEGORY
                # _classify_transport() returns ("scraper" | "bridge" |
                # "general") -- see that function's own docstring -- NEVER
                # the literal transport name. An earlier version of this
                # fix mistakenly compared transport_type itself against
                # "prometheus_out", which can never be true (prometheus_out
                # classifies as "bridge") and silently never ran. The
                # literal transport name is a separate value, read directly
                # from the section's own `transport =` config.cfg line.
                literal_transport_name: str = keys.get("transport", "").strip()
                if literal_transport_name == "prometheus_out":
                    derived_port: str = keys.get("metrics_port", "").strip() or str(PROMETHEUS_OUT_DEFAULT_PORT)
                    for derived_key, derived_value in (("host", "0.0.0.0"), ("port", derived_port)):  # noqa: S104
                        _upsert_setting(
                            db, section, derived_key, derived_value, transport_type,
                            default_value=derived_value,
                            cfg_is_truth=True,
                        )
                        seen_setting_keys.add((section, derived_key))
                        stats["settings_upserted"] += 1

            # ----------------------------------------------------------------
            #  Add known-but-unset keys for each transport section
            #    (so the UI can show all possible config options)
            #
            #  Merges the JSON registry baseline with the live AST-scanned
            #  keys so that:
            #    (a) new settings.get() calls in bridge/scraper code appear
            #        in the UI immediately on next scan (Bug 1 fix), and
            #    (b) the JSON files are kept current automatically via the
            #        sync_from_library() call inside scan_transport_library().
            # ----------------------------------------------------------------
            known_transport_keys: dict[str, dict[str, str]] = get_known_transport_keys()

            for section, keys in config_data.items():
                if not section.startswith("transport."):
                    continue
                transport_name: str = keys.get("transport", "").strip()
                transport_type = _classify_transport(section, keys, self.transports_dir)

                # Build the merged key→default map for this transport section.
                #
                # Start with the JSON registry (fully resolved, includes $extends
                # inheritance), then overlay live AST-scanned keys so that any
                # explicit default in the transport source code takes precedence.
                #
                # AST values may be None when settings.get("key") has no second
                # argument.  For those we keep the JSON registry default so the
                # UI always has something useful to show.
                json_defaults: dict[str, str] = dict(known_transport_keys.get(transport_name, {}))
                lib_entry: TransportLibraryEntry = transport_library.get(transport_name, EMPTY_TRANSPORT_ENTRY)
                ast_keys_raw: dict[str, str | None] = lib_entry.get("keys", {})

                # Merged: JSON baseline, overridden only where AST has a real value
                registry_keys: dict[str, str] = dict(json_defaults)
                for k, ast_val in ast_keys_raw.items():
                    if ast_val is not None:
                        # AST found an explicit default — it wins
                        registry_keys[k] = ast_val
                    elif k not in registry_keys:
                        # AST found the key exists but has no default;
                        # JSON has no entry either — add with empty default
                        registry_keys[k] = ""

                for k, default_v in registry_keys.items():
                    if transport_name == "prometheus_out" and k in ("host", "port"):
                        # Computed every scan from metrics_port in the first
                        # loop above (see "Derive host/port for
                        # prometheus_out sections") -- never touch them here,
                        # or this generic "known-but-unset" path would
                        # immediately overwrite that computed value with an
                        # inactive, blank placeholder row within the same
                        # scan pass, since it still finds them absent from
                        # `keys` (config.cfg's literal text was never
                        # written to). NOTE: transport_name (the literal
                        # transport name, e.g. "prometheus_out") -- not
                        # transport_type, which is only the broad "scraper"
                        # | "bridge" | "general" category and can never
                        # equal "prometheus_out" (see the near-identical bug
                        # this fixed in the first loop above).
                        continue
                    if k not in keys:  # not already set in config.cfg
                        _upsert_setting(
                            db, section, k,
                            value_disk="",
                            transport_type=transport_type,
                            default_value=default_v,
                            is_active=False,  # not currently in config
                        )
                    seen_setting_keys.add((section, k))
                    stats["settings_upserted"] += 1

            # ----------------------------------------------------------------
            #  Mark orphans
            # ----------------------------------------------------------------
            stats["settings_orphaned"] = _mark_orphaned_settings(db, seen_setting_keys)

            # ----------------------------------------------------------------
            #  Scan protocol CSV/JSON files
            # ----------------------------------------------------------------
            registers: List[dict[str, Any]] = scan_protocols_dir(self.protocols_dir)
            skipped = 0
            for reg in registers:
                result: ProtocolRegister | None = _upsert_protocol_register(db, reg)
                if result is not None:
                    stats["registers_upserted"] += 1
                else:
                    skipped += 1

            if skipped:
                _log.warning(f"Scanner: {skipped} protocol register row(s) skipped due to errors.")

            _sync_device_protocol_selections(
                db,
                config_data,
                self.project_root,
            )

            # Flush all pending INSERTs/UPDATEs in one shot.
            # Each INSERT already ran inside its own savepoint, so this flush
            # only covers the UPDATE path and is safe to call here.
            db.flush()

            # ----------------------------------------------------------------
            #  Refresh AppState
            # ----------------------------------------------------------------
            state = db.get(AppState, 1)
            if state is None:
                state = AppState(id=1)
                db.add(state)

            state.last_scan_at = datetime.now().astimezone()
            state.scanner_status = "idle"
            state.scanner_last_error = None
            db.commit()

            refresh_app_state(db)

            _log.info(
                f"Scanner complete: {stats['settings_upserted']} settings, "
                f"{stats['settings_orphaned']} orphaned, "
                f"{stats['registers_upserted']} registers."
            )
        except Exception as exc:
            _log.exception(f"Scanner error: {exc}")
            state = db.get(AppState, 1)
            if state:
                state.scanner_status = "error"
                state.scanner_last_error = str(exc)
                db.commit()
            raise

        return stats

    def _get_default(
        self,
        section: str,
        key: str,
        section_keys: dict[str, str],
        transport_library: dict[str, TransportLibraryEntry],
    ) -> str | None:
        """
        Look up the default value for a key.

        Returns None (not "") when no default is found anywhere. This is the
        sentinel that tells _upsert_setting to leave the existing stored default
        untouched rather than overwriting a good value with an empty string.

        Priority order:
          1. Live AST scan result for this transport (most current)
          2. JSON registry entry for this transport
          3. JSON registry _base fallback
          4. None — no default known; do not overwrite existing DB value
        """
        transport_name: str = section_keys.get("transport", "").strip()

        # 1. Live AST result.
        # The AST dict value is None when the transport source has no second
        # argument on settings.get("key") — meaning no default was specified
        # in code.  We skip those and fall through to the JSON registry so
        # the hand-curated defaults are used instead.
        # A key absent from the dict entirely also returns None from .get().
        lib_entry: TransportLibraryEntry = transport_library.get(transport_name, EMPTY_TRANSPORT_ENTRY)
        ast_keys: dict[str, str | None] = lib_entry["keys"]
        if key in ast_keys and ast_keys[key] is not None:
            return ast_keys[key]

        # 2 & 3. JSON registry for this transport, falling back to _base.
        # Use a private sentinel to distinguish found-with-value-""
        # from not-found-at-all.
        _MISSING = object()
        known: dict[str, dict[str, str]] = get_known_transport_keys()
        transport_defaults: dict[str, str] = known.get(transport_name, get_transport_base_keys())
        result: str | object = transport_defaults.get(key, _MISSING)
        if result is not _MISSING:
            return str(result)

        # 4. Not found anywhere — return None so existing DB default is preserved.
        return None
