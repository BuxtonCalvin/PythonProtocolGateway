# Description: routers/commit.py — Commit, diff, and backup endpoints.
# File: commit.py
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

"""routers/commit.py — Commit, diff, and backup endpoints."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, List

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from classes.WebServer.diff_engine import DiffResult
from classes.WebServer.models import ConfigBackup

from ..config_writer import commit_all
from ..database import get_session, refresh_app_state, session_scope
from ..diff_engine import build_diff
from ..models import (
    DeviceProtocolSelection,
    ProtocolRegister,
    Setting,
    SettingDescription,
)
from ..services.backup_service import list_backups, rollback_to
from ..services.bridge_service import clear_staged_deletions, commit_staged_deletions
from ..services.setting_description_service import (
    commit_descriptions,
    discard_descriptions,
)

if TYPE_CHECKING:
    # Deferred at runtime (see the local import in do_commit()) —
    # importing protocol_gateway at module load time risks a circular
    # import, since it's what wires up the WebServer app in the first
    # place. Only needed here, under TYPE_CHECKING, for annotations.
    from protocol_gateway import GatewayManager, ReloadStatus

_log: logging.Logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/commit", tags=["commit"])

# commit_staged_deletions() (bridge_service.py) is declared to return
# list[dict[str, Any]] — that Any is outside this file's scope (it's a
# service module, not a router), so it isn't tightened here.
BackupSummary = dict[str, int | str | None]
DiffResponse = dict[str, dict[str, int] | list[dict[str, str | bool | None]]]
CommitResponse = dict[str, str | int | list[str] | dict[str, bool | str] | None]


def _has_dirty_config_state(db: Session) -> bool:
    """
    True if anything commit_all() would act on is currently staged.

    commit_all() rebuilds config.cfg, every mask/screen file, and every
    override CSV from ALL rows in these three tables every time it runs --
    not just dirty ones (see config_writer.py). That means when nothing
    here is dirty, value_staged already equals value_disk for every row,
    so running it anyway would only reproduce byte-identical output on
    disk. Gating on exactly these three tables' is_dirty flags is safe
    because they're the same flags discard_changes() below already treats
    as authoritative for "is there a config-side change pending".

    Also checks ProtocolRegister.pending_delete separately from is_dirty:
    a staged deletion deliberately does NOT set is_dirty (see
    ProtocolRegister.pending_delete's docstring — it's not a value to
    write back, it's the row's absence), so without this OR clause a
    commit consisting of ONLY pending deletions would find every is_dirty
    flag False and skip commit_all() entirely — the deletion would stay
    staged forever, silently never applied, no matter how many times
    Commit was clicked.
    """
    return (
        db.query(Setting).filter(Setting.is_dirty == True).first() is not None  # noqa: E712
        or db.query(ProtocolRegister).filter(ProtocolRegister.is_dirty == True).first() is not None  # noqa: E712
        or db.query(ProtocolRegister).filter(ProtocolRegister.pending_delete == True).first() is not None  # noqa: E712
        or db.query(DeviceProtocolSelection).filter(DeviceProtocolSelection.is_dirty == True).first() is not None  # noqa: E712
    )


def _has_dirty_descriptions(db: Session) -> bool:
    """True if any setting_descriptions row is staged — gates commit_descriptions()."""
    return db.query(SettingDescription).filter(SettingDescription.is_dirty == True).first() is not None  # noqa: E712


@router.post("")
def do_commit(request: Request, db: Session = Depends(get_session))-> CommitResponse:
    """
    Full commit: backup → write config.cfg → write masks/screens/overrides →
    reset dirty flags → commit setting descriptions → apply any staged
    TimescaleDB column deletions.

    Each of those tracks only runs when it actually has something staged
    (see _has_dirty_config_state / _has_dirty_descriptions /
    commit_staged_deletions' own has_staged_deletions() check) — so a
    commit with only, say, a Timescale column deletion pending no longer
    also rewrites config.cfg and every mask/screen/override/description
    file along with it.
    """
    state = request.app.state
    try:
        result: dict[str, int | str] = {}
        config_was_dirty: bool = _has_dirty_config_state(db)

        if config_was_dirty:
            result = commit_all(
                db=db,
                config_path=state.config_path,
                project_root=state.project_root,
                protocols_dir=state.protocols_dir,
                config_dir=getattr(state, "config_dir", None),
            )

        if _has_dirty_descriptions(db):
            result["descriptions_committed"] = commit_descriptions(db)

        db.commit()

        # Apply any staged TimescaleDB wide-table column deletions. This is
        # live Postgres schema work (drop rollups -> ALTER TABLE -> rebuild
        # rollups), not a config.cfg write, so it runs against the live
        # gateway rather than through config_writer.commit_all(). A failure
        # here is raised same as any other commit-step failure below; any
        # protocol that finished before the failure is already cleared from
        # staging (see commit_staged_deletions), so retrying the commit only
        # retries what's left.
        #
        # Kept in its own dict rather than folded into `result` — result is
        # typed dict[str, int | str] to match commit_all()'s return type,
        # and timescale_protocols_updated is a list[str], which doesn't fit
        # that value type.
        timescale_results: list[dict[str, Any]] = commit_staged_deletions(
            getattr(state, "gateway", None), state
        )
        timescale_summary: dict[str, int | list[str]] = {}
        if timescale_results:
            timescale_summary["timescale_columns_deleted"] = sum(len(r["deleted"]) for r in timescale_results)
            timescale_summary["timescale_protocols_updated"] = [r["protocol_name"] for r in timescale_results]

        # Recompute AppState dirty/orphan counts from the now-cleared flags so
        # the very next /api/devices/state poll (fired by base.html after the
        # commit response) sees zero dirty items and disables the commit button
        # without requiring a second press.
        refresh_app_state(db)

        # Reload the live gateway from the config.cfg commit_all() just wrote,
        # so transport/read_mode changes take effect without restarting the
        # whole process (the webUI keeps running throughout). Only when
        # config.cfg actually changed — a commit that only touched
        # descriptions or staged Timescale deletions has nothing for the
        # gateway to pick up. A reload failure does NOT fail this commit:
        # commit_all() already succeeded and config.cfg is correctly on disk
        # either way; gateway_reload.ok communicates whether the *live*
        # gateway picked it up cleanly, separately from the commit itself
        # (see gateway_reload_status() / the banner in base.html).
        gateway_reload: dict[str, bool | str] | None = None
        if config_was_dirty:
            manager: GatewayManager | None = getattr(state, "gateway_manager", None)
            if manager is not None:
                reload_status: ReloadStatus = manager.reload(trigger="manual")
                state.gateway = manager.current
                gateway_reload = {
                    "ok": reload_status.ok,
                    "message": reload_status.message,
                    "using_fallback": reload_status.using_fallback,
                }
                if not reload_status.ok:
                    _log.error(f"do_commit: gateway reload did not fully succeed: {reload_status.message}")
    except Exception as exc:

        _log.error(f"do_commit: commit failed: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
    else:
        response: CommitResponse = {"status": "ok", **result, **timescale_summary}
        if gateway_reload is not None:
            response["gateway_reload"] = gateway_reload
        return response


@router.get("/diff")
def diff(db: Session = Depends(get_session))-> DiffResponse:
    """Return structured diff of staged vs disk state."""
    result: DiffResult = build_diff(db)
    return {
        "summary": result.summary,
        "settings": [
            {
                "section": d.section,
                "key": d.key,
                "old_value": d.old_value,
                "new_value": d.new_value,
                "change_type": d.change_type,
            }
            for d in result.settings
        ],
        "protocols": [
            {
                "protocol_name": d.protocol_name,
                "registry_type": d.registry_type,
                "register_address": d.register_address,
                "variable_name": d.variable_name,
                "field": d.field,
                "old_value": d.old_value,
                "new_value": d.new_value,
                "change_type": d.change_type,
            }
            for d in result.protocols
        ],
    }


@router.get("/backups")
def get_backups(db: Session = Depends(get_session))-> list[BackupSummary]:
    backups: List[ConfigBackup] = list_backups(db)
    return [
        {
            "id": b.id,
            "created_at": b.created_at.isoformat(),
            "filepath": b.filepath,
            "file_size_bytes": b.file_size_bytes,
            "trigger": b.trigger,
            "notes": b.notes,
        }
        for b in backups
    ]


@router.post("/discard")
def discard_changes(request: Request, db: Session = Depends(get_session)) -> dict[str, str]:
    """
    Discard all staged changes: reset value_staged = value_disk and
    clear all is_dirty flags — restoring is_active = True first for any
    row that's currently deactivated but still has a real value on disk
    (a staged key or bridge-section removal being undone; see the
    Setting-reset loop below). Does NOT touch the config file on disk.
    Also clears any staged TimescaleDB column deletions — those are
    in-memory only, so nothing on disk or in Postgres needs reverting.
    """

    # Reset Setting rows
    dirty_settings: List[Setting] = db.query(Setting).filter(Setting.is_dirty == True).all()  # noqa: E712
    for row in dirty_settings:
        # Restore is_active before resetting the value, for a row that's
        # currently deactivated but has a real value on disk. That's a
        # staged removal — a single key turned off via routers/devices.py's
        # update_setting()/update_device_setting(), or an entire bridge
        # section via bridge_service.delete_bridge() — and value_disk
        # being truthy is exactly this app's signal that the key still
        # genuinely exists in config.cfg (see Setting's docstring in
        # models.py). Discarding that staged removal has to bring
        # is_active back to True, or the key/section stays permanently
        # excluded from every future commit even though the user asked
        # to undo the change — is_active was never touched by this loop
        # before, so this was a real gap for both cases, not something
        # bridge deletion introduced on its own.
        # A row with no disk value (a staged-but-never-committed new
        # key/section, is_active already True) is untouched by this,
        # exactly as before.
        if row.value_disk:
            row.is_active = True
        row.value_staged = row.value_disk
        row.is_dirty = False

    # Reset ProtocolRegister dirty flags and un-stage any pending deletions
    dirty_or_pending_delete: List[ProtocolRegister] = (
        db.query(ProtocolRegister)
        .filter(
            (ProtocolRegister.is_dirty == True)  # noqa: E712
            | (ProtocolRegister.pending_delete == True)  # noqa: E712
        )
        .all()
    )
    for row in dirty_or_pending_delete:
        row.is_dirty = False
        row.pending_delete = False

    # Reset DeviceProtocolSelection dirty flags
    dirty_selections: List[DeviceProtocolSelection] = db.query(DeviceProtocolSelection).filter(DeviceProtocolSelection.is_dirty == True).all()  # noqa: E712
    for row in dirty_selections:
        row.is_dirty = False

    discard_descriptions(db)
    clear_staged_deletions(request.app.state)
    db.flush()
    refresh_app_state(db)
    db.commit()

    return {"status": "discarded"}


class RollbackRequest(BaseModel):
    backup_id: int


@router.post("/rollback")
def do_rollback(payload: RollbackRequest, request: Request, db: Session = Depends(get_session)) -> dict[str, str | int]:
    """
    Restore config.cfg from a backup, then re-scan so the DB matches the
    restored file. The config file is treated as ground truth after rollback:
    value_staged is reset to value_disk for every setting so there are no
    stale staged edits left over from before the rollback.
    """

    success: bool = rollback_to(db, payload.backup_id, request.app.state.config_path)
    if not success:
        raise HTTPException(status_code=404, detail="Backup not found or file missing")

    # Re-scan so value_disk reflects the restored config.
    # set_cfg_is_truth ensures value_staged = value_disk (cfg is ground truth after rollback).
    try:
        request.app.state.scanner.set_cfg_is_truth(True)
        request.app.state.scanner.run()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Rollback succeeded but re-scan failed: {exc}")

    refresh_app_state(db)
    db.commit()

    return {"status": "rolled_back", "backup_id": payload.backup_id}


# ---------------------------------------------------------------------------
# HTML partial route — renders the same build_diff() result as GET /diff
# above, just as HTML instead of JSON.
# ---------------------------------------------------------------------------


@router.get("/diff-panel", response_class=HTMLResponse, response_model=None)
async def diff_panel(request: Request):
    """HTMX partial — visual diff of staged vs disk state."""
    with session_scope() as db:
        diff: DiffResult = build_diff(db)

    return request.app.state.templates.TemplateResponse(
        request=request,
        name="partials/diff_panel.html",
        context={"diff": diff},
    )
