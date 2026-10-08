"""Persistent per-listing calendar history, kept in a named key-value store so that
availability snapshots accumulate across runs. Occupancy quality improves every time the
same market is scanned again."""

from __future__ import annotations

import asyncio
from typing import Any

import os

from apify import Actor

STORE_PREFIX = "str-calendar-history"
MAX_SNAPSHOTS = 6


def store_name() -> str:
    """One store per Actor. Under limited permissions an Actor can only open named storages it
    created itself, so the four sibling Actors (Airbnb, Vrbo, both, occupancy) must not share a
    name: whichever created it first locked the other three out of their history."""
    actor_id = os.environ.get("ACTOR_ID") or os.environ.get("APIFY_ACTOR_ID")
    return f"{STORE_PREFIX}-{actor_id}" if actor_id else STORE_PREFIX


class HistoryStore:
    def __init__(self, store: Any | None) -> None:
        self.store = store

    @classmethod
    async def open(cls, enabled: bool = True) -> "HistoryStore":
        if not enabled:
            return cls(None)
        try:
            store = await Actor.open_key_value_store(name=store_name())
            return cls(store)
        except Exception as exc:
            Actor.log.warning(f"calendar history store unavailable, continuing without history: {exc}")
            return cls(None)

    @staticmethod
    def _key(platform: str, listing_id: str) -> str:
        return f"{platform}_{listing_id}"

    async def get(self, platform: str, listing_id: str) -> dict:
        if not self.store:
            return {"snapshots": []}
        try:
            rec = await self.store.get_value(self._key(platform, listing_id))
        except Exception:
            rec = None
        if not isinstance(rec, dict):
            return {"snapshots": []}
        rec.setdefault("snapshots", [])
        return rec

    async def append(self, platform: str, listing_id: str, snapshot: dict) -> None:
        if not self.store:
            return
        rec = await self.get(platform, listing_id)
        snaps = rec.get("snapshots") or []
        snaps.append(snapshot)
        # Keep the earliest snapshot (baseline for booked/blocked) plus the most recent ones.
        if len(snaps) > MAX_SNAPSHOTS:
            snaps = [snaps[0]] + snaps[-(MAX_SNAPSHOTS - 1):]
        rec["snapshots"] = snaps
        rec.setdefault("firstSeenAt", snapshot.get("at"))
        rec["lastSeenAt"] = snapshot.get("at")
        try:
            await self.store.set_value(self._key(platform, listing_id), rec)
        except Exception as exc:
            Actor.log.debug(f"history write failed for {listing_id}: {exc}")

    async def get_many(self, platform: str, ids: list[str]) -> dict[str, dict]:
        results: dict[str, dict] = {}

        async def one(lid: str):
            results[lid] = await self.get(platform, lid)

        await asyncio.gather(*(one(i) for i in ids))
        return results