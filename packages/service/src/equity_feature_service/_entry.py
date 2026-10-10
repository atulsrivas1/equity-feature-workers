"""Frozen owned synthetic startup configuration; never HTTP-selected code."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                      allow_nan=False).encode("ascii")


@dataclass(frozen=True)
class OwnedEntryProfile:
    profile_id: str
    source: bytes
    source_sha256: str
    arguments: tuple[str, ...] = ()
    owned_synthetic: bool = False

    def __post_init__(self) -> None:
        if (type(self.profile_id) is not str or re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", self.profile_id) is None
                or type(self.source) is not bytes or not 1 <= len(self.source) <= 8192
                or type(self.source_sha256) is not str
                or self.source_sha256 != hashlib.sha256(self.source).hexdigest()
                or type(self.arguments) is not tuple or any(type(arg) is not str for arg in self.arguments)
                or len(canonical(self.arguments)) > 4096 or self.owned_synthetic is not True):
            raise ValueError("unadmitted_owned_entry")
        try:
            compile(self.source.decode("utf-8"), "<owned:"+self.profile_id+">", "exec")
        except (UnicodeError, SyntaxError, ValueError) as error:
            raise ValueError("unadmitted_owned_source") from error


@dataclass(frozen=True)
class OwnedEntryInventory:
    profiles: tuple[OwnedEntryProfile, ...]

    def __post_init__(self) -> None:
        if (type(self.profiles) is not tuple or not 1 <= len(self.profiles) <= 4
                or any(type(profile) is not OwnedEntryProfile for profile in self.profiles)
                or len({profile.profile_id for profile in self.profiles}) != len(self.profiles)):
            raise ValueError("unadmitted_owned_inventory")

    def select(self, profile_id: str) -> OwnedEntryProfile:
        if type(profile_id) is str:
            for profile in self.profiles:
                if profile.profile_id == profile_id:
                    return profile
        raise ValueError("unadmitted_owned_profile")


def startup_frame(epoch: str, challenge: str, profile: OwnedEntryProfile) -> bytes:
    return canonical(dict(schema="owned.startup.v1", epoch=epoch, challenge=challenge,
                          profile=profile.profile_id, source_sha256=profile.source_sha256))+b"\n"


def startup_ack(challenge: str) -> bytes:
    return ("ACK:"+challenge+"\n").encode("ascii")


def validate_startup(frame: bytes, expected: bytes) -> None:
    # Equality binds the complete canonical frame; no prefix or first-row ACK.
    if not 1 <= len(frame) <= 512 or frame != expected:
        raise ValueError("owned_startup_frame")
