"""Hiddify profile auto-discovery and outbound extraction."""

import json
import logging
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from zen_proxy.json_types import DocDict, as_doc_dict

logger = logging.getLogger("zen_proxy.profile")

HIDDIFY_DIR = Path.home() / ".local" / "share" / "app.hiddify.com"
HIDDIFY_DB = HIDDIFY_DIR / "db.sqlite"
HIDDIFY_CONFIGS = HIDDIFY_DIR / "configs"


@dataclass(frozen=True)
class OutboundNode:
    """Immutable representation of an outbound proxy node."""

    index: int
    tag: str
    outbound_type: str
    server: str
    server_port: int
    raw_config: DocDict
    country_code: str = "XX"
    country_emoji: str = "🌐"
    declared_latency_ms: int | None = None

    @property
    def auth_user(self) -> str:
        """Deterministic SOCKS5 user for sing-box routing."""
        return f"p{self.index}"


def find_active_hiddify_profile() -> tuple[Path, str]:
    """Find the path and name of the active Hiddify profile."""
    if HIDDIFY_DB.is_file():
        try:
            with sqlite3.connect(HIDDIFY_DB) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id, name FROM profile_entries WHERE active = 1 LIMIT 1")
                row = cursor.fetchone()
                if row:
                    profile_id, name = row[0], row[1]
                    cfg = HIDDIFY_CONFIGS / f"{profile_id}.json"
                    if cfg.is_file():
                        return cfg, name
        except (OSError, ValueError, sqlite3.Error) as e:
            logger.debug(f"[Profile] Hiddify lookup failed: {e}")

    # Fallback to repository profile if Hiddify desktop app is not installed (e.g. VPS/CI)
    repo_profile = Path("profiles/expanded_verified.json")
    if repo_profile.is_file():
        return repo_profile, repo_profile.stem

    raise FileNotFoundError(
        f"Could not find an active Hiddify profile in {HIDDIFY_CONFIGS} or {repo_profile}. "
        "Please provide --profile /path/to/profile.json manually."
    )


def _parse_tag_metadata(tag: str) -> tuple[str, str, int | None]:
    """Extract (emoji, country_code, latency_ms) from human-readable tag."""
    emoji = "🌐"
    cc = "XX"
    latency = None

    # Match country emoji (regional indicators) + 2-letter ISO code
    m_cc = re.search(r"([\U0001F1E6-\U0001F1FF]{2})\s+([A-Z]{2})", tag)
    if m_cc:
        emoji, cc = m_cc.group(1), m_cc.group(2)
    else:
        m_code = re.search(r"\b([A-Z]{2})\b", tag)
        if m_code:
            cc = m_code.group(1)

    m_lat = re.search(r"\|\s*(\d+)\s*ms", tag)
    if m_lat:
        try:
            latency = int(m_lat.group(1))
        except ValueError:
            pass

    return emoji, cc, latency


def load_profile_outbounds(
    profile_path: Path | str | None = None,
    max_proxies: int | None = None,
) -> tuple[list[OutboundNode], str]:
    """
    Load, parse, and filter valid outbounds from Hiddify.
    Filters out unsupported transports (e.g. xhttp in sing-box 1.11).
    """
    if profile_path:
        path = Path(profile_path).expanduser().resolve()
        name = path.stem
    else:
        path, name = find_active_hiddify_profile()

    with open(path, encoding="utf-8") as f:
        decoded: object = json.load(f)
    data: DocDict = as_doc_dict(decoded) or {}

    raw_outbounds = data.get("outbounds", [])
    outbound_list = raw_outbounds if isinstance(raw_outbounds, list) else []
    nodes: list[OutboundNode] = []

    idx = 0
    for entry in outbound_list:
        ob: DocDict = as_doc_dict(entry) or {}
        ob_type = ob.get("type", "")
        if ob_type in {"direct", "block", "dns"}:
            continue

        transport_raw = ob.get("transport", {})
        transport: DocDict = as_doc_dict(transport_raw) or {}
        if transport and transport.get("type") == "xhttp":
            continue

        server = ob.get("server")
        server_port = ob.get("server_port")
        if not isinstance(server, str) or not server:
            continue
        if not isinstance(server_port, int) or not server_port:
            continue

        tag_raw = ob.get("tag")
        tag = tag_raw if isinstance(tag_raw, str) and tag_raw else f"node-{idx}"
        emoji, cc, latency = _parse_tag_metadata(tag)

        nodes.append(
            OutboundNode(
                index=idx,
                tag=tag,
                outbound_type=ob_type if isinstance(ob_type, str) else "unknown",
                server=server,
                server_port=server_port,
                raw_config=ob,
                country_code=cc,
                country_emoji=emoji,
                declared_latency_ms=latency,
            )
        )
        idx += 1
        if max_proxies and len(nodes) >= max_proxies:
            break

    return nodes, name
