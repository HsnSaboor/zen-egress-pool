"""Unit tests for the egress-pool bookkeeping (no network, no sing-box)."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from zen_proxy import egress_pool
from zen_proxy.egress_pool import ExitState, PoolState


def test_exit_state_park_and_recover() -> None:
    now = 1_000.0
    exit_state = ExitState(url="socks5://a")
    assert not exit_state.parked(now)

    exit_state.dead_until = now + 30
    assert exit_state.parked(now)
    assert exit_state.cooldown(now) == 30
    # Cooldowns are bounded: the exit is usable again once it elapses.
    assert not exit_state.parked(now + 31)


def test_exit_state_score_prefers_fast_then_healthy() -> None:
    fast = ExitState(url="socks5://fast", latency_ms=200)
    slow = ExitState(url="socks5://slow", latency_ms=900)
    flaky = ExitState(url="socks5://flaky", latency_ms=210, fail=3)
    unknown = ExitState(url="socks5://unknown")
    # Unknown exits sit behind known-fast ones but ahead of a repeatedly failing
    # exit, so a fresh probe can never outrank a proven fast exit.
    assert fast.score() < unknown.score() < flaky.score() < slow.score()


def test_exit_state_round_trip() -> None:
    original = ExitState(url="socks5://a", latency_ms=123.5, ok=4, fail=1, dead_until=5.0, flagged_until=9.0)
    restored = ExitState.from_dict("socks5://a", original.to_dict())
    assert restored == original


def test_pool_state_persists(tmp_path: Path) -> None:
    path = tmp_path / "pool-state.json"
    state = PoolState(path)
    state.get("socks5://a").latency_ms = 100
    state.get("socks5://a").fail = 2
    state.save()

    reloaded = PoolState(path)
    reloaded.load()
    assert reloaded.get("socks5://a").latency_ms == 100
    assert reloaded.get("socks5://a").fail == 2


def test_pool_state_tolerates_corrupt_file(tmp_path: Path) -> None:
    path = tmp_path / "pool-state.json"
    path.write_text("{not json")
    state = PoolState(path)
    state.load()
    assert state.exits == {}


def test_rank_alive_is_fastest_first_and_skips_parked() -> None:
    now = time.time()
    fast = ExitState(url="socks5://fast", latency_ms=80)
    slow = ExitState(url="socks5://slow", latency_ms=900)
    parked = ExitState(url="socks5://parked", latency_ms=10)
    parked.flagged_until = now + 600
    unprobed = ExitState(url="socks5://unprobed")
    ranked = egress_pool._rank_alive([slow, fast, parked, unprobed], now)
    assert [entry.url for entry in ranked] == ["socks5://fast", "socks5://slow"]


def test_extra_list_normalises_and_filters(tmp_path: Path) -> None:
    listing = tmp_path / "proxies.txt"
    listing.write_text(
        "\n".join(
            [
                "# comment",
                "1.2.3.4:1080",
                "socks5://5.6.7.8:1080",
                "socks5h://9.9.9.9:1080",
                "socks4a://1.1.1.1:1080",
                "http://2.2.2.2:3128",
                "socks5://5.6.7.8:1080",
                "",
            ]
        )
    )
    urls = egress_pool._read_extra_list(listing, None)
    assert urls == [
        "socks5://1.2.3.4:1080",
        "socks5://5.6.7.8:1080",
        "socks5h://9.9.9.9:1080",
        "http://2.2.2.2:3128",
    ]


def test_write_atomic_replaces_file(tmp_path: Path) -> None:
    target = tmp_path / "socks.txt"
    egress_pool._write_atomic(target, ["socks5://a", "socks5://b"])
    assert target.read_text().splitlines() == ["socks5://a", "socks5://b"]
    egress_pool._write_atomic(target, ["socks5://c"])
    assert target.read_text().splitlines() == ["socks5://c"]


def test_build_parser_defaults() -> None:
    args = egress_pool.build_parser().parse_args([])
    assert args.port == 20800
    assert args.refresh == 300.0
    assert args.extra_limit == 200
    assert args.state.endswith("pool-state.json")


def test_state_file_shape(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    state = PoolState(path)
    state.get("socks5://a").latency_ms = 42
    state.save()
    payload = json.loads(path.read_text())
    assert payload["exits"]["socks5://a"]["latency_ms"] == 42


def test_expand_globs_picks_newest(tmp_path: Path) -> None:
    older = tmp_path / "list_20260101.txt"
    newer = tmp_path / "list_20260202.txt"
    older.write_text("1.1.1.1:1080\n")
    newer.write_text("2.2.2.2:1080\n")
    os.utime(older, (1_000_000, 1_000_000))
    os.utime(newer, (2_000_000, 2_000_000))

    selected = egress_pool._expand_globs([str(tmp_path / "list_*.txt")], 1)
    assert selected == [newer]

    both = egress_pool._expand_globs([str(tmp_path / "list_*.txt")], 0)
    assert len(both) == 2


def test_verified_exits_rank_first() -> None:
    now = time.time()
    slow_verified = ExitState(url="socks5://verified", latency_ms=900, verified_ok=True, verified_at=now)
    fast_unverified = ExitState(url="socks5://fast", latency_ms=100)
    ranked = egress_pool._rank_alive([fast_unverified, slow_verified], now)
    assert [entry.url for entry in ranked] == ["socks5://verified", "socks5://fast"]


def test_verification_expires_but_keeps_speed_order() -> None:
    now = time.time()
    stale = ExitState(url="socks5://stale", latency_ms=100, verified_ok=True, verified_at=now - 3600)
    slow = ExitState(url="socks5://slow", latency_ms=200)
    ranked = egress_pool._rank_alive([slow, stale], now, verify_ttl=900)
    assert not stale.verified_recently(now, 900)
    # An expired verification only drops the priority boost: raw speed decides.
    assert [entry.url for entry in ranked] == ["socks5://stale", "socks5://slow"]


def test_verification_survives_state_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    state = PoolState(path)
    entry = state.get("socks5://a")
    entry.verified_ok = True
    entry.verified_at = 12.0
    state.save()
    reloaded = PoolState(path)
    reloaded.load()
    assert reloaded.get("socks5://a").verified_ok is True
    assert reloaded.get("socks5://a").verified_at == 12.0


def test_second_instance_refuses_to_share_state(tmp_path: Path) -> None:
    lock_path = tmp_path / "pool.lock"
    first = egress_pool._acquire_lock(lock_path)
    assert first is not None
    assert egress_pool._acquire_lock(lock_path) is None
    first.close()
