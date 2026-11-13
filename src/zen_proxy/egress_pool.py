"""Zen egress pool: ranked proxy exits for the CLIProxyAPI rotator plugin.

This is the pool half of the old opencode/zen proxy server, with every API
format translation removed (CLIProxyAPI translates; the plugin only needs
exits) and the pool logic merged from ``opencode-free-proxies`` and
``omnibot``:

  * sing-box multiplexer over one or more subscription profiles, one SOCKS5
    username per exit (ported from ``zen_proxy.singbox``);
  * optional extra proxy lists (``omnibot/pool/proxies*.txt`` style, one URL
    per line, socks5:// or http://);
  * S-tier ranking: every exit is probed and scored by time-to-first-byte, and
    the written list is ordered fastest-first so the plugin's fastest-first
    selection starts from a good order;
  * expiring health state (dead / flagged / latency) persisted to disk, so a
    fast-but-rate-limited exit comes back after its cooldown instead of being
    banned forever — daily/hourly limits are not permanent.

The output file is what the plugin reads (``egress.socks_file``):

    socks5://p3:p@127.0.0.1:20800

Usage:

    zen-egress-pool --profile profiles/expanded_verified.json \
        --extra-list ~/code/omnibot/pool/proxies.txt \
        --count 40 --out ~/.cache/zen-egress/socks.txt
"""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import logging
import os
import signal
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

try:  # probing needs the aiohttp extras; the bookkeeping helpers do not.
    from aiohttp import ClientSession, ClientTimeout
    from aiohttp_socks import ProxyConnector
except ImportError:  # pragma: no cover - optional dependency
    ClientSession = None  # type: ignore[assignment,misc]
    ClientTimeout = None  # type: ignore[assignment,misc]
    ProxyConnector = None  # type: ignore[assignment,misc]

from zen_proxy.profile import load_profile_outbounds
from zen_proxy.singbox import SingBoxMultiplexer

logger = logging.getLogger("zen_proxy.egress")

DEFAULT_PROBE_URLS = (
    "https://api.kilo.ai/api/gateway/models",
    "https://ifconfig.me/ip",
)

#: Cooldowns are deliberately short. Upstream limits on free tiers are often
#: hourly/daily; an exit must return to the pool on its own, never stay banned.
DEAD_COOLDOWN_SEC = 600.0
FLAGGED_COOLDOWN_SEC = 1800.0
#: A fast exit that starts failing keeps its rank but is skipped while parked.
LATENCY_EWMA_ALPHA = 0.4
#: Stage-2 (real upstream) failures cool an exit down for this long only.
VERIFY_FAIL_COOLDOWN_SEC = 240.0


@dataclass
class ExitState:
    """Persisted per-exit health."""

    url: str
    latency_ms: float | None = None
    ok: int = 0
    fail: int = 0
    dead_until: float = 0.0
    flagged_until: float = 0.0
    verified_at: float = 0.0
    verified_ok: bool = False

    def verified_recently(self, now: float, ttl: float) -> bool:
        return self.verified_ok and (now - self.verified_at) < ttl

    def parked(self, now: float) -> bool:
        return now < self.dead_until or now < self.flagged_until

    def cooldown(self, now: float) -> float:
        return max(0.0, max(self.dead_until, self.flagged_until) - now)

    def score(self) -> float:
        if self.latency_ms is None:
            return 500.0  # unknown exits are probed but rank behind known-fast ones
        return self.latency_ms + self.fail * 120.0

    def to_dict(self) -> dict:
        return {
            "latency_ms": self.latency_ms,
            "ok": self.ok,
            "fail": self.fail,
            "dead_until": self.dead_until,
            "flagged_until": self.flagged_until,
            "verified_at": self.verified_at,
            "verified_ok": self.verified_ok,
        }

    @classmethod
    def from_dict(cls, url: str, data: dict) -> ExitState:
        return cls(
            url=url,
            latency_ms=data.get("latency_ms"),
            ok=int(data.get("ok", 0)),
            fail=int(data.get("fail", 0)),
            dead_until=float(data.get("dead_until", 0.0)),
            flagged_until=float(data.get("flagged_until", 0.0)),
            verified_at=float(data.get("verified_at", 0.0)),
            verified_ok=bool(data.get("verified_ok", False)),
        )


@dataclass
class PoolState:
    path: Path | None
    exits: dict[str, ExitState] = field(default_factory=dict)
    updated: float = 0.0

    def get(self, url: str) -> ExitState:
        existing = self.exits.get(url)
        if existing is None:
            existing = ExitState(url=url)
            self.exits[url] = existing
        return existing

    def load(self) -> None:
        if self.path is None or not self.path.is_file():
            return
        try:
            raw = json.loads(self.path.read_text())
        except (OSError, ValueError):
            return
        entries = raw.get("exits", {}) if isinstance(raw, dict) else {}
        for url, data in entries.items():
            if isinstance(data, dict):
                self.exits[url] = ExitState.from_dict(url, data)
        self.updated = float(raw.get("updated", 0.0)) if isinstance(raw, dict) else 0.0

    def save(self) -> None:
        if self.path is None:
            return
        payload = {
            "updated": time.time(),
            "exits": {url: state.to_dict() for url, state in self.exits.items()},
        }
        _write_atomic(self.path, [json.dumps(payload, indent=1)])


def _write_atomic(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=".pool-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write("\n".join(lines))
            if lines:
                handle.write("\n")
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


async def _probe(
    proxy_url: str,
    timeout: float,
    probe_urls: tuple[str, ...],
    want_status: int | None = None,
) -> float | None:
    """Return time-to-first-byte in ms when the exit works, else None.

    With ``want_status`` set, the exit only counts as working when the real
    target answers with that exact status code, which filters exits that pass a
    generic TTFB probe but cannot actually reach the upstream API.
    """
    if ClientSession is None or ClientTimeout is None or ProxyConnector is None:  # pragma: no cover
        raise RuntimeError("aiohttp and aiohttp-socks are required to probe exits")

    try:
        if proxy_url.startswith(("http://", "https://")):
            session_kwargs: dict = {"timeout": ClientTimeout(total=timeout)}
            proxy = proxy_url
        else:
            connector = ProxyConnector.from_url(proxy_url, rdns=True)
            session_kwargs = {"connector": connector, "timeout": ClientTimeout(total=timeout)}
            proxy = None
    except Exception:  # noqa: BLE001 - an unusable proxy URL is simply a dead exit
        return None

    for target in probe_urls:
        started = time.perf_counter()
        try:
            async with ClientSession(**session_kwargs) as session:
                kwargs = {"headers": {"User-Agent": "zen-egress-pool/1.0"}}
                if proxy:
                    kwargs["proxy"] = proxy
                async with session.get(target, **kwargs) as response:
                    if want_status is not None:
                        if response.status == want_status:
                            return (time.perf_counter() - started) * 1000.0
                        continue
                    if response.status < 500:
                        return (time.perf_counter() - started) * 1000.0
        except Exception:  # noqa: BLE001 - any error means the exit cannot serve
            continue
    return None


def _read_extra_list(path: Path, limit: int | None) -> list[str]:
    if not path.is_file():
        logger.warning("extra list %s missing", path)
        return []
    out: list[str] = []
    seen: set[str] = set()
    for line in path.read_text(errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "://" not in line:
            line = "socks5://" + line
        if not line.startswith(("socks5://", "socks5h://", "http://", "https://")):
            continue
        if line in seen:
            continue
        seen.add(line)
        out.append(line)
        if limit and len(out) >= limit:
            break
    return out


def _expand_globs(patterns: list[str], newest: int) -> list[Path]:
    """Expand glob patterns to the newest N matching files, newest first.

    Proxy lists are written with timestamps, so only the freshest few are worth
    probing; older snapshots are near-100% dead and would slow every round.
    """
    selected: list[Path] = []
    for pattern in patterns:
        matches = [p for p in Path(pattern).expanduser().parent.glob(Path(pattern).expanduser().name) if p.is_file()]
        matches.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        keep = matches[:newest] if newest > 0 else matches
        selected.extend(keep)
    return list(dict.fromkeys(selected))


async def _probe_all(
    urls: list[str],
    state: PoolState,
    timeout: float,
    probe_urls: tuple[str, ...],
    concurrency: int,
) -> list[ExitState]:
    semaphore = asyncio.Semaphore(concurrency)
    now = time.time()

    async def one(url: str) -> ExitState:
        entry = state.get(url)
        if entry.parked(now):
            return entry  # still cooling down; keep it out of the written list
        async with semaphore:
            latency = await _probe(url, timeout, probe_urls)
        if latency is None:
            entry.fail += 1
            entry.dead_until = time.time() + DEAD_COOLDOWN_SEC
            return entry
        entry.ok += 1
        entry.fail = 0
        entry.dead_until = 0.0
        if entry.latency_ms is None:
            entry.latency_ms = latency
        else:
            entry.latency_ms = entry.latency_ms * (1 - LATENCY_EWMA_ALPHA) + latency * LATENCY_EWMA_ALPHA
        return entry

    return list(await asyncio.gather(*(one(url) for url in urls)))


async def _verify_all(
    states: list[ExitState],
    verify_urls: tuple[str, ...],
    want_status: int,
    timeout: float,
    concurrency: int,
) -> None:
    """Validate the best exits against the real upstream API.

    Stage 2 of the pipeline: a TTFB probe proves an exit forwards traffic, but
    only a real request proves it can reach *this* upstream (CDN blocks, dead
    datacentres and inline rate-limit pages all pass stage 1). Verified exits
    are ranked first; the failures get a short cooldown and stay in the pool at
    a lower rank, because the upstream window is hourly/daily, not permanent.
    """
    if not states:
        return
    semaphore = asyncio.Semaphore(concurrency)

    async def one(entry: ExitState) -> None:
        async with semaphore:
            latency = await _probe(entry.url, timeout, verify_urls, want_status=want_status)
            if latency is None:
                # Not parked: an exit that cannot reach this upstream today may
                # still be fast elsewhere, and the window is hourly/daily.
                entry.verified_ok = False
                entry.verified_at = time.time()
                return
            entry.verified_ok = True
            entry.verified_at = time.time()
            entry.latency_ms = (
                latency
                if entry.latency_ms is None
                else (LATENCY_EWMA_ALPHA * latency + (1 - LATENCY_EWMA_ALPHA) * entry.latency_ms)
            )

    await asyncio.gather(*(one(entry) for entry in states))


def _rank_alive(states: list[ExitState], now: float, verify_ttl: float = 900.0) -> list[ExitState]:
    alive = [s for s in states if not s.parked(now) and s.latency_ms is not None]
    # Verified-for-this-upstream exits go first; inside each group the fastest
    # wins. A stale verification eventually stops counting so an exit cannot
    # coast on an old success forever.
    alive.sort(key=lambda s: (0 if s.verified_recently(now, verify_ttl) else 1, s.score()))
    return alive


def _acquire_lock(path: Path) -> object | None:
    """Hold an exclusive lock so two pool instances cannot overwrite each other.

    Two writers on the same ``--out``/``--state`` silently corrupt the ranking
    (each round replaces the other's list), and they also fight over the
    multiplexer port, so a duplicate must exit immediately.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(path, "w", encoding="utf-8")  # noqa: SIM115 - held for the process lifetime
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        return None
    handle.write(str(os.getpid()))
    handle.flush()
    return handle


async def run(args: argparse.Namespace) -> int:
    out_path = Path(args.out).expanduser()
    lock = _acquire_lock(out_path.with_suffix(".lock"))
    if lock is None:
        logger.error("another zen-egress-pool already owns %s; exiting", out_path)
        return 3
    state = PoolState(Path(args.state).expanduser() if args.state else None)
    state.load()

    nodes: list = []
    urls: list[str] = []
    port = args.port
    multiplexer: SingBoxMultiplexer | None = None

    profiles = [Path(p).expanduser() for p in args.profile]
    remaining = args.count
    for profile in profiles:
        if remaining <= 0:
            break
        loaded, name = load_profile_outbounds(profile, max_proxies=remaining)
        if not loaded:
            logger.warning("no outbounds in profile %s", profile)
            continue
        logger.info("loaded %d outbounds from %s (%s)", len(loaded), profile, name)
        if multiplexer is None:
            multiplexer = SingBoxMultiplexer(loaded, socks_port=port, binary=args.singbox)
            multiplexer.start(timeout_sec=15.0)
            logger.info("sing-box multiplexer listening on 127.0.0.1:%d", port)
            for node in loaded:
                urls.append(f"socks5://{node.auth_user}:p@127.0.0.1:{port}")
        else:
            # Additional profiles are appended to the same multiplexer.
            start_index = len(multiplexer.outbounds)
            for offset, node in enumerate(loaded):
                multiplexer.outbounds.append(node)
                urls.append(f"socks5://p{start_index + offset}:p@127.0.0.1:{port}")
        remaining = args.count - len(urls)

    for extra in _expand_globs(args.extra_glob or [], args.extra_glob_newest):
        logger.info("extra list (glob) %s", extra)
        extra_urls = _read_extra_list(extra, args.extra_limit)
        logger.info("extra list %s: %d proxies", extra, len(extra_urls))
        urls.extend(extra_urls)

    for extra in args.extra_list or []:
        extra_urls = _read_extra_list(Path(extra).expanduser(), args.extra_limit)
        logger.info("extra list %s: %d proxies", extra, len(extra_urls))
        urls.extend(extra_urls)

    urls = list(dict.fromkeys(urls))
    if not urls:
        logger.error("no exits to probe")
        return 2
    if args.dump_candidates:
        dump_path = Path(args.dump_candidates).expanduser()
        _write_atomic(dump_path, urls)
        logger.info("wrote %d merged candidate exits to %s", len(urls), dump_path)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover
            pass

    alive: list[ExitState] = []
    try:
        while not stop.is_set():
            # Supervise the multiplexer: a dead sing-box must be restarted, not
            # silently left handing out dead exits.
            if multiplexer is not None and multiplexer.process is not None and multiplexer.process.poll() is not None:
                logger.warning("sing-box exited (code %s); restarting the multiplexer", multiplexer.process.returncode)
                try:
                    multiplexer.stop()
                except Exception:  # noqa: BLE001
                    pass
                multiplexer = SingBoxMultiplexer(nodes, socks_port=port, binary=args.singbox)
                try:
                    multiplexer.start(timeout_sec=15.0)
                    logger.info("sing-box multiplexer back up on 127.0.0.1:%d", port)
                except Exception as err_restart:  # noqa: BLE001
                    logger.error("multiplexer restart failed: %s", err_restart)
                    await asyncio.sleep(10)
                    continue
            started = time.time()
            try:
                states = await asyncio.wait_for(
                    _probe_all(urls, state, args.probe_timeout, tuple(args.probe_url), args.concurrency),
                    timeout=args.round_timeout,
                )
            except TimeoutError:
                logger.error(
                    "probe round exceeded %.0fs with %d exits; keeping the previous list at %s",
                    args.round_timeout,
                    len(urls),
                    out_path,
                )
                if args.once:
                    return 1
                await asyncio.sleep(min(args.refresh, 30.0))
                continue
            alive = _rank_alive(states, time.time(), verify_ttl=args.verify_ttl)
            if alive and args.verify_url:
                # Stage 2: prove the best exits can actually reach the upstream.
                target = alive[: args.verify_top] if args.verify_top > 0 else alive
                await _verify_all(
                    target,
                    tuple(args.verify_url),
                    args.verify_status,
                    args.verify_timeout,
                    max(4, args.concurrency // 2),
                )
                alive = _rank_alive(states, time.time(), verify_ttl=args.verify_ttl)
                now_check = time.time()
                verified_exits = [e for e in alive if e.verified_recently(now_check, args.verify_ttl)]
                unverified_exits = [e for e in alive if not e.verified_recently(now_check, args.verify_ttl)]
                # Verified exits are the warm pool; the unverified tail keeps
                # throughput up while the next stage-2 round promotes them.
                tail = unverified_exits[: max(len(verified_exits), args.verify_min)]
                alive = verified_exits + tail
                logger.info(
                    "stage 2: %d/%d exits verified against %s (%d kept unverified)",
                    len(verified_exits),
                    len(target),
                    args.verify_url[0],
                    len(tail),
                )
            if alive:
                _write_atomic(out_path, [entry.url for entry in alive])
            else:
                logger.warning("no exit answered the probe; keeping the previous list at %s", out_path)
            state.save()
            fastest = f"{alive[0].latency_ms:.0f}ms" if alive and alive[0].latency_ms else "n/a"
            logger.info(
                "probed %d exits in %.1fs -> %d alive (fastest %s), wrote %s",
                len(urls),
                time.time() - started,
                len(alive),
                fastest,
                out_path,
            )
            if args.once:
                break
            try:
                await asyncio.wait_for(stop.wait(), timeout=args.refresh)
            except TimeoutError:
                continue
    finally:
        if multiplexer is not None:
            multiplexer.stop()
    return 0 if alive else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="zen-egress-pool", description=__doc__)
    parser.add_argument(
        "--profile",
        action="append",
        default=None,
        help="sing-box/Hiddify profile JSON (repeatable; merged into one multiplexer)",
    )
    parser.add_argument("--port", type=int, default=20800, help="local SOCKS5 port for the multiplexer")
    parser.add_argument("--count", type=int, default=40, help="number of subscription outbounds to expose")
    parser.add_argument("--extra-list", action="append", default=None, help="extra proxy list file (repeatable)")
    parser.add_argument("--extra-limit", type=int, default=200, help="max proxies taken from each extra list")
    parser.add_argument(
        "--extra-glob",
        action="append",
        default=None,
        help="glob of timestamped proxy lists (repeatable); only the newest few are read",
    )
    parser.add_argument(
        "--extra-glob-newest", type=int, default=3, help="how many of the newest files per glob to read"
    )
    parser.add_argument(
        "--verify-url",
        action="append",
        default=None,
        help="real upstream URL for stage-2 validation (repeatable); e.g. https://api.kilo.ai/api/gateway/models",
    )
    parser.add_argument("--verify-status", type=int, default=200, help="status code stage 2 must see")
    parser.add_argument("--verify-timeout", type=float, default=10.0, help="per-exit stage-2 timeout")
    parser.add_argument("--verify-top", type=int, default=40, help="how many ranked exits to validate (0 = all)")
    parser.add_argument("--verify-ttl", type=float, default=900.0, help="seconds a stage-2 pass stays trusted")
    parser.add_argument(
        "--verify-min",
        type=int,
        default=20,
        help="unverified exits kept as a tail so the pool never shrinks to nothing",
    )
    parser.add_argument("--out", default="~/.cache/zen-egress/socks.txt", help="ranked proxy list for the plugin")
    parser.add_argument("--state", default="~/.cache/zen-egress/pool-state.json", help="persisted pool health")
    parser.add_argument("--refresh", type=float, default=300.0, help="seconds between probe rounds")
    parser.add_argument(
        "--round-timeout",
        type=float,
        default=180.0,
        help="hard cap for one probe round; on timeout the previous list is kept",
    )
    parser.add_argument("--probe-timeout", type=float, default=8.0, help="per-exit probe timeout")
    parser.add_argument("--probe-url", action="append", default=None, help="probe URL (repeatable)")
    parser.add_argument("--concurrency", type=int, default=12, help="parallel probes")
    parser.add_argument("--singbox", default="sing-box", help="sing-box binary")
    parser.add_argument(
        "--dump-candidates",
        default=None,
        help="also write the merged, de-duplicated candidate list (all sources) so it can be committed",
    )
    parser.add_argument("--once", action="store_true", help="probe once and exit")
    parser.add_argument("--verbose", action="store_true", help="debug logging")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.profile is None:
        args.profile = ["profiles/expanded_verified.json"]
    if args.probe_url is None:
        args.probe_url = list(DEFAULT_PROBE_URLS)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:  # pragma: no cover
        return 130


if __name__ == "__main__":
    sys.exit(main())
