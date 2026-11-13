# zen-egress-pool

Egress provider for the **cliproxy-rotator** CLIProxyAPI plugin.

This repository does **one** thing: it turns subscription profiles and plain
proxy lists into a ranked list of *working SOCKS5/HTTP exits* that the rotator
plugin consumes. It contains no API server, no format translation and no
OpenCode-Zen specific code — CLIProxyAPI owns all of that.

```
Hiddify / sing-box profile  ─┐
external proxy lists        ─┼─►  sing-box multiplexer (127.0.0.1:20800)  ─►  probe  ─►  ~/.cache/zen-egress/socks.txt
extra list files            ─┘        one SOCKS5 user per exit                              (fastest first)
```

## How the pieces fit

| Component | Role |
| --- | --- |
| `src/zen_proxy/profile.py` | Reads outbound nodes (and the subscription URL) from a sing-box/Hiddify profile JSON or SQLite profile. |
| `src/zen_proxy/singbox.py` | Runs **one** sing-box process with every outbound exposed as a separate SOCKS5 user (`p0`, `p1`, …) on a single port. ~25 MB RSS for hundreds of exits. |
| `src/zen_proxy/egress_pool.py` | Probes each exit (TTFB), keeps per-exit health, writes the ranked list, and supervises sing-box. |
| `~/.cache/zen-egress/socks.txt` | Consumed by `cliproxy-rotator` (`egress.socks_file`); hot-reloaded when it changes. |
| `~/.cache/zen-egress/pool-state.json` | Latency EWMA + failure counts + bounded cooldowns, so a rate-limited exit returns to the pool on its own. |

The rotator plugin also has a native **IPv6 `/48`** source-address pool, so the
two mechanisms are complementary: IPv6 for hosts that publish AAAA records,
this pool for IPv4-only upstreams (e.g. `api.kilo.ai`).

## Usage

```bash
# one probe round, then exit (useful for testing)
zen-egress-pool --once --verbose

# long-running provider (what the systemd unit runs)
zen-egress-pool \
  --profile profiles/expanded_verified.json \
  --extra-list ~/code/omnibot/pool/proxies.txt \
  --count 60 --port 20800 \
  --out ~/.cache/zen-egress/socks.txt \
  --state ~/.cache/zen-egress/pool-state.json \
  --refresh 300
```

Flags worth knowing:

- `--profile` (repeatable): profile JSONs merged into a single multiplexer.
- `--extra-list` (repeatable): `host:port`, `socks5://…`, `socks5h://…`, `http://…`
  and `https://…` entries. Unsupported schemes (`socks4a://`) are skipped, not fatal.
- `--count`: how many subscription outbounds to expose.
- `--refresh`: seconds between probe rounds (default 300).
- `--concurrency`: parallel probes (default 12).
- `--probe-url` (repeatable): override the TTFB probe targets.

## Pool policy (S-tier behaviour)

- **Fastest first**: ranking is `latency_EWMA + 120 ms × failures`, so a proven
  fast exit always outranks an unmeasured one, and a flapping exit sinks.
- **Nothing is banned forever**: dead exits are parked for 10 min, flagged ones
  (`429`/quota) for 30 min, then they are re-probed automatically.
- **No hangs**: every probe has a hard timeout (`--probe-timeout`, default 8 s),
  and a whole round is bounded by `concurrency`, not by the number of exits.
- **Supervision**: if sing-box exits, the pool restarts it and keeps writing a
  usable list; the plugin meanwhile falls back to IPv6/direct for its own copies
  of the endpoints.

## Development

```bash
uv sync
uv run pytest -q
uv run ruff check src tests
```

CI-free by design: the unit tests cover ranking, cooldown/recovery, state
round-trips and list normalisation without any network access.

## Operational notes

- Managed by the user unit `zen-egress-pool.service`
  (`~/.config/systemd/user/zen-egress-pool.service`).
- The IPv6 `/48` source pool lives outside this repo:
  `/usr/local/bin/he-ipv6-pool.sh` + `he-ipv6.service` (HE tunnel 1041607,
  routed `2001:470:52fa::/48`).
- The translation layer and rotation logic live in
  `~/code/cliproxy-rotator` (a CLIProxyAPI plugin, stock upstream host).

## What is deliberately *not* in this repository

`profiles/` (Hiddify/sing-box exports), subscription URLs, proxy lists and usage
stats are local data: they carry real proxy credentials and change constantly.
Generate them yourself and point the CLI at them:

```bash
zen-egress-pool --profile /path/to/your-profile.json --once --verbose
```

Anything the pool writes (`socks.txt`, `pool-state.json`) lives under
`~/.cache/zen-egress/` and is never committed.
