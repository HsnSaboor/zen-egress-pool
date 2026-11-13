"""Unified sing-box subprocess manager running all outbounds under one local port."""

from __future__ import annotations

import atexit
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.error import URLError

from zen_proxy.json_types import DocDict
from zen_proxy.profile import OutboundNode


class SingBoxMultiplexer:
    """Manages a single sing-box process multiplexing N outbounds via SOCKS5 users."""

    def __init__(
        self,
        outbounds: list[OutboundNode],
        listen_host: str = "127.0.0.1",
        socks_port: int = 20800,
        binary: str = "sing-box",
        log_level: str = "warn",
    ) -> None:
        self.outbounds = outbounds
        self.listen_host = listen_host
        self.socks_port = socks_port
        self.binary = binary
        self.log_level = log_level

        self.temp_dir = Path(tempfile.mkdtemp(prefix="zen_sb_"))
        self.config_path = self.temp_dir / "config.json"
        self.log_path = self.temp_dir / "singbox.log"
        self.process: subprocess.Popen[bytes] | None = None
        self._is_stopped = False

        atexit.register(self.stop)

    def _get_singbox_version(self) -> tuple[int, int]:
        try:
            r = subprocess.run([self.binary, "version"], capture_output=True, text=True, timeout=2.0, check=False)
            m = re.search(r"version\s+(\d+)\.(\d+)", r.stdout)
            if m:
                return int(m.group(1)), int(m.group(2))
        except (OSError, subprocess.SubprocessError, ValueError, URLError):
            pass
        return 1, 14

    def _build_config(self) -> DocDict:
        users: list[DocDict] = []
        rules: list[DocDict] = []
        sb_outbounds: list[DocDict] = []

        major, minor = self._get_singbox_version()
        is_modern = (major, minor) >= (1, 12)

        for node in self.outbounds:
            user = node.auth_user
            users.append({"username": user, "password": "p"})
            rules.append({"auth_user": [user], "outbound": node.tag})

            ob_cfg = dict(node.raw_config)
            ob_cfg.setdefault("tcp_fast_open", True)
            ob_cfg.setdefault("tcp_multi_path", True)
            if is_modern:
                ob_cfg.pop("domain_strategy", None)
            else:
                if ob_cfg.get("type") in {"vless", "vmess", "trojan", "shadowsocks", "hysteria2", "tuic", "wireguard"}:
                    ob_cfg.setdefault("domain_strategy", "prefer_ipv4")
            sb_outbounds.append(ob_cfg)

        sb_outbounds.append({"type": "direct", "tag": "direct"})

        if is_modern:
            dns_servers: list[DocDict] = [
                {"tag": "dns-remote", "type": "udp", "server": "1.1.1.1"},
                {"tag": "dns-local", "type": "local"},
            ]
            dns_config: DocDict = {
                "servers": dns_servers,
                "strategy": "prefer_ipv4",
            }
            route_config: DocDict = {
                "rules": rules,
                "auto_detect_interface": True,
                "default_domain_resolver": "dns-remote",
                "final": "direct",
            }
        else:
            dns_servers = [
                {"tag": "dns-remote", "address": "1.1.1.1", "detour": "direct"},
                {"tag": "dns-local", "address": "local", "detour": "direct"},
            ]
            dns_config = {
                "servers": dns_servers,
                "strategy": "prefer_ipv4",
                "independent_cache": True,
            }
            route_config = {
                "rules": rules,
                "auto_detect_interface": True,
                "final": "direct",
            }

        return {
            "log": {
                "level": self.log_level,
                "output": str(self.log_path),
                "timestamp": True,
            },
            "dns": dns_config,
            "inbounds": [
                {
                    "type": "socks",
                    "tag": "socks-in",
                    "listen": self.listen_host,
                    "listen_port": self.socks_port,
                    "users": users,
                    "sniff": False,
                    "tcp_fast_open": True,
                    "tcp_multi_path": True,
                }
            ],
            "outbounds": sb_outbounds,
            "route": route_config,
        }

    def _port_in_use(self) -> bool:
        """True when anything (including a stale leftover) accepts on our port."""
        return self._check_socket()

    def _kill_stale_multiplexers(self) -> None:
        """Terminate leftover sing-box processes created by an earlier run.

        A previous pool instance killed with SIGKILL can leave its multiplexer
        behind. It keeps listening on our port, so a new start() would appear to
        succeed while every SOCKS5 user is unknown to the surviving process.
        Only processes running a ``/zen_sb_*/config.json`` config are touched.
        """
        try:
            listing = subprocess.run(
                ["ps", "-eo", "pid=,args="], capture_output=True, text=True, timeout=5.0, check=False
            ).stdout
        except (OSError, subprocess.SubprocessError):  # pragma: no cover
            return
        for line in listing.splitlines():
            line = line.strip()
            if "zen_sb_" not in line or "sing-box" not in line:
                continue
            pid_text = line.split(None, 1)[0]
            if not pid_text.isdigit() or int(pid_text) == os.getpid():
                continue
            try:
                os.kill(int(pid_text), signal.SIGTERM)
            except OSError:  # pragma: no cover
                continue

    def _ensure_port_free(self, wait_sec: float = 3.0) -> None:
        if not self._port_in_use():
            return
        self._kill_stale_multiplexers()
        deadline = time.time() + wait_sec
        while time.time() < deadline:
            if not self._port_in_use():
                return
            time.sleep(0.1)
        raise RuntimeError(
            f"{self.listen_host}:{self.socks_port} is already in use by another process; "
            "refusing to share a multiplexer we do not own"
        )

    def start(self, timeout_sec: float = 6.0) -> None:
        """Generate configuration, validate, and launch the multiplexer."""
        self._ensure_port_free()
        config = self._build_config()
        with open(self.config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2)

        # Pre-flight check
        check = subprocess.run(
            [self.binary, "check", "-c", str(self.config_path)],
            capture_output=True,
            text=True,
            check=False,
        )
        if check.returncode != 0:
            err = check.stderr.strip() or check.stdout.strip()
            raise RuntimeError(f"Sing-box config check failed:\n{err}")

        # Launch process
        self.process = subprocess.Popen(
            [self.binary, "run", "-c", str(self.config_path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Poll port until active
        t0 = time.time()
        while time.time() - t0 < timeout_sec:
            if self.process.poll() is not None:
                log_content = self.log_path.read_text(errors="ignore")[-800:] if self.log_path.exists() else ""
                raise RuntimeError(
                    f"sing-box terminated immediately (code {self.process.returncode}). Log:\n{log_content}"
                )
            if self._check_socket():
                # A process can bind and die within the same tick; confirm ours
                # is still alive before reporting success.
                time.sleep(0.15)
                if self.process.poll() is not None:
                    log_content = self.log_path.read_text(errors="ignore")[-800:] if self.log_path.exists() else ""
                    raise RuntimeError(
                        f"sing-box exited right after binding (code {self.process.returncode}). Log:\n{log_content}"
                    )
                return
            time.sleep(0.05)

        self.stop()
        raise TimeoutError(f"sing-box did not bind {self.listen_host}:{self.socks_port} within {timeout_sec}s")

    def _check_socket(self) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.1)
            try:
                return s.connect_ex((self.listen_host, self.socks_port)) == 0
            except OSError:
                return False

    def stop(self) -> None:
        """Terminate the sing-box process and clean temporary directories."""
        if self._is_stopped:
            return
        self._is_stopped = True

        if self.process is not None:
            try:
                if self.process.poll() is None:
                    self.process.terminate()
                try:
                    self.process.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    try:
                        self.process.kill()
                        self.process.wait(timeout=2.0)
                    except (OSError, subprocess.TimeoutExpired):  # pragma: no cover
                        pass
            except OSError:  # pragma: no cover
                pass
            self.process = None

        if self.temp_dir.exists():
            try:
                shutil.rmtree(self.temp_dir, ignore_errors=True)
            except OSError:
                pass
                pass
