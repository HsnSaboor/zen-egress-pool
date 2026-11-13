"""Egress-pool provider for cliproxy-rotator.

Keeps the transport plumbing (subscription profiles, the sing-box SOCKS
multiplexer, probing and exit scoring) and nothing else. API-format
translation is CLIProxyAPI's job, so no server code lives here.
"""

__version__ = "0.2.0"
