#!/usr/bin/env bash
set -euo pipefail

# Cloudflare WARP installs its policy rule ahead of Tailscale's own bypass
# rule. Restore Tailscale's fwmark precedence so its WireGuard UDP packets use
# the physical route instead of being captured by WARP.
if ! ip -4 rule show | grep -E '^5208:.*fwmark 0x80000/0xff0000.*lookup main' >/dev/null; then
  ip -4 rule add priority 5208 fwmark 0x80000/0xff0000 lookup main
fi

# WARP also owns base chains with a drop policy. Permit only Tailscale's UDP
# socket through those chains. The checks make this safe to run repeatedly and
# let the timer restore the rules after a WARP reconnect.
if nft list table inet cloudflare-warp >/dev/null 2>&1; then
  if ! nft -a list chain inet cloudflare-warp input | grep -F 'tailscale-direct-udp' >/dev/null; then
    nft insert rule inet cloudflare-warp input udp dport 41641 accept comment 'tailscale-direct-udp'
  fi
  if ! nft -a list chain inet cloudflare-warp output | grep -F 'tailscale-direct-udp' >/dev/null; then
    nft insert rule inet cloudflare-warp output udp sport 41641 accept comment 'tailscale-direct-udp'
  fi
fi
