# lzt-broker-stall-reaper

TCP-level watchdog for the self-hosted GitHub Actions runner that
detects when the broker long-poll socket is stalled (ESTABLISHED but
zero bytes received) and force-closes it at the OS level.

## Why

GitHub's V2 broker (`broker.actions.githubusercontent.com:443`) can
hold an HTTP long-poll request open **indefinitely** without
responding. The runner's `GetRunnerMessageAsync` task therefore
never returns null and never triggers application-level reconnect
logic. The runner enters a zombie state where:

- The TCP connection is `ESTABLISHED`
- `bytes_received` does not change
- No job assignment arrives
- The runner does not detect the stall and never reconnects

This is the complement to the
[BrokerMessageListener.cs stall-detector patch](../runner-1/docs/LZT-STALL-DETECTOR.md):
the C# patch covers the case where the broker returns null; this
binary covers the case where the broker holds the long-poll open
forever.

## What it does

1. Every 30 s, enumerate TCP connections from `Runner.Listener` to
   `broker.actions.githubusercontent.com:443` via `ss -tnpi`.
2. Track `bytes_received` per connection over time
   (`/var/run/lzt-broker-stall-reaper/<local_addr>.state`).
3. If `bytes_received` is unchanged for >5 min while the connection
   is still ESTABLISHED and no `Runner.Worker` process is alive
   (no job in flight) → `ss --kill state established dst :443`
   (force TCP RST via `tcp_diag`).
4. Cooldown-gated runner service restart (default 30 min) if the
   stall persists.
5. Emit Prometheus textfile metrics to
   `/var/lib/prometheus/node-exporter/lzt-broker-stall-reaper.prom`.

## Install

```bash
go build -trimpath -ldflags="-s -w" -o lzt-broker-stall-reaper \
    ./cmd/lzt-broker-stall-reaper/

sudo install -m 0755 lzt-broker-stall-reaper /usr/local/bin/

sudo install -m 0644 systemd/lzt-broker-stall-reaper.service /etc/systemd/system/
sudo install -m 0644 systemd/lzt-broker-stall-reaper.timer /etc/systemd/system/

sudo systemctl daemon-reload
sudo systemctl enable --now lzt-broker-stall-reaper.timer
```

## Verify

```bash
# One-shot check
sudo lzt-broker-stall-reaper -once

# Inspect state
ls -la /var/run/lzt-broker-stall-reaper/
cat /var/lib/prometheus/node-exporter/lzt-broker-stall-reaper.prom

# Tail journal
journalctl -u lzt-broker-stall-reaper.service -f
```

## Configuration

All flags override defaults:

| Flag | Default | Notes |
|---|---|---|
| `-interval` | 30 s | Watch loop period |
| `-stall` | 5 min | No-bytes-received threshold |
| `-runner-proc` | `Runner.Listener` | Substring to filter ss output |
| `-runner-service` | `actions.runner.LOUST-PRO.vps-runner-1.service` | systemd service to restart after kill |
| `-state-dir` | `/var/run/lzt-broker-stall-reaper` | Per-connection state files |
| `-prom-dir` | `/var/lib/prometheus/node-exporter` | Prometheus textfile collector |
| `-restart-cooldown` | 30 min | Min time between runner restarts |
| `-syslog` | `true` | Log via `logger(1)` to syslog/journald |
| `-once` | `false` | Single check then exit (smoke test mode) |

## Security notes

- `ss --kill` requires `CAP_NET_ADMIN` (the unit grants it via
  `CapabilityBoundingSet=`).
- The runner service restart uses `sudo` NOPASSWD rule on
  `systemctl restart actions.runner.*` (configure separately).
- The binary does NOT log credentials, OAuth tokens, or
  per-request bodies.
- The binary only writes its own state dir + the Prometheus
  textfile dir. No other filesystem access.

## Architecture context

This binary is the third layer of a three-layer stall recovery
strategy on the VPS self-hosted runner:

1. **C# patch** in
   `runner-1/src/Runner.Listener/BrokerMessageListener.cs`:
   application-level stall detection (when broker returns null).
2. **`lzt-runner-broker-watch.sh`**: diag-log-age-based restart
   watch (covers everything else, broad fallback).
3. **`lzt-broker-stall-reaper` (this binary)**: TCP-level stall
   detection (kills the underlying socket when broker holds the
   long-poll open indefinitely).

Layer 3 is the only one that can recover from a TCP socket that
neither the application nor the diag log notice as broken.

## License

Apache-2.0. See `LICENSE`.
