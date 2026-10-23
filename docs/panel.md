<div align="center">

<img src="https://github.com/openlibrecommunity/material/blob/master/olcrtc.png" width="250" height="250">

[RU](panel.ru.md) / **EN**

</div>

# Server web panel

<!-- ai-generated: the whole file (panel). -->

The panel is built into the olcrtc server: no separate process, no frontend server. It adds rooms, issues client keys, `olcrtc://` links and subscriptions, and shows room state and client traffic.

- **Rooms.** The server sits in every enabled room at once, a server per room in the same process. A room added starts at once; a room disabled or deleted is left. A new link, provider or transport restarts the room's server, a new name does not. Providers that create rooms themselves (`salutejazz`) get a "create on provider" button; for `vkcalls`, create the call in the VK app and paste its join link `https://vk.ru/call/join/<id>`.
- **Clients.** Each client has its own key in the server's `crypto.keys` ring; it opens every room. A disabled or deleted client is cut off in every room at once: its sessions end and new ones fail the handshake.
- **Links.** A client gets one [uri.md](uri.md) link per room, labelled `client name / room name`.
- **Subscription.** `https://<host>/sub/<token>` in the [sub.md](sub.md) format lists every enabled room; a room added reaches the client at its next refresh (`#refresh: 1h`). It lives outside the panel's secret path and does not give it away. "New subscription" rotates the token.
- **State and traffic.** Each room shows what its server does: in the call, connecting, reconnecting, error (hover for the message), disabled. Client traffic counts from server start.

The config's own room and keys (`room.id`, `crypto.key`, `crypto.keys`) keep working beside the panel's; the config room shows as "from config" and is not editable, so existing client links survive turning the panel on.

## Settings

| Field | Meaning |
|---|---|
| `panel.listen` | `host:port`; empty disables the panel |
| `panel.path` | secret URL prefix, 8+ of letters, digits, `-`, `_`; everything else gets 404 |
| `panel.password` | admin password, 12+ characters |
| `panel.store` | rooms and clients file, relative to the config; default `panel.json`, written atomically, mode 0600 |
| `panel.cert` / `panel.key` | panel TLS; required when `listen` is not loopback |
| `panel.public_url` | base of subscription addresses, e.g. `https://vpn.example.com:8884`; default is the address the panel was opened at |

The panel needs `mode: srv` without `profiles`. `room.id` and the key may be left out. `stats.listen` is served by the config room's server only, with the meter shared by all rooms.

## Access and install

By default the panel listens on loopback: `ssh -L 8884:127.0.0.1:8884 root@<server>`, then open `http://127.0.0.1:8884/<path>/`. A public panel refuses to start without TLS. Logins lock an address out for 15 minutes after 5 failures; the session cookie is `HttpOnly`, `SameSite=Strict` and `Secure` over TLS; changes need the `X-Olcrtc-Panel` header; the CSP is strict.

`scripts/build-server.sh` builds a Linux archive (amd64, arm64) with the binary, `install-server.sh` and checksums. On the server: `sudo ./install-server.sh` creates the `olcrtc` user, `/etc/olcrtc`, a config with a random path and password and a hardened `olcrtc` unit, and prints how to reach the panel. `--update` backs up, swaps the binary, checks the service and rolls back on failure; `--uninstall` removes the unit and keeps the data. `scripts/test-install-server.sh` runs the installer in a systemd container.
