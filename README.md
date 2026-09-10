# centauri-obico-bridge

**Use [Obico](https://www.obico.io/) (AI failure detection, remote control,
webcam) with an Elegoo Centauri Carbon running its stock firmware — no
flashing, no OpenCentauri/COSMOS, no firmware modification.**

This project is a small Docker container that speaks the printer's native
**SDCP** protocol and exposes a **Moonraker-compatible API**, which is exactly
what `moonraker-obico` expects. Everything happens on the network and in
Docker, so it is fully reversible: stop the container and your printer is back
to stock.

```
┌──────────────────────────────┐
│  Elegoo Centauri Carbon      │
│  (stock firmware)            │
│                              │
│  SDCP WebSocket  :3030       │
│  MJPEG camera    :3031/video │
└───────────────┬──────────────┘
                │
                ▼
┌──────────────────────────────┐
│  centauri-obico-bridge       │  ← this project (port 7125)
│  SDCP  ⇄  Moonraker API      │
└───────────────┬──────────────┘
                │  Moonraker HTTP + WebSocket JSON-RPC
                ▼
┌──────────────────────────────┐
│  moonraker-obico (agent)     │
└───────────────┬──────────────┘
                │
                ▼
┌──────────────────────────────┐
│  Obico (cloud or self-hosted)│
└──────────────────────────────┘
```

## Why

Obico only talks to printers through OctoPrint or Moonraker. The Centauri
Carbon speaks SDCP (Chitu's Smart Device Control Protocol) and has no Moonraker
server. This bridge translates between the two so you get:

- AI failure detection (spaghetti detection) with automatic **pause**
- Live webcam in the Obico app
- Print control: **start / pause / resume / cancel**
- Print status, progress, temperatures, layer info
- Upload a G-code to Obico and print it on the Centauri

## Features

| Feature | Status |
| --- | --- |
| Live status (progress, layers, temps, position) | ✅ |
| Webcam MJPEG stream + JPEG snapshot | ✅ |
| Obico failure detection → pause | ✅ |
| Start / pause / resume / cancel | ✅ |
| Upload & print G-code from Obico | ✅ |
| Job history / metadata | ✅ |
| Moonraker WebSocket JSON-RPC | ✅ |
| No firmware modification | ✅ |
| Fully reversible | ✅ |

## Requirements

- An Elegoo Centauri Carbon on your LAN with the stock firmware.
- A host that can run Docker (Linux recommended; the agent uses host
  networking, so Docker Desktop on macOS/Windows is not supported).
- An Obico account: [cloud](https://app.obico.io) or a
  [self-hosted server](https://github.com/TheSpaghettiDetective/obico-server).

## Quick install (script)

```bash
git clone https://github.com/<your-user>/centauri-obico-bridge.git
cd centauri-obico-bridge
./install.sh
```

The script will:

1. Check for Docker/Compose (and offer to install Docker if missing).
2. Ask for your **printer IP** and your **Obico server URL**.
3. Generate `.env` and `config/moonraker-obico.cfg`.
4. Build and start the bridge **and** `moonraker-obico`.
5. Wait until the bridge is healthy.

Then link the printer to Obico:

```bash
./install.sh --link
```

Open the Obico app/web UI, add a Klipper printer, and enter the 6-digit code
when prompted. That's it.

Useful commands:

```bash
./install.sh --status     # containers + bridge health
./install.sh --link       # link / re-link to Obico
docker compose logs -f    # follow logs
docker compose down       # stop everything
```

## Manual install

Prefer to understand every step? See
**[docs/MANUAL_INSTALL.md](docs/MANUAL_INSTALL.md)**. In short:

```bash
cp .env.example .env
# edit CENTAURI_HOST (and OBICO_SERVER_URL if self-hosting)
cp config/moonraker-obico.cfg.template config/moonraker-obico.cfg
docker compose up -d --build
```

If you already run `moonraker-obico` elsewhere, use the bridge only:

```bash
docker compose -f docker-compose.bridge-only.yml up -d --build
```

## Configuration

All settings live in `.env` (see `.env.example`):

| Variable | Default | Description |
| --- | --- | --- |
| `CENTAURI_HOST` | – | **Required.** Printer IP or hostname |
| `CENTAURI_SDCP_PORT` | `3030` | SDCP WebSocket port |
| `CENTAURI_VIDEO_PORT` | `3031` | MJPEG camera port |
| `START_CALIBRATION` | `1` | Bed levelling on `START_PRINT` (1/0) |
| `OBICO_SERVER_URL` | `https://app.obico.io` | Used to generate the agent config |

Additional bridge options (advanced, set in `docker-compose.yml`):

| Variable | Default | Description |
| --- | --- | --- |
| `POLL_INTERVAL` | `2` | Seconds between status polls |
| `BRIDGE_PUBLIC_URL` | *(auto)* | Public URL used in `/server/webcams/list` |
| `SDCP_RAW_LOG` | – | Path to append raw SDCP traffic (JSONL) |

## How it works

The bridge connects to `ws://<printer>:3030/websocket` and:

- keys every SDCP response by `Cmd` + `RequestID` (they all share the same
  `sdcp/response/<MainboardID>` topic),
- keeps the latest `sdcp/status/<MainboardID>` and
  `sdcp/attributes/<MainboardID>`,
- maps `PrintInfo.Status` to Moonraker `print_stats.state`,
- translates Moonraker print commands back into SDCP commands,
- proxies the camera and extracts JPEG snapshots from the MJPEG stream,
- proxies G-code uploads to the printer's HTTP upload endpoint.

### Exposed Moonraker API (port 7125)

`/server/info`, `/printer/info`, `/printer/objects/{list,query,subscribe}`,
`/server/files/{list,metadata,upload}`, `/server/history/list`,
`/server/webcams/list`, `/server/database/item`, `/machine/update/status`,
`/printer/print/{start,pause,resume,cancel}`, `/printer/gcode/script`, and a
Moonraker-compatible WebSocket at `/websocket`.

### Camera

- `GET /webcam/stream` – MJPEG proxy
- `GET /webcam/snapshot` – single JPEG frame (cached)

### Diagnostics

- `GET /health`
- `GET /debug/state` – parsed status / attributes / objects
- `GET /debug/raw` – SDCP responses keyed by Cmd/RequestID
- `GET /debug/messages` – last raw SDCP messages

## Troubleshooting

**The bridge reports `centauri_connected: false`.**
Check the printer IP and that ports 3030/3031 are reachable:
`curl http://<printer>:3031/video` should stream MJPEG.

**Obico shows the printer offline.**
Check `docker compose logs -f moonraker-obico`. The agent must reach the bridge
on `127.0.0.1:7125` (it uses host networking).

**A print started from Obico doesn't seem to launch.**
The printer runs a long calibration (nozzle PID tuning + bed levelling) before
actually printing — up to ~5 minutes. During that time the state is reported
as `printing` but pause/cancel are ignored by the firmware. Wait for the
nozzle to reach temperature and the first layer to begin.

**Uploading a G-code fails.**
The printer only accepts uploads while idle, not during a print.

## Limitations

- SDCP is a reverse-engineered, undocumented protocol. Mappings were validated
  against firmware **V1.4.49**; other firmware versions may differ.
- Obico's failure detection **pauses** the printer; it never auto-cancels.
- During the printer's calibration phase, pause/cancel commands are ignored by
  the firmware.
- `server/files/delete`, timelapse and history video are not implemented.
- Jog / home / temperature commands are translated best-effort.

## Reverse engineering

How the protocol was mapped (SDCP command IDs, status codes, upload format) is
documented in [docs/REVERSE_ENGINEERING.md](docs/REVERSE_ENGINEERING.md).

## License

[MIT](LICENSE). Not affiliated with Elegoo or Obico. `moonraker-obico` is a
separate GPL-3.0 project; this repository only runs its published image.
