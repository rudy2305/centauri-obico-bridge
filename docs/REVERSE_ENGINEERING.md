# Reverse engineering notes

Everything here was determined from the stock Centauri Carbon web UI
(`main.js` / the lazy-loaded `chunk25.js`) and from live captures of the SDCP
WebSocket, without modifying the firmware.

## SDCP transport

- WebSocket: `ws://<printer>:3030/websocket`
- Camera: `http://<printer>:3031/video` (MJPEG, no snapshot endpoint)
- Keepalive: send the literal string `ping` periodically.

Outgoing envelope:

```json
{"Id":"","Data":{"Cmd":128,"Data":{...},"RequestID":"<hex>","MainboardID":"","TimeStamp":123,"From":1}}
```

Incoming messages are routed by `Topic`:

| Topic | Meaning |
| --- | --- |
| `sdcp/status/<MainboardID>` | printer state (top-level `Status` key) |
| `sdcp/attributes/<MainboardID>` | static info (top-level `Attributes` key) |
| `sdcp/response/<MainboardID>` | command responses |
| `sdcp/error/<MainboardID>` | errors |

**Important:** every command response uses the same
`sdcp/response/<MainboardID>` topic, so responses must be matched by
`RequestID` (and/or `Cmd`). Naive bridges that store "the last response" lose
data.

## Command IDs (from the stock UI)

```
0   GET_PRINTER_STATUS          192 SEND_PRINTER_EDIT_NAME
1   GET_PRINTER_ATTR            257 EDIT_PRINTER_FILE_NAME
64  SEND_PRINTER_DISCONNECT     258 GET_PRINTER_FILE_LIST
128 START_PRINT                 259 DELETE_PRINTER_FILE_LIST
129 SUSPEND_PRINT               260 GET_PRINTER_FILE_DETAIL
130 STOP_PRINT                  320 GET_PRINTER_HISTORY_ID
131 RESTORE_PRINT               321 GET_PRINTER_TASK_DETAIL
134 GET_BLACKOUT_STATUS         322 DELETE_PRINTER_HISTORY
135 SEND_BLACKOUT_ACTION        323 GET_PRINTER_HISTORY_VIDEO
255 SEND_PRINTER_SEND_FILE_END  324 GET_MATERIAL_DATA
386 EDIT_PRINTER_VIDEO_STREAMING
387 EDIT_PRINTER_TIME_LAPSE_STATUS
401 EDIT_PRINTER_AXIS_NUMBER
402 EDIT_PRINTER_AXIS_ZERO
403 EDIT_PRINTER_STATUS_DATA
503 GET_FILE_COLOR_DATA
```

## Status model

`Status` looks like:

```json
{
  "CurrentStatus": [1],
  "TempOfNozzle": 209.9, "TempTargetNozzle": 210,
  "TempOfHotbed": 60.1, "TempTargetHotbed": 60,
  "TempOfBox": 32.0, "TempTargetBox": 0,
  "CurrenCoord": "134.41,120.71,8.32",
  "CurrentFanSpeed": {"ModelFan": 98, "AuxiliaryFan": 0, "BoxFan": 68},
  "LightStatus": {"SecondLight": 1, "RgbLight": [0,0,0]},
  "PrintInfo": {
    "Status": 13, "CurrentLayer": 45, "TotalLayer": 240,
    "CurrentTicks": 458.2, "TotalTicks": 2068,
    "Filename": "...gcode", "TaskId": "<uuid>",
    "PrintSpeedPct": 100, "Progress": 20
  }
}
```

`PrintInfo.Status` → UI state (from `chunk25.js`):

| Codes | State |
| --- | --- |
| `13` | printing |
| `5`, `6` | pausing / paused |
| `9` | complete |
| `8`, `14` | stopped / cancelled |
| `0` | idle |
| `1`, `10`, `12`, `15`, `16`, `18`, `19`, `20`, `21` | preparing (heating, homing, levelling, PID tuning, file check) |

`CurrentStatus` is an array of active flags (`0` idle, `1` printing, …).

`Attributes` includes `Name`, `FirmwareVersion`, `MainboardIP`,
`MainboardMAC`, `Capabilities`, `CameraStatus`, etc.

## Print commands

`START_PRINT` (128) payload as built by the stock UI:

```json
{
  "Filename": "/local/MODEL.gcode",
  "StartLayer": 0,
  "Calibration_switch": 1,
  "PrintPlatformType": 0,
  "Tlp_Switch": 0,
  "slot_map": []
}
```

- `Calibration_switch`: bed levelling on/off
- `PrintPlatformType`: textured / smooth plate
- `Tlp_Switch`: timelapse
- `slot_map`: AMS colour mapping (empty without an AMS)

`SUSPEND_PRINT` (129), `STOP_PRINT` (130) and `RESTORE_PRINT` (131) take an
empty `Data`.

Response `Ack` values for `START_PRINT` (from the UI i18n):

| Ack | Meaning |
| --- | --- |
| `0` | started |
| `1` | device busy |
| `2` | file not found |
| `3` | MD5 check failed |
| `4` | file reading failed |
| `5` | invalid resolution |
| `6` | invalid format |
| `7` | invalid model |

## Settings via EDIT_PRINTER_STATUS_DATA (403)

```json
{"TempTargetNozzle": 210}
{"TempTargetHotbed": 60}
{"TargetFanSpeed": {"ModelFan": 100, "AuxiliaryFan": 0, "BoxFan": 0}}
{"LightStatus": {"SecondLight": 1, "RgbLight": [0,0,0]}}
{"PrintSpeedPct": 100}
```

Axis move / home:

```json
{"Axis": "Z", "Step": -0.1}     // EDIT_PRINTER_AXIS_NUMBER (401)
{"Axis": "XYZ"}                  // EDIT_PRINTER_AXIS_ZERO (402)
```

## File upload is HTTP, not SDCP

The stock UI uploads to `POST http://<printer>/uploadFile/upload` in 1 MiB
chunks with multipart fields:

```
TotalSize, Uuid, Offset, Check, S-File-MD5, File
```

The file is stored flat under `/local/<name>`. Uploads are rejected while the
printer is printing.

File metadata (layers, estimated time/weight, thumbnail) comes from
`GET_PRINTER_FILE_DETAIL` (260) with `{"Url": "<path>"}`. Job history comes
from `GET_PRINTER_HISTORY_ID` (320) then `GET_PRINTER_TASK_DETAIL` (321) with
`{"Id": ["<uuid>"]}`.

## Moonraker compatibility

`moonraker-obico` expects, among others:

- `GET /server/info` → `{"result": {"klippy_state": "ready", ...}}`
- `GET /printer/objects/query?heaters=` → `result.status.heaters.available_heaters`
- `GET /printer/objects/list`, `printer.objects.subscribe/query` (WS JSON-RPC)
- `GET /server/files/metadata`, `POST /server/files/upload`
- `GET /server/history/list`, `GET /server/webcams/list`
- `POST /printer/print/{start,pause,resume,cancel}` (parameters sent as
  **form data**, not JSON)

The bridge implements all of these and wraps every REST reply in a `result`
key.
