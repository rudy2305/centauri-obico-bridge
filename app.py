"""
centauri-obico-bridge
=====================

A Moonraker-compatible bridge for the stock Elegoo Centauri Carbon.

The printer speaks SDCP over WebSocket (ws://HOST:3030/websocket) and exposes
its camera as an MJPEG stream (http://HOST:3031/video).  moonraker-obico only
understands Moonraker, so this process translates between the two.

Everything is read-only against the printer firmware: no flashing, no firmware
modification.  All state is kept in this process and the deployment is fully
reversible (stop the container).

Reverse-engineering references:
  * main.js / chunk25.js (stock Elegoo web UI) - command IDs and status codes
  * raw.json - SDCP capture
  * https://github.com/TheSpaghettiDetective/moonraker-obico - API consumer
"""

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import pathlib
import re
import time
import uuid
from collections import OrderedDict, deque
from contextlib import asynccontextmanager
from typing import Any, Optional

import httpx
import uvicorn
import websockets
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response, StreamingResponse

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


HOST = _env("CENTAURI_HOST", "192.168.1.43")
SDCP_PORT = int(_env("CENTAURI_SDCP_PORT", "3030"))
VIDEO_PORT = int(_env("CENTAURI_VIDEO_PORT", "3031"))
MOONRAKER_PORT = int(_env("MOONRAKER_PORT", "7125"))
WEB_PORT = int(_env("WEB_PORT", "8080"))
POLL_INTERVAL = float(_env("POLL_INTERVAL", "2"))
FILELIST_INTERVAL = float(_env("FILELIST_INTERVAL", "60"))
HISTORY_INTERVAL = float(_env("HISTORY_INTERVAL", "20"))
API_KEY = _env("MOONRAKER_API_KEY", "centauri-bridge")
# Optional explicit public base URL used in /server/webcams/list.  When empty the
# bridge derives it from the incoming request, which is what moonraker-obico used.
PUBLIC_URL = _env("BRIDGE_PUBLIC_URL", "").rstrip("/")
RAW_LOG = _env("SDCP_RAW_LOG", "")
# Bed levelling (Calibration_switch) sent on START_PRINT.
START_CALIBRATION = int(_env("START_CALIBRATION", "1"))
START_PLATFORM_TYPE = int(_env("START_PLATFORM_TYPE", "0"))
START_TIMELAPSE = int(_env("START_TIMELAPSE", "0"))

SDCP_WS = f"ws://{HOST}:{SDCP_PORT}/websocket"
VIDEO_URL = f"http://{HOST}:{VIDEO_PORT}/video"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("centauri-bridge")


# --------------------------------------------------------------------------- #
# SDCP command IDs (from the stock UI webpack module 543)
# --------------------------------------------------------------------------- #

CMD_STATUS = 0
CMD_ATTR = 1
CMD_DISCONNECT = 64
CMD_START_PRINT = 128
CMD_SUSPEND_PRINT = 129
CMD_STOP_PRINT = 130
CMD_RESTORE_PRINT = 131
CMD_BLACKOUT_STATUS = 134
CMD_BLACKOUT_ACTION = 135
CMD_EDIT_NAME = 192
CMD_EDIT_FILE_NAME = 257
CMD_FILE_LIST = 258
CMD_DELETE_FILE_LIST = 259
CMD_FILE_DETAIL = 260
CMD_HISTORY_ID = 320
CMD_TASK_DETAIL = 321
CMD_DELETE_HISTORY = 322
CMD_HISTORY_VIDEO = 323
CMD_MATERIAL_DATA = 324
CMD_VIDEO_STREAMING = 386
CMD_TIME_LAPSE = 387
CMD_AXIS_NUMBER = 401
CMD_AXIS_ZERO = 402
CMD_STATUS_DATA = 403
CMD_FILE_COLOR = 503

CMD_NAMES = {
    CMD_STATUS: "GET_PRINTER_STATUS",
    CMD_ATTR: "GET_PRINTER_ATTR",
    CMD_START_PRINT: "START_PRINT",
    CMD_SUSPEND_PRINT: "SUSPEND_PRINT",
    CMD_STOP_PRINT: "STOP_PRINT",
    CMD_RESTORE_PRINT: "RESTORE_PRINT",
    CMD_FILE_LIST: "GET_PRINTER_FILE_LIST",
    CMD_FILE_DETAIL: "GET_PRINTER_FILE_DETAIL",
    CMD_HISTORY_ID: "GET_PRINTER_HISTORY_ID",
    CMD_TASK_DETAIL: "GET_PRINTER_TASK_DETAIL",
    CMD_AXIS_NUMBER: "EDIT_PRINTER_AXIS_NUMBER",
    CMD_AXIS_ZERO: "EDIT_PRINTER_AXIS_ZERO",
    CMD_STATUS_DATA: "EDIT_PRINTER_STATUS_DATA",
}

# PrintInfo.Status -> UI state (chunk25.js printerStatus constants)
STATUS_PRINTING = {13}
STATUS_PAUSED = {5, 6}
STATUS_COMPLETE = {9}
STATUS_CANCELLED = {8, 14}
STATUS_LOADING = {0, 1, 15, 16, 18, 19, 20, 21}
STATUS_FILE_DETECTION = {10}
STATUS_RECOVERY = {12}


def sdcp_envelope(cmd: int, data: Any = None, mainboard_id: str = "") -> dict:
    return {
        "Id": "",
        "Data": {
            "Cmd": cmd,
            "Data": data if data is not None else {},
            "RequestID": uuid.uuid4().hex,
            "MainboardID": mainboard_id,
            "TimeStamp": int(time.time() * 1000),
            "From": 1,
        },
    }


def basename(path: str) -> str:
    return pathlib.PurePosixPath(path or "").name


def normalize_filename(name: str) -> str:
    """Accept Moonraker-style 'gcodes/foo.gcode' and SDCP '/local/foo.gcode'."""
    if not name:
        return name
    name = name.strip()
    if name.startswith("gcodes/"):
        name = "/local/" + name[len("gcodes/"):]
    elif not name.startswith("/local/") and not name.startswith("/"):
        name = "/local/" + name
    return name


# --------------------------------------------------------------------------- #
# Bridge state / SDCP client
# --------------------------------------------------------------------------- #

class CentauriBridge:
    def __init__(self) -> None:
        self.ws: Optional[Any] = None
        self.connected = False
        self.last_rx: Optional[float] = None
        self.last_error: Optional[str] = None
        self.connect_ts: Optional[float] = None

        self.status: dict[str, Any] = {}
        self.attributes: dict[str, Any] = {}
        self.mainboard_id: str = ""

        self.pending: dict[str, asyncio.Future] = {}
        # Raw SDCP traffic, newest last.
        self.messages: deque = deque(maxlen=1000)
        # Response history keyed by Cmd/RequestID so nothing is overwritten.
        self.responses: OrderedDict = OrderedDict()
        self.response_history: deque = deque(maxlen=300)
        self.last_response: dict[int, dict] = {}

        self.file_list: list[dict] = []
        self.file_list_ts: float = 0
        self.file_detail: dict[str, Any] = {}
        self.task_detail: dict[str, Any] = {}
        self.history_ids: list[str] = []
        self.print_started_ts: Optional[float] = None
        # Filename of the print we last started (the printer only reports
        # PrintInfo.Filename once actual printing begins).
        self.active_filename: str = ""

        self.clients: set[WebSocket] = set()
        self._last_notified_state: Optional[str] = None
        self._last_task_id: Optional[str] = None
        self.notify_count = 0
        # Latest JPEG frame, kept fresh by a single persistent MJPEG reader so
        # that /webcam/snapshot does not open a new printer stream per request.
        self.latest_frame: bytes = b""
        self.latest_frame_ts: float = 0.0
        self._raw_fh = None
        if RAW_LOG:
            with contextlib.suppress(Exception):
                self._raw_fh = open(RAW_LOG, "a", buffering=1)

    # -- raw logging ------------------------------------------------------- #

    def _record(self, direction: str, payload: Any) -> None:
        rec = {"ts": time.time(), "dir": direction, "payload": payload}
        self.messages.append(rec)
        if self._raw_fh:
            with contextlib.suppress(Exception):
                self._raw_fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # -- SDCP send / receive ----------------------------------------------- #

    async def send_command(
        self,
        cmd: int,
        data: Any = None,
        *,
        wait: bool = False,
        timeout: float = 8.0,
    ) -> Optional[dict]:
        if not self.ws or not self.connected:
            raise RuntimeError("Not connected to printer SDCP")
        msg = sdcp_envelope(cmd, data, self.mainboard_id)
        rid = msg["Data"]["RequestID"]
        fut: Optional[asyncio.Future] = None
        if wait:
            fut = asyncio.get_running_loop().create_future()
            self.pending[rid] = fut
        self._record("tx", msg)
        await self.ws.send(json.dumps(msg, separators=(",", ":")))
        if not wait:
            return None
        try:
            resp = await asyncio.wait_for(fut, timeout=timeout)
            return resp.get("Data")
        except asyncio.TimeoutError:
            self.pending.pop(rid, None)
            raise TimeoutError(f"No SDCP response for cmd {cmd} within {timeout}s")

    def _handle_message(self, msg: dict) -> None:
        topic = str(msg.get("Topic", ""))
        self.last_rx = time.time()
        self._record("rx", msg)

        if "status" in topic and isinstance(msg.get("Status"), dict):
            self.status = msg["Status"]
            if msg.get("MainboardID"):
                self.mainboard_id = msg["MainboardID"]
            self._track_filename()
            self._maybe_fetch_task_detail()
            asyncio.create_task(self._notify_if_changed())
        elif "attributes" in topic and isinstance(msg.get("Attributes"), dict):
            self.attributes = msg["Attributes"]
            if msg.get("MainboardID"):
                self.mainboard_id = msg["MainboardID"]
        elif "response" in topic:
            d = msg.get("Data") or {}
            cmd = d.get("Cmd")
            rid = d.get("RequestID")
            key = f"{cmd}:{rid}"
            self.responses[key] = msg
            if len(self.responses) > 400:
                self.responses.popitem(last=False)
            self.response_history.append(msg)
            if cmd is not None:
                self.last_response[cmd] = d
            self._process_response(d)
            fut = self.pending.pop(rid, None) if rid else None
            if fut and not fut.done():
                fut.set_result(d)
        elif "error" in topic:
            self.responses[f"error:{time.time()}"] = msg
            self.response_history.append(msg)

    def _process_response(self, d: dict) -> None:
        cmd = d.get("Cmd")
        payload = d.get("Data") or {}
        if cmd == CMD_FILE_LIST and payload.get("Ack") == 0:
            self.file_list = payload.get("FileList") or []
            self.file_list_ts = time.time()
        elif cmd == CMD_FILE_DETAIL and payload.get("Ack") == 0:
            info = payload.get("FileInfo") or {}
            if info:
                self.file_detail = info
        elif cmd == CMD_TASK_DETAIL and payload.get("Ack") == 0:
            details = payload.get("HistoryDetailList") or []
            if details:
                self.task_detail = details[0]
        elif cmd == CMD_HISTORY_ID and payload.get("Ack") == 0:
            self.history_ids = payload.get("HistoryData") or []

    def _track_filename(self) -> None:
        """Keep a filename available during the printer's prepare phase.

        The printer briefly reports ``standby`` right after START_PRINT and only
        fills ``PrintInfo.Filename`` once actual printing begins, so we must not
        clear the remembered filename on a transient standby.
        """
        info = self.status.get("PrintInfo") or {}
        if info.get("Filename"):
            self.active_filename = info["Filename"]
        elif not info.get("TaskId") and self.map_print_state() in ("cancelled", "complete"):
            self.active_filename = ""
            self.print_started_ts = None

    def _maybe_fetch_task_detail(self) -> None:
        info = self.status.get("PrintInfo") or {}
        task_id = info.get("TaskId")
        if task_id and task_id != self._last_task_id:
            self._last_task_id = task_id
            self.print_started_ts = time.time()
            asyncio.create_task(self._fetch_task_detail(task_id))
            filename = info.get("Filename")
            if filename:
                asyncio.create_task(self._fetch_file_detail(filename))

    async def _fetch_task_detail(self, task_id: str) -> None:
        with contextlib.suppress(Exception):
            await self.send_command(CMD_TASK_DETAIL, {"Id": [task_id]}, wait=False)

    async def _fetch_file_detail(self, filename: str) -> None:
        with contextlib.suppress(Exception):
            await self.send_command(CMD_FILE_DETAIL, {"Url": normalize_filename(filename)}, wait=False)

    async def _notify_if_changed(self) -> None:
        state = self.map_print_state()
        info = self.status.get("PrintInfo") or {}
        layer = info.get("CurrentLayer")
        progress = info.get("Progress")
        signature = (state, layer, progress, info.get("TaskId"), info.get("CurrentTicks"))
        if signature != self._last_notified_state:
            self._last_notified_state = signature
            self.notify_count += 1
            await self.broadcast({
                "jsonrpc": "2.0",
                "method": "notify_status_update",
                "params": [{"print_stats": self.build_print_stats()}, time.time()],
            })

    async def broadcast(self, message: dict) -> None:
        if not self.clients:
            return
        text = json.dumps(message, separators=(",", ":"))
        dead = []
        for client in list(self.clients):
            try:
                await client.send_text(text)
            except Exception:
                dead.append(client)
        for client in dead:
            self.clients.discard(client)

    # -- connection lifecycle ---------------------------------------------- #

    async def run(self) -> None:
        while True:
            try:
                log.info("Connecting to %s", SDCP_WS)
                async with websockets.connect(
                    SDCP_WS, ping_interval=None, max_size=32 * 1024 * 1024
                ) as ws:
                    self.ws = ws
                    self.connected = True
                    self.connect_ts = time.time()
                    self.last_error = None
                    log.info("Connected to Centauri SDCP")
                    await self._initial_query()
                    await asyncio.gather(
                        self._receiver(ws),
                        self._poller(),
                        self._keepalive(),
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.connected = False
                self.last_error = repr(exc)
                log.warning("SDCP connection lost: %s", exc)
                await asyncio.sleep(3)

    async def _initial_query(self) -> None:
        for cmd, data in (
            (CMD_STATUS, None),
            (CMD_ATTR, None),
            (CMD_HISTORY_ID, None),
            (CMD_FILE_LIST, {"Url": "/local"}),
        ):
            with contextlib.suppress(Exception):
                await self.send_command(cmd, data)

    async def _receiver(self, ws) -> None:
        async for raw in ws:
            if isinstance(raw, bytes):
                continue
            if raw == "pong":
                continue
            try:
                msg = json.loads(raw)
            except Exception:
                log.debug("Non-JSON SDCP message: %r", raw)
                continue
            if isinstance(msg, dict):
                self._handle_message(msg)

    async def _poller(self) -> None:
        last_filelist = 0.0
        last_history = 0.0
        while True:
            with contextlib.suppress(Exception):
                await self.send_command(CMD_STATUS)
                await self.send_command(CMD_ATTR)
                now = time.time()
                if now - last_history > HISTORY_INTERVAL:
                    last_history = now
                    await self.send_command(CMD_HISTORY_ID)
                if now - last_filelist > FILELIST_INTERVAL:
                    last_filelist = now
                    await self.send_command(CMD_FILE_LIST, {"Url": "/local"})
            await asyncio.sleep(POLL_INTERVAL)

    async def _keepalive(self) -> None:
        while True:
            await asyncio.sleep(25)
            with contextlib.suppress(Exception):
                await self.ws.send("ping")

    async def frame_reader(self) -> None:
        """Continuously cache the latest JPEG from the printer MJPEG stream."""
        while True:
            try:
                buf = bytearray()
                async with httpx.AsyncClient(timeout=None) as client:
                    async with client.stream("GET", VIDEO_URL) as upstream:
                        async for chunk in upstream.aiter_bytes():
                            buf.extend(chunk)
                            while True:
                                start = buf.find(b"\xff\xd8")
                                if start < 0:
                                    if len(buf) > 1:
                                        del buf[:-1]
                                    break
                                end = buf.find(b"\xff\xd9", start + 2)
                                if end < 0:
                                    if start > 0:
                                        del buf[:start]
                                    break
                                self.latest_frame = bytes(buf[start:end + 2])
                                self.latest_frame_ts = time.time()
                                del buf[:end + 2]
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.debug("frame reader error: %s", exc)
                await asyncio.sleep(2)

    # -- status mapping ---------------------------------------------------- #

    def _print_info(self) -> dict:
        return self.status.get("PrintInfo") or {}

    def map_print_state(self) -> str:
        if not self.connected:
            return "standby"
        info = self._print_info()
        ps = info.get("Status")
        cs = self.status.get("CurrentStatus") or []
        if ps in STATUS_CANCELLED:
            return "cancelled"
        if ps in STATUS_COMPLETE:
            return "complete"
        if ps in STATUS_PAUSED:
            return "paused"
        if ps in STATUS_PRINTING:
            return "printing"
        if ps == 0:
            return "standby"
        # Preparing / heating / homing / levelling / file checking / recovery.
        if ps in (STATUS_LOADING - {0}) or ps in STATUS_FILE_DETECTION or ps in STATUS_RECOVERY:
            return "printing"
        if 6 in cs:
            return "paused"
        if 1 in cs:
            return "printing"
        if 9 in cs:
            return "complete"
        if 8 in cs:
            return "cancelled"
        return "standby"

    def progress(self) -> float:
        info = self._print_info()
        try:
            v = float(info.get("Progress") or 0)
        except (TypeError, ValueError):
            v = 0.0
        return max(0.0, min(1.0, v / 100.0))

    def build_print_stats(self) -> dict:
        info = self._print_info()
        state = self.map_print_state()
        duration = info.get("CurrentTicks") or 0
        return {
            "state": state,
            "message": "",
            "filename": info.get("Filename") or self.active_filename or "",
            "info": {
                "total_layer": info.get("TotalLayer"),
                "current_layer": info.get("CurrentLayer"),
            },
            "print_duration": duration,
            "total_duration": duration,
            "filament_used": None,
        }

    def build_virtual_sdcard(self) -> dict:
        info = self._print_info()
        progress = self.progress()
        size = 0
        for f in self.file_list:
            if f.get("name") == info.get("Filename"):
                size = f.get("FileSize") or 0
                break
        state = self.map_print_state()
        return {
            "progress": progress,
            "file_position": int(progress * size) if size else 0,
            "is_active": state in ("printing", "paused"),
        }

    def _coord(self) -> list[float]:
        raw = self.status.get("CurrenCoord")
        if not raw:
            return [0.0, 0.0, 0.0, 0.0]
        with contextlib.suppress(Exception):
            parts = [float(x) for x in str(raw).split(",")]
            while len(parts) < 3:
                parts.append(0.0)
            return [parts[0], parts[1], parts[2], 0.0]
        return [0.0, 0.0, 0.0, 0.0]

    def build_gcode_move(self) -> dict:
        info = self._print_info()
        try:
            speed_factor = float(info.get("PrintSpeedPct") or 100) / 100.0
        except (TypeError, ValueError):
            speed_factor = 1.0
        return {
            "speed_factor": speed_factor,
            "extrude_factor": 1.0,
            "gcode_position": self._coord(),
            "absolute_coordinates": True,
            "homing_origin": [0.0, 0.0, 0.0, 0.0],
        }

    def build_heaters(self) -> dict:
        return {
            "available_heaters": ["extruder", "heater_bed"],
            "available_sensors": [],
            "available_monitors": [],
        }

    def build_objects(self) -> dict:
        info = self._print_info()
        state = self.map_print_state()
        progress = self.progress()
        fan = self.status.get("CurrentFanSpeed") or {}
        coord = self._coord()
        try:
            fan_speed = float(fan.get("ModelFan") or 0) / 100.0
        except (TypeError, ValueError):
            fan_speed = 0.0
        jobs = self.build_jobs()
        return {
            "print_stats": self.build_print_stats(),
            "virtual_sdcard": self.build_virtual_sdcard(),
            "display_status": {"progress": progress, "message": ""},
            "gcode_move": self.build_gcode_move(),
            "toolhead": {
                "position": coord,
                "homed_axes": "xyz",
                "extruder": "extruder",
            },
            "fan": {"speed": max(0.0, min(1.0, fan_speed))},
            "extruder": {
                "temperature": self.status.get("TempOfNozzle", 0),
                "target": self.status.get("TempTargetNozzle", 0),
                "power": 0.0,
            },
            "heater_bed": {
                "temperature": self.status.get("TempOfHotbed", 0),
                "target": self.status.get("TempTargetHotbed", 0),
            },
            "heaters": self.build_heaters(),
            "webhooks": {
                "state": "ready" if self.connected else "shutdown",
                "state_message": self.last_error or "",
            },
            "idle_timeout": {"state": "Idle" if state == "standby" else "Printing"},
            "history": {"job_history": {"jobs": jobs, "count": len(jobs)}},
            "configfile": {"config": {}, "settings": {}, "warnings": [], "save_config_pending": False},
        }

    # -- files / metadata / history ---------------------------------------- #

    def find_file(self, filename: str) -> Optional[dict]:
        target = normalize_filename(filename)
        for f in self.file_list:
            if normalize_filename(f.get("name", "")) == target:
                return f
        return None

    def build_metadata(self, filename: str) -> dict:
        target = normalize_filename(filename)
        f = self.find_file(target) or {}
        detail = self.file_detail if target == normalize_filename(self._print_info().get("Filename", "")) else {}
        est = detail.get("EstTime") or 0
        return {
            "filename": target,
            "size": f.get("FileSize", 0),
            "modified": f.get("CreateTime", 0),
            "created": f.get("CreateTime", 0),
            "layer_count": detail.get("TotalLayers") or f.get("TotalLayers"),
            "layer_height": f.get("LayerHeight", 0),
            "first_layer_height": None,
            "object_height": None,
            "estimated_time": est,
            "filament_total": detail.get("EstWeight"),
            "filament_weight_total": detail.get("EstWeight"),
            "thumbnails": [{"width": 0, "height": 0, "size": 0, "relative_path": detail.get("Thumbnail", "")}] if detail.get("Thumbnail") else [],
            "slicer": "Elegoo",
            "slicer_version": self.attributes.get("FirmwareVersion", ""),
        }

    def build_jobs(self) -> list[dict]:
        info = self._print_info()
        task_id = info.get("TaskId")
        state = self.map_print_state()
        if not task_id:
            # The printer only assigns a TaskId once actual printing starts.
            # Expose a synthetic job during the prepare phase so moonraker-obico
            # can resolve start_time/filename right away.
            if not (self.active_filename and state in ("printing", "paused")):
                return []
            task_id = "pending"
            start = self.print_started_ts or time.time()
            end = 0
            filename = self.active_filename
            duration = info.get("CurrentTicks") or 0
            td = {}
        else:
            td = self.task_detail if self.task_detail.get("TaskId") == task_id else {}
            start = td.get("BeginTime") or self.print_started_ts or time.time()
            end = td.get("EndTime") or 0
            filename = td.get("TaskName") or info.get("Filename") or ""
            duration = info.get("CurrentTicks") or 0
        status = self.map_print_state()
        mr_status = {
            "printing": "in_progress",
            "paused": "paused",
            "complete": "completed",
            "cancelled": "cancelled",
            "standby": "completed",
        }.get(status, "in_progress")
        return [{
            "job_id": task_id,
            "file": {"path": normalize_filename(filename), "filename": basename(filename)},
            "filename": normalize_filename(filename),
            "start_time": float(start),
            "end_time": float(end) if end else time.time(),
            "status": mr_status,
            "print_duration": duration,
            "total_duration": duration,
            "filament_used": 0,
            "metadata": {
                "layer_count": td.get("SliceInformation", {}).get("total_layer_numbers"),
            },
        }]


bridge = CentauriBridge()


# --------------------------------------------------------------------------- #
# FastAPI app
# --------------------------------------------------------------------------- #

@asynccontextmanager
async def lifespan(app: FastAPI):
    tasks = [asyncio.create_task(bridge.run()), asyncio.create_task(bridge.frame_reader())]
    yield
    for task in tasks:
        task.cancel()
    for task in tasks:
        with contextlib.suppress(asyncio.CancelledError):
            await task


app = FastAPI(title="Centauri Obico Bridge", lifespan=lifespan)


def result(data: Any) -> JSONResponse:
    return JSONResponse({"result": data})


async def request_params(request: Request) -> dict:
    """Merge query params with a JSON or form-encoded body.

    moonraker-obico's ``api_post`` sends parameters as form data
    (``application/x-www-form-urlencoded``), while curl/other clients may use
    the query string or JSON, so accept all three.
    """
    params: dict[str, Any] = dict(request.query_params)
    ctype = request.headers.get("content-type", "")
    if "application/json" in ctype:
        with contextlib.suppress(Exception):
            body = await request.json()
            if isinstance(body, dict):
                params.update(body)
    else:
        with contextlib.suppress(Exception):
            form = await request.form()
            params.update({k: v for k, v in form.items()})
    return params


# -- diagnostics ----------------------------------------------------------- #

@app.get("/health")
async def health():
    return {
        "ok": True,
        "centauri_connected": bridge.connected,
        "last_rx": bridge.last_rx,
        "last_error": bridge.last_error,
        "mainboard_id": bridge.mainboard_id,
        "print_state": bridge.map_print_state(),
        "ws_clients": len(bridge.clients),
        "notify_count": bridge.notify_count,
    }


@app.get("/debug/raw")
async def debug_raw():
    return JSONResponse({
        "responses": list(bridge.responses.values()),
        "response_history": list(bridge.response_history),
        "last_response": {str(k): v for k, v in bridge.last_response.items()},
    })


@app.get("/debug/messages")
async def debug_messages(limit: int = 100):
    return JSONResponse({"messages": list(bridge.messages)[-limit:]})


@app.get("/debug/state")
async def debug_state():
    return JSONResponse({
        "connected": bridge.connected,
        "last_rx": bridge.last_rx,
        "last_error": bridge.last_error,
        "mainboard_id": bridge.mainboard_id,
        "status": bridge.status,
        "attributes": bridge.attributes,
        "task_detail": bridge.task_detail,
        "file_list": bridge.file_list,
        "history_ids": bridge.history_ids,
        "objects": bridge.build_objects(),
    })


# -- Moonraker server / printer info --------------------------------------- #

@app.get("/server/info")
async def server_info():
    return result({
        "klippy_connected": bridge.connected,
        "klippy_state": "ready" if bridge.connected else "shutdown",
        "components": ["application", "websockets", "klippy_connection", "history"],
        "failed_components": [],
        "registered_directories": ["gcodes"],
        "moonraker_version": "centauri-bridge",
        "api_version": [1, 0, 0],
        "api_version_string": "1.0.0",
    })


@app.get("/server/config")
async def server_config():
    return result({
        "config": {},
        "orig": {},
        "files": [],
        "warnings": [],
    })


@app.get("/access/api_key")
async def access_api_key():
    return result(API_KEY)


@app.get("/printer/info")
async def printer_info():
    attrs = bridge.attributes
    return result({
        "state": "ready" if bridge.connected else "shutdown",
        "state_message": bridge.last_error or "",
        "hostname": attrs.get("Name") or attrs.get("MachineName") or "centauri-carbon",
        "software_version": attrs.get("FirmwareVersion", "Elegoo SDCP bridge"),
        "cpu_info": attrs.get("BrandName", "ELEGOO"),
        "klippy_path": "",
        "python_path": "",
        "log_file": "",
        "config_file": "",
    })


# -- printer objects ------------------------------------------------------- #

ALL_OBJECTS = [
    "print_stats", "virtual_sdcard", "display_status", "gcode_move", "toolhead",
    "fan", "extruder", "heater_bed", "heaters", "webhooks", "idle_timeout",
    "history", "configfile",
]


@app.get("/printer/objects/list")
async def objects_list():
    return result({"objects": ALL_OBJECTS})


def select_objects(requested: Any) -> dict:
    objects = bridge.build_objects()
    if not requested:
        return objects
    keys = set(requested.keys()) if isinstance(requested, dict) else set(requested)
    if not keys:
        return objects
    return {k: v for k, v in objects.items() if k in keys or k == "heaters"}


@app.get("/printer/objects/query")
async def objects_query(request: Request):
    objects = select_objects(dict(request.query_params))
    return result({"status": objects, "eventtime": time.time()})


@app.post("/printer/objects/query")
async def objects_query_post(request: Request):
    body = {}
    with contextlib.suppress(Exception):
        body = await request.json()
    objects = select_objects(body.get("objects") if isinstance(body, dict) else None)
    return result({"status": objects, "eventtime": time.time()})


@app.post("/printer/objects/subscribe")
async def objects_subscribe(request: Request):
    body = {}
    with contextlib.suppress(Exception):
        body = await request.json()
    objects = select_objects(body.get("objects") if isinstance(body, dict) else None)
    return result({"status": objects, "eventtime": time.time()})


# -- files ----------------------------------------------------------------- #

@app.get("/server/files/list")
async def files_list():
    out = []
    for f in bridge.file_list:
        name = normalize_filename(f.get("name", ""))
        out.append({
            "path": name,
            "modified": f.get("CreateTime", 0),
            "size": f.get("FileSize", 0),
            "permissions": "rw",
        })
    return result(out)


@app.get("/server/files/metadata")
async def files_metadata(filename: str = ""):
    # Always return a valid "result" object: moonraker-obico dereferences it
    # even for an empty filename during the printer's prepare phase.
    return result(bridge.build_metadata(filename))


@app.post("/server/files/upload")
async def files_upload(request: Request):
    """Proxy a Moonraker upload to the printer's HTTP upload endpoint.

    The stock UI uploads to ``http://PRINTER/uploadFile/upload`` in 1 MiB chunks
    with the fields TotalSize / Uuid / Offset / Check / S-File-MD5 / File.
    """
    form = await request.form()
    upload = form.get("file")
    if upload is None or not getattr(upload, "filename", ""):
        return JSONResponse(status_code=400, content={"error": {"code": 400, "message": "file is required"}})
    data = await upload.read()
    filename = pathlib.PurePosixPath(upload.filename).name
    # The printer stores uploads flat under /local regardless of any subfolder.
    path = "gcodes"
    md5 = hashlib.md5(data).hexdigest()
    upload_uuid = uuid.uuid4().hex
    chunk_size = 1024 * 1024
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            for offset in range(0, max(len(data), 1), chunk_size):
                piece = data[offset:offset + chunk_size]
                resp = await client.post(
                    f"http://{HOST}/uploadFile/upload",
                    data={
                        "TotalSize": str(len(data)),
                        "Uuid": upload_uuid,
                        "Offset": str(offset),
                        "Check": "1",
                        "S-File-MD5": md5,
                    },
                    files={"File": (filename, piece, "application/octet-stream")},
                )
                if resp.status_code != 200:
                    return JSONResponse(status_code=502, content={"error": {
                        "code": 502,
                        "message": f"Printer upload failed: HTTP {resp.status_code} {resp.text[:200]}",
                    }})
                body = {}
                with contextlib.suppress(Exception):
                    body = resp.json()
                if isinstance(body, dict) and body.get("success") is False:
                    return JSONResponse(status_code=502, content={"error": {
                        "code": 502,
                        "message": f"Printer upload rejected: {body}",
                    }})
    except Exception as exc:
        return JSONResponse(status_code=502, content={"error": {"code": 502, "message": f"Printer upload error: {exc}"}})

    with contextlib.suppress(Exception):
        await bridge.send_command(CMD_FILE_LIST, {"Url": "/local"})
        # Give the printer a moment to index the new file so that a subsequent
        # /server/files/metadata call returns the real size/modified values.
        for _ in range(10):
            await asyncio.sleep(0.4)
            if bridge.find_file(filename):
                break
    found = bridge.find_file(filename) or {}
    item = {
        "path": f"{path}/{filename}",
        "root": path,
        "size": found.get("FileSize", len(data)),
        "modified": found.get("CreateTime", time.time()),
    }
    # Moonraker wraps in "result"; moonraker-obico reads the top-level "item".
    return JSONResponse({"result": {"item": item}, "item": item})


# -- history --------------------------------------------------------------- #

@app.get("/server/history/list")
async def history_list(limit: int = 50):
    jobs = bridge.build_jobs()
    return result({"jobs": jobs[:limit], "count": len(jobs)})


@app.get("/server/history/totals")
async def history_totals():
    jobs = bridge.build_jobs()
    return result({
        "job_totals": {
            "total_jobs": len(jobs),
            "total_print_time": sum(j.get("print_duration", 0) for j in jobs),
            "total_filament_used": 0,
            "longest_print": max([j.get("print_duration", 0) for j in jobs] or [0]),
        }
    })


# -- database / machine ---------------------------------------------------- #

_db: dict[str, Any] = {}


@app.get("/server/database/item")
async def database_item_get(namespace: str = "", key: str = ""):
    value = _db.get(f"{namespace}/{key}")
    if value is None and namespace == "mainsail" and key == "presets":
        value = {"presets": {}}
    return result({"namespace": namespace, "key": key, "value": value})


@app.post("/server/database/item")
async def database_item_post(request: Request):
    body = {}
    with contextlib.suppress(Exception):
        body = await request.json()
    ns = body.get("namespace", "")
    key = body.get("key", "")
    _db[f"{ns}/{key}"] = body.get("value")
    return result({"namespace": ns, "key": key, "value": body.get("value")})


@app.get("/machine/update/status")
async def machine_update_status():
    return result({"version_info": {}, "busy": False})


@app.get("/machine/system_info")
async def machine_system_info():
    return result({"system_info": {"cpu_info": bridge.attributes.get("BrandName", "ELEGOO")}})


@app.get("/machine/device_power/devices")
async def device_power_devices():
    return result({"devices": []})


# -- print control --------------------------------------------------------- #

async def _run_command(cmd: int, data: Any = None, timeout: float = 10.0) -> JSONResponse:
    try:
        payload = await bridge.send_command(cmd, data, wait=True, timeout=timeout)
    except RuntimeError as exc:
        return JSONResponse(status_code=503, content={"error": {"code": 503, "message": str(exc)}})
    except TimeoutError as exc:
        return JSONResponse(status_code=504, content={"error": {"code": 504, "message": str(exc)}})
    ack = (payload or {}).get("Ack")
    if ack not in (0, None):
        return JSONResponse(
            status_code=400,
            content={"error": {"code": ack, "message": f"SDCP command {CMD_NAMES.get(cmd, cmd)} failed (Ack={ack})"}},
        )
    return result("ok")


@app.post("/printer/print/start")
async def print_start(request: Request):
    params = await request_params(request)
    filename = str(params.get("filename") or "")
    if not filename:
        return JSONResponse(status_code=400, content={"error": {"code": 400, "message": "filename is required"}})
    normalized = normalize_filename(filename)
    data = {
        "Filename": normalized,
        "StartLayer": 0,
        "Calibration_switch": START_CALIBRATION,
        "PrintPlatformType": START_PLATFORM_TYPE,
        "Tlp_Switch": START_TIMELAPSE,
        "slot_map": [],
    }
    response = await _run_command(CMD_START_PRINT, data, timeout=15)
    if response.status_code == 200:
        bridge.active_filename = normalized
        bridge.print_started_ts = time.time()
        bridge._last_task_id = None
    return response


@app.post("/printer/print/pause")
async def print_pause():
    return await _run_command(CMD_SUSPEND_PRINT, {})


@app.post("/printer/print/resume")
async def print_resume():
    return await _run_command(CMD_RESTORE_PRINT, {})


@app.post("/printer/print/cancel")
async def print_cancel():
    return await _run_command(CMD_STOP_PRINT, {})


@app.post("/printer/print/stop")
async def print_stop():
    return await _run_command(CMD_STOP_PRINT, {})


@app.post("/printer/emergency_stop")
async def emergency_stop():
    return await _run_command(CMD_STOP_PRINT, {})


# -- gcode script translation ---------------------------------------------- #

async def _handle_gcode_script(script: str) -> None:
    """Best-effort translation of the gcode moonraker-obico sends."""
    for raw_line in script.splitlines():
        line = raw_line.split(";")[0].strip()
        if not line:
            continue
        upper = line.upper()
        try:
            if upper.startswith("G28"):
                axes = "".join(a for a in upper[3:] if a in "XYZ") or "XYZ"
                await bridge.send_command(CMD_AXIS_ZERO, {"Axis": axes})
            elif upper.startswith("G0") or upper.startswith("G1"):
                feed = re.search(r"F([0-9.]+)", upper)
                for axis in "XYZ":
                    m = re.search(rf"{axis}(-?[0-9.]+)", upper)
                    if m:
                        await bridge.send_command(CMD_AXIS_NUMBER, {"Axis": axis, "Step": float(m.group(1))})
            elif "SET_HEATER_TEMPERATURE" in upper:
                heater = re.search(r"HEATER=(\w+)", line, re.I)
                target = re.search(r"TARGET=([0-9.]+)", line, re.I)
                if heater and target:
                    key = "TempTargetHotbed" if "bed" in heater.group(1).lower() else "TempTargetNozzle"
                    await bridge.send_command(CMD_STATUS_DATA, {key: float(target.group(1))})
            elif upper.startswith("M104") or upper.startswith("M109"):
                m = re.search(r"S([0-9.]+)", upper)
                if m:
                    await bridge.send_command(CMD_STATUS_DATA, {"TempTargetNozzle": float(m.group(1))})
            elif upper.startswith("M140") or upper.startswith("M190"):
                m = re.search(r"S([0-9.]+)", upper)
                if m:
                    await bridge.send_command(CMD_STATUS_DATA, {"TempTargetHotbed": float(m.group(1))})
            elif "SET_FAN_SPEED" in upper:
                m = re.search(r"SPEED=([0-9.]+)", line, re.I)
                if m:
                    pct = float(m.group(1))
                    pct = pct * 100 if pct <= 1 else pct
                    await bridge.send_command(CMD_STATUS_DATA, {"TargetFanSpeed": {"ModelFan": pct}})
            elif "M106" in upper:
                m = re.search(r"S([0-9.]+)", upper)
                pct = float(m.group(1)) / 255.0 * 100 if m else 100.0
                await bridge.send_command(CMD_STATUS_DATA, {"TargetFanSpeed": {"ModelFan": pct}})
        except Exception as exc:
            log.debug("gcode translation failed for %r: %s", line, exc)


@app.post("/printer/gcode/script")
async def gcode_script(request: Request):
    params = await request_params(request)
    script = str(params.get("script") or "")
    await _handle_gcode_script(script)
    return result("ok")


# -- webcams --------------------------------------------------------------- #

def _base_url(request: Request) -> str:
    if PUBLIC_URL:
        return PUBLIC_URL
    return str(request.base_url).rstrip("/")


@app.get("/server/webcams/list")
async def webcams_list(request: Request):
    base = _base_url(request)
    return result({
        "webcams": [{
            "name": "Centauri Camera",
            "location": "printer",
            "service": "mjpegstreamer",
            "stream_url": f"{base}/webcam/stream",
            "snapshot_url": f"{base}/webcam/snapshot",
            "flip_horizontal": False,
            "flip_vertical": False,
            "rotation": 0,
            "target_fps": 15,
            "enabled": True,
        }]
    })


@app.get("/webcam/stream")
async def webcam_stream():
    async def iterator():
        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream("GET", VIDEO_URL) as upstream:
                async for chunk in upstream.aiter_bytes():
                    yield chunk

    return StreamingResponse(
        iterator(),
        media_type="multipart/x-mixed-replace; boundary=--foo",
        headers={"Content-Type": "multipart/x-mixed-replace; boundary=--foo"},
    )


@app.get("/webcam/snapshot")
async def webcam_snapshot():
    """Return the latest cached JPEG frame (falling back to a live capture)."""
    if bridge.latest_frame and time.time() - bridge.latest_frame_ts < 10:
        return Response(content=bridge.latest_frame, media_type="image/jpeg")
    buf = bytearray()
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            async with client.stream("GET", VIDEO_URL) as upstream:
                async for chunk in upstream.aiter_bytes():
                    buf.extend(chunk)
                    start = buf.find(b"\xff\xd8")
                    end = buf.find(b"\xff\xd9", start + 2) if start >= 0 else -1
                    if start >= 0 and end >= 0:
                        return Response(content=bytes(buf[start:end + 2]), media_type="image/jpeg")
                    if len(buf) > 8_000_000:
                        break
    except Exception as exc:
        log.warning("snapshot failed: %s", exc)
    return JSONResponse(status_code=502, content={"error": {"code": 502, "message": "Unable to capture camera frame"}})


@app.get("/video")
async def video():
    return await webcam_stream()


# -- websocket JSON-RPC (Moonraker compatible) ----------------------------- #

def _jsonrpc_result(msg_id: Any, res: Any) -> dict:
    return {"jsonrpc": "2.0", "id": msg_id, "result": res}


def _jsonrpc_error(msg_id: Any, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": msg_id, "error": {"code": code, "message": message}}


@app.websocket("/websocket")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    bridge.clients.add(ws)
    try:
        await ws.send_text(json.dumps({
            "jsonrpc": "2.0",
            "method": "notify_klippy_ready",
            "params": [],
        }))
        while True:
            raw = await ws.receive_text()
            try:
                data = json.loads(raw)
            except Exception:
                continue
            method = data.get("method")
            msg_id = data.get("id")
            params = data.get("params") or {}
            if method == "server.connection.identify":
                await ws.send_text(json.dumps(_jsonrpc_result(msg_id, {
                    "connection_id": uuid.uuid4().hex,
                    "client_name": params.get("client_name", ""),
                    "version": params.get("version", ""),
                    "type": params.get("type", ""),
                })))
            elif method == "connection.register_remote_method":
                await ws.send_text(json.dumps(_jsonrpc_result(msg_id, "ok")))
            elif method == "printer.objects.list":
                await ws.send_text(json.dumps(_jsonrpc_result(msg_id, {"objects": ALL_OBJECTS})))
            elif method == "printer.objects.subscribe":
                await ws.send_text(json.dumps(_jsonrpc_result(msg_id, {
                    "status": bridge.build_objects(),
                    "eventtime": time.time(),
                })))
            elif method == "printer.objects.query":
                requested = params.get("objects") if isinstance(params, dict) else None
                await ws.send_text(json.dumps(_jsonrpc_result(msg_id, {
                    "status": select_objects(requested),
                    "eventtime": time.time(),
                })))
            elif method == "printer.gcode.script":
                script = params.get("script", "") if isinstance(params, dict) else ""
                await _handle_gcode_script(script)
                await ws.send_text(json.dumps(_jsonrpc_result(msg_id, "ok")))
            elif method == "printer.info":
                await ws.send_text(json.dumps(_jsonrpc_result(msg_id, {
                    "state": "ready" if bridge.connected else "shutdown",
                })))
            else:
                await ws.send_text(json.dumps(_jsonrpc_error(
                    msg_id, -32601, f"Method not found: {method}"
                )))
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        log.debug("websocket error: %s", exc)
    finally:
        bridge.clients.discard(ws)


# -- legacy aliases -------------------------------------------------------- #

@app.get("/webcams/list")
async def webcams_list_legacy(request: Request):
    return await webcams_list(request)


@app.get("/api/status")
async def api_status():
    return JSONResponse(bridge.build_objects())


@app.get("/")
async def root():
    return {
        "service": "centauri-obico-bridge",
        "printer": HOST,
        "sdcp": SDCP_WS,
        "video": VIDEO_URL,
        "moonraker_compat": f"http://0.0.0.0:{MOONRAKER_PORT}",
        "connected": bridge.connected,
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=MOONRAKER_PORT)
