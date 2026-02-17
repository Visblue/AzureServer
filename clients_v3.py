"""
Modbus Client Manager (refactored + improved)

Hvad denne version gør bedre (vigtigst):
- Pollers (læs/skriv Modbus) bliver IKKE stoppet af MongoDB writes:
  -> vi lægger docs i en queue, og en separat MongoBatchWriter tråd skriver i batches.
- Workers bliver PAUSET under config-reload for at undgå race conditions.
- Scheduling er stabil: næste run bliver altid "nu + 2-3 sek", så der ikke opbygges backlog.
- Runtime-state bevares ved reload (last_import/export, backoff, next_run_at osv).

Dependencies:
- pymongo
- pymodbus (sync client)

"""

from __future__ import annotations

import logging
import random
import re
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from queue import Empty, Full, Queue
from typing import Any, Optional

from pymodbus.client.sync import ModbusTcpClient
from pymodbus.constants import Endian
from pymodbus.exceptions import ModbusException
from pymodbus.payload import BinaryPayloadBuilder, BinaryPayloadDecoder
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import InvalidOperation

# Silence noisy pymodbus logs; we explicitly log important failures ourselves.
logging.getLogger("pymodbus").setLevel(logging.CRITICAL)

# --------------------------------------------------------------------
# Global constants / business rules
# --------------------------------------------------------------------

SLEEP_SECONDS_RANGE = (2, 3)  # poll interval per device
DB_SAVE_INTERVAL_SECONDS = 30
CONFIG_RELOAD_INTERVAL_SECONDS = 120  # 2 minutes

MAX_REASONABLE_DIFF_WH = 500_000
MAX_REASONABLE_DIFF_WH_EM_GROUP = 1_000_000
MAX_REASONABLE_POWER_W = 1_000_000
FLOAT_TOLERANCE_WH = 0.01
MIN_TOTAL_VALUE = 0
MAX_TOTAL_RESET_THRESHOLD = 0.5  # meter reset if new < 50% of old

# Backoff when modbus connect/read/write fails repeatedly
BACKOFF_MIN_SECONDS = 60
BACKOFF_MAX_SECONDS = 360

# Modbus register addresses on the "server" side.
SERVER_REGISTERS = {
    "active_power": 19026,
    "total_import": 19068,
    "total_export": 19076,
}

# --------------------------------------------------------------------
# Mongo batching (non-blocking DB writes)
# --------------------------------------------------------------------

DB_QUEUE_MAXSIZE = 50_000          # protects RAM if Mongo is down
MONGO_BATCH_SIZE = 250             # docs per insert_many per collection
MONGO_FLUSH_INTERVAL_SECONDS = 1.0 # max delay before flush

# Retry phases:
# - Fast exponential backoff while Mongo glitches (1s->2s->4s... cap 30s)
# - After many consecutive failures -> long backoff (60-360s) to avoid hammering
MONGO_RETRY_BACKOFF_MIN = 1.0
MONGO_RETRY_BACKOFF_MAX = 30.0

MONGO_LONG_BACKOFF_MIN = 60.0
MONGO_LONG_BACKOFF_MAX = 360.0
MONGO_LONG_BACKOFF_AFTER_FAILS = 20

# --------------------------------------------------------------------
# Infrastructure constants
# --------------------------------------------------------------------

METERS_DB = "Meters"
METERS_DEVICES_COLLECTION = "devices"
ENERGY_LOGS_DB = "customer_energy_logs"

SERVERS: list[dict[str, Any]] = [
    {"ip": "localhost", "port": 650},
]

# --------------------------------------------------------------------
# Error reporting (console + optional MongoDB)
# --------------------------------------------------------------------

@dataclass
class ErrorEvent:
    site: str
    message: str
    category: str = "runtime"
    severity: str = "error"


class ErrorReporter:
    def record(self, event: ErrorEvent, interval_seconds: int = 60) -> None:
        raise NotImplementedError


class RateLimitedConsoleReporter(ErrorReporter):
    def __init__(self) -> None:
        self._state: dict[str, dict[str, float | int]] = {}

    def record(self, event: ErrorEvent, interval_seconds: int = 60) -> None:
        key = f"{event.site}|{event.category}|{event.message}"
        now = time.time()
        st = self._state.setdefault(key, {"count": 0, "last_print": 0.0})
        st["count"] = int(st["count"]) + 1

        if now - float(st["last_print"]) >= interval_seconds:
            print(f"{event.site}: {event.message} (fejl #{int(st['count'])})")
            st["last_print"] = now


_NORMALIZE_PATTERNS = [
    (re.compile(r"\d+\.\d+ seconds"), "X seconds"),
    (re.compile(r"read of \d+ bytes"), "read of N bytes"),
    (re.compile(r"ModbusTcpClient\([^)]+\)"), "ModbusTcpClient(...)"),
]


def normalize_error_message(message: str) -> str:
    result = message
    for pattern, replacement in _NORMALIZE_PATTERNS:
        result = pattern.sub(replacement, result)
    return result


class ThrottledMongoEventReporter(ErrorReporter):
    def __init__(self, collection: Collection) -> None:
        self._coll = collection
        self._state: dict[str, dict[str, float | int]] = {}

        self._coll.create_index([("site", 1), ("category", 1), ("key", 1)], unique=True, background=True)
        self._coll.create_index([("last_seen", -1)], background=True)

    def _get_dynamic_interval(self, total_count: int) -> int:
        if total_count < 10:
            return 300
        elif total_count < 50:
            return 3600
        else:
            return 7200

    def record(self, event: ErrorEvent, interval_seconds: int = 60) -> None:
        normalized_msg = normalize_error_message(event.message)
        key_text = normalized_msg if len(normalized_msg) <= 240 else normalized_msg[:240]
        event_id = f"{event.site}|{event.category}|{key_text}"

        now = time.time()
        st = self._state.setdefault(event_id, {"pending": 0, "last_db": 0.0, "total_count": 0})
        st["pending"] = int(st["pending"]) + 1
        st["total_count"] = int(st["total_count"]) + 1

        effective_interval = self._get_dynamic_interval(int(st["total_count"]))
        should_write = float(st["last_db"]) == 0.0 or (now - float(st["last_db"]) >= effective_interval)
        if not should_write:
            return

        pending = int(st["pending"])
        if pending <= 0:
            return

        now_dt = datetime.utcnow()
        try:
            self._coll.update_one(
                {"site": event.site, "category": event.category, "key": key_text},
                {
                    "$setOnInsert": {"first_seen": now_dt},
                    "$set": {"last_seen": now_dt, "message": event.message, "severity": event.severity},
                    "$inc": {"count": pending},
                },
                upsert=True,
            )
            st["pending"] = 0
            st["last_db"] = now
            if int(st["total_count"]) >= 60:
                st["total_count"] = 0
        except Exception as e:
            print(f"[DEBUG] service_events write failed: {e}")


class CompositeReporter(ErrorReporter):
    def __init__(self, reporters: list[ErrorReporter]) -> None:
        self._reporters = reporters

    def record(self, event: ErrorEvent, interval_seconds: int = 60) -> None:
        for rep in self._reporters:
            rep.record(event, interval_seconds=interval_seconds)


def categorize_message(message: str) -> tuple[str, str]:
    msg_l = message.lower()
    if "invalid" in msg_l:
        return "data", "warning"
    if "mongodb" in msg_l:
        return "db", "error"
    if "modbus" in msg_l:
        return "modbus", "error"
    if "forbinde" in msg_l or "connect" in msg_l:
        return "connect", "error"
    return "runtime", "error"


# --------------------------------------------------------------------
# Modbus codec + reader
# --------------------------------------------------------------------

class ModbusCodec:
    @staticmethod
    def _decoder(registers: list[int], byteorder: Any, wordorder: Any) -> BinaryPayloadDecoder:
        return BinaryPayloadDecoder.fromRegisters(registers, byteorder=byteorder, wordorder=wordorder)

    def decode_int16(self, registers: list[int]) -> int:
        return self._decoder(registers, Endian.Big, Endian.Big).decode_16bit_int()

    def decode_uint16(self, registers: list[int]) -> int:
        return self._decoder(registers, Endian.Big, Endian.Big).decode_16bit_uint()

    def decode_uint32(self, registers: list[int]) -> int:
        return self._decoder(registers, Endian.Big, Endian.Big).decode_32bit_uint()

    def decode_int32_big_little(self, registers: list[int]) -> int:
        return self._decoder(registers, Endian.Big, Endian.Little).decode_32bit_int()

    def decode_int32_big_big(self, registers: list[int]) -> int:
        return self._decoder(registers, Endian.Big, Endian.Big).decode_32bit_int()

    def decode_float32_big_big(self, registers: list[int]) -> float:
        return round(self._decoder(registers, Endian.Big, Endian.Big).decode_32bit_float(), 3)

    def decode_float32_big_little(self, registers: list[int]) -> float:
        return round(self._decoder(registers, Endian.Big, Endian.Little).decode_32bit_float(), 3)

    def decode_float64_big_big(self, registers: list[int]) -> float:
        return self._decoder(registers, Endian.Big, Endian.Big).decode_64bit_float()

    def decode_float64_big_little(self, registers: list[int]) -> float:
        return self._decoder(registers, Endian.Big, Endian.Little).decode_64bit_float()


class ModbusReader(ModbusCodec):
    def __init__(self, client: ModbusTcpClient, unit_id: int):
        self.client = client
        self.unit_id = unit_id

    def is_open(self) -> bool:
        try:
            return bool(self.client.is_socket_open())
        except Exception:
            return False

    def close(self) -> None:
        try:
            self.client.close()
        except Exception:
            pass

    def read_register(self, reg_cfg: dict[str, Any], *, default_table: str = "holding") -> float | int:
        addr = int(reg_cfg["address"])
        fmt = reg_cfg["format"]

        table = (reg_cfg.get("reading") or reg_cfg.get("table") or default_table or "holding").strip().lower()
        if table not in ("holding", "input"):
            raise ValueError(f"Ukendt register-table: {table} (for {addr})")

        read_fn = self.client.read_input_registers if table == "input" else self.client.read_holding_registers

        if fmt in ("int16", "uint16"):
            res = read_fn(addr, count=1, unit=self.unit_id)
            if res.isError():
                raise ModbusException(f"Read error at {addr}")
            return self.decode_int16(res.registers) if fmt == "int16" else self.decode_uint16(res.registers)

        if fmt == "uint32":
            res = read_fn(addr, count=2, unit=self.unit_id)
            if res.isError():
                raise ModbusException(f"Read error at {addr}")
            if len(res.registers) != 2:
                raise ModbusException(f"uint32 expected 2 regs, got {len(res.registers)}")
            return self.decode_uint32(res.registers)

        if fmt == "int32":
            res = read_fn(addr, count=2, unit=self.unit_id)
            if res.isError():
                raise ModbusException(f"Read error at {addr}")
            return self.decode_int32_big_little(res.registers)

        if fmt == "int32_1":
            res = read_fn(addr, count=2, unit=self.unit_id)
            if res.isError():
                raise ModbusException(f"Read error at {addr}")
            return self.decode_int32_big_big(res.registers)

        if fmt in ("float32", "float32_1", "float32_2"):
            res = read_fn(addr, count=2, unit=self.unit_id)
            if res.isError():
                raise ModbusException(f"Read error at {addr}")
            if fmt in ("float32", "float32_1"):
                return self.decode_float32_big_big(res.registers)
            return self.decode_float32_big_little(res.registers)

        if fmt == "double":
            res = read_fn(addr, count=4, unit=self.unit_id)
            if res.isError():
                raise ModbusException(f"Read error at {addr}")
            return self.decode_float64_big_big(res.registers)

        if fmt == "double_little":
            res = read_fn(addr, count=4, unit=self.unit_id)
            if res.isError():
                raise ModbusException(f"Read error at {addr}")
            return self.decode_float64_big_little(res.registers)

        raise ValueError(f"Ukendt format: {fmt}")

    def write_float32(self, value: float, address: int, unit_id: int) -> None:
        builder = BinaryPayloadBuilder(byteorder=Endian.Big, wordorder=Endian.Big)
        builder.add_32bit_float(float(value))
        regs = builder.to_registers()

        res = self.client.write_registers(address, regs, unit=unit_id)
        if res.isError():
            raise ModbusException(f"Write error at {address}")


def connect_modbus(ip: str, port: int, timeout_seconds: int = 3) -> Optional[ModbusTcpClient]:
    try:
        client = ModbusTcpClient(ip, port=port, timeout=timeout_seconds, retries=1, retry_on_empty=True)
        if client.connect():
            return client
        client.close()
    except Exception:
        pass
    return None


# --------------------------------------------------------------------
# MongoDB config loading
# --------------------------------------------------------------------

class MongoConfigLoader:
    def __init__(self, client: MongoClient | None = None, uri: str = "mongodb://vbserver:27017") -> None:
        self._owns_client = client is None
        self._client = client or MongoClient(uri, serverSelectionTimeoutMS=5000)
        self._db = self._client[METERS_DB]
        self._coll = self._db[METERS_DEVICES_COLLECTION]

    def load(self) -> dict[str, Any]:
        devices = list(self._coll.find({}, {"_id": 0}))
        if not devices:
            raise ValueError("Ingen devices fundet i MongoDB (Meters.devices)")

        cfg: dict[str, Any] = {"devices": devices, "servers": list(SERVERS)}
        em_groups: dict[str, list[dict[str, Any]]] = {}
        single_devices: list[dict[str, Any]] = []

        for dev in devices:
            group = dev.get("em_group")
            if group:
                em_groups.setdefault(group, []).append(dev)
            else:
                single_devices.append(dev)

        if em_groups:
            print(f"[INFO] Fundet {len(em_groups)} em_groups i config.")

        cfg["devices"] = single_devices
        cfg["em_groups"] = em_groups
        return cfg

    def close(self) -> None:
        if self._owns_client:
            self._client.close()


# --------------------------------------------------------------------
# Validation + energy reading
# --------------------------------------------------------------------

@dataclass
class EnergyMeta:
    active_power_invalid: bool = False
    active_power_raw: Optional[float] = None
    active_power_reason: str = ""

    import_invalid: bool = False
    import_raw: Optional[float] = None
    import_reason: str = ""

    export_invalid: bool = False
    export_raw: Optional[float] = None
    export_reason: str = ""

    @property
    def has_invalid_data(self) -> bool:
        return bool(self.active_power_invalid or self.import_invalid or self.export_invalid)


class EnergyValidator:
    def validate_total(self, new: float, last: float, value_name: str, *, first_read: bool) -> tuple[bool, float, str]:
        if new < MIN_TOTAL_VALUE:
            return False, last, f"{value_name} negativ ({new})"

        if first_read:
            return True, new, "første læsning efter restart eller lang afbrydelse"

        if last <= 1.0:
            return True, new, "første læsning"

        if new < last * MAX_TOTAL_RESET_THRESHOLD:
            return True, new, "meter reset eller data nulstillet"

        diff = new - last

        if diff < -FLOAT_TOLERANCE_WH:
            return False, last, f"{value_name} faldt ({last} -> {new})"

        if last < 10000 and diff > MAX_REASONABLE_DIFF_WH:
            extended_limit = MAX_REASONABLE_DIFF_WH * 10
            if diff > extended_limit:
                return False, last, f"{value_name} spike ({diff} Wh) selv med udvidet grænse"
            return True, new, f"OK (tidlig fase, diff={diff} Wh)"

        if diff > MAX_REASONABLE_DIFF_WH:
            return False, last, f"{value_name} spike ({diff} Wh)"

        return True, new, "OK"


class EnergyReader:
    def __init__(self, validator: EnergyValidator, reporter: ErrorReporter) -> None:
        self._validator = validator
        self._reporter = reporter

    def read(
        self,
        reader: ModbusReader,
        device_cfg: dict[str, Any],
        last_import: float,
        last_export: float,
        interval_seconds: float,
        *,
        first_read: bool
    ) -> tuple[float, float, float, EnergyMeta]:
        regs = device_cfg["registers"]
        site = device_cfg.get("site", "unknown")

        has_ap = "active_power" in regs
        # Check if register exists AND has a valid address (not None, empty string, or empty dict)
        has_imp = "total_import" in regs and regs.get("total_import") and isinstance(regs.get("total_import"), dict) and regs["total_import"].get("address")
        has_exp = "total_export" in regs and regs.get("total_export") and isinstance(regs.get("total_export"), dict) and regs["total_export"].get("address")

        active_power = 0.0
        total_import = last_import
        total_export = last_export
        meta = EnergyMeta()

        default_table = (device_cfg.get("reading") or device_cfg.get("table") or "holding").strip().lower()

        def read_reg(reg_cfg: dict[str, Any]) -> float | int:
            return reader.read_register(reg_cfg, default_table=default_table)

        if has_ap:
            raw = read_reg(regs["active_power"])
            scale = regs["active_power"].get("scale", 1.0)
            ap_raw = float(raw) * float(scale)

            ap_offset = regs["active_power"].get("offset", 0.0) or 0.0
            ap_raw = ap_raw + float(ap_offset)

            meta.active_power_raw = ap_raw

            if ap_raw != ap_raw or abs(ap_raw) > MAX_REASONABLE_POWER_W:
                meta.active_power_invalid = True
                meta.active_power_reason = f"Urealistisk active_power: {ap_raw}"
                cat, sev = categorize_message(f"Active_power invalid: {ap_raw}")
                self._reporter.record(ErrorEvent(site=site, message=f"Active_power invalid: {ap_raw}", category=cat, severity=sev))
                active_power = 0.0
            else:
                active_power = ap_raw

        if has_imp:
            raw = read_reg(regs["total_import"])
            scale = regs["total_import"].get("scale", 1.0)
            new_val = round(float(raw) * float(scale), 3)
            imp_offset = regs["total_import"].get("offset")
            if imp_offset is not None:
                new_val = round(new_val + float(imp_offset), 3)
            meta.import_raw = new_val

            ok, validated, reason = self._validator.validate_total(new_val, last_import, "Import", first_read=first_read)
            if not ok:
                meta.import_invalid = True
                meta.import_reason = reason
                cat, sev = categorize_message(f"Import invalid: {reason}")
                self._reporter.record(ErrorEvent(site=site, message=f"Import invalid: {reason}", category=cat, severity=sev))
            total_import = validated

        if has_exp:
            raw = read_reg(regs["total_export"])
            scale = regs["total_export"].get("scale", 1.0)
            new_val = round(float(raw) * float(scale), 3)
            exp_offset = regs["total_export"].get("offset")
            if exp_offset is not None:
                new_val = round(new_val + float(exp_offset), 3)
            meta.export_raw = new_val

            ok, validated, reason = self._validator.validate_total(new_val, last_export, "Export", first_read=first_read)
            if not ok:
                meta.export_invalid = True
                meta.export_reason = reason
                cat, sev = categorize_message(f"Export invalid: {reason}")
                self._reporter.record(ErrorEvent(site=site, message=f"Export invalid: {reason}", category=cat, severity=sev))
            total_export = validated

        # If only active_power exists, derive totals by integrating power over time.
        if has_ap and not (has_imp or has_exp):
            effective_interval = max(float(interval_seconds), 1.0)
            energy_wh = (active_power * effective_interval) / 3600.0
            if active_power > 0:
                total_import = last_import + energy_wh
            elif active_power < 0:
                total_export = last_export + abs(energy_wh)

        return active_power, total_import, total_export, meta


# --------------------------------------------------------------------
# MongoDB repository (collection naming + doc building)
# --------------------------------------------------------------------

class MongoEnergyRepository:
    def __init__(self, mongo_db: Database, reporter: ErrorReporter) -> None:
        self._db = mongo_db
        self._reporter = reporter
        self._indexed_collections: set[str] = set()

    @staticmethod
    def collection_name(device_cfg: dict[str, Any]) -> str:
        data_coll = device_cfg.get("data_collection")
        if data_coll and str(data_coll).strip():
            return str(data_coll).strip()

        project_nr_raw = device_cfg.get("project_nr")
        project = str(project_nr_raw).strip() if project_nr_raw is not None else ""
        name = device_cfg.get("name", "device").strip()
        name_clean = "".join(c if c.isalnum() or c == "_" else "_" for c in name.replace(" ", "_"))
        return f"{project}_{name_clean}" if project else name_clean

    def get_collection(self, device_cfg: dict[str, Any]) -> Collection:
        coll_name = self.collection_name(device_cfg)
        coll = self._db[coll_name]
        if coll_name not in self._indexed_collections:
            coll.create_index([("Time", -1)], background=True)
            self._indexed_collections.add(coll_name)
        return coll

    def load_last_totals(self, collection: Collection) -> tuple[float, float]:
        try:
            latest = collection.find_one(
                {},
                {"_id": 0, "Total_Imported": 1, "Total_Exported": 1},
                sort=[("Time", -1)],
            )
            if latest:
                imp = float(latest.get("Total_Imported", 0.0))
                exp = float(latest.get("Total_Exported", 0.0))
                return imp, exp
        except Exception as e:
            self._reporter.record(ErrorEvent(site=collection.name, message=f"Fejl ved indlæsning af sidste totals: {e}", category="db"))
        return 0.0, 0.0

    def build_sample_doc(
        self,
        device_cfg: dict[str, Any],
        last_import: float,
        last_export: float,
        active_power: float,
        total_import: float,
        total_export: float,
        meta: EnergyMeta,
    ) -> tuple[dict[str, Any], float, float]:
        site = device_cfg.get("site") or "unknown"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if active_power != active_power:
            active_power = 0.0
        if total_import != total_import:
            total_import = last_import
        if total_export != total_export:
            total_export = last_export

        if total_import is None:
            total_import = last_import
        if total_export is None:
            total_export = last_export

        import_reason_l = (meta.import_reason or "").lower()
        export_reason_l = (meta.export_reason or "").lower()
        is_import_baseline = import_reason_l.startswith("første læsning")
        is_export_baseline = export_reason_l.startswith("første læsning")

        imp_diff = 0 if is_import_baseline else max(0, float(total_import) - float(last_import))
        exp_diff = 0 if is_export_baseline else max(0, float(total_export) - float(last_export))

        project_nr = device_cfg.get("project_nr") or ""
        device_name = device_cfg.get("name") or ""

        doc = {
            "Site": site,
            "Project_nr": project_nr,
            "Device_name": device_name,
            "Time": now,
            "Active_power": round(float(active_power), 2),
            "Imported_Wh": imp_diff,
            "Exported_Wh": exp_diff,
            "Total_Imported": float(total_import),
            "Total_Exported": float(total_export),
            "Total_Imported_kWh": float(total_import) / 1000.0 if total_import is not None else 0.0,
            "Total_Exported_kWh": float(total_export) / 1000.0 if total_export is not None else 0.0,
            "Active_power_valid": not meta.active_power_invalid,
            "Active_power_raw": float(meta.active_power_raw) if meta.active_power_raw is not None else 0.0,
            "Active_power_invalid_reason": meta.active_power_reason or "",
            "Import_valid": not meta.import_invalid,
            "Import_raw": float(meta.import_raw) if meta.import_raw is not None else 0.0,
            "Import_invalid_reason": meta.import_reason or "",
            "Export_valid": not meta.export_invalid,
            "Export_raw": float(meta.export_raw) if meta.export_raw is not None else 0.0,
            "Export_invalid_reason": meta.export_reason or "",
            "Has_invalid_data": meta.has_invalid_data,
        }

        return doc, float(total_import), float(total_export)


# --------------------------------------------------------------------
# State models
# --------------------------------------------------------------------

@dataclass
class DeviceState:
    device_cfg: dict[str, Any]
    server_cfg: dict[str, Any]
    collection: Collection
    last_import: float
    last_export: float
    site: str
    server_unit_id: int

    device_reader: Optional[ModbusReader] = None
    server_reader: Optional[ModbusReader] = None

    first_read: bool = True

    consecutive_errors: int = 0
    backoff_until: float = 0.0

    next_run_at: float = field(default_factory=lambda: time.time() + random.uniform(*SLEEP_SECONDS_RANGE))
    last_run_at: float = 0.0

    last_db_save: float = 0.0
    run_count: int = 0
    last_debug_print: float = 0.0


@dataclass
class EmMemberState:
    cfg: dict[str, Any]
    reader: Optional[ModbusReader] = None
    consecutive_errors: int = 0
    backoff_until: float = 0.0


@dataclass
class EmGroupState:
    group_name: str
    members: list[EmMemberState]
    server_cfg: dict[str, Any]
    collection: Collection
    collection_name: str

    last_import: float
    last_export: float

    site: str
    server_unit_id: int

    server_reader: Optional[ModbusReader] = None

    consecutive_errors: int = 0
    backoff_until: float = 0.0

    next_run_at: float = field(default_factory=lambda: time.time() + random.uniform(*SLEEP_SECONDS_RANGE))
    last_run_at: float = 0.0

    last_db_save: float = 0.0
    run_count: int = 0
    last_debug_print: float = 0.0


# --------------------------------------------------------------------
# Polling helpers
# --------------------------------------------------------------------

def safe_write_float32(reader: ModbusReader, value: Any, reg_key: str, unit_id: int) -> None:
    if value is None:
        return
    try:
        if value != value:
            return
        if abs(value) == float("inf"):
            return
        if not isinstance(value, (int, float)):
            return
        reader.write_float32(float(value), SERVER_REGISTERS[reg_key], unit_id)
    except Exception as e:
        print(f"[ERROR] Fejl ved skrivning til {reg_key}: {e}")


class DevicePoller:
    def __init__(self, energy_reader: EnergyReader, repo: MongoEnergyRepository, reporter: ErrorReporter) -> None:
        self._energy_reader = energy_reader
        self._repo = repo
        self._reporter = reporter

    def poll_once(self, state: DeviceState) -> None:
        now = time.time()
        if now < state.backoff_until:
            return

        site = state.site
        dev = state.device_cfg
        srv = state.server_cfg

        raw_interval = (now - state.last_run_at) if state.last_run_at > 0 else random.randint(*SLEEP_SECONDS_RANGE)
        actual_interval = min(raw_interval, 10.0)

        time_since_last_run = (now - state.last_run_at) if state.last_run_at > 0 else float("inf")
        baseline_read = state.first_read or time_since_last_run > 3600.0

        # Ensure device connection.
        if not state.device_reader or not state.device_reader.is_open():
            tcp = connect_modbus(dev["ip"], dev["port"])
            if tcp:
                state.device_reader = ModbusReader(tcp, dev["unit_id"])
                print(f"✅ {site}: device forbundet ({dev['ip']}:{dev['port']})")
            else:
                cat, sev = categorize_message("kan ikke forbinde til device")
                self._reporter.record(ErrorEvent(site=site, message="kan ikke forbinde til device", category=cat, severity=sev))
                self._apply_error_backoff(state, close_device=True)
                return

        # Ensure server connection.
        if not state.server_reader or not state.server_reader.is_open():
            ip = srv.get("ip", "localhost")
            port = srv.get("port", 650)
            tcp = connect_modbus(ip, port)
            if tcp:
                state.server_reader = ModbusReader(tcp, 1)
                print(f"✅ {site}: server forbundet ({ip}:{port})")
            else:
                cat, sev = categorize_message("kan ikke forbinde til server")
                self._reporter.record(ErrorEvent(site=site, message="kan ikke forbinde til server", category=cat, severity=sev))
                self._apply_error_backoff(state, close_server=True)
                return

        # Read values.
        try:
            ap, total_imp, total_exp, meta = self._energy_reader.read(
                state.device_reader,
                dev,
                state.last_import,
                state.last_export,
                actual_interval,
                first_read=baseline_read,
            )
            state.first_read = False
        except ModbusException as e:
            dev_ip = dev.get("ip", "unknown")
            dev_port = dev.get("port", "unknown")
            dev_unit = dev.get("unit_id", "unknown")
            msg = f"Modbus fejl på device {site} ({dev_ip}:{dev_port}, unit {dev_unit}): {e}"
            cat, sev = categorize_message(msg)
            self._reporter.record(ErrorEvent(site=site, message=msg, category=cat, severity=sev))
            if state.device_reader:
                state.device_reader.close()
            state.device_reader = None
            self._apply_error_backoff(state)
            return
        except Exception as e:
            msg = f"Uventet fejl i read for {site}: {e}"
            cat, sev = categorize_message(msg)
            self._reporter.record(ErrorEvent(site=site, message=msg, category=cat, severity=sev))
            self._apply_error_backoff(state)
            return

        # Write values to server.
        try:
            assert state.server_reader is not None
            safe_write_float32(state.server_reader, ap, "active_power", state.server_unit_id)
            if abs(total_imp - state.last_import) > FLOAT_TOLERANCE_WH:
                safe_write_float32(state.server_reader, total_imp, "total_import", state.server_unit_id)
            if abs(total_exp - state.last_export) > FLOAT_TOLERANCE_WH:
                safe_write_float32(state.server_reader, total_exp, "total_export", state.server_unit_id)
        except Exception as e:
            cat, sev = categorize_message(f"Fejl ved skrivning til server: {e}")
            self._reporter.record(ErrorEvent(site=site, message=f"Fejl ved skrivning til server: {e}", category=cat, severity=sev))
            self._apply_error_backoff(state, close_server=True)
            return

        # Persist to DB periodically (non-blocking: enqueue).
        if now - state.last_db_save >= DB_SAVE_INTERVAL_SECONDS:
            doc, new_imp, new_exp = self._repo.build_sample_doc(
                dev,
                state.last_import,
                state.last_export,
                ap,
                total_imp,
                total_exp,
                meta,
            )
            try:
                db_queue.put_nowait((state.collection.name, doc))
            except Full:
                # Drop oldest; keep newest (best effort)
                try:
                    _ = db_queue.get_nowait()
                    db_queue.put_nowait((state.collection.name, doc))
                except Exception:
                    pass

            # Update baselines immediately (DB may be delayed; logic continues)
            state.last_import, state.last_export = new_imp, new_exp
            state.last_db_save = now
        else:
            if not meta.import_invalid:
                state.last_import = total_imp
            if not meta.export_invalid:
                state.last_export = total_exp

        # Success path.
        state.consecutive_errors = 0
        state.backoff_until = 0.0

        state.last_run_at = now
        # ✅ Stabil scheduling: altid "nu + 2-3 sek"
        state.next_run_at = now + random.randint(*SLEEP_SECONDS_RANGE)
        state.run_count += 1

        self._maybe_print_status(state, ap, total_imp, total_exp, meta, actual_interval)

    def _apply_error_backoff(self, state: DeviceState, *, close_device: bool = False, close_server: bool = False) -> None:
        state.consecutive_errors += 1

        if close_device and state.device_reader:
            state.device_reader.close()
            state.device_reader = None
        if close_server and state.server_reader:
            state.server_reader.close()
            state.server_reader = None

        if state.consecutive_errors >= 3:
            backoff = random.randint(BACKOFF_MIN_SECONDS, BACKOFF_MAX_SECONDS)
            print(f"⏸️ {state.site}: backoff {backoff}s pga. gentagne fejl")
            state.backoff_until = time.time() + backoff
            state.consecutive_errors = 0

        # også stabil schedule efter fejl (så den ikke spammer loopet)
        state.next_run_at = time.time() + random.randint(*SLEEP_SECONDS_RANGE)

    def _maybe_print_status(
        self,
        state: DeviceState,
        ap: float,
        total_imp: float,
        total_exp: float,
        meta: EnergyMeta,
        interval_seconds: float
    ) -> None:
        if not meta.has_invalid_data:
            return

        now = time.time()
        if now - state.last_debug_print < 60:
            return

        state.last_debug_print = now
        invalid_flags = []
        if meta.active_power_invalid:
            invalid_flags.append("AP")
        if meta.import_invalid:
            invalid_flags.append("IMP")
        if meta.export_invalid:
            invalid_flags.append("EXP")

        print(
            f"⚠️ {state.site}: "
            f"AP={ap:.1f}W Imp={total_imp:.1f}Wh Exp={total_exp:.1f}Wh "
            f"(interval={interval_seconds:.1f}s, invalid={','.join(invalid_flags)})"
        )


class EmGroupPoller:
    def __init__(self, energy_reader: EnergyReader, repo: MongoEnergyRepository, reporter: ErrorReporter) -> None:
        self._energy_reader = energy_reader
        self._repo = repo
        self._reporter = reporter

    def _validate_total_with_tolerance(
        self, new: float, last: float, value_name: str, first_read: bool, max_diff: float
    ) -> tuple[bool, float, str]:
        if new < MIN_TOTAL_VALUE:
            return False, last, f"{value_name} negativ ({new})"

        if first_read:
            return True, new, "første læsning efter restart eller lang afbrydelse"

        if last <= 1.0:
            return True, new, "første læsning"

        if new < last * MAX_TOTAL_RESET_THRESHOLD:
            return True, new, "meter reset eller data nulstillet"

        diff = new - last
        if diff < -FLOAT_TOLERANCE_WH:
            return False, last, f"{value_name} faldt ({last} -> {new})"

        if last < 10000 and diff > max_diff:
            extended_limit = max_diff * 10
            if diff > extended_limit:
                return False, last, f"{value_name} spike ({diff} Wh) selv med udvidet grænse"
            return True, new, f"OK (tidlig fase, diff={diff} Wh)"

        if diff > max_diff:
            return False, last, f"{value_name} spike ({diff} Wh)"

        return True, new, "OK"

    def poll_once(self, state: EmGroupState) -> None:
        now = time.time()
        if now < state.backoff_until:
            return

        site = state.site
        srv = state.server_cfg

        raw_interval = (now - state.last_run_at) if state.last_run_at > 0 else random.randint(*SLEEP_SECONDS_RANGE)
        actual_interval = min(raw_interval, 10.0)
        target_interval = random.randint(*SLEEP_SECONDS_RANGE)

        # Ensure server connection.
        if not state.server_reader or not state.server_reader.is_open():
            ip = srv.get("ip", "localhost")
            port = srv.get("port", 650)
            tcp = connect_modbus(ip, port)
            if tcp:
                state.server_reader = ModbusReader(tcp, 1)
                print(f"✅ {site}: server forbundet ({ip}:{port})")
            else:
                cat, sev = categorize_message("kan ikke forbinde til server")
                self._reporter.record(ErrorEvent(site=site, message="kan ikke forbinde til server", category=cat, severity=sev))
                self._apply_group_backoff(state, close_server=True)
                return

        total_ap = 0.0
        total_imp_sum = 0.0
        total_exp_sum = 0.0
        success_count = 0

        group_meta = EnergyMeta(active_power_raw=0.0, import_raw=0.0, export_raw=0.0)
        reasons_ap: list[str] = []
        reasons_imp: list[str] = []
        reasons_exp: list[str] = []

        for member in state.members:
            em_cfg = member.cfg
            em_site = em_cfg.get("site", "EM")

            if now < member.backoff_until:
                continue

            if not member.reader or not member.reader.is_open():
                tcp = connect_modbus(em_cfg["ip"], em_cfg["port"])
                if tcp:
                    member.reader = ModbusReader(tcp, em_cfg["unit_id"])
                    print(f"✅ {site}/{em_site}: device forbundet ({em_cfg['ip']}:{em_cfg['port']})")
                else:
                    cat, sev = categorize_message("kan ikke forbinde til EM-device")
                    self._reporter.record(ErrorEvent(site=f"{site}/{em_site}", message="kan ikke forbinde til EM-device", category=cat, severity=sev))
                    self._apply_member_backoff(member, prefix=f"{site}/{em_site}")
                    continue

            try:
                ap, imp, exp, meta = self._energy_reader.read(
                    member.reader,
                    em_cfg,
                    0.0,
                    0.0,
                    target_interval,
                    first_read=True,
                )
            except ModbusException as e:
                dev_ip = em_cfg.get("ip", "unknown")
                dev_port = em_cfg.get("port", "unknown")
                dev_unit = em_cfg.get("unit_id", "unknown")
                msg = f"Modbus fejl på EM-device {site}/{em_site} ({dev_ip}:{dev_port}, unit {dev_unit}): {e}"
                cat, sev = categorize_message(msg)
                self._reporter.record(ErrorEvent(site=f"{site}/{em_site}", message=msg, category=cat, severity=sev))
                if member.reader:
                    member.reader.close()
                member.reader = None
                self._apply_member_backoff(member, prefix=f"{site}/{em_site}")
                continue
            except Exception as e:
                msg = f"Uventet fejl i read for EM-device {site}/{em_site}: {e}"
                cat, sev = categorize_message(msg)
                self._reporter.record(ErrorEvent(site=f"{site}/{em_site}", message=msg, category=cat, severity=sev))
                self._apply_member_backoff(member, prefix=f"{site}/{em_site}")
                continue

            member.consecutive_errors = 0

            total_ap += ap
            imp_value = meta.import_raw if meta.import_raw is not None else imp
            exp_value = meta.export_raw if meta.export_raw is not None else exp
            total_imp_sum += float(imp_value)
            total_exp_sum += float(exp_value)
            success_count += 1

            if meta.active_power_invalid:
                group_meta.active_power_invalid = True
                reasons_ap.append(f"{em_site}: {meta.active_power_reason}")
            group_meta.active_power_raw = float(group_meta.active_power_raw or 0.0) + float(meta.active_power_raw or ap)

            if meta.import_invalid:
                if "negativ" in meta.import_reason.lower() or "nan" in meta.import_reason.lower():
                    group_meta.import_invalid = True
                    reasons_imp.append(f"{em_site}: {meta.import_reason}")
            group_meta.import_raw = float(group_meta.import_raw or 0.0) + float(meta.import_raw or imp)

            if meta.export_invalid:
                if "negativ" in meta.export_reason.lower() or "nan" in meta.export_reason.lower():
                    group_meta.export_invalid = True
                    reasons_exp.append(f"{em_site}: {meta.export_reason}")
            group_meta.export_raw = float(group_meta.export_raw or 0.0) + float(meta.export_raw or exp)

        if success_count == 0:
            cat, sev = categorize_message("Ingen succesfulde EM-læsninger i gruppe")
            self._reporter.record(ErrorEvent(site=site, message="Ingen succesfulde EM-læsninger i gruppe", category=cat, severity=sev))
            self._apply_group_backoff(state, close_server=True)
            return

        group_meta.active_power_reason = "; ".join(r for r in reasons_ap if r)
        group_meta.import_reason = "; ".join(r for r in reasons_imp if r)
        group_meta.export_reason = "; ".join(r for r in reasons_exp if r)

        # Check if any member has a valid import/export register (with address)
        def has_valid_register(member_cfg: dict, reg_name: str) -> bool:
            regs = (member_cfg.get("registers", {}) or {})
            reg = regs.get(reg_name)
            return bool(reg and isinstance(reg, dict) and reg.get("address"))
        
        has_import_register = any(has_valid_register(m.cfg, "total_import") for m in state.members)
        has_export_register = any(has_valid_register(m.cfg, "total_export") for m in state.members)

        if not has_import_register and not has_export_register:
            effective_interval = max(float(target_interval), 1.0)
            energy_wh = (total_ap * effective_interval) / 3600.0

            if total_ap > 0:
                total_imp_sum = state.last_import + energy_wh
                total_exp_sum = state.last_export
            elif total_ap < 0:
                total_imp_sum = state.last_import
                total_exp_sum = state.last_export + abs(energy_wh)
            else:
                total_imp_sum = state.last_import
                total_exp_sum = state.last_export

            group_meta.import_raw = total_imp_sum
            group_meta.export_raw = total_exp_sum
            if not group_meta.import_reason:
                group_meta.import_reason = "Beregnet fra active power"
            if not group_meta.export_reason:
                group_meta.export_reason = "Beregnet fra active power"

        time_since_last_run = (now - state.last_run_at) if state.last_run_at > 0 else float("inf")
        first_read_for_group = (
            state.run_count == 0 or
            (state.last_import == 0.0 and state.last_export == 0.0) or
            time_since_last_run > 3600.0
        )

        if total_imp_sum >= 0:
            ok_imp, validated_imp, reason_imp = self._validate_total_with_tolerance(
                total_imp_sum, state.last_import, "Import (sum)", first_read_for_group, MAX_REASONABLE_DIFF_WH_EM_GROUP
            )
            if not ok_imp:
                group_meta.import_invalid = True
                group_meta.import_reason = (group_meta.import_reason + "; " if group_meta.import_reason else "") + f"Sum validation failed: {reason_imp}"
                cat, sev = categorize_message(f"Import sum invalid: {reason_imp}")
                self._reporter.record(ErrorEvent(site=site, message=f"Import sum invalid: {reason_imp}", category=cat, severity=sev))
                total_imp_sum = state.last_import
            else:
                total_imp_sum = validated_imp

        if total_exp_sum >= 0:
            ok_exp, validated_exp, reason_exp = self._validate_total_with_tolerance(
                total_exp_sum, state.last_export, "Export (sum)", first_read_for_group, MAX_REASONABLE_DIFF_WH_EM_GROUP
            )
            if not ok_exp:
                group_meta.export_invalid = True
                group_meta.export_reason = (group_meta.export_reason + "; " if group_meta.export_reason else "") + f"Sum validation failed: {reason_exp}"
                cat, sev = categorize_message(f"Export sum invalid: {reason_exp}")
                self._reporter.record(ErrorEvent(site=site, message=f"Export sum invalid: {reason_exp}", category=cat, severity=sev))
                total_exp_sum = state.last_export
            else:
                total_exp_sum = validated_exp

        # Write aggregated values.
        try:
            assert state.server_reader is not None
            safe_write_float32(state.server_reader, total_ap, "active_power", state.server_unit_id)
            if abs(total_imp_sum - state.last_import) > FLOAT_TOLERANCE_WH:
                safe_write_float32(state.server_reader, total_imp_sum, "total_import", state.server_unit_id)
            if abs(total_exp_sum - state.last_export) > FLOAT_TOLERANCE_WH:
                safe_write_float32(state.server_reader, total_exp_sum, "total_export", state.server_unit_id)
        except Exception as e:
            cat, sev = categorize_message(f"Fejl ved skrivning til server (EM-group): {e}")
            self._reporter.record(ErrorEvent(site=site, message=f"Fejl ved skrivning til server (EM-group): {e}", category=cat, severity=sev))
            self._apply_group_backoff(state, close_server=True)
            return

        # Persist to DB periodically (non-blocking: enqueue).
        if now - state.last_db_save >= DB_SAVE_INTERVAL_SECONDS:
            project_nr = ""
            if state.members:
                project_nr = state.members[0].cfg.get("project_nr") or ""

            group_device_cfg = {
                "site": state.group_name or "",
                "project_nr": project_nr,
                "name": state.group_name or "",
                "data_collection": state.collection_name,
            }

            doc, new_imp, new_exp = self._repo.build_sample_doc(
                group_device_cfg,
                state.last_import,
                state.last_export,
                total_ap,
                total_imp_sum,
                total_exp_sum,
                group_meta,
            )

            try:
                db_queue.put_nowait((state.collection.name, doc))
            except Full:
                try:
                    _ = db_queue.get_nowait()
                    db_queue.put_nowait((state.collection.name, doc))
                except Exception:
                    pass

            state.last_import, state.last_export = new_imp, new_exp
            state.last_db_save = now
        else:
            if not group_meta.import_invalid:
                state.last_import = total_imp_sum
            if not group_meta.export_invalid:
                state.last_export = total_exp_sum

        # Success path scheduling.
        state.consecutive_errors = 0
        state.backoff_until = 0.0
        state.last_run_at = now
        state.next_run_at = now + random.randint(*SLEEP_SECONDS_RANGE)
        state.run_count += 1

        self._maybe_print_status(state, total_ap, total_imp_sum, total_exp_sum, group_meta, actual_interval, success_count)

    def _apply_member_backoff(self, member: EmMemberState, *, prefix: str) -> None:
        member.consecutive_errors += 1
        if member.consecutive_errors >= 3:
            backoff = random.randint(BACKOFF_MIN_SECONDS, BACKOFF_MAX_SECONDS)
            print(f"⏸️ {prefix}: backoff {backoff}s pga. gentagne fejl (EM)")
            member.backoff_until = time.time() + backoff
            member.consecutive_errors = 0

    def _apply_group_backoff(self, state: EmGroupState, *, close_server: bool = False) -> None:
        state.consecutive_errors += 1
        if state.consecutive_errors >= 3:
            backoff = random.randint(BACKOFF_MIN_SECONDS, BACKOFF_MAX_SECONDS)
            print(f"⏸️ {state.site}: backoff {backoff}s pga. gentagne fejl (EM-group)")
            state.backoff_until = time.time() + backoff
            state.consecutive_errors = 0
            if close_server and state.server_reader:
                state.server_reader.close()
                state.server_reader = None

        state.next_run_at = time.time() + random.randint(*SLEEP_SECONDS_RANGE)

    def _maybe_print_status(
        self,
        state: EmGroupState,
        total_ap: float,
        total_imp_sum: float,
        total_exp_sum: float,
        meta: EnergyMeta,
        interval_seconds: float,
        success_count: int
    ) -> None:
        if not meta.has_invalid_data:
            return

        now = time.time()
        if now - state.last_debug_print < 60:
            return

        state.last_debug_print = now
        invalid_flags = []
        if meta.active_power_invalid:
            invalid_flags.append("AP")
        if meta.import_invalid:
            invalid_flags.append("IMP")
        if meta.export_invalid:
            invalid_flags.append("EXP")

        print(
            f"⚠️ {state.group_name} (EM-group): "
            f"AP={total_ap:.1f}W Imp={total_imp_sum:.1f}Wh Exp={total_exp_sum:.1f}Wh "
            f"(interval={interval_seconds:.1f}s, invalid={','.join(invalid_flags)}, EMs={success_count})"
        )


# --------------------------------------------------------------------
# Worker scheduling + Mongo batch writer
# --------------------------------------------------------------------

shutdown_event = threading.Event()
states_lock = threading.Lock()
reload_event = threading.Event()  # ✅ Pause workers during config reload

# Queue of (collection_name, doc)
db_queue: "Queue[tuple[str, dict[str, Any]]]" = Queue(maxsize=DB_QUEUE_MAXSIZE)


class MongoBatchWriter(threading.Thread):
    """
    Collect docs in RAM and write to MongoDB in batches PER COLLECTION.
    Pollers must never block on DB -> therefore we use a queue.
    """

    def __init__(self, mongo_db: Database):
        super().__init__(daemon=True, name="MongoBatchWriter")
        self._db = mongo_db
        self._buffers: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._last_flush = time.time()
        self._backoff = MONGO_RETRY_BACKOFF_MIN
        self._fail_count = 0

    def run(self) -> None:
        print("🧾 MongoBatchWriter startet")
        try:
            while not shutdown_event.is_set():
                self._drain_queue_nonblocking()
                self._flush_if_needed()
                time.sleep(0.05)

            # shutdown: final flush (best effort)
            self._drain_queue_nonblocking()
            self._flush_all_best_effort()
        finally:
            print("🧾 MongoBatchWriter stoppet")

    def _drain_queue_nonblocking(self) -> None:
        while True:
            try:
                coll_name, doc = db_queue.get_nowait()
            except Empty:
                break
            self._buffers[coll_name].append(doc)

    def _flush_if_needed(self) -> None:
        now = time.time()
        time_due = (now - self._last_flush) >= MONGO_FLUSH_INTERVAL_SECONDS
        size_due = any(len(buf) >= MONGO_BATCH_SIZE for buf in self._buffers.values())
        if not (time_due or size_due):
            return

        self._flush_some_with_retry()
        self._last_flush = time.time()

    def _flush_some_with_retry(self) -> None:
        if not any(self._buffers.values()):
            self._fail_count = 0
            self._backoff = MONGO_RETRY_BACKOFF_MIN
            return

        try:
            for coll_name, docs in list(self._buffers.items()):
                if not docs:
                    continue
                batch = docs[:MONGO_BATCH_SIZE]
                self._db[coll_name].insert_many(batch, ordered=False)
                del docs[:len(batch)]

            # success -> reset tracking
            self._fail_count = 0
            self._backoff = MONGO_RETRY_BACKOFF_MIN

        except Exception as e:
            self._fail_count += 1

            if self._fail_count >= MONGO_LONG_BACKOFF_AFTER_FAILS:
                sleep_s = random.uniform(MONGO_LONG_BACKOFF_MIN, MONGO_LONG_BACKOFF_MAX)
                print(f"💥 MongoBatchWriter: lang backoff {sleep_s:.0f}s (Mongo nede?) fejl={self._fail_count}: {e}")
                time.sleep(sleep_s)
            else:
                print(f"💥 MongoBatchWriter: insert fejlede ({e}) -> retry om {self._backoff:.1f}s")
                time.sleep(self._backoff)
                self._backoff = min(self._backoff * 2, MONGO_RETRY_BACKOFF_MAX)

    def _flush_all_best_effort(self) -> None:
        tries = 3
        for _ in range(tries):
            if not any(self._buffers.values()):
                return
            try:
                for coll_name, docs in list(self._buffers.items()):
                    if docs:
                        self._db[coll_name].insert_many(docs, ordered=False)
                        docs.clear()
                return
            except Exception as e:
                print(f"💥 MongoBatchWriter shutdown flush fejlede: {e}")
                time.sleep(1.0)


class Worker(threading.Thread):
    def __init__(
        self,
        name: str,
        device_states: list[DeviceState],
        group_states: list[EmGroupState],
        device_poller: DevicePoller,
        group_poller: EmGroupPoller
    ):
        super().__init__(daemon=True, name=name)
        self.device_states = device_states
        self.group_states = group_states
        self.device_poller = device_poller
        self.group_poller = group_poller

    def run(self) -> None:
        print(f"▶️ Worker {self.name} startet ({len(self.device_states) + len(self.group_states)} states)")
        try:
            while not shutdown_event.is_set():
                # ✅ Pause polling while reload in progress (avoid race conditions)
                if reload_event.is_set():
                    time.sleep(0.2)
                    continue

                now = time.time()
                with states_lock:
                    devices_copy = list(self.device_states)
                    groups_copy = list(self.group_states)

                for st in devices_copy:
                    if now >= st.next_run_at and now >= st.backoff_until:
                        self.device_poller.poll_once(st)

                for st in groups_copy:
                    if now >= st.next_run_at and now >= st.backoff_until:
                        self.group_poller.poll_once(st)

                time.sleep(0.2)
        finally:
            print(f"⏹️ Worker {self.name} lukker connections...")
            with states_lock:
                for st in self.device_states:
                    if st.device_reader:
                        st.device_reader.close()
                    if st.server_reader:
                        st.server_reader.close()
                    st.device_reader = None
                    st.server_reader = None

                for st in self.group_states:
                    if st.server_reader:
                        st.server_reader.close()
                    st.server_reader = None
                    for m in st.members:
                        if m.reader:
                            m.reader.close()
                        m.reader = None


# --------------------------------------------------------------------
# State builder + config reloader
# --------------------------------------------------------------------

class StateFactory:
    def __init__(self, repo: MongoEnergyRepository) -> None:
        self._repo = repo

    def build_device_states(self, devices: list[dict[str, Any]], servers: list[dict[str, Any]]) -> list[DeviceState]:
        out: list[DeviceState] = []
        for dev in devices:
            coll = self._repo.get_collection(dev)
            last_imp, last_exp = self._repo.load_last_totals(coll)
            for srv in servers:
                out.append(
                    DeviceState(
                        device_cfg=dev,
                        server_cfg=srv,
                        collection=coll,
                        last_import=last_imp,
                        last_export=last_exp,
                        site=dev.get("site", "unknown"),
                        server_unit_id=dev.get("server_unit_id", 1),
                    )
                )
        return out

    def build_group_states(self, em_groups: dict[str, list[dict[str, Any]]], servers: list[dict[str, Any]]) -> list[EmGroupState]:
        out: list[EmGroupState] = []
        for group_name, group_devices in em_groups.items():
            if not group_devices:
                continue

            first = group_devices[0]
            gdc = first.get("group_data_collection")
            if gdc and str(gdc).strip() and str(gdc).strip().lower() != "undefined":
                group_collection_name = str(gdc).strip()
            else:
                project_nr_raw = first.get("project_nr")
                project_nr = str(project_nr_raw).strip() if project_nr_raw is not None else ""
                device_name = first.get("name", "unknown_device").replace(" ", "_")
                device_name = "".join(c if c.isalnum() or c == "_" else "_" for c in device_name)
                if project_nr:
                    group_collection_name = f"{project_nr}_{device_name}"
                else:
                    group_collection_name = device_name
                print(f"⚠️ Warning: Auto-generated collection name for EM group '{group_name}': {group_collection_name}")

            group_device_cfg = {
                "site": group_name,
                "project_nr": first.get("project_nr", ""),
                "name": group_name,
                "data_collection": group_collection_name,
            }
            coll = self._repo.get_collection(group_device_cfg)
            last_imp, last_exp = self._repo.load_last_totals(coll)

            members = [EmMemberState(cfg=d) for d in group_devices]

            for srv in servers:
                out.append(
                    EmGroupState(
                        group_name=group_name,
                        members=members,
                        server_cfg=srv,
                        collection=coll,
                        collection_name=group_collection_name,
                        last_import=last_imp,
                        last_export=last_exp,
                        site=f"EM_GROUP:{group_name}",
                        server_unit_id=group_devices[0].get("server_unit_id", 1),
                    )
                )
        return out


def reload_config_if_needed(
    cfg_loader: MongoConfigLoader,
    repo: MongoEnergyRepository,
    factory: StateFactory,
    workers: list[Worker],
    last_reload_time: float,
    servers: list[dict[str, Any]],
) -> float:
    now = time.time()
    if now - last_reload_time < CONFIG_RELOAD_INTERVAL_SECONDS:
        return last_reload_time

    print("\n🔄 Reloader config fra MongoDB...")

    # ✅ Pause workers during reload to avoid race conditions
    reload_event.set()
    time.sleep(0.3)  # let any in-flight poll_once finish

    try:
        cfg = cfg_loader.load()
        devices = cfg.get("devices", [])
        em_groups = cfg.get("em_groups", {})

        new_devices = factory.build_device_states(devices, servers)
        new_groups = factory.build_group_states(em_groups, servers)

        # Identify by stable ids
        new_device_ids = {("device", st.site, st.server_unit_id) for st in new_devices}
        new_group_ids = {("group", st.group_name) for st in new_groups}

        new_device_map = {(st.site, st.server_unit_id): st for st in new_devices}
        new_group_map = {st.group_name: st for st in new_groups}

        with states_lock:
            existing_device_ids = {("device", st.site, st.server_unit_id) for w in workers for st in w.device_states}
            existing_group_ids = {("group", st.group_name) for w in workers for st in w.group_states}

            # Remove deleted
            removed = 0
            for w in workers:
                before = len(w.device_states)
                w.device_states = [st for st in w.device_states if ("device", st.site, st.server_unit_id) in new_device_ids]
                removed += before - len(w.device_states)

                before = len(w.group_states)
                w.group_states = [st for st in w.group_states if ("group", st.group_name) in new_group_ids]
                removed += before - len(w.group_states)

            if removed:
                print(f"🗑️ Fjernet {removed} slettede devices/groups")

            # Update existing with new config (preserve runtime state)
            updated = 0
            for w in workers:
                # Devices
                for i, st in enumerate(w.device_states):
                    key = (st.site, st.server_unit_id)
                    if key not in new_device_map:
                        continue

                    new_st = new_device_map[key]
                    old_cfg = st.device_cfg
                    new_cfg = new_st.device_cfg

                    config_changed = (
                        old_cfg.get("ip") != new_cfg.get("ip") or
                        old_cfg.get("port") != new_cfg.get("port") or
                        old_cfg.get("unit_id") != new_cfg.get("unit_id") or
                        old_cfg.get("reading") != new_cfg.get("reading") or
                        old_cfg.get("table") != new_cfg.get("table") or
                        old_cfg.get("registers") != new_cfg.get("registers")
                    )

                    if config_changed:
                        # close old connections (safe because workers paused)
                        if st.device_reader:
                            st.device_reader.close()
                        if st.server_reader:
                            st.server_reader.close()

                        # preserve runtime fields
                        new_st.last_import = st.last_import
                        new_st.last_export = st.last_export
                        new_st.last_db_save = st.last_db_save
                        new_st.run_count = st.run_count
                        new_st.last_run_at = st.last_run_at
                        new_st.next_run_at = st.next_run_at
                        new_st.first_read = st.first_read
                        new_st.backoff_until = st.backoff_until
                        new_st.consecutive_errors = st.consecutive_errors
                        new_st.last_debug_print = st.last_debug_print

                        w.device_states[i] = new_st
                        updated += 1

                # Groups
                for i, st in enumerate(w.group_states):
                    if st.group_name not in new_group_map:
                        continue

                    new_st = new_group_map[st.group_name]

                    # check member config differences
                    old_member_cfgs = [m.cfg for m in st.members]
                    new_member_cfgs = [m.cfg for m in new_st.members]
                    members_changed = old_member_cfgs != new_member_cfgs

                    if members_changed:
                        if st.server_reader:
                            st.server_reader.close()
                        for m in st.members:
                            if m.reader:
                                m.reader.close()

                        new_st.last_import = st.last_import
                        new_st.last_export = st.last_export
                        new_st.last_db_save = st.last_db_save
                        new_st.run_count = st.run_count
                        new_st.last_run_at = st.last_run_at
                        new_st.next_run_at = st.next_run_at
                        new_st.backoff_until = st.backoff_until
                        new_st.consecutive_errors = st.consecutive_errors
                        new_st.last_debug_print = st.last_debug_print

                        w.group_states[i] = new_st
                        updated += 1

            if updated:
                print(f"🔄 Opdateret {updated} eksisterende devices/groups med nye indstillinger")

            # Add new
            added = 0
            for st in new_devices:
                if ("device", st.site, st.server_unit_id) not in existing_device_ids:
                    min_worker = min(workers, key=lambda ww: len(ww.device_states) + len(ww.group_states))
                    min_worker.device_states.append(st)
                    added += 1

            for st in new_groups:
                if ("group", st.group_name) not in existing_group_ids:
                    min_worker = min(workers, key=lambda ww: len(ww.device_states) + len(ww.group_states))
                    min_worker.group_states.append(st)
                    added += 1

            if added:
                print(f"✅ Tilføjet {added} nye devices/groups")

            if added or removed or updated:
                total = sum(len(w.device_states) + len(w.group_states) for w in workers)
                print(f"📊 Total states nu: {total}")
            else:
                print("ℹ️ Ingen ændringer i devices/groups")

        return time.time()

    except Exception as e:
        print(f"[ERROR] Fejl ved config reload: {e}")
        return last_reload_time

    finally:
        reload_event.clear()  # ✅ resume workers


# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------

def build_reporter(meters_db: Database) -> ErrorReporter:
    reporters: list[ErrorReporter] = [RateLimitedConsoleReporter()]
    try:
        reporters.append(ThrottledMongoEventReporter(meters_db["service_events"]))
    except Exception:
        pass
    return CompositeReporter(reporters)


def main() -> None:
    print("Starter Modbus Client Manager (refactored)...")

    mongo_client = MongoClient(
        "mongodb://vbserver:27017",
        serverSelectionTimeoutMS=5000,
        maxPoolSize=50
    )

    cfg_loader = MongoConfigLoader(client=mongo_client)
    try:
        cfg = cfg_loader.load()
    except Exception as e:
        print(f"[ERROR] Kan ikke indlæse config: {e}")
        mongo_client.close()
        return

    devices = cfg.get("devices", [])
    em_groups = cfg.get("em_groups", {})
    servers = cfg.get("servers", []) or list(SERVERS)

    print(f"Config: {len(devices)} devices, {len(em_groups)} EM-groups, {len(servers)} servers")

    mongo_db = mongo_client[ENERGY_LOGS_DB]
    meters_db = mongo_client[METERS_DB]

    reporter = build_reporter(meters_db)
    repo = MongoEnergyRepository(mongo_db, reporter=reporter)
    factory = StateFactory(repo)

    validator = EnergyValidator()
    energy_reader = EnergyReader(validator, reporter)

    device_poller = DevicePoller(energy_reader, repo, reporter)
    group_poller = EmGroupPoller(energy_reader, repo, reporter)

    device_states = factory.build_device_states(devices, servers)
    group_states = factory.build_group_states(em_groups, servers)

    total_states = len(device_states) + len(group_states)
    print(f"Total states (devices + EM-groups): {total_states}")

    if total_states == 0:
        print("Ingen states i config – afslutter")
        shutdown_event.set()
        cfg_loader.close()
        mongo_client.close()
        return

    # Start Mongo batch writer (non-blocking DB writes)
    mongo_writer = MongoBatchWriter(mongo_db)
    mongo_writer.start()

    # Workers
    states_per_worker = 2
    worker_count = max(1, min(200, (total_states + states_per_worker - 1) // states_per_worker))
    print(f"Opretter {worker_count} workers (ca. {states_per_worker} states per)")
    print(f"🔄 Auto-reload aktiveret: Tjekker for nye devices hver {CONFIG_RELOAD_INTERVAL_SECONDS}s\n")

    workers: list[Worker] = []
    for i in range(worker_count):
        dev_chunk = device_states[i::worker_count]
        grp_chunk = group_states[i::worker_count]
        w = Worker(
            name=f"W{i + 1}",
            device_states=dev_chunk,
            group_states=grp_chunk,
            device_poller=device_poller,
            group_poller=group_poller
        )
        w.start()
        workers.append(w)

    last_reload_time = time.time()

    try:
        while any(w.is_alive() for w in workers):
            time.sleep(5)
            last_reload_time = reload_config_if_needed(cfg_loader, repo, factory, workers, last_reload_time, servers)
    except KeyboardInterrupt:
        print("⏹️ Shutdown requested via KeyboardInterrupt")
        shutdown_event.set()

    # Shutdown
    shutdown_event.set()
    time.sleep(0.5)

    # best-effort join on mongo writer (flushes remaining docs)
    try:
        mongo_writer.join(timeout=5)
    except Exception:
        pass

    cfg_loader.close()
    mongo_client.close()
    print("Program afsluttet.")


if __name__ == "__main__":
    main()
