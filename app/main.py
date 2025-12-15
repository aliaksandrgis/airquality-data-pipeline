from __future__ import annotations

import json
import logging
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

import requests
import psycopg

# kafka-python bundles a vendored copy of six that isn't PEP 451 friendly on Python 3.13.
# Pre-register aliases and patch its meta importer before importing KafkaProducer.
try:
    import six
    import sys

    sys.modules.setdefault("kafka.vendor.six", six)
    sys.modules.setdefault("kafka.vendor.six.moves", six.moves)
except Exception:
    pass

try:
    import importlib.util
    import kafka.vendor.six as kafka_six  # type: ignore[attr-defined]

    if not hasattr(kafka_six._SixMetaPathImporter, "find_spec"):  # pragma: no cover
        def _find_spec(self, fullname: str, path=None, target=None):
            if fullname in self.known_modules:
                return importlib.util.spec_from_loader(fullname, self)
            return None

        kafka_six._SixMetaPathImporter.find_spec = _find_spec  # type: ignore[attr-defined]
except Exception:
    # If kafka isn't installed yet or already patched, just continue.
    pass

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from .config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
LOGGER = logging.getLogger("pipeline")

# Simple in-memory metadata caches to avoid repeated station lookups
_de_station_cache: Dict[str, Dict[str, Any]] = {}
_nl_station_cache: Dict[str, Dict[str, Any]] = {}
_pl_station_cache: Dict[str, Dict[str, Any]] = {}
_cursor_cache: Dict[str, Dict[Tuple[str, str], datetime]] = {}
_cursor_table_ready = False


NL_DEFAULT_STATIONS: List[str] = ["NL01491"]
NL_DEFAULT_COMPONENTS: List[str] = ["no2", "pm10", "pm25", "o3"]


def _get_stations_from_db(source: str) -> List[Dict[str, Any]]:
    """Return stations with coords for a given source from DB."""
    sql = """
    SELECT station_id, country, city, location_name, lat, lon
    FROM stations
    WHERE source = %s AND lat IS NOT NULL AND lon IS NOT NULL
    """
    with _get_db_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (source,))
        rows = cur.fetchall()
    stations: List[Dict[str, Any]] = []
    for row in rows:
        stations.append(
            {
                "station_id": row[0],
                "country": row[1],
                "city": row[2],
                "location_name": row[3],
                "lat": row[4],
                "lon": row[5],
            }
        )
    return stations


def _get_db_conn():
    conn_kwargs = dict(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        autocommit=True,
    )
    if settings.db_sslmode:
        conn_kwargs["sslmode"] = settings.db_sslmode
    return psycopg.connect(**conn_kwargs)


def _ensure_cursor_table() -> bool:
    global _cursor_table_ready
    if _cursor_table_ready:
        return True
    try:
        with _get_db_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ingestion_cursors (
                    source TEXT NOT NULL,
                    station_id TEXT NOT NULL,
                    pollutant TEXT NOT NULL,
                    last_observed_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (source, station_id, pollutant)
                )
                """
            )
        _cursor_table_ready = True
        return True
    except Exception:
        LOGGER.warning(
            "Unable to ensure ingestion_cursors table; deduplication will be skipped",
            exc_info=True,
        )
        return False


def _ensure_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _get_cursor_map(source: str) -> Dict[Tuple[str, str], datetime] | None:
    if not _ensure_cursor_table():
        return None
    if source in _cursor_cache:
        return _cursor_cache[source]
    cursor_map: Dict[Tuple[str, str], datetime] = {}
    try:
        with _get_db_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT station_id, pollutant, last_observed_at
                FROM ingestion_cursors
                WHERE source = %s
                """,
                (source,),
            )
            for station_id, pollutant, last_observed_at in cur.fetchall():
                if station_id is None or pollutant is None or last_observed_at is None:
                    continue
                aware_ts = _ensure_utc(last_observed_at)
                if aware_ts:
                    cursor_map[(str(station_id), str(pollutant))] = aware_ts
    except Exception:
        LOGGER.warning("Failed to load ingestion cursors for %s", source, exc_info=True)
        return None
    _cursor_cache[source] = cursor_map
    return cursor_map


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, str):
        ts_str = value.strip()
        if not ts_str:
            return None
        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(ts_str)
        except ValueError:
            return None
    return None


def _filter_new_measurements(
    source: str, data: List[Dict[str, Any]]
) -> tuple[List[Dict[str, Any]], Dict[Tuple[str, str], datetime]]:
    cursor_map = _get_cursor_map(source)
    if cursor_map is None:
        return data, {}
    filtered: List[Dict[str, Any]] = []
    updates: Dict[Tuple[str, str], datetime] = {}
    for record in data:
        station = record.get("station_id")
        pollutant = record.get("pollutant")
        ts_value = record.get("timestamp")
        parsed_ts = _parse_timestamp(ts_value)
        parsed_ts = _ensure_utc(parsed_ts) or parsed_ts
        if (
            station is None
            or pollutant is None
            or parsed_ts is None
            or isinstance(station, dict)
            or isinstance(pollutant, dict)
        ):
            filtered.append(record)
            continue
        key = (str(station), str(pollutant))
        last_known = _ensure_utc(updates.get(key)) or _ensure_utc(cursor_map.get(key))
        if last_known is None or parsed_ts > last_known:
            filtered.append(record)
            updates[key] = _ensure_utc(parsed_ts) or parsed_ts
    return filtered, updates


def _commit_cursor_updates(
    pending_updates: Dict[str, Dict[Tuple[str, str], datetime]]
) -> None:
    if not pending_updates or not _ensure_cursor_table():
        return
    try:
        with _get_db_conn() as conn, conn.cursor() as cur:
            for source, updates in pending_updates.items():
                if not updates:
                    continue
                values = []
                for key, ts in updates.items():
                    aware_ts = _ensure_utc(ts)
                    if aware_ts:
                        values.append((source, key[0], key[1], aware_ts))
                if not values:
                    continue
                cur.executemany(
                    """
                    INSERT INTO ingestion_cursors (
                        source, station_id, pollutant, last_observed_at
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (source, station_id, pollutant)
                    DO UPDATE SET last_observed_at = GREATEST(
                        ingestion_cursors.last_observed_at,
                        EXCLUDED.last_observed_at
                    )
                    """,
                    values,
                )
                cursor_map = _cursor_cache.setdefault(source, {})
                for key, ts in updates.items():
                    aware_ts = _ensure_utc(ts)
                    if aware_ts:
                        cursor_map[key] = aware_ts
    except Exception:
        LOGGER.warning("Failed to persist ingestion cursors", exc_info=True)


def _build_producer() -> KafkaProducer | None:
    # Build Kafka producer; if broker is unavailable return None and retry later.
    try:
        kafka_config: Dict[str, Any] = {
            "bootstrap_servers": settings.kafka_bootstrap.split(","),
            "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
        }
        # Optional SASL/SASL_SSL configuration (e.g. for Confluent Cloud)
        if settings.kafka_sasl_username and settings.kafka_sasl_password:
            security_protocol = settings.kafka_security_protocol or "SASL_SSL"
            if security_protocol.upper() == "PLAINTEXT":
                security_protocol = "SASL_SSL"
            kafka_config.update(
                {
                    "security_protocol": security_protocol,
                    "sasl_mechanism": settings.kafka_sasl_mechanism or "PLAIN",
                    "sasl_plain_username": settings.kafka_sasl_username,
                    "sasl_plain_password": settings.kafka_sasl_password,
                }
            )
        producer = KafkaProducer(**kafka_config)
        return producer
    except NoBrokersAvailable:
        LOGGER.warning("Kafka broker unavailable at %s", settings.kafka_bootstrap)
        return None


def _create_synthetic_measurements() -> List[Dict[str, Any]]:
    # Synthetic fallback batch when live sources return nothing (used only when live API disabled).
    now = datetime.now(timezone.utc).isoformat()
    stations = ["PL001", "DE021", "FR045"]
    pollutants = [
        ("pm25", "ug/m3"),
        ("pm10", "ug/m3"),
        ("no2", "ppb"),
        ("o3", "ppb"),
    ]
    values = []
    for station in stations:
        pollutant, unit = random.choice(pollutants)
        values.append(
            {
                "station_id": station,
                "pollutant": pollutant,
                "value": round(random.uniform(5, 55), 2),
                "unit": unit,
                "country": station[:2],
                "city": "demo",
                "timestamp": now,
                "source": "synthetic",
            }
        )
    return values


def _fetch_openaq_latest(limit: int = 3) -> List[Dict[str, Any]]:
    # Small backup pull from OpenAQ if live sources are empty.
    try:
        response = requests.get(
            "https://api.openaq.org/v2/latest",
            params={"limit": limit, "order_by": "lastUpdated"},
            timeout=10,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        LOGGER.warning("Failed to fetch OpenAQ data: %s", exc)
        return []

    results = []
    for entry in response.json().get("results", []):
        location = entry.get("location")
        country = entry.get("country")
        city = entry.get("city")
        for measurement in entry.get("measurements", []):
            results.append(
                {
                    "station_id": location,
                    "pollutant": measurement.get("parameter"),
                    "value": measurement.get("value"),
                    "unit": measurement.get("unit"),
                    "country": country,
                    "city": city,
                    "timestamp": measurement.get("lastUpdated"),
                    "source": "openaq",
                }
            )
    return results


# ---------- Station catalogs ----------


def _refresh_de_catalog() -> List[Dict[str, Any]]:
    """Fetch all DE stations from UBA API."""
    try:
        resp = requests.get(f"{settings.de_base_url}/stations/json", timeout=20)
        resp.raise_for_status()
        data = resp.json().get("data") or {}
    except requests.RequestException as exc:
        LOGGER.warning("Failed to fetch DE catalog: %s", exc)
        return []
    stations = []
    if isinstance(data, dict):
        for sid, entry in data.items():
            if isinstance(entry, list):
                # [station_id, code, city, ..., lon, lat, ...] UBA list format
                lat = entry[8] if len(entry) > 8 else None
                lon = entry[7] if len(entry) > 7 else None
                name = entry[2] if len(entry) > 2 else None
                city = entry[3] if len(entry) > 3 else None
            else:
                lat = entry.get("latitude")
                lon = entry.get("longitude")
                name = entry.get("name")
                city = entry.get("city")
            try:
                lat = float(str(lat)) if lat not in (None, "") else None
                lon = float(str(lon)) if lon not in (None, "") else None
            except ValueError:
                lat, lon = None, None
            if lat is None or lon is None:
                continue
            stations.append(
                {
                    "station_id": sid,
                    "source": "de",
                    "country": "DE",
                    "city": city,
                    "location_name": name or city,
                    "lat": lat,
                    "lon": lon,
                }
            )
    return stations


NL_REQUEST_LIMIT = 280  # чуть ниже 300/5мин, чтобы не ловить 429


def _log_nl_request(
    request_counter: Dict[str, int],
    context: str,
    url: str,
    params: Dict[str, Any] | None = None,
) -> int:
    """Increment and log NL request counter and details."""
    request_counter["count"] = request_counter.get("count", 0) + 1
    count = request_counter["count"]
    LOGGER.info(
        "NL request #%s (%s): GET %s params=%s",
        count,
        context,
        url,
        params or {},
    )
    return count


def _refresh_nl_catalog(request_counter: Dict[str, int]) -> List[Dict[str, Any]]:
    """Fetch NL stations: список номеров + detail по каждой станции, но без повторов по БД."""
    stations: List[Dict[str, Any]] = []
    page = 1
    last_page = 1
    LOGGER.info("Fetch all NL")
    while page <= last_page:
        try:
            # Базовая пауза используется только при 429, в обычном пути задержек нет
            base_sleep = 1.0
            retries = 0
            while True:
                try:
                    url = f"{settings.nl_base_url}/stations"
                    params = {"page": page}
                    resp = requests.get(
                        url,
                        params=params,
                        timeout=15,
                    )
                    resp.raise_for_status()
                    count = _log_nl_request(
                        request_counter,
                        "catalog list",
                        url,
                        params=params,
                    )
                    if count >= NL_REQUEST_LIMIT:
                        LOGGER.warning(
                            "NL request budget reached during catalog (count=%s)",
                            count,
                        )
                        return stations
                    break
                except requests.RequestException as exc:
                    status = getattr(getattr(exc, "response", None), "status_code", None)
                    if status == 429:
                        request_counter["http_429"] = request_counter.get("http_429", 0) + 1
                        if retries < 5:
                            retries += 1
                            base_sleep = max(base_sleep + 0.5, 1.1)
                            time.sleep(base_sleep)
                            continue
                    raise
            body = resp.json()
            data = body.get("data") or []
            pag = body.get("pagination") or {}
            last_page = int(pag.get("last_page") or last_page or 1)
            LOGGER.info("NL page %s/%s: got %s station ids", page, last_page, len(data))
            page += 1
            # detail по каждой станции (если её нет в БД)
            for row in data:
                num = row.get("number")
                if not num:
                    continue
                # detail
                detail_sleep = 1.0
                retries = 0
                detail = {}
                while True:
                    try:
                        detail_url = f"{settings.nl_base_url}/stations/{num}"
                        dresp = requests.get(
                            detail_url,
                            timeout=15,
                        )
                        dresp.raise_for_status()
                        count = _log_nl_request(
                            request_counter,
                            "catalog detail",
                            detail_url,
                        )
                        if count >= NL_REQUEST_LIMIT:
                            LOGGER.warning(
                                "NL request budget reached during catalog detail (count=%s)",
                                count,
                            )
                            return stations
                        detail = dresp.json().get("data") or {}
                        break
                    except requests.RequestException as exc:
                        status = getattr(exc.response, "status_code", None)
                        if status == 429:
                            request_counter["http_429"] = request_counter.get("http_429", 0) + 1
                            if retries < 3:
                                retries += 1
                                detail_sleep = max(detail_sleep + 0.5, 1.0)
                                time.sleep(detail_sleep)
                                continue
                        LOGGER.warning("NL detail failed for %s: %s", num, exc)
                        break
                coords = detail.get("geometry", {}).get("coordinates") if isinstance(detail, dict) else None
                lat = coords[1] if coords else detail.get("lat")
                lon = coords[0] if coords else detail.get("lon")
                if lat is None or lon is None:
                    continue
                stations.append(
                    {
                        "station_id": num,
                        "source": "luchtmeetnet",
                        "country": "NL",
                        "city": detail.get("municipality") or detail.get("locality"),
                        "location_name": detail.get("location") or num,
                        "lat": lat,
                        "lon": lon,
                    }
                )
        except requests.RequestException as exc:
            LOGGER.warning("NL catalog fetch failed on page %s: %s", page, exc)
            break

    normalized: List[Dict[str, Any]] = []
    for st in stations:
        try:
            lat = float(str(st["lat"])) if st["lat"] not in (None, "") else None
            lon = float(str(st["lon"])) if st["lon"] not in (None, "") else None
        except ValueError:
            lat, lon = None, None
        if lat is None or lon is None:
            continue
        st["lat"] = lat
        st["lon"] = lon
        normalized.append(st)
    LOGGER.info("NL catalog collected %s stations", len(normalized))
    return normalized


def _refresh_pl_catalog() -> List[Dict[str, Any]]:
    """Fetch all PL stations from GIOS with pagination."""
    stations: List[Dict[str, Any]] = []
    page = 0
    last_page = 1
    size = 200
    while page < last_page:
        try:
            resp = requests.get(
                f"{settings.pl_base_url}/station/findAll",
                params={"page": page, "size": size},
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
            items = (
                data.get("Lista stacji pomiarowych")
                or data.get("stations")
                or data
            )
            if not isinstance(items, list):
                break
            for entry in items:
                sid = entry.get("Identyfikator stacji") or entry.get("id") or entry.get("stationId")
                if not sid:
                    continue
                station_type = entry.get("Typ stacji") or entry.get("typ stacji") or entry.get("stationType")
                if settings.pl_only_auto:
                    stype = str(station_type or "").lower()
                    if stype and "automat" not in stype:
                        continue
                lat_key = next((k for k in entry.keys() if "WGS84" in k and "N" in k), None)
                lon_key = next((k for k in entry.keys() if "WGS84" in k and "E" in k), None)
                lat_val = entry.get("gegrLat") or (entry.get(lat_key) if lat_key else None)
                lon_val = entry.get("gegrLon") or (entry.get(lon_key) if lon_key else None)
                try:
                    lat_val = float(str(lat_val)) if lat_val not in (None, "") else None
                    lon_val = float(str(lon_val)) if lon_val not in (None, "") else None
                except ValueError:
                    lat_val, lon_val = None, None
                if lat_val is None or lon_val is None:
                    continue
                stations.append(
                    {
                        "station_id": str(sid),
                        "source": "gios",
                        "country": "PL",
                        "city": entry.get("Nazwa miasta"),
                        "location_name": entry.get("Nazwa stacji"),
                        "lat": lat_val,
                        "lon": lon_val,
                        "station_type": station_type,
                    }
                )
                time.sleep(0.03)
            last_page = int(
                data.get("totalPages")
                or (data.get("meta") or {}).get("totalPages")
                or last_page
            )
            page += 1
            time.sleep(0.1)
        except requests.RequestException as exc:
            LOGGER.warning("Failed to fetch PL catalog (page %s): %s", page, exc)
            break
    return stations


def _upsert_catalog(stations: List[Dict[str, Any]]) -> None:
    if not stations:
        return
    sql = """
    INSERT INTO stations (station_id, source, country, city, location_name, lat, lon)
    VALUES (%(station_id)s, %(source)s, %(country)s, %(city)s, %(location_name)s, %(lat)s, %(lon)s)
    ON CONFLICT (station_id) DO UPDATE
      SET country = COALESCE(EXCLUDED.country, stations.country),
          city = COALESCE(EXCLUDED.city, stations.city),
          location_name = COALESCE(EXCLUDED.location_name, stations.location_name),
          lat = COALESCE(EXCLUDED.lat, stations.lat),
          lon = COALESCE(EXCLUDED.lon, stations.lon);
    """
    with _get_db_conn() as conn, conn.cursor() as cur:
        cur.executemany(sql, stations)
    LOGGER.info("Catalog upserted %s stations", len(stations))


def _get_de_station_meta(station: str) -> Dict[str, Any]:
    """Fetch minimal station metadata (city/coords) for DE station (UBA API)."""
    if station in _de_station_cache:
        return _de_station_cache[station]
    try:
        resp = requests.get(
            f"{settings.de_base_url}/stations/json",
            params={"station": station},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json().get("data") or {}
        entry = None
        if isinstance(data, dict):
            entry = data.get(str(station))
            if entry is None:
                # Sometimes catalog rows are lists; match by first element if dict lookup fails
                for row in data.values():
                    if isinstance(row, list) and row and str(row[0]) == str(station):
                        entry = row
                        break
        elif isinstance(data, list):
            entry = next(
                (row for row in data if str(row.get("station_id")) == str(station)), {}
            )
        if isinstance(entry, list):
            meta = {
                "city": entry[3] if len(entry) > 3 else None,
                "lat": entry[8] if len(entry) > 8 else None,
                "lon": entry[7] if len(entry) > 7 else None,
                "country": "DE",
            }
        else:
            if not isinstance(entry, dict):
                entry = {}
            meta = {
                "city": entry.get("city") or entry.get("ort") or entry.get("name"),
                "lat": entry.get("latitude") or entry.get("geoBreite") or entry.get("lat"),
                "lon": entry.get("longitude") or entry.get("geoLaenge") or entry.get("lon"),
                "country": "DE",
            }
        _de_station_cache[station] = meta
        return meta
    except requests.RequestException:
        return {"city": None, "country": "DE", "lat": None, "lon": None}


def _fetch_de_latest() -> List[Dict[str, Any]]:
    """Fetch latest measures from UBA (DE) Air Data API for configured stations/components."""
# For each station/component fetch hourly values for the day
    now = datetime.now(timezone.utc)
    date_from = now.date().isoformat()
    date_to = date_from
    results: List[Dict[str, Any]] = []
    stations = _get_stations_from_db("de") or [{"station_id": s} for s in settings.de_stations]
    # Always pull main components: 1=PM10, 2=PM25, 5=NO2, 7=O3
    components = ["1", "2", "5", "7"]
    de_429 = 0
    total_stations = len(stations)
    for idx, st in enumerate(stations, 1):
        if idx == 1 or idx % 100 == 0 or idx == total_stations:
            LOGGER.info("DE fetch progress: %s/%s stations", idx, total_stations)
        station = st.get("station_id")
        for component in components:
            try:
                resp = requests.get(
                    f"{settings.de_base_url}/measures/json",
                    params={
                        "station": station,
                        "component": component,
                        "date_from": date_from,
                        "date_to": date_to,
                        "time_from": 1,
                        "time_to": 24,
                        "lang": "en",
                    },
                    timeout=15,
                )
                resp.raise_for_status()
            except requests.RequestException as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status == 429:
                    de_429 += 1
                LOGGER.warning(
                    "Failed to fetch UBA data for station %s component %s: %s",
                    station,
                    component,
                    exc,
                )
                continue

            data = resp.json().get("data", {})
            station_data = data.get(str(station)) or {}
            for ts_str, payload in station_data.items():
                # payload format: [component_id, scope, value, next_ts, valid_flag]
                if not isinstance(payload, list) or len(payload) < 3:
                    continue
                value = payload[2]
                if value is None:
                    continue
                try:
                    ts_parsed = datetime.fromisoformat(ts_str).replace(
                        tzinfo=timezone.utc
                    )
                except ValueError:
                    ts_parsed = now
                comp_map = {
                    "1": ("pm10", "ug/m3"),
                    "2": ("pm25", "ug/m3"),
                    "5": ("no2", "ug/m3"),
                    "7": ("o3", "ug/m3"),
                }
                pollutant, unit = comp_map.get(str(component), (component, "ug/m3"))
                meta = st or _get_de_station_meta(str(station))
                results.append(
                    {
                        "station_id": station,
                        "pollutant": pollutant,
                        "value": value,
                        "unit": "ug/m3",
                        "country": meta.get("country") or "DE",
                        "city": meta.get("city"),
                        "location_name": meta.get("location_name") or meta.get("city"),
                        "lat": meta.get("lat"),
                        "lon": meta.get("lon"),
                        "timestamp": ts_parsed.isoformat(),
                        "source": "de",
                    }
                )
    LOGGER.info("DE fetch completed: %s records", len(results))
    if de_429:
        LOGGER.warning("DE fetch encountered %s HTTP 429 responses", de_429)
    return results


def _get_nl_station_meta(
    station: str,
    request_counter: Dict[str, int] | None = None,
) -> Dict[str, Any]:
    """Fetch municipality for Luchtmeetnet station."""
    if station in _nl_station_cache:
        return _nl_station_cache[station]
    try:
        # Direct /stations/{station_number} gives coordinates and description
        url = f"{settings.nl_base_url}/stations/{station}"
        resp = requests.get(
            url,
            timeout=10,
        )
        resp.raise_for_status()
        if request_counter is not None:
            _log_nl_request(
                request_counter,
                "station meta",
                url,
            )
        entry = resp.json().get("data") or {}
        coords = entry.get("geometry", {}).get("coordinates") if isinstance(entry, dict) else None
        meta = {
            "city": entry.get("municipality") or entry.get("locality"),
            "location_name": entry.get("location") or station,
            "lat": coords[1] if coords else entry.get("lat"),
            "lon": coords[0] if coords else entry.get("lon"),
            "country": "NL",
        }
        _nl_station_cache[station] = meta
        return meta
    except requests.RequestException:
        return {"city": None, "location_name": station, "country": "NL", "lat": None, "lon": None}


def _fetch_lucht_latest(request_counter: Dict[str, int]) -> List[Dict[str, Any]]:
    """Fetch latest measures from Luchtmeetnet (Netherlands)."""
    # Take last 6 hours for configured stations/components
    now = datetime.now(timezone.utc)
    start = (now - timedelta(hours=6)).isoformat()
    end = now.isoformat()
    results: List[Dict[str, Any]] = []
    stations = _get_stations_from_db("luchtmeetnet") or [
        {"station_id": s} for s in NL_DEFAULT_STATIONS
    ]
    components = [c.strip().lower() for c in NL_DEFAULT_COMPONENTS]
    # current_sleep используется только как backoff при 429
    current_sleep = 1.0
    for st in stations:
        station = st.get("station_id")
        if not station:
            continue
        page = 1
        last_page = 1
        while page <= last_page:
            try:
                url = f"{settings.nl_base_url}/measurements"
                params = {
                    "station_number": station,
                    "start": start,
                    "end": end,
                    "page": page,
                }
                resp = requests.get(
                    url,
                    params=params,
                    timeout=15,
                )
                resp.raise_for_status()
                count = _log_nl_request(
                    request_counter,
                    "measurements",
                    url,
                    params=params,
                )
                if count >= NL_REQUEST_LIMIT:
                    LOGGER.warning(
                        "NL request budget reached during measurements (count=%s)",
                        count,
                    )
                    return results
            except requests.RequestException as exc:
                LOGGER.warning(
                    "Failed to fetch NL data for station %s page %s: %s",
                    station,
                    page,
                    exc,
                )
                status = getattr(exc.response, "status_code", None)
                if status == 429:
                    request_counter["http_429"] = request_counter.get("http_429", 0) + 1
                    current_sleep = min(current_sleep + 0.5, 5.0)
                    time.sleep(current_sleep)
                break
            body = resp.json()
            data = body.get("data") or []
            pag = body.get("pagination") or {}
            try:
                last_page = int(pag.get("last_page") or last_page or 1)
            except Exception:
                last_page = last_page
            for entry in data:
                ts = entry.get("timestamp_measured")
                try:
                    ts_parsed = datetime.fromisoformat(ts)
                except Exception:
                    ts_parsed = now
                formula = (entry.get("formula") or "").lower()
                if formula not in components:
                    continue
                meta = st or _get_nl_station_meta(station, request_counter)
                results.append(
                    {
                        "station_id": station,
                        "pollutant": formula,
                        "value": entry.get("value"),
                        "unit": "ug/m3",
                        "country": meta.get("country") or "NL",
                        "city": meta.get("city"),
                        "location_name": meta.get("location_name") or meta.get("city"),
                        "lat": meta.get("lat"),
                        "lon": meta.get("lon"),
                        "timestamp": ts_parsed.isoformat(),
                        "source": "luchtmeetnet",
                    }
                )
            if last_page <= page:
                break
            page += 1
    return results


def _ensure_pl_station_cache() -> None:
    """Fetch PL stations metadata (lat/lon/name) once and cache."""
    if _pl_station_cache:
        return
    try:
        resp = requests.get(f"{settings.pl_base_url}/station/findAll", timeout=15)
        resp.raise_for_status()
        data = resp.json()
        stations = (
            data.get("Lista stacji pomiarowych")
            or data.get("Lista stacji pomiarowych: ")
            or data.get("stations")
            or data
        )
        if not isinstance(stations, list):
            return
        for entry in stations:
            sid = entry.get("Identyfikator stacji") or entry.get("id") or entry.get("stationId")
            if not sid:
                continue
            lat_key = next((k for k in entry.keys() if "WGS84" in k and "N" in k), None)
            lon_key = next((k for k in entry.keys() if "WGS84" in k and "E" in k), None)
            _pl_station_cache[str(sid)] = {
                "location_name": entry.get("Nazwa stacji")
                or entry.get("stationName")
                or entry.get("stationName"),
                "lat": entry.get("gegrLat")
                or (entry.get(lat_key) if lat_key else None)
                or entry.get("latitude")
                or entry.get("lat"),
                "lon": entry.get("gegrLon")
                or (entry.get(lon_key) if lon_key else None)
                or entry.get("longitude")
                or entry.get("lon"),
                "country": "PL",
            }
    except requests.RequestException as exc:
        LOGGER.warning("Failed to fetch PL station metadata: %s", exc)
        time.sleep(1)


def _fetch_gios_latest() -> List[Dict[str, Any]]:  # type: ignore[redefined-outer-name]
    """Fetch latest measures from GIOS (Poland) for all stations in catalog."""

    def _fetch_sensors_for_station(station_id: str) -> List[Dict[str, str]]:
        sensors_all: List[Dict[str, Any]] = []
        page = 0
        last_page = 0
        while page <= last_page:
            try:
                resp = requests.get(
                    f"{settings.pl_base_url}/station/sensors/{station_id}",
                    params={"page": page, "size": 50},
                    headers={
                        # Максимально «браузерный» заголовок, без строгого Accept, чтобы избежать 406
                        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) airquality-dev/1.0",
                    },
                    allow_redirects=True,
                    timeout=15,
                )
                resp.raise_for_status()
                raw = resp.json() or []
                sensors_page: List[Dict[str, Any]] = []
                if isinstance(raw, list):
                    sensors_page = raw
                elif isinstance(raw, dict):
                    candidates = [
                        v
                        for k, v in raw.items()
                        if isinstance(v, list)
                        and (
                            "lista" in k.lower()
                            or "sensors" in k.lower()
                            or "stanow" in k.lower()
                        )
                    ]
                    if candidates:
                        sensors_page = candidates[0]
                    else:
                        sensors_page = raw.get("data") or raw.get("sensors") or []
                    # totalPages может лежать в meta или корне
                    meta = raw.get("meta") or {}
                    if isinstance(meta, dict) and "totalPages" in meta:
                        try:
                            last_page = int(meta.get("totalPages") or last_page)
                        except Exception:
                            last_page = last_page
                    elif "totalPages" in raw:
                        try:
                            last_page = int(raw.get("totalPages") or last_page)
                        except Exception:
                            last_page = last_page
                sensors_all.extend(sensors_page if isinstance(sensors_page, list) else [])
            except requests.RequestException as exc:
                LOGGER.warning(
                    "Failed to fetch sensors for PL station %s page %s: %s",
                    station_id,
                    page,
                    exc,
                )
                break
            page += 1

        parsed: List[Dict[str, str]] = []
        for s in sensors_all:
            if not isinstance(s, dict):
                continue
            sid = (
                s.get("id")
                or s.get("sensorId")
                or s.get("Identyfikator stanowiska")
                or s.get("Id_stanowiska")
            )
            param = s.get("param") or {}
            code = (
                param.get("paramCode")
                or param.get("code")
                or s.get("Wskaźnik - kod")
                or s.get("Wskaźnik - wzór")
                or s.get("Wskaźnik")
                or ""
            )
            code = str(code).strip().lower()
            # нормализация кодов
            if code in {"pm2.5", "pm2,5"}:
                code = "pm25"
            if code == "pm10":
                code = "pm10"
            allowed = {"pm10", "pm25", "no2", "nox", "no", "o3", "co", "so2"}
            if code not in allowed:
                continue
            if not sid or not code:
                continue
            parsed.append({"sensor_id": sid, "pollutant": code})
        if not parsed:
            snippet = ""
            if sensors_all:
                snippet = str(sensors_all)[:200]
            LOGGER.warning(
                "PL sensors empty for station %s; raw count=%s snippet=%s",
                station_id,
                len(sensors_all),
                snippet,
            )
        return parsed

    now = datetime.now(timezone.utc)
    results: List[Dict[str, Any]] = []
    pl_429 = 0
    if settings.pl_only_auto:
        # Для корректности берём свежий каталог только автоматических станций
        stations = _refresh_pl_catalog()
    else:
        stations = _get_stations_from_db("gios") or []
        if not stations:
            _ensure_pl_station_cache()
            for sid, meta in _pl_station_cache.items():
                meta = dict(meta)
                meta["station_id"] = sid
                stations.append(meta)

    total_stations = len(stations)
    if total_stations:
        LOGGER.info("PL fetch start: %s stations", total_stations)

    for idx, st in enumerate(stations, 1):
        if idx == 1 or idx % 50 == 0 or idx == total_stations:
            LOGGER.info("PL fetch progress: %s/%s stations", idx, total_stations)
        station_id = st.get("station_id")
        if not station_id:
            continue
        station_meta = st
        sensors = _fetch_sensors_for_station(station_id)
        if not sensors:
            LOGGER.warning(
                "PL station %s has no sensors (maybe empty or 406/redirect). Check %s/station/sensors/%s",
                station_id,
                settings.pl_base_url,
                station_id,
            )
        time.sleep(0.05)
        for s in sensors:
            sensor_id = s["sensor_id"]
            pollutant = s["pollutant"]
            try:
                resp = requests.get(
                    f"{settings.pl_base_url}/data/getData/{sensor_id}", timeout=15
                )
                resp.raise_for_status()
            except requests.RequestException as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status == 429:
                    pl_429 += 1
                LOGGER.warning("Failed to fetch GIOS data for sensor %s: %s", sensor_id, exc)
                continue
            data = (
                resp.json().get("values")
                or resp.json().get("Lista danych pomiarowych")
                or []
            )
            for entry in data:
                ts = entry.get("date") or entry.get("Data")
                value = (
                    entry.get("value")
                    or entry.get("Wartosc")
                    or entry.get("Wartość")
                )
                if value is None:
                    continue
                try:
                    ts_parsed = datetime.fromisoformat(ts.replace(" ", "T"))
                except Exception:
                    ts_parsed = now
                results.append(
                    {
                        "station_id": station_id,
                        "pollutant": pollutant,
                        "value": value,
                        "unit": "ug/m3",
                        "country": station_meta.get("country") or "PL",
                        "city": station_meta.get("city"),
                        "location_name": station_meta.get("location_name"),
                        "lat": station_meta.get("lat"),
                        "lon": station_meta.get("lon"),
                        "timestamp": ts_parsed.isoformat(),
                        "source": "gios",
                    }
                )
            time.sleep(0.05)
    LOGGER.info("PL fetch completed: %s records", len(results))
    if pl_429:
        LOGGER.warning("PL fetch encountered %s HTTP 429 responses", pl_429)
    return results


def _emit_batch(producer: KafkaProducer | None, payload: List[Dict[str, Any]]) -> None:
    # Send batch to Kafka; if producer is missing, log to stdout for debugging
    if not payload:
        LOGGER.info("No payload to emit for this batch")
        return

    if producer is None:
        LOGGER.info(
            "Kafka producer not initialised; printing payload to stdout for debugging"
        )
        LOGGER.info(json.dumps(payload, indent=2))
        return

    futures = []
    for record in payload:
        futures.append(producer.send(settings.kafka_topic, record))
    try:
        for future in futures:
            future.get(timeout=10)
        LOGGER.info("Emitted %s records to %s", len(payload), settings.kafka_topic)
    except KafkaError as exc:
        LOGGER.error("Failed to publish records: %s", exc)


def _prepare_batch(payload: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Deduplicate and log batch composition before emitting.

    Deduplicates by (station_id, pollutant, timestamp) and logs breakdown by source.
    """
    if not payload:
        LOGGER.info("Empty payload before prepare_batch")
        return []

    deduped: Dict[tuple, Dict[str, Any]] = {}
    for record in payload:
        key = (
            record.get("station_id"),
            record.get("pollutant"),
            record.get("timestamp"),
        )
        if key not in deduped:
            deduped[key] = record
    prepared = list(deduped.values())

    counts: Dict[str, int] = {}
    for rec in prepared:
        src = rec.get("source") or "unknown"
        counts[src] = counts.get(src, 0) + 1
    LOGGER.info("Batch size %s, by source: %s", len(prepared), counts)
    return prepared


def run() -> None:
    # Main loop: collect -> dedupe -> emit with per-source logging
    LOGGER.info("Starting ingestion loop. Live API: %s", settings.use_live_api)
    catalog: List[Dict[str, Any]] = []
    request_counter: Dict[str, int] = {"count": 0}
    # Refresh catalogs per-source so падение одного не ломает остальные
    if not settings.disable_de_fetch:
        try:
            catalog.extend(_refresh_de_catalog())
        except Exception as exc:
            LOGGER.warning("DE catalog refresh failed: %s", exc)
    else:
        LOGGER.info("DE catalog refresh skipped (PIPELINE_DISABLE_DE=true)")
    if not settings.disable_nl_fetch:
        try:
            catalog.extend(_refresh_nl_catalog(request_counter))
        except Exception as exc:
            LOGGER.warning("NL catalog refresh failed: %s", exc)
    else:
        LOGGER.info("NL catalog refresh skipped (PIPELINE_DISABLE_NL=true)")
    if not settings.disable_pl_fetch:
        try:
            catalog.extend(_refresh_pl_catalog())
        except Exception as exc:
            LOGGER.warning("PL catalog refresh failed: %s", exc)
    else:
        LOGGER.info("PL catalog refresh skipped (PIPELINE_DISABLE_PL=true)")
    if catalog:
        try:
            _upsert_catalog(catalog)
        except Exception:
            LOGGER.exception(
                "Failed to upsert station catalog; continuing without DB write"
            )
    else:
        LOGGER.warning("Catalog refresh produced 0 stations")

    producer = _build_producer()
    while True:
        cycle_started = time.monotonic()
        pending_cursor_updates: Dict[str, Dict[Tuple[str, str], datetime]] = {}
        if producer is None:
            producer = _build_producer()
            if producer is None:
                LOGGER.info("Kafka producer unavailable; will retry after sleep")

        payload: List[Dict[str, Any]] = []
        if settings.use_live_api:
            de_data: List[Dict[str, Any]] = []
            nl_data: List[Dict[str, Any]] = []
            pl_data: List[Dict[str, Any]] = []
            request_counter["count"] = 0  # reset per live cycle
            request_counter["http_429"] = 0
            if settings.disable_de_fetch:
                LOGGER.info("DE fetch skipped (PIPELINE_DISABLE_DE=true)")
            else:
                try:
                    LOGGER.info("Fetching DE measurements...")
                    de_data = _fetch_de_latest()
                    before = len(de_data)
                    de_data, cursor_updates = _filter_new_measurements("de", de_data)
                    LOGGER.info(
                        "Filtered DE measurements %s -> %s (dedup)",
                        before,
                        len(de_data),
                    )
                    if cursor_updates:
                        pending_cursor_updates["de"] = cursor_updates
                except Exception:
                    LOGGER.exception("DE fetch failed; skipping DE this cycle")
            if settings.disable_nl_fetch:
                LOGGER.info("NL fetch skipped (PIPELINE_DISABLE_NL=true)")
            else:
                try:
                    LOGGER.info("Fetching NL measurements...")
                    nl_data = _fetch_lucht_latest(request_counter)
                    before = len(nl_data)
                    nl_data, cursor_updates = _filter_new_measurements(
                        "luchtmeetnet", nl_data
                    )
                    LOGGER.info(
                        "Filtered NL measurements %s -> %s (dedup)",
                        before,
                        len(nl_data),
                    )
                    if cursor_updates:
                        pending_cursor_updates["luchtmeetnet"] = cursor_updates
                except Exception:
                    LOGGER.exception("NL fetch failed; skipping NL this cycle")
            if settings.disable_pl_fetch:
                LOGGER.info("PL fetch skipped (PIPELINE_DISABLE_PL=true)")
            else:
                try:
                    LOGGER.info("Fetching PL measurements...")
                    pl_data = _fetch_gios_latest()
                    before = len(pl_data)
                    pl_data, cursor_updates = _filter_new_measurements("gios", pl_data)
                    LOGGER.info(
                        "Filtered PL measurements %s -> %s (dedup)",
                        before,
                        len(pl_data),
                    )
                    if cursor_updates:
                        pending_cursor_updates["gios"] = cursor_updates
                except Exception:
                    LOGGER.exception("PL fetch failed; skipping PL this cycle")

            LOGGER.info(
                "Fetched live data counts -> DE: %s, NL: %s, PL: %s",
                len(de_data),
                len(nl_data),
                len(pl_data),
            )
            nl_429 = request_counter.get("http_429", 0)
            if nl_429:
                LOGGER.warning("NL fetch encountered %s HTTP 429 responses this cycle", nl_429)
            payload.extend(de_data)
            payload.extend(nl_data)
            payload.extend(pl_data)
            if not payload:
                LOGGER.warning("Live API returned no data; skip emit this cycle")
                time.sleep(settings.batch_sleep_seconds)
                continue
        else:
            # Live API disabled => synthetic demo data
            payload = _create_synthetic_measurements()

        payload = _prepare_batch(payload)

        try:
            _emit_batch(producer, payload)
        except Exception:
            LOGGER.exception("Failed to emit batch")
        else:
            _commit_cursor_updates(pending_cursor_updates)
        cycle_duration = time.monotonic() - cycle_started
        LOGGER.info(
            "Ingestion cycle completed in %.2f seconds (sleep %.0f seconds)",
            cycle_duration,
            settings.batch_sleep_seconds,
        )
        time.sleep(settings.batch_sleep_seconds)


if __name__ == "__main__":
    run()
