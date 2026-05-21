"""
convert_detalles.py
===================
Convierte los JSONs crudos de detalle de empresas SEPREC a Parquets normalizados.

Modos de uso:
  1. Desde carpeta de JSONs ya descargados (FROM_JSON):
       python convert_detalles.py --from-json --input-dir data/seprec-detail/data --output-dir output/parquet

  2. Descarga en vivo + conversión (HTTP live), leyendo IDs desde el Parquet del buscador:
       python convert_detalles.py --live --buscador-parquet output/parquet/empresas_buscador.parquet --output-dir output/parquet

Salidas:
  - empresas_detalle.parquet       : Tabla principal con datos de empresa aplanados
  - detalle_objetos_sociales.parquet : Tabla normalizada (empresa_id, objetoSocial)
  - detalle_contactos.parquet      : Tabla normalizada de contactos (tipo, estado)
  - detalle_telefonos.parquet      : Tabla normalizada de teléfonos
  - detalle_emails.parquet         : Tabla normalizada de correos
  - detalle_actividades.parquet    : Tabla normalizada de actividades (si aplica)

Estructura JSON de entrada (result_{empresa}_{establecimiento}.json):
  {
    "finalizado": true,
    "datos": {
      "estado": "ACTIVO",
      "id": "100001",
      "nit": "...",
      "razonSocial": "...",
      "codTipoUnidadEconomica": {"id": 23, "nombre": "SOCIEDAD ANONIMA"},
      "objetos_sociales": [{"objetoSocial": "..."}],
      "contactos": [
        {
          "id": "91536",
          "tipoContacto": "CORREO",
          "descripcion": [{"tipo": "principal", "correo": "..."}]
        },
        {
          "id": "91535",
          "tipoContacto": "TELEFONO",
          "descripcion": [{"tipo": "Telefono fijo", "numero": "..."}]
        }
      ],
      "direccion": {...},
      "codEstadoActualizacion": {...}
    }
  }
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import random
import re
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Constantes del API
# ---------------------------------------------------------------------------
BASE_URL = (
    "https://servicios.seprec.gob.bo/api/empresas/"
    "informacionBasicaEmpresa/{empresa_id}/establecimiento/{idEstablecimiento}"
)
REQUEST_DELAY_RANGE = (1, 3)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _log(level: str, event: str, extra: dict | None = None) -> None:
    entry = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "level": level,
        "event": event,
    }
    if extra:
        entry.update(extra)
    print(json.dumps(entry, ensure_ascii=False), flush=True)


# ---------------------------------------------------------------------------
# Helpers de aplanado
# ---------------------------------------------------------------------------

def _flatten_scalar(value: Any) -> Any:
    """Aplana recursivamente. Las listas se serializan a JSON string."""
    if isinstance(value, dict):
        flat: dict[str, Any] = {}
        for k, v in value.items():
            fv = _flatten_scalar(v)
            if isinstance(fv, dict):
                for nk, nv in fv.items():
                    flat[f"{k}_{nk}"] = nv
            else:
                flat[k] = fv
        return flat
    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=False)
    return value


# Campos que se extraen como tablas separadas y NO se aplanan en el registro principal
_NORMALIZED_FIELDS = {"objetos_sociales", "contactos", "actividades"}


def flatten_detalle_record(obj: dict, empresa_id: str | None = None, establecimiento_id: str | None = None) -> dict:
    """
    Aplana el registro de detalle de empresa.
    - Extrae 'datos' como raíz si existe.
    - Serializa como JSON string los campos que serán tablas normalizadas.
    - Añade metadatos de empresa_id / establecimiento_id.
    """
    datos = obj.get("datos", obj) if isinstance(obj, dict) else {}
    if not isinstance(datos, dict):
        datos = {"value": datos}

    flat: dict[str, Any] = {}
    if empresa_id is not None:
        flat["empresa_id"] = empresa_id
    if establecimiento_id is not None:
        flat["establecimiento_id"] = establecimiento_id

    for key, value in datos.items():
        if key in _NORMALIZED_FIELDS:
            # Se guarda como JSON string en el parquet principal (referencia)
            flat[key] = json.dumps(value, ensure_ascii=False) if value is not None else None
            continue
        fv = _flatten_scalar(value)
        if isinstance(fv, dict):
            for nk, nv in fv.items():
                flat[f"{key}_{nk}"] = nv
        else:
            flat[key] = fv

    return flat


# ---------------------------------------------------------------------------
# Extracción de tablas normalizadas
# ---------------------------------------------------------------------------

def extract_objetos_sociales(records: list[dict]) -> pd.DataFrame:
    """
    Tabla: (empresa_id, objeto_social)
    Referencia: empresa_id -> empresas_detalle.parquet
    """
    rows = []
    for rec in records:
        empresa_id = rec.get("empresa_id") or rec.get("id")
        raw = rec.get("objetos_sociales")
        if not raw:
            continue
        try:
            items = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            continue
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict):
                rows.append({
                    "empresa_id": empresa_id,
                    "objeto_social": item.get("objetoSocial", ""),
                })
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["empresa_id", "objeto_social"])


def extract_contactos(records: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Extrae tres tablas desde el campo 'contactos':
    - df_contactos     : (empresa_id, contacto_id, tipo_contacto, estado)
    - df_telefonos     : (empresa_id, contacto_id, tipo_telefono, numero)
    - df_emails        : (empresa_id, contacto_id, tipo_email, correo)
    """
    contactos_rows = []
    telefonos_rows = []
    emails_rows = []

    for rec in records:
        empresa_id = rec.get("empresa_id") or rec.get("id")
        raw = rec.get("contactos")
        if not raw:
            continue
        try:
            items = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            continue
        if not isinstance(items, list):
            continue

        for contacto in items:
            if not isinstance(contacto, dict):
                continue
            contacto_id = contacto.get("id")
            tipo = contacto.get("tipoContacto", "")
            estado = contacto.get("estado", "")

            contactos_rows.append({
                "empresa_id": empresa_id,
                "contacto_id": contacto_id,
                "tipo_contacto": tipo,
                "estado": estado,
            })

            descripcion = contacto.get("descripcion", [])
            if not isinstance(descripcion, list):
                continue

            for desc in descripcion:
                if not isinstance(desc, dict):
                    continue
                if tipo == "TELEFONO":
                    telefonos_rows.append({
                        "empresa_id": empresa_id,
                        "contacto_id": contacto_id,
                        "tipo_telefono": desc.get("tipo", ""),
                        "numero": desc.get("numero", ""),
                    })
                elif tipo == "CORREO":
                    emails_rows.append({
                        "empresa_id": empresa_id,
                        "contacto_id": contacto_id,
                        "tipo_email": desc.get("tipo", ""),
                        "correo": desc.get("correo", ""),
                    })

    df_contactos = pd.DataFrame(contactos_rows) if contactos_rows else pd.DataFrame(
        columns=["empresa_id", "contacto_id", "tipo_contacto", "estado"])
    df_telefonos = pd.DataFrame(telefonos_rows) if telefonos_rows else pd.DataFrame(
        columns=["empresa_id", "contacto_id", "tipo_telefono", "numero"])
    df_emails = pd.DataFrame(emails_rows) if emails_rows else pd.DataFrame(
        columns=["empresa_id", "contacto_id", "tipo_email", "correo"])

    return df_contactos, df_telefonos, df_emails


def extract_actividades(records: list[dict]) -> pd.DataFrame:
    """
    Tabla: (empresa_id, actividad_id, descripcion, ...) si el campo 'actividades' existe.
    """
    rows = []
    for rec in records:
        empresa_id = rec.get("empresa_id") or rec.get("id")
        raw = rec.get("actividades")
        if not raw:
            continue
        try:
            items = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            continue
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict):
                row = {"empresa_id": empresa_id}
                row.update({k: v for k, v in item.items() if not isinstance(v, (dict, list))})
                rows.append(row)
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["empresa_id"])


# ---------------------------------------------------------------------------
# Procesamiento desde JSONs ya descargados
# ---------------------------------------------------------------------------

def load_jsons_detalles(input_dir: str | Path, limit: int | None = None) -> list[dict]:
    """
    Lee todos los archivos result_{empresa}_{establecimiento}.json del directorio.
    Retorna lista de registros aplanados con metadatos.
    Si limit está definido, procesa solo los primeros N archivos.
    """
    input_dir = Path(input_dir)
    json_files = sorted(glob.glob(str(input_dir / "result_*_*.json")))
    if limit is not None:
        json_files = json_files[:limit]

    _log("INFO", "FROM_JSON_START", {"archivos_encontrados": len(json_files), "limit": limit})

    records = []
    errors = 0

    for path in json_files:
        stem = Path(path).stem  # result_100001_100001
        parts = stem.split("_")
        empresa_id = parts[1] if len(parts) > 1 else None
        establecimiento_id = parts[2] if len(parts) > 2 else None

        try:
            with open(path, encoding="utf-8") as fh:
                obj = json.load(fh)
        except Exception as e:
            errors += 1
            _log("ERROR", "ERROR_LECTURA", {"archivo": path, "error": str(e)})
            continue

        # Filtrar JSONs que solo tienen "finalizado: false" o errores vacíos
        if isinstance(obj, dict) and not obj.get("finalizado", True):
            _log("WARN", "SKIP_NO_FINALIZADO", {"archivo": path})
            continue

        flat = flatten_detalle_record(obj, empresa_id=empresa_id, establecimiento_id=establecimiento_id)
        records.append(flat)

    _log("INFO", "FROM_JSON_DONE", {
        "archivos_leidos": len(json_files),
        "registros_ok": len(records),
        "errores": errors,
    })
    return records


# ---------------------------------------------------------------------------
# Descarga en vivo desde HTTP
# ---------------------------------------------------------------------------

def fetch_detail(empresa_id: str, establecimiento_id: str, raw_dir: Path | None = None) -> dict | None:
    """Descarga el detalle de una empresa y retorna el JSON parseado."""
    url = BASE_URL.format(empresa_id=empresa_id, idEstablecimiento=establecimiento_id)
    try:
        response = requests.get(url, timeout=60)
        data = response.json()
    except Exception as e:
        _log("ERROR", "LIVE_DETAIL_ERROR", {
            "empresa": empresa_id, "establecimiento": establecimiento_id, "error": str(e)
        })
        return None

    if raw_dir is not None:
        raw_dir.mkdir(parents=True, exist_ok=True)
        out_path = raw_dir / f"result_{empresa_id}_{establecimiento_id}.json"
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=4)

    return data


def download_detalles_live(
    company_pairs: list[tuple[str, str]],
    raw_dir: Path | None = None,
    limit: int | None = None,
) -> list[dict]:
    """
    Descarga en vivo los detalles de las empresas.
    company_pairs: lista de (empresa_id, establecimiento_id)
    Si limit está definido, solo procesa los primeros N pares.
    """
    import concurrent.futures

    if limit is not None:
        company_pairs = company_pairs[:limit]

    total = len(company_pairs)
    records = []
    _log("INFO", "LIVE_START", {"total_empresas": total})

    def _fetch_and_flatten(pair: tuple[str, str]) -> dict | None:
        empresa_id, establecimiento_id = pair
        data = fetch_detail(empresa_id, establecimiento_id, raw_dir)
        time.sleep(random.uniform(*REQUEST_DELAY_RANGE))
        if data is None:
            return None
        return flatten_detalle_record(data, empresa_id=empresa_id, establecimiento_id=establecimiento_id)

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_fetch_and_flatten, pair): pair for pair in company_pairs}
        done = 0
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            done += 1
            if result is not None:
                records.append(result)
            if done % 100 == 0 or done == total:
                _log("INFO", "LIVE_PROGRESS", {
                    "procesados": done, "total": total,
                    "pct": round(100 * done / total, 1),
                })

    _log("INFO", "LIVE_DONE", {"total_descargados": len(records)})
    return records


# ---------------------------------------------------------------------------
# Construcción y guardado de Parquets
# ---------------------------------------------------------------------------

def build_and_save_parquets(records: list[dict], output_dir: str | Path) -> dict[str, pd.DataFrame]:
    """
    Construye y guarda todos los Parquets normalizados.
    Retorna dict con nombre -> DataFrame.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not records:
        _log("WARN", "NO_RECORDS", {"msg": "No hay registros para consolidar."})
        return {}

    df_main = pd.DataFrame(records)

    # Deduplicar por id (empresa)
    if "id" in df_main.columns:
        before = len(df_main)
        df_main = df_main.drop_duplicates(subset=["id"])
        _log("INFO", "DEDUP", {"antes": before, "despues": len(df_main), "duplicados": before - len(df_main)})

    # Extraer tablas normalizadas
    df_obj_sociales = extract_objetos_sociales(records)
    df_contactos, df_telefonos, df_emails = extract_contactos(records)
    df_actividades = extract_actividades(records)

    output_map = {
        "empresas_detalle": df_main,
        "detalle_objetos_sociales": df_obj_sociales,
        "detalle_contactos": df_contactos,
        "detalle_telefonos": df_telefonos,
        "detalle_emails": df_emails,
    }
    if not df_actividades.empty and len(df_actividades.columns) > 1:
        output_map["detalle_actividades"] = df_actividades

    for name, df in output_map.items():
        if df.empty:
            _log("WARN", "EMPTY_TABLE", {"table": name})
            continue
        # Normalizar tipos mixtos: forzar object columns con mezcla int/str a str
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].where(df[col].isna(), df[col].astype(str))
        out_path = output_dir / f"{name}.parquet"
        df.to_parquet(out_path, index=False, engine="pyarrow")
        _log("INFO", "PARQUET_SAVED", {
            "path": str(out_path), "filas": len(df), "columnas": len(df.columns)
        })

    return output_map


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convierte JSONs de detalles SEPREC a Parquets normalizados."
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--from-json",
        action="store_true",
        help="Lee JSONs ya descargados en --input-dir.",
    )
    mode.add_argument(
        "--live",
        action="store_true",
        help="Descarga en vivo desde el API SEPREC.",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/seprec-detail/data"),
        help="Carpeta con los JSONs crudos (solo --from-json). Default: data/seprec-detail/data",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output/parquet"),
        help="Carpeta de salida para los Parquets. Default: output/parquet",
    )
    parser.add_argument(
        "--buscador-parquet",
        type=Path,
        default=None,
        help="(Solo --live) Parquet del buscador para extraer IDs. Default: output/parquet/empresas_buscador.parquet",
    )
    parser.add_argument(
        "--save-raw",
        type=Path,
        default=None,
        help="(Solo --live) Guarda los JSONs crudos descargados en esta carpeta.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Limita la extracción a los primeros N registros/empresas (para pruebas).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    _log("INFO", "START", {
        "mode": "from_json" if args.from_json else "live",
        "output_dir": str(args.output_dir),
    })

    if args.from_json:
        records = load_jsons_detalles(args.input_dir, limit=args.limit)
    else:
        buscador_parquet = args.buscador_parquet or Path("output/parquet/empresas_buscador.parquet")
        if not buscador_parquet.exists():
            _log("ERROR", "MISSING_BUSCADOR", {
                "path": str(buscador_parquet),
                "msg": "Ejecuta primero convert_buscador.py --live o especifica --buscador-parquet."
            })
            raise SystemExit(1)

        df_bus = pd.read_parquet(buscador_parquet)
        if "id" not in df_bus.columns or "idEstablecimiento" not in df_bus.columns:
            _log("ERROR", "MISSING_COLUMNS", {
                "msg": "El Parquet del buscador no contiene columnas 'id' e 'idEstablecimiento'.",
                "columnas": df_bus.columns.tolist(),
            })
            raise SystemExit(1)

        company_pairs = [
            (str(row["id"]), str(row["idEstablecimiento"]))
            for _, row in df_bus[["id", "idEstablecimiento"]].dropna().iterrows()
        ]
        _log("INFO", "PAIRS_LOADED", {"total": len(company_pairs)})
        records = download_detalles_live(company_pairs, raw_dir=args.save_raw, limit=args.limit)

    output_tables = build_and_save_parquets(records, args.output_dir)

    _log("INFO", "DONE", {
        table: len(df) for table, df in output_tables.items()
    })


if __name__ == "__main__":
    main()
