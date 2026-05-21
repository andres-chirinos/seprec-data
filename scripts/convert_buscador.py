"""
convert_buscador.py
===================
Convierte los JSONs crudos del buscador SEPREC a formato Parquet normalizado.

Modos de uso:
  1. Desde carpeta de JSONs ya descargados (FROM_JSON):
       python convert_buscador.py --from-json --input-dir data/seprec_data/data --output-dir output/parquet

  2. Descarga en vivo + conversión (HTTP live):
       python convert_buscador.py --live --output-dir output/parquet

Salidas:
  - empresas_buscador.parquet       : Tabla principal aplanada (una fila por empresa)
  - buscador_departamentos.parquet  : Tabla normalizada de departamentos (referencia a empresas_buscador)

Estructura JSON de entrada (result_{letra}_{pagina}.json):
  {
    "finalizado": true,
    "datos": {
      "total": N,
      "filas": [
        {
          "estado": "ACTIVO",
          "id": "173018",
          "matricula": "...",
          "codEstadoActualizacion": {"id": 17, "codigo": "1", "nombre": "..."},
          "codTipoUnidadEconomica": {"id": 18, "codigo": "01", "nombre": "..."},
          "direccion": {"id": "...", "codDepartamento": {"id": 421, "codigo": "03", "nombre": "..."}}
        },
        ...
      ]
    }
  }
"""

from __future__ import annotations

import argparse
import glob
import json
import math
import os
import random
import re
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Constantes del API
# ---------------------------------------------------------------------------
BASE_URL = "https://servicios.seprec.gob.bo/api/empresas/buscarEmpresas"
PALABRAS_CLAVE = ["a", "e", "i", "o", "u"]
LIMIT = 10
START_PAGE = 1
REQUEST_DELAY_RANGE = (1, 3)

# ---------------------------------------------------------------------------
# Helpers de aplanado
# ---------------------------------------------------------------------------

def _flatten_scalar(value: Any) -> Any:
    """Aplana recursivamente un valor. Las listas se serializan a JSON string."""
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


def flatten_buscador_record(record: dict, palabra: str, page: int, row_index: int) -> dict:
    """
    Aplana un registro de la lista 'filas' del buscador.
    Añade metadatos de paginación para trazabilidad.
    """
    flat: dict[str, Any] = {
        "_palabra": palabra,
        "_pagina": page,
        "_indice_fila": row_index,
    }
    for key, value in record.items():
        fv = _flatten_scalar(value)
        if isinstance(fv, dict):
            for nk, nv in fv.items():
                flat[f"{key}_{nk}"] = nv
        else:
            flat[key] = fv
    return flat


# ---------------------------------------------------------------------------
# Extracción de tablas normalizadas desde los registros planos
# ---------------------------------------------------------------------------

def extract_departamentos(df_main: pd.DataFrame) -> pd.DataFrame:
    """
    Extrae la tabla normalizada de departamentos desde el DataFrame principal.
    Columnas: empresa_id, dep_id, dep_codigo, dep_nombre
    """
    cols_map = {
        "id": "empresa_id",
        "direccion_codDepartamento_id": "dep_id",
        "direccion_codDepartamento_codigo": "dep_codigo",
        "direccion_codDepartamento_nombre": "dep_nombre",
    }
    available = {src: dst for src, dst in cols_map.items() if src in df_main.columns}
    if not available:
        return pd.DataFrame()

    df_dep = df_main[list(available.keys())].rename(columns=available).drop_duplicates()
    return df_dep


# ---------------------------------------------------------------------------
# Procesamiento desde JSONs ya descargados (FROM_JSON)
# ---------------------------------------------------------------------------

def load_jsons_buscador(input_dir: str | Path, palabras_clave: list[str], limit: int | None = None) -> list[dict]:
    """
    Lee todos los archivos result_{letra}_{pagina}.json del directorio dado.
    Retorna lista de registros aplanados (máx. `limit` si se especifica).
    """
    input_dir = Path(input_dir)
    all_records: list[dict] = []
    stats: dict[str, dict] = {}

    for palabra in palabras_clave:
        pattern = str(input_dir / f"result_{palabra}_*.json")
        matched = glob.glob(pattern)

        def _page_num(path: str) -> int:
            m = re.search(r"_(\d+)\.json$", os.path.basename(path))
            return int(m.group(1)) if m else 0

        matched = sorted(matched, key=_page_num)
        stats[palabra] = {"archivos": len(matched), "registros": 0, "errores": 0}

        _log("INFO", "FROM_JSON_START", {"palabra": palabra, "archivos": len(matched)})

        for file_path in matched:
            page = _page_num(file_path)
            try:
                with open(file_path, encoding="utf-8") as fh:
                    data = json.load(fh)
            except Exception as e:
                stats[palabra]["errores"] += 1
                _log("ERROR", "ERROR_LECTURA", {"archivo": file_path, "error": str(e)})
                continue

            filas = data.get("datos", {}).get("filas", [])
            if not isinstance(filas, list):
                filas = []

            for row_index, record in enumerate(filas, start=1):
                flat = flatten_buscador_record(record, palabra, page, row_index)
                all_records.append(flat)
                stats[palabra]["registros"] += 1
                if limit is not None and len(all_records) >= limit:
                    _log("INFO", "LIMIT_REACHED", {"limit": limit, "palabra": palabra})
                    break
            if limit is not None and len(all_records) >= limit:
                break

        _log("INFO", "FROM_JSON_DONE", {
            "palabra": palabra,
            "archivos_leidos": stats[palabra]["archivos"],
            "registros": stats[palabra]["registros"],
        })

    return all_records


# ---------------------------------------------------------------------------
# Procesamiento en vivo desde HTTP
# ---------------------------------------------------------------------------

def fetch_page(palabra: str, page: int, params_base: dict, raw_dir: Path | None = None) -> list[dict]:
    """Descarga una página del API y retorna filas aplanadas."""
    params = {**params_base, "filtro": palabra, "pagina": page}
    response = requests.get(BASE_URL, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    if raw_dir is not None:
        raw_dir.mkdir(parents=True, exist_ok=True)
        out_path = raw_dir / f"result_{palabra}_{page}.json"
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=4)

    filas = data.get("datos", {}).get("filas", [])
    total = data.get("datos", {}).get("total", None)
    if not isinstance(filas, list):
        filas = []

    records = [
        flatten_buscador_record(record, palabra, page, i)
        for i, record in enumerate(filas, start=1)
    ]
    return records, total


def download_buscador_live(palabras_clave: list[str], raw_dir: Path | None = None, limit: int | None = None) -> list[dict]:
    """
    Descarga en vivo todos los resultados del buscador SEPREC.
    Guarda JSONs crudos en raw_dir si se especifica.
    Si limit está definido, para cuando se alcanzan N registros totales.
    """
    params_base = {"limite": LIMIT, "pagina": START_PAGE}
    all_records: list[dict] = []

    for palabra in palabras_clave:
        page = START_PAGE
        total = None
        maxpages = 1
        consulted = 0
        failed = 0

        _log("INFO", "LIVE_START", {"palabra": palabra})

        while page <= maxpages:
            try:
                records, total_resp = fetch_page(palabra, page, params_base, raw_dir)
                if total is None and total_resp is not None:
                    total = int(total_resp)
                    maxpages = max(1, math.ceil(total / LIMIT))
                all_records.extend(records)
                consulted += 1
                _log("INFO", "LIVE_PAGE_OK", {
                    "palabra": palabra, "page": page, "maxpages": maxpages,
                    "registros_pagina": len(records), "total": total,
                })
                if limit is not None and len(all_records) >= limit:
                    _log("INFO", "LIMIT_REACHED", {"limit": limit, "total_acumulado": len(all_records)})
                    break
            except Exception as e:
                failed += 1
                _log("ERROR", "LIVE_PAGE_ERROR", {"palabra": palabra, "page": page, "error": str(e)})

            time.sleep(random.uniform(*REQUEST_DELAY_RANGE))
            page += 1

        _log("INFO", "LIVE_DONE", {
            "palabra": palabra, "consulted": consulted,
            "failed": failed, "total": total,
        })
        if limit is not None and len(all_records) >= limit:
            break

    return all_records[:limit] if limit is not None else all_records


# ---------------------------------------------------------------------------
# Construcción y guardado de Parquets
# ---------------------------------------------------------------------------

def build_and_save_parquets(
    records: list[dict],
    output_dir: str | Path,
    drop_meta_cols: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Construye el DataFrame principal y tablas normalizadas, los guarda como Parquet.

    Retorna (df_main, df_departamentos).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not records:
        _log("WARN", "NO_RECORDS", {"msg": "No hay registros para consolidar."})
        return pd.DataFrame(), pd.DataFrame()

    df = pd.DataFrame(records)

    # Eliminar duplicados por id (empresa)
    if "id" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["id"])
        _log("INFO", "DEDUP", {"antes": before, "despues": len(df), "duplicados": before - len(df)})

    # Tabla normalizada de departamentos
    df_dep = extract_departamentos(df)

    # Eliminar columnas de metadatos de paginación del parquet principal
    if drop_meta_cols:
        meta_cols = ["_palabra", "_pagina", "_indice_fila"]
        df = df.drop(columns=[c for c in meta_cols if c in df.columns])

    # Guardar
    main_path = output_dir / "empresas_buscador.parquet"
    df.to_parquet(main_path, index=False, engine="pyarrow")
    _log("INFO", "PARQUET_SAVED", {"path": str(main_path), "filas": len(df), "columnas": len(df.columns)})

    if not df_dep.empty:
        dep_path = output_dir / "buscador_departamentos.parquet"
        df_dep.to_parquet(dep_path, index=False, engine="pyarrow")
        _log("INFO", "PARQUET_SAVED", {"path": str(dep_path), "filas": len(df_dep)})

    return df, df_dep


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
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convierte JSONs del buscador SEPREC a Parquet normalizado."
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
        default=Path("data/seprec_data/data"),
        help="Carpeta con los JSONs crudos (solo para --from-json). Default: data/seprec_data/data",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output/parquet"),
        help="Carpeta de salida para los Parquets. Default: output/parquet",
    )
    parser.add_argument(
        "--save-raw",
        type=Path,
        default=None,
        help="(Solo --live) Guarda los JSONs crudos descargados en esta carpeta.",
    )
    parser.add_argument(
        "--palabras",
        nargs="+",
        default=PALABRAS_CLAVE,
        help=f"Palabras clave a buscar. Default: {PALABRAS_CLAVE}",
    )
    parser.add_argument(
        "--keep-meta",
        action="store_true",
        help="Conserva columnas _palabra, _pagina, _indice_fila en el Parquet principal.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Limita la extracción a los primeros N registros (útil para pruebas).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    _log("INFO", "START", {
        "mode": "from_json" if args.from_json else "live",
        "palabras": args.palabras,
        "output_dir": str(args.output_dir),
    })

    if args.from_json:
        records = load_jsons_buscador(args.input_dir, args.palabras, limit=args.limit)
    else:
        records = download_buscador_live(args.palabras, raw_dir=args.save_raw, limit=args.limit)

    _log("INFO", "TOTAL_RECORDS", {"total": len(records)})

    df_main, df_dep = build_and_save_parquets(
        records,
        args.output_dir,
        drop_meta_cols=not args.keep_meta,
    )

    _log("INFO", "DONE", {
        "empresas_buscador": len(df_main),
        "departamentos": len(df_dep),
    })


if __name__ == "__main__":
    main()
