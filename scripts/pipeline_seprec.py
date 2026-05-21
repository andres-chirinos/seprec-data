"""
pipeline_seprec.py
==================
Pipeline unificado SEPREC: descarga + conversión + consolidación en Parquet.

Flujo:
  1. buscador --live  → descarga páginas del API → empresas_buscador.parquet
  2. detalles --live  → lee IDs de empresas_buscador.parquet → descarga detalle por empresa
                     → empresas_detalle.parquet + tablas normalizadas

Subcomandos:
  buscador   Extrae lista de empresas del buscador SEPREC
  detalles   Extrae detalle de cada empresa (requiere buscador previo en modo live)
  full       Ejecuta buscador + detalles en secuencia

Ejemplos:
  # Prueba rápida: solo los primeros 10 registros de todo el pipeline
  python scripts/pipeline_seprec.py full --live --limit 10 --output-dir output/test

  # Buscador en vivo (completo)
  python scripts/pipeline_seprec.py buscador --live --output-dir output/buscador

  # Buscador en vivo con guardado de JSONs crudos
  python scripts/pipeline_seprec.py buscador --live --save-raw data/raw --output-dir output/buscador

  # Detalles en vivo (usando el Parquet del buscador)
  python scripts/pipeline_seprec.py detalles --live \\
      --buscador-parquet output/buscador/empresas_buscador.parquet \\
      --output-dir output/detalle

  # Pipeline completo en vivo
  python scripts/pipeline_seprec.py full --live \\
      --output-dir output

  # Pipeline completo desde JSONs ya descargados
  python scripts/pipeline_seprec.py full --from-json \\
      --buscador-input data/seprec_data/data \\
      --detalles-input data/seprec-detail/data \\
      --output-dir output

Estructura de salida (--output-dir = output):
  output/
    empresas_buscador.parquet        → Lista aplanada de empresas (una fila por empresa)
    buscador_departamentos.parquet   → Departamentos (FK: empresa_id)
    empresas_detalle.parquet         → Detalle de empresa aplanado
    detalle_objetos_sociales.parquet → Objetos sociales (FK: empresa_id)
    detalle_contactos.parquet        → Contactos (FK: empresa_id)
    detalle_telefonos.parquet        → Teléfonos (FK: empresa_id, contacto_id)
    detalle_emails.parquet           → Correos (FK: empresa_id, contacto_id)
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path


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
# Subcommand: buscador
# ---------------------------------------------------------------------------

def run_buscador(args: argparse.Namespace) -> None:
    """Descarga/lee la lista de empresas del buscador y genera empresas_buscador.parquet."""
    from convert_buscador import (
        PALABRAS_CLAVE,
        load_jsons_buscador,
        download_buscador_live,
        build_and_save_parquets,
    )

    palabras = getattr(args, "palabras", None) or PALABRAS_CLAVE
    limit = getattr(args, "limit", None)

    _log("INFO", "BUSCADOR_START", {
        "mode": "from_json" if args.from_json else "live",
        "palabras": palabras,
        "limit": limit,
    })

    if args.from_json:
        input_dir = getattr(args, "buscador_input", None) or getattr(args, "input_dir", None) or Path("data/seprec_data/data")
        records = load_jsons_buscador(input_dir, palabras, limit=limit)
    else:
        save_raw = getattr(args, "save_raw", None)
        records = download_buscador_live(palabras, raw_dir=save_raw, limit=limit)

    df_main, df_dep = build_and_save_parquets(
        records,
        args.output_dir,
        drop_meta_cols=not getattr(args, "keep_meta", False),
    )

    _log("INFO", "BUSCADOR_DONE", {
        "empresas": len(df_main),
        "departamentos": len(df_dep),
        "output_dir": str(args.output_dir),
    })
    return df_main, df_dep


# ---------------------------------------------------------------------------
# Subcommand: detalles
# ---------------------------------------------------------------------------

def run_detalles(args: argparse.Namespace) -> None:
    """Descarga/lee el detalle de cada empresa y genera Parquets normalizados."""
    from convert_detalles import (
        load_jsons_detalles,
        download_detalles_live,
        build_and_save_parquets as build_detalles_parquets,
    )
    import pandas as pd

    limit = getattr(args, "limit", None)

    _log("INFO", "DETALLES_START", {
        "mode": "from_json" if args.from_json else "live",
        "limit": limit,
    })

    if args.from_json:
        input_dir = (
            getattr(args, "detalles_input", None)
            or getattr(args, "input_dir", None)
            or Path("data/seprec-detail/data")
        )
        records = load_jsons_detalles(input_dir, limit=limit)
    else:
        # Obtener pares empresa/establecimiento desde el Parquet del buscador
        buscador_parquet = (
            getattr(args, "buscador_parquet", None)
            or (args.output_dir / "empresas_buscador.parquet")
        )
        if not Path(buscador_parquet).exists():
            _log("ERROR", "MISSING_BUSCADOR", {
                "path": str(buscador_parquet),
                "msg": "Ejecuta primero el paso 'buscador' o especifica --buscador-parquet.",
            })
            raise SystemExit(1)

        df_bus = pd.read_parquet(buscador_parquet)
        if "id" not in df_bus.columns or "idEstablecimiento" not in df_bus.columns:
            _log("ERROR", "MISSING_COLUMNS", {
                "msg": "El Parquet del buscador no tiene columnas 'id' e 'idEstablecimiento'.",
                "columnas": df_bus.columns.tolist(),
            })
            raise SystemExit(1)

        company_pairs = [
            (str(row["id"]), str(row["idEstablecimiento"]))
            for _, row in df_bus[["id", "idEstablecimiento"]].dropna().iterrows()
        ]
        _log("INFO", "PAIRS_LOADED", {"total": len(company_pairs)})

        save_raw = getattr(args, "save_raw", None)
        raw_dir = Path(save_raw) / "detalles" if save_raw else None
        records = download_detalles_live(company_pairs, raw_dir=raw_dir, limit=limit)

    output_tables = build_detalles_parquets(records, args.output_dir)

    _log("INFO", "DETALLES_DONE", {
        table: len(df) for table, df in output_tables.items()
    })
    return output_tables


# ---------------------------------------------------------------------------
# Subcommand: full
# ---------------------------------------------------------------------------

def run_full(args: argparse.Namespace) -> None:
    """Ejecuta buscador + detalles en secuencia."""
    _log("INFO", "FULL_PIPELINE_START", {
        "mode": "from_json" if args.from_json else "live",
        "limit": getattr(args, "limit", None),
    })

    # Paso 1: Buscador → genera empresas_buscador.parquet en output_dir
    _log("INFO", "STEP", {"step": "1/2", "name": "buscador"})
    run_buscador(args)

    # Paso 2: Detalles → lee IDs del Parquet generado en paso 1
    _log("INFO", "STEP", {"step": "2/2", "name": "detalles"})
    if not args.from_json:
        # Apuntar al parquet recién generado
        args.buscador_parquet = args.output_dir / "empresas_buscador.parquet"

    run_detalles(args)

    _log("INFO", "FULL_PIPELINE_DONE", {"output_dir": str(args.output_dir)})


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _add_limit(sub: argparse.ArgumentParser) -> None:
    sub.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Limita la extracción a los primeros N registros (útil para pruebas rápidas).",
    )


def _add_common(sub: argparse.ArgumentParser) -> None:
    mode = sub.add_mutually_exclusive_group(required=True)
    mode.add_argument("--from-json", action="store_true", help="Leer desde JSONs ya descargados.")
    mode.add_argument("--live", action="store_true", help="Descargar en vivo desde el API.")
    sub.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Carpeta de salida para los Parquets. Default: output",
    )
    _add_limit(sub)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pipeline_seprec",
        description="Pipeline SEPREC: descarga + conversión + Parquets normalizados.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # ---- Subcomando: buscador ----
    sub_bus = subparsers.add_parser(
        "buscador",
        help="Extrae lista de empresas del buscador SEPREC.",
    )
    _add_common(sub_bus)
    sub_bus.add_argument(
        "--input-dir",
        dest="buscador_input",
        type=Path,
        default=Path("data/seprec_data/data"),
        help="(--from-json) Carpeta con JSONs del buscador. Default: data/seprec_data/data",
    )
    sub_bus.add_argument(
        "--save-raw",
        type=Path,
        default=None,
        help="(--live) Carpeta donde guardar los JSONs crudos descargados.",
    )
    sub_bus.add_argument(
        "--palabras",
        nargs="+",
        default=None,
        help="Palabras clave a buscar. Default: a e i o u",
    )
    sub_bus.add_argument(
        "--keep-meta",
        action="store_true",
        help="Conserva columnas _palabra, _pagina, _indice_fila en el Parquet.",
    )

    # ---- Subcomando: detalles ----
    sub_det = subparsers.add_parser(
        "detalles",
        help="Extrae detalle de empresas SEPREC (requiere buscador previo en modo --live).",
    )
    _add_common(sub_det)
    sub_det.add_argument(
        "--input-dir",
        dest="detalles_input",
        type=Path,
        default=Path("data/seprec-detail/data"),
        help="(--from-json) Carpeta con JSONs crudos de detalles. Default: data/seprec-detail/data",
    )
    sub_det.add_argument(
        "--buscador-parquet",
        type=Path,
        default=None,
        help="(--live) Parquet del buscador con columnas id/idEstablecimiento. "
             "Default: <output-dir>/empresas_buscador.parquet",
    )
    sub_det.add_argument(
        "--save-raw",
        type=Path,
        default=None,
        help="(--live) Carpeta raíz donde guardar JSONs crudos (se guarda en <save-raw>/detalles/).",
    )

    # ---- Subcomando: full ----
    sub_full = subparsers.add_parser(
        "full",
        help="Ejecuta buscador + detalles en secuencia.",
    )
    _add_common(sub_full)
    sub_full.add_argument(
        "--buscador-input",
        type=Path,
        default=Path("data/seprec_data/data"),
        help="(--from-json) Carpeta con JSONs del buscador. Default: data/seprec_data/data",
    )
    sub_full.add_argument(
        "--detalles-input",
        type=Path,
        default=Path("data/seprec-detail/data"),
        help="(--from-json) Carpeta con JSONs de detalles. Default: data/seprec-detail/data",
    )
    sub_full.add_argument(
        "--save-raw",
        type=Path,
        default=None,
        help="(--live) Carpeta raíz para JSONs crudos (buscador en <save-raw>/, detalles en <save-raw>/detalles/).",
    )
    sub_full.add_argument(
        "--palabras",
        nargs="+",
        default=None,
        help="(--live) Palabras clave del buscador. Default: a e i o u",
    )
    sub_full.add_argument(
        "--keep-meta",
        action="store_true",
        help="Conserva columnas _palabra, _pagina, _indice_fila en el Parquet del buscador.",
    )

    return parser.parse_args()


def main() -> None:
    # Asegurar que el directorio de scripts esté en el path
    scripts_dir = Path(__file__).resolve().parent
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))

    args = parse_args()

    dispatch = {
        "buscador": run_buscador,
        "detalles": run_detalles,
        "full": run_full,
    }

    handler = dispatch.get(args.subcommand)
    if handler is None:
        _log("ERROR", "UNKNOWN_SUBCOMMAND", {"subcommand": args.subcommand})
        raise SystemExit(1)

    handler(args)


if __name__ == "__main__":
    main()
