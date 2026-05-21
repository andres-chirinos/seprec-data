"""
upload_kaggle.py
================
Sube un directorio a Kaggle como nueva versión de dataset.

Soporta dos tipos de autenticación:
  - kagglehub (recomendado): KAGGLE_API_TOKEN=KGAT_...
  - Legacy (kaggle.json):    KAGGLE_USERNAME + KAGGLE_KEY

Uso:
  python scripts/upload_kaggle.py \\
      --input-dir output/parquet \\
      --kaggle-id andreschirinos/seprec \\
      --version-message "Automatic Upload"

Variables de entorno (desde .env o entorno del sistema):
  KAGGLE_API_TOKEN   Token de kagglehub (recomendado)
  KAGGLE_USERNAME    Usuario legacy
  KAGGLE_KEY         Clave legacy
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
from pathlib import Path


# ---------------------------------------------------------------------------
# .env loader
# ---------------------------------------------------------------------------

def _load_dotenv(repo_root: Path) -> None:
    """Carga variables desde .env si existe, sin sobreescribir las del entorno."""
    for candidate in (repo_root / ".env", Path.cwd() / ".env"):
        if not candidate.exists():
            continue
        for raw in candidate.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].strip()
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip("\"'")
            if key:
                os.environ.setdefault(key, value)
        break


# ---------------------------------------------------------------------------
# Credenciales Kaggle
# ---------------------------------------------------------------------------

def _resolve_credentials() -> tuple[str, str, str]:
    """
    Retorna (username, key, api_token).
    Prefiere KAGGLE_API_TOKEN (kagglehub); cae a username+key si no está.
    """
    def _clean(v: str | None) -> str:
        return (v or "").strip().strip("\"'")

    username = _clean(os.getenv("KAGGLE_USERNAME"))
    key = _clean(os.getenv("KAGGLE_KEY"))
    api_token = _clean(os.getenv("KAGGLE_API_TOKEN"))

    # Algunos usuarios ponen el KGAT_... en KAGGLE_KEY
    if not api_token and key.startswith("KGAT_"):
        api_token = key

    if api_token:
        os.environ["KAGGLE_API_TOKEN"] = api_token
        return username, key, api_token

    missing = [name for name, val in (("KAGGLE_USERNAME", username), ("KAGGLE_KEY", key)) if not val]
    if missing:
        raise EnvironmentError(
            "Faltan credenciales de Kaggle. "
            "Define KAGGLE_API_TOKEN o el par KAGGLE_USERNAME + KAGGLE_KEY. "
            f"Faltantes: {', '.join(missing)}"
        )
    return username, key, api_token


def _write_kaggle_json(username: str, key: str) -> None:
    """Escribe ~/.kaggle/kaggle.json para autenticación legacy."""
    kaggle_dir = Path.home() / ".kaggle"
    kaggle_dir.mkdir(parents=True, exist_ok=True)
    kaggle_json = kaggle_dir / "kaggle.json"
    kaggle_json.write_text(json.dumps({"username": username, "key": key}), encoding="utf-8")
    kaggle_json.chmod(0o600)


# ---------------------------------------------------------------------------
# Publicación
# ---------------------------------------------------------------------------

def publish(input_dir: Path, kaggle_id: str, version_message: str) -> None:
    """Sube el contenido de input_dir como nueva versión del dataset en Kaggle."""
    try:
        import kagglehub
    except ImportError as exc:
        raise RuntimeError(
            "Paquete 'kagglehub' no encontrado. "
            "Instálalo con: pip install kagglehub"
        ) from exc

    print(f"Publicando dataset: {kaggle_id}")
    print(f"Directorio fuente: {input_dir}")
    print(f"Mensaje de versión: {version_message!r}")

    with tempfile.TemporaryDirectory(prefix="kaggle-upload-") as tmp:
        upload_dir = Path(tmp)
        for item in input_dir.iterdir():
            dest = upload_dir / item.name
            if item.is_dir():
                shutil.copytree(item, dest)
            else:
                shutil.copy2(item, dest)

        try:
            kagglehub.dataset_upload(kaggle_id, str(upload_dir), version_notes=version_message)
        except Exception as exc:
            msg = str(exc).lower()
            if "403" in msg or "forbidden" in msg:
                raise PermissionError(
                    f"Kaggle devolvió 403 Forbidden. "
                    f"Verifica autenticación y que el dataset '{kaggle_id}' exista y sea tuyo."
                ) from exc
            raise RuntimeError(f"Fallo en kagglehub.dataset_upload: {exc}") from exc

    print("Publicación completada.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[1]

    parser = argparse.ArgumentParser(
        description="Sube un directorio a Kaggle como nueva versión de dataset.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("output"),
        help="Directorio a subir. Default: output",
    )
    parser.add_argument(
        "--kaggle-id",
        type=str,
        default=None,
        help="ID del dataset en Kaggle (ej: andreschirinos/seprec). "
             "Si no se especifica, se lee de dataset-metadata.json.",
    )
    parser.add_argument(
        "--version-message",
        type=str,
        default="automatic update",
        help="Mensaje de versión. Default: 'automatic update'",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=repo_root,
        help=f"Raíz del repositorio (para buscar .env y dataset-metadata.json). Default: {repo_root}",
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Solo valida credenciales y argumentos; no sube nada.",
    )
    return parser.parse_args()


def _resolve_kaggle_id(args: argparse.Namespace) -> str:
    """Resuelve el kaggle-id desde CLI o desde dataset-metadata.json."""
    if args.kaggle_id:
        return args.kaggle_id

    for candidate in (args.repo_root / "dataset-metadata.json", Path.cwd() / "dataset-metadata.json"):
        if candidate.exists():
            meta = json.loads(candidate.read_text(encoding="utf-8"))
            if "id" in meta and isinstance(meta["id"], str):
                print(f"kaggle-id leído de {candidate}: {meta['id']}")
                return meta["id"]

    raise ValueError(
        "No se pudo determinar el kaggle-id. "
        "Usa --kaggle-id o crea un dataset-metadata.json con el campo 'id'."
    )


def main() -> None:
    args = parse_args()

    _load_dotenv(args.repo_root)

    if not args.input_dir.exists():
        raise SystemExit(f"Error: el directorio de entrada no existe: {args.input_dir}")

    kaggle_id = _resolve_kaggle_id(args)
    username, key, api_token = _resolve_credentials()

    if api_token:
        print("Autenticación: kagglehub (KAGGLE_API_TOKEN)")
    else:
        print(f"Autenticación: legacy kaggle.json (usuario: {username})")
        _write_kaggle_json(username, key)

    if args.skip_upload:
        print("--skip-upload activo. No se realizó ninguna subida.")
        return

    publish(
        input_dir=args.input_dir,
        kaggle_id=kaggle_id,
        version_message=args.version_message,
    )


if __name__ == "__main__":
    main()
