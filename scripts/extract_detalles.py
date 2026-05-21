"""
extract_detalles.py
===================
Equivalente al notebook extract_detalles.ipynb.

Parámetros (modificar la sección "Parameters"):
- FROM_JSON : bool
    Si True, lee los JSONs ya descargados en DATA_DIR (result_{empresa}_{establecimiento}.json)
    y genera JSON/JSONL/CSV aplanados con el mismo formato que el modo HTTP.
    Si False (default), hace requests al API de SEPREC.
"""

import requests
import os
import json
import time
import random
import glob
import concurrent.futures
import pandas as pd
from pathlib import Path

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------
BASE_URL = "https://servicios.seprec.gob.bo/api/empresas/informacionBasicaEmpresa/{empresa_id}/establecimiento/{idEstablecimiento}"

data_dir = "data"

# Si True, lee desde los JSONs ya descargados en data_dir en lugar de hacer requests HTTP.
# Los archivos deben tener el patrón: result_{empresa}_{establecimiento}.json
FROM_JSON = False

# ---------------------------------------------------------------------------
# Helpers de aplanado (misma lógica que el notebook)
# ---------------------------------------------------------------------------

def flatten_value(value):
    if isinstance(value, dict):
        flat = {}
        for k, v in value.items():
            fv = flatten_value(v)
            if isinstance(fv, dict):
                for nk, nv in fv.items():
                    flat[f"{k}_{nk}"] = nv
            else:
                flat[k] = fv
        return flat
    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=False)
    return value


def flatten_record(obj, empresa=None, establecimiento=None):
    """
    Aplana un registro de detalle de empresa.
    Si el objeto tiene campo 'datos', lo usa como raíz.
    Añade empresa_origen y establecimiento_origen como metadatos.
    """
    if isinstance(obj, dict) and "datos" in obj and isinstance(obj["datos"], dict):
        record = obj["datos"].copy()
    else:
        record = obj.copy() if isinstance(obj, dict) else {"value": obj}

    if empresa is not None:
        record["empresa_origen"] = empresa
    if establecimiento is not None:
        record["establecimiento_origen"] = establecimiento

    flat = {}
    for k, v in record.items():
        fv = flatten_value(v)
        if isinstance(fv, dict):
            for nk, nv in fv.items():
                flat[f"{k}_{nk}"] = nv
        else:
            flat[k] = fv
    return flat


# ---------------------------------------------------------------------------
# Modo HTTP (original)
# ---------------------------------------------------------------------------

def fetch_company_info(empresa_id, establecimiento_id, base_url, data_dir, log_path="log_extract_detalles.jsonl"):
    def log_event(level, event, extra=None):
        log = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "level": level,
            "event": event,
            "empresa": empresa_id,
            "establecimiento": establecimiento_id,
        }
        if extra:
            log.update(extra)
        log_str = json.dumps(log)
        print(log_str)
        with open(log_path, "a", encoding="utf-8") as flog:
            flog.write(log_str + "\n")

    url = base_url.format(empresa_id=empresa_id, idEstablecimiento=establecimiento_id)
    log_event("INFO", "Procesando solicitud", {"url": url})

    try:
        response = requests.get(url)
        status = response.status_code
        guardar = False
        data_resp = None
        try:
            data_resp = response.json()
            guardar = True
        except Exception:
            data_resp = None
        if status == 412:
            guardar = True
        if guardar:
            if not os.path.exists(data_dir):
                os.makedirs(data_dir)
            file_path = os.path.join(data_dir, f"raw/result_{empresa_id}_{establecimiento_id}.json")
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data_resp if data_resp is not None else {"error": "No JSON"}, f, ensure_ascii=False, indent=4)
        if status != 200:
            log_event("ERROR", "Solicitud fallida", {"status_code": status})
            time.sleep(random.uniform(1, 3))
            return {"empresa": empresa_id, "establecimiento": establecimiento_id, "success": False, "status_code": status}
    except Exception as e:
        log_event("ERROR", "Excepción al solicitar datos", {"error": str(e)})
        time.sleep(random.uniform(1, 3))
        return {"empresa": empresa_id, "establecimiento": establecimiento_id, "success": False, "error": str(e)}

    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    file_path = os.path.join(data_dir, f"result_{empresa_id}_{establecimiento_id}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data_resp, f, ensure_ascii=False, indent=4)

    log_event("INFO", "Solicitud completada exitosamente")
    time.sleep(random.uniform(1, 3))
    return {"empresa": empresa_id, "establecimiento": establecimiento_id, "success": True, "status_code": response.status_code}


def main(
    num_workers=5,
    company_data=[("426118", "439571")],
    log_path="log_extract_detalles.jsonl",
):
    total = len(company_data)
    processed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {
            executor.submit(
                fetch_company_info, empresa_id, establecimiento_id, BASE_URL, data_dir, log_path
            ): (empresa_id, establecimiento_id)
            for empresa_id, establecimiento_id in company_data
        }
        results = []
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            results.append(res)
            processed += 1
            try:
                print(
                    f"Empresa: {res['empresa']}, Establecimiento: {res['establecimiento']}, "
                    f"Success: {res['success']}, Status Code: {res.get('status_code')}"
                )
            except Exception as e:
                print(f"Error displaying result: {e}")
            if processed % 10 == 0 or processed == total:
                log = {
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "level": "INFO",
                    "event": "PROGRESS",
                    "processed": processed,
                    "total": total,
                    "progress_percent": round(100 * processed / total, 2),
                }
                log_str = json.dumps(log)
                print(log_str)
                with open(log_path, "a", encoding="utf-8") as flog:
                    flog.write(log_str + "\n")
    return results


# ---------------------------------------------------------------------------
# Modo FROM_JSON: leer desde JSONs ya descargados y aplanar
# ---------------------------------------------------------------------------

def read_from_jsons(data_dir, out_json, out_jsonl, out_csv):
    """
    Lee los archivos result_{empresa}_{establecimiento}.json ya descargados en data_dir,
    los aplana con la misma lógica que el notebook y genera JSON/JSONL/CSV consolidados.

    Patrón esperado de archivos: result_{numero}_{numero}.json
    Ejemplo: result_100001_100001.json
    """
    flattened = []
    count_loaded = 0

    json_files = sorted(glob.glob(os.path.join(data_dir, "result_*_*.json")))

    print(json.dumps({
        "event": "FROM_JSON_START",
        "archivos_encontrados": len(json_files),
    }, ensure_ascii=False))

    for path in json_files:
        try:
            count_loaded += 1
            with open(path, "r", encoding="utf-8") as fh:
                obj = json.load(fh)

            # Extraer empresa/establecimiento del nombre del archivo
            name = Path(path).stem  # e.g. result_426118_439571
            parts = name.split("_")
            empresa = parts[1] if len(parts) > 1 else None
            establecimiento = parts[2] if len(parts) > 2 else None

            flat = flatten_record(obj, empresa=empresa, establecimiento=establecimiento)
            flattened.append(flat)
        except Exception as e:
            print(f"Error cargando: {path} -> {e}")

    print(f"Archivos leídos: {count_loaded}, registros aplanados: {len(flattened)}")

    # Guardar JSON completo (array) y JSONL
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    with open(out_json, "w", encoding="utf-8") as fjson:
        json.dump(flattened, fjson, ensure_ascii=False, indent=2)

    with open(out_jsonl, "w", encoding="utf-8") as fjsonl:
        for rec in flattened:
            fjsonl.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print(f"Guardado JSON: {out_json}")
    print(f"Guardado JSONL: {out_jsonl}")

    # Consolidar en CSV
    if len(flattened) == 0:
        print("No hay registros a consolidar.")
        return []

    df = pd.DataFrame(flattened)
    # Eliminar columnas auxiliares si existen
    drop_cols = [c for c in ["empresa_origen", "establecimiento_origen"] if c in df.columns]
    if drop_cols:
        df = df.drop(columns=drop_cols)

    before = len(df)
    df_unique = df.drop_duplicates()
    after = len(df_unique)
    df_unique.to_csv(out_csv, index=False, encoding="utf-8")

    print(f"CSV guardado en: {out_csv} (registros: {before} -> únicos: {after})")
    return flattened


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    out_json = os.path.join(data_dir, "details.json")
    out_jsonl = os.path.join(data_dir, "details.jsonl")
    out_csv = os.path.join(data_dir, "details_consolidado.csv")

    if FROM_JSON:
        print("Modo FROM_JSON activado: leyendo datos desde JSONs ya descargados...")
        read_from_jsons(data_dir, out_json, out_jsonl, out_csv)
    else:
        print("Modo HTTP: extrayendo datos desde el API de SEPREC...")
        # Cargar company_data desde el CSV de buscador si existe
        csv_path = os.path.join(data_dir, "consolidado.csv")
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            company_data = df[["id", "idEstablecimiento"]].values.tolist()
            print(f"Cargados {len(company_data)} pares empresa/establecimiento desde {csv_path}")
        else:
            company_data = [("426118", "439571")]
            print(f"CSV no encontrado en {csv_path}, usando datos de ejemplo.")
        main(company_data=company_data)
        print("\nAplanando JSONs descargados -> JSON/JSONL/CSV...")
        read_from_jsons(data_dir, out_json, out_jsonl, out_csv)
