"""
extract_buscador.py
===================
Equivalente al notebook extract_buscador.ipynb.

Parámetros (modificar la sección "Parameters" o pasar por entorno/CLI):
- FROM_JSON : bool
    Si True, lee los JSONs ya descargados en DATA_DIR (result_{letra}_{pagina}.json)
    y genera los mismos archivos JSONL que el modo HTTP, sin hacer ningún request.
    Si False (default), hace requests al API de SEPREC.
"""

import requests
import os
import json
import time
import random
import math
import glob
import re
import concurrent.futures
import pandas as pd

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------
BASE_URL = "https://servicios.seprec.gob.bo/api/empresas/buscarEmpresas"
LIMIT = 10
START_PAGE = 1
PALABRAS_CLAVE = ["a", "e", "i", "o", "u"]
DATA_DIR = "data"
JSONL_DIR = "data/jsonl"
JSON_DIR = "data/seprec_data/data"
REQUEST_DELAY_RANGE = (1, 3)

# Si True, lee desde los JSONs ya descargados en DATA_DIR en lugar de hacer requests HTTP.
# Los archivos deben tener el patrón: result_{letra}_{pagina}.json
FROM_JSON = True

params = {"filtro": "", "limite": LIMIT, "pagina": START_PAGE}

palabras_clave = PALABRAS_CLAVE
data_dir = DATA_DIR
jsonl_dir = JSONL_DIR

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def flatten_value(value):
    if isinstance(value, dict):
        flattened = {}
        for key, nested_value in value.items():
            nested_flat = flatten_value(nested_value)
            if isinstance(nested_flat, dict):
                for nested_key, nested_item in nested_flat.items():
                    flattened[f"{key}.{nested_key}"] = nested_item
            else:
                flattened[key] = nested_flat
        return flattened
    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=False)
    return value


def flatten_record(record, palabra, page, row_index):
    flattened = {"palabra": palabra, "pagina": page, "indice_fila": row_index}
    for key, value in record.items():
        flattened_value = flatten_value(value)
        if isinstance(flattened_value, dict):
            for nested_key, nested_item in flattened_value.items():
                flattened[f"{key}_{nested_key}"] = nested_item
        else:
            flattened[key] = flattened_value
    return flattened


def write_jsonl_record(file_path, record):
    with open(file_path, "a", encoding="utf-8") as file_handle:
        file_handle.write(json.dumps(record, ensure_ascii=False))
        file_handle.write("\n")


# ---------------------------------------------------------------------------
# Modo HTTP (original)
# ---------------------------------------------------------------------------

def fetch_data_for_word(palabra, base_url, params, data_dir, jsonl_dir):
    params = params.copy()
    params["filtro"] = palabra
    page = START_PAGE
    consulted = 0
    failed = 0
    total_data = None
    maxpages = 1

    def log_event(level, event, extra=None):
        log = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "level": level,
            "event": event,
            "palabra": palabra,
            "page": page,
        }
        if extra:
            log.update(extra)
        print(json.dumps(log, ensure_ascii=False))

    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(jsonl_dir, exist_ok=True)

    while page <= maxpages:
        log_event("INFO", "Procesando página")
        try:
            response = requests.get(base_url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
        except Exception as error:
            failed += 1
            log_event("ERROR", "Excepción al solicitar página", {"error": str(error)})
            page += 1
            params["pagina"] = page
            continue

        raw_file_path = os.path.join(data_dir, f"result_{palabra}_{page}.json")
        with open(raw_file_path, "w", encoding="utf-8") as file_handle:
            json.dump(data, file_handle, ensure_ascii=False, indent=4)

        filas = data.get("datos", {}).get("filas", [])
        if not isinstance(filas, list):
            filas = []

        jsonl_file_path = os.path.join(jsonl_dir, f"result_{palabra}.jsonl")
        for row_index, record in enumerate(filas, start=1):
            flattened_record = flatten_record(record, palabra, page, row_index)
            write_jsonl_record(jsonl_file_path, flattened_record)

        consulted += 1

        if page == START_PAGE:
            try:
                total_data = int(data["datos"]["total"])
                maxpages = max(1, math.ceil(total_data / params["limite"]))
            except Exception as error:
                log_event("WARN", "No se pudo determinar el número de páginas", {"error": str(error)})

        faltantes = total_data - (consulted + failed) if total_data is not None else "N/A"
        log_event(
            "INFO",
            "Avance parcial",
            {
                "visitadas": consulted,
                "fallidas": failed,
                "faltantes": faltantes,
                "totales": total_data if total_data is not None else "N/A",
            },
        )

        time.sleep(random.uniform(*REQUEST_DELAY_RANGE))
        page += 1
        params["pagina"] = page

    return {
        "palabra": palabra,
        "consulted": consulted,
        "failed": failed,
        "total": total_data,
        "jsonl_file": os.path.join(jsonl_dir, f"result_{palabra}.jsonl"),
    }


def main(num_workers=5):
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {
            executor.submit(fetch_data_for_word, palabra, BASE_URL, params, data_dir, jsonl_dir): palabra
            for palabra in palabras_clave
        }
        results = []
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            results.append(res)
            try:
                faltantes = (
                    res.get("total", 0) - (res.get("consulted", 0) + res.get("failed", 0))
                    if res.get("total") is not None
                    else "N/A"
                )
                print(
                    f"Palabra: {res['palabra']}, Visitadas: {res['consulted']}, "
                    f"Fallidas: {res['failed']}, Faltantes: {faltantes}, Totales: {res.get('total', 'N/A')}, "
                    f"JSONL: {res.get('jsonl_file', 'N/A')}"
                )
            except Exception as error:
                print(f"Error mostrando estadísticas parciales: {error}")
    return results


# ---------------------------------------------------------------------------
# Modo FROM_JSON: leer desde JSONs ya descargados
# ---------------------------------------------------------------------------

def read_from_jsons(palabras_clave, data_dir, jsonl_dir):
    """
    Lee los archivos result_{palabra}_{pagina}.json ya descargados en data_dir,
    extrae las filas y los escribe en el mismo formato JSONL que el modo HTTP.

    Patrón esperado de archivos: result_{letra}_{numero_pagina}.json
    Ejemplo: result_a_10000.json, result_e_5.json
    """
    os.makedirs(jsonl_dir, exist_ok=True)
    results = []

    for palabra in palabras_clave:
        jsonl_file_path = os.path.join(jsonl_dir, f"result_{palabra}.jsonl")
        # Limpiar JSONL previo para esta palabra
        open(jsonl_file_path, "w", encoding="utf-8").close()

        consulted = 0
        total_records = 0
        total_data = None

        # Buscar todos los archivos result_{palabra}_{pagina}.json
        pattern = os.path.join(data_dir, f"result_{palabra}_*.json")
        matched_files = glob.glob(pattern)

        # Ordenar por número de página extraído del nombre del archivo
        def extract_page_num(path):
            m = re.search(r"_(\d+)\.json$", os.path.basename(path))
            return int(m.group(1)) if m else 0

        matched_files = sorted(matched_files, key=extract_page_num)

        print(json.dumps({
            "event": "FROM_JSON_START",
            "palabra": palabra,
            "archivos_encontrados": len(matched_files),
        }, ensure_ascii=False))

        for file_path in matched_files:
            # Extraer número de página del nombre de archivo
            m = re.search(r"result_" + re.escape(palabra) + r"_(\d+)\.json$", os.path.basename(file_path))
            page = int(m.group(1)) if m else 0

            try:
                with open(file_path, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
            except Exception as e:
                print(json.dumps({
                    "event": "ERROR_LECTURA",
                    "archivo": file_path,
                    "error": str(e),
                }, ensure_ascii=False))
                continue

            # Capturar el total declarado desde la primera página
            if total_data is None:
                try:
                    total_data = int(data["datos"]["total"])
                except Exception:
                    pass

            filas = data.get("datos", {}).get("filas", [])
            if not isinstance(filas, list):
                filas = []

            for row_index, record in enumerate(filas, start=1):
                flattened_record = flatten_record(record, palabra, page, row_index)
                write_jsonl_record(jsonl_file_path, flattened_record)
                total_records += 1

            consulted += 1

        results.append({
            "palabra": palabra,
            "archivos_leidos": consulted,
            "registros": total_records,
            "total_declarado": total_data,
            "jsonl_file": jsonl_file_path,
        })

        print(json.dumps({
            "event": "FROM_JSON_DONE",
            "palabra": palabra,
            "archivos_leidos": consulted,
            "registros_escritos": total_records,
            "total_declarado": total_data,
            "jsonl_file": jsonl_file_path,
        }, ensure_ascii=False))

    return results


# ---------------------------------------------------------------------------
# Consolidación de JSONL -> CSV (igual que en el notebook)
# ---------------------------------------------------------------------------

def consolidar_csv(jsonl_dir, data_dir):
    """Consolida todos los JSONL en un CSV, eliminando duplicados."""
    all_records = []
    jsonl_files = glob.glob(os.path.join(jsonl_dir, "*.jsonl"))

    print(f"Consolidando {len(jsonl_files)} archivos JSONL...")

    for jsonl_file in jsonl_files:
        with open(jsonl_file, "r", encoding="utf-8") as file_handle:
            for line in file_handle:
                if line.strip():
                    record = json.loads(line)
                    all_records.append(record)

    print(f"Total de registros cargados: {len(all_records)}")

    df = pd.DataFrame(all_records)
    fields_to_drop = ["palabra", "pagina", "indice_fila"]
    df = df.drop(columns=[col for col in fields_to_drop if col in df.columns], errors="ignore")

    print(f"Registros antes de eliminar duplicados: {len(df)}")

    df_unique = df.drop_duplicates()

    print(f"Registros únicos: {len(df_unique)}")
    print(f"Duplicados eliminados: {len(df) - len(df_unique)}")

    csv_output_path = os.path.join(data_dir, "consolidado.csv")
    df_unique.to_csv(csv_output_path, index=False, encoding="utf-8")

    print(f"CSV guardado en: {csv_output_path}")
    print(f"Columnas: {', '.join(df_unique.columns.tolist())}")
    return df_unique


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if FROM_JSON:
        print("Modo FROM_JSON activado: leyendo datos desde JSONs ya descargados...")
        results = read_from_jsons(palabras_clave, data_dir, jsonl_dir)
        for res in results:
            print(
                f"Palabra: {res['palabra']}, Archivos leídos: {res['archivos_leidos']}, "
                f"Registros: {res['registros']}, Total declarado: {res['total_declarado']}, "
                f"JSONL: {res['jsonl_file']}"
            )
    else:
        print("Modo HTTP: extrayendo datos desde el API de SEPREC...")
        results = main()

    print("\nConsolidando JSONL -> CSV...")
    consolidar_csv(jsonl_dir, data_dir)
