import json
import os


def _limpiar_valor(valor):
    if isinstance(valor, str):
        texto = valor.strip()
        return texto if texto else None

    if isinstance(valor, dict):
        limpio = {}

        for clave, subvalor in valor.items():
            subvalor_limpio = _limpiar_valor(subvalor)

            if subvalor_limpio is not None:
                limpio[clave] = subvalor_limpio

        return limpio

    if isinstance(valor, list):
        return [item for item in (_limpiar_valor(item) for item in valor) if item is not None]

    return valor


def _limpiar_json_archivo(ruta_archivo):
    with open(ruta_archivo, "r", encoding="utf-8") as archivo:
        contenido = json.load(archivo)

    contenido_limpio = _limpiar_valor(contenido)

    with open(ruta_archivo, "w", encoding="utf-8") as archivo:
        json.dump(contenido_limpio, archivo, ensure_ascii=False, indent=2)


def _limpiar_jsonl_archivo(ruta_archivo):
    registros_limpios = []

    with open(ruta_archivo, "r", encoding="utf-8") as archivo:
        for linea in archivo:
            linea = linea.strip()

            if not linea:
                continue

            registro = json.loads(linea)
            registro_limpio = _limpiar_valor(registro)

            if registro_limpio is not None:
                registros_limpios.append(registro_limpio)

    with open(ruta_archivo, "w", encoding="utf-8") as archivo:
        for registro in registros_limpios:
            archivo.write(json.dumps(registro, ensure_ascii=False) + "\n")


def limpiar_jsons_nodo(base_dir):
    """Normaliza todos los .json y .jsonl generados por un worker."""
    if not base_dir or not os.path.exists(base_dir):
        print("No existe la carpeta a limpiar:", base_dir)
        return {"json": 0, "jsonl": 0}

    total_json = 0
    total_jsonl = 0

    for raiz, _, archivos in os.walk(base_dir):
        for nombre_archivo in archivos:
            ruta_archivo = os.path.join(raiz, nombre_archivo)

            if nombre_archivo.endswith(".json"):
                _limpiar_json_archivo(ruta_archivo)
                total_json += 1
            elif nombre_archivo.endswith(".jsonl"):
                _limpiar_jsonl_archivo(ruta_archivo)
                total_jsonl += 1

    print("Limpieza de JSON terminada:", base_dir)
    print("Archivos JSON procesados:", total_json)
    print("Archivos JSONL procesados:", total_jsonl)

    return {"json": total_json, "jsonl": total_jsonl}