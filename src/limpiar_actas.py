#!/usr/bin/env python3
"""limpiar_actas.py

Lee un archivo JSON Lines (`.jsonl`) con registros de actas de votos y produce
una versión limpia descartando líneas/objetos corruptos o incompletos.

Características:
- Omite líneas que no se pueden parsear como JSON
- Intenta juntar múltiples líneas si un objeto JSON está partido en varias líneas
- Permite especificar campos requeridos; opcionalmente rellena los faltantes con null
- Registra estadísticas al final
"""
import argparse
import json
import logging
from typing import List


def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(format="%(levelname)s: %(message)s", level=level)


def limpiar_jsonl(input_path: str, output_path: str, required: List[str], fill_missing: bool, max_buffer_lines: int = 1000):
    total = 0
    kept = 0
    dropped_parse = 0
    dropped_missing = 0

    with open(input_path, "r", encoding="utf-8") as inf, open(output_path, "w", encoding="utf-8") as outf:
        line_number = 0
        while True:
            raw = inf.readline()
            if not raw:
                break
            line_number += 1
            total += 1
            raw = raw.rstrip("\n")
            if not raw.strip():
                logging.debug("Línea %d vacía, se omite", line_number)
                continue

            # Intento simple: parsear la línea tal cual
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError:
                # Si falla, intentar acumular hasta que podamos parsear (útil si un objeto se partió en varias líneas)
                buffer_lines = [raw]
                parsed = False
                for _ in range(max_buffer_lines - 1):
                    nxt = inf.readline()
                    if not nxt:
                        break
                    line_number += 1
                    buffer_lines.append(nxt.rstrip("\n"))
                    candidate = "\n".join(buffer_lines)
                    try:
                        obj = json.loads(candidate)
                        parsed = True
                        break
                    except json.JSONDecodeError:
                        continue

                if not parsed:
                    logging.warning("No se pudo parsear objeto (línea inicial %d). Se descarta.", line_number - len(buffer_lines) + 1)
                    dropped_parse += 1
                    continue

            # Validación de campos requeridos
            if required:
                missing = [f for f in required if f not in obj]
                if missing:
                    if fill_missing:
                        for f in missing:
                            obj[f] = None
                        logging.debug("Se rellenaron campos faltantes: %s", ",".join(missing))
                    else:
                        logging.debug("Objeto descartado por campos faltantes: %s", ",".join(missing))
                        dropped_missing += 1
                        continue

            # Escribir objeto limpio en una sola línea
            outf.write(json.dumps(obj, ensure_ascii=False) + "\n")
            kept += 1

    logging.info("Total leído: %d", total)
    logging.info("Conservados: %d", kept)
    logging.info("Descartados (parseo): %d", dropped_parse)
    logging.info("Descartados (campos faltantes): %d", dropped_missing)


def parse_required(s: str):
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def main():
    parser = argparse.ArgumentParser(description="Limpia actas en formato JSONL: descarta/rectifica registros corruptos.")
    parser.add_argument("input", help="Archivo JSONL de entrada (ej: actas_detalle_votos.jsonl)")
    parser.add_argument("output", nargs="?", help="Archivo JSONL de salida (por defecto: input_clean.jsonl)")
    parser.add_argument("--required", help="Campos requeridos separados por coma (ej: departamento,provincia,mesa)")
    parser.add_argument("--fill-missing", action="store_true", help="Rellenar campos faltantes con null en lugar de descartarlos")
    parser.add_argument("--max-buffer-lines", type=int, default=1000, help="Máximo de líneas a acumular intentando recomponer un JSON partido")
    parser.add_argument("--verbose", "-v", action="store_true", help="Mostrar logs de depuración")

    args = parser.parse_args()

    setup_logging(args.verbose)

    output = args.output or args.input.replace(".jsonl", "_clean.jsonl")
    required = parse_required(args.required)

    logging.info("Input: %s", args.input)
    logging.info("Output: %s", output)
    if required:
        logging.info("Campos requeridos: %s", ",".join(required))
    if args.fill_missing:
        logging.info("Modo: rellenar campos faltantes con null")

    limpiar_jsonl(args.input, output, required, args.fill_missing, args.max_buffer_lines)


if __name__ == "__main__":
    main()
