import os
import json
import re
import html
import time
import argparse
import subprocess
from datetime import datetime
from json import JSONDecodeError

import requests


# ============================================================
# CONFIGURACIÓN BASE ONPE
# ============================================================

BASE_SITE = "https://resultadoelectoral.onpe.gob.pe"
BASE_URL = "https://resultadoelectoral.onpe.gob.pe/presentacion-backend"

ID_ELECCION = 10
ID_AMBITO_GEOGRAFICO = 1
TAMANIO_PAGINA_ACTAS = 100

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-419,es;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": "https://resultadoelectoral.onpe.gob.pe/main/resumen",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}

session = requests.Session()
session.headers.update(HEADERS)


# ============================================================
# STORAGE MANAGER
# Guarda localmente o prepara salida para HDFS
# ============================================================

class StorageManager:
    def __init__(self, storage_mode, output_dir, hdfs_base, worker_id):
        self.storage_mode = storage_mode
        self.output_dir = output_dir
        self.hdfs_base = hdfs_base.rstrip("/")
        self.worker_id = worker_id

        # Todo el worker escribe primero aquí.
        # Si storage=local, este es el destino final.
        # Si storage=hdfs, este es staging local y luego se sube a HDFS.
        self.local_base = os.path.join(output_dir, worker_id)

        os.makedirs(self.local_base, exist_ok=True)

    def local_path(self, relative_path):
        return os.path.join(self.local_base, relative_path)

    def exists(self, relative_path):
        return os.path.exists(self.local_path(relative_path))

    def cargar_json_si_existe(self, relative_path):
        path = self.local_path(relative_path)

        if not os.path.exists(path):
            return None

        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (OSError, JSONDecodeError):
            return None

    def mkdir_parent(self, relative_path):
        path = self.local_path(relative_path)
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def guardar_json(self, relative_path, data):
        self.mkdir_parent(relative_path)
        path = self.local_path(relative_path)

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print("JSON guardado:", path)

    def checkpoint_json(self, relative_path, data_loader):
        existente = self.cargar_json_si_existe(relative_path)

        if existente is not None:
            print("JSON ya existe, usando checkpoint:", self.local_path(relative_path))
            return existente, False

        data = data_loader()

        if data is None:
            return None, False

        self.guardar_json(relative_path, data)
        return data, True

    def obtener_ultima_pagina_actas_checkpoint(self, ubigeo_distrito):
        # Fast path: pequeño índice con la última página válida.
        ruta_checkpoint = f"raw/actas/checkpoints/{ubigeo_distrito}.json"
        checkpoint = self.cargar_json_si_existe(ruta_checkpoint)

        if isinstance(checkpoint, dict):
            ultima_pagina = checkpoint.get("ultima_pagina")

            if isinstance(ultima_pagina, int) and ultima_pagina >= 0:
                return ultima_pagina

        # Fallback para ejecuciones antiguas que no tenían índice.
        directorio = self.local_path("raw/actas/listado")

        if not os.path.isdir(directorio):
            return None

        patron = re.compile(rf"^{re.escape(str(ubigeo_distrito))}_pagina_(\\d+)\\.json$")
        ultima_pagina = None
        ultimo_mtime = -1.0

        for nombre in os.listdir(directorio):
            match = patron.match(nombre)

            if not match:
                continue

            ruta = os.path.join(directorio, nombre)

            try:
                mtime = os.path.getmtime(ruta)
            except OSError:
                continue

            pagina = int(match.group(1))

            if (
                mtime > ultimo_mtime
                or (mtime == ultimo_mtime and (ultima_pagina is None or pagina > ultima_pagina))
            ):
                ultimo_mtime = mtime
                ultima_pagina = pagina

        return ultima_pagina

    def guardar_ultima_pagina_actas_checkpoint(self, ubigeo_distrito, pagina):
        if not isinstance(pagina, int) or pagina < 0:
            return

        self.guardar_json(
            f"raw/actas/checkpoints/{ubigeo_distrito}.json",
            {
                "ubigeo_distrito": ubigeo_distrito,
                "ultima_pagina": pagina,
                "actualizado_en": datetime.now().isoformat()
            }
        )

    def append_jsonl(self, relative_path, registro):
        self.mkdir_parent(relative_path)
        path = self.local_path(relative_path)

        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(registro, ensure_ascii=False) + "\n")

    def append_jsonl_many(self, relative_path, registros):
        if not registros:
            return

        self.mkdir_parent(relative_path)
        path = self.local_path(relative_path)

        with open(path, "a", encoding="utf-8") as f:
            for registro in registros:
                f.write(json.dumps(registro, ensure_ascii=False) + "\n")

    def guardar_texto(self, relative_path, texto):
        self.mkdir_parent(relative_path)
        path = self.local_path(relative_path)

        with open(path, "w", encoding="utf-8") as f:
            f.write(texto)

        print("Texto guardado:", path)

    def subir_a_hdfs_si_corresponde(self):
        if self.storage_mode != "hdfs":
            print()
            print("Modo local: no se sube a HDFS.")
            print("Salida local en:", self.local_base)
            return

        destino_worker_hdfs = f"{self.hdfs_base}/workers/{self.worker_id}"
        destino_parent_hdfs = f"{self.hdfs_base}/workers"

        print()
        print("=" * 80)
        print("SUBIENDO RESULTADOS A HDFS")
        print("=" * 80)
        print("Origen local:", self.local_base)
        print("Destino HDFS:", destino_worker_hdfs)

        subprocess.run(
            ["hdfs", "dfs", "-mkdir", "-p", destino_parent_hdfs],
            check=True
        )

        # Para evitar error si ya existe una ejecución anterior del mismo worker.
        subprocess.run(
            ["hdfs", "dfs", "-rm", "-r", "-f", destino_worker_hdfs],
            check=False
        )

        subprocess.run(
            ["hdfs", "dfs", "-put", self.local_base, destino_parent_hdfs],
            check=True
        )

        print("Subida a HDFS terminada:", destino_worker_hdfs)


# ============================================================
# UTILIDADES GENERALES
# ============================================================

def inicializar_sesion():
    try:
        r = session.get(
            "https://resultadoelectoral.onpe.gob.pe/main/resumen",
            timeout=30
        )
        print("Inicializando sesión:", r.status_code, r.url)
    except requests.RequestException as e:
        print("No se pudo inicializar sesión:", e)


def parece_html_app_onpe(texto):
    t = texto[:3000].lower()

    return (
        "<app-root" in t
        or "<!doctype html" in t
        or ("<html" in t and "main-" in t)
        or ("runtime" in t and "polyfills" in t)
    )


def extraer_json_desde_texto(texto):
    texto = html.unescape(texto).strip()

    if texto.startswith("{") or texto.startswith("["):
        return json.loads(texto)

    match = re.search(
        r"<pre[^>]*>(.*?)</pre>",
        texto,
        re.DOTALL | re.IGNORECASE
    )

    if match:
        contenido = html.unescape(match.group(1)).strip()
        return json.loads(contenido)

    if parece_html_app_onpe(texto):
        raise ValueError("La URL devolvió HTML de la app ONPE, no JSON del backend")

    match_json = re.search(
        r'(\{"success"\s*:\s*(true|false).*?\})\s*$',
        texto,
        re.DOTALL
    )

    if match_json:
        return json.loads(match_json.group(1))

    raise ValueError("No se encontró JSON válido en la respuesta")


def get_json(endpoint, params=None, debug_name="debug", storage=None):
    url = BASE_URL + endpoint

    try:
        response = session.get(
            url,
            params=params,
            timeout=30
        )

        response.encoding = "utf-8"

        print("GET:", response.url)
        print("STATUS:", response.status_code)
        print("CONTENT-TYPE:", response.headers.get("Content-Type"))
        print("PREVIEW:", response.text[:160].replace("\n", " "))
        print("-" * 80)

        if response.status_code != 200:
            if storage:
                guardar_debug(debug_name, response, storage)
            return None

        try:
            return extraer_json_desde_texto(response.text)

        except (JSONDecodeError, ValueError) as e:
            print("Error extrayendo JSON:", e)
            if storage:
                guardar_debug(debug_name, response, storage)
            return None

    except requests.RequestException as e:
        print("Error de conexión:", e)
        return None


def guardar_debug(nombre, response, storage):
    contenido = ""
    contenido += "URL:\n"
    contenido += response.url + "\n\n"

    contenido += "STATUS:\n"
    contenido += str(response.status_code) + "\n\n"

    contenido += "CONTENT-TYPE:\n"
    contenido += str(response.headers.get("Content-Type")) + "\n\n"

    contenido += "HEADERS:\n"
    contenido += str(dict(response.headers)) + "\n\n"

    contenido += "BODY PRIMEROS 8000 CARACTERES:\n"
    contenido += response.text[:8000]

    storage.guardar_texto(f"debug/{nombre}.txt", contenido)


def normalizar_ubigeo(ubigeo):
    if ubigeo is None:
        return ""

    return str(ubigeo).strip()


def ubigeo_para_actas(ubigeo):
    """
    Para /actas, ONPE suele usar el ubigeo como número.
    Ejemplo:
    '030104' -> '30104'
    """
    if ubigeo is None:
        return ""

    u = str(ubigeo).strip()

    try:
        return str(int(u))
    except ValueError:
        return u


def str_a_bool(valor):
    if isinstance(valor, bool):
        return valor

    valor = valor.lower().strip()

    if valor in ["true", "1", "yes", "y", "si", "sí"]:
        return True

    if valor in ["false", "0", "no", "n"]:
        return False

    raise argparse.ArgumentTypeError("Valor booleano inválido")


# ============================================================
# FUNCIONES ONPE
# ============================================================

def obtener_fecha_listar_fecha(storage):
    def _descargar():
        return get_json(
            "/fecha/listarFecha",
            params=None,
            debug_name="fecha_listarFecha",
            storage=storage
        )

    data, _ = storage.checkpoint_json("raw/fecha/listarFecha.json", _descargar)

    if not data:
        print("No se pudo obtener fecha/listarFecha")
        return None

    return data.get("data")


def obtener_departamentos(storage):
    params = {
        "idEleccion": ID_ELECCION,
        "idAmbitoGeografico": ID_AMBITO_GEOGRAFICO
    }

    def _descargar():
        return get_json(
            "/ubigeos/departamentos",
            params=params,
            debug_name="departamentos",
            storage=storage
        )

    data, _ = storage.checkpoint_json("raw/ubigeos/departamentos/departamentos.json", _descargar)

    if not data:
        print("No se pudo obtener departamentos")
        return []

    return data.get("data", [])


def obtener_provincias(ubigeo_departamento, storage):
    params = {
        "idEleccion": ID_ELECCION,
        "idAmbitoGeografico": ID_AMBITO_GEOGRAFICO,
        "idUbigeoDepartamento": ubigeo_departamento
    }

    ruta = f"raw/ubigeos/provincias/{ubigeo_departamento}.json"

    def _descargar():
        return get_json(
            "/ubigeos/provincias",
            params=params,
            debug_name=f"provincias_{ubigeo_departamento}",
            storage=storage
        )

    data, _ = storage.checkpoint_json(ruta, _descargar)

    if not data:
        print("No se pudo obtener provincias de:", ubigeo_departamento)
        return []

    return data.get("data", [])


def obtener_distritos(ubigeo_provincia, storage):
    params = {
        "idEleccion": ID_ELECCION,
        "idAmbitoGeografico": ID_AMBITO_GEOGRAFICO,
        "idUbigeoProvincia": ubigeo_provincia
    }

    ruta = f"raw/ubigeos/distritos/{ubigeo_provincia}.json"

    def _descargar():
        return get_json(
            "/ubigeos/distritos",
            params=params,
            debug_name=f"distritos_{ubigeo_provincia}",
            storage=storage
        )

    data, _ = storage.checkpoint_json(ruta, _descargar)

    if not data:
        print("No se pudo obtener distritos de:", ubigeo_provincia)
        return []

    return data.get("data", [])


def obtener_locales_distrito(ubigeo_distrito, storage):
    params = {
        "idUbigeo": ubigeo_distrito
    }

    ruta = f"raw/locales/{ubigeo_distrito}.json"

    def _descargar():
        return get_json(
            "/ubigeos/locales",
            params=params,
            debug_name=f"locales_{ubigeo_distrito}",
            storage=storage
        )

    data, _ = storage.checkpoint_json(ruta, _descargar)

    if not data:
        print("No se pudo obtener locales de:", ubigeo_distrito)
        return []

    return data.get("data", [])


def obtener_actas_distrito(ubigeo_distrito, storage, sleep_time):
    """
    Obtiene el listado de actas de un distrito.
    No es el detalle completo todavía.
    """
    id_ubigeo_actas = ubigeo_para_actas(ubigeo_distrito)

    pagina = 0
    ultima_pagina_checkpoint = storage.obtener_ultima_pagina_actas_checkpoint(ubigeo_distrito)

    if ultima_pagina_checkpoint is not None:
        pagina = ultima_pagina_checkpoint
        print(
            "Checkpoint detectado en actas; se revisará solo la última página creada:",
            pagina
        )

    todas_las_actas = []

    while True:
        ruta_pagina = f"raw/actas/listado/{ubigeo_distrito}_pagina_{pagina}.json"
        pagina_descargada = False

        existente = storage.cargar_json_si_existe(ruta_pagina)

        if existente is not None:
            print(f"Página ya existente, usando checkpoint: {ruta_pagina}")
            data = existente
            storage.guardar_ultima_pagina_actas_checkpoint(ubigeo_distrito, pagina)
        else:
            params = {
                "pagina": pagina,
                "tamanio": TAMANIO_PAGINA_ACTAS,
                "idAmbitoGeografico": ID_AMBITO_GEOGRAFICO,
                "idUbigeo": id_ubigeo_actas
            }

            data = get_json(
                "/actas",
                params=params,
                debug_name=f"actas_listado_{ubigeo_distrito}_pagina_{pagina}",
                storage=storage
            )

            if not data:
                print("No se pudo obtener listado de actas de:", ubigeo_distrito)
                break

            storage.guardar_json(ruta_pagina, data)
            storage.guardar_ultima_pagina_actas_checkpoint(ubigeo_distrito, pagina)
            pagina_descargada = True

        payload = data.get("data", {})
        content = payload.get("content", [])

        todas_las_actas.extend(content)

        pagina_actual = payload.get("paginaActual", pagina)
        total_paginas = payload.get("totalPaginas", 0)

        print(
            f"      Página actas {pagina_actual + 1}/{total_paginas}:",
            len(content)
        )

        pagina += 1

        if total_paginas is None or pagina >= total_paginas:
            break

        if pagina_descargada:
            time.sleep(sleep_time)

    consolidado = {
        "success": True,
        "message": "",
        "data": {
            "ubigeoDistrito": ubigeo_distrito,
            "idUbigeoActas": id_ubigeo_actas,
            "totalActasDescargadas": len(todas_las_actas),
            "content": todas_las_actas
        }
    }

    storage.guardar_json(
        f"raw/actas/listado/{ubigeo_distrito}_consolidado.json",
        consolidado
    )

    return todas_las_actas


def obtener_detalle_acta(id_acta, storage):
    endpoint = f"/actas/{id_acta}"

    ruta_detalle = f"raw/actas/detalle/{id_acta}.json"

    existente = storage.cargar_json_si_existe(ruta_detalle)

    if existente is not None:
        print("Detalle ya existente, usando checkpoint:", id_acta)
        return existente.get("data"), True

    data = get_json(
        endpoint,
        params=None,
        debug_name=f"acta_detalle_{id_acta}",
        storage=storage
    )

    if not data:
        print("No se pudo obtener detalle de acta:", id_acta)
        return None, False

    storage.guardar_json(ruta_detalle, data)

    return data.get("data"), False


# ============================================================
# EXTRACTORES PARA PROCESADO
# ============================================================

def extraer_resumen_detalle_acta(detalle_acta):
    if not detalle_acta:
        return None

    return {
        "id_acta": detalle_acta.get("id"),
        "codigo_mesa": detalle_acta.get("codigoMesa"),
        "descripcion_mesa": detalle_acta.get("descripcionMesa"),
        "id_eleccion": detalle_acta.get("idEleccion"),

        "departamento": detalle_acta.get("ubigeoNivel01"),
        "provincia": detalle_acta.get("ubigeoNivel02"),
        "distrito": detalle_acta.get("ubigeoNivel03"),
        "centro_poblado": detalle_acta.get("centroPoblado"),
        "nombre_local_votacion": detalle_acta.get("nombreLocalVotacion"),

        "total_electores_habiles": detalle_acta.get("totalElectoresHabiles"),
        "total_votos_emitidos": detalle_acta.get("totalVotosEmitidos"),
        "total_votos_validos": detalle_acta.get("totalVotosValidos"),
        "total_asistentes": detalle_acta.get("totalAsistentes"),
        "porcentaje_participacion_ciudadana": detalle_acta.get(
            "porcentajeParticipacionCiudadana"
        ),

        "estado_acta": detalle_acta.get("estadoActa"),
        "estado_computo": detalle_acta.get("estadoComputo"),
        "codigo_estado_acta": detalle_acta.get("codigoEstadoActa"),
        "descripcion_estado_acta": detalle_acta.get("descripcionEstadoActa"),
        "estado_acta_resolucion": detalle_acta.get("estadoActaResolucion"),
        "estado_descripcion_acta_resolucion": detalle_acta.get(
            "estadoDescripcionActaResolucion"
        ),
        "descripcion_sub_estado_acta": detalle_acta.get("descripcionSubEstadoActa"),

        "cantidad_detalle_votos": len(detalle_acta.get("detalle") or []),
        "cantidad_linea_tiempo": len(detalle_acta.get("lineaTiempo") or []),
        "cantidad_archivos": len(detalle_acta.get("archivos") or []),

        "codigo_solucion_tecnologica": detalle_acta.get("codigoSolucionTecnologica"),
        "descripcion_solucion_tecnologica": detalle_acta.get(
            "descripcionSolucionTecnologica"
        )
    }


def extraer_votos_detalle_acta(detalle_acta):
    if not detalle_acta:
        return []

    id_acta = detalle_acta.get("id")
    codigo_mesa = detalle_acta.get("codigoMesa")

    votos = []

    for item in detalle_acta.get("detalle") or []:
        candidatos = item.get("candidato") or []

        if candidatos:
            candidato = candidatos[0]
            candidato_apellido_paterno = candidato.get("apellidoPaterno")
            candidato_apellido_materno = candidato.get("apellidoMaterno")
            candidato_nombres = candidato.get("nombres")
            candidato_documento = candidato.get("cdocumentoIdentidad")
        else:
            candidato_apellido_paterno = None
            candidato_apellido_materno = None
            candidato_nombres = None
            candidato_documento = None

        votos.append({
            "id_acta": id_acta,
            "codigo_mesa": codigo_mesa,

            "descripcion": item.get("descripcion"),
            "estado": item.get("estado"),
            "grafico": item.get("grafico"),
            "cargo": item.get("cargo"),
            "sexo": item.get("sexo"),
            "total_candidatos": item.get("totalCandidatos"),

            "nvotos": item.get("nvotos"),
            "nagrupacion_politica": item.get("nagrupacionPolitica"),
            "nporcentaje_votos_validos": item.get("nporcentajeVotosValidos"),
            "nporcentaje_votos_emitidos": item.get("nporcentajeVotosEmitidos"),
            "ccodigo": item.get("ccodigo"),
            "nposicion": item.get("nposicion"),

            "candidato_apellido_paterno": candidato_apellido_paterno,
            "candidato_apellido_materno": candidato_apellido_materno,
            "candidato_nombres": candidato_nombres,
            "candidato_documento": candidato_documento
        })

    return votos


def extraer_linea_tiempo_detalle_acta(detalle_acta):
    if not detalle_acta:
        return []

    id_acta = detalle_acta.get("id")
    codigo_mesa = detalle_acta.get("codigoMesa")

    filas = []

    for item in detalle_acta.get("lineaTiempo") or []:
        filas.append({
            "id_acta": id_acta,
            "codigo_mesa": codigo_mesa,
            "codigo_estado_acta": item.get("codigoEstadoActa"),
            "descripcion_estado_acta": item.get("descripcionEstadoActa"),
            "descripcion_estado_acta_resolucion": item.get(
                "descripcionEstadoActaResolucion"
            ),
            "fecha_registro": item.get("fechaRegistro")
        })

    return filas


def extraer_archivos_detalle_acta(detalle_acta):
    if not detalle_acta:
        return []

    id_acta = detalle_acta.get("id")
    codigo_mesa = detalle_acta.get("codigoMesa")

    filas = []

    for item in detalle_acta.get("archivos") or []:
        filas.append({
            "id_acta": id_acta,
            "codigo_mesa": codigo_mesa,
            "id_archivo": item.get("id"),
            "tipo": item.get("tipo"),
            "nombre": item.get("nombre"),
            "descripcion": item.get("descripcion"),
            "fecha_creacion": item.get("daudFechaCreacion")
        })

    return filas


# ============================================================
# REGISTROS PROCESADOS
# ============================================================

def construir_registro_acta_listado(
    dep,
    prov,
    dist,
    acta
):
    return {
        "ubigeo_departamento": dep["ubigeo"],
        "nombre_departamento": dep["nombre"],

        "ubigeo_provincia": prov["ubigeo"],
        "nombre_provincia": prov["nombre"],

        "ubigeo_distrito": dist["ubigeo"],
        "nombre_distrito": dist["nombre"],

        "id_acta": acta.get("id"),
        "id_mesa": acta.get("idMesa"),
        "codigo_mesa": acta.get("codigoMesa"),
        "numero_copia": acta.get("numeroCopia"),
        "id_ubigeo_eleccion": acta.get("idUbigeoEleccion"),
        "id_eleccion": acta.get("idEleccion"),
        "id_ambito_geografico": acta.get("idAmbitoGeografico"),
        "id_ubigeo": acta.get("idUbigeo"),
        "centro_poblado": acta.get("centroPoblado"),
        "nombre_local_votacion": acta.get("nombreLocalVotacion"),
        "codigo_local_votacion": acta.get("codigoLocalVotacion"),
        "total_electores_habiles": acta.get("totalElectoresHabiles"),
        "total_votos_emitidos": acta.get("totalVotosEmitidos"),
        "total_votos_validos": acta.get("totalVotosValidos"),
        "total_asistentes": acta.get("totalAsistentes"),
        "porcentaje_participacion_ciudadana": acta.get(
            "porcentajeParticipacionCiudadana"
        ),
        "estado_acta": acta.get("estadoActa"),
        "estado_computo": acta.get("estadoComputo"),
        "codigo_estado_acta": acta.get("codigoEstadoActa"),
        "descripcion_estado_acta": acta.get("descripcionEstadoActa")
    }


def agregar_contexto_geografico(registro, dep, prov, dist):
    registro["ubigeo_departamento"] = dep["ubigeo"]
    registro["nombre_departamento"] = dep["nombre"]

    registro["ubigeo_provincia"] = prov["ubigeo"]
    registro["nombre_provincia"] = prov["nombre"]

    registro["ubigeo_distrito"] = dist["ubigeo"]
    registro["nombre_distrito"] = dist["nombre"]

    return registro


# ============================================================
# FUNCIÓN PRINCIPAL DEL WORKER
# ============================================================

def ejecutar_worker(
    ubigeos_asignados,
    storage,
    descargar_detalle=True,
    sleep_time=0.05
):
    print()
    print("=" * 80)
    print("WORKER ONPE INICIADO")
    print("=" * 80)
    print("Worker ID:", storage.worker_id)
    print("Ubigeos asignados:", ", ".join(ubigeos_asignados))
    print("Storage:", storage.storage_mode)
    print("Descargar detalle:", descargar_detalle)
    print("=" * 80)
    print()

    inicializar_sesion()

    print()
    print("Descargando fecha/listarFecha...")
    obtener_fecha_listar_fecha(storage)

    print()
    print("Descargando departamentos...")
    departamentos_totales = obtener_departamentos(storage)

    if not departamentos_totales:
        print("No se pudo obtener departamentos. Abortando worker.")
        return

    # Filtramos solo los departamentos asignados por el orquestador.
    departamentos = []

    for d in departamentos_totales:
        ubigeo_dep = normalizar_ubigeo(d.get("ubigeo"))

        if ubigeo_dep in ubigeos_asignados:
            departamentos.append({
                "ubigeo": ubigeo_dep,
                "nombre": d.get("nombre")
            })

    print()
    print(f"Departamentos que procesará este worker: {len(departamentos)}")

    for d in departamentos:
        print(f" - {d['ubigeo']} {d['nombre']}")

    contadores = {
        "departamentos": len(departamentos),
        "provincias": 0,
        "distritos": 0,
        "locales": 0,
        "actas_listado": 0,
        "actas_detalle": 0,
        "votos_detalle": 0,
        "lineas_tiempo": 0,
        "archivos_acta": 0,
        "errores_detalle": 0
    }

    for dep in departamentos:
        print()
        print("=" * 80)
        print("PROCESANDO DEPARTAMENTO:", dep["ubigeo"], dep["nombre"])
        print("=" * 80)

        provincias_raw = obtener_provincias(dep["ubigeo"], storage)
        provincias = []
        filas_provincias = []

        for p in provincias_raw:
            prov = {
                "ubigeo": normalizar_ubigeo(p.get("ubigeo")),
                "nombre": p.get("nombre")
            }
            provincias.append(prov)

            filas_provincias.append({
                "ubigeo_departamento": dep["ubigeo"],
                "nombre_departamento": dep["nombre"],
                "ubigeo_provincia": prov["ubigeo"],
                "nombre_provincia": prov["nombre"]
            })

        storage.append_jsonl_many(
            "processed/provincias_por_departamento.jsonl",
            filas_provincias
        )

        contadores["provincias"] += len(provincias)

        print("Provincias encontradas:", len(provincias))

        for prov in provincias:
            print()
            print("  Provincia:", prov["ubigeo"], prov["nombre"])

            distritos_raw = obtener_distritos(prov["ubigeo"], storage)
            distritos = []
            filas_distritos = []

            for d in distritos_raw:
                dist = {
                    "ubigeo": normalizar_ubigeo(d.get("ubigeo")),
                    "nombre": d.get("nombre")
                }
                distritos.append(dist)

                filas_distritos.append({
                    "ubigeo_departamento": dep["ubigeo"],
                    "nombre_departamento": dep["nombre"],
                    "ubigeo_provincia": prov["ubigeo"],
                    "nombre_provincia": prov["nombre"],
                    "ubigeo_distrito": dist["ubigeo"],
                    "nombre_distrito": dist["nombre"],
                    "longitud_ubigeo_distrito": len(dist["ubigeo"])
                })

            storage.append_jsonl_many(
                "processed/distritos_por_provincia.jsonl",
                filas_distritos
            )

            contadores["distritos"] += len(distritos)

            print("  Distritos encontrados:", len(distritos))

            for dist in distritos:
                print()
                print("    Distrito:", dist["ubigeo"], dist["nombre"])

                # -----------------------------
                # Locales
                # -----------------------------
                locales = obtener_locales_distrito(dist["ubigeo"], storage)
                contadores["locales"] += len(locales)

                filas_locales = []

                for local in locales:
                    filas_locales.append({
                        "ubigeo_departamento": dep["ubigeo"],
                        "nombre_departamento": dep["nombre"],
                        "ubigeo_provincia": prov["ubigeo"],
                        "nombre_provincia": prov["nombre"],
                        "ubigeo_distrito": dist["ubigeo"],
                        "nombre_distrito": dist["nombre"],
                        "codigo_local_votacion": local.get("codigoLocalVotacion"),
                        "nombre_local_votacion": local.get("nombreLocalVotacion")
                    })

                storage.append_jsonl_many(
                    "processed/locales_por_distrito.jsonl",
                    filas_locales
                )

                # -----------------------------
                # Listado de actas
                # -----------------------------
                actas = obtener_actas_distrito(
                    dist["ubigeo"],
                    storage,
                    sleep_time
                )

                contadores["actas_listado"] += len(actas)

                for acta in actas:
                    id_acta = acta.get("id")

                    registro_acta = construir_registro_acta_listado(
                        dep,
                        prov,
                        dist,
                        acta
                    )

                    storage.append_jsonl(
                        "processed/actas_listado_por_distrito.jsonl",
                        registro_acta
                    )

                    # -----------------------------
                    # Detalle de acta
                    # -----------------------------
                    if descargar_detalle and id_acta is not None:
                        detalle_acta, es_checkpoint = obtener_detalle_acta(id_acta, storage)

                        if not detalle_acta:
                            contadores["errores_detalle"] += 1
                            continue

                        contadores["actas_detalle"] += 1

                        resumen = extraer_resumen_detalle_acta(detalle_acta)

                        if resumen:
                            resumen = agregar_contexto_geografico(
                                resumen,
                                dep,
                                prov,
                                dist
                            )

                            storage.append_jsonl(
                                "processed/actas_detalle_resumen.jsonl",
                                resumen
                            )

                        votos = extraer_votos_detalle_acta(detalle_acta)
                        votos_con_contexto = [
                            agregar_contexto_geografico(voto, dep, prov, dist)
                            for voto in votos
                        ]

                        storage.append_jsonl_many(
                            "processed/actas_detalle_votos.jsonl",
                            votos_con_contexto
                        )

                        contadores["votos_detalle"] += len(votos)

                        lineas = extraer_linea_tiempo_detalle_acta(detalle_acta)
                        lineas_con_contexto = [
                            agregar_contexto_geografico(linea, dep, prov, dist)
                            for linea in lineas
                        ]

                        storage.append_jsonl_many(
                            "processed/actas_detalle_linea_tiempo.jsonl",
                            lineas_con_contexto
                        )

                        contadores["lineas_tiempo"] += len(lineas)

                        archivos = extraer_archivos_detalle_acta(detalle_acta)
                        archivos_con_contexto = [
                            agregar_contexto_geografico(archivo, dep, prov, dist)
                            for archivo in archivos
                        ]

                        storage.append_jsonl_many(
                            "processed/actas_detalle_archivos.jsonl",
                            archivos_con_contexto
                        )

                        contadores["archivos_acta"] += len(archivos)

                        if not es_checkpoint:
                            time.sleep(sleep_time)

    manifest = {
        "worker_id": storage.worker_id,
        "storage_mode": storage.storage_mode,
        "ubigeos_asignados": ubigeos_asignados,
        "fecha_ejecucion": datetime.now().isoformat(),
        "contadores": contadores
    }

    storage.guardar_json("manifest_worker.json", manifest)

    print()
    print("=" * 80)
    print("WORKER TERMINADO")
    print("=" * 80)
    print(json.dumps(manifest, ensure_ascii=False, indent=2))

    storage.subir_a_hdfs_si_corresponde()


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Worker de descarga ONPE por ubigeos asignados"
    )

    parser.add_argument(
        "--ubigeos",
        required=True,
        help="Lista de ubigeos de departamentos separados por coma. Ejemplo: 03,04,05"
    )

    parser.add_argument(
        "--worker-id",
        default=None,
        help="ID del worker. Ejemplo: worker_1. Si no se manda, se genera automáticamente."
    )

    parser.add_argument(
        "--storage",
        choices=["local", "hdfs"],
        default="local",
        help="Dónde guardar la salida: local o hdfs"
    )

    parser.add_argument(
        "--hdfs-base",
        default="/onpe",
        help="Ruta base en HDFS. Ejemplo: /onpe"
    )

    parser.add_argument(
        "--output-dir",
        default="data_workers",
        help="Carpeta local de salida o staging"
    )

    parser.add_argument(
        "--descargar-detalle",
        type=str_a_bool,
        default=True,
        help="true/false. Si true descarga detalle de cada acta"
    )

    parser.add_argument(
        "--sleep",
        type=float,
        default=0.05,
        help="Pausa entre requests para no golpear demasiado el servidor"
    )

    args = parser.parse_args()

    ubigeos_asignados = [
        normalizar_ubigeo(u)
        for u in args.ubigeos.split(",")
        if normalizar_ubigeo(u)
    ]

    if not ubigeos_asignados:
        raise ValueError("No se recibió ningún ubigeo válido.")

    if args.worker_id:
        worker_id = args.worker_id
    else:
        worker_id = f"worker_chunk_{ubigeos_asignados[0]}"

    storage = StorageManager(
        storage_mode=args.storage,
        output_dir=args.output_dir,
        hdfs_base=args.hdfs_base,
        worker_id=worker_id
    )

    ejecutar_worker(
        ubigeos_asignados=ubigeos_asignados,
        storage=storage,
        descargar_detalle=args.descargar_detalle,
        sleep_time=args.sleep
    )


if __name__ == "__main__":
    main()