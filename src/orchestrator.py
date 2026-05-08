import os
import json
import re
import html
from operator import itemgetter
import requests

BASE_URL = "https://resultadoelectoral.onpe.gob.pe/presentacion-backend"
ID_ELECCION = 10 # id de las elecciones presidenciales, diputados, senadores y parlamento andino
ID_AMBITO_GEOGRAFICO = 1

# Usamos EXACTAMENTE los mismos headers de tu crawler original
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

def inicializar_sesion():
    try:
        r = session.get("https://resultadoelectoral.onpe.gob.pe/main/resumen", timeout=30)
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

    match = re.search(r"<pre[^>]*>(.*?)</pre>", texto, re.DOTALL | re.IGNORECASE)
    if match:
        contenido = html.unescape(match.group(1)).strip()
        return json.loads(contenido)

    if parece_html_app_onpe(texto):
        raise ValueError("La URL devolvió HTML de la app ONPE, no JSON del backend")

    match_json = re.search(r'(\{"success"\s*:\s*(true|false).*?\})\s*$', texto, re.DOTALL)
    if match_json:
        return json.loads(match_json.group(1))

    raise ValueError("No se encontró JSON válido en la respuesta")

def get_json(endpoint, params=None):
    url = BASE_URL + endpoint
    try:
        response = session.get(url, params=params, timeout=30)
        response.encoding = "utf-8"
        
        # Agregamos los prints de debug igual que en tu crawler
        print(f"GET: {response.url}")
        print(f"STATUS: {response.status_code}")
        print(f"PREVIEW: {response.text[:100].replace(chr(10), ' ')}")
        print("-" * 60)

        if response.status_code == 200:
            return extraer_json_desde_texto(response.text)
            
    except Exception as e:
        print(f"Error procesando {url}: {e}")
    return None

def obtener_departamentos():
    data = get_json("/ubigeos/departamentos", {"idEleccion": ID_ELECCION, "idAmbitoGeografico": ID_AMBITO_GEOGRAFICO})
    return data.get("data", []) if data else []

def obtener_provincias(ubigeo_dep):
    data = get_json("/ubigeos/provincias", {"idEleccion": ID_ELECCION, "idAmbitoGeografico": ID_AMBITO_GEOGRAFICO, "idUbigeoDepartamento": ubigeo_dep})
    return data.get("data", []) if data else []

def obtener_distritos(ubigeo_prov):
    data = get_json("/ubigeos/distritos", {"idEleccion": ID_ELECCION, "idAmbitoGeografico": ID_AMBITO_GEOGRAFICO, "idUbigeoProvincia": ubigeo_prov})
    return data.get("data", []) if data else []

def estimar_peso_departamentos():
    departamentos = obtener_departamentos()
    pesos_deptos = []
    
    if not departamentos:
        print("Error: No se pudo obtener la lista inicial de departamentos.")
        return []

    print("\nCalculando pesos heurísticos de los departamentos (esto tomará unos minutos)...")
    for dep in departamentos:
        ubigeo_dep = str(dep["ubigeo"]).strip()
        nombre_dep = dep["nombre"]
        provincias = obtener_provincias(ubigeo_dep)
        
        total_distritos = 0
        for prov in provincias:
            distritos = obtener_distritos(str(prov["ubigeo"]).strip())
            total_distritos += len(distritos)
        
        pesos_deptos.append({
            "ubigeo": ubigeo_dep,
            "nombre": nombre_dep,
            "peso": total_distritos
        })
        print(f"[{nombre_dep}] estimado en {total_distritos} distritos.")
        
    return pesos_deptos

def distribuir_carga_lpt(pesos_deptos, num_workers):
    if not pesos_deptos:
        return []
        
    deptos_ordenados = sorted(pesos_deptos, key=itemgetter("peso"), reverse=True)
    workers = [{"id": i+1, "carga_total": 0, "departamentos": []} for i in range(num_workers)]
    
    for dep in deptos_ordenados:
        worker_mas_vacio = min(workers, key=itemgetter("carga_total"))
        worker_mas_vacio["departamentos"].append({
            "ubigeo": dep["ubigeo"],
            "nombre": dep["nombre"]
        })
        worker_mas_vacio["carga_total"] += dep["peso"]
        
    return workers

def main():
    NUM_WORKERS = 20
    
    print("="*60)
    print("ORQUESTADOR ONPE - ASIGNACIÓN DE CARGA")
    print("="*60)
    
    inicializar_sesion()
    
    pesos = estimar_peso_departamentos()
    if not pesos:
        print("\nAbortando ejecución. No se pudo obtener la data base.")
        return
        
    distribucion = distribuir_carga_lpt(pesos, NUM_WORKERS)
    
    print("\n" + "="*60)
    print("PLAN DE DISTRIBUCIÓN FINAL")
    print("="*60)
    
    for w in distribucion:
        nombres_deps = [d["nombre"] for d in w["departamentos"]]
        ubigeos_deps = [d["ubigeo"] for d in w["departamentos"]]
        
        print(f"\nWorker {w['id']} | Carga Total Asignada: {w['carga_total']} unidades")
        print(f"Departamentos: {', '.join(nombres_deps)}")
        
        parametro_ubigeos = ",".join(ubigeos_deps)
        print(f"-> Comando a ejecutar en EC2 Worker {w['id']}:")
        print(f"   python crawler.py --ubigeos {parametro_ubigeos}")

    with open("distribucion_cluster.json", "w", encoding="utf-8") as f:
        json.dump(distribucion, f, indent=2, ensure_ascii=False)
    print("\nPlan de distribución guardado en 'distribucion_cluster.json'")

if __name__ == "__main__":
    main()