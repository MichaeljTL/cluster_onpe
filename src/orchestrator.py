import os
import json
import re
import html
import subprocess
from operator import itemgetter
import requests
import boto3

# Importamos las variables de tu configuración
from config import REGION, USUARIO_SSH

# ==============================================================================
# CONFIGURACIÓN BASE
# ==============================================================================
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

# ==============================================================================
# 1. AUTO-DESCUBRIMIENTO EN AWS
# ==============================================================================
def obtener_ips_workers():
    """Consulta a AWS las IPs privadas de las instancias etiquetadas como HadoopWorker"""
    print("\n[+] Consultando a AWS por los nodos Workers activos...")
    try:
        ec2 = boto3.client('ec2', region_name=REGION) 
        respuesta = ec2.describe_instances(
            Filters=[
                {'Name': 'tag:Rol', 'Values': ['HadoopWorker']},
                {'Name': 'instance-state-name', 'Values': ['running']}
            ]
        )
        
        ips_workers = []
        for reservacion in respuesta['Reservations']:
            for instancia in reservacion['Instances']:
                ips_workers.append(instancia['PrivateIpAddress'])
                
        return ips_workers
    except Exception as e:
        print(f"Error al conectar con AWS: {e}")
        return []

# ==============================================================================
# 2. FUNCIONES DE EXTRACCIÓN ONPE
# ==============================================================================
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
        
        # Agregamos los prints de debug
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

# ==============================================================================
# 3. EJECUCIÓN PRINCIPAL Y DISPATCH REMOTO
# ==============================================================================
def main():
    print("="*60)
    print("ORQUESTADOR ONPE - DESPLIEGUE AUTOMÁTICO EN AWS")
    print("="*60)
    
    # 1. Ya no forzamos NUM_WORKERS, AWS nos dice cuántos hay encendidos
    lista_ips = obtener_ips_workers()
    NUM_WORKERS = len(lista_ips)
    
    if NUM_WORKERS == 0:
        print("\nError: No se encontraron Nodos Workers activos en AWS.")
        return

    print(f"\nSe encontraron {NUM_WORKERS} Workers listos para recibir órdenes.")
    
    inicializar_sesion()
    
    pesos = estimar_peso_departamentos()
    if not pesos:
        print("\nAbortando ejecución. No se pudo obtener la data base.")
        return
        
    distribucion = distribuir_carga_lpt(pesos, NUM_WORKERS)
    
    # Guardamos el plan localmente por si necesitamos auditarlo después
    with open("distribucion_cluster.json", "w", encoding="utf-8") as f:
        json.dump(distribucion, f, indent=2, ensure_ascii=False)
    
    print("\n" + "="*60)
    print("DISTRIBUYENDO CÓDIGO Y LANZANDO PROCESOS ")
    print("="*60)
    
    # 2. Bucle mágico: Conectamos a cada IP y le damos la orden
    for w, ip_worker in zip(distribucion, lista_ips):
        id_worker = w['id']
        nombres_deps = [d["nombre"] for d in w["departamentos"]]
        ubigeos_deps = [d["ubigeo"] for d in w["departamentos"]]
        parametro_ubigeos = ",".join(ubigeos_deps)
        
        print(f"\n[+] Worker {id_worker} | Carga: {w['carga_total']} | IP: {ip_worker}")
        print(f"    Asignación: {', '.join(nombres_deps)}")
        
        ruta_script_remota = f"/home/{USUARIO_SSH}/ONPE-CONSULTA/src/worker_descarga.py"
        
        # A) Crear carpeta en el worker (por si acaso no existe)
        subprocess.run(["ssh", "-o", "StrictHostKeyChecking=no", f"{USUARIO_SSH}@{ip_worker}", f"mkdir -p /home/{USUARIO_SSH}/ONPE-CONSULTA/src"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        # B) Copiarle el script al worker (garantiza que siempre corra tu última versión)
        print("    -> Sincronizando script...")
        subprocess.run(["scp", "-o", "StrictHostKeyChecking=no", "src/worker_descarga.py", f"{USUARIO_SSH}@{ip_worker}:{ruta_script_remota}"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        # C) Ejecutar el script en segundo plano activando el guardado en HDFS
        print("    -> Iniciando descarga masiva...")
        comando_ssh = [
            "ssh", "-o", "StrictHostKeyChecking=no", f"{USUARIO_SSH}@{ip_worker}",
            f"nohup python3 {ruta_script_remota} --ubigeos {parametro_ubigeos} --storage local > /home/{USUARIO_SSH}/worker_{id_worker}.log 2>&1 &"
        ]
        subprocess.Popen(comando_ssh)

    print("\n" + "="*60)
    print("¡TODOS LOS WORKERS HAN SIDO DESPLEGADOS!")
    print("Las máquinas están trabajando en segundo plano y subirán todo a Hadoop al terminar.")
    print("="*60)

if __name__ == "__main__":
    main()