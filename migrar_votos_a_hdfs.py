import subprocess
import boto3
import argparse

# Importamos las variables globales de tu entorno
from config import REGION, USUARIO_SSH

def obtener_ips_workers():
    """Consulta a AWS las IPs privadas de las instancias etiquetadas como HadoopWorker"""
    print("Consultando a AWS por los nodos Workers activos...")
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

def migrar_votos_especificos(ips, directorio_local, hdfs_base):
    if not ips:
        print("No hay workers activos para migrar.")
        return

    print("\n" + "="*60)
    print("INICIANDO EXTRACCIÓN DE VOTOS HACIA HDFS")
    print("="*60)

    # 1. Crear la carpeta destino centralizada en HDFS
    carpeta_destino_hdfs = f"{hdfs_base}/votos_consolidados"
    print(f"-> Preparando carpeta destino en HDFS: {carpeta_destino_hdfs}")
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", carpeta_destino_hdfs], check=False)

    for i, ip in enumerate(ips):
        worker_id = f"worker_{i+1}"
        
        print(f"\n[+] Conectando al Worker {worker_id} ({ip})...")
        
        # El comando que ejecutaremos dentro del Worker
        # 1. Busca el archivo jsonl en cualquier subcarpeta dentro de data_workers
        # 2. Si lo encuentra, lo sube al HDFS renombrándolo
        comando_worker = f"""
        source ~/.bashrc &&
        ARCHIVO=$(find {directorio_local} -name "actas_detalle_votos.jsonl" | head -n 1) &&
        if [ ! -z "$ARCHIVO" ]; then
            hdfs dfs -put -f "$ARCHIVO" "{carpeta_destino_hdfs}/votos_{worker_id}.jsonl" && echo "Exito:$ARCHIVO"
        else
            echo "Archivo no encontrado"
        fi
        """
        
        # Ejecutamos el comando remotamente vía SSH
        comando_ssh = ["ssh", "-o", "StrictHostKeyChecking=no", f"{USUARIO_SSH}@{ip}", comando_worker]
        
        try:
            resultado = subprocess.run(comando_ssh, capture_output=True, text=True)
            
            if "Exito" in resultado.stdout:
                # Extraemos la ruta real que encontró para mostrarla en el log
                ruta_encontrada = resultado.stdout.split("Exito:")[1].strip()
                print(f"    Votos encontrados en: {ruta_encontrada}")
                print(f"    Archivo subido a HDFS como: {carpeta_destino_hdfs}/votos_{worker_id}.jsonl")
            elif "Archivo no encontrado" in resultado.stdout:
                print(f"    No se encontró 'actas_detalle_votos.jsonl' en {directorio_local}.")
            else:
                print(f"    Error subiendo datos de {worker_id}:")
                print(resultado.stderr)
                
        except Exception as e:
            print(f"    Falla de conexión con {worker_id}: {e}")

    print("\n" + "="*60)
    print("MIGRACIÓN FINALIZADA")
    print(f"Puedes verificar ejecutando: hdfs dfs -ls {carpeta_destino_hdfs}")

def main():
    parser = argparse.ArgumentParser(description="Sube SOLO los archivos de votos de los Workers al HDFS")
    
    parser.add_argument(
        "--local-dir", 
        default=f"/home/{USUARIO_SSH}/data_workers", 
        help="La carpeta base en el disco duro del worker (ej: /home/ec2-user/data_workers)"
    )
    
    parser.add_argument(
        "--hdfs-base", 
        default="/onpe", 
        help="La carpeta destino en HDFS (ej: /onpe)"
    )
    
    args = parser.parse_args()
    
    ips = obtener_ips_workers()
    migrar_votos_especificos(ips, args.local_dir, args.hdfs_base)

if __name__ == "__main__":
    main()