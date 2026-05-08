import boto3
import time
import argparse
import sys
import os
import subprocess


# Importamos las variables globales desde nuestro archivo config.py
from config import REGION, AMI_ID, KEY_NAME, SECURITY_GROUP_ID, SUBNET_ID, TIPO_INSTANCIA, USUARIO_SSH

ec2_resource = boto3.resource('ec2', region_name=REGION)

def levantar_workers(cantidad):
    """Crea la cantidad especificada de instancias EC2 y les inyecta el User Data."""

    # =========================================================
    # [NUEVA LÓGICA] Generar y leer la llave SSH del Maestro
    # =========================================================
    archivo_llave = "/home/ec2-user/.ssh/id_rsa"
    
    # Si el Maestro no tiene llave propia, la crea sin pedir contraseña
    if not os.path.exists(archivo_llave):
        print("🔑 Generando llave SSH interna para el clúster...")
        subprocess.run(["ssh-keygen", "-t", "rsa", "-N", "", "-f", archivo_llave], check=True)
        
    # Leemos la llave pública del Maestro
    with open(f"{archivo_llave}.pub", "r") as f:
        llave_publica_maestro = f.read().strip()
    # =========================================================

    # Tu User Data original (sin usar f-strings para no romper el bash)
    
    user_data_script = """#!/bin/bash
# ==========================================
# User Data para Nodos Hadoop (Master/Worker)
# ==========================================

# 1. Actualizar sistema e instalar dependencias (¡Git añadido!)
yum update -y
yum install -y java-11-amazon-corretto-devel wget tar python3 python3-pip git

# 1.1 Instalar librerías de Python necesarias para el clúster
# (argparse y subprocess ya vienen por defecto en Python, no usan pip)
pip3 install requests boto3

# 2. Descargar y extraer Hadoop
cd /home/ec2-user/
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzvf hadoop-3.3.6.tar.gz
rm hadoop-3.3.6.tar.gz

# 3. Configurar variables de entorno
cat << 'EOF' >> /home/ec2-user/.bashrc

# Hadoop & Java Variables
export JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto.x86_64
export PATH=$JAVA_HOME/bin:$PATH
export HADOOP_HOME=/home/ec2-user/hadoop-3.3.6
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
EOF

echo 'export JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto.x86_64' >> /home/ec2-user/hadoop-3.3.6/etc/hadoop/hadoop-env.sh

# 4. Configurar archivos XML de Hadoop
HADOOP_ETC=/home/ec2-user/hadoop-3.3.6/etc/hadoop

cat > $HADOOP_ETC/core-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://master:9000</value>
    </property>
</configuration>
EOF

cat > $HADOOP_ETC/hdfs-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>2</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///home/ec2-user/hadoop-3.3.6/hdfs/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///home/ec2-user/hadoop-3.3.6/hdfs/datanode</value>
    </property>
    <property>
        <name>dfs.namenode.http-address</name>
        <value>0.0.0.0:9870</value>
    </property>
</configuration>
EOF

cat > $HADOOP_ETC/mapred-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.map.memory.mb</name>
        <value>512</value>
    </property>
    <property>
        <name>mapreduce.reduce.memory.mb</name>
        <value>512</value>
    </property>
    <property>
        <name>mapreduce.map.cpu.vcores</name>
        <value>1</value>
    </property>
    <property>
        <name>mapreduce.reduce.cpu.vcores</name>
        <value>1</value>
    </property>
    <property>
        <name>yarn.app.mapreduce.am.env</name>
        <value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
    </property>
    <property>
        <name>mapreduce.map.env</name>
        <value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
    </property>
    <property>
        <name>mapreduce.reduce.env</name>
        <value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
    </property>
</configuration>
EOF

cat > $HADOOP_ETC/yarn-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>master</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>2048</value>
    </property>
    <property>
        <name>yarn.nodemanager.resource.cpu-vcores</name>
        <value>2</value>
    </property>
</configuration>
EOF

# 5. Crear carpetas de HDFS
mkdir -p /home/ec2-user/hadoop-3.3.6/hdfs/namenode
mkdir -p /home/ec2-user/hadoop-3.3.6/hdfs/datanode

# 6. Cambiar el propietario a ec2-user
chown -R ec2-user:ec2-user /home/ec2-user/hadoop-3.3.6
chown ec2-user:ec2-user /home/ec2-user/.bashrc
"""

    # =========================================================
    # [NUEVA LÓGICA] Inyectar la llave al final del script bash
    # =========================================================
    user_data_script += f"\n# Autorizar al Maestro\n"
    user_data_script += f"echo '{llave_publica_maestro}' >> /home/ec2-user/.ssh/authorized_keys\n"
    user_data_script += f"chmod 600 /home/ec2-user/.ssh/authorized_keys\n"
    user_data_script += f"chown ec2-user:ec2-user /home/ec2-user/.ssh/authorized_keys\n"
    # =========================================================

    print(f"Solicitando {cantidad} instancias Worker a AWS...")
    
    instancias = ec2_resource.create_instances(
        ImageId=AMI_ID,
        MinCount=cantidad,
        MaxCount=cantidad,
        InstanceType=TIPO_INSTANCIA,
        KeyName=KEY_NAME,
        SecurityGroupIds=[SECURITY_GROUP_ID],
        SubnetId=SUBNET_ID,
        UserData=user_data_script,
        TagSpecifications=[
            {
                'ResourceType': 'instance',
                'Tags': [
                    {'Key': 'Name', 'Value': 'Worker-ONPE'},
                    {'Key': 'Rol', 'Value': 'HadoopWorker'}
                ]
            }
        ]
    )
    
    print("Instancias creadas. Esperando a que AWS las encienda...")
    for instancia in instancias:
        instancia.wait_until_running()
        instancia.reload()
        print(f"  -> Worker en línea - IP Privada: {instancia.private_ip_address}")
        
    print("\nLas instancias están encendidas, pero la instalación de Hadoop (User Data) toma unos 3 minutos.")
    print("Puedes usar el comando '--check_ssh' más tarde para verificar cuándo estén listas.")

def verificar_conexion_ssh():
    """Busca los workers activos e intenta una conexión SSH para validar si ya terminaron de instalar todo."""
    print("Buscando Nodos Workers activos para probar la conexión SSH...")
    
    instancias_workers = ec2_resource.instances.filter(
        Filters=[
            {'Name': 'tag:Rol', 'Values': ['HadoopWorker']},
            {'Name': 'instance-state-name', 'Values': ['running']}
        ]
    )
    
    ips_workers = [instancia.private_ip_address for instancia in instancias_workers]
    
    if not ips_workers:
        print("No se encontraron Nodos Workers en ejecución.")
        return

    print(f"Se encontraron {len(ips_workers)} Workers. Iniciando pruebas de enlace...\n")

    for ip in ips_workers:
        print(f"  [SSH] Conectando a {ip}...")
        
        comando_ssh = [
            "ssh", "-o", "StrictHostKeyChecking=no", f"{USUARIO_SSH}@{ip}",
            "echo 'SSH exitoso' && echo 'Version de Hadoop:' && /home/ec2-user/hadoop-3.3.6/bin/hadoop version | head -n 1"
        ]
        
        try:
            resultado = subprocess.run(comando_ssh, capture_output=True, text=True, timeout=15)
            if resultado.returncode == 0:
                # Si todo sale bien, imprimimos el resultado limpio
                salida_limpia = resultado.stdout.replace('\n', ' - ')
                print(f"    -> {salida_limpia}")
            else:
                print(f"    -> Aún no responde correctamente (Puede que el User Data siga instalando).")
        except subprocess.TimeoutExpired:
            print(f"    -> Tiempo de espera agotado. El nodo no responde por SSH.")
        except Exception as e:
            print(f"    -> Falla inesperada: {e}")

def destruir_workers():
    """Busca todas las instancias con la etiqueta 'Rol: HadoopWorker' y las destruye (Terminate)."""
    print("Buscando Nodos Workers en el clúster para destruirlos...")
    
    instancias_workers = ec2_resource.instances.filter(
        Filters=[
            {'Name': 'tag:Rol', 'Values': ['HadoopWorker']},
            {'Name': 'instance-state-name', 'Values': ['running', 'pending', 'stopped', 'stopping']}
        ]
    )
    
    ids_a_borrar = [instancia.id for instancia in instancias_workers]
    
    if not ids_a_borrar:
        print("No se encontraron Nodos Workers activos. El clúster ya está limpio.")
        return

    print(f"Se encontrarón {len(ids_a_borrar)} Workers. Procediendo a eliminarlos...")
    ec2_resource.instances.filter(InstanceIds=ids_a_borrar).terminate()
    
    for instance_id in ids_a_borrar:
        print(f"  -> Terminando instancia: {instance_id}")
        
    print("\nOrden de destrucción enviada. Las instancias desaparecerán de AWS en unos minutos.")

def main():
    parser = argparse.ArgumentParser(description="Gestor de Infraestructura del Clúster ONPE en AWS")
    
    # Grupo mutuamente exclusivo para que solo se pueda elegir una acción a la vez
    grupo = parser.add_mutually_exclusive_group(required=True)
    grupo.add_argument('--start_nodes', type=int, metavar='N', help="Número de Nodos Workers a levantar")
    grupo.add_argument('--delete', action='store_true', help="Borrar todos los Nodos Workers creados")
    grupo.add_argument('--check_ssh', action='store_true', help="Verifica el enlace SSH y la versión de Hadoop en todos los nodos")
    
    args = parser.parse_args()

    if args.delete:
        destruir_workers()
    elif args.check_ssh:
        verificar_conexion_ssh()
    elif args.start_nodes is not None:
        if args.start_nodes <= 0:
            print("Error: El número de nodos debe ser mayor a 0.")
            sys.exit(1)
        levantar_workers(args.start_nodes)

if __name__ == "__main__":
    main()