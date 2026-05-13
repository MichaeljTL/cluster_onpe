import boto3
import time
import argparse
import sys
import os
import subprocess
import socket

# Importamos las variables globales desde nuestro archivo config.py
from config import REGION, AMI_ID, KEY_NAME, SECURITY_GROUP_ID, SUBNET_ID, TIPO_INSTANCIA, USUARIO_SSH

ec2_resource = boto3.resource('ec2', region_name=REGION)

def obtener_ip_privada_maestro():
    """Obtiene la IP privada de la instancia actual (Maestro)."""
    return socket.gethostbyname(socket.gethostname())

def iniciar_hadoop_en_maestro():
    """Ejecuta los comandos de inicio de Hadoop en el nodo Maestro."""
    ip_maestro = obtener_ip_privada_maestro()
    print("\nTodos los nodos responden. Auto-configurando el Maestro e iniciando servicios...")
    
    comandos = [
        # 1. CRÍTICO: Usar la IP privada real en lugar de 127.0.0.1 para que la red no colapse
        "sudo sed -i '/master/d' /etc/hosts", # Borra entradas antiguas
        f"sudo sh -c 'echo \"{ip_maestro} master\" >> /etc/hosts'",
        
        # 2. Asegurar que el Maestro tiene permisos para conectarse a sí mismo por SSH
        "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys",
        "chmod 600 ~/.ssh/authorized_keys",
        
        # 3. Cargar variables de entorno
        "source ~/.bashrc",

        # 4. CRÍTICO: Sobrescribir yarn-site.xml en el Maestro con el hack del disco
        """cat > /home/ec2-user/hadoop-3.3.6/etc/hadoop/yarn-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property><name>yarn.resourcemanager.hostname</name><value>master</value></property>
    <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
    <property><name>yarn.nodemanager.disk-health-checker.enable</name><value>false</value></property>
    <property><name>yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage</name><value>100.0</value></property>
</configuration>
EOF""",
        
        # 5. CRÍTICO: Crear la carpeta de logs y darle permisos 777 antes de iniciar YARN
        "mkdir -p /home/ec2-user/hadoop-3.3.6/logs/userlogs",
        "chmod 777 /home/ec2-user/hadoop-3.3.6/logs/userlogs",
        
        # 6. Formatear e iniciar
        "/home/ec2-user/hadoop-3.3.6/bin/hdfs namenode -format -nonInteractive || echo 'Ya formateado'",
        "/home/ec2-user/hadoop-3.3.6/sbin/start-dfs.sh",
        "/home/ec2-user/hadoop-3.3.6/sbin/start-yarn.sh"
    ]
    
    un_solo_comando = " && ".join(comandos)
    
    try:
        subprocess.run(["bash", "-c", un_solo_comando], check=True)
        print("Servicios iniciados. Revisa el estado con el comando 'jps'.")
    except subprocess.CalledProcessError as e:
        print(f"Error al iniciar Hadoop: {e}")

def levantar_workers(cantidad):
    """Crea la cantidad especificada de instancias EC2 y les inyecta el User Data."""
    ip_maestro = obtener_ip_privada_maestro()
    
    archivo_llave = "/home/ec2-user/.ssh/id_rsa"
    if not os.path.exists(archivo_llave):
        print("Generando llave SSH interna para el clúster...")
        subprocess.run(["ssh-keygen", "-t", "rsa", "-N", "", "-f", archivo_llave], check=True)
        
    with open(f"{archivo_llave}.pub", "r") as f:
        llave_publica_maestro = f.read().strip()

    # MODIFICACIÓN AQUÍ: Añadimos la IP del maestro al inicio del script para que el worker lo reconozca
    user_data_script = f"#!/bin/bash\necho '{ip_maestro} master' >> /etc/hosts\n" 
    
    # Continuación del script (tu código original)
    user_data_script += """
# ==========================================
# User Data para Nodos Hadoop (Master/Worker)
# ==========================================

# 1. Actualizar sistema e instalar dependencias
yum update -y
yum install -y java-11-amazon-corretto-devel wget tar python3 python3-pip git
pip3 install requests boto3

# 2. Descargar y extraer Hadoop
cd /home/ec2-user/
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzvf hadoop-3.3.6.tar.gz
rm hadoop-3.3.6.tar.gz

# 3. Configurar variables de entorno
cat << 'EOF' >> /home/ec2-user/.bashrc
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
    <property><name>dfs.replication</name><value>2</value></property>
    <property><name>dfs.namenode.name.dir</name><value>file:///home/ec2-user/hadoop-3.3.6/hdfs/namenode</value></property>
    <property><name>dfs.datanode.data.dir</name><value>file:///home/ec2-user/hadoop-3.3.6/hdfs/datanode</value></property>
    <property><name>dfs.namenode.http-address</name><value>0.0.0.0:9870</value></property>
</configuration>
EOF

cat > $HADOOP_ETC/mapred-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property><name>mapreduce.framework.name</name><value>yarn</value></property>
    <property><name>yarn.app.mapreduce.am.env</name><value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value></property>
    <property><name>mapreduce.map.env</name><value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value></property>
    <property><name>mapreduce.reduce.env</name><value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value></property>
</configuration>
EOF

cat > $HADOOP_ETC/yarn-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property><name>yarn.resourcemanager.hostname</name><value>master</value></property>
    <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
    
    <!-- HACK PARA DISCOS DE 15GB: Apagar sensor de pánico y forzar límite a 100% -->
    <property><name>yarn.nodemanager.disk-health-checker.enable</name><value>false</value></property>
    <property><name>yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage</name><value>100.0</value></property>
</configuration>
EOF



mkdir -p /home/ec2-user/hadoop-3.3.6/hdfs/namenode
mkdir -p /home/ec2-user/hadoop-3.3.6/hdfs/datanode

# CRÍTICO: Pre-crear la carpeta de logs con permisos absolutos
mkdir -p /home/ec2-user/hadoop-3.3.6/logs/userlogs
chmod 777 /home/ec2-user/hadoop-3.3.6/logs/userlogs

chown -R ec2-user:ec2-user /home/ec2-user/hadoop-3.3.6
chown ec2-user:ec2-user /home/ec2-user/.bashrc
"""

    # Inyectar la llave del maestro
    user_data_script += f"\n# Autorizar al Maestro\nmkdir -p /home/ec2-user/.ssh\n"
    user_data_script += f"echo '{llave_publica_maestro}' >> /home/ec2-user/.ssh/authorized_keys\n"
    user_data_script += f"chmod 600 /home/ec2-user/.ssh/authorized_keys\n"
    user_data_script += f"chown -R ec2-user:ec2-user /home/ec2-user/.ssh\n"

    print(f"Solicitando {cantidad} Workers. Master IP: {ip_maestro}")
    
    instancias = ec2_resource.create_instances(
        ImageId=AMI_ID,
        MinCount=cantidad,
        MaxCount=cantidad,
        InstanceType=TIPO_INSTANCIA,
        KeyName=KEY_NAME,
        SecurityGroupIds=[SECURITY_GROUP_ID],
        SubnetId=SUBNET_ID,
        UserData=user_data_script,
        BlockDeviceMappings=[
            {
                'DeviceName': '/dev/xvda',
                'Ebs': {
                    'VolumeSize': 15,      # GB
                    'VolumeType': 'gp3',
                    'DeleteOnTermination': True
                }
            }
        ],
        TagSpecifications=[{'ResourceType': 'instance', 'Tags': [{'Key': 'Name', 'Value': 'Worker-ONPE'}, {'Key': 'Rol', 'Value': 'HadoopWorker'}]}]
    )
    
    # Registro en el archivo 'workers' del Maestro
    path_workers_file = "/home/ec2-user/hadoop-3.3.6/etc/hadoop/workers"
    with open(path_workers_file, "w") as f:
        f.write("localhost\n")

    for instancia in instancias:
        instancia.wait_until_running()
        instancia.reload()
        ip_privada = instancia.private_ip_address
        with open(path_workers_file, "a") as f:
            f.write(f"{ip_privada}\n")
        print(f"  -> Nodo configurado y registrado: {ip_privada}")

    print("\nClúster desplegado. Pasos finales en el Maestro:")
    print("1. hdfs namenode -format")
    print("2. start-dfs.sh && start-yarn.sh")

def verificar_conexion_ssh():
    """Verifica conexión y si todos están listos, inicia Hadoop."""
    instancias_workers = ec2_resource.instances.filter(
        Filters=[{'Name': 'tag:Rol', 'Values': ['HadoopWorker']}, {'Name': 'instance-state-name', 'Values': ['running']}]
    )
    
    ips_workers = [instancia.private_ip_address for instancia in instancias_workers]
    if not ips_workers:
        print("No hay Workers activos.")
        return

    nodos_listos = 0
    print(f"Verificando {len(ips_workers)} Workers...")

    for ip in ips_workers:
        comando_ssh = ["ssh", "-o", "StrictHostKeyChecking=no", f"{USUARIO_SSH}@{ip}", "ls /home/ec2-user/hadoop-3.3.6/bin/hadoop"]
        try:
            resultado = subprocess.run(comando_ssh, capture_output=True, text=True, timeout=10)
            if resultado.returncode == 0:
                print(f"  -> {ip}: LISTO")
                nodos_listos += 1
            else:
                print(f"  -> {ip}: Instalando todavía...")
        except Exception:
            print(f"  -> {ip}: Sin respuesta.")

    # SI TODOS LOS NODOS ESTÁN LISTOS, INICIAMOS HADOOP
    if nodos_listos == len(ips_workers):
        iniciar_hadoop_en_maestro()
    else:
        print(f"\nFaltan {len(ips_workers) - nodos_listos} nodos por estar listos. Vuelve a intentar en un momento.")

def detener_instancias():
    """Pausa (Stop) las instancias de AWS para no generar costos sin borrarlas."""
    instancias = ec2_resource.instances.filter(
        Filters=[{'Name': 'tag:Rol', 'Values': ['HadoopWorker']}, {'Name': 'instance-state-name', 'Values': ['running']}]
    )
    ids = [i.id for i in instancias]
    if ids:
        print(f"Pausando {len(ids)} Workers... (Dejarán de cobrar computo)")
        ec2_resource.instances.filter(InstanceIds=ids).stop()
    else:
        print("No hay Workers encendidos para pausar.")

def reanudar_instancias():
    """Reanuda (Start) las instancias pausadas."""
    instancias = ec2_resource.instances.filter(
        Filters=[{'Name': 'tag:Rol', 'Values': ['HadoopWorker']}, {'Name': 'instance-state-name', 'Values': ['stopped']}]
    )
    ids = [i.id for i in instancias]
    if ids:
        print(f"Encendiendo {len(ids)} Workers previamente pausados...")
        ec2_resource.instances.filter(InstanceIds=ids).start()
        print("Espera unos minutos a que arranquen y vuelve a correr '--check_ssh'.")
    else:
        print("No hay Workers pausados.")

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
    grupo.add_argument('--delete', action='store_true', help="Borrar (Terminate) todos los Nodos Workers creados")
    grupo.add_argument('--check_ssh', action='store_true', help="Monitor en bucle para iniciar Hadoop")
    
    grupo.add_argument('--pause', action='store_true', help="Pausa (Stop) las instancias EC2 para ahorrar dinero")
    grupo.add_argument('--resume', action='store_true', help="Reanuda (Start) las instancias EC2 pausadas")
    
    args = parser.parse_args()

    # 2. CONECTAMOS LOS ARGUMENTOS CON SUS FUNCIONES
    if args.delete:
        destruir_workers()
    elif args.check_ssh:
        verificar_conexion_ssh()
    elif args.pause:
        detener_instancias()   
    elif args.resume:
        reanudar_instancias() 
    elif args.start_nodes is not None:
        if args.start_nodes <= 0:
            print("Error: El número de nodos debe ser mayor a 0.")
            sys.exit(1)
        levantar_workers(args.start_nodes)

if __name__ == "__main__":
    main()