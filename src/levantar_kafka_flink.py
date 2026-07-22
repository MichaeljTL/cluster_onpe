import argparse
import sys
from datetime import datetime
from pathlib import Path

import boto3

from config import AMI_ID, KEY_NAME, REGION, SECURITY_GROUP_ID, SUBNET_ID, TIPO_INSTANCIA

PROJECT_TAG = "ONPE-Kafka-Flink-Lab7"
ROOT_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT_DIR / "scripts"


ec2_resource = boto3.resource("ec2", region_name=REGION)


def leer_script(nombre):
    path = SCRIPTS_DIR / nombre
    if not path.exists():
        raise FileNotFoundError(f"No existe el script requerido: {path}")
    return path.read_text(encoding="utf-8")


def tags_base(nombre, rol, run_id, indice=None):
    tags = [
        {"Key": "Name", "Value": nombre},
        {"Key": "Proyecto", "Value": PROJECT_TAG},
        {"Key": "Rol", "Value": rol},
        {"Key": "ClusterRun", "Value": run_id},
    ]
    if indice is not None:
        tags.append({"Key": "Nodo", "Value": str(indice)})
    return tags


def crear_master(run_id):
    user_data = leer_script("user_data_kafka_flink_master.sh")
    nombre = f"Lab7-Kafka-Flink-Master-{run_id}"
    print(f"Creando instancia EC2 para {nombre}...")

    instancias = ec2_resource.create_instances(
        ImageId=AMI_ID,
        MinCount=1,
        MaxCount=1,
        InstanceType=TIPO_INSTANCIA,
        KeyName=KEY_NAME,
        SecurityGroupIds=[SECURITY_GROUP_ID],
        SubnetId=SUBNET_ID,
        UserData=user_data,
        BlockDeviceMappings=[
            {
                "DeviceName": "/dev/xvda",
                "Ebs": {
                    "VolumeSize": 25,
                    "VolumeType": "gp3",
                    "DeleteOnTermination": True,
                },
            }
        ],
        TagSpecifications=[
            {
                "ResourceType": "instance",
                "Tags": tags_base(nombre, "KafkaFlinkMaster", run_id, indice="master"),
            }
        ],
    )

    master = instancias[0]
    master.wait_until_running()
    master.reload()
    print(f"Master listo: {master.id} | Name={nombre}")
    print(f"IP privada broker: {master.private_ip_address}")
    print(f"DNS publico master: {master.public_dns_name}")
    return master


def crear_worker(indice, broker_ip, run_id):
    worker_base = leer_script("user_data_kafka_flink_worker.sh")
    user_data = f"#!/usr/bin/env bash\nexport KAFKA_BROKER_IP='{broker_ip}'\n" + "\n".join(
        worker_base.splitlines()[1:]
    )
    nombre = f"Lab7-Kafka-Flink-Worker-{indice}-{run_id}"

    print(f"Creando instancia EC2 para {nombre}...")
    instancias = ec2_resource.create_instances(
        ImageId=AMI_ID,
        MinCount=1,
        MaxCount=1,
        InstanceType=TIPO_INSTANCIA,
        KeyName=KEY_NAME,
        SecurityGroupIds=[SECURITY_GROUP_ID],
        SubnetId=SUBNET_ID,
        UserData=user_data,
        BlockDeviceMappings=[
            {
                "DeviceName": "/dev/xvda",
                "Ebs": {
                    "VolumeSize": 12,
                    "VolumeType": "gp3",
                    "DeleteOnTermination": True,
                },
            }
        ],
        TagSpecifications=[
            {
                "ResourceType": "instance",
                "Tags": tags_base(nombre, "KafkaFlinkWorker", run_id, indice=indice),
            }
        ],
    )

    worker = instancias[0]
    worker.wait_until_running()
    worker.reload()
    print(f"  -> Worker listo: {worker.id} | Name={nombre} | IP privada: {worker.private_ip_address}")
    return worker


def levantar_cluster(cantidad_workers):
    run_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    print("La instancia actual actua como controlador/orquestador.")
    print(
        f"Se crearan {cantidad_workers + 1} instancia(s) EC2 nuevas: "
        f"1 master Kafka+Flink y {cantidad_workers} worker(s) cliente."
    )
    print(f"Identificador del cluster: {run_id}")

    master = crear_master(run_id)
    workers = [crear_worker(i, master.private_ip_address, run_id) for i in range(1, cantidad_workers + 1)]

    print("\nCluster Kafka + Flink solicitado correctamente.")
    print("Espera unos minutos a que cloud-init termine la instalacion.")
    print("\nConectate al master:")
    print(f"  ssh -i ~/.ssh/{KEY_NAME}.pem ec2-user@{master.public_dns_name}")
    print("\nRevisa instalacion:")
    print("  tail -f /var/log/cloud-init-output.log")
    print("\nEjecuta la demostracion completa del laboratorio 7:")
    print("  /home/ec2-user/lab7_kafka_flink.sh")
    print("\nConceptos demostrados: Producer, Topic, Partition, Offset, Consumer Group y Flink DataStream.")

    if workers:
        print("\nOpcional en un worker, para ver consumidores Kafka adicionales:")
        print("  /home/ec2-user/lab7_worker_consumer.sh eventos-lab7 grupo-workers-lab7")

    ids = [master.id] + [worker.id for worker in workers]
    print("\nPara borrar estas instancias:")
    print("  python3 src/levantar_kafka_flink.py --delete")
    print(f"\nIDs creados: {' '.join(ids)}")


def listar_instancias():
    return list(
        ec2_resource.instances.filter(
            Filters=[
                {"Name": "tag:Proyecto", "Values": [PROJECT_TAG]},
                {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
            ]
        )
    )


def verificar_cluster():
    instancias = listar_instancias()
    if not instancias:
        print("No hay instancias Kafka + Flink creadas por este script.")
        return

    print("Instancias Kafka + Flink Lab 7:")
    for instancia in instancias:
        instancia.reload()
        tags = {tag["Key"]: tag["Value"] for tag in instancia.tags or []}
        print(
            f"  -> {instancia.id} | Name={tags.get('Name', 'SinName')} | "
            f"Rol={tags.get('Rol', 'SinRol')} | Run={tags.get('ClusterRun', '-')} | "
            f"{instancia.state['Name']} | privada={instancia.private_ip_address} | publica={instancia.public_dns_name}"
        )


def destruir_cluster():
    instancias = listar_instancias()
    ids = [instancia.id for instancia in instancias]
    if not ids:
        print("No hay instancias Kafka + Flink Lab 7 para terminar.")
        return

    print(f"Terminando {len(ids)} instancia(s) Kafka + Flink Lab 7: {' '.join(ids)}")
    ec2_resource.instances.filter(InstanceIds=ids).terminate()


def main():
    parser = argparse.ArgumentParser(description="Gestor de infraestructura Kafka + Flink en EC2")
    grupo = parser.add_mutually_exclusive_group(required=True)
    grupo.add_argument("--start_nodes", type=int, metavar="N", help="Crea 1 master Kafka+Flink y N workers cliente")
    grupo.add_argument("--check", action="store_true", help="Lista las instancias Kafka + Flink creadas")
    grupo.add_argument("--delete", action="store_true", help="Termina las instancias Kafka + Flink creadas")

    args = parser.parse_args()

    if args.start_nodes is not None:
        if args.start_nodes < 0:
            print("Error: el numero de workers no puede ser negativo.")
            sys.exit(1)
        levantar_cluster(args.start_nodes)
    elif args.check:
        verificar_cluster()
    elif args.delete:
        destruir_cluster()


if __name__ == "__main__":
    main()
