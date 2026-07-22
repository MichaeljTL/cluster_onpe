#!/usr/bin/env bash
set -euxo pipefail

# User Data para la instancia controladora del Laboratorio 07.
# Esta instancia se usa para clonar el proyecto y ejecutar boto3.

yum update -y
yum install -y \
  git \
  python3 \
  python3-pip \
  unzip \
  tar \
  wget \
  curl \
  nano \
  vim \
  tree \
  jq

python3 -m pip install --upgrade pip
python3 -m pip install boto3 botocore

mkdir -p /home/ec2-user/lab7
chown -R ec2-user:ec2-user /home/ec2-user/lab7

cat > /home/ec2-user/.bash_profile <<'EOF'
export PATH=$HOME/.local/bin:$PATH
export AWS_DEFAULT_REGION=us-east-1
EOF

cat > /home/ec2-user/README_LAB7_CONTROLADOR.txt <<'EOF'
Instancia controladora lista para el Laboratorio 07 Kafka + Flink.

1. Clonar el repositorio:
   git clone URL_DEL_REPOSITORIO

2. Entrar al proyecto:
   cd onpe-consulta

3. Revisar configuracion AWS:
   nano src/config.py

4. Levantar el laboratorio:
   python3 src/levantar_kafka_flink.py --start_nodes 1

5. Ver instancias creadas:
   python3 src/levantar_kafka_flink.py --check

6. Borrar instancias al finalizar:
   python3 src/levantar_kafka_flink.py --delete
EOF

chown ec2-user:ec2-user /home/ec2-user/.bash_profile /home/ec2-user/README_LAB7_CONTROLADOR.txt

echo "Controlador Lab 7 listo: git, python3, pip, boto3 y utilidades instaladas."
