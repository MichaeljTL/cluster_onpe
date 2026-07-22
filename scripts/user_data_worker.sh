#!/usr/bin/env bash
set -euxo pipefail

KAFKA_VERSION="3.9.2"
SCALA_VERSION="2.13"
KAFKA_HOME="/opt/kafka"
KAFKA_TGZ="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
KAFKA_URL="https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_TGZ}"

if [ -z "${KAFKA_BROKER_IP:-}" ]; then
  echo "KAFKA_BROKER_IP no fue definido por src/levantar_kafka.py" >&2
  exit 1
fi

yum update -y
yum install -y java-11-amazon-corretto-headless wget tar nmap-ncat

cd /opt
wget -nv --tries=5 --timeout=60 "$KAFKA_URL"
tar -xzf "$KAFKA_TGZ"
mv "kafka_${SCALA_VERSION}-${KAFKA_VERSION}" kafka
rm -f "$KAFKA_TGZ"
chown -R ec2-user:ec2-user "$KAFKA_HOME"

cat > /home/ec2-user/.bash_profile <<EOF
export KAFKA_HOME=${KAFKA_HOME}
export PATH=\$KAFKA_HOME/bin:\$PATH
export KAFKA_BROKER=${KAFKA_BROKER_IP}:9092
EOF
chown ec2-user:ec2-user /home/ec2-user/.bash_profile

cat > /home/ec2-user/kafka_client_demo.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

source /home/ec2-user/.bash_profile
TOPIC="${1:-onpe-votos-demo}"
GROUP="${2:-grupo-workers-onpe}"

echo "Probando conexion a $KAFKA_BROKER..."
until nc -z "${KAFKA_BROKER%:*}" "${KAFKA_BROKER#*:}"; do
  sleep 3
done

echo "Topics disponibles:"
kafka-topics.sh --bootstrap-server "$KAFKA_BROKER" --list

echo
echo "Consumer del worker en el grupo $GROUP. Usa Ctrl+C para detenerlo."
kafka-console-consumer.sh \
  --bootstrap-server "$KAFKA_BROKER" \
  --topic "$TOPIC" \
  --group "$GROUP" \
  --property print.partition=true \
  --property print.offset=true \
  --property print.key=true \
  --property key.separator=" | "
EOF

chmod +x /home/ec2-user/kafka_client_demo.sh
chown ec2-user:ec2-user /home/ec2-user/kafka_client_demo.sh
