#!/usr/bin/env bash
set -euxo pipefail

KAFKA_VERSION="4.3.1"
SCALA_VERSION="2.13"
KAFKA_HOME="/opt/kafka"
KAFKA_TGZ="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
KAFKA_URL="https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_TGZ}"
BROKER_HOST="$(hostname -I | awk '{print $1}')"
CLUSTER_ID="ONPEKafkaCluster01"

yum update -y
yum install -y java-11-amazon-corretto-headless wget tar nmap-ncat

cd /opt
wget -nv --tries=5 --timeout=60 "$KAFKA_URL"
tar -xzf "$KAFKA_TGZ"
mv "kafka_${SCALA_VERSION}-${KAFKA_VERSION}" kafka
rm -f "$KAFKA_TGZ"

mkdir -p /var/lib/kafka/kraft-combined-logs
chown -R ec2-user:ec2-user "$KAFKA_HOME" /var/lib/kafka

cat > "$KAFKA_HOME/config/server.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@${BROKER_HOST}:9093
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=PLAINTEXT://${BROKER_HOST}:9092
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=/var/lib/kafka/kraft-combined-logs
num.partitions=3
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
EOF

sudo -u ec2-user "$KAFKA_HOME/bin/kafka-storage.sh" format \
  --ignore-formatted \
  --cluster-id "$CLUSTER_ID" \
  --config "$KAFKA_HOME/config/server.properties"

cat > /etc/systemd/system/kafka.service <<EOF
[Unit]
Description=Apache Kafka KRaft broker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ec2-user
Group=ec2-user
Environment=JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto.x86_64
ExecStart=${KAFKA_HOME}/bin/kafka-server-start.sh ${KAFKA_HOME}/config/server.properties
ExecStop=${KAFKA_HOME}/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=10
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now kafka

cat > /home/ec2-user/.bash_profile <<EOF
export KAFKA_HOME=${KAFKA_HOME}
export PATH=\$KAFKA_HOME/bin:\$PATH
export KAFKA_BROKER=${BROKER_HOST}:9092
EOF
chown ec2-user:ec2-user /home/ec2-user/.bash_profile

cat > /home/ec2-user/kafka_demo.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

source /home/ec2-user/.bash_profile
TOPIC="onpe-votos-demo"
GROUP="grupo-consulta-onpe"

echo "Esperando a Kafka en $KAFKA_BROKER..."
until nc -z "${KAFKA_BROKER%:*}" "${KAFKA_BROKER#*:}"; do
  sleep 3
done

echo
echo "1) Topic y Partition: crear topic con 3 particiones"
kafka-topics.sh --bootstrap-server "$KAFKA_BROKER" \
  --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions 3 \
  --replication-factor 1
kafka-topics.sh --bootstrap-server "$KAFKA_BROKER" --describe --topic "$TOPIC"

echo
echo "2) Producer: publicar mensajes con llave para distribuirlos entre particiones"
cat <<MESSAGES | kafka-console-producer.sh \
  --bootstrap-server "$KAFKA_BROKER" \
  --topic "$TOPIC" \
  --property parse.key=true \
  --property key.separator=:
LIMA:mesa=001 votos=120
AREQUIPA:mesa=002 votos=95
CUSCO:mesa=003 votos=110
LIMA:mesa=004 votos=130
AREQUIPA:mesa=005 votos=102
CUSCO:mesa=006 votos=99
MESSAGES

echo
echo "3) Consumer: leer desde el inicio mostrando partition, offset, key y value"
kafka-console-consumer.sh \
  --bootstrap-server "$KAFKA_BROKER" \
  --topic "$TOPIC" \
  --from-beginning \
  --timeout-ms 10000 \
  --property print.partition=true \
  --property print.offset=true \
  --property print.key=true \
  --property key.separator=" | " || true

echo
echo "4) Consumer Group y Offset: consumir como grupo y consultar offsets confirmados"
kafka-console-consumer.sh \
  --bootstrap-server "$KAFKA_BROKER" \
  --topic "$TOPIC" \
  --group "$GROUP" \
  --from-beginning \
  --max-messages 6

kafka-consumer-groups.sh \
  --bootstrap-server "$KAFKA_BROKER" \
  --describe \
  --group "$GROUP"

echo
echo "Demo finalizado."
EOF

chmod +x /home/ec2-user/kafka_demo.sh
chown ec2-user:ec2-user /home/ec2-user/kafka_demo.sh
