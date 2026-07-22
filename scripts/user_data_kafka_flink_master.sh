#!/usr/bin/env bash
set -euxo pipefail

KAFKA_VERSION="4.3.1"
KAFKA_SCALA_VERSION="2.13"
KAFKA_HOME="/opt/kafka"
KAFKA_TGZ="kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}.tgz"
KAFKA_URL="https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_TGZ}"
BROKER_HOST="$(hostname -I | awk '{print $1}')"
CLUSTER_ID="ONPEKafkaFlinkLab07"

yum update -y
yum install -y java-11-amazon-corretto-headless wget tar nmap-ncat python3 maven

cd /opt
wget -nv --tries=5 --timeout=60 "$KAFKA_URL"
tar -xzf "$KAFKA_TGZ"
mv "kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}" kafka
rm -f "$KAFKA_TGZ"


mkdir -p /var/lib/kafka/kraft-combined-logs /home/ec2-user/lab7/flink-kafka-lab7/src/main/java/edu/unsa/bigdata
chown -R ec2-user:ec2-user "$KAFKA_HOME" /var/lib/kafka /home/ec2-user/lab7

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
export JAVA_HOME=/usr/lib/jvm/java-11-amazon-corretto.x86_64
export KAFKA_HOME=${KAFKA_HOME}
export PATH=\$KAFKA_HOME/bin:\$PATH
export KAFKA_BROKER=${BROKER_HOST}:9092
EOF
chown ec2-user:ec2-user /home/ec2-user/.bash_profile

cat > /home/ec2-user/lab7/productor_eventos.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

source /home/ec2-user/.bash_profile
TOPIC="${1:-eventos-lab7}"
eventos=("VIEW_PRODUCT" "SEARCH" "ADD_CART" "PURCHASE" "VIEW_PRODUCT" "VIEW_PRODUCT")
productos=("Laptop Lenovo:Electronics:3200" "Laptop Dell:Electronics:3500" "Mouse Logitech:Electronics:120" "Silla Ergonomica:Office:650" "Monitor Samsung:Electronics:900" "Audifonos Sony:Electronics:450")
ciudades=("Arequipa" "Lima" "Cusco" "Tacna")
contador=1

while true; do
  for item in "${productos[@]}"; do
    IFS=":" read -r producto categoria precio <<< "$item"
    evento="${eventos[$((contador % ${#eventos[@]}))]}"
    ciudad="${ciudades[$((contador % ${#ciudades[@]}))]}"
    user_id="$(printf 'USR%03d' "$((100 + contador))")"
    timestamp="$(date '+%Y-%m-%dT%H:%M:%S')"
    printf '{"user":"%s","event":"%s","product":"%s","category":"%s","city":"%s","price":%d,"timestamp":"%s"}\n' \
      "$user_id" "$evento" "$producto" "$categoria" "$ciudad" "$precio" "$timestamp"
    contador=$((contador + 1))
    sleep 1
  done
done | kafka-console-producer.sh --bootstrap-server "$KAFKA_BROKER" --topic "$TOPIC"
EOF

cat > /home/ec2-user/lab7/flink-kafka-lab7/pom.xml <<'EOF'
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>edu.unsa.bigdata</groupId>
  <artifactId>lab7-flink-kafka</artifactId>
  <version>1.0.0</version>
  <properties>
    <maven.compiler.source>11</maven.compiler.source>
    <maven.compiler.target>11</maven.compiler.target>
    <flink.version>1.19.1</flink.version>
  </properties>
  <dependencies>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-streaming-java</artifactId>
      <version>${flink.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-clients</artifactId>
      <version>${flink.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-connector-kafka</artifactId>
      <version>3.2.0-1.19</version>
    </dependency>
    <dependency>
      <groupId>com.fasterxml.jackson.core</groupId>
      <artifactId>jackson-databind</artifactId>
      <version>2.17.1</version>
    </dependency>
  </dependencies>
  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.5.3</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
              <transformers>
                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                  <mainClass>edu.unsa.bigdata.Lab7KafkaFlink</mainClass>
                </transformer>
              </transformers>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
EOF

cat > /home/ec2-user/lab7/flink-kafka-lab7/src/main/java/edu/unsa/bigdata/Lab7KafkaFlink.java <<'EOF'
package edu.unsa.bigdata;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Lab7KafkaFlink {
    public static void main(String[] args) throws Exception {
        String broker = System.getenv().getOrDefault("KAFKA_BROKER", "localhost:9092");
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "eventos-lab7");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(broker)
                .setTopics(topic)
                .setGroupId("grupo-flink-lab7")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<Evento> eventos = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka eventos")
                .map(new JsonToEvento());

        eventos.map(e -> "RECIBIDO | " + e.event + " | " + e.product + " | " + e.user).print();

        eventos
                .filter((FilterFunction<Evento>) e -> e.event.equals("PURCHASE") || e.event.equals("ADD_CART"))
                .map(e -> "FILTRADO | " + e.event + " | " + e.product + " | " + e.user)
                .print();

        eventos
                .map(new EnriquecerEvento())
                .map(e -> "TRANSFORMADO | " + e.event + " | hora=" + e.hour + " dia=" + e.day
                        + " mes=" + e.month + " finDeSemana=" + e.weekend)
                .print();

        eventos
                .map(e -> new ConteoEvento(e.event, 1))
                .keyBy(c -> c.event)
                .reduce((ReduceFunction<ConteoEvento>) (a, b) -> new ConteoEvento(a.event, a.total + b.total))
                .map(c -> "CONTEO_EVENTOS | " + c.event + " = " + c.total)
                .print();

        eventos
                .filter((FilterFunction<Evento>) e -> e.event.equals("VIEW_PRODUCT"))
                .map(e -> new ConteoProducto(e.product, 1))
                .keyBy(c -> c.product)
                .reduce((ReduceFunction<ConteoProducto>) (a, b) -> new ConteoProducto(a.product, a.views + b.views))
                .map(c -> "PRODUCTO_ACTIVO | " + c.product + " = " + c.views + " visualizaciones")
                .print();

        env.execute("Laboratorio 07 Kafka con Flink");
    }

    public static class JsonToEvento implements MapFunction<String, Evento> {
        private static final ObjectMapper mapper = new ObjectMapper();
        public Evento map(String value) throws Exception { return mapper.readValue(value, Evento.class); }
    }

    public static class EnriquecerEvento implements MapFunction<Evento, EventoEnriquecido> {
        public EventoEnriquecido map(Evento e) {
            LocalDateTime fecha = LocalDateTime.parse(e.timestamp, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            boolean weekend = fecha.getDayOfWeek().getValue() >= 6;
            return new EventoEnriquecido(e, fecha.getHour(), fecha.getDayOfMonth(), fecha.getMonthValue(), weekend);
        }
    }

    public static class Evento implements Serializable {
        public String user;
        public String event;
        public String product;
        public String category;
        public String city;
        public int price;
        public String timestamp;
    }

    public static class EventoEnriquecido extends Evento {
        public int hour;
        public int day;
        public int month;
        public boolean weekend;
        public EventoEnriquecido() {}
        public EventoEnriquecido(Evento e, int hour, int day, int month, boolean weekend) {
            this.user = e.user;
            this.event = e.event;
            this.product = e.product;
            this.category = e.category;
            this.city = e.city;
            this.price = e.price;
            this.timestamp = e.timestamp;
            this.hour = hour;
            this.day = day;
            this.month = month;
            this.weekend = weekend;
        }
    }

    public static class ConteoEvento implements Serializable {
        public String event;
        public int total;
        public ConteoEvento() {}
        public ConteoEvento(String event, int total) { this.event = event; this.total = total; }
    }

    public static class ConteoProducto implements Serializable {
        public String product;
        public int views;
        public ConteoProducto() {}
        public ConteoProducto(String product, int views) { this.product = product; this.views = views; }
    }
}
EOF

cat > /home/ec2-user/lab7_kafka_flink.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

source /home/ec2-user/.bash_profile
TOPIC="eventos-lab7"
GROUP="grupo-flink-lab7"

export KAFKA_TOPIC="$TOPIC"

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
echo "2) Producer: iniciar generador automatico de eventos JSON"
/home/ec2-user/lab7/productor_eventos.sh "$TOPIC" &
PRODUCER_PID=$!
sleep 12

echo
echo "3) Offset: leer eventos mostrando partition y offset"
kafka-console-consumer.sh \
  --bootstrap-server "$KAFKA_BROKER" \
  --topic "$TOPIC" \
  --from-beginning \
  --timeout-ms 10000 \
  --property print.partition=true \
  --property print.offset=true \
  --max-messages 6 || true

echo
echo "4) Maven + Flink: consumir Kafka, filtrar, transformar, contar y agrupar"
cd /home/ec2-user/lab7/flink-kafka-lab7
mvn -q -DskipTests package
set +e
timeout 45s java -jar target/lab7-flink-kafka-1.0.0.jar
FLINK_EXIT=$?
set -e

kill "$PRODUCER_PID" 2>/dev/null || true
wait "$PRODUCER_PID" 2>/dev/null || true

echo
echo "5) Consumer Group: mostrar offsets confirmados"
kafka-consumer-groups.sh \
  --bootstrap-server "$KAFKA_BROKER" \
  --describe \
  --group "$GROUP" || true

echo
echo "Demo Lab 7 finalizado."
echo "Flink exit code esperado por timeout: $FLINK_EXIT"
echo "Conceptos demostrados: Producer, Topic, Partition, Offset, Consumer Group, Maven, Kafka Connector y Flink DataStream."
EOF

chmod +x /home/ec2-user/lab7/productor_eventos.sh /home/ec2-user/lab7_kafka_flink.sh
chown -R ec2-user:ec2-user /home/ec2-user/lab7 /home/ec2-user/lab7_kafka_flink.sh