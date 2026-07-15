# Kafka en EC2 AWS

Esta guia cubre la instalacion automatica de Apache Kafka en instancias EC2 y la demostracion de los conceptos solicitados:

- Producer
- Consumer
- Topic
- Partition
- Offset
- Consumer Group

## 1. Configuracion previa

Edita `src/config.py` y verifica que estos valores correspondan a tu cuenta AWS:

```python
REGION = 'us-east-1'
AMI_ID = 'ami-...'
KEY_NAME = 'cluster'
SECURITY_GROUP_ID = 'sg-...'
SUBNET_ID = 'subnet-...'
TIPO_INSTANCIA = 't3.small'
USUARIO_SSH = 'ec2-user'
```

El Security Group debe permitir como minimo:

- SSH: puerto `22`
- Kafka broker: puerto `9092` entre las instancias del mismo Security Group
- Kafka controller interno: puerto `9093` entre las instancias del mismo Security Group

## 2. Crear las instancias EC2 desde Python

El aprovisionamiento se hace desde `src/levantar_kafka.py`, usando `boto3` y la configuracion de `src/config.py`.

```bash
python3 src/levantar_kafka.py --start_nodes 2
```

La instancia donde ejecutas este comando actua como controlador/orquestador. Esa instancia no cuenta como nodo Kafka.

Esto crea 3 instancias EC2 nuevas:

- 1 instancia `Kafka-Master-<run_id>`, que funciona como broker Kafka.
- 1 instancia `Kafka-Worker-1-<run_id>`, cliente Kafka conectado al broker.
- 1 instancia `Kafka-Worker-2-<run_id>`, cliente Kafka conectado al broker.

Cada nodo recibe un `Name` diferente en AWS usando el identificador de ejecucion `<run_id>`.

El script inyecta automaticamente:

- `scripts/user_data_master.sh` en el master.
- `scripts/user_data_worker.sh` en los workers, incluyendo la IP privada del broker.

## 3. Verificar instancias creadas

```bash
python3 src/levantar_kafka.py --check
```

Tambien puedes revisar en AWS Console las instancias con etiquetas:

- `Proyecto=ONPE-Kafka`
- `Rol=KafkaMaster`
- `Rol=KafkaWorker`
- `ClusterRun=<run_id>`

## 4. Entrar al nodo master

Cuando `--start_nodes` termine, mostrara el nombre, ID, IP privada y DNS publico del master. Conectate asi:

```bash
ssh -i ~/.ssh/cluster.pem ec2-user@DNS_PUBLICO_DEL_MASTER
```

Si tu llave tiene otro nombre, usa el valor configurado en `KEY_NAME`.

Para ver si la instalacion termino:

```bash
tail -f /var/log/cloud-init-output.log
```

Kafka queda instalado en:

```bash
/opt/kafka
```

Y el broker queda como servicio:

```bash
sudo systemctl status kafka
```

## 5. Ejecutar la demostracion solicitada

En el master ejecuta:

```bash
/home/ec2-user/kafka_demo.sh
```

Ese script demuestra los conceptos asi:

### Topic

Crea el topic `onpe-votos-demo`:

```bash
kafka-topics.sh --bootstrap-server "$KAFKA_BROKER" \
  --create --if-not-exists \
  --topic onpe-votos-demo \
  --partitions 3 \
  --replication-factor 1
```

### Partition

El topic se crea con 3 particiones. Se comprueba con:

```bash
kafka-topics.sh --bootstrap-server "$KAFKA_BROKER" --describe --topic onpe-votos-demo
```

La salida muestra particiones como `Partition: 0`, `Partition: 1` y `Partition: 2`.

### Producer

Publica mensajes al topic usando `kafka-console-producer.sh`:

```bash
kafka-console-producer.sh \
  --bootstrap-server "$KAFKA_BROKER" \
  --topic onpe-votos-demo \
  --property parse.key=true \
  --property key.separator=:
```

Ejemplo de mensajes:

```text
LIMA:mesa=001 votos=120
AREQUIPA:mesa=002 votos=95
CUSCO:mesa=003 votos=110
```

### Consumer

Lee los mensajes desde el inicio con `kafka-console-consumer.sh`:

```bash
kafka-console-consumer.sh \
  --bootstrap-server "$KAFKA_BROKER" \
  --topic onpe-votos-demo \
  --from-beginning
```

### Offset

El consumer del demo imprime el offset de cada mensaje:

```bash
--property print.offset=true
```

La salida permite observar en que posicion quedo cada mensaje dentro de su particion.

### Consumer Group

El demo consume los mensajes usando el grupo `grupo-consulta-onpe`:

```bash
kafka-console-consumer.sh \
  --bootstrap-server "$KAFKA_BROKER" \
  --topic onpe-votos-demo \
  --group grupo-consulta-onpe \
  --from-beginning \
  --max-messages 6
```

Luego muestra los offsets confirmados del grupo:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server "$KAFKA_BROKER" \
  --describe \
  --group grupo-consulta-onpe
```

## 6. Probar consumers desde workers

En un worker puedes ejecutar:

```bash
/home/ec2-user/kafka_client_demo.sh
```

Esto se conecta automaticamente al broker usando la variable `KAFKA_BROKER` configurada por el user-data.

Para ver reparto entre consumers, abre dos workers y ejecuta en ambos el mismo grupo:

```bash
/home/ec2-user/kafka_client_demo.sh onpe-votos-demo grupo-workers-onpe
```

Kafka reparte las particiones del topic entre los consumidores del mismo grupo.

## 7. Eliminar las instancias

Para evitar costos en AWS:

```bash
python3 src/levantar_kafka.py --delete
```
