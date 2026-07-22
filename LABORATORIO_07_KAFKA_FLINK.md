# Laboratorio 07 GA: Apache Flink con Kafka

Guia basada en el PDF `Lab07.pdf`. El laboratorio implementa procesamiento continuo de eventos con Apache Flink consumiendo informacion desde Apache Kafka.

## Objetivo

Implementar un sistema de procesamiento continuo de eventos mediante Apache Flink, consumiendo informacion desde Apache Kafka para generar metricas, detectar patrones de comportamiento y construir indicadores en tiempo real.

## Flujo implementado

```text
Producer automatico JSON -> Kafka topic eventos-lab7 -> Flink DataStream -> metricas en consola
```

Cada evento tiene la estructura solicitada:

```json
{
  "user":"USR001",
  "event":"VIEW_PRODUCT",
  "product":"Laptop Lenovo",
  "category":"Electronics",
  "city":"Arequipa",
  "price":3200,
  "timestamp":"2026-07-20T19:15:20"
}
```

## 1. Levantar infraestructura

Desde el controlador donde ya funciona boto3 y `src/config.py`:

```bash
python3 src/levantar_kafka_flink.py --start_nodes 1
```

Esto crea:

- 1 master con Kafka, Flink y Maven.
- 1 worker cliente Kafka opcional.

Tambien se puede crear solo el master:

```bash
python3 src/levantar_kafka_flink.py --start_nodes 0
```

## 2. Entrar al master

```bash
ssh -i ~/.ssh/cluster.pem ec2-user@DNS_PUBLICO_DEL_MASTER
```

Verificar instalacion:

```bash
tail -f /var/log/cloud-init-output.log
```

## 3. Ejecutar todo el laboratorio

```bash
/home/ec2-user/lab7_kafka_flink.sh
```

El script realiza automaticamente:

- Crea el topic `eventos-lab7` con 3 particiones.
- Inicia un producer automatico de eventos JSON.
- Muestra mensajes consumidos con partition y offset.
- Crea y compila un proyecto Maven en `/home/ec2-user/lab7/flink-kafka-lab7`.
- Agrega dependencias de Flink y el conector Kafka.
- Configura el consumer Kafka de Flink.
- Muestra continuamente los eventos recibidos.
- Filtra eventos `PURCHASE` y `ADD_CART`.
- Agrega atributos derivados: hora, dia, mes y fin de semana.
- Cuenta eventos por tipo: `SEARCH`, `PURCHASE`, `VIEW_PRODUCT`, `ADD_CART`.
- Agrupa productos por actividad y muestra visualizaciones acumuladas.
- Muestra offsets del consumer group `grupo-flink-lab7`.

## 4. Evidencias para el informe PDF

Capturas recomendadas:

- Instancias EC2 creadas en AWS.
- `cloud-init` finalizado correctamente.
- Topic `eventos-lab7` descrito con 3 particiones.
- Producer generando eventos JSON.
- Consumer mostrando `partition` y `offset`.
- Compilacion Maven del proyecto Flink.
- Salida de Flink con lineas `RECIBIDO`.
- Salida de Flink con lineas `FILTRADO` para `PURCHASE` y `ADD_CART`.
- Salida de Flink con lineas `TRANSFORMADO` mostrando hora, dia, mes y finDeSemana.
- Salida de Flink con lineas `CONTEO_EVENTOS`.
- Salida de Flink con lineas `PRODUCTO_ACTIVO`.
- Consumer group `grupo-flink-lab7` con offsets.

## 5. Borrar instancias

```bash
python3 src/levantar_kafka_flink.py --delete
```