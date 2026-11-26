# SmartStock · Inventario

Microservicio que administra las existencias de SmartStock. Mantiene el saldo y el umbral de reposición de cada SKU, registra los movimientos que modifican el stock y avisa a Compras cuando un producto necesita reposición.

**Stack** · Go, PostgreSQL, gRPC y Kafka.

## Responsabilidades

- Mantener el catálogo operativo de SKU, sus existencias y sus umbrales de reposición.
- Consultar el saldo de un SKU y registrar los movimientos que explican cómo cambió.
- Aplicar los descuentos solicitados por Ventas y validar la cantidad disponible antes de confirmar cada movimiento.
- Emitir una alerta cuando el saldo pasa de estar sobre el umbral de reposición a quedar en el umbral o por debajo de él.

## Datos y reglas de negocio

La base PostgreSQL de Inventario es la fuente de verdad para saldos, umbrales y movimientos. Cada descuento vinculado a una venta se identifica por `sale_id`. Repetir la misma operación conserva su resultado y evita duplicar el movimiento. Una solicitud que reutiliza el mismo identificador con una cantidad o SKU distintos se considera inválida. El saldo se mantiene íntegro si la operación es rechazada.

## Interfaces

| Interlocutor | Canal | Propósito |
|---|---|---|
| `smartstock-gateway` | gRPC | Consultar las existencias. |
| `smartstock-ventas` | gRPC | Descontar stock mediante `DeductStock`. |
| `smartstock-compras` | Kafka | Recibir el evento `inventory.stock_low.v1`. |

La definición compartida del servicio gRPC vive en `smartstock-infra/contracts/proto/inventory/v1/inventory.proto`. Los consumidores consultan las existencias a través del servicio.

## Desarrollo y despliegue

Este repositorio mantiene el código Go, las migraciones de Inventario, sus pruebas, su Dockerfile y su pipeline. La cobertura mínima exigida es del 60 %. Las comprobaciones del dominio cubren, entre otros casos, stock insuficiente, reintentos y cruces del umbral. Docker Compose y Kubernetes se coordinan desde `smartstock-infra`.

### Contratos y generación

El submódulo `vendor/smartstock-infra` fija la revisión
`6237ae7a047c3a16077db5bdd51a6ee89f5f90f1`. Después de clonar:

```sh
git submodule update --init --recursive
git -C vendor/smartstock-infra rev-parse HEAD
```

El código generado está en `gen/inventory/v1`. Para regenerarlo con
`protoc`, `protoc-gen-go` 1.36.5 y `protoc-gen-go-grpc` 1.5.1:

```sh
protoc -I vendor/smartstock-infra/contracts/proto \
  --go_out=. --go_opt=module=smartstock/inventory \
  --go_opt=Minventory/v1/inventory.proto=smartstock/inventory/gen/inventory/v1 \
  --go-grpc_out=. --go-grpc_opt=module=smartstock/inventory \
  --go-grpc_opt=Minventory/v1/inventory.proto=smartstock/inventory/gen/inventory/v1 \
  vendor/smartstock-infra/contracts/proto/inventory/v1/inventory.proto
```

### Ejecución

Crear una base PostgreSQL propia y el topic `inventory.stock_low.v1` en Kafka
antes de arrancar. El rol de base de datos necesita permisos para crear las
tablas. Las variables son:

| Variable | Uso |
|---|---|
| `DATABASE_URL` | Obligatoria; URL de PostgreSQL de Inventario. |
| `KAFKA_BROKERS` | Obligatoria; direcciones `host:puerto` separadas por comas. |
| `GRPC_ADDR` | Dirección de escucha; por defecto `:50051`. |

```sh
docker build -t smartstock-inventory .
docker run --rm -p 50051:50051 \
  -e DATABASE_URL='postgres://usuario:clave@postgres:5432/inventory?sslmode=disable' \
  -e KAFKA_BROKERS='kafka:9092' smartstock-inventory
```

Las direcciones deben ser accesibles desde el contenedor y las anunciadas por
Kafka también. El esquema inicial y el seed se aplican al arrancar; el SKU
`SKU-001` comienza con stock 10 y umbral 7. El seed usa `ON CONFLICT DO NOTHING`:
reiniciar no restablece un producto existente. `internal/inventory/schema.sql`
es el esquema inicial idempotente; cambios futuros requieren migraciones explícitas.
El proceso registra errores de publicación y vuelve a intentar. Kafka puede
recibir duplicados; el consumidor debe deduplicar por `event_id`.

### Pruebas

Las pruebas de integración **eliminan las tablas de Inventario** de la base
indicada: usar exclusivamente una base desechable dedicada. El directorio
`vendor` contiene el submódulo, no dependencias Go vendorizadas; por ello los
comandos Go necesitan `GOFLAGS=-mod=mod`.

```sh
export GOFLAGS=-mod=mod
export TEST_DATABASE_URL='postgres://usuario:clave@localhost:5432/inventory_test?sslmode=disable'
go test -race -coverprofile=coverage.out -v ./internal/inventory/...
go tool cover -func=coverage.out
go build ./cmd/inventory
```

Para probar la publicación y redelivery real, crear el topic con una sola
partición en un Kafka de pruebas y añadir `TEST_KAFKA_BROKER=localhost:9092`.
Sin esa variable la prueba Kafka se omite; el pipeline ejecuta PostgreSQL y la
prueba del outbox con un publicador simulado, no levanta Kafka. La prueba
`TestLiveServer` es opcional: requiere `TEST_GRPC_ADDR` y un servicio con una
base nueva, y modifica su SKU de seed. No usarla contra datos reales.

La recepción de mercancía queda pendiente hasta que se acuerde su contrato;
este servicio no accede a las bases de Ventas ni Compras.
