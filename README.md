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
