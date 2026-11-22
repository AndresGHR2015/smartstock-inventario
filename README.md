# SmartStock · Inventario

Microservicio que administra las existencias y los movimientos de stock de SmartStock. Es el dueño del saldo disponible y del umbral de reposición de cada SKU.

**Stack** · Go, PostgreSQL, gRPC y Kafka.

## Alcance del MVP

- Consultar las existencias de un SKU y registrar descuentos asociados a ventas.
- Validar que haya unidades suficientes y conservar el saldo cuando una operación es rechazada.
- Publicar una alerta cuando el saldo pasa de estar por encima del umbral a quedar en el umbral o por debajo de él.

## Integraciones

| Conexión | Protocolo | Operación |
|---|---|---|
| Ventas → Inventario | gRPC | Solicitar un descuento mediante `DeductStock`. |
| Gateway → Inventario | gRPC | Consultar el saldo de un SKU. |
| Inventario → Compras | Kafka | Publicar `inventory.stock_low.v1` al cruzar el umbral. |

El contrato gRPC se mantiene en `smartstock-infra/contracts/proto/inventory/v1/inventory.proto`.

## Primera verificación

Con 10 unidades disponibles, una solicitud de 3 unidades debe devolver 7. Repetirla con el mismo `sale_id` debe devolver el resultado original y dejar el saldo en 7. Una solicitud que supere el saldo disponible debe dejarlo intacto.

Los saldos y movimientos se guardan en la base PostgreSQL de Inventario. El repo tendrá su Dockerfile, su pipeline y una exigencia de cobertura mínima del 60 %. El despliegue en Kubernetes se integra desde `smartstock-infra`.
