# binary-to-uuid-smt

Kafka Connect **Single Message Transforms (SMT)** for Debezium-style CDC: convert between **MySQL `BINARY(16)`** (Kafka Connect `BYTES`) and **canonical UUID strings** (PostgreSQL `uuid` / JDBC), and the reverse for PostgreSQL → MySQL.

| SMT | Direction | Connect types |
|-----|-----------|----------------|
| `BinaryToUuid` | binary → text | `BYTES` → `STRING` |
| `UuidToBinary` | text → binary | `STRING` → `BYTES` |

Nested structs are handled recursively (e.g. Debezium envelope `before` / `after`).

## Requirements

- Java **17+**
- Kafka Connect worker (Debezium Connect or any Connect distribution)
- Place the built JAR on the worker **`plugin.path`** and restart workers.

## Build

```bash
mvn package
```

Artifact: `target/binary-to-uuid-smt-1.0.0-SNAPSHOT.jar`

Run tests:

```bash
mvn test
```

## Configuration

Both transforms share the same options:

| Property | Required | Description |
|----------|----------|-------------|
| `fields` | yes | Comma-separated logical field names to convert (must exist as the source type below). |
| `byte.order` | no | `RFC_4122` (default) or `MYSQL_UUID_TO_BIN_SWAP` (MySQL `UUID_TO_BIN(uuid, 1)` / Percona ordered layout). |

### Classes

- **Value:** `io.debezium.uuid.BinaryToUuid$Value` / `io.debezium.uuid.UuidToBinary$Value`
- **Key:** `io.debezium.uuid.BinaryToUuid$Key` / `io.debezium.uuid.UuidToBinary$Key`

### Example: MySQL → PostgreSQL (binary to UUID string)

```properties
transforms=bin2uuid
transforms.bin2uuid.type=io.debezium.uuid.BinaryToUuid$Value
transforms.bin2uuid.fields=id,tenant_id
transforms.bin2uuid.byte.order=RFC_4122
```

Use `MYSQL_UUID_TO_BIN_SWAP` if the source column was stored with `UUID_TO_BIN(uuid, 1)`.

### Example: PostgreSQL → MySQL (UUID string to binary)

```properties
transforms=uuid2bin
transforms.uuid2bin.type=io.debezium.uuid.UuidToBinary$Value
transforms.uuid2bin.fields=id,tenant_id
transforms.uuid2bin.byte.order=RFC_4122
```

### Unwrap (optional)

You do **not** need `ExtractNewRecordState` only for these conversions: the SMT walks nested structs. Add `unwrap` if you want a flattened payload for other reasons; chain order matters, e.g. `unwrap,bin2uuid`.

### RegexRouter (optional)

Chain with other SMTs as usual, e.g. `bin2uuid,route` — see Kafka Connect `RegexRouter`.

## REST registration

```bash
curl -sS -X POST -H "Content-Type: application/json" \
  --data @examples/jdbc-postgres-sink-bin2uuid.json \
  http://localhost:8083/connectors
```

## Example connector JSON

| File | Use case |
|------|----------|
| `examples/jdbc-postgres-sink-bin2uuid.json` | Debezium JDBC sink → PostgreSQL + `BinaryToUuid` |
| `examples/jdbc-postgres-sink-bin2uuid-unwrap.json` | Same + `ExtractNewRecordState` before transform |
| `examples/jdbc-postgres-sink-bin2uuid-confluent.json` | Confluent JDBC sink |
| `examples/jdbc-mysql-sink-uuid2bin.json` | Debezium JDBC sink → MySQL + `UuidToBinary` |
| `examples/mysql-debezium-source-bin2uuid.json` | Debezium MySQL source + `BinaryToUuid` on the value |

Edit `topics`, JDBC URLs, credentials, and `fields` to match your environment.
