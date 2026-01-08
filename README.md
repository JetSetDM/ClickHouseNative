# ClickHouseNative (Swift)

Native ClickHouse TCP driver written in Swift, built on SwiftNIO.
Designed for Swift 6.2, cross‑platform, async/await‑first.

## Highlights

- Native TCP protocol (no HTTP wrapper).
- Async/await API with streaming results.
- LZ4 compression support.
- TLS support (custom CA + optional client cert).
- Multi‑host failover (round‑robin or random).
- Query options (query id, stage).
- Query events stream (data/progress/totals/extremes/profileInfo).
- Strong coverage for ClickHouse core types (see below).

## Installation (SwiftPM)

Add the package to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/JetSetDM/ClickHouseNative.git", from: "0.1.0")
]
```

```swift
targets: [
    .target(
        name: "YourApp",
        dependencies: [
            .product(name: "ClickHouseNative", package: "ClickHouseNative")
        ]
    )
]
```

## Quick Start

```swift
import ClickHouseNative

let config = CHConfig(
    host: "127.0.0.1",
    port: 9000,
    database: "default",
    user: "default",
    password: "default",
    compressionEnabled: true
)

let client = try await ClickHouseClient(config: config)

try await client.execute("CREATE TABLE IF NOT EXISTS demo(id UInt64, name String) ENGINE=Memory")

var builder = CHBlockBuilder()
builder.addColumn(name: "id", type: CHUInt64Type(), values: [1, 2])
builder.addColumn(name: "name", type: CHStringType(), values: ["alice", "bob"])
let block = try builder.build()
try await client.insert(into: "demo", block: block)

let result = try await client.query("SELECT id, name FROM demo ORDER BY id")
for try await row in result.rows() {
    print(row["id"] as Any, row["name"] as Any)
}

await client.close()
```

## Row Decoding (Decodable)

```swift
struct User: Decodable, Sendable {
    let id: UInt64
    let name: String
}

let rows = try await client.queryRows("SELECT id, name FROM demo ORDER BY id", as: User.self)
for try await user in rows {
    print(user.id, user.name)
}
```

## Query Options (queryId / stage)

```swift
let options = CHQueryOptions(queryId: "q-\(UUID().uuidString)", stage: .complete)
try await client.execute("SELECT 1", options: options)
```

## Query Events (progress / totals / extremes / profileInfo)

```swift
let settings: [String: CHSettingValue] = [
    "extremes": .int64(1),
    "max_block_size": .int64(2048)
]

let events = try await client.queryEvents(
    "SELECT number % 10 AS k, sum(number) AS s FROM system.numbers LIMIT 100000 GROUP BY k WITH TOTALS",
    settings: settings
)

for try await event in events {
    switch event {
    case .data(let block): print("block rows:", block.rowCount)
    case .progress(let p): print("progress:", p.readRows)
    case .totals: print("totals")
    case .extremes: print("extremes")
    case .profileInfo: print("profile")
    }
}
```

## TLS

```swift
var config = CHConfig(host: "127.0.0.1", port: 9440, user: "default", password: "default")
config.tlsEnabled = true
config.tlsVerifyMode = .verifyCA
config.tlsCAFilePath = "/path/to/ca.crt"
config.tlsClientCertificatePath = "/path/to/client.crt" // optional
config.tlsClientKeyPath = "/path/to/client.key"         // optional

let client = try await ClickHouseClient(config: config)
```

## Failover

```swift
var config = CHConfig(host: "ch-primary", port: 9000)
config.hosts = [
    CHHost(host: "ch-primary", port: 9000),
    CHHost(host: "ch-secondary", port: 9000)
]
config.hostSelectionPolicy = .roundRobin
let client = try await ClickHouseClient(config: config)
```

## Settings / Timeouts / Socket Options

```swift
var config = CHConfig(host: "127.0.0.1")
config.connectTimeout = 5
config.queryTimeout = 30
config.tcpKeepAlive = true
config.socketSendBufferBytes = 1 << 20
config.socketRecvBufferBytes = 1 << 20

try await client.execute(
    "SELECT 1",
    settings: ["max_threads": .int64(4), "send_logs_level": .string("trace")]
)
```

## Supported Types

The driver covers ClickHouse core types commonly used in production.  
Values are mapped to Swift as listed below:

### Numeric

- `Int8/16/32/64` → `Int64`
- `UInt8/16/32/64` → `UInt64`
- `Float32` → `Float`
- `Float64` → `Double`
- `Decimal32/64/128/256`, `Decimal(p,s)` → `Decimal` (precision up to 76)
- `Bool` → `Bool`

### Strings / Binary

- `String` → `String`
- `FixedString(N)` / `Binary(N)` → `Data`

### Date / Time

- `Date`, `Date32` → `Date`
- `DateTime`, `DateTime('tz')` → `Date`
- `DateTime64(scale[, 'tz'])` → `Date`

### Network / IDs

- `UUID` → `UUID`
- `IPv4` → `UInt32` (network byte order)
- `IPv6` → `Data` (16 bytes)

### Complex

- `Array(T)` → `[Any?]`
- `Tuple(...)` → `[Any?]`
- `Map(K, V)` → `[AnyHashable: Any?]` (fallback to array of pairs if key is non‑hashable)
- `LowCardinality(T)` → same as `T`
- `Nullable(T)` → `Optional`
- `Enum8/Enum16` → `String`
- `Nothing` → `nil` (cannot be stored in table columns; usable in result decoding / local round‑trip)

## Sample App

The repo includes a full end‑to‑end sample that:
- Connects to ClickHouse
- Creates a table with all supported types
- Inserts rows
- Reads back and validates every field
- Exercises query events, query options, decodable rows, and early cancel

Run it:

```bash
swift run ClickHouseNativeSample
```

Environment variables used by the sample:

```bash
CLICKHOUSE_HOST=127.0.0.1
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=default
CLICKHOUSE_DB=default
CLICKHOUSE_COMPRESSION=1
```

TLS (optional):

```bash
CLICKHOUSE_USE_TLS=1
CLICKHOUSE_TLS_HOST=127.0.0.1
CLICKHOUSE_TLS_PORT=9440
CLICKHOUSE_TLS_CA_PATH=/path/to/ca.crt
CLICKHOUSE_TLS_CLIENT_CERT_PATH=/path/to/client.crt
CLICKHOUSE_TLS_CLIENT_KEY_PATH=/path/to/client.key
```

## Dev Tools (Probe / Bench)

The Probe and Bench executables live in a separate SwiftPM package under `Tools/`.

Build them:

```bash
swift build --package-path Tools --product ClickHouseNativeProbe
swift build --package-path Tools --product ClickHouseNativeBench
```

Run them:

```bash
swift run --package-path Tools ClickHouseNativeProbe
swift run --package-path Tools ClickHouseNativeBench
```

Integration tests that validate cancellation/restart behavior use the Probe binary. If you want to run those tests manually, build it first or export one of:

```bash
CLICKHOUSE_NATIVE_PROBE_PATH=/path/to/ClickHouseNativeProbe
CLICKHOUSE_NATIVE_TOOLS_BUILD_DIR=/path/to/Tools/.build/arm64-apple-macosx/debug
```

## Docker Quickstart (dev/test)

From the repo root:

```bash
./Scripts/clickhouse-docker.sh up
./Scripts/clickhouse-docker.sh test
```

## Notes & Limitations

- One in‑flight query per client connection. Use multiple clients for parallelism.
- No JDBC abstraction (by design).
- No HTTP interface.
- Some advanced ClickHouse types are not implemented yet (e.g., AggregateFunction, JSON/Object, Geo types).
- `Nothing` cannot be used as a table column (ClickHouse restriction).

## License

Specify your license here.
