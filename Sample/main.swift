import Foundation
import ClickHouseNative
import NIOCore

// This sample is intentionally verbose and beginner-friendly.
// It demonstrates:
// - Connecting to ClickHouse
// - Running queries and streaming results
// - Inserting data using CHBlockBuilder
// - Decoding rows into Swift types
// - Handling query events (progress/totals/extremes/profileInfo)
// - Simple resiliency checks (early cancel)

#if canImport(Darwin)
import Darwin
#else
import Glibc
#endif

enum SampleError: Error, CustomStringConvertible {
    case failed(String)

    var description: String {
        switch self {
        case .failed(let message):
            return message
        }
    }
}

@main
struct ClickHouseNativeSample {
    static func main() async {
        do {
            try await SampleRunner().run()
        } catch {
            print("Sample failed: \(describeError(error))")
            exit(1)
        }
    }
}

struct SampleRunner {
    private let env = ProcessInfo.processInfo.environment

    func run() async throws {
        let config = makeConfig()
        log("ClickHouseNative sample starting")
        log("Host: \(config.host):\(config.port) DB: \(config.database) TLS: \(config.tlsEnabled) Compression: \(config.compressionEnabled)")

        let client = try await ClickHouseClient(config: config)
        var tables: [String] = []

        do {
            // 1) Read server timezone so our Date/DateTime checks are consistent.
            let serverTimeZone = try await fetchServerTimeZone(client: client)

            // 2) Basic ping.
            try await step("01 - ping") {
                let ok = try await client.ping()
                try require(ok, "Ping failed")
            }

            // 3) queryOne + CHQueryOptions (query id + stage).
            try await step("02 - queryOne + queryOptions") {
                struct OneRow: Decodable, Sendable { let value: Int }
                let options = CHQueryOptions(queryId: "sample-\(UUID().uuidString)", stage: .complete)
                let one = try await client.queryOne("SELECT 1 AS value", as: OneRow.self, options: options)
                try require(one?.value == 1, "queryOne did not return expected value")
            }

            // 4) queryEvents: demonstrate metadata streams (progress/totals/extremes/profileInfo).
            //    We enable extremes explicitly via settings.
            try await step("03 - queryEvents (progress/totals/extremes/profileInfo)") {
                let sql = """
                SELECT k, sum(number) AS s
                FROM (
                  SELECT number, number % 10 AS k
                  FROM system.numbers
                  LIMIT 100000
                )
                GROUP BY k WITH TOTALS
                """
                let settings: [String: CHSettingValue] = [
                    "extremes": .int64(1),
                    "max_block_size": .int64(2048)
                ]
                let options = CHQueryOptions(queryId: "sample-events-\(UUID().uuidString)", stage: .complete)
                let events = try await client.queryEvents(sql, settings: settings, options: options)
                var sawData = false
                var sawProgress = false
                var sawTotals = false
                var sawExtremes = false
                var sawProfile = false
                for try await event in events {
                    switch event {
                    case .data:
                        sawData = true
                    case .progress:
                        sawProgress = true
                    case .totals:
                        sawTotals = true
                    case .extremes:
                        sawExtremes = true
                    case .profileInfo:
                        sawProfile = true
                    }
                }
                try require(sawData, "queryEvents: missing data")
                try require(sawProgress, "queryEvents: missing progress")
                try require(sawTotals, "queryEvents: missing totals")
                try require(sawExtremes, "queryEvents: missing extremes")
                try require(sawProfile, "queryEvents: missing profileInfo")
            }

            // 5) Create a table with (almost) every supported type.
            let allTypesTable = uniqueName(prefix: "sample_all_types")
            tables.append(allTypesTable)
            try await step("04 - create all-types table") {
                try await client.execute("DROP TABLE IF EXISTS \(allTypesTable)")
                try await client.execute("""
                CREATE TABLE \(allTypesTable) (
                  i8 Int8,
                  i16 Int16,
                  i32 Int32,
                  i64 Int64,
                  u8 UInt8,
                  u16 UInt16,
                  u32 UInt32,
                  u64 UInt64,
                  f32 Float32,
                  f64 Float64,
                  b Bool,
                  str String,
                  fixed FixedString(4),
                  bin FixedString(4),
                  uuid UUID,
                  ipv4 IPv4,
                  ipv6 IPv6,
                  d Date,
                  d32 Date32,
                  dt DateTime,
                  dt64 DateTime64(3),
                  e8 Enum8('a' = -1, 'b' = 2),
                  e16 Enum16('x' = -1, 'y' = 2),
                  dec Decimal(10,2),
                  dec32 Decimal32(2),
                  dec64 Decimal64(4),
                  dec128 Decimal128(6),
                  dec256 Decimal256(6),
                  arr Array(Float64),
                  tuple Tuple(Int32, String),
                  map Map(String, Nullable(Int32)),
                  lc LowCardinality(String),
                  ndate Nullable(Date),
                  ndt Nullable(DateTime)
                ) ENGINE=Memory
                """)
            }

            // 6) Prepare sample values. Use server timezone for Date/DateTime values
            //    so comparisons are stable regardless of local machine timezone.
            let tz = serverTimeZone
            let date1 = makeDate(year: 2022, month: 1, day: 2, timeZone: tz)
            let date2 = makeDate(year: 2023, month: 4, day: 5, timeZone: tz)
            let date32_1 = makeDate(year: 1971, month: 1, day: 2, timeZone: tz)
            let date32_2 = makeDate(year: 2024, month: 12, day: 31, timeZone: tz)
            let dt1 = makeDate(year: 2022, month: 1, day: 2, hour: 3, minute: 4, second: 5, timeZone: tz)
            let dt2 = makeDate(year: 2023, month: 4, day: 5, hour: 6, minute: 7, second: 8, timeZone: tz)
            let dt64_1 = makeDate(year: 2022, month: 6, day: 7, hour: 8, minute: 9, second: 10, millisecond: 123, timeZone: tz)
            let dt64_2 = makeDate(year: 2024, month: 9, day: 10, hour: 11, minute: 12, second: 13, millisecond: 987, timeZone: tz)

            // FixedString/Binary are represented as Data.
            let fixed1 = Data([0x41, 0x42, 0x43, 0x44])
            let fixed2 = Data([0x31, 0x32])
            let bin1 = Data([0x01, 0x02, 0x03, 0x04])
            let bin2 = Data([0x05, 0x06])
            let uuid1 = UUID(uuidString: "00000000-0000-0000-0000-000000000001")!
            let uuid2 = UUID(uuidString: "00000000-0000-0000-0000-000000000002")!
            let ipv4_1 = "127.0.0.1"
            let ipv4_2 = "10.0.0.42"
            let ipv6_1 = "2001:db8::1"
            let ipv6_2 = "ff00::1"

            // 7) Insert two rows using CHBlockBuilder (columnar insert).
            try await step("05 - insert all-types rows (CHBlockBuilder)") {
                var builder = CHBlockBuilder()
                builder.addColumn(name: "i8", type: CHInt8Type(), values: [Int64(-5), Int64(7)])
                builder.addColumn(name: "i16", type: CHInt16Type(), values: [Int64(-300), Int64(301)])
                builder.addColumn(name: "i32", type: CHInt32Type(), values: [Int64(-40000), Int64(50000)])
                builder.addColumn(name: "i64", type: CHInt64Type(), values: [Int64(-9_000_000_000), Int64(9_000_000_000)])
                builder.addColumn(name: "u8", type: CHUInt8Type(), values: [UInt64(5), UInt64(255)])
                builder.addColumn(name: "u16", type: CHUInt16Type(), values: [UInt64(65000), UInt64(42)])
                builder.addColumn(name: "u32", type: CHUInt32Type(), values: [UInt64(4_000_000_000), UInt64(123_456)])
                builder.addColumn(name: "u64", type: CHUInt64Type(), values: [UInt64(9_000_000_000), UInt64(18_000_000_000)])
                builder.addColumn(name: "f32", type: CHFloat32Type(), values: [Float(1.25), Float(-2.5)])
                builder.addColumn(name: "f64", type: CHFloat64Type(), values: [Double(3.14159), Double(-6.022)])
                builder.addColumn(name: "b", type: CHBoolType(), values: [true, false])
                builder.addColumn(name: "str", type: CHStringType(), values: ["hello", "world"])
                builder.addColumn(name: "fixed", type: CHFixedStringType(length: 4), values: [fixed1, fixed2])
                builder.addColumn(name: "bin", type: CHFixedStringType(length: 4), values: [bin1, bin2])
                builder.addColumn(name: "uuid", type: CHUUIDType(), values: [uuid1, uuid2])
                builder.addColumn(name: "ipv4", type: CHIPv4Type(), values: [ipv4_1, ipv4_2])
                builder.addColumn(name: "ipv6", type: CHIPv6Type(), values: [ipv6_1, ipv6_2])
                builder.addColumn(name: "d", type: CHDateType(), values: [date1, date2])
                builder.addColumn(name: "d32", type: CHDate32Type(), values: [date32_1, date32_2])
                builder.addColumn(name: "dt", type: CHDateTimeType(timezone: tz), values: [dt1, dt2])
                builder.addColumn(name: "dt64", type: CHDateTime64Type(scale: 3, timezone: tz), values: [dt64_1, dt64_2])
                builder.addColumn(
                    name: "e8",
                    type: CHEnum8Type(names: ["a", "b"], values: [-1, 2]),
                    values: ["a", "b"]
                )
                builder.addColumn(
                    name: "e16",
                    type: CHEnum16Type(names: ["x", "y"], values: [-1, 2]),
                    values: ["x", "y"]
                )
                builder.addColumn(name: "dec", type: CHDecimalType(precision: 10, scale: 2), values: [
                    Decimal(string: "12345.67")!,
                    Decimal(string: "-0.01")!
                ])
                builder.addColumn(name: "dec32", type: CHDecimalType(precision: 9, scale: 2), values: [
                    Decimal(string: "100.25")!,
                    Decimal(string: "9999.99")!
                ])
                builder.addColumn(name: "dec64", type: CHDecimalType(precision: 18, scale: 4), values: [
                    Decimal(string: "1234.5678")!,
                    Decimal(string: "-9999.0001")!
                ])
                builder.addColumn(name: "dec128", type: CHDecimalType(precision: 38, scale: 6), values: [
                    Decimal(string: "123456.789012")!,
                    Decimal(string: "-1.000001")!
                ])
                builder.addColumn(name: "dec256", type: CHDecimalType(precision: 76, scale: 6), values: [
                    Decimal(string: "42.000001")!,
                    Decimal(string: "-999.999999")!
                ])
                builder.addColumn(name: "arr", type: CHArrayType(nested: CHFloat64Type()), values: [
                    [1.25, 2.5],
                    [3.75]
                ])
                // Tuple maps to [Any?] in a single column.
                builder.addColumn(name: "tuple", type: CHTupleType(nested: [CHInt32Type(), CHStringType()]), values: [
                    [Int64(7), "tuple-a"],
                    [Int64(9), "tuple-b"]
                ])
                // Map maps to [AnyHashable: Any?] (nullable values supported).
                builder.addColumn(name: "map", type: CHMapType(key: CHStringType(), value: CHNullableType(nested: CHInt32Type())), values: [
                    ["a": Int64(1), "b": nil],
                    ["a": Int64(2)]
                ])
                builder.addColumn(name: "lc", type: CHLowCardinalityType(nested: CHStringType()), values: ["alpha", "beta"])
                builder.addColumn(name: "ndate", type: CHNullableType(nested: CHDateType()), values: [Optional<Date>.none, date2])
                builder.addColumn(name: "ndt", type: CHNullableType(nested: CHDateTimeType(timezone: tz)), values: [dt1, Optional<Date>.none])
                let block = try builder.build()
                try await client.insert(into: allTypesTable, block: block)
            }

            // 8) Read back and verify that everything matches.
            try await step("06 - read + verify all-types rows") {
                let result = try await client.query("SELECT * FROM \(allTypesTable) ORDER BY i8")
                var rows: [CHRow] = []
                for try await row in result.rows() {
                    rows.append(row)
                }
                try require(rows.count == 2, "Expected 2 rows from all-types table")

                try verifyAllTypesRow(
                    row: rows[0],
                    expectedIndex: 0,
                    tz: tz,
                    fixed: padData(fixed1, length: 4),
                    bin: padData(bin1, length: 4),
                    ipv4: parseIPv4(ipv4_1),
                    ipv6: try parseIPv6(ipv6_1),
                    date: date1,
                    date32: date32_1,
                    dt: dt1,
                    dt64: dt64_1,
                    dec: Decimal(string: "12345.67")!,
                    dec32: Decimal(string: "100.25")!,
                    dec64: Decimal(string: "1234.5678")!,
                    dec128: Decimal(string: "123456.789012")!,
                    dec256: Decimal(string: "42.000001")!
                )
                try verifyAllTypesRow(
                    row: rows[1],
                    expectedIndex: 1,
                    tz: tz,
                    fixed: padData(fixed2, length: 4),
                    bin: padData(bin2, length: 4),
                    ipv4: parseIPv4(ipv4_2),
                    ipv6: try parseIPv6(ipv6_2),
                    date: date2,
                    date32: date32_2,
                    dt: dt2,
                    dt64: dt64_2,
                    dec: Decimal(string: "-0.01")!,
                    dec32: Decimal(string: "9999.99")!,
                    dec64: Decimal(string: "-9999.0001")!,
                    dec128: Decimal(string: "-1.000001")!,
                    dec256: Decimal(string: "-999.999999")!
                )
            }

            // 9) Nothing is not allowed as a table column in ClickHouse.
            //    We still show that encoding/decoding works locally.
            try await step("07 - nothing type local roundtrip") {
                let type = CHNothingType()
                let allocator = ByteBufferAllocator()
                var buffer = allocator.buffer(capacity: 0)
                var writer = CHBinaryWriter(buffer: buffer)
                try type.encodeColumn(values: [nil, nil], writer: &writer)
                buffer = writer.buffer
                var reader = CHBinaryReader(buffer: buffer)
                let decoded = try type.decodeColumn(rows: 2, reader: &reader)
                try require(decoded.count == 2, "Nothing decode count mismatch")
                try require(decoded.allSatisfy { $0 == nil }, "Nothing decode values mismatch")
                try require(reader.index == buffer.writerIndex, "Nothing decode did not consume buffer")
            }

            // 10) Insert using SQL string (INSERT INTO ... VALUES) as well.
            let usersTable = uniqueName(prefix: "sample_users")
            tables.append(usersTable)
            try await step("08 - create users table + insert via insert(sql:)") {
                try await client.execute("DROP TABLE IF EXISTS \(usersTable)")
                try await client.execute("""
                CREATE TABLE \(usersTable) (
                  id UInt64,
                  name String
                ) ENGINE=Memory
                """)
                var builder = CHBlockBuilder()
                builder.addColumn(name: "id", type: CHUInt64Type(), values: [UInt64(1), UInt64(2)])
                builder.addColumn(name: "name", type: CHStringType(), values: ["alice", "bob"])
                let block = try builder.build()
                try await client.insert(sql: "INSERT INTO \(usersTable) VALUES", block: block)
            }

            // 11) Decode rows into a Swift struct using Decodable.
            try await step("09 - queryRows decode to Decodable") {
                struct User: Decodable, Sendable {
                    let id: UInt64
                    let name: String
                }
                let rows = try await client.queryRows("SELECT id, name FROM \(usersTable) ORDER BY id", as: User.self)
                var users: [User] = []
                for try await user in rows {
                    users.append(user)
                }
                try require(users.count == 2, "Expected 2 users")
                try require(users[0].id == 1 && users[0].name == "alice", "User 1 mismatch")
                try require(users[1].id == 2 && users[1].name == "bob", "User 2 mismatch")
            }

            // 12) Early cancel: stop consuming rows and ensure connection stays usable.
            try await step("10 - early cancel does not poison connection") {
                let result = try await client.query("SELECT number FROM system.numbers LIMIT 1000000")
                let stream = result.rows()
                let task = Task {
                    var count = 0
                    for try await _ in stream {
                        count += 1
                        if count >= 1000 { break }
                        if Task.isCancelled { break }
                    }
                    return count
                }
                try await Task.sleep(nanoseconds: 50_000_000)
                task.cancel()
                _ = try? await task.value
                try await client.execute("SELECT 1")
            }
        } catch {
            await cleanupTables(tables, client: client)
            await client.close()
            throw error
        }

        await cleanupTables(tables, client: client)
        await client.close()
        log("Sample completed successfully")
    }

    private func fetchServerTimeZone(client: ClickHouseClient) async throws -> TimeZone {
        struct TZRow: Decodable, Sendable { let timezone: String }
        let tzRow = try await client.queryOne("SELECT timezone() AS timezone", as: TZRow.self)
        if let name = tzRow?.timezone, let tz = TimeZone(identifier: name) {
            log("Server timezone: \(name)")
            return tz
        }
        log("Server timezone not resolved, defaulting to UTC")
        return TimeZone(secondsFromGMT: 0) ?? .current
    }

    private func makeConfig() -> CHConfig {
        // Environment variables allow you to point the sample at any ClickHouse instance.
        // Defaults are set for local Docker usage.
        let host = env["CLICKHOUSE_HOST"] ?? "127.0.0.1"
        let port = Int(env["CLICKHOUSE_PORT"] ?? "") ?? 9000
        let database = env["CLICKHOUSE_DB"] ?? "default"
        let user = env["CLICKHOUSE_USER"] ?? "default"
        let password = env["CLICKHOUSE_PASSWORD"] ?? "default"
        let compression = env["CLICKHOUSE_COMPRESSION"] == "1"
        let connectTimeout = Double(env["CLICKHOUSE_CONNECT_TIMEOUT"] ?? "") ?? 10
        let queryTimeout = Double(env["CLICKHOUSE_QUERY_TIMEOUT"] ?? "") ?? 60

        var config = CHConfig(
            host: host,
            port: port,
            database: database,
            user: user,
            password: password,
            connectTimeout: connectTimeout,
            queryTimeout: queryTimeout,
            compressionEnabled: compression
        )

        if let keepAlive = env["CLICKHOUSE_TCP_KEEPALIVE"] {
            config.tcpKeepAlive = keepAlive == "1"
        }
        if let sendBuf = Int(env["CLICKHOUSE_SOCKET_SENDBUF"] ?? "") {
            config.socketSendBufferBytes = sendBuf
        }
        if let recvBuf = Int(env["CLICKHOUSE_SOCKET_RECVBUF"] ?? "") {
            config.socketRecvBufferBytes = recvBuf
        }

        if let hostList = env["CLICKHOUSE_HOSTS"], !hostList.isEmpty {
            let hosts = hostList.split(separator: ",").compactMap { entry -> CHHost? in
                let parts = entry.split(separator: ":")
                guard let hostPart = parts.first else { return nil }
                let portPart = parts.count > 1 ? Int(parts[1]) : nil
                return CHHost(host: String(hostPart), port: portPart ?? port)
            }
            if !hosts.isEmpty {
                config.hosts = hosts
            }
            if let policy = env["CLICKHOUSE_HOST_SELECTION"]?.lowercased() {
                config.hostSelectionPolicy = policy == "random" ? .random : .roundRobin
            }
        }

        let tlsEnabled = env["CLICKHOUSE_TLS_ENABLED"] == "1" || env["CLICKHOUSE_USE_TLS"] == "1"
        if tlsEnabled {
            config.tlsEnabled = true
            if let tlsHost = env["CLICKHOUSE_TLS_HOST"] {
                config.host = tlsHost
            }
            if let tlsPort = Int(env["CLICKHOUSE_TLS_PORT"] ?? "") {
                config.port = tlsPort
            }
            if env["CLICKHOUSE_TLS_VERIFY"] == "0" {
                config.tlsVerifyMode = .none
            }
            config.tlsCAFilePath = env["CLICKHOUSE_TLS_CA_PATH"]
            config.tlsClientCertificatePath = env["CLICKHOUSE_TLS_CLIENT_CERT_PATH"]
            config.tlsClientKeyPath = env["CLICKHOUSE_TLS_CLIENT_KEY_PATH"]
        }

        return config
    }
}

// Best-effort cleanup so the sample can be re-run safely.
private func cleanupTables(_ tables: [String], client: ClickHouseClient) async {
    for table in tables {
        try? await client.execute("DROP TABLE IF EXISTS \(table)")
    }
}

// Adds a unique suffix to avoid collisions when multiple runs execute in parallel.
private func uniqueName(prefix: String) -> String {
    let stamp = Int(Date().timeIntervalSince1970)
    let suffix = UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(8)
    return "\(prefix)_\(stamp)_\(suffix)"
}

private func log(_ message: String) {
    let formatter = DateFormatter()
    formatter.dateFormat = "HH:mm:ss"
    let ts = formatter.string(from: Date())
    print("[\(ts)] \(message)")
}

private func step(_ name: String, _ body: () async throws -> Void) async throws {
    log("== \(name)")
    let start = Date()
    try await body()
    let elapsed = Date().timeIntervalSince(start)
    log("== \(name) OK (\(String(format: "%.2f", elapsed))s)")
}

private func require(_ condition: Bool, _ message: String) throws {
    if !condition {
        throw SampleError.failed(message)
    }
}

private func describeError(_ error: Error) -> String {
    if let server = error as? CHServerException {
        var parts: [String] = []
        parts.append("CHServerException(code=\(server.code), name=\(server.name))")
        if !server.message.isEmpty {
            parts.append("message=\(server.message)")
        }
        if !server.stackTrace.isEmpty {
            parts.append("stack=\(server.stackTrace)")
        }
        if let nested = server.nested {
            parts.append("nested=\(describeError(nested))")
        }
        return parts.joined(separator: " | ")
    }
    if let clientError = error as? CHClientError {
        return "CHClientError: \(clientError)"
    }
    if let binaryError = error as? CHBinaryError {
        return "CHBinaryError: \(binaryError)"
    }
    if let sample = error as? SampleError {
        return sample.description
    }
    return String(describing: error)
}

private func padData(_ data: Data, length: Int) -> Data {
    if data.count >= length {
        return data.prefix(length)
    }
    var padded = data
    padded.append(contentsOf: repeatElement(0, count: length - data.count))
    return padded
}

private func formatDate(_ date: Date, format: String, timeZone: TimeZone) -> String {
    let formatter = DateFormatter()
    formatter.timeZone = timeZone
    formatter.dateFormat = format
    return formatter.string(from: date)
}

private func makeDate(
    year: Int,
    month: Int,
    day: Int,
    hour: Int = 0,
    minute: Int = 0,
    second: Int = 0,
    millisecond: Int = 0,
    timeZone: TimeZone
) -> Date {
    var calendar = Calendar(identifier: .gregorian)
    calendar.timeZone = timeZone
    var components = DateComponents()
    components.calendar = calendar
    components.timeZone = timeZone
    components.year = year
    components.month = month
    components.day = day
    components.hour = hour
    components.minute = minute
    components.second = second
    components.nanosecond = millisecond * 1_000_000
    return components.date ?? Date(timeIntervalSince1970: 0)
}

private func parseIPv4(_ string: String) -> UInt32 {
    var addr = in_addr()
    let res = string.withCString { cstr in
        inet_pton(AF_INET, cstr, &addr)
    }
    if res == 1 {
        return UInt32(bigEndian: addr.s_addr)
    }
    return 0
}

private func parseIPv6(_ string: String) throws -> Data {
    var addr = in6_addr()
    let res = string.withCString { cstr in
        inet_pton(AF_INET6, cstr, &addr)
    }
    if res == 1 {
        return withUnsafeBytes(of: &addr) { raw in
            Data(raw)
        }
    }
    throw SampleError.failed("Invalid IPv6 literal: \(string)")
}

private func verifyDecimal(_ actual: Any?, expected: Decimal, label: String) throws {
    guard let dec = actual as? Decimal else {
        throw SampleError.failed("Expected Decimal for \(label)")
    }
    let actualStr = NSDecimalNumber(decimal: dec).stringValue
    let expectedStr = NSDecimalNumber(decimal: expected).stringValue
    try require(actualStr == expectedStr, "Decimal mismatch for \(label): \(actualStr) != \(expectedStr)")
}

// Verifies a single row for all supported data types.
private func verifyAllTypesRow(
    row: CHRow,
    expectedIndex: Int,
    tz: TimeZone,
    fixed: Data,
    bin: Data,
    ipv4: UInt32,
    ipv6: Data,
    date: Date,
    date32: Date,
    dt: Date,
    dt64: Date,
    dec: Decimal,
    dec32: Decimal,
    dec64: Decimal,
    dec128: Decimal,
    dec256: Decimal
) throws {
    if expectedIndex == 0 {
        try require((row["i8"] as? Int64) == -5, "i8 mismatch")
        try require((row["i16"] as? Int64) == -300, "i16 mismatch")
        try require((row["i32"] as? Int64) == -40000, "i32 mismatch")
        try require((row["i64"] as? Int64) == -9_000_000_000, "i64 mismatch")
        try require((row["u8"] as? UInt64) == 5, "u8 mismatch")
        try require((row["u16"] as? UInt64) == 65000, "u16 mismatch")
        try require((row["u32"] as? UInt64) == 4_000_000_000, "u32 mismatch")
        try require((row["u64"] as? UInt64) == 9_000_000_000, "u64 mismatch")
        try require((row["f32"] as? Float) == Float(1.25), "f32 mismatch")
        try require((row["f64"] as? Double) == Double(3.14159), "f64 mismatch")
        try require((row["b"] as? Bool) == true, "bool mismatch")
        try require((row["str"] as? String) == "hello", "str mismatch")
        try require((row["fixed"] as? Data) == fixed, "fixed mismatch")
        try require((row["bin"] as? Data) == bin, "bin mismatch")
        try require((row["uuid"] as? UUID)?.uuidString.lowercased() == "00000000-0000-0000-0000-000000000001", "uuid mismatch")
        try require((row["ipv4"] as? UInt32) == ipv4, "ipv4 mismatch")
        try require((row["ipv6"] as? Data) == ipv6, "ipv6 mismatch")
        try require((row["e8"] as? String) == "a", "enum8 mismatch")
        try require((row["e16"] as? String) == "x", "enum16 mismatch")
        try require((row["lc"] as? String) == "alpha", "low cardinality mismatch")
    } else {
        try require((row["i8"] as? Int64) == 7, "i8 mismatch")
        try require((row["i16"] as? Int64) == 301, "i16 mismatch")
        try require((row["i32"] as? Int64) == 50000, "i32 mismatch")
        try require((row["i64"] as? Int64) == 9_000_000_000, "i64 mismatch")
        try require((row["u8"] as? UInt64) == 255, "u8 mismatch")
        try require((row["u16"] as? UInt64) == 42, "u16 mismatch")
        try require((row["u32"] as? UInt64) == 123_456, "u32 mismatch")
        try require((row["u64"] as? UInt64) == 18_000_000_000, "u64 mismatch")
        try require((row["f32"] as? Float) == Float(-2.5), "f32 mismatch")
        try require((row["f64"] as? Double) == Double(-6.022), "f64 mismatch")
        try require((row["b"] as? Bool) == false, "bool mismatch")
        try require((row["str"] as? String) == "world", "str mismatch")
        try require((row["fixed"] as? Data) == fixed, "fixed mismatch")
        try require((row["bin"] as? Data) == bin, "bin mismatch")
        try require((row["uuid"] as? UUID)?.uuidString.lowercased() == "00000000-0000-0000-0000-000000000002", "uuid mismatch")
        try require((row["ipv4"] as? UInt32) == ipv4, "ipv4 mismatch")
        try require((row["ipv6"] as? Data) == ipv6, "ipv6 mismatch")
        try require((row["e8"] as? String) == "b", "enum8 mismatch")
        try require((row["e16"] as? String) == "y", "enum16 mismatch")
        try require((row["lc"] as? String) == "beta", "low cardinality mismatch")
    }

    let dateStr = formatDate(date, format: "yyyy-MM-dd", timeZone: tz)
    let actualDateStr = (row["d"] as? Date).map { formatDate($0, format: "yyyy-MM-dd", timeZone: tz) }
    try require(actualDateStr == dateStr, "date mismatch")

    let date32Str = formatDate(date32, format: "yyyy-MM-dd", timeZone: tz)
    let actualDate32Str = (row["d32"] as? Date).map { formatDate($0, format: "yyyy-MM-dd", timeZone: tz) }
    try require(actualDate32Str == date32Str, "date32 mismatch")

    let dtStr = formatDate(dt, format: "yyyy-MM-dd HH:mm:ss", timeZone: tz)
    let actualDtStr = (row["dt"] as? Date).map { formatDate($0, format: "yyyy-MM-dd HH:mm:ss", timeZone: tz) }
    try require(actualDtStr == dtStr, "datetime mismatch")

    let actualDt64 = row["dt64"] as? Date
    if let actualDt64 {
        let diff = abs(actualDt64.timeIntervalSince1970 - dt64.timeIntervalSince1970)
        try require(diff < 0.002, "datetime64 mismatch (diff \(diff))")
    } else {
        throw SampleError.failed("datetime64 missing")
    }

    try verifyDecimal(row["dec"], expected: dec, label: "dec")
    try verifyDecimal(row["dec32"], expected: dec32, label: "dec32")
    try verifyDecimal(row["dec64"], expected: dec64, label: "dec64")
    try verifyDecimal(row["dec128"], expected: dec128, label: "dec128")
    try verifyDecimal(row["dec256"], expected: dec256, label: "dec256")

    if let arr = row["arr"] as? [Any?] {
        let doubles = arr.compactMap { $0 as? Double }
        if expectedIndex == 0 {
            try require(doubles == [1.25, 2.5], "array mismatch")
        } else {
            try require(doubles == [3.75], "array mismatch")
        }
    } else {
        throw SampleError.failed("array missing")
    }

    if let tuple = row["tuple"] as? [Any?] {
        if expectedIndex == 0 {
            try require((tuple.first as? Int64) == 7, "tuple[0] mismatch")
            try require((tuple.dropFirst().first as? String) == "tuple-a", "tuple[1] mismatch")
        } else {
            try require((tuple.first as? Int64) == 9, "tuple[0] mismatch")
            try require((tuple.dropFirst().first as? String) == "tuple-b", "tuple[1] mismatch")
        }
    } else {
        throw SampleError.failed("tuple missing")
    }

    if let map = row["map"] as? [AnyHashable: Any?] {
        if expectedIndex == 0 {
            let valA = map["a"] as? Int64
            let valB = map["b"] ?? nil
            try require(valA == 1, "map[a] mismatch")
            try require(valB == nil, "map[b] mismatch")
        } else {
            let valA = map["a"] as? Int64
            try require(valA == 2, "map[a] mismatch")
        }
    } else {
        throw SampleError.failed("map missing")
    }

    if expectedIndex == 0 {
        try require(row["ndate"] == nil, "nullable date mismatch")
        let ndt = row["ndt"] as? Date
        try require(ndt != nil, "nullable datetime mismatch")
    } else {
        let ndate = row["ndate"] as? Date
        try require(ndate != nil, "nullable date mismatch")
        try require(row["ndt"] == nil, "nullable datetime mismatch")
    }

    // Nothing is validated separately because ClickHouse does not allow Nothing columns in tables.
}
