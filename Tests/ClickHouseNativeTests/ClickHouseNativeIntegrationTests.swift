import Testing
import Foundation
@testable import ClickHouseNative
import NIOCore
import NIOPosix

@Test func integrationSmoke_compressionOffAndOn() async throws {
    let env = ProcessInfo.processInfo.environment
    guard env["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        var base = try requireClickHouseConfig()

        base.compressionEnabled = false
        do {
            let client = try await ClickHouseClient(config: base)
            do {
                try await client.execute("SELECT 1")
            } catch {
                await client.close()
                throw error
            }
            await client.close()
        }

        base.compressionEnabled = true
        do {
            let client = try await ClickHouseClient(config: base)
            do {
                try await client.execute("SELECT 1")
            } catch {
                await client.close()
                throw error
            }
            await client.close()
        }
    }
}

@Test func integrationClientAPIs() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()

        let client = try await ClickHouseClient(config: config)
        do {
            #expect(try await client.ping())

            struct UserRow: Decodable, Sendable {
                let userId: Int64
                let userName: String
            }

            let one = try await client.queryOne("SELECT toInt64(7)", as: Int64.self)
            #expect(one == 7)

            let rows = try await client.queryRows("SELECT toInt64(42) AS user_id, 'Bob' AS user_name", as: UserRow.self)
            var first: UserRow?
            for try await row in rows {
                first = row
                break
            }
            #expect(first?.userId == 42)
            #expect(first?.userName == "Bob")

            let result = try await client.query(
                "SELECT toUInt64(number) AS n FROM numbers(10)",
                settings: ["max_block_size": .int32(1)]
            )
            var blockCount = 0
            var total = 0
            for try await block in result.blocks {
                blockCount += 1
                total += block.rowCount
            }
            #expect(total == 10)
            #expect(blockCount > 1)
        } catch {
            await client.close()
            throw error
        }
        await client.close()
    }
}

@Test func integrationQueryOptions_queryIdAndStage() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()

        let clientA = try await ClickHouseClient(config: config)
        let clientB = try await ClickHouseClient(config: config)
        do {
            let queryId = "swift-query-id-\(UUID().uuidString)"
            let running = Task {
                try await clientA.execute(
                    "SELECT sleep(1)",
                    settings: [:],
                    options: CHQueryOptions(queryId: queryId, stage: .complete)
                )
            }

            try await Task.sleep(nanoseconds: 150_000_000)
            let seen = try await clientB.queryOne(
                "SELECT count() FROM system.processes WHERE query_id = '\(queryId)'",
                as: UInt64.self
            )
            #expect((seen ?? 0) >= 1)

            _ = try await running.value

            _ = try await clientA.query(
                "SELECT number FROM numbers(5)",
                settings: [:],
                options: CHQueryOptions(stage: .fetchColumns)
            )
        } catch {
            await clientA.close()
            await clientB.close()
            throw error
        }
        await clientA.close()
        await clientB.close()
    }
}

@Test func integrationQueryRows_earlyCancelDoesNotPoisonConnection() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        var config = try requireClickHouseConfig()
        config.compressionEnabled = false

        let client = try await ClickHouseClient(config: config)
        do {
            struct Row: Decodable, Sendable { let n: UInt64 }
            let rows = try await client.queryRows(
                "SELECT toUInt64(number) AS n FROM numbers(1000)",
                as: Row.self
            )
            var seen = false
            for try await row in rows {
                _ = row.n
                seen = true
                break
            }
            #expect(seen)

            try await client.execute("SELECT 1")
        } catch {
            await client.close()
            throw error
        }
        await client.close()
    }
}

@Test func integrationQueryResult_drainReleasesConnection() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()

        let client = try await ClickHouseClient(config: config)
        do {
            let result = try await client.query(
                "SELECT toUInt64(number) AS n FROM numbers(1000)",
                settings: ["max_block_size": .int32(10)]
            )
            try await result.drain()
            try await client.execute("SELECT 1")
        } catch {
            await client.close()
            throw error
        }
        await client.close()
    }
}

@Test func integrationConcurrency_twoQueriesSameClient() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let client = try await ClickHouseClient(config: config)
        do {
            async let a: Int64? = client.queryOne("SELECT toInt64(1)", as: Int64.self)
            async let b: Int64? = client.queryOne("SELECT toInt64(2)", as: Int64.self)
            let (ra, rb) = try await (a, b)
            #expect(ra == 1)
            #expect(rb == 2)
        } catch {
            await client.close()
            throw error
        }
        await client.close()
    }
}

@Test func integrationEarlyCancel_queryDoesNotPoisonConnection() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        var config = try requireClickHouseConfig()
        config.compressionEnabled = false

        let client = try await ClickHouseClient(config: config)
        do {
            let result = try await client.query(
                "SELECT toUInt64(number) AS n FROM numbers(1000)",
                settings: ["max_block_size": .int32(10)]
            )
            var seen = false
            for try await row in result.rows() {
                _ = row["n"] as? UInt64
                seen = true
                break
            }
            #expect(seen)

            try await client.execute("SELECT 1")
        } catch {
            await client.close()
            throw error
        }
        await client.close()
    }
}

@Test func integrationErrorHandling_serverExceptionDoesNotBreakSession() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()

        let client = try await ClickHouseClient(config: config)
        do {
            do {
                _ = try await client.queryOne("SELEC 1", as: Int64.self)
                #expect(Bool(false))
            } catch is CHServerException {
                #expect(Bool(true))
            }

            let ok = try await client.queryOne("SELECT toInt64(1)", as: Int64.self)
            #expect(ok == 1)
        } catch {
            await client.close()
            throw error
        }
        await client.close()
    }
}

@Test func integrationTimeout_queryTimeoutWithDockerPause_recovers() async throws {
    let env = ProcessInfo.processInfo.environment
    guard env["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    guard env["CLICKHOUSE_DOCKER_CONTAINER_NAME"] != nil else { return }

    try await ClickHouseIntegrationLock.shared.withLock {
        var config = try requireClickHouseConfig()
        config.compressionEnabled = false
        config.connectTimeout = 5
        config.queryTimeout = 1

        let container = env["CLICKHOUSE_DOCKER_CONTAINER_NAME"] ?? "clickhouse-native-test"

        let client = try await ClickHouseClient(config: config)
        do {
            try await client.execute("SELECT 1")

            try await dockerPause(container: container)
            var sawFailure = false
            do {
                try await client.execute("SELECT sleep(10)")
            } catch {
                sawFailure = true
                // Depending on where the TCP failure is observed, we may see either our timeout wrapper
                // or a lower-level NIO error like "I/O on closed channel".
                #expect(error is CHClientError || error is ChannelError)
            }
            #expect(sawFailure)

            try await dockerUnpause(container: container)
            try await dockerWaitReady(container: container, user: config.user, password: config.password)

            // The first request may still hit a stale connection; the second must succeed.
            _ = try? await client.execute("SELECT 1")
            try await client.execute("SELECT 1")
        } catch {
            // Best-effort unpause to avoid poisoning subsequent tests.
            _ = try? await dockerUnpause(container: container)
            await client.close()
            throw error
        }
        await client.close()
    }
}

@Test func integrationTimeout_connectTimeoutDuringHello() async throws {
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let (port, closeServer) = try await startBlackholeServer()
        defer { closeServer() }

        var config = CHConfig(host: "127.0.0.1", port: port, database: "default", user: "default", password: "")
        config.compressionEnabled = false
        config.connectTimeout = 0.5
        config.queryTimeout = 0.5

        do {
            _ = try await ClickHouseClient(config: config)
            #expect(Bool(false))
        } catch is CHClientError {
            #expect(Bool(true))
        }
    }
}

@Test func integrationFailover_connectsToHealthyHost() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let (badPort, closeServer) = try await startBlackholeServer()
        defer { closeServer() }

        var config = try requireClickHouseConfig()
        config.connectTimeout = 1
        config.hosts = [
            CHHost(host: "127.0.0.1", port: badPort),
            CHHost(host: config.host, port: config.port),
        ]
        config.hostSelectionPolicy = .roundRobin

        let client = try await ClickHouseClient(config: config)
        do {
            #expect(try await client.ping())
            try await client.execute("SELECT 1")
        } catch {
            await client.close()
            throw error
        }
        await client.close()
    }
}

@Test func integrationSocketOptions_applied() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        var config = try requireClickHouseConfig()
        config.tcpKeepAlive = true
        config.socketSendBufferBytes = 128 * 1024
        config.socketRecvBufferBytes = 128 * 1024

        let connection = try await CHNIOConnection.connect(config: config)
        do {
            let options = try await connection.readSocketOptions()
            #expect(options.keepAlive)
            #expect(options.sendBuffer >= 128 * 1024)
            #expect(options.recvBuffer >= 128 * 1024)
        } catch {
            await connection.close()
            throw error
        }
        await connection.close()
    }
}

@Test func integrationTLS_smoke_verifyOff_succeeds() async throws {
    let env = ProcessInfo.processInfo.environment
    guard let host = env["CLICKHOUSE_TLS_HOST"] else { return }
    let port = Int(env["CLICKHOUSE_TLS_PORT"] ?? "0") ?? 0
    guard port > 0 else { return }
    guard !restartOnlyMode() else { return }

    try await ClickHouseIntegrationLock.shared.withLock {
        var cfg = try requireClickHouseConfig()
        cfg.host = host
        cfg.port = port
        cfg.compressionEnabled = false
        cfg.tlsEnabled = true
        cfg.tlsVerify = false
        cfg.connectTimeout = 5
        cfg.queryTimeout = 10

        let client = try await ClickHouseClient(config: cfg)
        do {
            #expect(try await client.ping())
            try await client.execute("SELECT 1")
        } catch {
            await client.close()
            throw error
        }
        await client.close()
    }
}

@Test func integrationTLS_smoke_verifyOn_withCA_succeeds() async throws {
    let env = ProcessInfo.processInfo.environment
    guard let host = env["CLICKHOUSE_TLS_HOST"] else { return }
    let port = Int(env["CLICKHOUSE_TLS_PORT"] ?? "0") ?? 0
    guard port > 0 else { return }
    guard let caPath = env["CLICKHOUSE_TLS_CA_PATH"] else { return }
    guard !restartOnlyMode() else { return }

    try await ClickHouseIntegrationLock.shared.withLock {
        var cfg = try requireClickHouseConfig()
        cfg.host = host
        cfg.port = port
        cfg.compressionEnabled = false
        cfg.tlsEnabled = true
        cfg.tlsVerify = true
        cfg.tlsCAFilePath = caPath
        cfg.connectTimeout = 5
        cfg.queryTimeout = 10

        let client = try await ClickHouseClient(config: cfg)
        do {
            #expect(try await client.ping())
            try await client.execute("SELECT 1")
        } catch {
            await client.close()
            throw error
        }
        await client.close()
    }
}

@Test func integrationTLS_smoke_verifyOn_failsWithSelfSigned() async throws {
    let env = ProcessInfo.processInfo.environment
    guard let host = env["CLICKHOUSE_TLS_HOST"] else { return }
    let port = Int(env["CLICKHOUSE_TLS_PORT"] ?? "0") ?? 0
    guard port > 0 else { return }
    guard !restartOnlyMode() else { return }

    try await ClickHouseIntegrationLock.shared.withLock {
        var cfg = try requireClickHouseConfig()
        cfg.host = host
        cfg.port = port
        cfg.compressionEnabled = false
        cfg.tlsEnabled = true
        cfg.tlsVerify = true
        cfg.tlsCAFilePath = nil
        cfg.tlsCABytes = nil
        cfg.connectTimeout = 5
        cfg.queryTimeout = 10

        do {
            _ = try await ClickHouseClient(config: cfg)
            #expect(Bool(false))
        } catch {
            #expect(Bool(true))
        }
    }
}

@Test func integrationTypeAliases_boolBinaryNothing() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let table = uniqueTableName(prefix: "swift_native_alias")
        let client = try await ClickHouseClient(config: config)
        do {
            try await client.execute("DROP TABLE IF EXISTS \(table)")
            try await client.execute("""
            CREATE TABLE \(table) (
              b Bool,
              nb Nullable(Bool),
              bin Binary(3)
            ) ENGINE=Memory
            """)

            var builder = CHBlockBuilder()
            builder.addColumn(name: "b", type: CHBoolType(), values: [true, false])
            builder.addColumn(name: "nb", type: CHNullableType(nested: CHBoolType()), values: [false, nil as Bool?])
            builder.addColumn(name: "bin", type: CHFixedStringType(length: 3), values: [Data("abc".utf8), Data([0x78, 0x79, 0x00])])
            let block = try builder.build()
            try await client.insert(into: table, block: block)

            let result = try await client.query("SELECT b, nb, bin FROM \(table) ORDER BY b DESC")
            var rows: [CHRow] = []
            for try await row in result.rows() {
                rows.append(row)
            }
            #expect(rows.count == 2)
            let r0 = rows[0]
            #expect((r0["b"] as? Bool) == true)
            #expect((r0["nb"] as? Bool) == false)
            #expect((r0["bin"] as? Data) == Data("abc".utf8))

            let r1 = rows[1]
            #expect((r1["b"] as? Bool) == false)
            #expect(r1["nb"] == nil)
            #expect((r1["bin"] as? Data) == Data([0x78, 0x79, 0x00]))

            let nullResult = try await client.query("SELECT NULL AS n")
            var nullValue: Any?
            for try await row in nullResult.rows() {
                nullValue = row["n"]
                break
            }
            #expect(nullValue == nil)
        } catch {
            try? await client.execute("DROP TABLE IF EXISTS \(table)")
            await client.close()
            throw error
        }
        try? await client.execute("DROP TABLE IF EXISTS \(table)")
        await client.close()
    }
}

@Test func integrationDateTime_timezoneAware() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let client = try await ClickHouseClient(config: config)
        do {
            let result = try await client.query("""
            SELECT
              toDateTime('2020-01-01 00:00:00', 'Asia/Tokyo') AS dt,
              toDateTime64('2020-01-01 00:00:00', 3, 'Asia/Tokyo') AS dt64
            """)
            var block: CHBlock?
            for try await b in result.blocks {
                if b.rowCount > 0 {
                    block = b
                    break
                }
            }
            guard let firstBlock = block else {
                throw CHBinaryError.malformed("No data block returned for timezone query")
            }
            guard firstBlock.columns.count == 2 else {
                throw CHBinaryError.malformed("Unexpected column count for timezone query")
            }
            let dtColumn = firstBlock.columns[0]
            let dt64Column = firstBlock.columns[1]
            let dtValue = dtColumn.values.first as? Date
            let dt64Value = dt64Column.values.first as? Date
            let dtType = dtColumn.type as? CHDateTimeType
            let dt64Type = dt64Column.type as? CHDateTime64Type
            #expect(dtType?.timezone.identifier == "Asia/Tokyo")
            #expect(dt64Type?.timezone.identifier == "Asia/Tokyo")

            let formatter = DateFormatter()
            formatter.timeZone = dtType?.timezone
            formatter.dateFormat = "yyyy-MM-dd HH:mm:ss"
            #expect(formatter.string(from: dtValue ?? Date(timeIntervalSince1970: 0)) == "2020-01-01 00:00:00")

            let formatter64 = DateFormatter()
            formatter64.timeZone = dt64Type?.timezone
            formatter64.dateFormat = "yyyy-MM-dd HH:mm:ss.SSS"
            #expect(formatter64.string(from: dt64Value ?? Date(timeIntervalSince1970: 0)) == "2020-01-01 00:00:00.000")
        } catch {
            await client.close()
            throw error
        }
        await client.close()
    }
}

@Test func integrationNestedTypes_deepCombinations() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let table = uniqueTableName(prefix: "swift_native_nested_deep")
        let client = try await ClickHouseClient(config: config)
        do {
            try await client.execute("DROP TABLE IF EXISTS \(table)")
            try await client.execute("""
            CREATE TABLE \(table) (
              aa Array(Array(String)),
              tt Tuple(Array(Nullable(String)), Nullable(UInt8)),
              at Array(Tuple(UInt32, Nullable(String)))
            ) ENGINE=Memory
            """)

            let aaType = CHArrayType(nested: CHArrayType(nested: CHStringType()))
            let ttType = CHTupleType(nested: [
                CHArrayType(nested: CHNullableType(nested: CHStringType())),
                CHNullableType(nested: CHUInt8Type()),
            ])
            let atType = CHArrayType(nested: CHTupleType(nested: [
                CHUInt32Type(),
                CHNullableType(nested: CHStringType()),
            ]))

            var builder = CHBlockBuilder()
            builder.addColumn(name: "aa", type: aaType, values: [
                [["a", "b"], []],
                [["x"], ["y", "z"]],
            ])
            builder.addColumn(name: "tt", type: ttType, values: [
                [["x", nil], UInt64(7)],
                [[], nil],
            ])
            builder.addColumn(name: "at", type: atType, values: [
                [[UInt64(1), "q"], [UInt64(2), nil]],
                [[UInt64(42), "z"]],
            ])

            let block = try builder.build()
            try await client.insert(into: table, block: block)

            let result = try await client.query("SELECT aa, tt, at FROM \(table) ORDER BY length(aa)")
            var rows: [CHRow] = []
            for try await row in result.rows() { rows.append(row) }
            #expect(rows.count == 2)

            let r1 = rows[0]
            let aa1 = r1["aa"] as? [Any?]
            #expect((aa1?.count) == 2)
            let tt1 = r1["tt"] as? [Any?]
            let tt1Arr = tt1?.first as? [Any?]
            #expect((tt1Arr?.count) == 2)
            #expect(tt1Arr?[1] == nil)
            let at1 = r1["at"] as? [Any?]
            #expect((at1?.count) == 2)

            let r2 = rows[1]
            let aa2 = r2["aa"] as? [Any?]
            #expect((aa2?.count) == 2)
            let tt2 = r2["tt"] as? [Any?]
            let tt2Arr = tt2?.first as? [Any?]
            #expect((tt2Arr?.isEmpty) == true)
            #expect(tt2?[1] == nil)
        } catch {
            try? await client.execute("DROP TABLE IF EXISTS \(table)")
            await client.close()
            throw error
        }
        try? await client.execute("DROP TABLE IF EXISTS \(table)")
        await client.close()
    }
}

@Test func integrationDateTime64_scale9_minMax() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let table = uniqueTableName(prefix: "swift_native_dt64_9")
        let client = try await ClickHouseClient(config: config)
        do {
            try await client.execute("DROP TABLE IF EXISTS \(table)")
            try await client.execute("""
            CREATE TABLE \(table) (
              seq UInt8,
              dt DateTime64(9, 'UTC')
            ) ENGINE=Memory
            """)

            try await client.execute("""
            INSERT INTO \(table) VALUES
              (1, toDateTime64('1970-01-01 00:00:00.000000000', 9, 'UTC')),
              (2, toDateTime64('2000-01-01 00:01:01.123000000', 9, 'UTC')),
              (3, toDateTime64('2105-12-31 23:59:59.999000000', 9, 'UTC'))
            """)

            let result = try await client.query("SELECT seq, dt FROM \(table) ORDER BY seq")
            var rows: [CHRow] = []
            for try await row in result.rows() { rows.append(row) }
            #expect(rows.count == 3)

            let formatter = DateFormatter()
            formatter.timeZone = TimeZone(identifier: "UTC")
            formatter.dateFormat = "yyyy-MM-dd HH:mm:ss.SSSSSSSSS"

            let r1 = rows[0]
            let d1 = r1["dt"] as? Date
            #expect(formatter.string(from: d1 ?? Date(timeIntervalSince1970: 0)) == "1970-01-01 00:00:00.000000000")

            let r2 = rows[1]
            let d2 = r2["dt"] as? Date
            #expect(formatter.string(from: d2 ?? Date(timeIntervalSince1970: 0)) == "2000-01-01 00:01:01.123000000")

            let r3 = rows[2]
            let d3 = r3["dt"] as? Date
            #expect(formatter.string(from: d3 ?? Date(timeIntervalSince1970: 0)) == "2105-12-31 23:59:59.999000000")
        } catch {
            try? await client.execute("DROP TABLE IF EXISTS \(table)")
            await client.close()
            throw error
        }
        try? await client.execute("DROP TABLE IF EXISTS \(table)")
        await client.close()
    }
}

@Test func integrationGenerateRandom_smokeTypes() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let table = uniqueTableName(prefix: "swift_native_random")
        let client = try await ClickHouseClient(config: config)
        do {
            try await client.execute("DROP TABLE IF EXISTS \(table)")
            do {
                try await client.execute("""
                CREATE TABLE \(table) (
                  name String,
                  value UInt32,
                  arr Array(Float64),
                  day Date,
                  time DateTime,
                  dc Decimal(7,2)
                ) ENGINE=GenerateRandom(1, 8, 8)
                """)
            } catch let ex as CHServerException {
                if ex.message.contains("Unknown table engine") {
                    return
                }
                throw ex
            }

            let result = try await client.query("SELECT * FROM \(table) LIMIT 10000")
            var seen = 0
            for try await row in result.rows() {
                seen += 1
                #expect(row["name"] is String)
                #expect(row["value"] is UInt64)
                #expect(row["arr"] is [Any?])
                #expect(row["day"] is Date)
                #expect(row["time"] is Date)
                #expect(row["dc"] is Decimal)
            }
            #expect(seen == 10000)
        } catch {
            try? await client.execute("DROP TABLE IF EXISTS \(table)")
            await client.close()
            throw error
        }
        try? await client.execute("DROP TABLE IF EXISTS \(table)")
        await client.close()
    }
}

@Test func integrationWideTable_insertManyColumns() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let table = uniqueTableName(prefix: "swift_native_wide")
        let client = try await ClickHouseClient(config: config)
        do {
            try await client.execute("DROP TABLE IF EXISTS \(table)")
            let columnCount = 36
            let columns = (0..<columnCount).map { "t_\($0) String" }.joined(separator: ", ")
            try await client.execute("CREATE TABLE \(table) (\(columns)) ENGINE=Memory")

            var builder = CHBlockBuilder()
            let rows = 100
            for i in 0..<columnCount {
                let name = "t_\(i)"
                let values = (0..<rows).map { "String\(i)-\($0)" }
                builder.addColumn(name: name, type: CHStringType(), values: values)
            }
            let block = try builder.build()
            try await client.insert(into: table, block: block)

            let count = try await client.queryOne("SELECT count() FROM \(table)", as: UInt64.self)
            #expect(count == 100)
        } catch {
            try? await client.execute("DROP TABLE IF EXISTS \(table)")
            await client.close()
            throw error
        }
        try? await client.execute("DROP TABLE IF EXISTS \(table)")
        await client.close()
    }
}

@Test func integrationQueryScanStats_progressMatchesQueryLog() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let table = uniqueTableName(prefix: "swift_native_scanstat")
        let client = try await ClickHouseClient(config: config)
        do {
            let queryLogExists = try await client.queryOne(
                "SELECT count() FROM system.tables WHERE database='system' AND name='query_log'",
                as: UInt64.self
            ) ?? 0
            if queryLogExists == 0 { return }

            try await client.execute("DROP TABLE IF EXISTS \(table)")
            try await client.execute("""
            CREATE TABLE \(table) (
              c1 UInt32
            ) ENGINE=MergeTree()
            PARTITION BY tuple()
            ORDER BY tuple()
            """)
            try await client.execute("INSERT INTO \(table) SELECT number FROM system.numbers LIMIT 100000")

            let queryId = "swift-scan-\(UUID().uuidString)"
            var progressRows: UInt64 = 0
            var progressBytes: UInt64 = 0
            let events = try await client.queryEvents(
                "SELECT c1 FROM \(table) LIMIT 1000",
                settings: ["log_queries": .int32(1)],
                options: CHQueryOptions(queryId: queryId)
            )
            for try await event in events {
                if case .progress(let progress) = event {
                    progressRows += progress.newRows
                    progressBytes += progress.newBytes
                }
            }

            if progressRows == 0 { return }

            if let stats = try await waitForQueryLogRow(client: client, queryId: queryId, timeoutSeconds: 15) {
                let diffRows = stats.readRows > progressRows ? stats.readRows - progressRows : progressRows - stats.readRows
                let diffBytes = stats.readBytes > progressBytes ? stats.readBytes - progressBytes : progressBytes - stats.readBytes
                let maxRowDiff = max(UInt64(1), stats.readRows / 20)
                let maxByteDiff = max(UInt64(1), stats.readBytes / 20)
                #expect(diffRows <= maxRowDiff)
                #expect(diffBytes <= maxByteDiff)
            }
        } catch {
            try? await client.execute("DROP TABLE IF EXISTS \(table)")
            await client.close()
            throw error
        }
        try? await client.execute("DROP TABLE IF EXISTS \(table)")
        await client.close()
    }
}

@Test func integrationNullableSorting_semantics() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let table = uniqueTableName(prefix: "swift_native_nullable_sort")
        let client = try await ClickHouseClient(config: config)
        do {
            try await client.execute("DROP TABLE IF EXISTS \(table)")
            try await client.execute("CREATE TABLE \(table) (v Nullable(UInt8)) ENGINE=Memory")
            try await client.execute("INSERT INTO \(table) VALUES (NULL),(1),(3),(NULL)")

            let result = try await client.query("SELECT v FROM \(table) ORDER BY isNull(v) ASC, v ASC")
            var values: [Any?] = []
            for try await row in result.rows() {
                values.append(row["v"])
            }
            #expect(values.count == 4)
            #expect(values[0] as? UInt64 == 1)
            #expect(values[1] as? UInt64 == 3)
            #expect(values[2] == nil)
            #expect(values[3] == nil)
        } catch {
            try? await client.execute("DROP TABLE IF EXISTS \(table)")
            await client.close()
            throw error
        }
        try? await client.execute("DROP TABLE IF EXISTS \(table)")
        await client.close()
    }
}

@Test func integrationEnum16_roundtrip() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let table = uniqueTableName(prefix: "swift_native_enum16")
        let client = try await ClickHouseClient(config: config)
        do {
            try await client.execute("DROP TABLE IF EXISTS \(table)")
            try await client.execute("""
            CREATE TABLE \(table) (
              e Enum16('a' = 1, 'b' = 2, 'c' = 300)
            ) ENGINE=Memory
            """)

            var builder = CHBlockBuilder()
            builder.addColumn(name: "e", type: CHEnum16Type(names: ["a", "b", "c"], values: [1, 2, 300]), values: ["a", "c"])
            let block = try builder.build()
            try await client.insert(into: table, block: block)

            let result = try await client.query("SELECT e FROM \(table) ORDER BY e")
            var values: [String] = []
            for try await row in result.rows() {
                if let v = row["e"] as? String { values.append(v) }
            }
            #expect(values == ["a", "c"])
        } catch {
            try? await client.execute("DROP TABLE IF EXISTS \(table)")
            await client.close()
            throw error
        }
        try? await client.execute("DROP TABLE IF EXISTS \(table)")
        await client.close()
    }
}

@Test func integrationFixedString_paddingAndTruncation() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let table = uniqueTableName(prefix: "swift_native_fixed")
        let client = try await ClickHouseClient(config: config)
        do {
            try await client.execute("DROP TABLE IF EXISTS \(table)")
            try await client.execute("CREATE TABLE \(table) (s FixedString(3)) ENGINE=Memory")

            var builder = CHBlockBuilder()
            builder.addColumn(name: "s", type: CHFixedStringType(length: 3), values: [
                Data("abcd".utf8),
                Data("a".utf8),
                Data("abc".utf8),
            ])
            let block = try builder.build()
            try await client.insert(into: table, block: block)

            let result = try await client.query("SELECT s FROM \(table)")
            var rows: [Data] = []
            for try await row in result.rows() {
                if let d = row["s"] as? Data { rows.append(d) }
            }
            #expect(rows.count == 3)
            #expect(rows[0] == Data([0x61, 0x62, 0x63]))
            #expect(rows[1] == Data([0x61, 0x00, 0x00]))
            #expect(rows[2] == Data([0x61, 0x62, 0x63]))
        } catch {
            try? await client.execute("DROP TABLE IF EXISTS \(table)")
            await client.close()
            throw error
        }
        try? await client.execute("DROP TABLE IF EXISTS \(table)")
        await client.close()
    }
}

@Test func integrationMap_nullableValues() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let table = uniqueTableName(prefix: "swift_native_map_nullable")
        let client = try await ClickHouseClient(config: config)
        do {
            try await client.execute("DROP TABLE IF EXISTS \(table)")
            try await client.execute("CREATE TABLE \(table) (m Map(String, Nullable(UInt32))) ENGINE=Memory")

            var builder = CHBlockBuilder()
            builder.addColumn(
                name: "m",
                type: CHMapType(key: CHStringType(), value: CHNullableType(nested: CHUInt32Type())),
                values: [
                    ["a": UInt64(1), "b": nil],
                    ["x": UInt64(7)],
                ]
            )
            let block = try builder.build()
            try await client.insert(into: table, block: block)

            let result = try await client.query("SELECT m FROM \(table) ORDER BY length(m)")
            var rows: [[AnyHashable: Any?]] = []
            for try await row in result.rows() {
                if let map = row["m"] as? [AnyHashable: Any?] { rows.append(map) }
            }
            #expect(rows.count == 2)
            #expect(rows[0]["x"] as? UInt64 == 7)
            #expect(rows[1]["a"] as? UInt64 == 1)
            #expect(rows[1].keys.contains("b"))
            let bValue = rows[1]["b"]
            #expect(bValue != nil)
            #expect(bValue! == nil)
        } catch {
            try? await client.execute("DROP TABLE IF EXISTS \(table)")
            await client.close()
            throw error
        }
        try? await client.execute("DROP TABLE IF EXISTS \(table)")
        await client.close()
    }
}

@Test func integrationInsertRoundtrip_primitivesAndSpecialTypes() async throws {
    let env = ProcessInfo.processInfo.environment
    guard env["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()

        let table = uniqueTableName(prefix: "swift_native_primitives")
        let client = try await ClickHouseClient(config: config)
        do {
            try await client.execute("DROP TABLE IF EXISTS \(table)")
            try await client.execute("""
            CREATE TABLE \(table) (
              i8 Int8,
              i32 Int32,
              u64 UInt64,
              f64 Float64,
              s String,
              fs FixedString(6),
              d Date,
              dt DateTime,
              dt64 DateTime64(3),
              dec Decimal(18,4),
              id UUID,
              e Enum8('a' = 1, 'b' = 2),
              ip4 IPv4,
              ip6 IPv6,
              lc LowCardinality(String)
            ) ENGINE=Memory
            """)

            let date = Date(timeIntervalSince1970: 86_400 * 20_000)
            let dt = Date(timeIntervalSince1970: 1_700_000_000)
            let dt64 = Date(timeIntervalSince1970: 1_700_000_000.123)
            let dec1 = Decimal(string: "1234.5678")!
            let dec2 = Decimal(string: "-0.0001")!
            let uuid1 = UUID(uuidString: "00000000-0000-0000-0000-000000000001")!
            let uuid2 = UUID(uuidString: "00000000-0000-0000-0000-000000000002")!
            let ip4_1 = "127.0.0.1"
            let ip4_2 = "10.0.0.1"
            let ip6_1 = "2001:db8::1"
            let ip6_2 = "::1"

            var builder = CHBlockBuilder()
            builder.addColumn(name: "i8", type: CHInt8Type(), values: [Int64(-5), Int64(7)])
            builder.addColumn(name: "i32", type: CHInt32Type(), values: [Int64(-123_456), Int64(123_456)])
            builder.addColumn(name: "u64", type: CHUInt64Type(), values: [UInt64(1), UInt64(2)])
            builder.addColumn(name: "f64", type: CHFloat64Type(), values: [Double(1.25), Double(-2.5)])
            builder.addColumn(name: "s", type: CHStringType(), values: ["hello", "world"])
            builder.addColumn(name: "fs", type: CHFixedStringType(length: 6), values: [Data("abc".utf8), Data("xyz123".utf8)])
            builder.addColumn(name: "d", type: CHDateType(), values: [date, date])
            builder.addColumn(name: "dt", type: CHDateTimeType(), values: [dt, dt])
            builder.addColumn(name: "dt64", type: CHDateTime64Type(scale: 3, timezone: nil), values: [dt64, dt64])
            builder.addColumn(name: "dec", type: CHDecimalType(precision: 18, scale: 4), values: [dec1, dec2])
            builder.addColumn(name: "id", type: CHUUIDType(), values: [uuid1, uuid2])
            builder.addColumn(name: "e", type: CHEnum8Type(names: ["a", "b"], values: [1, 2]), values: ["a", "b"])
            builder.addColumn(name: "ip4", type: CHIPv4Type(), values: [ip4_1, ip4_2])
            builder.addColumn(name: "ip6", type: CHIPv6Type(), values: [ip6_1, ip6_2])
            builder.addColumn(name: "lc", type: CHLowCardinalityType(nested: CHStringType()), values: ["x", "x"])

            let block = try builder.build()
            try await client.insert(into: table, block: block)

            let result = try await client.query("""
            SELECT i8, i32, u64, f64, s, fs, d, dt, dt64, dec, id, e, ip4, ip6, lc
            FROM \(table)
            ORDER BY u64
            """)

            var rows: [CHRow] = []
            for try await row in result.rows() { rows.append(row) }
            #expect(rows.count == 2)

            let r1 = rows[0]
            #expect((r1["i8"] as? Int64) == -5)
            #expect((r1["i32"] as? Int64) == -123_456)
            #expect((r1["u64"] as? UInt64) == 1)
            #expect(abs((r1["f64"] as? Double ?? 0) - 1.25) < 0.000_000_1)
            #expect((r1["s"] as? String) == "hello")
            #expect((r1["fs"] as? Data) == Data([0x61, 0x62, 0x63, 0x00, 0x00, 0x00]))

            let d1 = r1["d"] as? Date
            #expect(Int(d1?.timeIntervalSince1970 ?? 0) == Int(date.timeIntervalSince1970))

            let dt1 = r1["dt"] as? Date
            #expect(Int(dt1?.timeIntervalSince1970 ?? 0) == Int(dt.timeIntervalSince1970))

            let dt64_1 = r1["dt64"] as? Date
            #expect(abs((dt64_1?.timeIntervalSince1970 ?? 0) - dt64.timeIntervalSince1970) < 0.001)

            let decOut1 = r1["dec"] as? Decimal
            #expect(NSDecimalNumber(decimal: decOut1 ?? 0).stringValue == NSDecimalNumber(decimal: dec1).stringValue)

            #expect((r1["id"] as? UUID) == uuid1)
            #expect((r1["e"] as? String) == "a")

            let ip4Out = r1["ip4"] as? UInt32
            #expect(ip4Out == parseIPv4(ip4_1))
            let ip6Out = r1["ip6"] as? Data
            #expect(ip6Out == parseIPv6(ip6_1))

            #expect((r1["lc"] as? String) == "x")
        } catch {
            try? await client.execute("DROP TABLE IF EXISTS \(table)")
            await client.close()
            throw error
        }
        try? await client.execute("DROP TABLE IF EXISTS \(table)")
        await client.close()
    }
}

@Test func integrationInsertRoundtrip_nestedTypes() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()

        let table = uniqueTableName(prefix: "swift_native_nested")
        let client = try await ClickHouseClient(config: config)
        do {
            try await client.execute("DROP TABLE IF EXISTS \(table)")
            try await client.execute("""
            CREATE TABLE \(table) (
              n Nullable(Int32),
              arr Array(UInt16),
              arrN Array(Nullable(String)),
              m Map(String, UInt32),
              t Tuple(Int32, String)
            ) ENGINE=Memory
            """)

            var builder = CHBlockBuilder()
            builder.addColumn(name: "n", type: CHNullableType(nested: CHInt32Type()), values: [Int64(1), nil])
            builder.addColumn(name: "arr", type: CHArrayType(nested: CHUInt16Type()), values: [[UInt64(1), UInt64(2)], [UInt64(42)]])
            builder.addColumn(name: "arrN", type: CHArrayType(nested: CHNullableType(nested: CHStringType())), values: [["a", nil, "b"], []])
            builder.addColumn(name: "m", type: CHMapType(key: CHStringType(), value: CHUInt32Type()), values: [["a": UInt64(1), "b": UInt64(2)], [:]])
            builder.addColumn(name: "t", type: CHTupleType(nested: [CHInt32Type(), CHStringType()]), values: [[Int64(7), "x"], [Int64(-1), "y"]])
            let block = try builder.build()

            try await client.insert(into: table, block: block)

            let result = try await client.query("SELECT n, arr, arrN, m, t FROM \(table) ORDER BY length(arr)")
            var rows: [CHRow] = []
            for try await row in result.rows() { rows.append(row) }
            #expect(rows.count == 2)

            let r1 = rows[0]
            #expect((r1["n"] as? Int64) == nil)
            #expect((r1["arr"] as? [Any?])?.count == 1)
            #expect(((r1["arr"] as? [Any?])?.first as? UInt64) == 42)
            #expect((r1["arrN"] as? [Any?])?.isEmpty == true)
            #expect(((r1["m"] as? [AnyHashable: Any?])?.isEmpty) == true)

            let tuple1 = r1["t"] as? [Any?]
            #expect((tuple1?.first as? Int64) == -1)
            #expect((tuple1?.dropFirst().first as? String) == "y")

            let r2 = rows[1]
            #expect((r2["n"] as? Int64) == 1)
            let arr2 = r2["arr"] as? [Any?]
            #expect((arr2?.count) == 2)
            #expect((arr2?[0] as? UInt64) == 1)
            #expect((arr2?[1] as? UInt64) == 2)

            let arrN2 = r2["arrN"] as? [Any?]
            #expect((arrN2?.count) == 3)
            #expect((arrN2?[0] as? String) == "a")
            #expect(arrN2?[1] == nil)
            #expect((arrN2?[2] as? String) == "b")

            let map2 = r2["m"] as? [AnyHashable: Any?]
            #expect((map2?["a"] as? UInt64) == 1)
            #expect((map2?["b"] as? UInt64) == 2)

            let tuple2 = r2["t"] as? [Any?]
            #expect((tuple2?.first as? Int64) == 7)
            #expect((tuple2?.dropFirst().first as? String) == "x")
        } catch {
            try? await client.execute("DROP TABLE IF EXISTS \(table)")
            await client.close()
            throw error
        }
        try? await client.execute("DROP TABLE IF EXISTS \(table)")
        await client.close()
    }
}

@Test func integrationInsert_schemaMismatch_throws() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()

        let table = uniqueTableName(prefix: "swift_native_mismatch")
        let client = try await ClickHouseClient(config: config)
        do {
            try await client.execute("DROP TABLE IF EXISTS \(table)")
            try await client.execute("CREATE TABLE \(table) (id UInt64, name String) ENGINE=Memory")

            var builder = CHBlockBuilder()
            builder.addColumn(name: "id", type: CHUInt64Type(), values: [UInt64(1)])
            builder.addColumn(name: "wrong", type: CHStringType(), values: ["x"])
            let block = try builder.build()

            do {
                try await client.insert(into: table, block: block)
                #expect(Bool(false))
            } catch is CHBinaryError {
                #expect(Bool(true))
            }

            // Connection stays usable after a failed insert (client will reconnect if needed).
            let ok = try await client.queryOne("SELECT toInt64(1)", as: Int64.self)
            #expect(ok == 1)
        } catch {
            try? await client.execute("DROP TABLE IF EXISTS \(table)")
            await client.close()
            throw error
        }
        try? await client.execute("DROP TABLE IF EXISTS \(table)")
        await client.close()
    }
}

@Test func integrationInsert_sqlAPI() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()

        let table = uniqueTableName(prefix: "swift_native_insert_sql")
        let client = try await ClickHouseClient(config: config)
        do {
            try await client.execute("DROP TABLE IF EXISTS \(table)")
            try await client.execute("CREATE TABLE \(table) (id UInt64, name String) ENGINE=Memory")

            var builder = CHBlockBuilder()
            builder.addColumn(name: "id", type: CHUInt64Type(), values: [UInt64(7)])
            builder.addColumn(name: "name", type: CHStringType(), values: ["bob"])
            let block = try builder.build()

            try await client.insert(sql: "INSERT INTO \(table) VALUES", block: block)

            struct Row: Decodable, Sendable { let id: UInt64; let name: String }
            let row = try await client.queryOne("SELECT id, name FROM \(table)", as: Row.self)
            #expect(row?.id == 7)
            #expect(row?.name == "bob")
        } catch {
            try? await client.execute("DROP TABLE IF EXISTS \(table)")
            await client.close()
            throw error
        }
        try? await client.execute("DROP TABLE IF EXISTS \(table)")
        await client.close()
    }
}

@Test func integrationInsertRoundtrip_moreNumericAndDateTypes() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()

        let table = uniqueTableName(prefix: "swift_native_more_types")
        let client = try await ClickHouseClient(config: config)
        do {
            try await client.execute("DROP TABLE IF EXISTS \(table)")
            try await client.execute("""
            CREATE TABLE \(table) (
              i16 Int16,
              u8 UInt8,
              u16 UInt16,
              u32 UInt32,
              f32 Float32,
              d32 Date32,
              e16 Enum16('x' = 10, 'y' = 20),
              ns Nullable(String),
              fs3 FixedString(3),
              dec38 Decimal(38,6)
            ) ENGINE=Memory
            """)

            let d32 = Date(timeIntervalSince1970: 86_400 * 20_000)
            let dec = Decimal(string: "12345.678901")!

            var builder = CHBlockBuilder()
            builder.addColumn(name: "i16", type: CHInt16Type(), values: [Int64(-123), Int64(123)])
            builder.addColumn(name: "u8", type: CHUInt8Type(), values: [UInt64(1), UInt64(255)])
            builder.addColumn(name: "u16", type: CHUInt16Type(), values: [UInt64(1), UInt64(65_535)])
            builder.addColumn(name: "u32", type: CHUInt32Type(), values: [UInt64(1), UInt64(4_000_000_000)])
            builder.addColumn(name: "f32", type: CHFloat32Type(), values: [Float(1.5), Float(-2.25)])
            builder.addColumn(name: "d32", type: CHDate32Type(), values: [d32, d32])
            builder.addColumn(name: "e16", type: CHEnum16Type(names: ["x", "y"], values: [10, 20]), values: ["x", "y"])
            builder.addColumn(name: "ns", type: CHNullableType(nested: CHStringType()), values: [Optional("a"), Optional<String>.none])
            builder.addColumn(name: "fs3", type: CHFixedStringType(length: 3), values: [Data("abcdef".utf8), Data("xy".utf8)])
            builder.addColumn(name: "dec38", type: CHDecimalType(precision: 38, scale: 6), values: [dec, dec])
            let block = try builder.build()

            try await client.insert(into: table, block: block)

            let result = try await client.query("SELECT * FROM \(table) ORDER BY u8")
            var rows: [CHRow] = []
            for try await row in result.rows() { rows.append(row) }
            #expect(rows.count == 2)

            let r1 = rows[0]
            #expect((r1["i16"] as? Int64) == -123)
            #expect((r1["u8"] as? UInt64) == 1)
            #expect((r1["u16"] as? UInt64) == 1)
            #expect((r1["u32"] as? UInt64) == 1)
            #expect(abs(Double(r1["f32"] as? Float ?? 0) - 1.5) < 0.000_1)
            let dOut = r1["d32"] as? Date
            #expect(Int(dOut?.timeIntervalSince1970 ?? 0) == Int(d32.timeIntervalSince1970))
            #expect((r1["e16"] as? String) == "x")
            #expect((r1["ns"] as? String) == "a")
            #expect((r1["fs3"] as? Data) == Data("abc".utf8))
            let decOut = r1["dec38"] as? Decimal
            #expect(NSDecimalNumber(decimal: decOut ?? 0).stringValue == NSDecimalNumber(decimal: dec).stringValue)

            let r2 = rows[1]
            #expect((r2["u8"] as? UInt64) == 255)
            #expect(r2["ns"] == nil)
            #expect((r2["fs3"] as? Data) == Data([0x78, 0x79, 0x00]))
        } catch {
            try? await client.execute("DROP TABLE IF EXISTS \(table)")
            await client.close()
            throw error
        }
        try? await client.execute("DROP TABLE IF EXISTS \(table)")
        await client.close()
    }
}

@Test func integrationRowDecoding_fromQuery() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()

        struct Model: Decodable, Sendable {
            let userId: Int
            let userName: String
            let items: [Int]
            let meta: [String: String]
            let maybe: Int?
        }

        let client = try await ClickHouseClient(config: config)
        do {
            let result = try await client.query("""
            SELECT
              toInt32(42) AS user_id,
              'Bob' AS user_name,
              [1,2,3] AS items,
              map('a','1','b','2') AS meta,
              CAST(NULL, 'Nullable(Int32)') AS maybe
            """)

            var decoded: Model?
            for try await row in result.rows() {
                decoded = try row.decode(Model.self)
                break
            }
            #expect(decoded?.userId == 42)
            #expect(decoded?.userName == "Bob")
            #expect(decoded?.items == [1, 2, 3])
            #expect(decoded?.meta["a"] == "1")
            #expect(decoded?.maybe == nil)
        } catch {
            await client.close()
            throw error
        }
        await client.close()
    }
}

@Test func integrationProtocol_totalsExtremesProfileInfo() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let connection = try await CHNIOConnection.connect(config: config)
        do {
            // Totals packet.
            try await connection.sendQuery(
                "SELECT number % 3 AS k, sum(number) AS s FROM numbers(100) GROUP BY k WITH TOTALS ORDER BY k",
                settings: [:]
            )
            var sawTotals = false
            var sawProfile = false
            var totalsSum: UInt64?
            totalsLoop: while let response = try await connection.nextResponse() {
                switch response {
                case .totals(let totals):
                    sawTotals = true
                    if let col = totals.block.columns.first(where: { $0.name == "s" }) {
                        totalsSum = (col.values.first as? UInt64) ?? (col.values.first as? Int64).flatMap { UInt64($0) }
                    }
                case .profileInfo:
                    sawProfile = true
                case .exception(let ex):
                    throw ex
                case .eof:
                    break totalsLoop
                default:
                    continue
                }
            }
            #expect(sawTotals)
            #expect(totalsSum == 4_950)
            #expect(sawProfile)

            // Extremes packet.
            try await connection.sendQuery(
                "SELECT toInt64(number) AS x FROM numbers(100)",
                settings: ["extremes": .int32(1), "max_block_size": .int32(100)]
            )
            var sawExtremes = false
            var sawProfile2 = false
            var minX: Int64?
            var maxX: Int64?
            extremesLoop: while let response = try await connection.nextResponse() {
                switch response {
                case .extremes(let extremes):
                    sawExtremes = true
                    if let col = extremes.block.columns.first(where: { $0.name == "x" }) {
                        minX = col.values.first as? Int64
                        if col.values.count > 1 { maxX = col.values[1] as? Int64 }
                    }
                case .profileInfo:
                    sawProfile2 = true
                case .exception(let ex):
                    throw ex
                case .eof:
                    break extremesLoop
                default:
                    continue
                }
            }
            #expect(sawExtremes)
            #expect(minX == 0)
            #expect(maxX == 99)
            #expect(sawProfile2)
        } catch {
            await connection.close()
            throw error
        }
        await connection.close()
    }
}

@Test func integrationQueryEvents_streamsMetadata() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let client = try await ClickHouseClient(config: config)
        do {
            let events = try await client.queryEvents(
                "SELECT number % 3 AS k, sum(number) AS s FROM numbers(1000) GROUP BY k WITH TOTALS ORDER BY k",
                settings: ["max_block_size": .int32(50)]
            )
            var sawTotals = false
            var sawProfile = false
            var sawProgress = false
            var sawData = false
            var totalsSum: UInt64?

            for try await event in events {
                switch event {
                case .data(let block):
                    sawData = sawData || block.rowCount > 0
                case .progress:
                    sawProgress = true
                case .profileInfo:
                    sawProfile = true
                case .totals(let totals):
                    sawTotals = true
                    if let col = totals.block.columns.first(where: { $0.name == "s" }) {
                        totalsSum = (col.values.first as? UInt64) ?? (col.values.first as? Int64).flatMap { UInt64($0) }
                    }
                default:
                    continue
                }
            }
            #expect(sawData)
            #expect(sawTotals)
            #expect(sawProfile)
            #expect(sawProgress)
            #expect(totalsSum == 499_500)

            let events2 = try await client.queryEvents(
                "SELECT toInt64(number) AS x FROM numbers(100)",
                settings: ["extremes": .int32(1), "max_block_size": .int32(100)]
            )
            var sawExtremes = false
            var minX: Int64?
            var maxX: Int64?
            for try await event in events2 {
                switch event {
                case .extremes(let extremes):
                    sawExtremes = true
                    if let col = extremes.block.columns.first(where: { $0.name == "x" }) {
                        minX = col.values.first as? Int64
                        if col.values.count > 1 { maxX = col.values[1] as? Int64 }
                    }
                default:
                    continue
                }
            }
            #expect(sawExtremes)
            #expect(minX == 0)
            #expect(maxX == 99)
        } catch {
            await client.close()
            throw error
        }
        await client.close()
    }
}

@Test func integrationProtocol_cancelStopsLongRunningQuery() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        var config = try requireClickHouseConfig()
        // Cancelling is easiest to validate without compression affecting timing.
        config.compressionEnabled = false

        let connection = try await CHNIOConnection.connect(config: config)
        do {
            try await connection.sendQuery(
                "SELECT toUInt64(number) AS n FROM numbers(1000000000)",
                settings: ["max_block_size": .int32(1)]
            )

            var blocksSeen = 0
            var sawCancelException = false
            var sawEOF = false
            var didSendCancel = false
            var cancelDeadline: Date?
            cancelLoop: while let response = try await connection.nextResponse() {
                switch response {
                case .data:
                    blocksSeen += 1
                    if blocksSeen == 3 {
                        didSendCancel = true
                        try await connection.cancel()
                        cancelDeadline = Date().addingTimeInterval(2)
                    }
                case .exception:
                    // ClickHouse may return an exception for cancelled queries, but some versions return only EOF.
                    sawCancelException = true
                case .eof:
                    sawEOF = true
                    break cancelLoop
                default:
                    continue
                }

                if let deadline = cancelDeadline, Date() >= deadline {
                    throw CHBinaryError.malformed("Timed out waiting for server to terminate query after cancel")
                }
            }
            #expect(blocksSeen >= 1)
            #expect(didSendCancel)
            #expect(sawEOF || sawCancelException)

            // Connection should remain usable.
            try await connection.sendQuery("SELECT toInt64(1)", settings: [:])
            var one: Int64?
            oneLoop: while let response = try await connection.nextResponse() {
                switch response {
                case .data(let data):
                    if let col = data.block.columns.first {
                        one = col.values.first as? Int64
                    }
                case .exception(let ex):
                    throw ex
                case .eof:
                    break oneLoop
                default:
                    continue
                }
            }
            #expect(one == 1)
        } catch {
            await connection.close()
            throw error
        }
        await connection.close()
    }
}

@Test func integrationCompression_stressMultiFrameQueryAndInsert() async throws {
    let env = ProcessInfo.processInfo.environment
    guard env["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    // This test is only meaningful when protocol compression is enabled.
    guard (env["CLICKHOUSE_COMPRESSION"] ?? "1") != "0" else { return }

    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let client = try await ClickHouseClient(config: config)
        do {
            let bigLen = 8_192
            let rows = 2_000

            // Multi-frame compressed decoding (large result set, large strings).
            let result = try await client.query(
                "SELECT repeat('a', \(bigLen)) AS s FROM numbers(\(rows))",
                settings: ["max_block_size": .int32(200)]
            )
            var totalRows = 0
            for try await block in result.blocks {
                totalRows += block.rowCount
                if let col = block.columns.first(where: { $0.name == "s" }) {
                    if let first = col.values.first as? String {
                        #expect(first.count == bigLen)
                    }
                }
            }
            #expect(totalRows == rows)

            // Multi-frame compressed encoding for inserts (avoid round-tripping all data back).
            let table = uniqueTableName(prefix: "swift_native_big_insert")
            try await client.execute("DROP TABLE IF EXISTS \(table)")
            try await client.execute("CREATE TABLE \(table) (s String) ENGINE=Memory")

            var builder = CHBlockBuilder()
            builder.addColumn(name: "s", type: CHStringType(), values: (0..<rows).map { _ in String(repeating: "b", count: bigLen) })
            let block = try builder.build()
            try await client.insert(into: table, block: block)

            let sumLen = try await client.queryOne("SELECT toUInt64(sum(length(s))) FROM \(table)", as: UInt64.self)
            #expect(sumLen == UInt64(rows * bigLen))
            try? await client.execute("DROP TABLE IF EXISTS \(table)")
        } catch {
            await client.close()
            throw error
        }
        await client.close()
    }
}

@Test func integrationResilience_reconnectAfterDockerRestart() async throws {
    let env = ProcessInfo.processInfo.environment
    guard env["CLICKHOUSE_HOST"] != nil else { return }
    guard env["CLICKHOUSE_DOCKER_RESTART_TESTS"] == "1" else { return }
    // Run restart tests once per matrix run.
    guard (env["CLICKHOUSE_COMPRESSION"] ?? "1") == "0" else { return }
    guard restartOnlyMode() else { return }

    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let container = env["CLICKHOUSE_DOCKER_CONTAINER_NAME"] ?? "clickhouse-native-test"

        let client = try await ClickHouseClient(config: config)
        do {
            try await client.execute("SELECT 1")
            try await dockerRestart(container: container)
            try await dockerWaitReady(container: container, user: config.user, password: config.password)

            // First request may still fail (old TCP connection).
            _ = try? await client.execute("SELECT 1")
            // But the client should recover on the next call without re-creating it.
            try await client.execute("SELECT 1")
        } catch {
            await client.close()
            throw error
        }
        await client.close()
    }
}

@Test func integrationResilience_queryInterruptedByRestart_recovers() async throws {
    let env = ProcessInfo.processInfo.environment
    guard env["CLICKHOUSE_HOST"] != nil else { return }
    guard env["CLICKHOUSE_DOCKER_RESTART_TESTS"] == "1" else { return }
    guard (env["CLICKHOUSE_COMPRESSION"] ?? "1") == "0" else { return }
    guard restartOnlyMode() else { return }

    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let container = env["CLICKHOUSE_DOCKER_CONTAINER_NAME"] ?? "clickhouse-native-test"
        let client = try await ClickHouseClient(config: config)
        do {
            let result = try await client.query(
                "SELECT toUInt64(number) AS n FROM numbers(1000000000)",
                settings: ["max_block_size": .int32(1)]
            )

            let restartTask = Task {
                try await Task.sleep(nanoseconds: 250_000_000)
                try await dockerRestart(container: container)
                try await dockerWaitReady(container: container, user: config.user, password: config.password)
            }

            var sawAnyBlock = false
            var sawError = false
            do {
                for try await block in result.blocks {
                    sawAnyBlock = true
                    _ = block
                }
            } catch {
                sawError = true
            }
            _ = await restartTask.result
            #expect(sawAnyBlock)
            #expect(sawError)

            // After reconnect-on-error, the same client should work again.
            try await client.execute("SELECT 1")
        } catch {
            await client.close()
            throw error
        }
        await client.close()
    }
}

@Test func integrationInsert_columnList_withDefaultsMaterializedAlias_roundtrip() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let table = uniqueTableName(prefix: "swift_native_defaults")
        let client = try await ClickHouseClient(config: config)
        do {
            try await client.execute("SET session_timezone='UTC'")
            try await client.execute("DROP TABLE IF EXISTS \(table)")
            try await client.execute("""
            CREATE TABLE \(table) (
              id UInt64,
              ts DateTime DEFAULT toDateTime(id),
              day Date MATERIALIZED toDate(ts),
              message String,
              msg_len UInt32 ALIAS length(message)
            ) ENGINE=Memory
            """)

            let ids: [UInt64] = [1, 86_400 + 123]
            let messages = ["hello", "swift"]
            var builder = CHBlockBuilder()
            builder.addColumn(name: "id", type: CHUInt64Type(), values: ids)
            builder.addColumn(name: "message", type: CHStringType(), values: messages)
            let block = try builder.build()
            try await client.insert(sql: "INSERT INTO \(table) (id, message) VALUES", block: block)

            let result = try await client.query("SELECT id, ts, day, msg_len, message FROM \(table) ORDER BY id")
            var rows: [CHRow] = []
            for try await row in result.rows() { rows.append(row) }
            #expect(rows.count == ids.count)

            for idx in 0..<rows.count {
                let row = rows[idx]
                #expect((row["id"] as? UInt64) == ids[idx])
                #expect((row["message"] as? String) == messages[idx])
                let ts = row["ts"] as? Date
                #expect(Int(ts?.timeIntervalSince1970 ?? -1) == Int(ids[idx]))
                let day = row["day"] as? Date
                let expectedDaySeconds = Int((ids[idx] / 86_400) * 86_400)
                #expect(Int(day?.timeIntervalSince1970 ?? -1) == expectedDaySeconds)
                let msgLen = row["msg_len"] as? UInt64
                #expect(msgLen == UInt64(messages[idx].count))
            }
        } catch {
            try? await client.execute("DROP TABLE IF EXISTS \(table)")
            await client.close()
            throw error
        }
        try? await client.execute("DROP TABLE IF EXISTS \(table)")
        await client.close()
    }
}

@Test func integrationInsert_columnOrder_withColumnList_roundtrip() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let table = uniqueTableName(prefix: "swift_native_colorder")
        let client = try await ClickHouseClient(config: config)
        do {
            try await client.execute("DROP TABLE IF EXISTS \(table)")
            try await client.execute("CREATE TABLE \(table) (a UInt64, b String, c UInt8) ENGINE=Memory")

            var builder = CHBlockBuilder()
            builder.addColumn(name: "b", type: CHStringType(), values: ["left", "right"])
            builder.addColumn(name: "a", type: CHUInt64Type(), values: [UInt64(2), UInt64(1)])
            builder.addColumn(name: "c", type: CHUInt8Type(), values: [UInt64(7), UInt64(9)])
            let block = try builder.build()

            try await client.insert(sql: "INSERT INTO \(table) (b, a, c) VALUES", block: block)

            let result = try await client.query("SELECT a, b, c FROM \(table) ORDER BY a")
            var rows: [CHRow] = []
            for try await row in result.rows() { rows.append(row) }
            #expect(rows.count == 2)
            #expect((rows[0]["a"] as? UInt64) == 1)
            #expect((rows[0]["b"] as? String) == "right")
            #expect((rows[0]["c"] as? UInt64) == 9)
            #expect((rows[1]["a"] as? UInt64) == 2)
            #expect((rows[1]["b"] as? String) == "left")
            #expect((rows[1]["c"] as? UInt64) == 7)
        } catch {
            try? await client.execute("DROP TABLE IF EXISTS \(table)")
            await client.close()
            throw error
        }
        try? await client.execute("DROP TABLE IF EXISTS \(table)")
        await client.close()
    }
}

@Test func integrationInsert_emptyBlock_noopAndConnectionReusable() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let table = uniqueTableName(prefix: "swift_native_empty_insert")
        let client = try await ClickHouseClient(config: config)
        do {
            try await client.execute("DROP TABLE IF EXISTS \(table)")
            try await client.execute("CREATE TABLE \(table) (id UInt64, name String) ENGINE=Memory")

            var builder = CHBlockBuilder()
            builder.addColumn(name: "id", type: CHUInt64Type(), values: [UInt64]())
            builder.addColumn(name: "name", type: CHStringType(), values: [String]())
            let emptyBlock = try builder.build()
            try await client.insert(into: table, block: emptyBlock)

            let count0 = try await client.queryOne("SELECT count() FROM \(table)", as: UInt64.self)
            #expect(count0 == 0)

            var builder2 = CHBlockBuilder()
            builder2.addColumn(name: "id", type: CHUInt64Type(), values: [UInt64(1)])
            builder2.addColumn(name: "name", type: CHStringType(), values: ["ok"])
            let block = try builder2.build()
            try await client.insert(into: table, block: block)

            let count1 = try await client.queryOne("SELECT count() FROM \(table)", as: UInt64.self)
            #expect(count1 == 1)
        } catch {
            try? await client.execute("DROP TABLE IF EXISTS \(table)")
            await client.close()
            throw error
        }
        try? await client.execute("DROP TABLE IF EXISTS \(table)")
        await client.close()
    }
}

@Test func integrationIngestion_microBatches_1000xSmallInsert() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    guard stressTestsEnabled() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let table = uniqueTableName(prefix: "swift_native_microbatch")
        let client = try await ClickHouseClient(config: config)
        do {
            try await client.execute("DROP TABLE IF EXISTS \(table)")
            try await client.execute("CREATE TABLE \(table) (id UInt64, payload String) ENGINE=Memory")

            let totalBatches = 1000
            var expected: UInt64 = 0
            for i in 0..<totalBatches {
                var builder = CHBlockBuilder()
                builder.addColumn(name: "id", type: CHUInt64Type(), values: [UInt64(i)])
                builder.addColumn(name: "payload", type: CHStringType(), values: ["p\(i)"])
                let block = try builder.build()
                try await client.insert(into: table, block: block)
                expected += 1

                if i % 100 == 0 {
                    let count = try await client.queryOne("SELECT count() FROM \(table)", as: UInt64.self)
                    #expect(count == expected)
                }
            }

            let final = try await client.queryOne("SELECT count() FROM \(table)", as: UInt64.self)
            #expect(final == expected)
        } catch {
            try? await client.execute("DROP TABLE IF EXISTS \(table)")
            await client.close()
            throw error
        }
        try? await client.execute("DROP TABLE IF EXISTS \(table)")
        await client.close()
    }
}

@Test func integrationUseCase_logsTable_ingestAndAnalytics() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let table = uniqueTableName(prefix: "swift_native_logs")
        let client = try await ClickHouseClient(config: config)
        do {
            try await client.execute("SET session_timezone='UTC'")
            try await client.execute("DROP TABLE IF EXISTS \(table)")
            try await client.execute("""
            CREATE TABLE \(table) (
              ts DateTime64(3, 'UTC'),
              service LowCardinality(String),
              level Enum8('debug' = 1, 'info' = 2, 'warn' = 3, 'error' = 4),
              trace_id UUID,
              message String,
              duration_ms UInt32,
              success Bool,
              tags Array(String),
              attrs Map(String, String)
            ) ENGINE=MergeTree()
            ORDER BY (service, ts)
            """)

            let rows = 1000
            let base = Date(timeIntervalSince1970: 1_700_000_000)
            var tsValues: [Date] = []
            var serviceValues: [String] = []
            var levelValues: [String] = []
            var traceValues: [UUID] = []
            var messageValues: [String] = []
            var durationValues: [UInt64] = []
            var successValues: [Bool] = []
            var tagsValues: [[String]] = []
            var attrsValues: [[String: Any?]] = []

            var serviceCounts: [String: UInt64] = [:]
            var serviceDuration: [String: UInt64] = [:]
            var serviceSuccess: [String: UInt64] = [:]

            func serviceFor(_ i: Int) -> String {
                switch i % 10 {
                case 0..<6: return "auth"
                case 6..<8: return "payments"
                default: return "search"
                }
            }

            func levelFor(_ i: Int) -> String {
                switch i % 4 {
                case 0: return "debug"
                case 1: return "info"
                case 2: return "warn"
                default: return "error"
                }
            }

            for i in 0..<rows {
                let svc = serviceFor(i)
                let lvl = levelFor(i)
                let ts = base.addingTimeInterval(TimeInterval(i))
                let duration = UInt64(100 + (i % 50))
                let success = (i % 5) != 0
                let message = "msg-\(i)"
                let tags = ["t\(i % 3)", "t\(i % 5)"]
                let attrs: [String: Any?] = ["host": "h\(i % 4)", "env": (i % 2 == 0) ? "prod" : "dev"]

                tsValues.append(ts)
                serviceValues.append(svc)
                levelValues.append(lvl)
                traceValues.append(UUID())
                messageValues.append(message)
                durationValues.append(duration)
                successValues.append(success)
                tagsValues.append(tags)
                attrsValues.append(attrs)

                serviceCounts[svc, default: 0] += 1
                serviceDuration[svc, default: 0] += duration
                if success { serviceSuccess[svc, default: 0] += 1 }
            }

            let utc = TimeZone(secondsFromGMT: 0)!
            let dt64Type = CHDateTime64Type(scale: 3, timezone: utc)
            var builder = CHBlockBuilder()
            builder.addColumn(name: "ts", type: dt64Type, values: tsValues)
            builder.addColumn(name: "service", type: CHLowCardinalityType(nested: CHStringType()), values: serviceValues)
            builder.addColumn(name: "level", type: CHEnum8Type(names: ["debug", "info", "warn", "error"], values: [1, 2, 3, 4]), values: levelValues)
            builder.addColumn(name: "trace_id", type: CHUUIDType(), values: traceValues)
            builder.addColumn(name: "message", type: CHStringType(), values: messageValues)
            builder.addColumn(name: "duration_ms", type: CHUInt32Type(), values: durationValues)
            builder.addColumn(name: "success", type: CHBoolType(), values: successValues)
            builder.addColumn(name: "tags", type: CHArrayType(nested: CHStringType()), values: tagsValues)
            builder.addColumn(name: "attrs", type: CHMapType(key: CHStringType(), value: CHStringType()), values: attrsValues)
            let block = try builder.build()
            try await client.insert(into: table, block: block)

            let count = try await client.queryOne("SELECT count() FROM \(table)", as: UInt64.self)
            #expect(count == UInt64(rows))

            struct ServiceCount: Decodable, Sendable { let service: String; let count: UInt64 }
            let svcCounts = try await client.queryRows(
                "SELECT service, count() AS count FROM \(table) GROUP BY service ORDER BY service",
                as: ServiceCount.self
            )
            var countMap: [String: UInt64] = [:]
            for try await row in svcCounts { countMap[row.service] = row.count }
            for (svc, expected) in serviceCounts {
                #expect(countMap[svc] == expected)
            }

            struct ServiceSum: Decodable, Sendable { let service: String; let total: UInt64 }
            let sums = try await client.queryRows(
                "SELECT service, sum(duration_ms) AS total FROM \(table) GROUP BY service ORDER BY service",
                as: ServiceSum.self
            )
            var sumMap: [String: UInt64] = [:]
            for try await row in sums { sumMap[row.service] = row.total }
            for (svc, expected) in serviceDuration {
                #expect(sumMap[svc] == expected)
            }

            struct ServiceSuccess: Decodable, Sendable { let service: String; let ok: UInt64 }
            let oks = try await client.queryRows(
                "SELECT service, countIf(success) AS ok FROM \(table) GROUP BY service ORDER BY service",
                as: ServiceSuccess.self
            )
            var okMap: [String: UInt64] = [:]
            for try await row in oks { okMap[row.service] = row.ok }
            for (svc, expected) in serviceSuccess {
                #expect(okMap[svc] == expected)
            }

            let topService = try await client.queryOne(
                "SELECT service FROM \(table) GROUP BY service ORDER BY count() DESC, service LIMIT 1",
                as: String.self
            )
            #expect(topService == "auth")

            let baseMs = Int64(base.timeIntervalSince1970 * 1000)
            let startMs = baseMs + 100_000
            let endMs = baseMs + 200_000
            let rangeCount = try await client.queryOne(
                "SELECT count() FROM \(table) WHERE toUnixTimestamp64Milli(ts) >= \(startMs) AND toUnixTimestamp64Milli(ts) < \(endMs)",
                as: UInt64.self
            )
            #expect(rangeCount == 100)

            struct SampleRow: Decodable, Sendable { let tags: [String]; let attrs: [String: String] }
            let sample = try await client.queryOne(
                "SELECT tags, attrs FROM \(table) WHERE message = 'msg-42'",
                as: SampleRow.self
            )
            #expect(sample?.tags == ["t0", "t2"])
            #expect(sample?.attrs["host"] == "h2")
            #expect(sample?.attrs["env"] == "prod")
        } catch {
            try? await client.execute("DROP TABLE IF EXISTS \(table)")
            await client.close()
            throw error
        }
        try? await client.execute("DROP TABLE IF EXISTS \(table)")
        await client.close()
    }
}

@Test func integrationStreaming_queryEvents_earlyTerminate_sendsCancelAndReleasesConnection() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let exe = try probeExecutablePath()
        let result = try await runProcess(exe, ["query-events-cancel-suite"], timeoutSeconds: 30)
        #expect(result.exitCode == 0)
    }
}

@Test func integrationStreaming_queryRows_taskCancel_doesNotHangAndClientReusable() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let client = try await ClickHouseClient(config: config)
        do {
            struct Row: Decodable, Sendable { let n: UInt64 }
            let rows = try await client.queryRows(
                "SELECT number AS n FROM numbers(1000000000) SETTINGS max_block_size=1",
                as: Row.self
            )
            let task = Task {
                var seen = 0
                for try await _ in rows { seen += 1 }
                return seen
            }

            try await Task.sleep(nanoseconds: 200_000_000)
            task.cancel()
            _ = try? await withTimeout(seconds: 3) { try await task.value }

            try await withTimeout(seconds: 3) {
                try await client.execute("SELECT 1")
            }
        } catch {
            await client.close()
            throw error
        }
        await client.close()
    }
}

@Test func integrationErrors_maxExecutionTime_serverAborts_queryAndClientRecovers() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let client = try await ClickHouseClient(config: config)
        do {
            do {
                try await client.execute("SELECT sleep(3)", settings: ["max_execution_time": .int32(1)])
                #expect(Bool(false))
            } catch is CHServerException {
                #expect(Bool(true))
            }

            try await client.execute("SELECT 1")
        } catch {
            await client.close()
            throw error
        }
        await client.close()
    }
}

@Test func integrationErrors_readonlySetting_blocksDDL_butClientContinues() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let table = uniqueTableName(prefix: "swift_native_readonly")
        let client = try await ClickHouseClient(config: config)
        do {
            do {
                try await client.execute(
                    "CREATE TABLE \(table) (id UInt64) ENGINE=Memory",
                    settings: ["readonly": .int32(1)]
                )
                #expect(Bool(false))
            } catch is CHServerException {
                #expect(Bool(true))
            }
            try await client.execute("SELECT 1")
        } catch {
            await client.close()
            throw error
        }
        await client.close()
    }
}

@Test func integrationErrors_resultRowsLimit_throwsAndRecovers() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    guard stressTestsEnabled() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let client = try await ClickHouseClient(config: config)
        do {
            let result = try await client.query(
                "SELECT number FROM numbers(1000)",
                settings: [
                    "max_result_rows": .int32(10),
                    "result_overflow_mode": .string("throw"),
                ]
            )
            do {
                try await result.drain()
                #expect(Bool(false))
            } catch is CHServerException {
                #expect(Bool(true))
            }

            try await client.execute("SELECT 1")
        } catch {
            await client.close()
            throw error
        }
        await client.close()
    }
}

@Test func integrationProtocol_varIntBoundaries_string_roundtrip() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let table = uniqueTableName(prefix: "swift_native_varint")
        let client = try await ClickHouseClient(config: config)
        do {
            let sizes = [0, 127, 128, 16_383, 16_384]
            for size in sizes {
                let result = try await client.query("SELECT repeat('a', \(size)) AS s")
                var value: String?
                for try await row in result.rows() {
                    value = row["s"] as? String
                    break
                }
                #expect(value?.count == size)
            }

            try await client.execute("DROP TABLE IF EXISTS \(table)")
            try await client.execute("CREATE TABLE \(table) (s String) ENGINE=Memory")
            var builder = CHBlockBuilder()
            builder.addColumn(
                name: "s",
                type: CHStringType(),
                values: sizes.map { String(repeating: "b", count: $0) }
            )
            let block = try builder.build()
            try await client.insert(into: table, block: block)

            let result2 = try await client.query("SELECT length(s) AS len FROM \(table) ORDER BY len")
            var seen: [Int] = []
            for try await row in result2.rows() {
                if let len = row["len"] as? UInt64 { seen.append(Int(len)) }
            }
            #expect(seen == sizes.sorted())
        } catch {
            try? await client.execute("DROP TABLE IF EXISTS \(table)")
            await client.close()
            throw error
        }
        try? await client.execute("DROP TABLE IF EXISTS \(table)")
        await client.close()
    }
}

@Test func integrationTypes_floatSpecialValues_roundtrip() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let table = uniqueTableName(prefix: "swift_native_float_specials")
        let client = try await ClickHouseClient(config: config)
        do {
            try await client.execute("DROP TABLE IF EXISTS \(table)")
            try await client.execute("CREATE TABLE \(table) (seq UInt8, f64 Float64, f32 Float32) ENGINE=Memory")

            let values64: [Double] = [Double.nan, Double.infinity, -Double.infinity, -0.0]
            let values32: [Float] = [Float.nan, Float.infinity, -Float.infinity, -0.0]
            var builder = CHBlockBuilder()
            builder.addColumn(name: "seq", type: CHUInt8Type(), values: [UInt64(1), UInt64(2), UInt64(3), UInt64(4)])
            builder.addColumn(name: "f64", type: CHFloat64Type(), values: values64)
            builder.addColumn(name: "f32", type: CHFloat32Type(), values: values32)
            let block = try builder.build()
            try await client.insert(into: table, block: block)

            let result = try await client.query("SELECT seq, f64, f32 FROM \(table) ORDER BY seq")
            var rows: [CHRow] = []
            for try await row in result.rows() { rows.append(row) }
            #expect(rows.count == 4)

            if let f64_0 = rows[0]["f64"] as? Double { #expect(f64_0.isNaN) } else { #expect(Bool(false)) }
            if let f32_0 = rows[0]["f32"] as? Float { #expect(f32_0.isNaN) } else { #expect(Bool(false)) }

            if let f64_1 = rows[1]["f64"] as? Double { #expect(f64_1 == Double.infinity) } else { #expect(Bool(false)) }
            if let f32_1 = rows[1]["f32"] as? Float { #expect(f32_1 == Float.infinity) } else { #expect(Bool(false)) }

            if let f64_2 = rows[2]["f64"] as? Double { #expect(f64_2 == -Double.infinity) } else { #expect(Bool(false)) }
            if let f32_2 = rows[2]["f32"] as? Float { #expect(f32_2 == -Float.infinity) } else { #expect(Bool(false)) }

            if let f64_3 = rows[3]["f64"] as? Double {
                #expect(f64_3 == 0.0)
                #expect(f64_3.bitPattern == (-0.0).bitPattern)
            } else { #expect(Bool(false)) }
            if let f32_3 = rows[3]["f32"] as? Float {
                #expect(f32_3 == 0.0)
                #expect(f32_3.bitPattern == Float(-0.0).bitPattern)
            } else { #expect(Bool(false)) }
        } catch {
            try? await client.execute("DROP TABLE IF EXISTS \(table)")
            await client.close()
            throw error
        }
        try? await client.execute("DROP TABLE IF EXISTS \(table)")
        await client.close()
    }
}

@Test func integrationConcurrency_multiClient_readWriteMix() async throws {
    guard ProcessInfo.processInfo.environment["CLICKHOUSE_HOST"] != nil else { return }
    guard !restartOnlyMode() else { return }
    guard stressTestsEnabled() else { return }
    try await ClickHouseIntegrationLock.shared.withLock {
        let config = try requireClickHouseConfig()
        let table = uniqueTableName(prefix: "swift_native_concurrency")
        let writerCount = 3
        let readerCount = 2
        let rowsPerWriter = 200

        var clients: [ClickHouseClient] = []
        do {
            for _ in 0..<(writerCount + readerCount) {
                clients.append(try await ClickHouseClient(config: config))
            }
            let writerClients = Array(clients.prefix(writerCount))
            let readerClients = Array(clients.dropFirst(writerCount))

            let ddlClient = clients[0]
            try await ddlClient.execute("DROP TABLE IF EXISTS \(table)")
            try await ddlClient.execute("CREATE TABLE \(table) (id UInt64, payload String) ENGINE=Memory")

            try await withTimeout(seconds: 30) {
                try await withThrowingTaskGroup(of: Void.self) { group in
                    for w in 0..<writerCount {
                        let client = writerClients[w]
                        group.addTask {
                            for i in 0..<rowsPerWriter {
                                var builder = CHBlockBuilder()
                                let id = UInt64(w * rowsPerWriter + i)
                                builder.addColumn(name: "id", type: CHUInt64Type(), values: [id])
                                builder.addColumn(name: "payload", type: CHStringType(), values: ["w\(w)-\(i)"])
                                let block = try builder.build()
                                try await client.insert(into: table, block: block)
                            }
                        }
                    }

                    for r in 0..<readerCount {
                        let client = readerClients[r]
                        group.addTask {
                            for _ in 0..<20 {
                                _ = try await client.queryOne("SELECT count() FROM \(table)", as: UInt64.self)
                                try await Task.sleep(nanoseconds: 50_000_000)
                            }
                        }
                    }
                    try await group.waitForAll()
                }
            }

            let expected = UInt64(writerCount * rowsPerWriter)
            let finalCount = try await ddlClient.queryOne("SELECT count() FROM \(table)", as: UInt64.self)
            #expect(finalCount == expected)
        } catch {
            if let cleanup = clients.first {
                try? await cleanup.execute("DROP TABLE IF EXISTS \(table)")
            }
            for client in clients { await client.close() }
            throw error
        }
        if let cleanup = clients.first {
            try? await cleanup.execute("DROP TABLE IF EXISTS \(table)")
        }
        for client in clients { await client.close() }
    }
}

private func requireClickHouseConfig() throws -> CHConfig {
    let env = ProcessInfo.processInfo.environment
    guard let host = env["CLICKHOUSE_HOST"] else {
        throw CHBinaryError.malformed("CLICKHOUSE_HOST is not set")
    }
    let port = Int(env["CLICKHOUSE_PORT"] ?? "9000") ?? 9000
    let database = env["CLICKHOUSE_DB"] ?? "default"
    let user = env["CLICKHOUSE_USER"] ?? "default"
    let password = env["CLICKHOUSE_PASSWORD"] ?? ""
    let compression = (env["CLICKHOUSE_COMPRESSION"] ?? "1") != "0"
    return CHConfig(host: host, port: port, database: database, user: user, password: password, compressionEnabled: compression)
}

private func restartOnlyMode() -> Bool {
    (ProcessInfo.processInfo.environment["CLICKHOUSE_RESTART_TESTS_ONLY"] ?? "0") == "1"
}

private func stressTestsEnabled() -> Bool {
    (ProcessInfo.processInfo.environment["CLICKHOUSE_STRESS_TESTS"] ?? "0") == "1"
}

private actor ClickHouseIntegrationLock {
    static let shared = ClickHouseIntegrationLock()
    private var locked: Bool = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func lock() async {
        if !locked {
            locked = true
            return
        }
        await withCheckedContinuation { cont in
            waiters.append(cont)
        }
    }

    func unlock() {
        if waiters.isEmpty {
            locked = false
            return
        }
        let cont = waiters.removeFirst()
        cont.resume()
    }

    func withLock<T: Sendable>(_ body: @Sendable () async throws -> T) async rethrows -> T {
        await lock()
        defer { unlock() }
        return try await body()
    }
}

private enum CHTestProcessError: Error {
    case timeout(String)
    case nonZeroExit(String, Int32, String)
}

private struct CHTestProcessResult: Sendable {
    let exitCode: Int32
    let stdout: String
    let stderr: String
}

private struct CHQueryLogStats: Sendable {
    let readRows: UInt64
    let readBytes: UInt64
}

private func withTimeout<T: Sendable>(seconds: TimeInterval, operation: @escaping @Sendable () async throws -> T) async throws -> T {
    try await withThrowingTaskGroup(of: T.self) { group in
        group.addTask {
            try await operation()
        }
        group.addTask {
            let ns = UInt64(max(0, seconds) * 1_000_000_000)
            try await Task.sleep(nanoseconds: ns)
            throw CHTestProcessError.timeout("async timeout \(seconds)s")
        }
        let result = try await group.next()!
        group.cancelAll()
        return result
    }
}


private func runProcess(_ launchPath: String, _ args: [String], timeoutSeconds: TimeInterval) async throws -> CHTestProcessResult {
    try await withCheckedThrowingContinuation { continuation in
        DispatchQueue.global().async {
            do {
                let process = Process()
                process.executableURL = URL(fileURLWithPath: launchPath)
                process.arguments = args
                let out = Pipe()
                let err = Pipe()
                process.standardOutput = out
                process.standardError = err

                let group = DispatchGroup()
                group.enter()
                process.terminationHandler = { _ in group.leave() }

                try process.run()
                let waitRes = group.wait(timeout: .now() + timeoutSeconds)
                if waitRes == .timedOut {
                    process.terminate()
                    // Ensure the process does not linger (can wedge subsequent docker invocations).
                    Thread.sleep(forTimeInterval: 0.5)
                    if process.isRunning {
                        _ = kill(process.processIdentifier, SIGKILL)
                    }
                    continuation.resume(throwing: CHTestProcessError.timeout("\(launchPath) \(args.joined(separator: " "))"))
                    return
                }

                let stdout = String(data: out.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
                let stderr = String(data: err.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
                continuation.resume(returning: CHTestProcessResult(exitCode: process.terminationStatus, stdout: stdout, stderr: stderr))
            } catch {
                continuation.resume(throwing: error)
            }
        }
    }
}

private func docker(_ args: [String], timeoutSeconds: TimeInterval = 60) async throws -> CHTestProcessResult {
    let result = try await runProcess("/usr/bin/env", ["docker"] + args, timeoutSeconds: timeoutSeconds)
    if result.exitCode != 0 {
        throw CHTestProcessError.nonZeroExit("docker \(args.joined(separator: " "))", result.exitCode, result.stderr)
    }
    return result
}

private func dockerRestart(container: String) async throws {
    _ = try await docker(["restart", container], timeoutSeconds: 60)
}

private func waitForQueryLogRow(client: ClickHouseClient, queryId: String, timeoutSeconds: TimeInterval) async throws -> CHQueryLogStats? {
    let deadline = Date().addingTimeInterval(timeoutSeconds)
    struct LogRow: Decodable, Sendable {
        let readRows: UInt64
        let readBytes: UInt64
    }
    while Date() < deadline {
        if let row = try await client.queryOne(
            "SELECT read_rows, read_bytes FROM system.query_log WHERE query_id = '\(queryId)' AND type = 'QueryFinish' ORDER BY query_start_time DESC LIMIT 1",
            as: LogRow.self
        ) {
            return CHQueryLogStats(readRows: row.readRows, readBytes: row.readBytes)
        }
        try? await Task.sleep(nanoseconds: 1_000_000_000)
    }
    return nil
}

private func dockerPause(container: String) async throws {
    _ = try await docker(["pause", container], timeoutSeconds: 30)
}

private func dockerUnpause(container: String) async throws {
    _ = try await docker(["unpause", container], timeoutSeconds: 30)
}

private func dockerWaitReady(container: String, user: String, password: String) async throws {
    // Wait until clickhouse-client in container can execute a simple query.
    let deadline = Date().addingTimeInterval(90)
    while Date() < deadline {
        do {
            _ = try await docker(
                ["exec", container, "clickhouse-client", "--user", user, "--password", password, "--query", "SELECT 1"],
                timeoutSeconds: 5
            )
            return
        } catch {
            try await Task.sleep(nanoseconds: 500_000_000)
        }
    }
    throw CHTestProcessError.timeout("clickhouse ready in container \(container)")
}

private func probeExecutablePath() throws -> String {
    let fm = FileManager.default
    if let explicit = ProcessInfo.processInfo.environment["CLICKHOUSE_NATIVE_PROBE_PATH"],
       fm.isExecutableFile(atPath: explicit) {
        return explicit
    }

    if let toolsBuild = ProcessInfo.processInfo.environment["CLICKHOUSE_NATIVE_TOOLS_BUILD_DIR"] {
        let candidate = URL(fileURLWithPath: toolsBuild).appendingPathComponent("ClickHouseNativeProbe")
        if fm.isExecutableFile(atPath: candidate.path) {
            return candidate.path
        }
    }

    if let exec = CommandLine.arguments.first {
        var url = URL(fileURLWithPath: exec)
        for _ in 0..<6 {
            let candidate = url.deletingLastPathComponent().appendingPathComponent("ClickHouseNativeProbe")
            if fm.isExecutableFile(atPath: candidate.path) {
                return candidate.path
            }
            url.deleteLastPathComponent()
        }
    }

    var bundleURL = Bundle.main.bundleURL
    for _ in 0..<4 {
        let candidate = bundleURL.deletingLastPathComponent().appendingPathComponent("ClickHouseNativeProbe")
        if fm.isExecutableFile(atPath: candidate.path) {
            return candidate.path
        }
        bundleURL.deleteLastPathComponent()
    }

    if let buildDir = ProcessInfo.processInfo.environment["SWIFT_BUILD_DIR"] {
        let candidate = URL(fileURLWithPath: buildDir).appendingPathComponent("ClickHouseNativeProbe")
        if fm.isExecutableFile(atPath: candidate.path) {
            return candidate.path
        }
    }

    let repoRoot = URL(fileURLWithPath: #filePath)
        .deletingLastPathComponent()
        .deletingLastPathComponent()
        .deletingLastPathComponent()
    let toolsBuild = repoRoot.appendingPathComponent("Tools").appendingPathComponent(".build")
    if fm.fileExists(atPath: toolsBuild.path) {
        if let found = findExecutable(named: "ClickHouseNativeProbe", in: toolsBuild) {
            return found
        }
    }

    throw CHBinaryError.malformed(
        "ClickHouseNativeProbe executable not found. Build it with: swift build --package-path Tools --product ClickHouseNativeProbe"
    )
}

private func findExecutable(named name: String, in root: URL) -> String? {
    let fm = FileManager.default
    guard let enumerator = fm.enumerator(
        at: root,
        includingPropertiesForKeys: [.isExecutableKey],
        options: [.skipsHiddenFiles]
    ) else {
        return nil
    }
    for case let url as URL in enumerator {
        if url.lastPathComponent == name && fm.isExecutableFile(atPath: url.path) {
            return url.path
        }
    }
    return nil
}

private func waitForQueryPresent(client: ClickHouseClient, queryId: String, timeoutSeconds: TimeInterval) async throws -> Bool {
    let deadline = Date().addingTimeInterval(timeoutSeconds)
    while Date() < deadline {
        let count = try await client.queryOne(
            "SELECT count() FROM system.processes WHERE query_id = '\(queryId)'",
            as: UInt64.self
        ) ?? 0
        if count > 0 { return true }
        try await Task.sleep(nanoseconds: 200_000_000)
    }
    return false
}

private func waitForQueryGone(client: ClickHouseClient, queryId: String, timeoutSeconds: TimeInterval) async throws -> Bool {
    let deadline = Date().addingTimeInterval(timeoutSeconds)
    while Date() < deadline {
        let count = try await client.queryOne(
            "SELECT count() FROM system.processes WHERE query_id = '\(queryId)'",
            as: UInt64.self
        ) ?? 0
        if count == 0 { return true }
        try await Task.sleep(nanoseconds: 200_000_000)
    }
    return false
}

private func uniqueTableName(prefix: String) -> String {
    let id = UUID().uuidString.replacingOccurrences(of: "-", with: "")
    return "\(prefix)_\(id)"
}

private final class BlackholeChildChannels: @unchecked Sendable {
    private let lock = NSLock()
    private var channels: [Channel] = []

    func add(_ channel: Channel) {
        lock.lock()
        channels.append(channel)
        lock.unlock()

        channel.closeFuture.whenComplete { [weak self] _ in
            self?.remove(channel)
        }
    }

    private func remove(_ channel: Channel) {
        lock.lock()
        channels.removeAll { $0 === channel }
        lock.unlock()
    }

    func snapshot() -> [Channel] {
        lock.lock()
        let copy = channels
        lock.unlock()
        return copy
    }
}

private func startBlackholeServer() async throws -> (port: Int, close: @Sendable () -> Void) {
    let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    let children = BlackholeChildChannels()
    let bootstrap = ServerBootstrap(group: group)
        .childChannelInitializer { channel in
            // Accept the connection and do nothing (no ClickHouse hello response).
            children.add(channel)
            return channel.eventLoop.makeSucceededFuture(())
        }
        .serverChannelOption(ChannelOptions.backlog, value: 16)
        .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
        .childChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)

    let serverChannel = try await bootstrap.bind(host: "127.0.0.1", port: 0).get()
    let port = serverChannel.localAddress?.port ?? 0

    let close: @Sendable () -> Void = {
        // Stop accepting, then close any accepted child channels, then shutdown the ELG.
        _ = try? serverChannel.close().wait()
        for ch in children.snapshot() {
            _ = try? ch.close(mode: .all).wait()
        }
        let sem = DispatchSemaphore(value: 0)
        group.shutdownGracefully { _ in sem.signal() }
        _ = sem.wait(timeout: .now() + 5)
    }
    return (port: port, close: close)
}

#if canImport(Darwin)
import Darwin
#else
import Glibc
#endif

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

private func parseIPv6(_ string: String) -> Data {
    var addr = in6_addr()
    let res = string.withCString { cstr in
        inet_pton(AF_INET6, cstr, &addr)
    }
    if res == 1 {
        return withUnsafeBytes(of: &addr) { raw in
            Data(raw)
        }
    }
    return Data(repeating: 0, count: 16)
}
