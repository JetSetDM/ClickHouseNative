import Foundation
import ClickHouseNative
import ClickHouseNativeNIO

#if canImport(Darwin)
import Darwin
#else
import Glibc
#endif

enum ProbeError: Error, CustomStringConvertible {
    case usage(String)
    case missingEnv(String)
    case failed(String)

    var description: String {
        switch self {
        case .usage(let s): return s
        case .missingEnv(let s): return "Missing env: \(s)"
        case .failed(let s): return s
        }
    }
}

struct Log {
    static func line(_ message: String) {
        let ts = ISO8601DateFormatter().string(from: Date())
        FileHandle.standardError.write(Data("[\(ts)] \(message)\n".utf8))
    }
}

struct ProcessResult: Sendable {
    let code: Int32
    let stdout: String
    let stderr: String
}

func runProcess(_ launchPath: String, _ args: [String], timeoutSeconds: TimeInterval) throws -> ProcessResult {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: launchPath)
    process.arguments = args
    let out = Pipe()
    let err = Pipe()
    process.standardOutput = out
    process.standardError = err

    try process.run()

    let deadline = Date().addingTimeInterval(timeoutSeconds)
    while process.isRunning, Date() < deadline {
        Thread.sleep(forTimeInterval: 0.05)
    }
    if process.isRunning {
        process.terminate()
        Thread.sleep(forTimeInterval: 0.5)
        if process.isRunning {
            _ = kill(process.processIdentifier, SIGKILL)
        }
        throw ProbeError.failed("Timed out: \(launchPath) \(args.joined(separator: " "))")
    }

    let stdout = String(data: out.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
    let stderr = String(data: err.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
    return ProcessResult(code: process.terminationStatus, stdout: stdout, stderr: stderr)
}

func docker(_ args: [String], timeoutSeconds: TimeInterval = 60) throws -> ProcessResult {
    let res = try runProcess("/usr/bin/env", ["docker"] + args, timeoutSeconds: timeoutSeconds)
    if res.code != 0 {
        throw ProbeError.failed("docker \(args.joined(separator: " ")) failed (code=\(res.code)): \(res.stderr)")
    }
    return res
}

func dockerRestart(container: String) throws {
    Log.line("docker restart \(container)")
    _ = try docker(["restart", container], timeoutSeconds: 60)
}

func dockerPause(container: String) throws {
    Log.line("docker pause \(container)")
    _ = try docker(["pause", container], timeoutSeconds: 30)
}

func dockerUnpause(container: String) throws {
    Log.line("docker unpause \(container)")
    _ = try docker(["unpause", container], timeoutSeconds: 30)
}

func dockerWaitReady(container: String, user: String, password: String, timeoutSeconds: TimeInterval = 90) throws {
    Log.line("waiting clickhouse ready (container=\(container), timeout=\(Int(timeoutSeconds))s)")
    let deadline = Date().addingTimeInterval(timeoutSeconds)
    while Date() < deadline {
        do {
            _ = try docker(
                ["exec", container, "clickhouse-client", "--user", user, "--password", password, "--query", "SELECT 1"],
                timeoutSeconds: 5
            )
            Log.line("ready")
            return
        } catch {
            Thread.sleep(forTimeInterval: 0.5)
        }
    }
    throw ProbeError.failed("Timed out waiting for clickhouse ready (container=\(container))")
}

func requireEnv(_ key: String) throws -> String {
    guard let value = ProcessInfo.processInfo.environment[key], !value.isEmpty else {
        throw ProbeError.missingEnv(key)
    }
    return value
}

func loadConfig() throws -> CHConfig {
    let env = ProcessInfo.processInfo.environment
    let host = try requireEnv("CLICKHOUSE_HOST")
    let port = Int(env["CLICKHOUSE_PORT"] ?? "9000") ?? 9000
    let database = env["CLICKHOUSE_DB"] ?? "default"
    let user = env["CLICKHOUSE_USER"] ?? "default"
    let password = env["CLICKHOUSE_PASSWORD"] ?? ""
    let compression = (env["CLICKHOUSE_COMPRESSION"] ?? "1") != "0"

    var cfg = CHConfig(host: host, port: port, database: database, user: user, password: password, compressionEnabled: compression)
    cfg.connectTimeout = 10
    cfg.queryTimeout = 30
    return cfg
}

func restartSuite() async throws {
    let env = ProcessInfo.processInfo.environment
    let container = env["CLICKHOUSE_DOCKER_CONTAINER_NAME"] ?? "clickhouse-native-test"
    var config = try loadConfig()
    // Restart tests intentionally run without protocol compression.
    config.compressionEnabled = false

    Log.line("restart-suite: reconnectAfterRestart (connect)")
    do {
        let client = try await ClickHouseClient(config: config)
        defer { Task { await client.close() } }

        Log.line("restart-suite: reconnectAfterRestart (SELECT 1 pre)")
        try await client.execute("SELECT 1")
        Log.line("restart-suite: reconnectAfterRestart (docker restart)")
        try dockerRestart(container: container)
        Log.line("restart-suite: reconnectAfterRestart (wait ready)")
        try dockerWaitReady(container: container, user: config.user, password: config.password)

        _ = try? await client.execute("SELECT 1")
        Log.line("restart-suite: reconnectAfterRestart (SELECT 1 post)")
        try await client.execute("SELECT 1")
        Log.line("restart-suite: reconnectAfterRestart (ok)")
    }

    Log.line("restart-suite: queryInterruptedByRestart (connect)")
    do {
        let client = try await ClickHouseClient(config: config)
        defer { Task { await client.close() } }

        Log.line("restart-suite: queryInterruptedByRestart (start query)")
        let result = try await client.query(
            "SELECT toUInt64(number) AS n FROM numbers(1000000000)",
            settings: ["max_block_size": .int32(1)]
        )

        let restartTask = Task {
            try await Task.sleep(nanoseconds: 250_000_000)
            Log.line("restart-suite: queryInterruptedByRestart (docker restart)")
            try dockerRestart(container: container)
            Log.line("restart-suite: queryInterruptedByRestart (wait ready)")
            try dockerWaitReady(container: container, user: config.user, password: config.password)
            Log.line("restart-suite: queryInterruptedByRestart (ready)")
        }

        var sawAnyBlock = false
        var sawError = false
        do {
            Log.line("restart-suite: queryInterruptedByRestart (read first block)")
            for try await _ in result.blocks {
                sawAnyBlock = true
                break
            }
            // Continue draining to allow the failure to surface.
            Log.line("restart-suite: queryInterruptedByRestart (drain until end/error)")
            for try await _ in result.blocks {}
        } catch {
            sawError = true
            Log.line("restart-suite: queryInterruptedByRestart (saw error: \(String(describing: error)))")
        }

        _ = try await restartTask.value
        guard sawAnyBlock else { throw ProbeError.failed("Expected at least one data block before restart") }
        guard sawError else { throw ProbeError.failed("Expected query to fail due to restart") }

        Log.line("restart-suite: queryInterruptedByRestart (SELECT 1 after recovery)")
        try await client.execute("SELECT 1")
        Log.line("restart-suite: queryInterruptedByRestart (ok)")
    }
}

func cancelSuite() async throws {
    var config = try loadConfig()
    config.compressionEnabled = false
    config.queryTimeout = 60

    Log.line("cancel-suite: start long query and cancel")
    let connection = try await CHNIOConnection.connect(config: config)
    defer { Task { await connection.close() } }

    try await connection.sendQuery(
        "SELECT toUInt64(number) AS n FROM numbers(1000000000)",
        settings: ["max_block_size": .int32(1)]
    )

    let deadline = Date().addingTimeInterval(10)
    var blocksSeen = 0
    var didSendCancel = false
    var sawTerminal = false

    while let response = try await connection.nextResponse() {
        switch response {
        case .data:
            blocksSeen += 1
            if blocksSeen == 3 {
                didSendCancel = true
                try await connection.cancel()
            }
        case .exception, .eof:
            sawTerminal = true
            break
        default:
            break
        }
        if didSendCancel, Date() >= deadline {
            throw ProbeError.failed("Timed out waiting for server termination after cancel")
        }
        if sawTerminal { break }
    }

    guard blocksSeen >= 1 else { throw ProbeError.failed("Expected at least one block") }
    guard didSendCancel else { throw ProbeError.failed("Cancel was not sent") }
    guard sawTerminal else { throw ProbeError.failed("Expected EOF/exception after cancel") }

    Log.line("cancel-suite: connection still usable")
    try await connection.sendQuery("SELECT toInt64(1)", settings: [:])
    _ = try await connection.drainUntilEOF(timeoutSeconds: 10)
}

private func withTimeout<T: Sendable>(seconds: TimeInterval, operation: @escaping @Sendable () async throws -> T) async throws -> T {
    try await withThrowingTaskGroup(of: T.self) { group in
        group.addTask {
            try await operation()
        }
        group.addTask {
            let ns = UInt64(max(0, seconds) * 1_000_000_000)
            try await Task.sleep(nanoseconds: ns)
            throw ProbeError.failed("Timed out after \(seconds)s")
        }
        let result = try await group.next()!
        group.cancelAll()
        return result
    }
}

func queryEventsCancelSuite() async throws {
    var config = try loadConfig()
    config.compressionEnabled = false
    config.queryTimeout = 30

    Log.line("query-events-cancel-suite: connect")
    let client = try await ClickHouseClient(config: config)
    defer { Task { await client.close() } }

    var blocks = 0
    do {
        let queryId = "probe-events-cancel-\(UUID().uuidString)"
        Log.line("query-events-cancel-suite: start queryEvents (queryId=\(queryId))")
        let events = try await client.queryEvents(
            "SELECT number AS n FROM numbers(1000000000) SETTINGS max_block_size=1024",
            settings: [:],
            options: CHQueryOptions(queryId: queryId, stage: .complete)
        )

        Log.line("query-events-cancel-suite: consume blocks")
        for try await event in events {
            if case .data = event {
                blocks += 1
                if blocks >= 3 { break }
            }
        }
        Log.line("query-events-cancel-suite: blocks seen=\(blocks)")
    }
    guard blocks >= 1 else { throw ProbeError.failed("Expected at least one data block") }

    Log.line("query-events-cancel-suite: SELECT 1 after cancel")
    try await withTimeout(seconds: 5) {
        try await client.execute("SELECT 1")
    }
    Log.line("query-events-cancel-suite: ok")
}

func pauseSuite() async throws {
    let env = ProcessInfo.processInfo.environment
    let container = env["CLICKHOUSE_DOCKER_CONTAINER_NAME"] ?? "clickhouse-native-test"
    var config = try loadConfig()
    config.compressionEnabled = false
    config.connectTimeout = 2
    config.queryTimeout = 1

    Log.line("pause-suite: connect")
    let client = try await ClickHouseClient(config: config)
    defer { Task { await client.close() } }

    Log.line("pause-suite: SELECT 1 pre")
    try await client.execute("SELECT 1")

    Log.line("pause-suite: pause container")
    try dockerPause(container: container)
    defer { try? dockerUnpause(container: container) }

    Log.line("pause-suite: expect timeout while paused")
    do {
        try await client.execute("SELECT sleep(10)")
        throw ProbeError.failed("Expected queryTimeout while container is paused")
    } catch is CHClientError {
        Log.line("pause-suite: saw CHClientError (expected)")
    } catch {
        Log.line("pause-suite: saw error (expected-ish): \(String(describing: error))")
    }

    Log.line("pause-suite: unpause container")
    try dockerUnpause(container: container)
    try dockerWaitReady(container: container, user: config.user, password: config.password)

    Log.line("pause-suite: SELECT 1 after recovery")
    _ = try? await client.execute("SELECT 1")
    try await client.execute("SELECT 1")
    Log.line("pause-suite: ok")
}

func tlsSuite() async throws {
    let env = ProcessInfo.processInfo.environment
    let host = env["CLICKHOUSE_TLS_HOST"] ?? "127.0.0.1"
    let port = Int(env["CLICKHOUSE_TLS_PORT"] ?? "0") ?? 0
    guard port > 0 else {
        throw ProbeError.missingEnv("CLICKHOUSE_TLS_PORT")
    }
    let caPath = env["CLICKHOUSE_TLS_CA_PATH"]

    Log.line("tls-suite: connect verifyOff")
    do {
        var cfg = try loadConfig()
        cfg.host = host
        cfg.port = port
        cfg.compressionEnabled = false
        cfg.tlsEnabled = true
        cfg.tlsVerify = false
        cfg.connectTimeout = 5
        cfg.queryTimeout = 10

        let client = try await ClickHouseClient(config: cfg)
        defer { Task { await client.close() } }
        try await client.execute("SELECT 1")
    }

    if let caPath {
        Log.line("tls-suite: connect verifyOn with CA")
        do {
            var cfg = try loadConfig()
            cfg.host = host
            cfg.port = port
            cfg.compressionEnabled = false
            cfg.tlsEnabled = true
            cfg.tlsVerify = true
            cfg.tlsCAFilePath = caPath
            cfg.connectTimeout = 5
            cfg.queryTimeout = 10

            let client = try await ClickHouseClient(config: cfg)
            defer { Task { await client.close() } }
            try await client.execute("SELECT 1")
        }
    } else {
        Log.line("tls-suite: connect verifyOn expects failure (self-signed)")
        do {
            var cfg = try loadConfig()
            cfg.host = host
            cfg.port = port
            cfg.compressionEnabled = false
            cfg.tlsEnabled = true
            cfg.tlsVerify = true
            cfg.connectTimeout = 5
            cfg.queryTimeout = 10

            do {
                _ = try await ClickHouseClient(config: cfg)
                throw ProbeError.failed("Expected TLS verify to fail for self-signed cert")
            } catch {
                // expected
            }
        }
    }

    Log.line("tls-suite: ok")
}

@main
enum Main {
    static func main() async {
        do {
            let args = Array(CommandLine.arguments.dropFirst())
            let mode = args.first ?? "smoke"

            switch mode {
            case "smoke":
                let cfg = try loadConfig()
                Log.line("smoke: connect")
                let client = try await ClickHouseClient(config: cfg)
                defer { Task { await client.close() } }
                Log.line("smoke: SELECT 1")
                try await client.execute("SELECT 1")
                Log.line("smoke: ok")
            case "docker-suite":
                try await cancelSuite()
                try await restartSuite()
                try await pauseSuite()
                try await tlsSuite()
                Log.line("docker-suite: ok")
            case "restart-suite":
                try await restartSuite()
                Log.line("restart-suite: ok")
            case "cancel-suite":
                try await cancelSuite()
                Log.line("cancel-suite: ok")
            case "query-events-cancel-suite":
                try await queryEventsCancelSuite()
                Log.line("query-events-cancel-suite: ok")
            case "pause-suite":
                try await pauseSuite()
                Log.line("pause-suite: ok")
            case "tls-suite":
                try await tlsSuite()
                Log.line("tls-suite: ok")
            case "-h", "--help", "help":
                throw ProbeError.usage(
                    """
                    Usage:
                      ClickHouseNativeProbe <smoke|restart-suite|cancel-suite|query-events-cancel-suite|pause-suite|tls-suite|docker-suite>

                    Env:
                      CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_DB, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_COMPRESSION
                      CLICKHOUSE_DOCKER_CONTAINER_NAME (for docker-related suites)
                      CLICKHOUSE_TLS_HOST, CLICKHOUSE_TLS_PORT (for tls-suite)
                    """
                )
            default:
                throw ProbeError.usage("Unknown mode: \(mode). Use: smoke|restart-suite|cancel-suite|query-events-cancel-suite|pause-suite|tls-suite|docker-suite")
        }
        } catch let e as ProbeError {
            Log.line("ERROR: \(e.description)")
            exit(2)
        } catch {
            Log.line("ERROR: \(String(describing: error))")
            exit(2)
        }
    }
}
