import Foundation
import ClickHouseNativeCore
import ClickHouseNativeNIO

public final class ClickHouseClient: @unchecked Sendable {
    private let config: CHConfig
    private let gate: CHConnectionGate
    private var connection: CHNIOConnection
    private let hostSelector: CHHostSelector

    public init(config: CHConfig) async throws {
        self.config = config
        self.gate = CHConnectionGate()
        self.hostSelector = CHHostSelector(hosts: config.resolvedHosts(), policy: config.hostSelectionPolicy)
        self.connection = try await Self.connectWithFailover(config: config, selector: hostSelector)
    }

    private func resetConnection() async {
        let old = connection

        // Important: never shut down the old connection's EventLoopGroup unless we have a new connection
        // ready, otherwise callers may keep using a connection whose EventLoop is already shut down and
        // hang forever waiting on futures to complete.
        let deadline = Date().addingTimeInterval(max(1, config.connectTimeout))
        var fresh: CHNIOConnection?
        while Date() < deadline {
            if let c = try? await Self.connectWithFailover(config: config, selector: hostSelector) {
                fresh = c
                break
            }
            try? await Task.sleep(nanoseconds: 200_000_000)
        }

        if let fresh {
            connection = fresh
            await old.close()
        }
    }

    public func query(_ sql: String, settings: [String: CHSettingValue] = [:]) async throws -> CHQueryResult {
        try await query(sql, settings: settings, options: CHQueryOptions())
    }

    public func query(_ sql: String, settings: [String: CHSettingValue] = [:], options: CHQueryOptions) async throws -> CHQueryResult {
        await gate.lock()
        let conn = connection
        do {
            try await conn.sendQuery(sql, settings: settings, queryId: options.queryId, stage: options.stage)
            return CHQueryResult(
                connection: conn,
                onError: { [weak self] in await self?.resetConnection() },
                onFinish: { [gate] in await gate.unlock() }
            )
        } catch {
            await resetConnection()
            await gate.unlock()
            throw error
        }
    }

    public func queryEvents(_ sql: String, settings: [String: CHSettingValue] = [:]) async throws -> AsyncThrowingStream<CHQueryEvent, Error> {
        try await queryEvents(sql, settings: settings, options: CHQueryOptions())
    }

    public func queryEvents(
        _ sql: String,
        settings: [String: CHSettingValue] = [:],
        options: CHQueryOptions
    ) async throws -> AsyncThrowingStream<CHQueryEvent, Error> {
        await gate.lock()
        let conn = connection
        do {
            try await conn.sendQuery(sql, settings: settings, queryId: options.queryId, stage: options.stage)
            return AsyncThrowingStream { continuation in
                let flag = CHQueryResultTerminationFlag()
                continuation.onTermination = { _ in
                    if flag.isCompleted() { return }
                    flag.setTerminated()
                    Task {
                        try? await conn.cancel()
                    }
                }
                Task {
                    do {
                        var pendingError: Error?
                        var sawEOF = false
                        func yieldEvent(_ event: CHQueryEvent) async {
                            if flag.isTerminated() { return }
                            let result = continuation.yield(event)
                            if case .terminated = result {
                                flag.setTerminated()
                                try? await conn.cancel()
                            }
                        }
                        eventLoop: while true {
                            if flag.isTerminated() {
                                let drained = (try? await conn.drainUntilEOF(timeoutSeconds: 1)) ?? false
                                _ = drained
                                await self.resetConnection()
                                await self.gate.unlock()
                                return
                            }
                            guard let response = try await conn.nextResponse() else {
                                break eventLoop
                            }
                            switch response {
                            case .data(let data):
                                if pendingError == nil {
                                    await yieldEvent(.data(data.block))
                                }
                            case .progress(let progress):
                                if pendingError == nil {
                                    await yieldEvent(.progress(progress))
                                }
                            case .totals(let totals):
                                if pendingError == nil {
                                    await yieldEvent(.totals(totals))
                                }
                            case .extremes(let extremes):
                                if pendingError == nil {
                                    await yieldEvent(.extremes(extremes))
                                }
                            case .profileInfo(let info):
                                if pendingError == nil {
                                    await yieldEvent(.profileInfo(info))
                                }
                            case .pong, .hello:
                                continue
                            case .exception(let ex):
                                pendingError = ex
                                break eventLoop
                            case .eof:
                                sawEOF = true
                                break eventLoop
                            }
                        }
                        if pendingError == nil, !sawEOF {
                            pendingError = CHBinaryError.malformed("Connection closed while waiting for end-of-stream")
                        }
                        flag.setCompleted()
                        if let error = pendingError {
                            continuation.finish(throwing: error)
                        } else {
                            continuation.finish()
                        }
                        if pendingError != nil || flag.isTerminated() {
                            await self.resetConnection()
                        }
                        await self.gate.unlock()
                    } catch {
                        flag.setCompleted()
                        continuation.finish(throwing: error)
                        await self.resetConnection()
                        await self.gate.unlock()
                    }
                }
            }
        } catch {
            await resetConnection()
            await gate.unlock()
            throw error
        }
    }

    public func queryRows<T: Decodable & Sendable>(_ sql: String, as type: T.Type = T.self) async throws -> AsyncThrowingStream<T, Error> {
        try await queryRows(sql, as: type, options: CHQueryOptions())
    }

    public func queryRows<T: Decodable & Sendable>(_ sql: String, as type: T.Type = T.self, options: CHQueryOptions) async throws -> AsyncThrowingStream<T, Error> {
        let result = try await query(sql, settings: [:], options: options)
        return AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    for try await row in result.rows() {
                        if Task.isCancelled { break }
                        let decoded = try row.decode(T.self)
                        continuation.yield(decoded)
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in
                task.cancel()
            }
        }
    }

    public func queryOne<T: Decodable & Sendable>(_ sql: String, as type: T.Type = T.self) async throws -> T? {
        try await queryOne(sql, as: type, options: CHQueryOptions())
    }

    public func queryOne<T: Decodable & Sendable>(_ sql: String, as type: T.Type = T.self, options: CHQueryOptions) async throws -> T? {
        await gate.lock()
        let conn = connection
        do {
            try await conn.sendQuery(sql, settings: [:], queryId: options.queryId, stage: options.stage)

            var first: T?
            var pendingError: Error?
            var sawEOF = false
            queryLoop: while let response = try await conn.nextResponse() {
                switch response {
                case .data(let data):
                    if pendingError == nil, first == nil, data.block.rowCount > 0 {
                        let columnNames = data.block.columns.map { $0.name }
                        var rowValues: [Any?] = []
                        rowValues.reserveCapacity(data.block.columns.count)
                        for column in data.block.columns {
                            rowValues.append(column.values.first ?? nil)
                        }
                        first = try CHRow(columns: columnNames, values: rowValues).decode(T.self)
                    }
                case .progress, .profileInfo, .totals, .extremes:
                    continue
                case .pong, .hello:
                    continue
                case .exception(let ex):
                    pendingError = ex
                    break queryLoop
                case .eof:
                    sawEOF = true
                    break queryLoop
                }
            }

            if pendingError == nil, !sawEOF {
                await resetConnection()
                await gate.unlock()
                throw CHBinaryError.malformed("Connection closed while waiting for end-of-stream")
            }
            if pendingError != nil {
                await resetConnection()
            }
            await gate.unlock()
            if let error = pendingError { throw error }
            return first
        } catch {
            await resetConnection()
            await gate.unlock()
            throw error
        }
    }

    public func execute(_ sql: String, settings: [String: CHSettingValue] = [:]) async throws {
        try await execute(sql, settings: settings, options: CHQueryOptions())
    }

    public func execute(_ sql: String, settings: [String: CHSettingValue] = [:], options: CHQueryOptions) async throws {
        await gate.lock()
        let conn = connection
        do {
            try await conn.sendQuery(sql, settings: settings, queryId: options.queryId, stage: options.stage)
            var pendingError: Error?
            var sawEOF = false
            execLoop: while let response = try await conn.nextResponse() {
                switch response {
                case .eof:
                    sawEOF = true
                    break execLoop
                case .data, .progress, .profileInfo, .totals, .extremes:
                    continue
                case .pong, .hello:
                    continue
                case .exception(let ex):
                    pendingError = ex
                    break execLoop
                }
            }
            if pendingError == nil, !sawEOF {
                await resetConnection()
                await gate.unlock()
                throw CHBinaryError.malformed("Connection closed while waiting for end-of-stream")
            }
            if pendingError != nil {
                await resetConnection()
            }
            await gate.unlock()
            if let error = pendingError { throw error }
        } catch {
            await resetConnection()
            await gate.unlock()
            throw error
        }
    }

    public func insert(into table: String, block: CHBlock) async throws {
        try await insert(into: table, block: block, options: CHQueryOptions())
    }

    public func insert(into table: String, block: CHBlock, options: CHQueryOptions) async throws {
        let sql = "INSERT INTO \(table) VALUES"
        await gate.lock()
        let conn = connection
        do {
            try await conn.insert(sql: sql, block: block, queryId: options.queryId, stage: options.stage)
            await gate.unlock()
        } catch {
            await resetConnection()
            await gate.unlock()
            throw error
        }
    }

    public func insert(sql: String, block: CHBlock) async throws {
        try await insert(sql: sql, block: block, options: CHQueryOptions())
    }

    public func insert(sql: String, block: CHBlock, options: CHQueryOptions) async throws {
        await gate.lock()
        let conn = connection
        do {
            try await conn.insert(sql: sql, block: block, queryId: options.queryId, stage: options.stage)
            await gate.unlock()
        } catch {
            await resetConnection()
            await gate.unlock()
            throw error
        }
    }

    public func ping() async throws -> Bool {
        await gate.lock()
        let conn = connection
        do {
            let ok = try await conn.ping()
            await gate.unlock()
            return ok
        } catch {
            await resetConnection()
            await gate.unlock()
            throw error
        }
    }

    public func close() async {
        await gate.lock()
        let conn = connection
        await conn.close()
        await gate.unlock()
    }

    private static func connectWithFailover(config: CHConfig, selector: CHHostSelector) async throws -> CHNIOConnection {
        let deadline = Date().addingTimeInterval(max(1, config.connectTimeout))
        var lastError: Error?

        while Date() < deadline {
            let candidates = await selector.nextAttemptOrder()
            if candidates.isEmpty {
                break
            }
            let remaining = deadline.timeIntervalSinceNow
            let perAttempt = max(0.1, remaining / Double(candidates.count))

            for host in candidates {
                do {
                    return try await CHNIOConnection.connect(
                        config: config,
                        host: host,
                        connectTimeoutOverride: perAttempt
                    )
                } catch {
                    lastError = error
                }
            }
            try? await Task.sleep(nanoseconds: 200_000_000)
        }

        if let lastError {
            throw lastError
        }
        throw CHClientError.timeout("Timed out connecting to ClickHouse hosts (connectTimeout=\(config.connectTimeout)s)")
    }
}

public struct CHQueryResult: @unchecked Sendable {
    public let blocks: AsyncThrowingStream<CHBlock, Error>

    init(connection: CHNIOConnection, onError: @Sendable @escaping () async -> Void, onFinish: @Sendable @escaping () async -> Void) {
        self.blocks = AsyncThrowingStream { continuation in
            let flag = CHQueryResultTerminationFlag()
            continuation.onTermination = { _ in
                if flag.isCompleted() { return }
                flag.setTerminated()
                Task {
                    try? await connection.cancel()
                }
            }
            Task {
                do {
                    var pendingError: Error?
                    var sawEOF = false
                    pumpLoop: while true {
                        if flag.isTerminated() {
                            let drained = (try? await connection.drainUntilEOF(timeoutSeconds: 1)) ?? false
                            _ = drained
                            await onError()
                            await onFinish()
                            return
                        }
                        guard let response = try await connection.nextResponse() else {
                            break pumpLoop
                        }
                        switch response {
                        case .data(let data):
                            if pendingError == nil, !flag.isTerminated() {
                                continuation.yield(data.block)
                            }
                        case .totals, .extremes, .progress, .profileInfo:
                            continue
                        case .pong, .hello:
                            continue
                        case .exception(let ex):
                            pendingError = ex
                            break pumpLoop
                        case .eof:
                            sawEOF = true
                            flag.setCompleted()
                            if let error = pendingError {
                                continuation.finish(throwing: error)
                            } else {
                                continuation.finish()
                            }
                            if pendingError != nil || flag.isTerminated() {
                                await onError()
                            }
                            await onFinish()
                            return
                        }
                    }
                    if pendingError == nil, !sawEOF {
                        pendingError = CHBinaryError.malformed("Connection closed while waiting for end-of-stream")
                    }
                    flag.setCompleted()
                    if let error = pendingError {
                        continuation.finish(throwing: error)
                    } else {
                        continuation.finish()
                    }
                    if pendingError != nil || flag.isTerminated() {
                        await onError()
                    }
                    await onFinish()
                } catch {
                    flag.setCompleted()
                    continuation.finish(throwing: error)
                    await onError()
                    await onFinish()
                }
            }
        }
    }

    public func rows() -> AsyncThrowingStream<CHRow, Error> {
        AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    for try await block in blocks {
                        if Task.isCancelled { break }
                        let columnNames = block.columns.map { $0.name }
                        for rowIndex in 0..<block.rowCount {
                            if Task.isCancelled { break }
                            var rowValues: [Any?] = []
                            rowValues.reserveCapacity(block.columns.count)
                            for column in block.columns {
                                if rowIndex < column.values.count {
                                    rowValues.append(column.values[rowIndex])
                                } else {
                                    rowValues.append(nil)
                                }
                            }
                            continuation.yield(CHRow(columns: columnNames, values: rowValues))
                        }
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in
                task.cancel()
            }
        }
    }

    public func drain() async throws {
        for try await _ in blocks {}
    }
}

private final class CHQueryResultTerminationFlag: @unchecked Sendable {
    private let lock = NSLock()
    private var terminated: Bool = false
    private var completed: Bool = false

    func setTerminated() {
        lock.lock()
        terminated = true
        lock.unlock()
    }

    func isTerminated() -> Bool {
        lock.lock()
        let v = terminated
        lock.unlock()
        return v
    }

    func setCompleted() {
        lock.lock()
        completed = true
        lock.unlock()
    }

    func isCompleted() -> Bool {
        lock.lock()
        let v = completed
        lock.unlock()
        return v
    }
}

package actor CHHostSelector {
    private let hosts: [CHHost]
    private let policy: CHHostSelectionPolicy
    private var index: Int = 0

    package init(hosts: [CHHost], policy: CHHostSelectionPolicy) {
        self.hosts = hosts
        self.policy = policy
    }

    package func nextAttemptOrder() -> [CHHost] {
        guard !hosts.isEmpty else { return [] }
        switch policy {
        case .random:
            return hosts.shuffled()
        case .roundRobin:
            let start = index % hosts.count
            index = (index + 1) % hosts.count
            if start == 0 { return hosts }
            return Array(hosts[start...]) + Array(hosts[..<start])
        }
    }
}
