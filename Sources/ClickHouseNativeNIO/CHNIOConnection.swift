import Foundation
@preconcurrency import NIOCore
import NIOPosix
import NIOSSL
import ClickHouseNativeCore

#if canImport(Darwin)
import Darwin
#else
import Glibc
#endif

public final class CHNIOConnection: @unchecked Sendable {
    public let config: CHConfig
    public let clientContext: CHClientContext
    public private(set) var serverContext: CHServerContext?

    package let gate: CHConnectionGate

    private let group: EventLoopGroup
    private let channel: Channel
    private let responseQueue: CHResponseQueue
    private let serverContextBox: CHServerContextBox

    private init(
        config: CHConfig,
        clientContext: CHClientContext,
        group: EventLoopGroup,
        channel: Channel,
        responseQueue: CHResponseQueue,
        gate: CHConnectionGate,
        serverContextBox: CHServerContextBox
    ) {
        self.config = config
        self.clientContext = clientContext
        self.group = group
        self.channel = channel
        self.responseQueue = responseQueue
        self.gate = gate
        self.serverContextBox = serverContextBox
    }

    public static func connect(
        config: CHConfig,
        host: CHHost? = nil,
        connectTimeoutOverride: TimeInterval? = nil
    ) async throws -> CHNIOConnection {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let serverContextBox = CHServerContextBox()

        var continuation: AsyncThrowingStream<CHResponse, Error>.Continuation?
        let responseStream = AsyncThrowingStream<CHResponse, Error> { c in
            continuation = c
        }
        guard let streamContinuation = continuation else {
            await shutdown(group: group)
            throw CHBinaryError.malformed("Failed to create response stream")
        }

        let resolvedHost = host?.host ?? config.host
        let resolvedPort = host?.port ?? config.port
        let connectTimeout = connectTimeoutOverride ?? config.connectTimeout

        var bootstrap = ClientBootstrap(group: group)
        if connectTimeout > 0 {
            let nanos = Int64(max(0, connectTimeout) * 1_000_000_000)
            bootstrap = bootstrap.connectTimeout(.nanoseconds(nanos))
        }
        bootstrap = bootstrap.channelOption(
            ChannelOptions.socketOption(.so_keepalive),
            value: config.tcpKeepAlive ? 1 : 0
        )
        if let sendBuf = config.socketSendBufferBytes {
            let clamped = max(0, min(sendBuf, Int(Int32.max)))
            bootstrap = bootstrap.channelOption(
                ChannelOptions.socketOption(.so_sndbuf),
                value: ChannelOptions.Types.SocketOption.Value(clamped)
            )
        }
        if let recvBuf = config.socketRecvBufferBytes {
            let clamped = max(0, min(recvBuf, Int(Int32.max)))
            bootstrap = bootstrap.channelOption(
                ChannelOptions.socketOption(.so_rcvbuf),
                value: ChannelOptions.Types.SocketOption.Value(clamped)
            )
        }
        bootstrap = bootstrap.channelInitializer { channel in
            do {
                return try Self.configurePipeline(
                    channel: channel,
                    config: config,
                    resolvedHost: resolvedHost,
                    serverContextBox: serverContextBox,
                    streamContinuation: streamContinuation
                )
            } catch {
                return channel.eventLoop.makeFailedFuture(error)
            }
        }

        let channel: Channel
        do {
            channel = try await bootstrap.connect(host: resolvedHost, port: resolvedPort).get()
        } catch {
            await shutdown(group: group)
            throw error
        }

        let hostName = ProcessInfo.processInfo.hostName
        let clientContext = CHClientContext(
            initialAddress: "0.0.0.0:0",
            clientHostname: hostName,
            clientName: config.clientName
        )

        let queue = CHResponseQueue(stream: responseStream)
        let gate = CHConnectionGate()
        let connection = CHNIOConnection(
            config: config,
            clientContext: clientContext,
            group: group,
            channel: channel,
            responseQueue: queue,
            gate: gate,
            serverContextBox: serverContextBox
        )

        do {
            try await connection.sendHello()
            return connection
        } catch {
            await connection.close()
            throw error
        }
    }

    private func sendHello() async throws {
        let hello = CHHelloRequest(
            clientName: config.clientName,
            clientRevision: CHDefines.clientRevision,
            database: config.database,
            user: config.user,
            password: config.password
        )
        try await send(.hello(hello))
        let helloTimeout = max(0.1, config.connectTimeout)
        let response: CHResponse
        do {
            guard let r = try await withTimeout(seconds: helloTimeout, operation: { try await self.responseQueue.next() }) else {
                throw CHBinaryError.malformed("Connection closed while waiting for hello response")
            }
            response = r
        } catch is CHTimeoutError {
            _ = try? await channel.close(mode: .all).get()
            throw CHClientError.timeout("Timed out waiting for ClickHouse hello (connectTimeout=\(config.connectTimeout)s)")
        }
        if case .hello(let helloResp) = response {
            let context = CHServerContext(
                majorVersion: helloResp.majorVersion,
                minorVersion: helloResp.minorVersion,
                revision: helloResp.revision,
                timezone: helloResp.timezone,
                displayName: helloResp.displayName
            )
            serverContext = context
            serverContextBox.context = context
        } else if case .exception(let ex) = response {
            throw ex
        } else {
            throw CHBinaryError.malformed("Expected hello response, got \(response)")
        }
    }

    private static func configurePipeline(
        channel: Channel,
        config: CHConfig,
        resolvedHost: String,
        serverContextBox: CHServerContextBox,
        streamContinuation: AsyncThrowingStream<CHResponse, Error>.Continuation
    ) throws -> EventLoopFuture<Void> {
        do {
            if config.tlsEnabled {
                var tlsConfig = TLSConfiguration.makeClientConfiguration()
                if config.tlsVerifyMode == .none {
                    tlsConfig.certificateVerification = .none
                }
                if let caBytes = config.tlsCABytes {
                    let certs = try NIOSSLCertificate.fromPEMBytes([UInt8](caBytes))
                    tlsConfig.trustRoots = .certificates(certs)
                } else if let caPath = config.tlsCAFilePath {
                    tlsConfig.trustRoots = .file(caPath)
                }
                if let certPath = config.tlsClientCertificatePath,
                   let keyPath = config.tlsClientKeyPath {
                    let certs = try NIOSSLCertificate.fromPEMFile(certPath)
                    tlsConfig.certificateChain = certs.map { .certificate($0) }
                    let key = try NIOSSLPrivateKey(file: keyPath, format: .pem)
                    tlsConfig.privateKey = .privateKey(key)
                }
                let sslContext = try NIOSSLContext(configuration: tlsConfig)
                let sniHost: String? = isIPAddress(resolvedHost) ? nil : resolvedHost
                try channel.pipeline.syncOperations.addHandler(
                    NIOSSLClientHandler(context: sslContext, serverHostname: sniHost)
                )
            }
            try channel.pipeline.syncOperations.addHandler(ByteToMessageHandler(CHMessageDecoder(
                compressionEnabled: config.compressionEnabled,
                serverContextBox: serverContextBox
            )))
            try channel.pipeline.syncOperations.addHandler(MessageToByteHandler(CHMessageEncoder()))
            try channel.pipeline.syncOperations.addHandler(CHResponseHandler(continuation: streamContinuation))
            return channel.eventLoop.makeSucceededFuture(())
        } catch {
            return channel.eventLoop.makeFailedFuture(error)
        }
    }

    public func sendQuery(
        _ sql: String,
        settings: [String: CHSettingValue],
        queryId: String? = nil,
        stage: CHQueryStage? = nil
    ) async throws {
        let finalQueryId = queryId ?? UUID().uuidString
        let finalStage = stage?.rawValue ?? CHQueryRequest.stageComplete
        let mergedSettings = config.settings.merging(settings) { _, new in new }
        let query = CHQueryRequest(
            queryId: finalQueryId,
            clientContext: clientContext,
            stage: finalStage,
            compression: config.compressionEnabled,
            query: sql,
            settings: mergedSettings
        )
        try await send(.query(query))
    }

    public func insert(
        sql: String,
        block: CHBlock,
        queryId: String? = nil,
        stage: CHQueryStage? = nil
    ) async throws {
        try await sendQuery(sql, settings: [:], queryId: queryId, stage: stage)

        var sampleBlock: CHBlock?
        var pendingError: Error?
        while true {
            let response: CHResponse
            do {
                guard let r = try await withTimeout(
                    seconds: max(0.1, config.queryTimeout),
                    operation: { try await self.responseQueue.next() }
                ) else {
                    throw CHBinaryError.malformed("Connection closed while waiting for insert sample block")
                }
                response = r
            } catch is CHTimeoutError {
                _ = try? await channel.close(mode: .all).get()
                throw CHClientError.timeout("Timed out waiting for insert sample block (queryTimeout=\(config.queryTimeout)s)")
            }
            switch response {
            case .data(let data):
                if pendingError == nil {
                    sampleBlock = data.block
                    break
                }
            case .progress, .profileInfo, .totals, .extremes:
                continue
            case .pong, .hello:
                continue
            case .exception(let ex):
                pendingError = ex
                continue
            case .eof:
                if let error = pendingError { throw error }
                throw CHBinaryError.malformed("Unexpected EOF while waiting for sample block")
            }
            if sampleBlock != nil { break }
        }

        guard let sample = sampleBlock else {
            if let error = pendingError { throw error }
            throw CHBinaryError.malformed("No sample block returned for insert")
        }

        let codec: CHCompressionCodec? = config.compressionEnabled ? CHLZ4Codec() : nil
        let normalized: CHBlock
        do {
            normalized = try block.normalizedForInsert(sample: sample)
        } catch {
            // Abort insert cleanly so the connection stays usable: send terminating empty block and drain until EOF.
            try? await send(.data(CHDataRequest(name: "", block: CHBlock.empty(), compression: codec)))
            _ = try? await drainUntilEOF(timeoutSeconds: 10.0)
            throw error
        }

        try await send(.data(CHDataRequest(name: "", block: normalized, compression: codec)))
        try await send(.data(CHDataRequest(name: "", block: CHBlock.empty(), compression: codec)))

        var finalError: Error?
        while true {
            let response: CHResponse
            do {
                guard let r = try await withTimeout(
                    seconds: max(0.1, config.queryTimeout),
                    operation: { try await self.responseQueue.next() }
                ) else {
                    throw CHBinaryError.malformed("Connection closed while waiting for insert end-of-stream")
                }
                response = r
            } catch is CHTimeoutError {
                _ = try? await channel.close(mode: .all).get()
                throw CHClientError.timeout("Timed out waiting for insert end-of-stream (queryTimeout=\(config.queryTimeout)s)")
            }
            switch response {
            case .eof:
                if let error = finalError { throw error }
                return
            case .progress, .profileInfo, .totals, .extremes:
                continue
            case .data:
                continue
            case .pong, .hello:
                continue
            case .exception(let ex):
                finalError = ex
                continue
            }
        }
    }

    public func nextResponse() async throws -> CHResponse? {
        if config.queryTimeout <= 0 {
            return try await responseQueue.next()
        }
        do {
            return try await withTimeout(seconds: config.queryTimeout) { try await self.responseQueue.next() }
        } catch is CHTimeoutError {
            _ = try? await channel.close(mode: .all).get()
            throw CHClientError.timeout("Timed out waiting for ClickHouse response (queryTimeout=\(config.queryTimeout)s)")
        }
    }

    package func drainUntilEOF(timeoutSeconds: TimeInterval) async throws -> Bool {
        let deadline = Date().addingTimeInterval(timeoutSeconds)
        while true {
            let remaining = deadline.timeIntervalSinceNow
            if remaining <= 0 {
                return false
            }
            do {
                let response = try await withTimeout(seconds: remaining) { try await self.responseQueue.next() }
                guard let response else { return false }
                if case .eof = response { return true }
            } catch is CHTimeoutError {
                return false
            }
        }
    }

    public func ping() async throws -> Bool {
        try await send(.ping)
        while let response = try await nextResponse() {
            if case .pong = response { return true }
            if case .exception(let ex) = response { throw ex }
        }
        return false
    }

    package func cancel() async throws {
        try await send(.cancel)
    }

    public func close() async {
        _ = try? await channel.close(mode: .all).get()
        await Self.shutdown(group: group)
    }

    package func readSocketOptions() async throws -> (keepAlive: Bool, sendBuffer: Int, recvBuffer: Int) {
        let keepAliveRaw = try await channel.getOption(ChannelOptions.socketOption(.so_keepalive)).get()
        let sendBufferRaw = try await channel.getOption(ChannelOptions.socketOption(.so_sndbuf)).get()
        let recvBufferRaw = try await channel.getOption(ChannelOptions.socketOption(.so_rcvbuf)).get()
        return (keepAliveRaw != 0, Int(sendBufferRaw), Int(recvBufferRaw))
    }

    private func send(_ request: CHRequest) async throws {
        try await channel.writeAndFlush(request).get()
    }

    private enum CHTimeoutError: Error {
        case timeout
    }

    private func withTimeout<T: Sendable>(seconds: TimeInterval, operation: @escaping @Sendable () async throws -> T) async throws -> T {
        try await withThrowingTaskGroup(of: T.self) { group in
            group.addTask {
                try await operation()
            }
            group.addTask {
                let ns = UInt64(max(0, seconds) * 1_000_000_000)
                try await Task.sleep(nanoseconds: ns)
                throw CHTimeoutError.timeout
            }
            defer { group.cancelAll() }
            return try await group.next()!
        }
    }

    private static func shutdown(group: EventLoopGroup) async {
        await withCheckedContinuation { continuation in
            group.shutdownGracefully { _ in
                continuation.resume()
            }
        }
    }
}

private func isIPAddress(_ host: String) -> Bool {
    var v4 = in_addr()
    var v6 = in6_addr()
    return host.withCString { cstr in
        inet_pton(AF_INET, cstr, &v4) == 1 || inet_pton(AF_INET6, cstr, &v6) == 1
    }
}

package actor CHConnectionGate {
    private var locked: Bool = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    package init() {}

    package func lock() async {
        if !locked {
            locked = true
            return
        }
        await withCheckedContinuation { cont in
            waiters.append(cont)
        }
    }

    package func unlock() {
        if waiters.isEmpty {
            locked = false
            return
        }
        let cont = waiters.removeFirst()
        cont.resume()
    }
}

final class CHResponseQueue: @unchecked Sendable {
    private var iterator: AsyncThrowingStream<CHResponse, Error>.AsyncIterator

    init(stream: AsyncThrowingStream<CHResponse, Error>) {
        self.iterator = stream.makeAsyncIterator()
    }

    func next() async throws -> CHResponse? {
        return try await iterator.next()
    }
}
