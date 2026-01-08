import Foundation

public struct CHConfig: Sendable {
    public var host: String
    public var port: Int
    public var hosts: [CHHost]
    public var hostSelectionPolicy: CHHostSelectionPolicy
    public var database: String
    public var user: String
    public var password: String
    public var connectTimeout: TimeInterval
    public var queryTimeout: TimeInterval
    public var compressionEnabled: Bool
    public var settings: [String: CHSettingValue]
    public var clientName: String
    public var tcpKeepAlive: Bool
    public var socketSendBufferBytes: Int?
    public var socketRecvBufferBytes: Int?
    public var tlsEnabled: Bool
    public var tlsVerifyMode: CHTlsVerifyMode
    public var tlsCAFilePath: String?
    public var tlsCABytes: Data?
    public var tlsClientCertificatePath: String?
    public var tlsClientKeyPath: String?

    public init(
        host: String,
        port: Int = 9000,
        hosts: [CHHost] = [],
        hostSelectionPolicy: CHHostSelectionPolicy = .roundRobin,
        database: String = "default",
        user: String = "default",
        password: String = "",
        connectTimeout: TimeInterval = 10,
        queryTimeout: TimeInterval = 60,
        compressionEnabled: Bool = false,
        settings: [String: CHSettingValue] = [:],
        clientName: String = "swift-native",
        tcpKeepAlive: Bool = false,
        socketSendBufferBytes: Int? = nil,
        socketRecvBufferBytes: Int? = nil,
        tlsEnabled: Bool = false,
        tlsVerify: Bool = true,
        tlsVerifyMode: CHTlsVerifyMode? = nil,
        tlsCAFilePath: String? = nil,
        tlsCABytes: Data? = nil,
        tlsClientCertificatePath: String? = nil,
        tlsClientKeyPath: String? = nil
    ) {
        self.host = host
        self.port = port
        self.hosts = hosts
        self.hostSelectionPolicy = hostSelectionPolicy
        self.database = database
        self.user = user
        self.password = password
        self.connectTimeout = connectTimeout
        self.queryTimeout = queryTimeout
        self.compressionEnabled = compressionEnabled
        self.settings = settings
        self.clientName = clientName
        self.tcpKeepAlive = tcpKeepAlive
        self.socketSendBufferBytes = socketSendBufferBytes
        self.socketRecvBufferBytes = socketRecvBufferBytes
        self.tlsEnabled = tlsEnabled
        self.tlsVerifyMode = tlsVerifyMode ?? (tlsVerify ? .verifyCA : .none)
        self.tlsCAFilePath = tlsCAFilePath
        self.tlsCABytes = tlsCABytes
        self.tlsClientCertificatePath = tlsClientCertificatePath
        self.tlsClientKeyPath = tlsClientKeyPath
    }

    public var tlsVerify: Bool {
        get { tlsVerifyMode == .verifyCA }
        set { tlsVerifyMode = newValue ? .verifyCA : .none }
    }

    public func resolvedHosts() -> [CHHost] {
        if hosts.isEmpty {
            return [CHHost(host: host, port: port)]
        }
        return hosts
    }
}

public enum CHTlsVerifyMode: Sendable {
    case none
    case verifyCA
}

public enum CHHostSelectionPolicy: Sendable {
    case roundRobin
    case random
}

public struct CHHost: Sendable, Hashable {
    public var host: String
    public var port: Int

    public init(host: String, port: Int) {
        self.host = host
        self.port = port
    }
}
