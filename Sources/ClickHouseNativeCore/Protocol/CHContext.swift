import Foundation

public struct CHClientContext: Sendable {
    public static let tcpKind: UInt64 = 1
    public static let initialQuery: UInt64 = 1

    public var initialAddress: String
    public var clientHostname: String
    public var clientName: String

    public init(initialAddress: String, clientHostname: String, clientName: String) {
        self.initialAddress = initialAddress
        self.clientHostname = clientHostname
        self.clientName = clientName
    }

    public func write(to writer: inout CHBinaryWriter) {
        writer.writeVarInt(Self.initialQuery)
        writer.writeUTF8String("")
        writer.writeUTF8String("")
        writer.writeUTF8String(initialAddress)

        writer.writeVarInt(Self.tcpKind)
        writer.writeUTF8String("")
        writer.writeUTF8String(clientHostname)
        writer.writeUTF8String(clientName)
        writer.writeVarInt(UInt64(CHDefines.majorVersion))
        writer.writeVarInt(UInt64(CHDefines.minorVersion))
        writer.writeVarInt(UInt64(CHDefines.clientRevision))
        writer.writeUTF8String("")
    }
}

public struct CHServerContext: Sendable {
    public var majorVersion: Int
    public var minorVersion: Int
    public var revision: Int
    public var timezone: TimeZone
    public var displayName: String

    public init(majorVersion: Int, minorVersion: Int, revision: Int, timezone: TimeZone, displayName: String) {
        self.majorVersion = majorVersion
        self.minorVersion = minorVersion
        self.revision = revision
        self.timezone = timezone
        self.displayName = displayName
    }
}
