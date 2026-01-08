import Foundation

public struct CHHelloRequest {
    public var clientName: String
    public var clientRevision: Int
    public var database: String
    public var user: String
    public var password: String

    public init(clientName: String, clientRevision: Int, database: String, user: String, password: String) {
        self.clientName = clientName
        self.clientRevision = clientRevision
        self.database = database
        self.user = user
        self.password = password
    }

    public func write(to writer: inout CHBinaryWriter) {
        writer.writeUTF8String("\(CHDefines.name) \(clientName)")
        writer.writeVarInt(UInt64(CHDefines.majorVersion))
        writer.writeVarInt(UInt64(CHDefines.minorVersion))
        writer.writeVarInt(UInt64(clientRevision))
        writer.writeUTF8String(database)
        writer.writeUTF8String(user)
        writer.writeUTF8String(password)
    }
}

public struct CHHelloResponse: Sendable {
    public var serverName: String
    public var majorVersion: Int
    public var minorVersion: Int
    public var revision: Int
    public var timezone: TimeZone
    public var displayName: String

    public static func read(from reader: inout CHBinaryReader) throws -> CHHelloResponse {
        let name = try reader.readUTF8String()
        let major = Int(try reader.readVarInt())
        let minor = Int(try reader.readVarInt())
        let revision = Int(try reader.readVarInt())
        let tzName: String
        if revision >= CHDefines.dbmsMinRevisionWithServerTimezone {
            tzName = try reader.readUTF8String()
        } else {
            tzName = TimeZone.current.identifier
        }
        let displayName: String
        if revision >= CHDefines.dbmsMinRevisionWithServerDisplayName {
            displayName = try reader.readUTF8String()
        } else {
            displayName = "localhost"
        }
        return CHHelloResponse(
            serverName: name,
            majorVersion: major,
            minorVersion: minor,
            revision: revision,
            timezone: TimeZone(identifier: tzName) ?? TimeZone(secondsFromGMT: 0)!,
            displayName: displayName
        )
    }
}
