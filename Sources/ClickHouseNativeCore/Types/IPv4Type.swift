#if canImport(Darwin)
import Darwin
#else
import Glibc
#endif

public struct CHIPv4Type: CHDataType {
    public let name = "IPv4"
    public init() {}

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var values: [Any?] = []
        values.reserveCapacity(rows)
        for _ in 0..<rows {
            let v = try reader.readUInt32()
            values.append(v)
        }
        return values
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        for value in values {
            if let v = value as? UInt32 {
                writer.writeUInt32(v)
            } else if let v = value as? UInt64 {
                writer.writeUInt32(UInt32(v))
            } else if let v = value as? Int64 {
                writer.writeUInt32(UInt32(truncatingIfNeeded: v))
            } else if let s = value as? String, let parsed = parseIPv4(s) {
                writer.writeUInt32(parsed)
            } else {
                writer.writeUInt32(0)
            }
        }
    }

    private func parseIPv4(_ string: String) -> UInt32? {
        var addr = in_addr()
        let res = string.withCString { cstr in
            inet_pton(AF_INET, cstr, &addr)
        }
        if res == 1 {
            return UInt32(bigEndian: addr.s_addr)
        }
        return nil
    }
}
