import Foundation
#if canImport(Darwin)
import Darwin
#else
import Glibc
#endif

public struct CHIPv6Type: CHDataType {
    public let name = "IPv6"
    public init() {}

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var values: [Any?] = []
        values.reserveCapacity(rows)
        for _ in 0..<rows {
            let bytes = try reader.readBytes(count: 16)
            values.append(Data(bytes))
        }
        return values
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        for value in values {
            if let data = value as? Data, data.count == 16 {
                writer.writeBytes([UInt8](data))
            } else if let bytes = value as? [UInt8], bytes.count == 16 {
                writer.writeBytes(bytes)
            } else if let s = value as? String, let parsed = parseIPv6(s) {
                writer.writeBytes(parsed)
            } else {
                writer.writeBytes([UInt8](repeating: 0, count: 16))
            }
        }
    }

    private func parseIPv6(_ string: String) -> [UInt8]? {
        var addr = in6_addr()
        let res = string.withCString { cstr in
            inet_pton(AF_INET6, cstr, &addr)
        }
        if res == 1 {
            return withUnsafeBytes(of: &addr) { raw in
                Array(raw)
            }
        }
        return nil
    }
}
