import Foundation
#if canImport(Darwin)
import Darwin
#else
import Glibc
#endif

public struct CHUUIDType: CHDataType {
    public let name = "UUID"
    public init() {}

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var values: [Any?] = []
        values.reserveCapacity(rows)
        for _ in 0..<rows {
            let msb = try reader.readUInt64()
            let lsb = try reader.readUInt64()
            let uuid = CHUUIDType.uuidFrom(msb: msb, lsb: lsb)
            values.append(uuid)
        }
        return values
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        for value in values {
            if let uuid = value as? UUID {
                let (msb, lsb) = CHUUIDType.uuidTo(msbLsb: uuid)
                writer.writeUInt64(msb)
                writer.writeUInt64(lsb)
            } else if let string = value as? String, let uuid = UUID(uuidString: string) {
                let (msb, lsb) = CHUUIDType.uuidTo(msbLsb: uuid)
                writer.writeUInt64(msb)
                writer.writeUInt64(lsb)
            } else {
                writer.writeUInt64(0)
                writer.writeUInt64(0)
            }
        }
    }

    private static func uuidTo(msbLsb uuid: UUID) -> (UInt64, UInt64) {
        var bytes = [UInt8](repeating: 0, count: 16)
        var u = uuid.uuid
        withUnsafeBytes(of: &u) { raw in
            for i in 0..<16 {
                bytes[i] = raw[i]
            }
        }
        let msb = bytesToUInt64BE(bytes[0..<8])
        let lsb = bytesToUInt64BE(bytes[8..<16])
        return (msb, lsb)
    }

    private static func uuidFrom(msb: UInt64, lsb: UInt64) -> UUID {
        let msbBytes = uint64ToBytesBE(msb)
        let lsbBytes = uint64ToBytesBE(lsb)
        let all = msbBytes + lsbBytes
        let tuple: uuid_t = (
            all[0], all[1], all[2], all[3],
            all[4], all[5], all[6], all[7],
            all[8], all[9], all[10], all[11],
            all[12], all[13], all[14], all[15]
        )
        return UUID(uuid: tuple)
    }

    private static func bytesToUInt64BE(_ slice: ArraySlice<UInt8>) -> UInt64 {
        var result: UInt64 = 0
        for byte in slice {
            result = (result << 8) | UInt64(byte)
        }
        return result
    }

    private static func uint64ToBytesBE(_ value: UInt64) -> [UInt8] {
        return [
            UInt8(truncatingIfNeeded: value >> 56),
            UInt8(truncatingIfNeeded: value >> 48),
            UInt8(truncatingIfNeeded: value >> 40),
            UInt8(truncatingIfNeeded: value >> 32),
            UInt8(truncatingIfNeeded: value >> 24),
            UInt8(truncatingIfNeeded: value >> 16),
            UInt8(truncatingIfNeeded: value >> 8),
            UInt8(truncatingIfNeeded: value)
        ]
    }
}
