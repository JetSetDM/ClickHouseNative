import NIOCore
import Foundation

public struct CHBinaryWriter {
    public private(set) var buffer: ByteBuffer

    public init(buffer: ByteBuffer) {
        self.buffer = buffer
    }

    public mutating func writeVarInt(_ value: UInt64) {
        var x = value
        while true {
            var byte = UInt8(x & 0x7F)
            x >>= 7
            if x != 0 {
                byte |= 0x80
            }
            buffer.writeInteger(byte)
            if x == 0 { break }
        }
    }

    public mutating func writeByte(_ value: UInt8) {
        buffer.writeInteger(value)
    }

    public mutating func writeBool(_ value: Bool) {
        writeVarInt(value ? 1 : 0)
    }

    public mutating func writeInt16(_ value: Int16) {
        buffer.writeInteger(value, endianness: .little)
    }

    public mutating func writeInt32(_ value: Int32) {
        buffer.writeInteger(value, endianness: .little)
    }

    public mutating func writeInt64(_ value: Int64) {
        buffer.writeInteger(value, endianness: .little)
    }

    public mutating func writeUInt16(_ value: UInt16) {
        buffer.writeInteger(value, endianness: .little)
    }

    public mutating func writeUInt32(_ value: UInt32) {
        buffer.writeInteger(value, endianness: .little)
    }

    public mutating func writeUInt64(_ value: UInt64) {
        buffer.writeInteger(value, endianness: .little)
    }

    public mutating func writeFloat32(_ value: Float) {
        buffer.writeInteger(value.bitPattern, endianness: .little)
    }

    public mutating func writeFloat64(_ value: Double) {
        buffer.writeInteger(value.bitPattern, endianness: .little)
    }

    public mutating func writeBytes(_ bytes: [UInt8]) {
        buffer.writeBytes(bytes)
    }

    public mutating func writeBytesBinary(_ bytes: [UInt8]) {
        writeVarInt(UInt64(bytes.count))
        buffer.writeBytes(bytes)
    }

    public mutating func writeUTF8String(_ string: String) {
        let bytes = Array(string.utf8)
        writeBytesBinary(bytes)
    }
}
