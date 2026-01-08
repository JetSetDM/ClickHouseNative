import NIOCore
import Foundation

public struct CHBinaryReader {
    private let buffer: ByteBuffer
    public private(set) var index: Int

    public init(buffer: ByteBuffer) {
        self.buffer = buffer
        self.index = buffer.readerIndex
    }

    public mutating func readVarInt() throws -> UInt64 {
        var result: UInt64 = 0
        for shift in stride(from: 0, through: 63, by: 7) {
            let byte: UInt8 = try readByte()
            result |= UInt64(byte & 0x7F) << UInt64(shift)
            if (byte & 0x80) == 0 {
                return result
            }
        }
        throw CHBinaryError.malformed("VarInt too long")
    }

    public mutating func readByte() throws -> UInt8 {
        guard let value: UInt8 = buffer.getInteger(at: index) else {
            throw CHBinaryError.needMoreData
        }
        index += 1
        return value
    }

    public mutating func readBool() throws -> Bool {
        return try readVarInt() != 0
    }

    public mutating func readInt16() throws -> Int16 {
        guard let value: Int16 = buffer.getInteger(at: index, endianness: .little) else {
            throw CHBinaryError.needMoreData
        }
        index += 2
        return value
    }

    public mutating func readInt32() throws -> Int32 {
        guard let value: Int32 = buffer.getInteger(at: index, endianness: .little) else {
            throw CHBinaryError.needMoreData
        }
        index += 4
        return value
    }

    public mutating func readInt64() throws -> Int64 {
        guard let value: Int64 = buffer.getInteger(at: index, endianness: .little) else {
            throw CHBinaryError.needMoreData
        }
        index += 8
        return value
    }

    public mutating func readUInt16() throws -> UInt16 {
        guard let value: UInt16 = buffer.getInteger(at: index, endianness: .little) else {
            throw CHBinaryError.needMoreData
        }
        index += 2
        return value
    }

    public mutating func readUInt32() throws -> UInt32 {
        guard let value: UInt32 = buffer.getInteger(at: index, endianness: .little) else {
            throw CHBinaryError.needMoreData
        }
        index += 4
        return value
    }

    public mutating func readUInt64() throws -> UInt64 {
        guard let value: UInt64 = buffer.getInteger(at: index, endianness: .little) else {
            throw CHBinaryError.needMoreData
        }
        index += 8
        return value
    }

    public mutating func readFloat32() throws -> Float {
        let bits = try readUInt32()
        return Float(bitPattern: bits)
    }

    public mutating func readFloat64() throws -> Double {
        let bits = try readUInt64()
        return Double(bitPattern: bits)
    }

    public mutating func readBytes(count: Int) throws -> [UInt8] {
        guard let bytes = buffer.getBytes(at: index, length: count) else {
            throw CHBinaryError.needMoreData
        }
        index += count
        return bytes
    }

    public mutating func readBytesBinary() throws -> [UInt8] {
        let length = Int(try readVarInt())
        return try readBytes(count: length)
    }

    public mutating func readUTF8String() throws -> String {
        let bytes = try readBytesBinary()
        return String(decoding: bytes, as: UTF8.self)
    }
}
