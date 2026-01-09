import Foundation

public struct CHBoolType: CHDataType {
    public let name = "Bool"
    public init() {}

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var values: [Any?] = []
        values.reserveCapacity(rows)
        for _ in 0..<rows {
            let byte = try reader.readByte()
            values.append(byte != 0)
        }
        return values
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        for value in values {
            let boolValue: Bool
            if let v = value as? Bool {
                boolValue = v
            } else if let v = value as? Int {
                boolValue = v != 0
            } else if let v = value as? Int64 {
                boolValue = v != 0
            } else if let v = value as? UInt64 {
                boolValue = v != 0
            } else {
                boolValue = false
            }
            writer.writeByte(boolValue ? 1 : 0)
        }
    }
}

public struct CHInt8Type: CHDataType {
    public let name = "Int8"
    public init() {}

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var values: [Any?] = []
        values.reserveCapacity(rows)
        for _ in 0..<rows {
            let byte = try reader.readByte()
            values.append(Int64(Int8(bitPattern: byte)))
        }
        return values
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        for value in values {
            let v = value as? Int64 ?? 0
            writer.writeByte(UInt8(bitPattern: Int8(v)))
        }
    }
}

public struct CHInt16Type: CHDataType {
    public let name = "Int16"
    public init() {}

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var values: [Any?] = []
        values.reserveCapacity(rows)
        for _ in 0..<rows {
            values.append(Int64(try reader.readInt16()))
        }
        return values
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        for value in values {
            let v = value as? Int64 ?? 0
            writer.writeInt16(Int16(v))
        }
    }
}

public struct CHInt32Type: CHDataType {
    public let name = "Int32"
    public init() {}

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var values: [Any?] = []
        values.reserveCapacity(rows)
        for _ in 0..<rows {
            values.append(Int64(try reader.readInt32()))
        }
        return values
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        for value in values {
            let v = value as? Int64 ?? 0
            writer.writeInt32(Int32(v))
        }
    }
}

public struct CHInt64Type: CHDataType {
    public let name = "Int64"
    public init() {}

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var values: [Any?] = []
        values.reserveCapacity(rows)
        for _ in 0..<rows {
            values.append(try reader.readInt64())
        }
        return values
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        for value in values {
            let v = value as? Int64 ?? 0
            writer.writeInt64(v)
        }
    }
}

public struct CHUInt8Type: CHDataType {
    public let name = "UInt8"
    public init() {}

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var values: [Any?] = []
        values.reserveCapacity(rows)
        for _ in 0..<rows {
            values.append(UInt64(try reader.readByte()))
        }
        return values
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        for value in values {
            let v = value as? UInt64 ?? 0
            writer.writeByte(UInt8(v))
        }
    }
}

public struct CHUInt16Type: CHDataType {
    public let name = "UInt16"
    public init() {}

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var values: [Any?] = []
        values.reserveCapacity(rows)
        for _ in 0..<rows {
            values.append(UInt64(try reader.readUInt16()))
        }
        return values
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        for value in values {
            let v = value as? UInt64 ?? 0
            writer.writeUInt16(UInt16(v))
        }
    }
}

public struct CHUInt32Type: CHDataType {
    public let name = "UInt32"
    public init() {}

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var values: [Any?] = []
        values.reserveCapacity(rows)
        for _ in 0..<rows {
            values.append(UInt64(try reader.readUInt32()))
        }
        return values
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        for value in values {
            let v = value as? UInt64 ?? 0
            writer.writeUInt32(UInt32(v))
        }
    }
}

public struct CHUInt64Type: CHDataType {
    public let name = "UInt64"
    public init() {}

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var values: [Any?] = []
        values.reserveCapacity(rows)
        for _ in 0..<rows {
            values.append(try reader.readUInt64())
        }
        return values
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        for value in values {
            let v = value as? UInt64 ?? 0
            writer.writeUInt64(v)
        }
    }
}

public struct CHFloat32Type: CHDataType {
    public let name = "Float32"
    public init() {}

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var values: [Any?] = []
        values.reserveCapacity(rows)
        for _ in 0..<rows {
            values.append(try reader.readFloat32())
        }
        return values
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        for value in values {
            let v = value as? Float ?? 0
            writer.writeFloat32(v)
        }
    }
}

public struct CHFloat64Type: CHDataType {
    public let name = "Float64"
    public init() {}

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var values: [Any?] = []
        values.reserveCapacity(rows)
        for _ in 0..<rows {
            values.append(try reader.readFloat64())
        }
        return values
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        for value in values {
            let v = value as? Double ?? 0
            writer.writeFloat64(v)
        }
    }
}

public struct CHStringType: CHDataType {
    public let name = "String"
    public init() {}

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var values: [Any?] = []
        values.reserveCapacity(rows)
        for _ in 0..<rows {
            values.append(try reader.readUTF8String())
        }
        return values
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        for value in values {
            let v = value as? String ?? ""
            writer.writeUTF8String(v)
        }
    }
}

public struct CHJSONType: CHDataType {
    public let name = "JSON"
    public init() {}

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var values: [Any?] = []
        values.reserveCapacity(rows)
        for _ in 0..<rows {
            values.append(try reader.readUTF8String())
        }
        return values
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        for value in values {
            let v = value as? String ?? ""
            writer.writeUTF8String(v)
        }
    }
}

public struct CHFixedStringType: CHDataType {
    public let name: String
    public let length: Int

    public init(length: Int) {
        self.length = length
        self.name = "FixedString(\(length))"
    }

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var values: [Any?] = []
        values.reserveCapacity(rows)
        for _ in 0..<rows {
            let bytes = try reader.readBytes(count: length)
            values.append(Data(bytes))
        }
        return values
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        for value in values {
            if let data = value as? Data {
                var bytes = [UInt8](data)
                if bytes.count < length {
                    bytes.append(contentsOf: repeatElement(0, count: length - bytes.count))
                }
                writer.writeBytes(Array(bytes.prefix(length)))
            } else {
                writer.writeBytes(Array(repeating: 0, count: length))
            }
        }
    }
}

public struct CHDateType: CHDataType {
    public let name = "Date"
    public init() {}

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var values: [Any?] = []
        values.reserveCapacity(rows)
        for _ in 0..<rows {
            let days = try reader.readUInt16()
            let date = Date(timeIntervalSince1970: TimeInterval(days) * 86_400)
            values.append(date)
        }
        return values
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        for value in values {
            let date = value as? Date ?? Date(timeIntervalSince1970: 0)
            let days = UInt16(date.timeIntervalSince1970 / 86_400)
            writer.writeUInt16(days)
        }
    }
}

public struct CHDateTimeType: CHDataType {
    public let name: String
    public let timezone: TimeZone

    public init(timezone: TimeZone? = nil, name: String = "DateTime") {
        self.timezone = timezone ?? TimeZone.current
        self.name = name
    }

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var values: [Any?] = []
        values.reserveCapacity(rows)
        for _ in 0..<rows {
            let seconds = try reader.readUInt32()
            values.append(Date(timeIntervalSince1970: TimeInterval(seconds)))
        }
        return values
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        for value in values {
            let date = value as? Date ?? Date(timeIntervalSince1970: 0)
            let seconds = UInt32(date.timeIntervalSince1970)
            writer.writeUInt32(seconds)
        }
    }
}
