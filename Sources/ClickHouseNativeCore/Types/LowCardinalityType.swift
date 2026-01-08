public struct CHLowCardinalityType: CHDataType {
    public let name: String
    public let nested: CHDataType

    public init(nested: CHDataType) {
        self.nested = nested
        self.name = "LowCardinality(\(nested.name))"
    }

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        let dictionarySize = Int(try reader.readVarInt())
        let dictionary = try nested.decodeColumn(rows: dictionarySize, reader: &reader)
        let keyWidth = keyWidthForDictionarySize(dictionarySize)
        var result: [Any?] = []
        result.reserveCapacity(rows)
        for _ in 0..<rows {
            let key = try readKey(width: keyWidth, reader: &reader)
            if key >= 0 && key < dictionary.count {
                result.append(dictionary[key])
            } else {
                result.append(nil)
            }
        }
        return result
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        var dict: [CHLowCardKey: Int] = [:]
        var dictionaryValues: [Any?] = []
        var keys: [Int] = []
        dictionaryValues.reserveCapacity(values.count)
        keys.reserveCapacity(values.count)

        for value in values {
            let key = CHLowCardKey.from(value)
            if case .null = key, !(nested is CHNullableType) {
                throw CHBinaryError.malformed("LowCardinality value is nil but nested type is not Nullable")
            }
            if let index = dict[key] {
                keys.append(index)
            } else {
                let index = dictionaryValues.count
                dict[key] = index
                dictionaryValues.append(value)
                keys.append(index)
            }
        }

        writer.writeVarInt(UInt64(dictionaryValues.count))
        try nested.encodeColumn(values: dictionaryValues, writer: &writer)

        let keyWidth = keyWidthForDictionarySize(dictionaryValues.count)
        for key in keys {
            writeKey(key, width: keyWidth, writer: &writer)
        }
    }

    private func keyWidthForDictionarySize(_ size: Int) -> Int {
        if size <= Int(UInt8.max) { return 8 }
        if size <= Int(UInt16.max) { return 16 }
        if size <= Int(UInt32.max) { return 32 }
        return 64
    }

    private func readKey(width: Int, reader: inout CHBinaryReader) throws -> Int {
        switch width {
        case 8:
            return Int(try reader.readByte())
        case 16:
            return Int(try reader.readUInt16())
        case 32:
            return Int(try reader.readUInt32())
        default:
            return Int(try reader.readUInt64())
        }
    }

    private func writeKey(_ value: Int, width: Int, writer: inout CHBinaryWriter) {
        switch width {
        case 8:
            writer.writeByte(UInt8(truncatingIfNeeded: value))
        case 16:
            writer.writeUInt16(UInt16(truncatingIfNeeded: value))
        case 32:
            writer.writeUInt32(UInt32(truncatingIfNeeded: value))
        default:
            writer.writeUInt64(UInt64(value))
        }
    }
}

private enum CHLowCardKey: Hashable {
    case null
    case hashable(AnyHashable)
    case string(String)

    static func from(_ value: Any?) -> CHLowCardKey {
        if value == nil { return .null }
        if let hashable = value as? AnyHashable {
            return .hashable(hashable)
        }
        return .string(String(describing: value))
    }
}
