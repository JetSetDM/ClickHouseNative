import Foundation

public struct CHNullableType: CHDataType {
    public let name: String
    public let nested: CHDataType

    public init(nested: CHDataType) {
        self.nested = nested
        self.name = "Nullable(\(nested.name))"
    }

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var nullMap: [UInt8] = []
        nullMap.reserveCapacity(rows)
        for _ in 0..<rows {
            nullMap.append(try reader.readByte())
        }
        let nestedValues = try nested.decodeColumn(rows: rows, reader: &reader)
        var values: [Any?] = []
        values.reserveCapacity(rows)
        for i in 0..<rows {
            if nullMap[i] == 1 {
                values.append(nil)
            } else {
                values.append(nestedValues[i])
            }
        }
        return values
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        var nullMap: [UInt8] = []
        var nestedValues: [Any?] = []
        nullMap.reserveCapacity(values.count)
        nestedValues.reserveCapacity(values.count)
        for value in values {
            if value == nil {
                nullMap.append(1)
                nestedValues.append(defaultValue())
            } else {
                nullMap.append(0)
                nestedValues.append(value)
            }
        }
        writer.writeBytes(nullMap)
        try nested.encodeColumn(values: nestedValues, writer: &writer)
    }

    private func defaultValue() -> Any? {
        switch nested {
        case is CHInt8Type, is CHInt16Type, is CHInt32Type, is CHInt64Type:
            return Int64(0)
        case is CHUInt8Type, is CHUInt16Type, is CHUInt32Type, is CHUInt64Type:
            return UInt64(0)
        case is CHFloat32Type:
            return Float(0)
        case is CHFloat64Type:
            return Double(0)
        case is CHStringType:
            return ""
        case is CHDateType:
            return Date(timeIntervalSince1970: 0)
        case is CHDateTimeType:
            return Date(timeIntervalSince1970: 0)
        default:
            return nil
        }
    }
}
