public struct CHEnum8Type: CHDataType {
    public let name: String
    public let names: [String]
    public let values: [Int8]

    public init(names: [String], values: [Int8]) {
        self.names = names
        self.values = values
        var parts: [String] = []
        for i in 0..<names.count {
            parts.append("'\(names[i])' = \(values[i])")
        }
        self.name = "Enum8(\(parts.joined(separator: ",")))"
    }

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var result: [Any?] = []
        result.reserveCapacity(rows)
        for _ in 0..<rows {
            let byte = Int8(bitPattern: try reader.readByte())
            if let index = values.firstIndex(of: byte) {
                result.append(names[index])
            } else {
                result.append(nil)
            }
        }
        return result
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        for value in values {
            if let s = value as? String, let index = names.firstIndex(of: s) {
                writer.writeByte(UInt8(bitPattern: valueForIndex(index)))
            } else {
                writer.writeByte(0)
            }
        }
    }

    private func valueForIndex(_ idx: Int) -> Int8 {
        return self.values[idx]
    }
}

public struct CHEnum16Type: CHDataType {
    public let name: String
    public let names: [String]
    public let values: [Int16]

    public init(names: [String], values: [Int16]) {
        self.names = names
        self.values = values
        var parts: [String] = []
        for i in 0..<names.count {
            parts.append("'\(names[i])' = \(values[i])")
        }
        self.name = "Enum16(\(parts.joined(separator: ",")))"
    }

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var result: [Any?] = []
        result.reserveCapacity(rows)
        for _ in 0..<rows {
            let val = try reader.readInt16()
            if let index = values.firstIndex(of: val) {
                result.append(names[index])
            } else {
                result.append(nil)
            }
        }
        return result
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        for value in values {
            if let s = value as? String, let index = names.firstIndex(of: s) {
                writer.writeInt16(valueForIndex(index))
            } else {
                writer.writeInt16(0)
            }
        }
    }

    private func valueForIndex(_ idx: Int) -> Int16 {
        return self.values[idx]
    }
}
