public struct CHArrayType: CHDataType {
    public let name: String
    public let nested: CHDataType

    public init(nested: CHDataType) {
        self.nested = nested
        self.name = "Array(\(nested.name))"
    }

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        if rows == 0 { return [] }
        var offsets: [Int] = []
        offsets.reserveCapacity(rows)
        for _ in 0..<rows {
            let offset = Int(try reader.readInt64())
            offsets.append(offset)
        }
        let total = offsets.last ?? 0
        let elements = try nested.decodeColumn(rows: total, reader: &reader)
        var result: [Any?] = []
        result.reserveCapacity(rows)
        var last = 0
        for offset in offsets {
            let slice = Array(elements[last..<offset])
            result.append(slice)
            last = offset
        }
        return result
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        var offsets: [Int] = []
        offsets.reserveCapacity(values.count)
        var flat: [Any?] = []
        flat.reserveCapacity(values.count)
        var count = 0
        for value in values {
            let arrayValue: [Any?]
            if let v = value as? [Any?] {
                arrayValue = v
            } else if let v = value as? [Any] {
                arrayValue = v.map { Optional($0) }
            } else {
                arrayValue = []
            }
            flat.append(contentsOf: arrayValue)
            count += arrayValue.count
            offsets.append(count)
        }
        for offset in offsets {
            writer.writeInt64(Int64(offset))
        }
        try nested.encodeColumn(values: flat, writer: &writer)
    }
}
