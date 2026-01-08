public struct CHMapType: CHDataType {
    public let name: String
    public let keyType: CHDataType
    public let valueType: CHDataType

    public init(key: CHDataType, value: CHDataType) {
        self.keyType = key
        self.valueType = value
        self.name = "Map(\(key.name), \(value.name))"
    }

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        if rows == 0 { return [] }
        var offsets: [Int] = []
        offsets.reserveCapacity(rows)
        for _ in 0..<rows {
            offsets.append(Int(try reader.readInt64()))
        }
        let total = offsets.last ?? 0
        let keys = try keyType.decodeColumn(rows: total, reader: &reader)
        let values = try valueType.decodeColumn(rows: total, reader: &reader)

        var result: [Any?] = []
        result.reserveCapacity(rows)
        var last = 0
        for offset in offsets {
            if offset > last {
                var dict: [AnyHashable: Any?] = [:]
                var fallbackPairs: [(Any?, Any?)] = []
                var canDict = true
                for i in last..<offset {
                    let key = keys[i]
                    let value = values[i]
                    if let h = key as? AnyHashable {
                        dict[h] = value
                    } else {
                        canDict = false
                        fallbackPairs.append((key, value))
                    }
                }
                if canDict {
                    result.append(dict)
                } else {
                    if fallbackPairs.isEmpty {
                        fallbackPairs = (last..<offset).map { (keys[$0], values[$0]) }
                    }
                    result.append(fallbackPairs)
                }
            } else {
                result.append([AnyHashable: Any?]())
            }
            last = offset
        }
        return result
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        var offsets: [Int] = []
        var allKeys: [Any?] = []
        var allValues: [Any?] = []
        offsets.reserveCapacity(values.count)
        var count = 0

        for value in values {
            let pairs: [(Any?, Any?)]
            if let dict = value as? [AnyHashable: Any?] {
                pairs = dict.map { ($0.key, $0.value) }
            } else if let dict = value as? [String: Any?] {
                pairs = dict.map { ($0.key, $0.value) }
            } else if let arr = value as? [(Any?, Any?)] {
                pairs = arr
            } else {
                pairs = []
            }
            for pair in pairs {
                allKeys.append(pair.0)
                allValues.append(pair.1)
            }
            count += pairs.count
            offsets.append(count)
        }

        for offset in offsets {
            writer.writeInt64(Int64(offset))
        }
        try keyType.encodeColumn(values: allKeys, writer: &writer)
        try valueType.encodeColumn(values: allValues, writer: &writer)
    }
}
