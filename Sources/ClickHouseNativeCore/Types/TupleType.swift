public struct CHTupleType: CHDataType {
    public let name: String
    public let nested: [CHDataType]

    public init(nested: [CHDataType]) {
        self.nested = nested
        let inner = nested.map { $0.name }.joined(separator: ",")
        self.name = "Tuple(\(inner))"
    }

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var columns: [[Any?]] = []
        columns.reserveCapacity(nested.count)
        for type in nested {
            let column = try type.decodeColumn(rows: rows, reader: &reader)
            columns.append(column)
        }
        var rowsData: [Any?] = []
        rowsData.reserveCapacity(rows)
        for row in 0..<rows {
            var tuple: [Any?] = []
            tuple.reserveCapacity(nested.count)
            for col in columns {
                tuple.append(col[row])
            }
            rowsData.append(tuple)
        }
        return rowsData
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        var columns: [[Any?]] = Array(repeating: [], count: nested.count)
        for value in values {
            let tuple: [Any?]
            if let v = value as? [Any?] {
                tuple = v
            } else if let v = value as? [Any] {
                tuple = v.map { Optional($0) }
            } else {
                tuple = []
            }
            for i in 0..<nested.count {
                if i < tuple.count {
                    columns[i].append(tuple[i])
                } else {
                    columns[i].append(nil)
                }
            }
        }
        for (index, type) in nested.enumerated() {
            try type.encodeColumn(values: columns[index], writer: &writer)
        }
    }
}
