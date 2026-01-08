public struct CHBlockBuilder {
    private var columns: [CHColumn] = []

    public init() {}

    public mutating func addColumn(name: String, type: CHDataType, values: [Any?]) {
        columns.append(CHColumn(name: name, type: type, values: values))
    }

    public mutating func addColumn<T>(name: String, type: CHDataType, values: [T]) {
        let boxed: [Any?] = values.map { value in
            if let opt = value as? any _CHOptional {
                return opt._wrapped
            }
            return value
        }
        columns.append(CHColumn(name: name, type: type, values: boxed))
    }

    public func build() throws -> CHBlock {
        let rowCount = try inferRowCount()
        return CHBlock(rowCount: rowCount, columns: columns)
    }

    private func inferRowCount() throws -> Int {
        guard let first = columns.first else { return 0 }
        let expected = first.values.count
        for column in columns {
            if column.values.count != expected {
                throw CHBinaryError.malformed("Column sizes mismatch")
            }
        }
        return expected
    }
}

private protocol _CHOptional {
    var _wrapped: Any? { get }
}

extension Optional: _CHOptional {
    fileprivate var _wrapped: Any? { self }
}
