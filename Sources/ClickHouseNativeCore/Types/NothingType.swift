public struct CHNothingType: CHDataType {
    public let name = "Nothing"

    public init() {}

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        if rows == 0 { return [] }
        return Array(repeating: nil, count: rows)
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        // Nothing columns have zero bytes per row; nothing to write.
    }
}
