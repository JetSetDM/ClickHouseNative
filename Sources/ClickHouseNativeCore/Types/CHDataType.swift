public protocol CHDataType {
    var name: String { get }

    func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?]
    func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws
}
