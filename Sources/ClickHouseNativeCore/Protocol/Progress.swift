public struct CHProgressResponse: Sendable {
    public var newRows: UInt64
    public var newBytes: UInt64
    public var newTotalRows: UInt64

    public static func read(from reader: inout CHBinaryReader) throws -> CHProgressResponse {
        let newRows = try reader.readVarInt()
        let newBytes = try reader.readVarInt()
        let newTotal = try reader.readVarInt()
        return CHProgressResponse(newRows: newRows, newBytes: newBytes, newTotalRows: newTotal)
    }
}
