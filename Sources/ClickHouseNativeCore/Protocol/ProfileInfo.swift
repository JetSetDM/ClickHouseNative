public struct CHProfileInfoResponse: Sendable {
    public var rows: UInt64
    public var blocks: UInt64
    public var bytes: UInt64
    public var appliedLimit: UInt64
    public var rowsBeforeLimit: UInt64
    public var calculatedRowsBeforeLimit: Bool

    public static func read(from reader: inout CHBinaryReader) throws -> CHProfileInfoResponse {
        let rows = try reader.readVarInt()
        let blocks = try reader.readVarInt()
        let bytes = try reader.readVarInt()
        let appliedLimit = try reader.readVarInt()
        let rowsBeforeLimit = try reader.readVarInt()
        let calculated = try reader.readBool()
        return CHProfileInfoResponse(
            rows: rows,
            blocks: blocks,
            bytes: bytes,
            appliedLimit: appliedLimit,
            rowsBeforeLimit: rowsBeforeLimit,
            calculatedRowsBeforeLimit: calculated
        )
    }
}
