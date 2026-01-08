import Foundation

public struct CHDate32Type: CHDataType {
    public let name = "Date32"
    public init() {}

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var values: [Any?] = []
        values.reserveCapacity(rows)
        for _ in 0..<rows {
            let days = try reader.readInt32()
            let date = Date(timeIntervalSince1970: TimeInterval(days) * 86_400)
            values.append(date)
        }
        return values
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        for value in values {
            let date = value as? Date ?? Date(timeIntervalSince1970: 0)
            let days = Int32(date.timeIntervalSince1970 / 86_400)
            writer.writeInt32(days)
        }
    }
}
