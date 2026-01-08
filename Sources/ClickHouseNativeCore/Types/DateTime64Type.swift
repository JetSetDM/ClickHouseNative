import Foundation

public struct CHDateTime64Type: CHDataType {
    public let name: String
    public let scale: Int
    public let timezone: TimeZone

    public init(scale: Int, timezone: TimeZone? = nil, name: String? = nil) {
        self.scale = scale
        self.timezone = timezone ?? TimeZone.current
        if let name {
            self.name = name
        } else {
            self.name = "DateTime64(\(scale))"
        }
    }

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        var values: [Any?] = []
        values.reserveCapacity(rows)
        let divisor = pow10(scale)
        for _ in 0..<rows {
            let raw = try reader.readInt64()
            let seconds = Double(raw) / divisor
            values.append(Date(timeIntervalSince1970: seconds))
        }
        return values
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        let multiplier = pow10(scale)
        for value in values {
            let date = value as? Date ?? Date(timeIntervalSince1970: 0)
            let raw = Int64((date.timeIntervalSince1970 * multiplier).rounded())
            writer.writeInt64(raw)
        }
    }

    private func pow10(_ n: Int) -> Double {
        return pow(10.0, Double(n))
    }
}
