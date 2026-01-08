public struct CHBlockSettings {
    public enum SettingKey: UInt64 {
        case isOverflows = 1
        case bucketNum = 2
    }

    public var settings: [SettingKey: Int64]

    public init(settings: [SettingKey: Int64]) {
        self.settings = settings
    }

    public static func read(from reader: inout CHBinaryReader) throws -> CHBlockSettings {
        var result: [SettingKey: Int64] = [:]
        while true {
            let num = try reader.readVarInt()
            if num == 0 { break }
            guard let key = SettingKey(rawValue: num) else {
                throw CHBinaryError.unsupported("Unknown block setting: \(num)")
            }
            switch key {
            case .isOverflows:
                let value = try reader.readBool() ? Int64(1) : 0
                result[key] = value
            case .bucketNum:
                let value = try reader.readInt32()
                result[key] = Int64(value)
            }
        }
        return CHBlockSettings(settings: result)
    }

    public func write(to writer: inout CHBinaryWriter) {
        for (key, value) in settings {
            writer.writeVarInt(key.rawValue)
            switch key {
            case .isOverflows:
                writer.writeBool(value != 0)
            case .bucketNum:
                writer.writeInt32(Int32(value))
            }
        }
        writer.writeVarInt(0)
    }
}
