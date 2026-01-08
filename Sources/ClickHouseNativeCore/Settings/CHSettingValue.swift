import Foundation

public enum CHSettingValue: Sendable {
    case int64(Int64)
    case int32(Int32)
    case float32(Float)
    case bool(Bool)
    case string(String)
    case seconds(TimeInterval)
    case milliseconds(TimeInterval)
    case char(Character)
}
