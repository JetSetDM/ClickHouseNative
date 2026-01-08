import Foundation

public enum CHDefines {
    public static let name = "ClickHouse"

    public static let majorVersion: Int = 1
    public static let minorVersion: Int = 1
    public static let clientRevision: Int = 54_380

    public static let dbmsMinRevisionWithServerTimezone: Int = 54_058
    public static let dbmsMinRevisionWithServerDisplayName: Int = 54_372

    public static let compressionHeaderLength = 9
    public static let checksumLength = 16
}
