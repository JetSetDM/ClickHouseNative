import Foundation

public protocol CHCompressionCodec {
    var methodByte: UInt8 { get }
    func compress(_ data: [UInt8]) throws -> [UInt8]
    func decompress(_ data: [UInt8], originalSize: Int) throws -> [UInt8]
}

public enum CHCompressionMethod {
    public static let none: UInt8 = 0x02
    public static let lz4: UInt8 = 0x82
}
