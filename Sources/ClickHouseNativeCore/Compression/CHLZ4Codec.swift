import Foundation
import CLZ4

public struct CHLZ4Codec: CHCompressionCodec {
    public let methodByte: UInt8 = CHCompressionMethod.lz4

    public init() {}

    public func compress(_ data: [UInt8]) throws -> [UInt8] {
        let maxDestSize = Int(LZ4_compressBound(Int32(data.count)))
        var dest = [UInt8](repeating: 0, count: maxDestSize)
        let compressedSize = data.withUnsafeBytes { srcPtr in
            dest.withUnsafeMutableBytes { dstPtr in
                LZ4_compress_default(
                    srcPtr.bindMemory(to: Int8.self).baseAddress,
                    dstPtr.bindMemory(to: Int8.self).baseAddress,
                    Int32(data.count),
                    Int32(maxDestSize)
                )
            }
        }
        if compressedSize <= 0 {
            throw CHBinaryError.malformed("LZ4 compression failed")
        }
        dest.removeSubrange(Int(compressedSize)..<dest.count)
        return dest
    }

    public func decompress(_ data: [UInt8], originalSize: Int) throws -> [UInt8] {
        var dest = [UInt8](repeating: 0, count: originalSize)
        let res = data.withUnsafeBytes { srcPtr in
            dest.withUnsafeMutableBytes { dstPtr in
                LZ4_decompress_safe(
                    srcPtr.bindMemory(to: Int8.self).baseAddress,
                    dstPtr.bindMemory(to: Int8.self).baseAddress,
                    Int32(data.count),
                    Int32(originalSize)
                )
            }
        }
        if res < 0 || res != Int32(originalSize) {
            throw CHBinaryError.malformed("LZ4 decompression failed")
        }
        return dest
    }
}
