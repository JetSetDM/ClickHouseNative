public enum CHCompressedFrame {
    public static func read(from reader: inout CHBinaryReader) throws -> [UInt8] {
        _ = try reader.readBytes(count: CHDefines.checksumLength)
        let header = try reader.readBytes(count: CHDefines.compressionHeaderLength)
        guard header.count == CHDefines.compressionHeaderLength else {
            throw CHBinaryError.malformed("Invalid compression header")
        }

        let method = header[0]
        let compressedSize = Int(decodeInt32LE(header, offset: 1))
        let decompressedSize = Int(decodeInt32LE(header, offset: 5))
        let payloadSize = compressedSize - CHDefines.compressionHeaderLength
        if payloadSize < 0 {
            throw CHBinaryError.malformed("Invalid compressed size")
        }

        switch method {
        case CHCompressionMethod.none:
            return try reader.readBytes(count: decompressedSize)
        case CHCompressionMethod.lz4:
            let payload = try reader.readBytes(count: payloadSize)
            let codec = CHLZ4Codec()
            return try codec.decompress(payload, originalSize: decompressedSize)
        default:
            throw CHBinaryError.unsupported("Unknown compression method: \(method)")
        }
    }

    public static func write(uncompressed: [UInt8], to writer: inout CHBinaryWriter, codec: CHCompressionCodec) throws {
        let compressed = try codec.compress(uncompressed)
        let compressedSize = compressed.count + CHDefines.compressionHeaderLength
        let header = buildHeader(
            method: codec.methodByte,
            compressedSize: compressedSize,
            decompressedSize: uncompressed.count
        )

        var checksumInput = header
        checksumInput.append(contentsOf: compressed)
        let checksum = CHCityHash.cityHash128(bytes: checksumInput, offset: 0, length: checksumInput.count)

        writer.writeBytes(encodeUInt64LE(checksum.0))
        writer.writeBytes(encodeUInt64LE(checksum.1))
        writer.writeBytes(header)
        writer.writeBytes(compressed)
    }

    private static func buildHeader(method: UInt8, compressedSize: Int, decompressedSize: Int) -> [UInt8] {
        var header = [UInt8](repeating: 0, count: CHDefines.compressionHeaderLength)
        header[0] = method
        let csize = encodeInt32LE(Int32(compressedSize))
        let dsize = encodeInt32LE(Int32(decompressedSize))
        header[1] = csize[0]
        header[2] = csize[1]
        header[3] = csize[2]
        header[4] = csize[3]
        header[5] = dsize[0]
        header[6] = dsize[1]
        header[7] = dsize[2]
        header[8] = dsize[3]
        return header
    }

    private static func decodeInt32LE(_ bytes: [UInt8], offset: Int) -> Int32 {
        let b0 = UInt32(bytes[offset])
        let b1 = UInt32(bytes[offset + 1]) << 8
        let b2 = UInt32(bytes[offset + 2]) << 16
        let b3 = UInt32(bytes[offset + 3]) << 24
        return Int32(bitPattern: b0 | b1 | b2 | b3)
    }

    private static func encodeInt32LE(_ value: Int32) -> [UInt8] {
        let v = UInt32(bitPattern: value)
        return [
            UInt8(truncatingIfNeeded: v),
            UInt8(truncatingIfNeeded: v >> 8),
            UInt8(truncatingIfNeeded: v >> 16),
            UInt8(truncatingIfNeeded: v >> 24)
        ]
    }

    private static func encodeUInt64LE(_ value: UInt64) -> [UInt8] {
        return [
            UInt8(truncatingIfNeeded: value),
            UInt8(truncatingIfNeeded: value >> 8),
            UInt8(truncatingIfNeeded: value >> 16),
            UInt8(truncatingIfNeeded: value >> 24),
            UInt8(truncatingIfNeeded: value >> 32),
            UInt8(truncatingIfNeeded: value >> 40),
            UInt8(truncatingIfNeeded: value >> 48),
            UInt8(truncatingIfNeeded: value >> 56)
        ]
    }
}
