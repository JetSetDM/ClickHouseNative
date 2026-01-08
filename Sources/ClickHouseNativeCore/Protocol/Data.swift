import Foundation

import NIOCore

public struct CHDataRequest {
    public var name: String
    public var block: CHBlock
    public var compression: CHCompressionCodec?

    public init(name: String, block: CHBlock, compression: CHCompressionCodec? = nil) {
        self.name = name
        self.block = block
        self.compression = compression
    }

    public func write(to writer: inout CHBinaryWriter) throws {
        writer.writeUTF8String(name)
        if let compression = compression {
            let allocator = ByteBufferAllocator()
            var tempBuffer = allocator.buffer(capacity: 0)
            var tempWriter = CHBinaryWriter(buffer: tempBuffer)
            try block.write(to: &tempWriter)
            tempBuffer = tempWriter.buffer
            let bytes = tempBuffer.getBytes(at: tempBuffer.readerIndex, length: tempBuffer.readableBytes) ?? []
            try CHCompressedFrame.write(uncompressed: bytes, to: &writer, codec: compression)
        } else {
            try block.write(to: &writer)
        }
    }
}

public struct CHDataResponse {
    public var name: String
    public var block: CHBlock

    public static func read(from reader: inout CHBinaryReader, compressionEnabled: Bool, serverContext: CHServerContext? = nil) throws -> CHDataResponse {
        let name = try reader.readUTF8String()
        let block: CHBlock
        if compressionEnabled {
            // When compression is enabled, ClickHouse streams the block through a sequence of compressed frames.
            // We incrementally decompress frames until a full block can be decoded.
            let allocator = ByteBufferAllocator()
            var decompressedBuffer = allocator.buffer(capacity: 0)
            while true {
                do {
                    var tempReader = CHBinaryReader(buffer: decompressedBuffer)
                    let decoded = try CHBlock.read(from: &tempReader, serverContext: serverContext)
                    block = decoded
                    break
                } catch CHBinaryError.needMoreData {
                    let chunk = try CHCompressedFrame.read(from: &reader)
                    decompressedBuffer.writeBytes(chunk)
                }
            }
        } else {
            block = try CHBlock.read(from: &reader, serverContext: serverContext)
        }
        return CHDataResponse(name: name, block: block)
    }
}

public struct CHTotalsResponse: Sendable {
    public var name: String
    public var block: CHBlock

    public static func read(from reader: inout CHBinaryReader, compressionEnabled: Bool, serverContext: CHServerContext? = nil) throws -> CHTotalsResponse {
        let name = try reader.readUTF8String()
        let block: CHBlock
        if compressionEnabled {
            let allocator = ByteBufferAllocator()
            var decompressedBuffer = allocator.buffer(capacity: 0)
            while true {
                do {
                    var tempReader = CHBinaryReader(buffer: decompressedBuffer)
                    let decoded = try CHBlock.read(from: &tempReader, serverContext: serverContext)
                    block = decoded
                    break
                } catch CHBinaryError.needMoreData {
                    let chunk = try CHCompressedFrame.read(from: &reader)
                    decompressedBuffer.writeBytes(chunk)
                }
            }
        } else {
            block = try CHBlock.read(from: &reader, serverContext: serverContext)
        }
        return CHTotalsResponse(name: name, block: block)
    }
}

public struct CHExtremesResponse: Sendable {
    public var name: String
    public var block: CHBlock

    public static func read(from reader: inout CHBinaryReader, compressionEnabled: Bool, serverContext: CHServerContext? = nil) throws -> CHExtremesResponse {
        let name = try reader.readUTF8String()
        let block: CHBlock
        if compressionEnabled {
            let allocator = ByteBufferAllocator()
            var decompressedBuffer = allocator.buffer(capacity: 0)
            while true {
                do {
                    var tempReader = CHBinaryReader(buffer: decompressedBuffer)
                    let decoded = try CHBlock.read(from: &tempReader, serverContext: serverContext)
                    block = decoded
                    break
                } catch CHBinaryError.needMoreData {
                    let chunk = try CHCompressedFrame.read(from: &reader)
                    decompressedBuffer.writeBytes(chunk)
                }
            }
        } else {
            block = try CHBlock.read(from: &reader, serverContext: serverContext)
        }
        return CHExtremesResponse(name: name, block: block)
    }
}
