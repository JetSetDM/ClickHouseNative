import NIOCore
import ClickHouseNativeCore

final class CHMessageEncoder: MessageToByteEncoder {
    typealias OutboundIn = CHRequest

    func encode(data: CHRequest, out: inout ByteBuffer) throws {
        var writer = CHBinaryWriter(buffer: out)
        try data.write(to: &writer)
        out = writer.buffer
    }
}
