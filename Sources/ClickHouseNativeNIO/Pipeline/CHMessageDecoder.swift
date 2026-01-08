import NIOCore
import ClickHouseNativeCore

final class CHMessageDecoder: ByteToMessageDecoder {
    typealias InboundOut = CHResponse
    private let compressionEnabled: Bool
    private let serverContextBox: CHServerContextBox

    init(compressionEnabled: Bool, serverContextBox: CHServerContextBox) {
        self.compressionEnabled = compressionEnabled
        self.serverContextBox = serverContextBox
    }

    func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        let startIndex = buffer.readerIndex
        var reader = CHBinaryReader(buffer: buffer)

        do {
            let response = try CHResponseDecoder.read(
                from: &reader,
                compressionEnabled: compressionEnabled,
                serverContext: serverContextBox.context
            )
            let consumed = reader.index - startIndex
            if consumed > 0 {
                buffer.moveReaderIndex(forwardBy: consumed)
            }
            context.fireChannelRead(self.wrapInboundOut(response))
            return .continue
        } catch CHBinaryError.needMoreData {
            return .needMoreData
        }
    }
}

final class CHServerContextBox: @unchecked Sendable {
    var context: CHServerContext?
}
