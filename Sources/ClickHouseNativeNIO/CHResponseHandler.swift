import NIOCore
import ClickHouseNativeCore

final class CHResponseHandler: ChannelInboundHandler {
    typealias InboundIn = CHResponse

    private let continuation: AsyncThrowingStream<CHResponse, Error>.Continuation

    init(continuation: AsyncThrowingStream<CHResponse, Error>.Continuation) {
        self.continuation = continuation
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let response = self.unwrapInboundIn(data)
        continuation.yield(response)
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        continuation.finish(throwing: error)
        context.close(promise: nil)
    }

    func channelInactive(context: ChannelHandlerContext) {
        continuation.finish()
    }
}
