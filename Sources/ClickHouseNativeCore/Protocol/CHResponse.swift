public enum CHResponse: @unchecked Sendable {
    case hello(CHHelloResponse)
    case data(CHDataResponse)
    case progress(CHProgressResponse)
    case pong
    case eof
    case profileInfo(CHProfileInfoResponse)
    case totals(CHTotalsResponse)
    case extremes(CHExtremesResponse)
    case exception(CHServerException)
}

public enum CHResponseType: UInt64 {
    case hello = 0
    case data = 1
    case exception = 2
    case progress = 3
    case pong = 4
    case endOfStream = 5
    case profileInfo = 6
    case totals = 7
    case extremes = 8
    case tablesStatus = 9
}

public enum CHResponseDecoder {
    public static func read(from reader: inout CHBinaryReader, compressionEnabled: Bool, serverContext: CHServerContext? = nil) throws -> CHResponse {
        let typeId = try reader.readVarInt()
        guard let type = CHResponseType(rawValue: typeId) else {
            throw CHBinaryError.malformed("Unknown response type: \(typeId)")
        }
        switch type {
        case .hello:
            return .hello(try CHHelloResponse.read(from: &reader))
        case .data:
            return .data(try CHDataResponse.read(from: &reader, compressionEnabled: compressionEnabled, serverContext: serverContext))
        case .exception:
            return .exception(try CHServerException.read(from: &reader))
        case .progress:
            return .progress(try CHProgressResponse.read(from: &reader))
        case .pong:
            return .pong
        case .endOfStream:
            return .eof
        case .profileInfo:
            return .profileInfo(try CHProfileInfoResponse.read(from: &reader))
        case .totals:
            return .totals(try CHTotalsResponse.read(from: &reader, compressionEnabled: compressionEnabled, serverContext: serverContext))
        case .extremes:
            return .extremes(try CHExtremesResponse.read(from: &reader, compressionEnabled: compressionEnabled, serverContext: serverContext))
        case .tablesStatus:
            throw CHBinaryError.unsupported("tables status response not implemented")
        }
    }
}
