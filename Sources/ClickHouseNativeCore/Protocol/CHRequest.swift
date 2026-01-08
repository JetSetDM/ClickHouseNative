public enum CHRequest: @unchecked Sendable {
    case hello(CHHelloRequest)
    case query(CHQueryRequest)
    case data(CHDataRequest)
    case cancel
    case ping

    public func write(to writer: inout CHBinaryWriter) throws {
        switch self {
        case .hello(let hello):
            writer.writeVarInt(CHRequestType.hello.rawValue)
            hello.write(to: &writer)
        case .query(let query):
            writer.writeVarInt(CHRequestType.query.rawValue)
            try query.write(to: &writer)
        case .data(let data):
            writer.writeVarInt(CHRequestType.data.rawValue)
            try data.write(to: &writer)
        case .cancel:
            writer.writeVarInt(CHRequestType.cancel.rawValue)
        case .ping:
            writer.writeVarInt(CHRequestType.ping.rawValue)
        }
    }
}

public enum CHRequestType: UInt64 {
    case hello = 0
    case query = 1
    case data = 2
    case cancel = 3
    case ping = 4
}
