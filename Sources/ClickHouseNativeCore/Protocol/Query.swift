import Foundation

public enum CHQueryStage: UInt64, Sendable {
    case fetchColumns = 0
    case withMergeableState = 1
    case complete = 2
    case withMergeableStateAfterAggregation = 3
}

public struct CHQueryRequest {
    public static let stageComplete: UInt64 = CHQueryStage.complete.rawValue

    public var queryId: String
    public var clientContext: CHClientContext
    public var stage: UInt64
    public var compression: Bool
    public var query: String
    public var settings: [String: CHSettingValue]

    public init(
        queryId: String,
        clientContext: CHClientContext,
        stage: UInt64 = CHQueryRequest.stageComplete,
        compression: Bool,
        query: String,
        settings: [String: CHSettingValue]
    ) {
        self.queryId = queryId
        self.clientContext = clientContext
        self.stage = stage
        self.compression = compression
        self.query = query
        self.settings = settings
    }

    public func write(to writer: inout CHBinaryWriter) throws {
        writer.writeUTF8String(queryId)
        clientContext.write(to: &writer)

        for (key, value) in settings {
            writer.writeUTF8String(key)
            writeSettingValue(value, to: &writer)
        }
        writer.writeUTF8String("")

        writer.writeVarInt(stage)
        writer.writeBool(compression)
        writer.writeUTF8String(query)

        let codec: CHCompressionCodec? = compression ? CHLZ4Codec() : nil
        let emptyData = CHDataRequest(name: "", block: CHBlock.empty(), compression: codec)
        // Even when there are no external tables, ClickHouse expects an (empty) DATA packet here,
        // including the packet type prefix.
        writer.writeVarInt(CHRequestType.data.rawValue)
        try emptyData.write(to: &writer)
    }

    private func writeSettingValue(_ value: CHSettingValue, to writer: inout CHBinaryWriter) {
        switch value {
        case .int64(let v):
            writer.writeVarInt(UInt64(bitPattern: v))
        case .int32(let v):
            writer.writeVarInt(UInt64(bitPattern: Int64(v)))
        case .float32(let v):
            writer.writeUTF8String(String(v))
        case .bool(let v):
            writer.writeVarInt(v ? 1 : 0)
        case .string(let v):
            writer.writeUTF8String(v)
        case .seconds(let v):
            writer.writeVarInt(UInt64(Int64(v)))
        case .milliseconds(let v):
            writer.writeVarInt(UInt64(Int64(v)))
        case .char(let v):
            writer.writeUTF8String(String(v))
        }
    }
}
