import Foundation
import ClickHouseNativeCore

public struct CHQueryOptions: Sendable {
    public var queryId: String?
    public var stage: CHQueryStage?

    public init(queryId: String? = nil, stage: CHQueryStage? = nil) {
        self.queryId = queryId
        self.stage = stage
    }
}
