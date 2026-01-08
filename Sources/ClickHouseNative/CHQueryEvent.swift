import Foundation
import ClickHouseNativeCore

public enum CHQueryEvent: Sendable {
    case data(CHBlock)
    case progress(CHProgressResponse)
    case totals(CHTotalsResponse)
    case extremes(CHExtremesResponse)
    case profileInfo(CHProfileInfoResponse)
}
