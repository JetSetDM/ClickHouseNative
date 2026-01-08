import Foundation
import NIOCore
import ClickHouseNativeCore

struct Bench {
    static func run() throws {
        let rows = 10_000
        let iterations = 200

        var builder = CHBlockBuilder()
        builder.addColumn(name: "id", type: CHUInt64Type(), values: (0..<rows).map { UInt64($0) })
        builder.addColumn(name: "value", type: CHStringType(), values: (0..<rows).map { "v\($0)" })
        let block = try builder.build()

        let allocator = ByteBufferAllocator()

        var encodeTotal: UInt64 = 0
        var decodeTotal: UInt64 = 0

        for _ in 0..<iterations {
            var buffer = allocator.buffer(capacity: 0)
            var writer = CHBinaryWriter(buffer: buffer)
            let t0 = DispatchTime.now().uptimeNanoseconds
            try block.write(to: &writer)
            let t1 = DispatchTime.now().uptimeNanoseconds
            buffer = writer.buffer
            encodeTotal += (t1 - t0)

            var reader = CHBinaryReader(buffer: buffer)
            let t2 = DispatchTime.now().uptimeNanoseconds
            _ = try CHBlock.read(from: &reader)
            let t3 = DispatchTime.now().uptimeNanoseconds
            decodeTotal += (t3 - t2)
        }

        let encodeAvg = Double(encodeTotal) / Double(iterations) / 1_000_000_000.0
        let decodeAvg = Double(decodeTotal) / Double(iterations) / 1_000_000_000.0

        print("Rows: \(rows), iterations: \(iterations)")
        print(String(format: "Encode avg: %.6f s", encodeAvg))
        print(String(format: "Decode avg: %.6f s", decodeAvg))
    }
}

try Bench.run()
