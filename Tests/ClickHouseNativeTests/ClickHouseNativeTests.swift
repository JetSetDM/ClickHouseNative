import Testing
import Foundation
import NIOCore
@testable import ClickHouseNative
@testable import ClickHouseNativeCore

@Test func dataTypeFactoryParsesComplexTypes() async throws {
    let type = try CHDataTypeFactory.parse("Array(Nullable(Int32))")
    #expect(type.name == "Array(Nullable(Int32))")

    let type2 = try CHDataTypeFactory.parse("Tuple(String, UInt64, DateTime64(3, 'UTC'))")
    #expect(type2.name == "Tuple(String,UInt64,DateTime64(3, 'UTC'))")

    let type3 = try CHDataTypeFactory.parse("LowCardinality(String)")
    #expect(type3.name == "LowCardinality(String)")
}

@Test func dataTypeFactoryParsesAliasesAndNothing() async throws {
    let boolType = try CHDataTypeFactory.parse("Bool")
    #expect(boolType.name == "Bool")

    let binaryType = try CHDataTypeFactory.parse("Binary(4)")
    let fixed = binaryType as? CHFixedStringType
    #expect(fixed?.length == 4)

    let nothingType = try CHDataTypeFactory.parse("Nothing")
    #expect(nothingType.name == "Nothing")
}

@Test func boolTypeRoundtrip() async throws {
    let type = CHBoolType()
    let values: [Any?] = [true, false, true]

    let allocator = ByteBufferAllocator()
    var buffer = allocator.buffer(capacity: 0)
    var writer = CHBinaryWriter(buffer: buffer)
    try type.encodeColumn(values: values, writer: &writer)
    buffer = writer.buffer

    var reader = CHBinaryReader(buffer: buffer)
    let decoded = try type.decodeColumn(rows: values.count, reader: &reader)
    let output = decoded.compactMap { $0 as? Bool }
    #expect(output == [true, false, true])
}

@Test func sqlLiteralParsing_stringAndBool() async throws {
    let text = try CHSQLLiteralParser.parseString("'hello''world'")
    #expect(text == "hello'world")

    #expect(try CHSQLLiteralParser.parseBool("1"))
    #expect(!(try CHSQLLiteralParser.parseBool("0")))
    #expect(try CHSQLLiteralParser.parseBool("true"))
    #expect(!(try CHSQLLiteralParser.parseBool("false")))
}

@Test func sqlLiteralParsing_dateAndDateTime() async throws {
    let tz = TimeZone(secondsFromGMT: 0)!
    let date = try CHSQLLiteralParser.parseDate("'2020-01-02'", timeZone: tz)
    var calendar = Calendar(identifier: .gregorian)
    calendar.timeZone = tz
    let comps = calendar.dateComponents([.year, .month, .day], from: date)
    #expect(comps.year == 2020)
    #expect(comps.month == 1)
    #expect(comps.day == 2)

    let dt = try CHSQLLiteralParser.parseDateTime("'2020-01-02 03:04:05.123'", timeZone: tz)
    let comps2 = calendar.dateComponents([.year, .month, .day, .hour, .minute, .second, .nanosecond], from: dt)
    #expect(comps2.year == 2020)
    #expect(comps2.month == 1)
    #expect(comps2.day == 2)
    #expect(comps2.hour == 3)
    #expect(comps2.minute == 4)
    #expect(comps2.second == 5)
    #expect((comps2.nanosecond ?? 0) / 1_000_000 == 123)
}

@Test func sqlLiteralParsing_uuid() async throws {
    let uuid = try CHSQLLiteralParser.parseUUID("'00000000-0000-0000-0000-000000000001'")
    #expect(uuid.uuidString.lowercased() == "00000000-0000-0000-0000-000000000001")
}

@Test func decimalRoundtrip128() async throws {
    let type = CHDecimalType(precision: 38, scale: 6)
    let input = Decimal(string: "12345.678901")!

    let allocator = ByteBufferAllocator()
    var buffer = allocator.buffer(capacity: 0)
    var writer = CHBinaryWriter(buffer: buffer)
    try type.encodeColumn(values: [input], writer: &writer)
    buffer = writer.buffer

    var reader = CHBinaryReader(buffer: buffer)
    let decoded = try type.decodeColumn(rows: 1, reader: &reader)
    let output = decoded.first as? Decimal
    #expect(output == input)
}

@Test func decimalRoundtrip32() async throws {
    let type = CHDecimalType(precision: 7, scale: 2)
    let input1 = Decimal(string: "1234.56")!
    let input2 = Decimal(string: "-0.01")!

    let allocator = ByteBufferAllocator()
    var buffer = allocator.buffer(capacity: 0)
    var writer = CHBinaryWriter(buffer: buffer)
    try type.encodeColumn(values: [input1, input2], writer: &writer)
    buffer = writer.buffer

    var reader = CHBinaryReader(buffer: buffer)
    let decoded = try type.decodeColumn(rows: 2, reader: &reader)
    let out1 = decoded.first as? Decimal
    let out2 = decoded.dropFirst().first as? Decimal
    #expect(NSDecimalNumber(decimal: out1 ?? 0).stringValue == NSDecimalNumber(decimal: input1).stringValue)
    #expect(NSDecimalNumber(decimal: out2 ?? 0).stringValue == NSDecimalNumber(decimal: input2).stringValue)
    #expect(reader.index == buffer.writerIndex)
}

@Test func lowCardinalityRoundtrip() async throws {
    let type = CHLowCardinalityType(nested: CHStringType())
    let input: [Any?] = ["a", "b", "a"]

    let allocator = ByteBufferAllocator()
    var buffer = allocator.buffer(capacity: 0)
    var writer = CHBinaryWriter(buffer: buffer)
    try type.encodeColumn(values: input, writer: &writer)
    buffer = writer.buffer

    var reader = CHBinaryReader(buffer: buffer)
    let decoded = try type.decodeColumn(rows: input.count, reader: &reader)
    let output = decoded.compactMap { $0 as? String }
    #expect(output == ["a", "b", "a"])
}

@Test func rowDecoderSnakeCaseMapping() async throws {
    struct User: Decodable {
        let userId: Int
    }

    let row = CHRow(columns: ["user_id"], values: [1])
    let user = try row.decode(User.self)
    #expect(user.userId == 1)
}

@Test func rowDecoderUnkeyedArray() async throws {
    struct Wrapper: Decodable {
        let items: [Int]
    }

    let row = CHRow(columns: ["items"], values: [[1, 2, 3]])
    let decoded = try row.decode(Wrapper.self)
    #expect(decoded.items == [1, 2, 3])
}

@Test func mapDecodesToDictionary() async throws {
    struct Wrapper: Decodable {
        let meta: [String: String]
    }

    let mapValue: [AnyHashable: Any?] = ["a": "1", "b": "2"]
    let row = CHRow(columns: ["meta"], values: [mapValue])
    let decoded = try row.decode(Wrapper.self)
    #expect(decoded.meta["a"] == "1")
}

@Test func queryRequestEncodesStage() async throws {
    let clientContext = CHClientContext(
        initialAddress: "0.0.0.0:0",
        clientHostname: "unit-test-host",
        clientName: "unit-test-client"
    )
    let request = CHQueryRequest(
        queryId: "stage-test",
        clientContext: clientContext,
        stage: CHQueryStage.fetchColumns.rawValue,
        compression: false,
        query: "SELECT 1",
        settings: [:]
    )

    let allocator = ByteBufferAllocator()
    var buffer = allocator.buffer(capacity: 0)
    var writer = CHBinaryWriter(buffer: buffer)
    try request.write(to: &writer)
    buffer = writer.buffer

    var reader = CHBinaryReader(buffer: buffer)
    let queryId = try reader.readUTF8String()
    #expect(queryId == "stage-test")

    _ = try reader.readVarInt() // initial query flag
    _ = try reader.readUTF8String()
    _ = try reader.readUTF8String()
    _ = try reader.readUTF8String() // initial address
    _ = try reader.readVarInt() // tcp kind
    _ = try reader.readUTF8String()
    _ = try reader.readUTF8String() // hostname
    _ = try reader.readUTF8String() // client name
    _ = try reader.readVarInt() // major
    _ = try reader.readVarInt() // minor
    _ = try reader.readVarInt() // revision
    _ = try reader.readUTF8String()

    let settingsTerminator = try reader.readUTF8String()
    #expect(settingsTerminator.isEmpty)

    let stage = try reader.readVarInt()
    #expect(stage == CHQueryStage.fetchColumns.rawValue)
}
