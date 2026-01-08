import Foundation
import ClickHouseNativeCore

public struct CHRow: @unchecked Sendable {
    public let columns: [String]
    public let values: [Any?]

    public subscript(_ index: Int) -> Any? {
        values[index]
    }

    public subscript(_ name: String) -> Any? {
        guard let idx = columnIndex(for: name) else { return nil }
        return values[idx]
    }

    public func decode<T: Decodable>(_ type: T.Type) throws -> T {
        try T(from: CHRowDecoder(row: self))
    }

    fileprivate func columnIndex(for key: String) -> Int? {
        if let idx = columns.firstIndex(of: key) { return idx }
        if let idx = columns.firstIndex(where: { $0.caseInsensitiveCompare(key) == .orderedSame }) {
            return idx
        }
        let snake = CHRow.toSnakeCase(key)
        if let idx = columns.firstIndex(of: snake) { return idx }
        let camel = CHRow.toCamelCase(key)
        if let idx = columns.firstIndex(of: camel) { return idx }
        return nil
    }

    private static func toSnakeCase(_ input: String) -> String {
        var result = ""
        for ch in input {
            if ch.isUppercase {
                if !result.isEmpty { result.append("_") }
                result.append(ch.lowercased())
            } else {
                result.append(ch)
            }
        }
        return result
    }

    private static func toCamelCase(_ input: String) -> String {
        let parts = input.split(separator: "_")
        guard let first = parts.first else { return input }
        let head = first.lowercased()
        let tail = parts.dropFirst().map { $0.prefix(1).uppercased() + $0.dropFirst().lowercased() }
        return ([head] + tail).joined()
    }
}

private struct CHRowDecoder: Decoder {
    let row: CHRow

    var codingPath: [CodingKey] = []
    var userInfo: [CodingUserInfoKey: Any] = [:]

    func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> {
        let container = CHRowKeyedContainer<Key>(row: row, codingPath: codingPath)
        return KeyedDecodingContainer(container)
    }

    func unkeyedContainer() throws -> UnkeyedDecodingContainer {
        if let first = row.values.first, let array = first as? [Any?] {
            return CHRowUnkeyedContainer(values: array, codingPath: codingPath)
        }
        throw DecodingError.typeMismatch(
            [Any].self,
            DecodingError.Context(codingPath: codingPath, debugDescription: "Unkeyed decoding not supported")
        )
    }

    func singleValueContainer() throws -> SingleValueDecodingContainer {
        return CHRowSingleValueContainer(row: row, codingPath: codingPath)
    }
}

private struct CHRowKeyedContainer<Key: CodingKey>: KeyedDecodingContainerProtocol {
    let row: CHRow
    var codingPath: [CodingKey]

    var allKeys: [Key] {
        row.columns.compactMap { Key(stringValue: $0) }
    }

    func contains(_ key: Key) -> Bool {
        row.columnIndex(for: key.stringValue) != nil
    }

    func decodeNil(forKey key: Key) throws -> Bool {
        return value(forKey: key) == nil
    }

    func decode(_ type: String.Type, forKey key: Key) throws -> String { try convert(value(forKey: key), to: String.self) }
    func decode(_ type: Bool.Type, forKey key: Key) throws -> Bool { try convert(value(forKey: key), to: Bool.self) }
    func decode(_ type: Int.Type, forKey key: Key) throws -> Int { try convert(value(forKey: key), to: Int.self) }
    func decode(_ type: Int64.Type, forKey key: Key) throws -> Int64 { try convert(value(forKey: key), to: Int64.self) }
    func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 { try convert(value(forKey: key), to: UInt64.self) }
    func decode(_ type: Double.Type, forKey key: Key) throws -> Double { try convert(value(forKey: key), to: Double.self) }
    func decode(_ type: Float.Type, forKey key: Key) throws -> Float { try convert(value(forKey: key), to: Float.self) }
    func decode(_ type: Decimal.Type, forKey key: Key) throws -> Decimal { try convert(value(forKey: key), to: Decimal.self) }
    func decode(_ type: Date.Type, forKey key: Key) throws -> Date { try convert(value(forKey: key), to: Date.self) }
    func decode(_ type: UUID.Type, forKey key: Key) throws -> UUID { try convert(value(forKey: key), to: UUID.self) }
    func decode(_ type: Data.Type, forKey key: Key) throws -> Data { try convert(value(forKey: key), to: Data.self) }

    func decode<T>(_ type: T.Type, forKey key: Key) throws -> T where T : Decodable {
        let value = value(forKey: key)
        let decoder = CHRowSingleValueDecoder(value: value, codingPath: codingPath + [key])
        return try T(from: decoder)
    }

    func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: Key) throws -> KeyedDecodingContainer<NestedKey> {
        let value = value(forKey: key)
        if let dict = Self.dictionary(from: value) {
            let container = CHDictionaryKeyedContainer<NestedKey>(dict: dict, codingPath: codingPath + [key])
            return KeyedDecodingContainer(container)
        }
        throw DecodingError.typeMismatch(
            [String: Any].self,
            DecodingError.Context(codingPath: codingPath, debugDescription: "Nested container not supported")
        )
    }

    func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
        let value = value(forKey: key)
        if let array = value as? [Any?] {
            return CHRowUnkeyedContainer(values: array, codingPath: codingPath + [key])
        }
        throw DecodingError.typeMismatch(
            [Any].self,
            DecodingError.Context(codingPath: codingPath, debugDescription: "Nested unkeyed container not supported")
        )
    }

    func superDecoder() throws -> Decoder {
        CHRowDecoder(row: row)
    }

    func superDecoder(forKey key: Key) throws -> Decoder {
        CHRowSingleValueDecoder(value: value(forKey: key), codingPath: codingPath + [key])
    }

    private func value(forKey key: Key) -> Any? {
        guard let idx = row.columnIndex(for: key.stringValue) else { return nil }
        return row.values[idx]
    }

    fileprivate static func dictionary(from value: Any?) -> [String: Any?]? {
        if let dict = value as? [String: Any?] { return dict }
        if let dict = value as? [AnyHashable: Any?] {
            var out: [String: Any?] = [:]
            for (k, v) in dict {
                if let key = k as? String {
                    out[key] = v
                }
            }
            return out.isEmpty ? nil : out
        }
        if let pairs = value as? [(Any?, Any?)] {
            var dict: [String: Any?] = [:]
            for (k, v) in pairs {
                if let key = k as? String { dict[key] = v }
            }
            return dict.isEmpty ? nil : dict
        }
        return nil
    }
}

private struct CHRowSingleValueContainer: SingleValueDecodingContainer {
    let row: CHRow
    var codingPath: [CodingKey]

    private var firstValue: Any? { row.values.first ?? nil }

    func decodeNil() -> Bool { firstValue == nil }
    func decode(_ type: String.Type) throws -> String { try convert(firstValue, to: String.self) }
    func decode(_ type: Bool.Type) throws -> Bool { try convert(firstValue, to: Bool.self) }
    func decode(_ type: Int.Type) throws -> Int { try convert(firstValue, to: Int.self) }
    func decode(_ type: Int64.Type) throws -> Int64 { try convert(firstValue, to: Int64.self) }
    func decode(_ type: UInt64.Type) throws -> UInt64 { try convert(firstValue, to: UInt64.self) }
    func decode(_ type: Double.Type) throws -> Double { try convert(firstValue, to: Double.self) }
    func decode(_ type: Float.Type) throws -> Float { try convert(firstValue, to: Float.self) }
    func decode(_ type: Decimal.Type) throws -> Decimal { try convert(firstValue, to: Decimal.self) }
    func decode(_ type: Date.Type) throws -> Date { try convert(firstValue, to: Date.self) }
    func decode(_ type: UUID.Type) throws -> UUID { try convert(firstValue, to: UUID.self) }
    func decode(_ type: Data.Type) throws -> Data { try convert(firstValue, to: Data.self) }

    func decode<T>(_ type: T.Type) throws -> T where T : Decodable {
        let decoder = CHRowSingleValueDecoder(value: firstValue, codingPath: codingPath)
        return try T(from: decoder)
    }
}

private struct CHRowSingleValueDecoder: Decoder {
    let value: Any?
    var codingPath: [CodingKey]
    var userInfo: [CodingUserInfoKey : Any] = [:]

    func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> {
        if let dict = CHRowKeyedContainer<Key>.dictionary(from: value) {
            let container = CHDictionaryKeyedContainer<Key>(dict: dict, codingPath: codingPath)
            return KeyedDecodingContainer(container)
        }
        throw DecodingError.typeMismatch(
            [String: Any].self,
            DecodingError.Context(codingPath: codingPath, debugDescription: "Keyed container not supported")
        )
    }

    func unkeyedContainer() throws -> UnkeyedDecodingContainer {
        if let array = value as? [Any?] {
            return CHRowUnkeyedContainer(values: array, codingPath: codingPath)
        }
        throw DecodingError.typeMismatch(
            [Any].self,
            DecodingError.Context(codingPath: codingPath, debugDescription: "Unkeyed container not supported")
        )
    }

    func singleValueContainer() throws -> SingleValueDecodingContainer {
        CHSingleValueContainer(value: value, codingPath: codingPath)
    }
}

private struct CHSingleValueContainer: SingleValueDecodingContainer {
    let value: Any?
    var codingPath: [CodingKey]

    func decodeNil() -> Bool { value == nil }
    func decode(_ type: String.Type) throws -> String { try convert(value, to: String.self) }
    func decode(_ type: Bool.Type) throws -> Bool { try convert(value, to: Bool.self) }
    func decode(_ type: Int.Type) throws -> Int { try convert(value, to: Int.self) }
    func decode(_ type: Int64.Type) throws -> Int64 { try convert(value, to: Int64.self) }
    func decode(_ type: UInt64.Type) throws -> UInt64 { try convert(value, to: UInt64.self) }
    func decode(_ type: Double.Type) throws -> Double { try convert(value, to: Double.self) }
    func decode(_ type: Float.Type) throws -> Float { try convert(value, to: Float.self) }
    func decode(_ type: Decimal.Type) throws -> Decimal { try convert(value, to: Decimal.self) }
    func decode(_ type: Date.Type) throws -> Date { try convert(value, to: Date.self) }
    func decode(_ type: UUID.Type) throws -> UUID { try convert(value, to: UUID.self) }
    func decode(_ type: Data.Type) throws -> Data { try convert(value, to: Data.self) }

    func decode<T>(_ type: T.Type) throws -> T where T : Decodable {
        throw DecodingError.typeMismatch(
            type,
            DecodingError.Context(codingPath: codingPath, debugDescription: "Unsupported type")
        )
    }
}

private struct CHRowUnkeyedContainer: UnkeyedDecodingContainer {
    let values: [Any?]
    var codingPath: [CodingKey]
    var currentIndex: Int = 0

    var count: Int? { values.count }
    var isAtEnd: Bool { currentIndex >= values.count }

    mutating func decodeNil() throws -> Bool {
        let value = nextValue()
        return value == nil
    }

    mutating func decode(_ type: String.Type) throws -> String { try decodeValue(type) }
    mutating func decode(_ type: Bool.Type) throws -> Bool { try decodeValue(type) }
    mutating func decode(_ type: Int.Type) throws -> Int { try decodeValue(type) }
    mutating func decode(_ type: Int64.Type) throws -> Int64 { try decodeValue(type) }
    mutating func decode(_ type: UInt64.Type) throws -> UInt64 { try decodeValue(type) }
    mutating func decode(_ type: Double.Type) throws -> Double { try decodeValue(type) }
    mutating func decode(_ type: Float.Type) throws -> Float { try decodeValue(type) }
    mutating func decode(_ type: Decimal.Type) throws -> Decimal { try decodeValue(type) }
    mutating func decode(_ type: Date.Type) throws -> Date { try decodeValue(type) }
    mutating func decode(_ type: UUID.Type) throws -> UUID { try decodeValue(type) }
    mutating func decode(_ type: Data.Type) throws -> Data { try decodeValue(type) }

    mutating func decode<T>(_ type: T.Type) throws -> T where T : Decodable {
        let value = nextValue()
        let decoder = CHRowSingleValueDecoder(value: value, codingPath: codingPath)
        return try T(from: decoder)
    }

    mutating func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type) throws -> KeyedDecodingContainer<NestedKey> {
        let value = nextValue()
        if let dict = CHRowKeyedContainer<NestedKey>.dictionary(from: value) {
            let container = CHDictionaryKeyedContainer<NestedKey>(dict: dict, codingPath: codingPath)
            return KeyedDecodingContainer(container)
        }
        throw DecodingError.typeMismatch(
            [String: Any].self,
            DecodingError.Context(codingPath: codingPath, debugDescription: "Nested keyed container not supported")
        )
    }

    mutating func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
        let value = nextValue()
        if let array = value as? [Any?] {
            return CHRowUnkeyedContainer(values: array, codingPath: codingPath)
        }
        throw DecodingError.typeMismatch(
            [Any].self,
            DecodingError.Context(codingPath: codingPath, debugDescription: "Nested unkeyed container not supported")
        )
    }

    mutating func superDecoder() throws -> Decoder {
        let value = nextValue()
        return CHRowSingleValueDecoder(value: value, codingPath: codingPath)
    }

    private mutating func decodeValue<T>(_ type: T.Type) throws -> T {
        let value = nextValue()
        return try convert(value, to: type)
    }

    private mutating func nextValue() -> Any? {
        guard !isAtEnd else { return nil }
        let value = values[currentIndex]
        currentIndex += 1
        return value
    }
}

private struct CHDictionaryKeyedContainer<Key: CodingKey>: KeyedDecodingContainerProtocol {
    let dict: [String: Any?]
    var codingPath: [CodingKey]

    var allKeys: [Key] {
        dict.keys.compactMap { Key(stringValue: $0) }
    }

    func contains(_ key: Key) -> Bool {
        dict[key.stringValue] != nil
    }

    func decodeNil(forKey key: Key) throws -> Bool {
        return dict[key.stringValue] == nil
    }

    func decode(_ type: String.Type, forKey key: Key) throws -> String { try convert(dict[key.stringValue] ?? nil, to: String.self) }
    func decode(_ type: Bool.Type, forKey key: Key) throws -> Bool { try convert(dict[key.stringValue] ?? nil, to: Bool.self) }
    func decode(_ type: Int.Type, forKey key: Key) throws -> Int { try convert(dict[key.stringValue] ?? nil, to: Int.self) }
    func decode(_ type: Int64.Type, forKey key: Key) throws -> Int64 { try convert(dict[key.stringValue] ?? nil, to: Int64.self) }
    func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 { try convert(dict[key.stringValue] ?? nil, to: UInt64.self) }
    func decode(_ type: Double.Type, forKey key: Key) throws -> Double { try convert(dict[key.stringValue] ?? nil, to: Double.self) }
    func decode(_ type: Float.Type, forKey key: Key) throws -> Float { try convert(dict[key.stringValue] ?? nil, to: Float.self) }
    func decode(_ type: Decimal.Type, forKey key: Key) throws -> Decimal { try convert(dict[key.stringValue] ?? nil, to: Decimal.self) }
    func decode(_ type: Date.Type, forKey key: Key) throws -> Date { try convert(dict[key.stringValue] ?? nil, to: Date.self) }
    func decode(_ type: UUID.Type, forKey key: Key) throws -> UUID { try convert(dict[key.stringValue] ?? nil, to: UUID.self) }
    func decode(_ type: Data.Type, forKey key: Key) throws -> Data { try convert(dict[key.stringValue] ?? nil, to: Data.self) }

    func decode<T>(_ type: T.Type, forKey key: Key) throws -> T where T : Decodable {
        let decoder = CHRowSingleValueDecoder(value: dict[key.stringValue] ?? nil, codingPath: codingPath + [key])
        return try T(from: decoder)
    }

    func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: Key) throws -> KeyedDecodingContainer<NestedKey> {
        throw DecodingError.typeMismatch([String: Any].self, DecodingError.Context(codingPath: codingPath, debugDescription: "Nested container not supported"))
    }

    func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
        throw DecodingError.typeMismatch([Any].self, DecodingError.Context(codingPath: codingPath, debugDescription: "Nested unkeyed container not supported"))
    }

    func superDecoder() throws -> Decoder { CHRowSingleValueDecoder(value: dict, codingPath: codingPath) }
    func superDecoder(forKey key: Key) throws -> Decoder { CHRowSingleValueDecoder(value: dict[key.stringValue] ?? nil, codingPath: codingPath + [key]) }
}

private func convert<T>(_ value: Any?, to type: T.Type) throws -> T {
    if let v = value as? T {
        return v
    }

    if type == String.self {
        if let v = value {
            return String(describing: v) as! T
        }
    }

    if type == Bool.self {
        if let v = value as? Bool { return v as! T }
        if let v = value as? Int { return (v != 0) as! T }
        if let v = value as? Int64 { return (v != 0) as! T }
        if let v = value as? UInt64 { return (v != 0) as! T }
        if let v = value as? String {
            return (v == "1" || v.lowercased() == "true") as! T
        }
    }

    if type == Int.self {
        if let v = value as? Int { return v as! T }
        if let v = value as? Int64 { return Int(v) as! T }
        if let v = value as? UInt64 { return Int(v) as! T }
        if let v = value as? Double { return Int(v) as! T }
        if let v = value as? String, let i = Int(v) { return i as! T }
    }

    if type == Int64.self {
        if let v = value as? Int64 { return v as! T }
        if let v = value as? Int { return Int64(v) as! T }
        if let v = value as? UInt64 { return Int64(v) as! T }
        if let v = value as? String, let i = Int64(v) { return i as! T }
    }

    if type == UInt64.self {
        if let v = value as? UInt64 { return v as! T }
        if let v = value as? Int64 { return UInt64(v) as! T }
        if let v = value as? Int { return UInt64(v) as! T }
        if let v = value as? String, let i = UInt64(v) { return i as! T }
    }

    if type == Double.self {
        if let v = value as? Double { return v as! T }
        if let v = value as? Float { return Double(v) as! T }
        if let v = value as? Int { return Double(v) as! T }
        if let v = value as? Int64 { return Double(v) as! T }
        if let v = value as? UInt64 { return Double(v) as! T }
        if let v = value as? String, let d = Double(v) { return d as! T }
    }

    if type == Float.self {
        if let v = value as? Float { return v as! T }
        if let v = value as? Double { return Float(v) as! T }
        if let v = value as? Int { return Float(v) as! T }
        if let v = value as? String, let f = Float(v) { return f as! T }
    }

    if type == Decimal.self {
        if let v = value as? Decimal { return v as! T }
        if let v = value as? String, let d = Decimal(string: v) { return d as! T }
        if let v = value as? Double { return Decimal(v) as! T }
        if let v = value as? Int { return Decimal(v) as! T }
    }

    if type == Date.self {
        if let v = value as? Date { return v as! T }
        if let v = value as? Double { return Date(timeIntervalSince1970: v) as! T }
        if let v = value as? Int { return Date(timeIntervalSince1970: Double(v)) as! T }
        if let v = value as? Int64 { return Date(timeIntervalSince1970: Double(v)) as! T }
    }

    if type == UUID.self {
        if let v = value as? UUID { return v as! T }
        if let v = value as? String, let uuid = UUID(uuidString: v) { return uuid as! T }
    }

    if type == Data.self {
        if let v = value as? Data { return v as! T }
        if let v = value as? [UInt8] { return Data(v) as! T }
        if let v = value as? String, let data = Data(base64Encoded: v) { return data as! T }
    }

    throw DecodingError.typeMismatch(
        type,
        DecodingError.Context(codingPath: [], debugDescription: "Unsupported conversion")
    )
}
