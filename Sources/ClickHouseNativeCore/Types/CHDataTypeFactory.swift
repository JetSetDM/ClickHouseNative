import Foundation

public enum CHDataTypeFactory {
    public static func parse(_ typeName: String, serverContext: CHServerContext? = nil) throws -> CHDataType {
        var lexer = CHTypeLexer(input: typeName)
        let type = try parseType(&lexer, serverContext: serverContext)
        lexer.skipWhitespace()
        if !lexer.isAtEnd {
            throw CHBinaryError.malformed("Unexpected token in type: \(typeName)")
        }
        return type
    }

    private static func parseType(_ lexer: inout CHTypeLexer, serverContext: CHServerContext?) throws -> CHDataType {
        let ident = try lexer.readIdentifier()
        switch ident.lowercased() {
        case "int8":
            return CHInt8Type()
        case "int16":
            return CHInt16Type()
        case "int32":
            return CHInt32Type()
        case "int64":
            return CHInt64Type()
        case "uint8":
            return CHUInt8Type()
        case "uint16":
            return CHUInt16Type()
        case "uint32":
            return CHUInt32Type()
        case "uint64":
            return CHUInt64Type()
        case "float32":
            return CHFloat32Type()
        case "float64":
            return CHFloat64Type()
        case "string":
            return CHStringType()
        case "json":
            return CHJSONType()
        case "bool":
            return CHBoolType()
        case "uuid":
            return CHUUIDType()
        case "ipv4":
            return CHIPv4Type()
        case "ipv6":
            return CHIPv6Type()
        case "fixedstring":
            try lexer.consume("(")
            let len = try lexer.readInt()
            try lexer.consume(")")
            return CHFixedStringType(length: len)
        case "binary":
            try lexer.consume("(")
            let len = try lexer.readInt()
            try lexer.consume(")")
            return CHFixedStringType(length: len)
        case "array":
            try lexer.consume("(")
            let nested = try parseType(&lexer, serverContext: serverContext)
            try lexer.consume(")")
            return CHArrayType(nested: nested)
        case "tuple":
            try lexer.consume("(")
            var items: [CHDataType] = []
            while true {
                let item = try parseType(&lexer, serverContext: serverContext)
                items.append(item)
                lexer.skipWhitespace()
                if lexer.peekNonWhitespace() == ")" {
                    try lexer.consume(")")
                    break
                } else {
                    try lexer.consume(",")
                }
            }
            return CHTupleType(nested: items)
        case "map":
            try lexer.consume("(")
            let key = try parseType(&lexer, serverContext: serverContext)
            try lexer.consume(",")
            let value = try parseType(&lexer, serverContext: serverContext)
            try lexer.consume(")")
            return CHMapType(key: key, value: value)
        case "enum8":
            try lexer.consume("(")
            var names: [String] = []
            var values: [Int8] = []
            while true {
                let name = try lexer.readStringLiteral()
                try lexer.consume("=")
                let number = try lexer.readSignedInt()
                names.append(name)
                values.append(Int8(number))
                lexer.skipWhitespace()
                if lexer.peekNonWhitespace() == ")" {
                    try lexer.consume(")")
                    break
                } else {
                    try lexer.consume(",")
                }
            }
            return CHEnum8Type(names: names, values: values)
        case "enum16":
            try lexer.consume("(")
            var names: [String] = []
            var values: [Int16] = []
            while true {
                let name = try lexer.readStringLiteral()
                try lexer.consume("=")
                let number = try lexer.readSignedInt()
                names.append(name)
                values.append(Int16(number))
                lexer.skipWhitespace()
                if lexer.peekNonWhitespace() == ")" {
                    try lexer.consume(")")
                    break
                } else {
                    try lexer.consume(",")
                }
            }
            return CHEnum16Type(names: names, values: values)
        case "decimal":
            try lexer.consume("(")
            let precision = try lexer.readInt()
            try lexer.consume(",")
            let scale = try lexer.readInt()
            try lexer.consume(")")
            return CHDecimalType(precision: precision, scale: scale)
        case "decimal32":
            let scale = try parseSingleScale(&lexer)
            return CHDecimalType(precision: 9, scale: scale)
        case "decimal64":
            let scale = try parseSingleScale(&lexer)
            return CHDecimalType(precision: 18, scale: scale)
        case "decimal128":
            let scale = try parseSingleScale(&lexer)
            return CHDecimalType(precision: 38, scale: scale)
        case "decimal256":
            let scale = try parseSingleScale(&lexer)
            return CHDecimalType(precision: 76, scale: scale)
        case "date":
            return CHDateType()
        case "date32":
            return CHDate32Type()
        case "datetime":
            if lexer.peekNonWhitespace() == "(" {
                try lexer.consume("(")
                let tzName = try lexer.readStringLiteral()
                try lexer.consume(")")
                let tz = TimeZone(identifier: tzName) ?? serverContext?.timezone ?? TimeZone.current
                return CHDateTimeType(timezone: tz, name: "DateTime('\(tzName)')")
            } else {
                let tz = serverContext?.timezone ?? TimeZone.current
                return CHDateTimeType(timezone: tz, name: "DateTime")
            }
        case "datetime64":
            var scale = 3
            var timezoneName: String? = nil
            if lexer.peekNonWhitespace() == "(" {
                try lexer.consume("(")
                scale = try lexer.readInt()
                lexer.skipWhitespace()
                if lexer.peekNonWhitespace() == "," {
                    try lexer.consume(",")
                    lexer.skipWhitespace()
                    timezoneName = try lexer.readStringLiteral()
                }
                try lexer.consume(")")
            }
            let tz = timezoneName.flatMap { TimeZone(identifier: $0) } ?? serverContext?.timezone ?? TimeZone.current
            let name: String
            if let timezoneName {
                name = "DateTime64(\(scale), '\(timezoneName)')"
            } else {
                name = "DateTime64(\(scale))"
            }
            return CHDateTime64Type(scale: scale, timezone: tz, name: name)
        case "lowcardinality":
            try lexer.consume("(")
            let nested = try parseType(&lexer, serverContext: serverContext)
            try lexer.consume(")")
            return CHLowCardinalityType(nested: nested)
        case "nullable":
            try lexer.consume("(")
            let nested = try parseType(&lexer, serverContext: serverContext)
            try lexer.consume(")")
            return CHNullableType(nested: nested)
        case "nothing":
            return CHNothingType()
        default:
            throw CHBinaryError.unsupported("Type not implemented: \(ident)")
        }
    }

    private static func parseSingleScale(_ lexer: inout CHTypeLexer) throws -> Int {
        lexer.skipWhitespace()
        guard lexer.peekNonWhitespace() == "(" else { return 0 }
        try lexer.consume("(")
        let scale = try lexer.readInt()
        try lexer.consume(")")
        return scale
    }
}

public struct CHTypeLexer {
    private var chars: [Character]
    private var index: Int

    public init(input: String) {
        self.chars = Array(input)
        self.index = 0
    }

    public var isAtEnd: Bool {
        return index >= chars.count
    }

    public mutating func skipWhitespace() {
        while index < chars.count, chars[index].isWhitespace {
            index += 1
        }
    }

    public func peekNonWhitespace() -> Character? {
        var i = index
        while i < chars.count, chars[i].isWhitespace {
            i += 1
        }
        return i < chars.count ? chars[i] : nil
    }

    public mutating func readIdentifier() throws -> String {
        skipWhitespace()
        guard index < chars.count else {
            throw CHBinaryError.malformed("Unexpected end while reading identifier")
        }
        let start = index
        while index < chars.count {
            let c = chars[index]
            if c.isLetter || c.isNumber || c == "_" {
                index += 1
            } else {
                break
            }
        }
        let value = String(chars[start..<index])
        if value.isEmpty {
            throw CHBinaryError.malformed("Expected identifier")
        }
        return value
    }

    public mutating func readInt() throws -> Int {
        return Int(try readSignedInt())
    }

    public mutating func readSignedInt() throws -> Int64 {
        skipWhitespace()
        guard index < chars.count else {
            throw CHBinaryError.malformed("Unexpected end while reading integer")
        }
        var sign: Int64 = 1
        if chars[index] == "-" {
            sign = -1
            index += 1
        } else if chars[index] == "+" {
            index += 1
        }
        let start = index
        while index < chars.count, chars[index].isNumber {
            index += 1
        }
        let value = String(chars[start..<index])
        guard let intValue = Int64(value) else {
            throw CHBinaryError.malformed("Expected integer")
        }
        return intValue * sign
    }

    public mutating func readStringLiteral() throws -> String {
        skipWhitespace()
        guard index < chars.count, chars[index] == "'" else {
            throw CHBinaryError.malformed("Expected string literal")
        }
        index += 1
        let start = index
        while index < chars.count, chars[index] != "'" {
            index += 1
        }
        guard index < chars.count else {
            throw CHBinaryError.malformed("Unterminated string literal")
        }
        let value = String(chars[start..<index])
        index += 1
        return value
    }

    public mutating func consume(_ expected: Character) throws {
        skipWhitespace()
        guard index < chars.count, chars[index] == expected else {
            throw CHBinaryError.malformed("Expected '\(expected)'")
        }
        index += 1
    }

    public mutating func skipBalancedParens() throws {
        skipWhitespace()
        guard index < chars.count, chars[index] == "(" else {
            throw CHBinaryError.malformed("Expected '('")
        }
        var depth = 0
        while index < chars.count {
            let c = chars[index]
            if c == "(" { depth += 1 }
            if c == ")" { depth -= 1 }
            index += 1
            if depth == 0 { return }
        }
        throw CHBinaryError.malformed("Unbalanced parentheses")
    }
}
