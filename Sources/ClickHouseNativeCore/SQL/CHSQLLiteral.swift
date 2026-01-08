import Foundation

public enum CHSQLLiteralError: Error {
    case malformed(String)
}

public struct CHSQLLexer {
    private var chars: [Character]
    private var index: Int

    public init(_ input: String) {
        self.chars = Array(input)
        self.index = 0
    }

    public mutating func skipWhitespace() {
        while index < chars.count, chars[index].isWhitespace {
            index += 1
        }
    }

    public mutating func readStringLiteral() throws -> String {
        skipWhitespace()
        guard index < chars.count, chars[index] == "'" else {
            throw CHSQLLiteralError.malformed("Expected string literal")
        }
        index += 1
        var out = ""
        while index < chars.count {
            let c = chars[index]
            if c == "'" {
                if index + 1 < chars.count, chars[index + 1] == "'" {
                    out.append("'")
                    index += 2
                    continue
                }
                index += 1
                return out
            }
            out.append(c)
            index += 1
        }
        throw CHSQLLiteralError.malformed("Unterminated string literal")
    }

    public mutating func readNumberToken() throws -> String {
        skipWhitespace()
        guard index < chars.count else {
            throw CHSQLLiteralError.malformed("Expected number")
        }
        let start = index
        if chars[index] == "-" || chars[index] == "+" {
            index += 1
        }
        var sawDigit = false
        while index < chars.count, chars[index].isNumber {
            sawDigit = true
            index += 1
        }
        if index < chars.count, chars[index] == "." {
            index += 1
            while index < chars.count, chars[index].isNumber {
                sawDigit = true
                index += 1
            }
        }
        guard sawDigit else {
            throw CHSQLLiteralError.malformed("Expected number")
        }
        return String(chars[start..<index])
    }
}

public enum CHSQLLiteralParser {
    public static func parseString(_ input: String) throws -> String {
        var lexer = CHSQLLexer(input)
        return try lexer.readStringLiteral()
    }

    public static func parseBool(_ input: String) throws -> Bool {
        let trimmed = input.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed == "1" || trimmed.lowercased() == "true" { return true }
        if trimmed == "0" || trimmed.lowercased() == "false" { return false }
        throw CHSQLLiteralError.malformed("Invalid boolean literal: \(input)")
    }

    public static func parseInt64(_ input: String) throws -> Int64 {
        var lexer = CHSQLLexer(input)
        let token = try lexer.readNumberToken()
        guard let value = Int64(token) else {
            throw CHSQLLiteralError.malformed("Invalid Int64 literal: \(input)")
        }
        return value
    }

    public static func parseUInt64(_ input: String) throws -> UInt64 {
        var lexer = CHSQLLexer(input)
        let token = try lexer.readNumberToken()
        guard let value = UInt64(token) else {
            throw CHSQLLiteralError.malformed("Invalid UInt64 literal: \(input)")
        }
        return value
    }

    public static func parseUUID(_ input: String) throws -> UUID {
        let raw = try parseString(input)
        guard let uuid = UUID(uuidString: raw) else {
            throw CHSQLLiteralError.malformed("Invalid UUID literal: \(input)")
        }
        return uuid
    }

    public static func parseDate(_ input: String, timeZone: TimeZone = .current) throws -> Date {
        let raw = try parseString(input)
        let parts = raw.split(separator: "-")
        guard parts.count == 3,
              let year = Int(parts[0]),
              let month = Int(parts[1]),
              let day = Int(parts[2]) else {
            throw CHSQLLiteralError.malformed("Invalid Date literal: \(input)")
        }
        var comps = DateComponents()
        comps.calendar = Calendar(identifier: .gregorian)
        comps.timeZone = timeZone
        comps.year = year
        comps.month = month
        comps.day = day
        guard let date = comps.date else {
            throw CHSQLLiteralError.malformed("Invalid Date components: \(raw)")
        }
        return date
    }

    public static func parseDateTime(_ input: String, timeZone: TimeZone = .current) throws -> Date {
        let raw = try parseString(input)
        let parts = raw.split(separator: " ")
        guard parts.count == 2 else {
            throw CHSQLLiteralError.malformed("Invalid DateTime literal: \(input)")
        }
        let datePart = parts[0].split(separator: "-")
        let timePart = parts[1].split(separator: ":")
        guard datePart.count == 3, timePart.count == 3,
              let year = Int(datePart[0]),
              let month = Int(datePart[1]),
              let day = Int(datePart[2]) else {
            throw CHSQLLiteralError.malformed("Invalid DateTime literal: \(input)")
        }

        let secondsParts = timePart[2].split(separator: ".")
        guard let hour = Int(timePart[0]),
              let minute = Int(timePart[1]),
              let second = Int(secondsParts[0]) else {
            throw CHSQLLiteralError.malformed("Invalid DateTime literal: \(input)")
        }
        var nanos = 0
        if secondsParts.count > 1 {
            let frac = String(secondsParts[1])
            let padded = frac.padding(toLength: 9, withPad: "0", startingAt: 0)
            nanos = Int(padded.prefix(9)) ?? 0
        }

        var comps = DateComponents()
        comps.calendar = Calendar(identifier: .gregorian)
        comps.timeZone = timeZone
        comps.year = year
        comps.month = month
        comps.day = day
        comps.hour = hour
        comps.minute = minute
        comps.second = second
        comps.nanosecond = nanos
        guard let date = comps.date else {
            throw CHSQLLiteralError.malformed("Invalid DateTime components: \(raw)")
        }
        return date
    }
}
