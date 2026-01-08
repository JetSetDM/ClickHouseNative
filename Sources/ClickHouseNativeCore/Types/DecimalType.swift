import Foundation
import BigInt

public struct CHDecimalType: CHDataType {
    public let name: String
    public let precision: Int
    public let scale: Int

    public init(precision: Int, scale: Int) {
        self.precision = precision
        self.scale = scale
        self.name = "Decimal(\(precision),\(scale))"
    }

    public func decodeColumn(rows: Int, reader: inout CHBinaryReader) throws -> [Any?] {
        let bits = bitWidth()
        if bits < 0 {
            throw CHBinaryError.unsupported("Decimal precision > 76 not supported")
        }
        var values: [Any?] = []
        values.reserveCapacity(rows)
        for _ in 0..<rows {
            let intValue = try readBigInt(bits: bits, reader: &reader)
            let value = bigIntToDecimal(intValue, scale: scale)
            values.append(value)
        }
        return values
    }

    public func encodeColumn(values: [Any?], writer: inout CHBinaryWriter) throws {
        let bits = bitWidth()
        if bits < 0 {
            throw CHBinaryError.unsupported("Decimal precision > 76 not supported")
        }
        for value in values {
            let bigInt = decimalToBigInt(value: value, scale: scale)
            try writeBigInt(bigInt, bits: bits, writer: &writer)
        }
    }

    private func pow10(_ n: Int) -> Decimal {
        var result = Decimal(1)
        if n <= 0 { return result }
        for _ in 0..<n {
            result *= 10
        }
        return result
    }

    private func bitWidth() -> Int {
        if precision <= 9 { return 32 }
        if precision <= 18 { return 64 }
        if precision <= 38 { return 128 }
        if precision <= 76 { return 256 }
        return -1
    }

    private func decimalToBigInt(value: Any?, scale: Int) -> BigInt {
        let decimal: Decimal
        if let d = value as? Decimal {
            decimal = d
        } else if let s = value as? String, let d = Decimal(string: s) {
            decimal = d
        } else if let n = value as? NSNumber {
            decimal = n.decimalValue
        } else {
            decimal = Decimal(0)
        }

        var factor = pow10(scale)
        var scaled = Decimal()
        var dec = decimal
        _ = NSDecimalMultiply(&scaled, &dec, &factor, .plain)
        var rounded = Decimal()
        NSDecimalRound(&rounded, &scaled, 0, .plain)
        let str = NSDecimalNumber(decimal: rounded).stringValue
        return BigInt(str) ?? BigInt(0)
    }

    private func bigIntToDecimal(_ value: BigInt, scale: Int) -> Decimal {
        if scale == 0 {
            return Decimal(string: value.description) ?? Decimal(0)
        }
        let sign = value.sign == .minus ? "-" : ""
        let digits = value.magnitude.description
        let padded: String
        if digits.count <= scale {
            padded = String(repeating: "0", count: scale - digits.count + 1) + digits
        } else {
            padded = digits
        }
        let idx = padded.index(padded.endIndex, offsetBy: -scale)
        let intPart = padded[..<idx]
        let fracPart = padded[idx...]
        let str = "\(sign)\(intPart).\(fracPart)"
        return Decimal(string: str) ?? Decimal(0)
    }

    private func readBigInt(bits: Int, reader: inout CHBinaryReader) throws -> BigInt {
        if bits == 32 {
            let word = try reader.readUInt32()
            let value = BigUInt(word)
            let signBit = BigUInt(1) << 31
            if value & signBit != 0 {
                let modulus = BigUInt(1) << 32
                return BigInt(value) - BigInt(modulus)
            }
            return BigInt(value)
        }
        let limbs = bits / 64
        var value = BigUInt(0)
        for i in 0..<limbs {
            let word = try reader.readUInt64()
            value |= BigUInt(word) << (64 * i)
        }
        let signBit = BigUInt(1) << (bits - 1)
        if value & signBit != 0 {
            let modulus = BigUInt(1) << bits
            return BigInt(value) - BigInt(modulus)
        } else {
            return BigInt(value)
        }
    }

    private func writeBigInt(_ value: BigInt, bits: Int, writer: inout CHBinaryWriter) throws {
        if bits == 32 {
            let modulus = BigUInt(1) << 32
            let unsigned: BigUInt
            if value.sign == .minus {
                unsigned = BigUInt(modulus) - BigUInt(value.magnitude)
            } else {
                unsigned = BigUInt(value.magnitude)
            }
            let masked = unsigned & BigUInt(0xffff_ffff)
            writer.writeUInt32(UInt32(masked))
            return
        }
        let modulus = BigUInt(1) << bits
        let unsigned: BigUInt
        if value.sign == .minus {
            unsigned = BigUInt(modulus) - BigUInt(value.magnitude)
        } else {
            unsigned = BigUInt(value.magnitude)
        }
        let limbs = bits / 64
        for i in 0..<limbs {
            let word = (unsigned >> (64 * i)) & 0xffff_ffff_ffff_ffff
            writer.writeUInt64(UInt64(word))
        }
    }
}
