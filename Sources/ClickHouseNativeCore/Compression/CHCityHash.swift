public enum CHCityHash {
    private static let k0: UInt64 = 0xc3a5c85c97cb3127
    private static let k1: UInt64 = 0xb492b66fbe98f273
    private static let k2: UInt64 = 0x9ae16a3b2f90404f
    private static let k3: UInt64 = 0xc949d7c7509e6557
    private static let kMul: UInt64 = 0x9ddfea08eb382d69

    public static func cityHash128(bytes: [UInt8], offset: Int, length: Int) -> (UInt64, UInt64) {
        if length >= 16 {
            let seed0 = fetch64(bytes, offset) ^ k3
            let seed1 = fetch64(bytes, offset + 8)
            return cityHash128WithSeed(bytes: bytes, offset: offset + 16, length: length - 16, seed0: seed0, seed1: seed1)
        }
        if length >= 8 {
            let seed0 = fetch64(bytes, offset) ^ (UInt64(length) &* k0)
            let seed1 = fetch64(bytes, offset + length - 8) ^ k1
            return cityHash128WithSeed(bytes: [], offset: 0, length: 0, seed0: seed0, seed1: seed1)
        }
        return cityHash128WithSeed(bytes: bytes, offset: offset, length: length, seed0: k0, seed1: k1)
    }

    private static func cityHash128WithSeed(bytes: [UInt8], offset: Int, length: Int, seed0: UInt64, seed1: UInt64) -> (UInt64, UInt64) {
        if length < 128 {
            return cityMurmur(bytes: bytes, offset: offset, length: length, seed0: seed0, seed1: seed1)
        }

        var x = seed0
        var y = seed1
        var z = k1 &* UInt64(length)
        var v0 = (rotate(y ^ k1, 49) &* k1) &+ fetch64(bytes, offset)
        var v1 = (rotate(v0, 42) &* k1) &+ fetch64(bytes, offset + 8)
        var w0 = (rotate(y &+ z, 35) &* k1) &+ x
        var w1 = rotate(x &+ fetch64(bytes, offset + 88), 53) &* k1

        var pos = offset
        var len = length
        repeat {
            x = rotate(x &+ y &+ v0 &+ fetch64(bytes, pos + 16), 37) &* k1
            y = rotate(y &+ v1 &+ fetch64(bytes, pos + 48), 42) &* k1
            x ^= w1
            y ^= v0
            z = rotate(z ^ w0, 33)
            let v = weakHashLen32WithSeeds(bytes: bytes, offset: pos, a: v1 &* k1, b: x &+ w0)
            v0 = v.0
            v1 = v.1
            let w = weakHashLen32WithSeeds(bytes: bytes, offset: pos + 32, a: z &+ w1, b: y)
            w0 = w.0
            w1 = w.1
            swap(&z, &x)
            pos += 64

            x = rotate(x &+ y &+ v0 &+ fetch64(bytes, pos + 16), 37) &* k1
            y = rotate(y &+ v1 &+ fetch64(bytes, pos + 48), 42) &* k1
            x ^= w1
            y ^= v0
            z = rotate(z ^ w0, 33)
            let v2 = weakHashLen32WithSeeds(bytes: bytes, offset: pos, a: v1 &* k1, b: x &+ w0)
            v0 = v2.0
            v1 = v2.1
            let w2 = weakHashLen32WithSeeds(bytes: bytes, offset: pos + 32, a: z &+ w1, b: y)
            w0 = w2.0
            w1 = w2.1
            swap(&z, &x)
            pos += 64
            len -= 128
        } while len >= 128

        y = y &+ (rotate(w0, 37) &* k0) &+ z
        x = x &+ (rotate(v0 &+ z, 49) &* k0)

        var tailDone = 0
        while tailDone < len {
            tailDone += 32
            y = (rotate(y &- x, 42) &* k0) &+ v1
            w0 = w0 &+ fetch64(bytes, pos + len - tailDone + 16)
            x = (rotate(x, 49) &* k0) &+ w0
            w0 = w0 &+ v0
            let v = weakHashLen32WithSeeds(bytes: bytes, offset: pos + len - tailDone, a: v0, b: v1)
            v0 = v.0
            v1 = v.1
        }

        x = hashLen16(x, v0)
        y = hashLen16(y, w0)

        let first = hashLen16(x &+ v1, w1) &+ y
        let second = hashLen16(x &+ w1, y &+ v1)
        return (first, second)
    }

    private static func cityMurmur(bytes: [UInt8], offset: Int, length: Int, seed0: UInt64, seed1: UInt64) -> (UInt64, UInt64) {
        var a = seed0
        var b = seed1
        var c: UInt64 = 0
        var d: UInt64 = 0

        let l = length - 16
        if l <= 0 {
            a = shiftMix(a &* k1) &* k1
            c = (b &* k1) &+ hashLen0to16(bytes, offset, length)
            d = shiftMix(a &+ (length >= 8 ? fetch64(bytes, offset) : c))
        } else {
            c = hashLen16(fetch64(bytes, offset + length - 8) &+ k1, a)
            d = hashLen16(b &+ UInt64(length), c &+ fetch64(bytes, offset + length - 16))
            a = a &+ d
            var pos = offset
            var remaining = l
            while remaining > 0 {
                a ^= shiftMix(fetch64(bytes, pos) &* k1) &* k1
                a = a &* k1
                b ^= a
                c ^= shiftMix(fetch64(bytes, pos + 8) &* k1) &* k1
                c = c &* k1
                d ^= c
                pos += 16
                remaining -= 16
            }
        }
        a = hashLen16(a, c)
        b = hashLen16(d, b)
        return (a ^ b, hashLen16(b, a))
    }

    private static func weakHashLen32WithSeeds(bytes: [UInt8], offset: Int, a: UInt64, b: UInt64) -> (UInt64, UInt64) {
        let w = fetch64(bytes, offset)
        let x = fetch64(bytes, offset + 8)
        let y = fetch64(bytes, offset + 16)
        let z = fetch64(bytes, offset + 24)
        return weakHashLen32WithSeeds(w: w, x: x, y: y, z: z, a: a, b: b)
    }

    private static func weakHashLen32WithSeeds(w: UInt64, x: UInt64, y: UInt64, z: UInt64, a: UInt64, b: UInt64) -> (UInt64, UInt64) {
        var aVar = a &+ w
        var bVar = b &+ aVar &+ z
        bVar = rotate(bVar, 21)
        let c = aVar
        aVar = aVar &+ x
        aVar = aVar &+ y
        bVar = bVar &+ rotate(aVar, 44)
        return (aVar &+ z, bVar &+ c)
    }

    private static func hashLen0to16(_ bytes: [UInt8], _ offset: Int, _ len: Int) -> UInt64 {
        if len > 8 {
            let a = fetch64(bytes, offset)
            let b = fetch64(bytes, offset + len - 8)
            return hashLen16(a, rotateByAtLeast1(b &+ UInt64(len), len)) ^ b
        }
        if len >= 4 {
            let a = fetch32(bytes, offset)
            return hashLen16((UInt64(a << 3) &+ UInt64(len)), fetch32(bytes, offset + len - 4))
        }
        if len > 0 {
            let a = bytes[offset]
            let b = bytes[offset + (len >> 1)]
            let c = bytes[offset + len - 1]
            let y = UInt64(Int(a) + (Int(b) << 8))
            let z = UInt64(len + (Int(c) << 2))
            return shiftMix((y &* k2) ^ (z &* k3)) &* k2
        }
        return k2
    }

    private static func hashLen16(_ u: UInt64, _ v: UInt64) -> UInt64 {
        return hash128to64(u, v)
    }

    private static func hash128to64(_ u: UInt64, _ v: UInt64) -> UInt64 {
        var a = (u ^ v) &* kMul
        a ^= a >> 47
        var b = (v ^ a) &* kMul
        b ^= b >> 47
        b = b &* kMul
        return b
    }

    private static func shiftMix(_ val: UInt64) -> UInt64 {
        return val ^ (val >> 47)
    }

    private static func rotate(_ val: UInt64, _ shift: Int) -> UInt64 {
        return shift == 0 ? val : (val >> UInt64(shift)) | (val << UInt64(64 - shift))
    }

    private static func rotateByAtLeast1(_ val: UInt64, _ shift: Int) -> UInt64 {
        return (val >> UInt64(shift)) | (val << UInt64(64 - shift))
    }

    private static func fetch32(_ bytes: [UInt8], _ pos: Int) -> UInt64 {
        return UInt64(toIntLE(bytes, pos))
    }

    private static func fetch64(_ bytes: [UInt8], _ pos: Int) -> UInt64 {
        return toLongLE(bytes, pos)
    }

    private static func toIntLE(_ bytes: [UInt8], _ offset: Int) -> UInt32 {
        let b0 = UInt32(bytes[offset])
        let b1 = UInt32(bytes[offset + 1]) << 8
        let b2 = UInt32(bytes[offset + 2]) << 16
        let b3 = UInt32(bytes[offset + 3]) << 24
        return b0 | b1 | b2 | b3
    }

    private static func toLongLE(_ bytes: [UInt8], _ offset: Int) -> UInt64 {
        var result: UInt64 = 0
        for i in 0..<8 {
            result |= UInt64(bytes[offset + i]) << UInt64(8 * i)
        }
        return result
    }
}
