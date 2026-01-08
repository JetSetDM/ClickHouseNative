import Foundation

public final class CHServerException: Error, @unchecked Sendable {
    public let code: Int32
    public let name: String
    public let message: String
    public let stackTrace: String
    public let nested: CHServerException?

    public init(code: Int32, name: String, message: String, stackTrace: String, nested: CHServerException?) {
        self.code = code
        self.name = name
        self.message = message
        self.stackTrace = stackTrace
        self.nested = nested
    }

    public static func read(from reader: inout CHBinaryReader) throws -> CHServerException {
        let code = try reader.readInt32()
        let name = try reader.readUTF8String()
        let message = try reader.readUTF8String()
        let stack = try reader.readUTF8String()
        let hasNested = try reader.readBool()
        let nested = hasNested ? try CHServerException.read(from: &reader) : nil
        return CHServerException(code: code, name: name, message: message, stackTrace: stack, nested: nested)
    }
}
