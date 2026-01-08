public enum CHBinaryError: Error {
    case needMoreData
    case malformed(String)
    case unsupported(String)
}
