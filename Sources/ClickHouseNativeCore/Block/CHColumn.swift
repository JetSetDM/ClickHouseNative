public struct CHColumn {
    public var name: String
    public var type: CHDataType
    public var values: [Any?]

    public init(name: String, type: CHDataType, values: [Any?]) {
        self.name = name
        self.type = type
        self.values = values
    }
}
