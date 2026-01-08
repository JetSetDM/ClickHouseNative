public struct CHBlock: @unchecked Sendable {
    public var rowCount: Int
    public var columns: [CHColumn]
    public var settings: CHBlockSettings

    public init(rowCount: Int, columns: [CHColumn], settings: CHBlockSettings = CHBlockSettings(settings: [:])) {
        self.rowCount = rowCount
        self.columns = columns
        self.settings = settings
    }

    public static func empty() -> CHBlock {
        CHBlock(rowCount: 0, columns: [])
    }

    public static func read(from reader: inout CHBinaryReader, serverContext: CHServerContext? = nil) throws -> CHBlock {
        let settings = try CHBlockSettings.read(from: &reader)
        let columnCount = Int(try reader.readVarInt())
        let rowCount = Int(try reader.readVarInt())

        var columns: [CHColumn] = []
        columns.reserveCapacity(columnCount)

        for _ in 0..<columnCount {
            let name = try reader.readUTF8String()
            let typeName = try reader.readUTF8String()
            let type = try CHDataTypeFactory.parse(typeName, serverContext: serverContext)
            let values = try type.decodeColumn(rows: rowCount, reader: &reader)
            columns.append(CHColumn(name: name, type: type, values: values))
        }

        return CHBlock(rowCount: rowCount, columns: columns, settings: settings)
    }

    public func write(to writer: inout CHBinaryWriter) throws {
        settings.write(to: &writer)
        writer.writeVarInt(UInt64(columns.count))
        writer.writeVarInt(UInt64(rowCount))
        for column in columns {
            writer.writeUTF8String(column.name)
            writer.writeUTF8String(column.type.name)
            try column.type.encodeColumn(values: column.values, writer: &writer)
        }
    }

    public func validate(against sample: CHBlock) throws {
        if columns.count != sample.columns.count {
            throw CHBinaryError.malformed("Column count mismatch: \(columns.count) vs \(sample.columns.count)")
        }
        for i in 0..<columns.count {
            let lhs = columns[i]
            let rhs = sample.columns[i]
            if lhs.name != rhs.name {
                throw CHBinaryError.malformed("Column name mismatch at index \(i): \(lhs.name) vs \(rhs.name)")
            }
            if lhs.type.name != rhs.type.name {
                throw CHBinaryError.malformed("Column type mismatch for \(lhs.name): \(lhs.type.name) vs \(rhs.type.name)")
            }
        }
    }

    public func normalizedForInsert(sample: CHBlock) throws -> CHBlock {
        if columns.count != sample.columns.count {
            throw CHBinaryError.malformed("Column count mismatch: \(columns.count) vs \(sample.columns.count)")
        }

        var outColumns: [CHColumn] = []
        outColumns.reserveCapacity(columns.count)
        for i in 0..<columns.count {
            let clientCol = columns[i]
            let sampleCol = sample.columns[i]
            if clientCol.name != sampleCol.name {
                throw CHBinaryError.malformed("Column name mismatch at index \(i): \(clientCol.name) vs \(sampleCol.name)")
            }
            outColumns.append(CHColumn(name: sampleCol.name, type: sampleCol.type, values: clientCol.values))
        }
        return CHBlock(rowCount: rowCount, columns: outColumns, settings: settings)
    }
}
