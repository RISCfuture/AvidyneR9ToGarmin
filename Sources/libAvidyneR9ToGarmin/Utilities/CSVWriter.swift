import Foundation
import StreamingCSV

/// StreamingCSV-based CSV writer for efficient output
public final class CSVWriter: Sendable {
    private let writer: StreamingCSVWriter
    private let url: URL

    public init(fileURL: URL) throws {
        self.url = fileURL
        self.writer = try StreamingCSVWriter(url: fileURL)
    }

    public func write(row: [String]) async throws {
        try await writer.writeRow(row)
    }

    public func flush() async throws {
        try await writer.flush()
    }
}
