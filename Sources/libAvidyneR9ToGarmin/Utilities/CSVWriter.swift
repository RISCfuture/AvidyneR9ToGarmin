import Foundation
import StreamingCSV

final class CSVWriter: Sendable {
    private let writer: StreamingCSVWriter
    private let url: URL

    init(fileURL: URL) throws {
        self.url = fileURL
        self.writer = try StreamingCSVWriter(url: fileURL)
    }

    func write(row: [String]) async throws {
        try await writer.writeRow(row)
    }

    func flush() async throws {
        try await writer.flush()
    }
}
