import Foundation

enum AvidyneR9ToGarminError: Swift.Error {
    case invalidDate(date: String, time: String)
    case invalidValue(field: String, value: String)
    case missingField(_ field: String)
    case missingValue(_ field: String)
    case invalidRow(_ row: UInt, error: Swift.Error)
    case incompleteRecordsForDate(_ date: Date)
    case missingHeaderField(_ fields: Array<String>)
    case urlNotDirectory(_ url: URL)
    case cannotWriteFile(_ url: URL)
    case encodingError
}

extension AvidyneR9ToGarminError: LocalizedError {
    var errorDescription: String? {
        switch self {
            case let .invalidDate(date, time):
                return "Invalid datetime string '\(date),\(time)'"
            case let .invalidValue(field, value):
                return "Invalid value '\(value)' for '\(field)'"
            case let .missingField(field):
                return "Missing '\(field)' column"
            case let .missingValue(field):
                return "Missing value for '\(field)'"
            case let .invalidRow(_, error):
                return error.localizedDescription
            case let .incompleteRecordsForDate(date):
                return "Incomplete R9 entries for time \(date.ISO8601Format())"
            case let .missingHeaderField(fields):
                return "Couldn't find one of the header fields \(fields.description)"
            case let .urlNotDirectory(url):
                return "Not a directory: \(url.path)"
            case let .cannotWriteFile(url):
                return "Cannot write to file: \(url.path)"
            case .encodingError:
                return "Failed to encode CSV data"
        }
    }
}
