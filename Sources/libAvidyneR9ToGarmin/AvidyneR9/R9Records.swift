import Foundation
import Logging

actor R9Records {
    var logger: Logger? = nil
    
    private var records = Array<R9Record>()
    private var recordsForTimestamp = Dictionary<UInt, Array<Int>>()
    private var delimiters = Set<UInt>()
    
    private static let recordingIntervals: Dictionary<R9RecordType, UInt> = [
        .engine: 4,
        .flight: 1,
        .system: 2
    ]
    
    private static let recordingInterval: TimeInterval = 4
    
    private(set) var processedFiles = 0
    private(set) var failedFiles = 0
    private(set) var totalFiles = 0
    
    var fractionComplete: Float? {
        guard totalFiles != 0 else { return nil }
        return Float(processedFiles + failedFiles)/Float(totalFiles)
    }

    func setLogger(_ logger: Logger?) {
        self.logger = logger
    }

    func process(url: URL) async {
        self.totalFiles += 1
        
        do {
            guard let parser = try R9FileParser(url: url, logger: logger) else {
                self.failedFiles += 1
                return
            }
            
            for await record in try await parser.parse() {
                switch record {
                    case let .record(record): self.add(record: record)
                    case let .powerOn(date): self.add(delimiterAt: UInt(date.timeIntervalSince1970))
                    default: break
                }
            }
            
            self.processedFiles += 1
        } catch {
            logger?.error("\(url.path): \(error.localizedDescription)")
            self.failedFiles += 1
        }
    }
    
    func reset() {
        records.removeAll()
        recordsForTimestamp.removeAll()
        delimiters.removeAll()
        processedFiles = 0
        failedFiles = 0
        totalFiles = 0
    }

    func dates() -> AsyncStream<(Date, Array<R9Record>, Bool)> {
        AsyncStream { continuation in
            var delimiters = self.delimiters.sorted { $1 < $0 }
            for timestamp in recordsForTimestamp.keys.sorted() {
                let date = Date(timeIntervalSince1970: TimeInterval(timestamp))
                let values = recordsForTimestamp[timestamp]!.map { records[$0] }
                var newFile = false
                while delimiters.last != nil && delimiters.last! < timestamp {
                    _ = delimiters.popLast()
                    newFile = true
                }

                continuation.yield((date, values, newFile))
            }
            continuation.finish()
        }
    }
    
    private func add(record: R9Record) {
        records.append(record)
        
        let timeStep = Self.recordingIntervals[record.type]!/2
        let timestamp = UInt(record.date!.timeIntervalSince1970)
        let earliestDate = timestamp - timeStep
        let latestDate = timestamp + timeStep
        
        var date = earliestDate
        while date <= latestDate {
            if recordsForTimestamp[date] == nil { recordsForTimestamp[date] = .init() }
            recordsForTimestamp[date]!.append(records.count - 1)
            date += 1
        }
    }
    
    private func add(delimiterAt timestamp: UInt) {
        delimiters.insert(timestamp)
    }
}
