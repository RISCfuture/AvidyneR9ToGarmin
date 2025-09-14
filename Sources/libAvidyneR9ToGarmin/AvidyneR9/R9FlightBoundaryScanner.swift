import Foundation
import Logging
import StreamingCSV

struct FlightBoundary: Sendable {
    let startTime: Date
    let endTime: Date?
    let sourceFiles: Set<String>
}

actor R9FlightBoundaryScanner {
    private let logger: Logger?
    private let progressManager: ProgressManager?
    private let timeWindow: TimeInterval = 30.0 // 30 seconds to group PFD/MFD power-ons

    init(logger: Logger? = nil, progressManager: ProgressManager? = nil) {
        self.logger = logger
        self.progressManager = progressManager
    }

    func scanForFlightBoundaries(in directory: URL) async throws -> ScanResult {
        var powerOnEvents: [(date: Date, file: String)] = []
        var totalFiles = 0
        var scannedFiles = 0

        // Find all CSV files
        let csvFiles = findCSVFiles(in: directory)
        totalFiles = csvFiles.count

        logger?.info("Starting Phase 1: Scanning \(totalFiles) CSV files for flight boundaries")
        await progressManager?.startPhase1(totalFiles: totalFiles)

        // Process files concurrently to find POWER ON markers
        await withTaskGroup(of: [(Date, String)]?.self) { group in
            for url in csvFiles {
                group.addTask { [logger] in
                    do {
                        return try await self.scanFile(url: url, logger: logger)
                    } catch {
                        logger?.warning("Failed to scan file", metadata: ["file": .string(url.path), "error": .string(error.localizedDescription)])
                        return nil
                    }
                }
            }

            // Collect results and update progress
            for await result in group {
                scannedFiles += 1
                // Update progress as each file completes
                await progressManager?.updatePhase1Progress(filesScanned: scannedFiles)

                if let events = result {
                    powerOnEvents.append(contentsOf: events)
                }
            }
        }

        // Sort power-on events by time
        powerOnEvents.sort { $0.date < $1.date }

        // Group power-on events into flight boundaries
        let boundaries = groupIntoFlightBoundaries(powerOnEvents)

        logger?.info("Phase 1 complete: Found \(boundaries.count) flights from \(powerOnEvents.count) power-on events")
        await progressManager?.completePhase1(flightsFound: boundaries.count, powerOnEvents: powerOnEvents.count)

        return ScanResult(
            boundaries: boundaries,
            totalFiles: totalFiles,
            scannedFiles: scannedFiles
        )
    }

    private func findCSVFiles(in directory: URL) -> [URL] {
        var csvFiles: [URL] = []

        guard let enumerator = FileManager.default.enumerator(at: directory, includingPropertiesForKeys: [.nameKey, .isDirectoryKey]) else {
            return []
        }

        while let url = enumerator.nextObject() as? URL {
            guard let resourceValues = try? url.resourceValues(forKeys: [.isDirectoryKey, .nameKey]),
                  let isDirectory = resourceValues.isDirectory,
                  let name = resourceValues.name else { continue }

            if !isDirectory && name.hasSuffix(".CSV") {
                csvFiles.append(url)
            }
        }

        return csvFiles
    }

    private func scanFile(url: URL, logger: Logger?) async throws -> [(Date, String)] {
        let fileName = url.lastPathComponent
        var powerOnEvents: [(Date, String)] = []

        // Determine file type
        let fileType: R9RecordType
        if fileName.hasSuffix("_ENGINE.CSV") {
            fileType = .engine
        } else if fileName.hasSuffix("_FLIGHT.CSV") {
            fileType = .flight
        } else if fileName.hasSuffix("_SYSTEM.CSV") {
            fileType = .system
        } else {
            return [] // Skip non-R9 CSV files
        }

        // Use StreamingCSV to scan for POWER ON markers
        let reader = try StreamingCSVReader(url: url, encoding: .windowsCP1252)

        // Read header row
        guard let headers = try await reader.readRow() else {
            return []
        }

        // Process data rows
        while let row = try await reader.readRow() {
            // Check for power on marker in appropriate column
            let isPowerOn: Bool
            switch fileType {
            case .engine:
                // Check multiple possible columns for POWER ON marker
                // Try CHT[1] column first
                if let cht1Index = headers.firstIndex(of: "Eng1 CHT[1] (°F)") ?? headers.firstIndex(of: "Eng1CHT[1](°F)"),
                   cht1Index < row.count,
                   row[cht1Index] == powerOnSentinel {
                    isPowerOn = true
                // Try Oil Temperature column (appears in some files)
                } else if let oilTempIndex = headers.firstIndex(of: "Eng1 Oil Temperature") ?? headers.firstIndex(of: "Eng1OilTemperature(°F)"),
                          oilTempIndex < row.count,
                          row[oilTempIndex] == powerOnSentinel {
                    isPowerOn = true
                } else {
                    isPowerOn = false
                }
            case .flight:
                // Check Filtered NormAcc column
                if let normAccIndex = headers.firstIndex(of: "Filtered NormAcc (G)"),
                   normAccIndex < row.count {
                    isPowerOn = row[normAccIndex] == powerOnSentinel
                } else {
                    isPowerOn = false
                }
            case .system:
                // Check OAT column
                if let oatIndex = headers.firstIndex(of: "OutsideAirTemperature (°C)"),
                   oatIndex < row.count {
                    isPowerOn = row[oatIndex] == powerOnSentinel
                } else {
                    isPowerOn = false
                }
            }

            if isPowerOn {
                // Get date and time from row
                if let dateIndex = headers.firstIndex(of: "Date"),
                   let timeIndex = headers.firstIndex(of: "Time"),
                   dateIndex < row.count,
                   timeIndex < row.count {
                    if let date = parseDate(dateStr: row[dateIndex], timeStr: row[timeIndex]) {
                        powerOnEvents.append((date, fileName))
                    }
                }
            }
        }

        if !powerOnEvents.isEmpty {
            logger?.debug("Found \(powerOnEvents.count) POWER ON events in \(fileName)")
        }

        return powerOnEvents
    }

    private func parseDate(dateStr: String, timeStr: String) -> Date? {
        guard dateStr.count >= 8 else { return nil }

        let year = Int(dateStr.prefix(4))
        let month = Int(dateStr.dropFirst(4).prefix(2))
        let day = Int(dateStr.suffix(2))

        let timeParts = timeStr.split(separator: ":")
        guard timeParts.count == 3 else { return nil }

        let hour = Int(timeParts[0])
        let minute = Int(timeParts[1])
        let second = Int(timeParts[2])

        guard let year, let month, let day, let hour, let minute, let second else { return nil }

        var components = DateComponents()
        components.timeZone = TimeZone(identifier: "UTC")
        components.year = year
        components.month = month
        components.day = day
        components.hour = hour
        components.minute = minute
        components.second = second

        return Calendar.current.date(from: components)
    }

    private func groupIntoFlightBoundaries(_ events: [(date: Date, file: String)]) -> [FlightBoundary] {
        guard !events.isEmpty else { return [] }

        var boundaries: [FlightBoundary] = []
        var currentBoundaryStart: Date?
        var currentBoundaryFiles: Set<String> = []
        var lastEventTime: Date?

        for event in events {
            if let lastTime = lastEventTime, event.date.timeIntervalSince(lastTime) <= timeWindow {
                // Within time window - part of same flight
                currentBoundaryFiles.insert(event.file)
                lastEventTime = event.date
            } else {
                // New flight boundary
                if let start = currentBoundaryStart {
                    boundaries.append(FlightBoundary(
                        startTime: start,
                        endTime: lastEventTime,
                        sourceFiles: currentBoundaryFiles
                    ))
                }

                currentBoundaryStart = event.date
                currentBoundaryFiles = [event.file]
                lastEventTime = event.date
            }
        }

        // Add final boundary
        if let start = currentBoundaryStart {
            boundaries.append(FlightBoundary(
                startTime: start,
                endTime: lastEventTime,
                sourceFiles: currentBoundaryFiles
            ))
        }

        return boundaries
    }

    struct ScanResult: Sendable {
        let boundaries: [FlightBoundary]
        let totalFiles: Int
        let scannedFiles: Int
    }
}
