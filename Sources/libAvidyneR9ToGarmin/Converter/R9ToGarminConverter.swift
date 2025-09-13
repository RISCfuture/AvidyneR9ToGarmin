import Foundation
import Logging
import StreamingCSV

/// Memory-efficient converter using two-phase approach with StreamingCSV
public actor R9ToGarminConverter {
    private static let headers = """
    #airframe_info,log_version="1.00",log_content_version="1.02",product="GDU 460",aircraft_ident="N171MA",unit_software_part_number="710-00118-000",product="GDU 460",software_version="9.00",system_id="M093570387",unit="PFD1"
    Date (yyyy-mm-dd),Time (hh:mm:ss),UTC Time (hh:mm:ss),UTC Offset (hh:mm),Latitude (deg),Longitude (deg),GPS Altitude (ft),GPS Fix Status,GPS Time of Week (sec),GPS Ground Speed (kt),GPS Ground Track (deg),GPS Velocity E (m/sec),GPS Velocity N (m/sec),GPS Velocity U (m/sec),Magnetic Heading (deg),GPS PDOP,GPS Sats,Pressure Altitude (ft),Baro Altitude (ft),Vertical Speed (ft/min),Indicated Airspeed (kt),True Airspeed (kt),Pitch (deg),Roll (deg),Lateral Acceleration (G),Normal Acceleration (G),Selected Heading (deg),Selected Altitude (ft),Selected Vertical Speed (ft/min),Selected Airspeed (kt),Baro Setting (inch Hg),COM Frequency 1 (MHz),COM Frequency 2 (MHz),NAV Frequency (MHz),Active Nav Source,Nav Annunciation,Nav Identifier,Nav Distance (nm),Nav Bearing (deg),Nav Course (deg),Nav Cross Track Distance (nm),Horizontal CDI Deflection,Horizontal CDI Full Scale (ft),Horizontal CDI Scale,Vertical CDI Deflection,Vertical CDI Full Scale (ft),VNAV CDI Deflection,VNAV Altitude (ft),Autopilot State,FD Lateral Mode,FD Vertical Mode,FD Roll Command (deg),FD Pitch Command (deg),FD Altitude (ft),AP Roll Command (deg),AP Pitch Command (deg),AP VS Command (ft/min),AP Altitude Command (ft),AP Roll Torque (%),AP Pitch Torque (%),AP Roll Trim Motor,AP Pitch Trim Motor,Magnetic Variation (deg),Outside Air Temp (deg C),Density Altitude (ft),Height Above Ground (ft),Wind Speed (kt),Wind Direction (deg),AHRS Status,AHRS Dev (%),Magnetometer Status,Network Status,Transponder Code,Transponder Mode,Oil Temp (deg F),Fuel L Qty (gal),Fuel R Qty (gal),Fuel Press (PSI),Oil Press (PSI),RPM,Manifold Press (inch Hg),Volts,Volts,Amps,Amps,Fuel Flow (gal/hour),Elevator Trim,Aileron Trim,CHT1 (deg F),CHT2 (deg F),CHT3 (deg F),CHT4 (deg F),CHT5 (deg F),CHT6 (deg F),EGT1 (deg F),EGT2 (deg F),EGT3 (deg F),EGT4 (deg F),EGT5 (deg F),EGT6 (deg F),Engine Power (%),CAS Alert,Terrain Alert,Engine 1 Cycle Count
    Lcl Date,Lcl Time,UTC Time,UTCOfst,Latitude,Longitude,AltGPS,GPSfix,,GndSpd,TRK,GPSVelE,GPSVelN,GPSVelU,HDG,PDOP,,AltP,AltInd,VSpd,IAS,TAS,Pitch,Roll,LatAc,NormAc,SelHDG,SelALT,SelVSpd,SelIAS,Baro,COM1,COM2,NAV1,NavSrc,,NavIdent,NavDist,NavBrg,NavCRS,NavXTK,HCDI,,,VCDI,,VNAV CDI,VNAVAlt,,,,,,,,,,,,,MagVar,OAT,AltD,AGL,WndSpd,WndDr,,,,,,,E1 OilT,FQty1,FQty2,E1 FPres,E1 OilP,E1 RPM,E1 MAP,Volts1,Volts2,Amps1,Amps2,E1 FFlow,PTrim,RTrim,E1 CHT1,E1 CHT2,E1 CHT3,E1 CHT4,E1 CHT5,E1 CHT6,E1 EGT1,E1 EGT2,E1 EGT3,E1 EGT4,E1 EGT5,E1 EGT6,E1 %Pwr
    """

    private static var headerRows: [[String]] {
        headers.split(separator: "\n").map { line in line.split(separator: ",", omittingEmptySubsequences: false).map { cell in String(cell) } }
    }

    private static var headerRowCount: Int {
        headerRows.map(\.count).max()!
    }

    private static let garminFilenameFormat = "log_%04d%02d%02d_%02d%02d%02d_%@.csv"
    private static let placeholderAirportID = "____"
    private static let recordingInterval: TimeInterval = 4.0
    private static let timeWindowSize: TimeInterval = 8.0 // Buffer 8 seconds of records

    /// The logger instance.
    public var logger: Logger?

    /// Progress reporter for external monitoring
    public var progressReporter: (@Sendable (Float, String) -> Void)?

    /// Memory usage statistics
    public struct MemoryStats: Sendable {
        public let phase1MemoryMB: Double
        public let phase2PeakMemoryMB: Double
        public let totalProcessingTime: TimeInterval
    }

    private var stats = MemoryStats(phase1MemoryMB: 0, phase2PeakMemoryMB: 0, totalProcessingTime: 0)

    /// Creates a new streaming converter.
    public init() {}

    /// Sets the logger instance.
    public func setLogger(_ logger: Logger?) { self.logger = logger }

    /// Sets the progress reporter.
    public func setProgressReporter(_ reporter: @escaping @Sendable (Float, String) -> Void) {
        self.progressReporter = reporter
    }

    /// Gets memory statistics from the last conversion
    public func getMemoryStats() -> MemoryStats { stats }

    /// Converts R9 records to Garmin format using two-phase streaming approach
    public func convert(from inputDirectory: URL, to outputDirectory: URL) async throws {
        let startTime = Date()

        guard outputDirectory.isDirectory else {
            throw AvidyneR9ToGarminError.urlNotDirectory(outputDirectory)
        }

        // Phase 1: Scan for flight boundaries
        progressReporter?(0.0, "Phase 1: Scanning for flight boundaries...")
        let scanner = R9FlightBoundaryScanner(logger: logger)
        let phase1Start = Date()
        let scanResult = try await scanner.scanForFlightBoundaries(in: inputDirectory)
        let phase1Memory = getMemoryUsage()
        let phase1Time = Date().timeIntervalSince(phase1Start)

        logger?.info("Phase 1 complete", metadata: [
            "flights": .string("\(scanResult.boundaries.count)"),
            "files": .string("\(scanResult.scannedFiles)"),
            "memory_mb": .string(String(format: "%.2f", phase1Memory)),
            "time_sec": .string(String(format: "%.2f", phase1Time))
        ])

        progressReporter?(0.3, "Found \(scanResult.boundaries.count) flights in \(scanResult.scannedFiles) files")

        // Phase 2: Stream and convert records
        progressReporter?(0.35, "Phase 2: Converting records...")
        let phase2Start = Date()
        try await streamAndConvertRecords(
            from: inputDirectory,
            to: outputDirectory,
            boundaries: scanResult.boundaries
        )
        let phase2Time = Date().timeIntervalSince(phase2Start)
        let phase2Memory = getMemoryUsage()

        let totalTime = Date().timeIntervalSince(startTime)
        stats = MemoryStats(
            phase1MemoryMB: phase1Memory,
            phase2PeakMemoryMB: phase2Memory,
            totalProcessingTime: totalTime
        )

        logger?.info("Conversion complete", metadata: [
            "total_time_sec": .string(String(format: "%.2f", totalTime)),
            "phase1_time_sec": .string(String(format: "%.2f", phase1Time)),
            "phase2_time_sec": .string(String(format: "%.2f", phase2Time)),
            "peak_memory_mb": .string(String(format: "%.2f", max(phase1Memory, phase2Memory)))
        ])

        progressReporter?(1.0, "Conversion complete!")
    }

    private func streamAndConvertRecords(
        from inputDirectory: URL,
        to outputDirectory: URL,
        boundaries: [FlightBoundary]
    ) async throws {
        // Find all CSV files
        let csvFiles = findCSVFiles(in: inputDirectory)
        let totalFiles = csvFiles.count

        // Create record combiner with time window
        let combiner = StreamingRecordCombiner(timeWindow: Self.timeWindowSize, logger: logger)

        // Process files concurrently and add to combiner
        await withTaskGroup(of: Void.self) { group in
            for (fileIndex, url) in csvFiles.enumerated() {
                group.addTask { [logger] in
                    do {
                        guard let parser = try R9FileParser(url: url, logger: logger) else { return }

                        for try await entry in try await parser.parse() {
                            switch entry {
                            case .engineRow(let row):
                                await combiner.addEngineRow(row)
                            case .engineLegacyRow(let row):
                                await combiner.addEngineLegacyRow(row)
                            case .flightRow(let row):
                                await combiner.addFlightRow(row)
                            case .systemRow(let row):
                                await combiner.addSystemRow(row)
                            case .powerOn, .incrementalExtract:
                                break // Already handled in Phase 1
                            }
                        }

                        // Update progress
                        let progress = 0.35 + (Float(fileIndex + 1) / Float(totalFiles)) * 0.45
                        await self.progressReporter?(progress, "Processing file \(fileIndex + 1)/\(totalFiles)")
                    } catch {
                        logger?.warning("Failed to parse file", metadata: [
                            "file": .string(url.path),
                            "error": .string(error.localizedDescription)
                        ])
                    }
                }
            }
        }

        // Process combined records sequentially
        var currentWriter: CSVWriter?
        var currentOutputFile: URL?
        var currentBoundaryIndex = 0
        var recordsWritten = 0
        var totalRecordsProcessed = 0

        // Finish processing and get all combined records
        await combiner.finishProcessing()

        let allRecords = await combiner.getCombinedRecords()
        logger?.info("Processing combined records...")

        var currentBoundary: FlightBoundary? = boundaries.first
        var nextBoundary: FlightBoundary? = boundaries.count > 1 ? boundaries[1] : nil

        for await (date, bundle) in allRecords {
            totalRecordsProcessed += 1

            // Check if we need to move to next boundary
            while nextBoundary != nil && date >= nextBoundary!.startTime {
                // Close current file if empty
                if let oldFile = currentOutputFile, recordsWritten == 0 {
                    logger?.debug("Removing empty file: \(oldFile.lastPathComponent)")
                    try? FileManager.default.removeItem(at: oldFile)
                } else if recordsWritten > 0 {
                    logger?.info("Completed file with \(recordsWritten) records")
                }

                // Move to next boundary
                currentBoundaryIndex += 1
                currentBoundary = currentBoundaryIndex < boundaries.count ? boundaries[currentBoundaryIndex] : nil
                nextBoundary = currentBoundaryIndex + 1 < boundaries.count ? boundaries[currentBoundaryIndex + 1] : nil
                currentWriter = nil
                currentOutputFile = nil
                recordsWritten = 0
            }

            // Skip records before the first boundary or after all boundaries
            guard let boundary = currentBoundary else { continue }
            guard date >= boundary.startTime else { continue }

            // Start new file if needed
            if currentWriter == nil {
                let result = try await startNewFile(date: boundary.startTime, directory: outputDirectory)
                currentWriter = result.0
                currentOutputFile = result.1
                recordsWritten = 0
                logger?.debug("Started new Garmin file", metadata: [
                    "file": .string(currentOutputFile?.lastPathComponent ?? "unknown"),
                    "boundary": .string("\(currentBoundaryIndex)")
                ])
            }

            // Convert and write record
            if let garminRecord = try await bundleToGarminRecord(bundle, date: date) {
                let row = garminRecordToRow(garminRecord)
                try await currentWriter?.write(row: row)
                recordsWritten += 1
            }
        }

        // Clean up empty file if needed
        if let oldFile = currentOutputFile, recordsWritten == 0 {
            logger?.debug("Removing empty file: \(oldFile.lastPathComponent)")
            try? FileManager.default.removeItem(at: oldFile)
        }

        logger?.info("Phase 2 complete: processed \(totalRecordsProcessed) records, wrote \(recordsWritten) records")
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

            if !isDirectory && name.hasSuffix(".CSV") &&
               (name.contains("_ENGINE") || name.contains("_FLIGHT") || name.contains("_SYSTEM")) {
                csvFiles.append(url)
            }
        }

        return csvFiles
    }

    private func startNewFile(date: Date, directory: URL) async throws -> (CSVWriter, URL) {
        let calendarDate = Calendar.current.dateComponents(in: zulu, from: date)
        let filename = String(format: Self.garminFilenameFormat,
                              calendarDate.year!, calendarDate.month!, calendarDate.day!,
                              calendarDate.hour!, calendarDate.minute!, calendarDate.second!,
                              Self.placeholderAirportID)

        let url = directory.appendingPathComponent(filename)
        let writer = try CSVWriter(fileURL: url)
        try await writeHeaderRows(writer: writer)

        return (writer, url)
    }

    private func writeHeaderRows(writer: CSVWriter) async throws {
        for row in Self.headerRows {
            var fixedRow = [String](row)
            fixedRow.reserveCapacity(Self.headerRowCount)
            if row.count < Self.headerRowCount {
                fixedRow.append(contentsOf: Array(repeating: "", count: Self.headerRowCount - row.count))
            }
            try await writer.write(row: fixedRow)
        }
    }

    private func getMemoryUsage() -> Double {
        var info = mach_task_basic_info()
        var count = mach_msg_type_number_t(MemoryLayout<mach_task_basic_info>.size) / 4

        let result = withUnsafeMutablePointer(to: &info) {
            $0.withMemoryRebound(to: integer_t.self, capacity: 1) {
                task_info(mach_task_self_,
                         task_flavor_t(MACH_TASK_BASIC_INFO),
                         $0,
                         &count)
            }
        }

        return result == KERN_SUCCESS ? Double(info.resident_size) / 1024.0 / 1024.0 : 0
    }

    // Convert bundle to Garmin record
    private func bundleToGarminRecord(_ bundle: StreamingRecordCombiner.RecordBundle, date: Date) async throws -> GarminRecord? {
        let engineRows = bundle.engineRows
        let engineLegacyRows = bundle.engineLegacyRows
        let flightRows = bundle.flightRows
        let systemRows = bundle.systemRows

        // Skip if no records
        guard !engineRows.isEmpty || !engineLegacyRows.isEmpty || !flightRows.isEmpty || !systemRows.isEmpty else { return nil }

        // Combine each type
        var garminRecord = GarminRecord()
        garminRecord.date = date

        if !engineRows.isEmpty {
            let combiner = R9CSVRowCombiner(rows: engineRows)
            // Copy engine fields
            garminRecord.oilTemperature = await combiner.get(\.oilTemperature)
            garminRecord.oilPressure = await combiner.get(\.oilPressure)
            garminRecord.RPM = await combiner.get(\.RPM)
            garminRecord.manifoldPressure = await combiner.get(\.manifoldPressure)
            // Convert CHT fields (already individual Float fields)
            garminRecord.CHTs = [
                await combiner.get(\.CHT1).map { Int($0) },
                await combiner.get(\.CHT2).map { Int($0) },
                await combiner.get(\.CHT3).map { Int($0) },
                await combiner.get(\.CHT4).map { Int($0) },
                await combiner.get(\.CHT5).map { Int($0) },
                await combiner.get(\.CHT6).map { Int($0) }
            ]
            // Convert EGT fields (already individual Float fields)
            garminRecord.EGTs = [
                await combiner.get(\.EGT1).map { Int($0) },
                await combiner.get(\.EGT2).map { Int($0) },
                await combiner.get(\.EGT3).map { Int($0) },
                await combiner.get(\.EGT4).map { Int($0) },
                await combiner.get(\.EGT5).map { Int($0) },
                await combiner.get(\.EGT6).map { Int($0) }
            ]
            // Convert Float to UInt8 for percentPower (clamp to valid range)
            if let power = await combiner.get(\.percentPower) {
                let clamped = max(0, min(255, power))
                garminRecord.percentPower = UInt8(clamped)
            }
            garminRecord.fuelFlow = await combiner.get(\.fuelFlow)
            // Map mainBus1Potential to potential1 and emergencyBusPotential to potential2
            garminRecord.potential1 = await combiner.get(\.mainBus1Potential)
            garminRecord.potential2 = await combiner.get(\.emergencyBusPotential)
        } else if !engineLegacyRows.isEmpty {
            let combiner = R9CSVRowCombiner(rows: engineLegacyRows)
            // Copy engine fields from legacy rows
            garminRecord.oilTemperature = await combiner.get(\.oilTemperature)
            garminRecord.oilPressure = await combiner.get(\.oilPressure)
            garminRecord.RPM = await combiner.get(\.RPM)
            garminRecord.manifoldPressure = await combiner.get(\.manifoldPressure)
            // Convert CHT fields
            garminRecord.CHTs = [
                await combiner.get(\.CHT1).map { Int($0) },
                await combiner.get(\.CHT2).map { Int($0) },
                await combiner.get(\.CHT3).map { Int($0) },
                await combiner.get(\.CHT4).map { Int($0) },
                await combiner.get(\.CHT5).map { Int($0) },
                await combiner.get(\.CHT6).map { Int($0) }
            ]
            // Convert EGT fields
            garminRecord.EGTs = [
                await combiner.get(\.EGT1).map { Int($0) },
                await combiner.get(\.EGT2).map { Int($0) },
                await combiner.get(\.EGT3).map { Int($0) },
                await combiner.get(\.EGT4).map { Int($0) },
                await combiner.get(\.EGT5).map { Int($0) },
                await combiner.get(\.EGT6).map { Int($0) }
            ]
            // Convert Float to UInt8 for percentPower (clamp to valid range)
            if let power = await combiner.get(\.percentPower) {
                let clamped = max(0, min(255, power))
                garminRecord.percentPower = UInt8(clamped)
            }
            garminRecord.fuelFlow = await combiner.get(\.fuelFlow)
            // Map mainBus1Potential to potential1 and emergencyBusPotential to potential2
            garminRecord.potential1 = await combiner.get(\.mainBus1Potential)
            garminRecord.potential2 = await combiner.get(\.emergencyBusPotential)
        }

        if !flightRows.isEmpty {
            let combiner = R9CSVRowCombiner(rows: flightRows)
            // Copy flight fields
            // Use filteredNormalAcceleration if available, otherwise fall back to normalAcceleration
            if let filteredNormal = await combiner.get(\.filteredNormalAcceleration) {
                garminRecord.normalAcceleration = filteredNormal
            } else {
                garminRecord.normalAcceleration = await combiner.get(\.normalAcceleration)
            }
            garminRecord.lateralAcceleration = await combiner.get(\.lateralAcceleration)
            garminRecord.heading = await combiner.get(\.heading)
            garminRecord.pitch = await combiner.get(\.pitch)
            garminRecord.roll = await combiner.get(\.roll)
            garminRecord.pressureAltitude = await combiner.get(\.pressureAltitude)
            // Convert UInt to Float for airspeed
            if let ias = await combiner.get(\.indicatedAirspeed) {
                garminRecord.indicatedAirspeed = Float(ias)
            }
            if let tas = await combiner.get(\.trueAirspeed) {
                garminRecord.trueAirspeed = Float(tas)
            }
            garminRecord.verticalSpeed = await combiner.get(\.verticalSpeed)
            garminRecord.latitude = await combiner.get(\.GPSLatitude)
            garminRecord.longitude = await combiner.get(\.GPSLongitude)

            // Flight director fields
            garminRecord.FDPitchCommand = await combiner.get(\.FDPitch)
            garminRecord.FDRollCommand = await combiner.get(\.FDRoll)

            // Convert DFC100 modes to strings
            if let latMode = await combiner.get(\.DFC100_activeLateralMode) {
                garminRecord.FDLateralMode = String(describing: latMode)
            }
            if let vertMode = await combiner.get(\.DFC100_activeVerticalMode) {
                garminRecord.FDVerticalMode = String(describing: vertMode)
            }
        }

        if !systemRows.isEmpty {
            let combiner = R9CSVRowCombiner(rows: systemRows)
            // Copy system fields
            // Convert Int to Float for OAT
            if let oat = await combiner.get(\.oat) {
                garminRecord.outsideAirTemperature = Float(oat)
            }
            // Convert UInt to Float for groundSpeed
            if let gs = await combiner.get(\.groundSpeed) {
                garminRecord.groundSpeed = Float(gs)
            }
            // Convert Int to UInt16 for groundTrack (handle negative values)
            if let gt = await combiner.get(\.groundTrack) {
                // Normalize to 0-359 range
                let normalized = gt < 0 ? gt + 360 : gt
                garminRecord.groundTrack = normalized >= 0 && normalized <= 65535 ? UInt16(normalized) : nil
            }
            garminRecord.altimeterSetting = await combiner.get(\.altimeterSetting)
            garminRecord.magneticVariation = await combiner.get(\.magneticVariation)
            garminRecord.headingBug = await combiner.get(\.headingBug)
            garminRecord.altitudeBug = await combiner.get(\.altitudeBug)

            // Calculate baroAltitude from pressureAltitude and altimeterSetting
            if let pressureAlt = garminRecord.pressureAltitude,
               let altimeterSetting = garminRecord.altimeterSetting {
                // Barometric altitude correction: approximately 1000 ft per inch of mercury
                garminRecord.baroAltitude = pressureAlt + Int(((altimeterSetting - 29.92) * 1000).rounded())
            }

            garminRecord.navBearing = await combiner.get(\.navaidBearing)
            // Try OBS, desiredTrack, or FMSCourse in order
            if let obs = await combiner.get(\.OBS) {
                garminRecord.navCourse = obs
            } else if let dt = await combiner.get(\.desiredTrack) {
                garminRecord.navCourse = dt
            } else if let fc = await combiner.get(\.FMSCourse) {
                garminRecord.navCourse = fc
            }
            garminRecord.navDistance = await combiner.get(\.distanceToWaypoint)
            garminRecord.crossTrackDistance = await combiner.get(\.crossTrackDeviation)
            garminRecord.navIdentifier = await combiner.get(\.activeWaypoint)

            // Handle autopilot mode conversion
            if let apMode = await combiner.get(\.autopilotMode) {
                garminRecord.autopilotState = apMode.rawValue
            }
        }

        return garminRecord
    }

    private func garminRecordToRow(_ record: GarminRecord) -> [String] {
        // Use the existing conversion logic from Output.swift
        // We'll need to make our own simplified version since we can't use the extension
        return convertGarminRecordToRow(record)
    }

    // Formatting helper functions

    private func convertGarminRecordToRow(_ record: GarminRecord) -> [String] {
        // Use the toCSVRow() method generated by @CSVRowEncoderBuilder
        return record.toCSVRow()
    }
}

/// Combines records within a time window for streaming processing
actor StreamingRecordCombiner {
    struct RecordBundle {
        var engineRows: [R9EngineRow] = []
        var engineLegacyRows: [R9EngineLegacyRow] = []
        var flightRows: [R9FlightRow] = []
        var systemRows: [R9SystemRow] = []
        var date: Date
    }

    private let timeWindow: TimeInterval
    private let logger: Logger?
    private var buffer: [Date: RecordBundle] = [:]
    private var lastFlushTime = Date.distantPast
    private var recordsWithoutDates = 0
    private var totalRecordsAdded = 0

    init(timeWindow: TimeInterval, logger: Logger?) {
        self.timeWindow = timeWindow
        self.logger = logger
    }

    func addEngineRow(_ row: R9EngineRow) {
        guard let dateStr = row.Date, let timeStr = row.Time,
              let date = parseDateTime(dateStr: dateStr, timeStr: timeStr) else {
            recordsWithoutDates += 1
            return
        }
        totalRecordsAdded += 1
        let timestamp = date.timeIntervalSince1970.rounded()
        let roundedDate = Date(timeIntervalSince1970: timestamp)

        if buffer[roundedDate] == nil {
            buffer[roundedDate] = RecordBundle(date: roundedDate)
        }
        buffer[roundedDate]?.engineRows.append(row)

        cleanOldEntries()
    }

    func addEngineLegacyRow(_ row: R9EngineLegacyRow) {
        guard let dateStr = row.Date, let timeStr = row.Time,
              let date = parseDateTime(dateStr: dateStr, timeStr: timeStr) else {
            recordsWithoutDates += 1
            return
        }
        totalRecordsAdded += 1
        let timestamp = date.timeIntervalSince1970.rounded()
        let roundedDate = Date(timeIntervalSince1970: timestamp)

        if buffer[roundedDate] == nil {
            buffer[roundedDate] = RecordBundle(date: roundedDate)
        }
        buffer[roundedDate]?.engineLegacyRows.append(row)

        cleanOldEntries()
    }

    func addFlightRow(_ row: R9FlightRow) {
        guard let dateStr = row.Date, let timeStr = row.Time,
              let date = parseDateTime(dateStr: dateStr, timeStr: timeStr) else {
            recordsWithoutDates += 1
            return
        }
        totalRecordsAdded += 1
        let timestamp = date.timeIntervalSince1970.rounded()
        let roundedDate = Date(timeIntervalSince1970: timestamp)

        if buffer[roundedDate] == nil {
            buffer[roundedDate] = RecordBundle(date: roundedDate)
        }
        buffer[roundedDate]?.flightRows.append(row)

        cleanOldEntries()
    }

    func addSystemRow(_ row: R9SystemRow) {
        guard let dateStr = row.Date, let timeStr = row.Time,
              let date = parseDateTime(dateStr: dateStr, timeStr: timeStr) else {
            recordsWithoutDates += 1
            return
        }
        totalRecordsAdded += 1
        let timestamp = date.timeIntervalSince1970.rounded()
        let roundedDate = Date(timeIntervalSince1970: timestamp)

        if buffer[roundedDate] == nil {
            buffer[roundedDate] = RecordBundle(date: roundedDate)
        }
        buffer[roundedDate]?.systemRows.append(row)

        cleanOldEntries()
    }

    private func cleanOldEntries() {
        // Don't clean during processing - we'll do it in finishProcessing
        // The original logic was wrong - it was using current time instead of record time
    }

    func finishProcessing() {
        // Flush all remaining records
        // This will be called after all files are processed
        logger?.info("StreamingRecordCombiner finished: totalAdded=\(totalRecordsAdded), withoutDates=\(recordsWithoutDates), buffered=\(buffer.count)")
    }

    func getCombinedRecords() -> AsyncStream<(Date, RecordBundle)> {
        AsyncStream { continuation in
            // Return all buffered records sorted by date
            let sortedDates = buffer.keys.sorted()
            logger?.debug("StreamingRecordCombiner: Returning \(sortedDates.count) combined records")
            for date in sortedDates {
                if let bundle = buffer[date] {
                    continuation.yield((date, bundle))
                }
            }
            continuation.finish()
        }
    }

    func getTotalRecordCount() -> Int {
        return buffer.count
    }

    private func parseDateTime(dateStr: String, timeStr: String) -> Date? {
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
        guard year >= 2005 else { return nil }

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
}
