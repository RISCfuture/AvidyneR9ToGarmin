import Foundation
import Logging
import StreamingCSV

enum R9LogEntry: Sendable {
    case engineRow(_ row: R9EngineRow)
    case engineLegacyRow(_ row: R9EngineLegacyRow)
    case flightRow(_ row: R9FlightRow)
    case systemRow(_ row: R9SystemRow)
    case powerOn(date: Date)
    case incrementalExtract(date: Date)
}

actor R9FileParser {
    let url: URL
    let type: R9RecordType
    var logger: Logger?

    private var path: String { url.path }

    init?(url: URL, logger: Logger? = nil) throws {
        self.url = url
        self.logger = logger

        if url.lastPathComponent.hasSuffix("_ENGINE.CSV") { type = .engine }
        else if url.lastPathComponent.hasSuffix("_FLIGHT.CSV") { type = .flight }
        else if url.lastPathComponent.hasSuffix("_SYSTEM.CSV") { type = .system }
        else {
            logger?.info("Bad file name; skipping",
                         metadata: ["path": .string(url.path)])
            return nil
        }
    }

    func parse() throws -> AsyncThrowingStream<R9LogEntry, Error> {
        return AsyncThrowingStream { continuation in
            Task {
                do {
                    let reader = try StreamingCSVReader(url: url, encoding: .windowsCP1252)

                    // Read header row
                    guard let headers = try await reader.readRow() else {
                        continuation.finish()
                        return
                    }

                    // Pre-compute column indices for faster lookup
                    let columnIndices = createColumnIndexMap(headers: headers)

                    // Process data rows
                    while let row = try await reader.readRow() {
                        if let entry = try parseRowOptimized(row, columnIndices: columnIndices) {
                            continuation.yield(entry)
                        }
                    }
                } catch {
                    continuation.finish(throwing: error)
                    return
                }
                continuation.finish()
            }
        }
    }

    // Create a map of column names to indices for O(1) lookup
    private func createColumnIndexMap(headers: [String]) -> [String: Int] {
        var map: [String: Int] = [:]
        for (index, header) in headers.enumerated() {
            map[header] = index
        }
        return map
    }

    // Optimized row parser using pre-computed indices
    private func parseRowOptimized(_ row: [String], columnIndices: [String: Int]) throws -> R9LogEntry? {
        // Check for special rows first
        if row.count > 3 {
            if row[3] == powerOnSentinel || (!row.isEmpty && row[0] == powerOnSentinel) {
                if let date = parseDateFromRow(row) {
                    return .powerOn(date: date)
                }
            }
            if row[3] == incrementalExtractSentinel || (!row.isEmpty && row[0] == incrementalExtractSentinel) {
                if let date = parseDateFromRow(row) {
                    return .incrementalExtract(date: date)
                }
            }
        }

        // Helper function to get value by column name
        func getValue(_ columnName: String) -> String? {
            guard let index = columnIndices[columnName],
                  index < row.count else { return nil }
            let value = row[index]
            return value.isEmpty ? nil : value
        }

        switch type {
        case .engine:
            // Determine if this is legacy format
            let isLegacy = columnIndices["Eng1OilTemperature(°F)"] != nil
            if isLegacy {
                return parseEngineLegacyRowOptimized(row: row, columnIndices: columnIndices, getValue: getValue)
            }
            return parseEngineRowOptimized(row: row, columnIndices: columnIndices, getValue: getValue)
        case .flight:
            return parseFlightRowOptimized(row: row, columnIndices: columnIndices, getValue: getValue)
        case .system:
            return parseSystemRowOptimized(row: row, columnIndices: columnIndices, getValue: getValue)
        }
    }

    private func parseRow(_ row: [String], headers: [String]) throws -> R9LogEntry? {
        // Check for special rows first
        if row.count > 3 {
            if row[3] == powerOnSentinel || (!row.isEmpty && row[0] == powerOnSentinel) {
                if let date = parseDateFromRow(row) {
                    return .powerOn(date: date)
                }
            }
            if row[3] == incrementalExtractSentinel || (!row.isEmpty && row[0] == incrementalExtractSentinel) {
                if let date = parseDateFromRow(row) {
                    return .incrementalExtract(date: date)
                }
            }
        }

        // Create dictionary for field access
        var fields: [String: String] = [:]
        for (index, header) in headers.enumerated() where index < row.count {
            fields[header] = row[index]
        }

        // Skip empty rows
        if fields["Systime"] == nil || fields["Systime"] == "-" || fields["Systime"]?.isEmpty == true {
            return nil
        }

        switch type {
        case .engine:
            // Determine if this is legacy format
            let isLegacy = headers.contains("Eng1OilTemperature(°F)")
            if isLegacy {
                return parseEngineLegacyRow(fields: fields)
            }
            return parseEngineRow(fields: fields)
        case .flight:
            return parseFlightRow(fields: fields)
        case .system:
            return parseSystemRow(fields: fields)
        }
    }

    // Optimized parsing functions using pre-computed indices
    private func parseEngineRowOptimized(row _: [String], columnIndices _: [String: Int], getValue: (String) -> String?) -> R9LogEntry? {
        var engineRow = R9EngineRow()

        engineRow.Systime = getValue("Systime").flatMap(parseUInt)
        engineRow.Date = getValue("Date")
        engineRow.Time = getValue("Time")

        engineRow.oilTemperature = getValue("Eng1 Oil Temperature").flatMap(parseInt)
        engineRow.oilPressure = getValue("Eng1 Oil Pressure").flatMap(parseUInt)
        engineRow.RPM = getValue("Eng1 RPM").flatMap(parseUInt)
        engineRow.manifoldPressure = getValue("Eng1 Manifold Pressure (In.Hg)").flatMap(parseFloat)

        // CHT fields
        engineRow.CHT1 = getValue("Eng1 CHT1 (°F)").flatMap(parseFloat)
        engineRow.CHT2 = getValue("Eng1 CHT2 (°F)").flatMap(parseFloat)
        engineRow.CHT3 = getValue("Eng1 CHT3 (°F)").flatMap(parseFloat)
        engineRow.CHT4 = getValue("Eng1 CHT4 (°F)").flatMap(parseFloat)
        engineRow.CHT5 = getValue("Eng1 CHT5 (°F)").flatMap(parseFloat)
        engineRow.CHT6 = getValue("Eng1 CHT6 (°F)").flatMap(parseFloat)

        // EGT fields
        engineRow.EGT1 = getValue("Eng1 EGT1 (°F)").flatMap(parseFloat)
        engineRow.EGT2 = getValue("Eng1 EGT2 (°F)").flatMap(parseFloat)
        engineRow.EGT3 = getValue("Eng1 EGT3 (°F)").flatMap(parseFloat)
        engineRow.EGT4 = getValue("Eng1 EGT4 (°F)").flatMap(parseFloat)
        engineRow.EGT5 = getValue("Eng1 EGT5 (°F)").flatMap(parseFloat)
        engineRow.EGT6 = getValue("Eng1 EGT6 (°F)").flatMap(parseFloat)

        engineRow.percentPower = getValue("Eng1 Percent Pwr").flatMap(parseFloat)
        engineRow.fuelFlow = getValue("FuelFlow").flatMap(parseFloat)
        engineRow.mainBus1Potential = getValue("Eng1 MBus1 Volts").flatMap(parseFloat)
        engineRow.emergencyBusPotential = getValue("Eng1 Bus2 Volts").flatMap(parseFloat)

        return .engineRow(engineRow)
    }

    private func parseEngineLegacyRowOptimized(row _: [String], columnIndices _: [String: Int], getValue: (String) -> String?) -> R9LogEntry? {
        var engineRow = R9EngineLegacyRow()

        engineRow.Systime = getValue("Systime").flatMap(parseUInt)
        engineRow.Date = getValue("Date")
        engineRow.Time = getValue("Time")

        engineRow.oilTemperature = getValue("Eng1OilTemperature(°F)").flatMap(parseInt)
        engineRow.oilPressure = getValue("Eng1OilPressure").flatMap(parseUInt)
        engineRow.RPM = getValue("Eng1RPM").flatMap(parseUInt)
        engineRow.manifoldPressure = getValue("Eng1ManifoldPressure(In.Hg)").flatMap(parseFloat)

        // CHT fields
        engineRow.CHT1 = getValue("Eng1CHT1(°F)").flatMap(parseFloat)
        engineRow.CHT2 = getValue("Eng1CHT2(°F)").flatMap(parseFloat)
        engineRow.CHT3 = getValue("Eng1CHT3(°F)").flatMap(parseFloat)
        engineRow.CHT4 = getValue("Eng1CHT4(°F)").flatMap(parseFloat)
        engineRow.CHT5 = getValue("Eng1CHT5(°F)").flatMap(parseFloat)
        engineRow.CHT6 = getValue("Eng1CHT6(°F)").flatMap(parseFloat)

        // EGT fields
        engineRow.EGT1 = getValue("Eng1EGT1(°F)").flatMap(parseFloat)
        engineRow.EGT2 = getValue("Eng1EGT2(°F)").flatMap(parseFloat)
        engineRow.EGT3 = getValue("Eng1EGT3(°F)").flatMap(parseFloat)
        engineRow.EGT4 = getValue("Eng1EGT4(°F)").flatMap(parseFloat)
        engineRow.EGT5 = getValue("Eng1EGT5(°F)").flatMap(parseFloat)
        engineRow.EGT6 = getValue("Eng1EGT6(°F)").flatMap(parseFloat)

        engineRow.percentPower = getValue("Eng1PercentPwr").flatMap(parseFloat)
        engineRow.fuelFlow = getValue("FuelFlow(Gal/hr)").flatMap(parseFloat)
        engineRow.mainBus1Potential = getValue("Eng1Bus1Volts").flatMap(parseFloat)
        engineRow.emergencyBusPotential = getValue("Eng1Bus3Volts").flatMap(parseFloat)

        return .engineLegacyRow(engineRow)
    }

    private func parseFlightRowOptimized(row _: [String], columnIndices _: [String: Int], getValue: (String) -> String?) -> R9LogEntry? {
        var flightRow = R9FlightRow()

        flightRow.Systime = getValue("Systime").flatMap(parseUInt)
        flightRow.Date = getValue("Date")
        flightRow.Time = getValue("Time")

        flightRow.filteredNormalAcceleration = getValue("Filtered NormAcc (G)").flatMap(parseFloat)
        flightRow.normalAcceleration = getValue("NormAcc (G)").flatMap(parseFloat)
        flightRow.longitudinalAcceleration = getValue("LongAcc (G)").flatMap(parseFloat)
        flightRow.lateralAcceleration = getValue("LateralAcc (G)").flatMap(parseFloat)
        flightRow.heading = getValue("Heading (°M)").flatMap(parseFloat)
        flightRow.pitch = getValue("Pitch (°)").flatMap(parseFloat)
        flightRow.roll = getValue("Roll (°)").flatMap(parseFloat)
        flightRow.pressureAltitude = getValue("PressureAltitude (ft)").flatMap(parseInt)
        flightRow.indicatedAirspeed = getValue("IndicatedAirspeed (kts)").flatMap(parseUInt)
        flightRow.trueAirspeed = getValue("TrueAirspeed (kts)").flatMap(parseUInt)
        flightRow.verticalSpeed = getValue("VerticalSpeed (ft/min)").flatMap(parseInt)
        flightRow.GPSLatitude = getValue("GPSLatitude").flatMap(parseFloat)
        flightRow.GPSLongitude = getValue("GPSLongitude").flatMap(parseFloat)

        if let latMode = getValue("DFC100 Lat Active").flatMap(parseUInt8) {
            flightRow.DFC100_activeLateralMode = DFC100LateralMode(rawValue: latMode)
        }
        if let vertMode = getValue("DFC100 Vert Active").flatMap(parseUInt8) {
            flightRow.DFC100_activeVerticalMode = DFC100VerticalMode(rawValue: vertMode)
        }

        return .flightRow(flightRow)
    }

    private func parseSystemRowOptimized(row _: [String], columnIndices _: [String: Int], getValue: (String) -> String?) -> R9LogEntry? {
        var systemRow = R9SystemRow()

        systemRow.Systime = getValue("Systime").flatMap(parseUInt)
        systemRow.Date = getValue("Date")
        systemRow.Time = getValue("Time")

        systemRow.oat = getValue("OutsideAirTemperature (°C)").flatMap(parseInt)
        systemRow.groundSpeed = getValue("GroundSpeed (kts)").flatMap(parseUInt)
        systemRow.groundTrack = getValue("GroundTrack (°M)").flatMap(parseInt)
        systemRow.altimeterSetting = getValue("AltimeterSetting (in.hg)").flatMap(parseFloat)
        systemRow.altitudeBug = getValue("AltBug (ft)").flatMap(parseInt)
        systemRow.headingBug = getValue("HdgBug (°)").flatMap(parseUInt16)
        systemRow.activeWaypoint = getValue("ActiveWptId")
        systemRow.navaidBearing = getValue("NavaidBrg (°M)").flatMap(parseUInt16)
        systemRow.OBS = getValue("OBS (°M)").flatMap(parseUInt16)
        systemRow.desiredTrack = getValue("DesiredTrack (°M)").flatMap(parseUInt16)
        systemRow.FMSCourse = getValue("FMSCourse (°M)").flatMap(parseUInt16)
        systemRow.magneticVariation = getValue("MagneticVariation (°)").flatMap(parseFloat)
        systemRow.distanceToWaypoint = getValue("DistanceToActiveWpt (nm)").flatMap(parseFloat)
        systemRow.crossTrackDeviation = getValue("CrossTrackDeviation (nm)").flatMap(parseFloat)

        if let apModeStr = getValue("AutopilotMode") {
            systemRow.autopilotMode = AutopilotMode(rawValue: apModeStr)
        }

        return .systemRow(systemRow)
    }

    private func parseEngineRow(fields: [String: String]) -> R9LogEntry? {
        var row = R9EngineRow()

        row.Systime = parseUInt(fields["Systime"])
        row.Date = fields["Date"]
        row.Time = fields["Time"]

        row.oilTemperature = parseInt(fields["Eng1 Oil Temperature"])
        row.oilPressure = parseUInt(fields["Eng1 Oil Pressure"])
        row.RPM = parseUInt(fields["Eng1 RPM"])
        row.manifoldPressure = parseFloat(fields["Eng1 Manifold Pressure (In.Hg)"])
        row.TIT = parseFloat(fields["Eng1 TIT (°F)"])

        row.CHT1 = parseFloat(fields["Eng1 CHT[1] (°F)"])
        row.CHT2 = parseFloat(fields["Eng1 CHT[2] (°F)"])
        row.CHT3 = parseFloat(fields["Eng1 CHT[3] (°F)"])
        row.CHT4 = parseFloat(fields["Eng1 CHT[4] (°F)"])
        row.CHT5 = parseFloat(fields["Eng1 CHT[5] (°F)"])
        row.CHT6 = parseFloat(fields["Eng1 CHT[6] (°F)"])

        row.EGT1 = parseFloat(fields["Eng1 EGT[1] (°F)"])
        row.EGT2 = parseFloat(fields["Eng1 EGT[2] (°F)"])
        row.EGT3 = parseFloat(fields["Eng1 EGT[3] (°F)"])
        row.EGT4 = parseFloat(fields["Eng1 EGT[4] (°F)"])
        row.EGT5 = parseFloat(fields["Eng1 EGT[5] (°F)"])
        row.EGT6 = parseFloat(fields["Eng1 EGT[6] (°F)"])

        row.percentPower = parseFloat(fields["Eng1 Percent Pwr"])
        row.fuelFlow = parseFloat(fields["FuelFlow"])
        row.fuelUsed = parseFloat(fields["FuelUsed"])
        row.fuelRemaining = parseFloat(fields["FuelRemaining"])
        row.fuelTimeRemaining = parseInt(fields["FuelTimeRemaining (min)"])
        row.fuelEconomy = parseFloat(fields["FuelEconomy"])

        row.mainBus1Potential = parseFloat(fields["Eng1 MBus1 Volts"])
        row.emergencyBusPotential = parseFloat(fields["Eng1 Bus2 Volts"])

        // Only return if we have a valid date
        guard row.Date != nil && row.Time != nil else { return nil }
        return .engineRow(row)
    }

    private func parseEngineLegacyRow(fields: [String: String]) -> R9LogEntry? {
        var row = R9EngineLegacyRow()

        row.Systime = parseUInt(fields["Systime"])
        row.Date = fields["Date"]
        row.Time = fields["Time"]

        row.oilTemperature = parseInt(fields["Eng1OilTemperature(°F)"])
        row.oilPressure = parseUInt(fields["Eng1OilPressure"])
        row.RPM = parseUInt(fields["Eng1RPM"])
        row.manifoldPressure = parseFloat(fields["Eng1ManifoldPressure(In.Hg)"])
        row.TIT = parseFloat(fields["Eng1TIT(°F)"])

        row.CHT1 = parseFloat(fields["Eng1CHT[1](°F)"])
        row.CHT2 = parseFloat(fields["Eng1CHT[2](°F)"])
        row.CHT3 = parseFloat(fields["Eng1CHT[3](°F)"])
        row.CHT4 = parseFloat(fields["Eng1CHT[4](°F)"])
        row.CHT5 = parseFloat(fields["Eng1CHT[5](°F)"])
        row.CHT6 = parseFloat(fields["Eng1CHT[6](°F)"])

        row.EGT1 = parseFloat(fields["Eng1EGT[1](°F)"])
        row.EGT2 = parseFloat(fields["Eng1EGT[2](°F)"])
        row.EGT3 = parseFloat(fields["Eng1EGT[3](°F)"])
        row.EGT4 = parseFloat(fields["Eng1EGT[4](°F)"])
        row.EGT5 = parseFloat(fields["Eng1EGT[5](°F)"])
        row.EGT6 = parseFloat(fields["Eng1EGT[6](°F)"])

        row.percentPower = parseFloat(fields["Eng1PercentPwr"])
        row.fuelFlow = parseFloat(fields["FuelFlow(Gal/hr)"])
        row.fuelUsed = parseFloat(fields["FuelUsed(Gal)"])
        row.fuelRemaining = parseFloat(fields["FuelRemaining(Gal)"])
        row.fuelTimeRemaining = parseInt(fields["FuelTimeRemaining(min)"])
        row.fuelEconomy = parseFloat(fields["FuelEconomy(nm/gal)"])

        row.mainBus1Potential = parseFloat(fields["Eng1Bus1Volts"])
        row.emergencyBusPotential = parseFloat(fields["Eng1Bus3Volts"])

        // Only return if we have a valid date
        guard row.Date != nil && row.Time != nil else { return nil }
        return .engineLegacyRow(row)
    }

    private func parseFlightRow(fields: [String: String]) -> R9LogEntry? {
        var row = R9FlightRow()

        row.Systime = parseUInt(fields["Systime"])
        row.Date = fields["Date"]
        row.Time = fields["Time"]

        row.filteredNormalAcceleration = parseFloat(fields["Filtered NormAcc (G)"])
        row.normalAcceleration = parseFloat(fields["NormAcc (G)"])
        row.longitudinalAcceleration = parseFloat(fields["LongAcc (G)"])
        row.lateralAcceleration = parseFloat(fields["LateralAcc (G)"])
        row.activeADAHRS = parseUInt8(fields["ADAHRSUsed"])
        row.AHRSStatus = parseUInt8Hex(fields["AHRSStatusbits"])
        row.heading = parseFloat(fields["Heading (°M)"])
        row.pitch = parseFloat(fields["Pitch (°)"])
        row.roll = parseFloat(fields["Roll (°)"])
        row.FDPitch = parseFloat(fields["FlightDirectorPitch (°)"])
        row.FDRoll = parseFloat(fields["FlightDirectorRoll (°)"])
        row.headingRate = parseFloat(fields["HeadingRate (°/sec)"])
        row.pressureAltitude = parseInt(fields["PressureAltitude (ft)"])
        row.indicatedAirspeed = parseUInt(fields["IndicatedAirspeed (kts)"])
        row.trueAirspeed = parseUInt(fields["TrueAirspeed (kts)"])
        row.verticalSpeed = parseInt(fields["VerticalSpeed (ft/min)"])
        row.GPSLatitude = parseFloatZeroAsNull(fields["GPSLatitude"])
        row.GPSLongitude = parseFloatZeroAsNull(fields["GPSLongitude"])
        row.bodyYawRate = parseFloat(fields["BodyYawRate (°/sec)"])
        row.bodyPitchRate = parseFloat(fields["BodyPitchRate (°/sec)"])
        row.bodyRollRate = parseFloat(fields["BodyRollRate (°/sec)"])
        row.magnetometerStatus = parseUInt8(fields["MagStatus"])
        row.IRUStatus = parseUInt8(fields["IRUStatus"])
        row.MPUStatus = parseUInt8(fields["MPUStatus"])
        row.ADCStatus = fields["ADCStatus"]
        row.AHRSSequence = parseUInt(fields["AHRSSeq"])
        row.ADCSequence = parseUInt(fields["ADCSeq"])
        row.AHRStartupMode = parseUInt8(fields["AHRSStartupMode"])

        if let val = parseUInt8(fields["DFC100 Lat Active"]) {
            row.DFC100_activeLateralMode = DFC100LateralMode(rawValue: val)
        }
        if let val = parseUInt8(fields["DFC100 Lat Armed"]) {
            row.DFC100_armedLateralMode = DFC100LateralMode(rawValue: val)
        }
        if let val = parseUInt8(fields["DFC100 Vert Active"]) {
            row.DFC100_activeVerticalMode = DFC100VerticalMode(rawValue: val)
        }
        if let val = parseUInt8(fields["DFC100 Vert Armed"]) {
            row.DFC100_armedVerticalMode = DFC100VerticalMode(rawValue: val)
        }

        row.DFC100_statusFlags = parseUIntHex(fields["DFC100 Status Flags"])
        row.DFC100_failFlags = parseUIntHex(fields["DFC100 Fail Flags"])
        row.DFC100_altitudeTarget = parseInt(fields["DFC100 Alt Target"])

        // Only return if we have a valid date
        guard row.Date != nil && row.Time != nil else { return nil }
        return .flightRow(row)
    }

    private func parseSystemRow(fields: [String: String]) -> R9LogEntry? {
        var row = R9SystemRow()

        row.Systime = parseUInt(fields["Systime"])
        row.Date = fields["Date"]
        row.Time = fields["Time"]

        row.oat = parseInt(fields["OutsideAirTemperature (°C)"])
        row.localizerDeviation = parseFloat(fields["LocalizerDeviation (-1..1)"])
        row.glideslopeDeviation = parseFloat(fields["GlideslopeDeviation (-1..1)"])
        row.flightDirectorOnOff = parseBool(fields["FlightDirectorOn_Off"])

        if let apModeStr = fields["AutopilotMode"] {
            row.autopilotMode = AutopilotMode(rawValue: apModeStr)
        }

        row.groundSpeed = parseUInt(fields["GroundSpeed (kts)"])
        row.groundTrack = parseInt(fields["GroundTrack (°M)"])
        row.crossTrackDeviation = parseFloat(fields["CrossTrackDeviation (nm)"])
        row.verticalDeviation = parseInt(fields["VerticalDeviation (ft)"])
        row.altimeterSetting = parseFloat(fields["AltimeterSetting (in.hg)"])
        row.altitudeBug = parseInt(fields["AltBug (ft)"])
        row.verticalSpeedBug = parseInt(fields["VSIBug (ft/min)"])
        row.headingBug = parseUInt16(fields["HdgBug (°)"])
        row.displayMode = parseUInt8(fields["DisplayMode"])
        row.navigationMode = parseUInt8(fields["NavigationMode"])
        row.activeWaypoint = fields["ActiveWptId"]
        row.activeGPS = parseUInt8(fields["GPSSelect"])
        row.navaidBearing = parseUInt16(fields["NavaidBrg (°M)"])
        row.OBS = parseUInt16(fields["OBS (°M)"])
        row.desiredTrack = parseUInt16(fields["DesiredTrack (°M)"])
        row.navFrequency = parseUInt(fields["NavFreq (kHz)"])
        row.courseSelect = parseUInt8(fields["CrsSelect"])
        row.navType = parseUInt8(fields["NavType"])
        row.courseDeviation = parseInt(fields["CourseDeviation (°)"])
        row.GPSAltitude = parseInt(fields["GPSAltitude (m)"])
        row.distanceToWaypoint = parseFloat(fields["DistanceToActiveWpt (nm)"])
        row.GPSState = parseUInt8(fields["GPSState"])
        row.GPSHorizontalProterctionLimit = parseFloat(fields["GPSHorizProtLimit (m)"])
        row.GPSVerticalProterctionLimit = parseFloat(fields["GPSVertProtLimit (m)"])
        row.SBAS_HPL = parseFloat(fields["HPL_SBAS (m)"])
        row.SBAS_VPL = parseFloat(fields["VPL_SBAS (m)"])
        row.HFOM = parseFloat(fields["HFOM (m)"])
        row.VFOM = parseFloat(fields["VFOM (m)"])
        row.FMSCourse = parseUInt16(fields["FmsCourse (°M)"])
        row.magneticVariation = parseFloatZeroAsNull(fields["MagVar (° -W/+E)"])
        row.GPSAltitudeMSL = parseInt(fields["GPS MSL Altitude (m)"])
        row.GPSHeightAGL = parseInt(fields["GPS AGL Height (m)"])
        row.FLTA_RTC = parseInt(fields["FLTA RTC (m)"])
        row.FLTA_ATC = parseInt(fields["FLTA ATC (m)"])
        row.FLTA_VerticalSpeed = parseInt(fields["FLTA vspd (fpm)"])
        row.FLTA_RTCDistance = parseInt(fields["FLTA RTC dist (m)"])
        row.FLTA_TerrainDistance = parseInt(fields["FLTA terr dist (m)"])
        row.FLTA_Status = parseUInt8Hex(fields["FLTA Status"])

        // Only return if we have a valid date
        guard row.Date != nil && row.Time != nil else { return nil }
        return .systemRow(row)
    }

    private func parseDateFromRow(_ row: [String]) -> Date? {
        guard row.count >= 3 else { return nil }
        let dateStr = row[1]
        let timeStr = row[2]

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

    // Parsing helper functions
    private func parseFloat(_ value: String?) -> Float? {
        guard let value, !value.isEmpty, value != "-" else { return nil }
        return Float(value)
    }

    private func parseFloatZeroAsNull(_ value: String?) -> Float? {
        guard let floatValue = parseFloat(value) else { return nil }
        return floatValue.isZero ? nil : floatValue
    }

    private func parseUInt(_ value: String?) -> UInt? {
        guard let value, !value.isEmpty, value != "-" else { return nil }
        return UInt(value)
    }

    private func parseInt(_ value: String?) -> Int? {
        guard let value, !value.isEmpty, value != "-" else { return nil }
        return Int(value)
    }

    private func parseUInt8(_ value: String?) -> UInt8? {
        guard let value, !value.isEmpty, value != "-" else { return nil }
        return UInt8(value)
    }

    private func parseUInt16(_ value: String?) -> UInt16? {
        guard let value, !value.isEmpty, value != "-" else { return nil }
        return UInt16(value)
    }

    private func parseUInt8Hex(_ value: String?) -> UInt8? {
        guard let value, !value.isEmpty, value != "-" else { return nil }
        let cleaned = value.replacingOccurrences(of: "0x", with: "")
        return UInt8(cleaned, radix: 16)
    }

    private func parseUIntHex(_ value: String?) -> UInt? {
        guard let value, !value.isEmpty, value != "-" else { return nil }
        let cleaned = value.replacingOccurrences(of: "0x", with: "")
        return UInt(cleaned, radix: 16)
    }

    private func parseBool(_ value: String?) -> Bool? {
        guard let value, !value.isEmpty, value != "-" else { return nil }
        switch value {
        case "0": return false
        case "1": return true
        default: return nil
        }
    }
}
